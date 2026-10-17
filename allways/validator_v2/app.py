"""Wiring: one typed config in, the component graph out. Nothing is probed with getattr; a feature is on
exactly when its component is in the list."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

import bittensor as bt

from allways import __spec_version__
from allways.solana.rpc import resolve_ws_url
from allways.validator_v2.actuator.actuator import Actuator
from allways.validator_v2.actuator.executors import EXECUTORS, SendingClient
from allways.validator_v2.actuator.planners import HygienePlanner, PoolPlanner, SwapPlanner
from allways.validator_v2.actuator.swap_context import SwapMemory
from allways.validator_v2.chain.gateway import Endpoint, Meter, UsageReport
from allways.validator_v2.chains.subtensor_view import SubtensorView
from allways.validator_v2.chains.watcher import ChainWatcher
from allways.validator_v2.export.component import Export
from allways.validator_v2.feed.events import EventFeed, LiveFeed
from allways.validator_v2.hubs import HubSpec, relayed_hubs
from allways.validator_v2.relay.component import VAULT_ADAPTERS, HubRelay
from allways.validator_v2.runtime.bus import Latest, Topic
from allways.validator_v2.runtime.component import Component
from allways.validator_v2.runtime.supervisor import Supervisor
from allways.validator_v2.scoring.component import Scoring, WeightSetter
from allways.validator_v2.serving.component import Serving
from allways.validator_v2.store.db import Store
from allways.validator_v2.world.snapshot import WorldReader

# Components that record their own feed position; the feed resumes from the oldest of them.
FEED_CONSUMERS = ('world', 'scoring')


@dataclass(frozen=True)
class Config:
    solana_rpc_url: str
    program_id: Any
    keypair: Any
    state_dir: str
    netuid: int
    # Rate limits and daily budgets per endpoint host. Unlisted hosts are still counted, never throttled.
    endpoints: Tuple[Endpoint, ...] = ()
    # Bittensor weights: set only with a wallet. The EMA alpha and epoch are the neuron's CLI settings.
    wallet: Any = None
    weights_alpha: float = 1.0
    epoch_length: int = 150
    # Not ported yet: off, so nothing half-built runs.
    relay: bool = False  # relay every vault-custody hub in hubs.HUBS
    serve: bool = False
    export: bool = False


@dataclass
class App:
    components: List[Component]
    supervisor: Supervisor
    meter: Meter


def build(
    cfg: Config,
    assets: Dict[str, Any],
    subtensor_factory: Callable[[], Any],
    export_storage: Optional[Any] = None,
) -> App:
    store = Store(os.path.join(cfg.state_dir, 'validator.db'))
    meter = Meter(list(cfg.endpoints))
    for asset in assets.values():
        session = getattr(asset, 'http', None) or getattr(getattr(asset, 'rpc', None), '_session', None)
        if session is not None:
            meter.attach(session)
    world = Latest()
    rounds = Latest()
    events = Topic('events')
    gaps = Topic('gaps')
    feed_status = Latest()
    head = Latest()

    world_reader = WorldReader(
        lambda: meter.solana_client(cfg.solana_rpc_url, cfg.program_id),
        events.subscribe('world'),
        gaps.subscribe('world'),
        feed_status,
        world,
        store,
    )
    watchers = chain_watchers(assets)
    swaps = SwapPlanner(watchers, assets, SwapMemory(store), program_id=cfg.program_id)
    actuator = Actuator(
        world,
        [swaps, PoolPlanner(), HygienePlanner(head, me=cfg.keypair.pubkey())],
        dict(EXECUTORS),
        lambda: meter.solana_client(cfg.solana_rpc_url, cfg.program_id, cfg.keypair, cls=SendingClient),
    )
    feed = EventFeed(
        lambda: meter.solana_client(cfg.solana_rpc_url, cfg.program_id).rpc,
        LiveFeed(resolve_ws_url(cfg.solana_rpc_url), cfg.program_id),
        lambda: store.resume_point(list(FEED_CONSUMERS)),
        events,
        gaps,
        cfg.program_id,
        status=feed_status,
    )
    scoring = Scoring(
        world,
        head,
        events.subscribe('scoring'),
        gaps.subscribe('scoring'),
        feed_status,
        store,
        rounds,
        assets,
        watchers,
    )
    components: List[Component] = [
        world_reader,
        actuator,
        feed,
        scoring,
        SubtensorView(subtensor_factory, cfg.netuid, head),
        WeightSetter(
            rounds,
            head,
            subtensor_factory,
            store,
            cfg.wallet,
            cfg.netuid,
            cfg.weights_alpha,
            cfg.epoch_length,
            version_key=__spec_version__,
        ),
        UsageReport(meter, list(cfg.endpoints)),
        *{id(w): w for w in watchers.values()}.values(),
    ]
    if cfg.relay:
        components += [_relay(h, world, events, actuator, store) for h in relayed_hubs()]
    if cfg.serve:
        components.append(Serving(world, actuator.queue, store))
    if cfg.export and export_storage is not None:
        components.append(Export(rounds, export_storage))

    def on_fatal(why: str) -> None:
        bt.logging.error(f'supervisor: {why} — exiting for restart')
        os._exit(1)

    return App(components, Supervisor(components, on_fatal), meter)


def chain_watchers(assets: Dict[str, Any]) -> Dict[str, ChainWatcher]:
    """One watcher per chain, shared by every asset on it (EVM tokens share their network's tip)."""
    by_chain: Dict[int, ChainWatcher] = {}
    out: Dict[str, ChainWatcher] = {}
    for asset_id, asset in assets.items():
        chain = asset.chain
        watcher = by_chain.get(id(chain))
        if watcher is None:
            watcher = ChainWatcher(asset.chain_def.host_chain or asset_id, chain, {}, asset.chain_def.seconds_per_block)
            by_chain[id(chain)] = watcher
        watcher.assets[asset_id] = asset
        out[asset_id] = watcher
    return out


def _relay(hub: HubSpec, world, events: Topic, actuator: Actuator, store) -> HubRelay:
    adapter = VAULT_ADAPTERS.get(hub.vault_kind or '')
    if adapter is None:
        raise RuntimeError(f'no vault adapter registered for hub {hub.chain} ({hub.vault_kind})')
    return HubRelay(hub, world, events.subscribe(f'relay-{hub.chain}'), lambda: adapter(hub), actuator.queue, store)
