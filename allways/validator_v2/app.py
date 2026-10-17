"""Wiring: one typed config in, the component graph out. Nothing is probed with getattr; a feature is on
exactly when its component is in the list."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
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
from allways.validator_v2.hubs import relayed_hubs
from allways.validator_v2.relay.component import HubRelay
from allways.validator_v2.runtime.bus import Latest, Topic
from allways.validator_v2.runtime.component import Component
from allways.validator_v2.runtime.deadline_queue import DeadlineQueue
from allways.validator_v2.runtime.supervisor import Supervisor
from allways.validator_v2.scoring.component import Scoring, WeightSetter
from allways.validator_v2.serving.component import Serving
from allways.validator_v2.serving.store import ServingStore
from allways.validator_v2.store.db import Store
from allways.validator_v2.world.snapshot import WorldReader

# Components that record their own feed position; the feed resumes from the oldest of them (each relay too).
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
    # Vault clients by hub chain: a vault-custody hub is relayed exactly when its vault is configured.
    vaults: Dict[str, Callable[[], Any]] = field(default_factory=dict)
    # Serving runs when the axon (a wallet and the neuron config) or the seam ((port, secret)) is on. Its chain
    # checks use their own connections: request threads must not share the watchers'.
    axon_wallet: Any = None
    neuron_config: Any = None
    seam: Optional[Tuple[int, str]] = None
    serving_assets: Dict[str, Any] = field(default_factory=dict)


@dataclass
class App:
    components: List[Component]
    supervisor: Supervisor
    meter: Meter


def build(
    cfg: Config,
    assets: Dict[str, Any],
    subtensor_factory: Callable[[], Any],
    export_storage: Optional[Any] = None,  # the dashboard's Postgres writer: export runs when given
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
    inbox = DeadlineQueue()
    relays = {
        h.chain: HubRelay(
            h,
            world,
            events.subscribe(f'relay-{h.chain}'),
            feed_status,
            cfg.vaults[h.chain],
            inbox,
            store,
            cfg.program_id,
        )
        for h in relayed_hubs()
        if h.chain in cfg.vaults
    }

    def owes_settlement(swap: Any) -> bool:
        relay = relays.get(str(swap.collateral_chain).lower())
        return relay is not None and relay.has_pending_debit(str(swap.miner))

    memory = SwapMemory(store)
    swaps = SwapPlanner(watchers, assets, memory, owes_settlement, program_id=cfg.program_id)
    serving = None
    if (cfg.axon_wallet is not None and cfg.neuron_config is not None) or cfg.seam is not None:
        serving = Serving(
            world,
            head,
            events.subscribe('serving'),
            feed_status,
            store,
            ServingStore(store, lambda: world.get()[0], memory, cfg.serving_assets),
            lambda: meter.solana_client(cfg.solana_rpc_url, cfg.program_id, cfg.keypair),
            cfg.serving_assets,
            swaps.report,
            relays,
            subtensor_factory,
            cfg.axon_wallet,
            cfg.neuron_config,
            cfg.seam,
            cfg.program_id,
        )
    actuator = Actuator(
        world,
        [swaps, PoolPlanner(), HygienePlanner(head, me=cfg.keypair.pubkey())],
        dict(EXECUTORS),
        lambda: meter.solana_client(cfg.solana_rpc_url, cfg.program_id, cfg.keypair, cls=SendingClient),
        inbox,
    )
    feed = EventFeed(
        lambda: meter.solana_client(cfg.solana_rpc_url, cfg.program_id).rpc,
        LiveFeed(resolve_ws_url(cfg.solana_rpc_url), cfg.program_id),
        lambda: store.resume_point(
            [*FEED_CONSUMERS, *(r.name for r in relays.values()), *(['serving'] if serving is not None else [])]
        ),
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
    components += relays.values()
    if serving is not None:
        components.append(serving)
    if export_storage is not None:
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
