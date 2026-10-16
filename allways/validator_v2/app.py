"""Wiring: one typed config in, the component graph out. Nothing is probed with getattr; a feature is on
exactly when its component is in the list."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

import bittensor as bt

from allways.solana.rpc import resolve_ws_url
from allways.validator_v2.actuator.actuator import Actuator
from allways.validator_v2.actuator.planners import HygienePlanner, PoolPlanner, SwapPlanner
from allways.validator_v2.chain.gateway import RpcGateway
from allways.validator_v2.chains.watcher import ChainWatcher
from allways.validator_v2.export.component import Export
from allways.validator_v2.feed.events import EventFeed, FileCursorStore, LiveFeed
from allways.validator_v2.hubs import HubSpec, relayed_hubs
from allways.validator_v2.relay.component import VAULT_ADAPTERS, HubRelay
from allways.validator_v2.runtime.bus import Latest, Topic
from allways.validator_v2.runtime.component import Component
from allways.validator_v2.runtime.supervisor import Supervisor
from allways.validator_v2.scoring.component import Scoring, WeightSetter
from allways.validator_v2.serving.component import Serving
from allways.validator_v2.world.snapshot import WorldReader


@dataclass(frozen=True)
class Config:
    solana_rpc_url: str
    program_id: Any
    keypair: Any
    state_dir: str
    rpc_rate: float = 8.0  # requests/second, under the plan's limit with headroom for the miner/indexer
    rpc_burst: float = 16.0
    relay: bool = True  # relay every vault-custody hub in hubs.HUBS
    serve: bool = True
    export: bool = False


@dataclass
class App:
    components: List[Component]
    supervisor: Supervisor
    gateway: RpcGateway


def build(
    cfg: Config,
    store: Any,
    assets: Dict[str, Any],
    subtensor_factory: Callable[[], Any],
    executors: Dict[str, Any],
    export_storage: Optional[Any] = None,
) -> App:
    gateway = RpcGateway(cfg.solana_rpc_url, cfg.rpc_rate, cfg.rpc_burst)
    world = Latest()
    rounds = Latest()
    events = Topic('events')
    gaps = Topic('gaps')
    feed_status = Latest()

    world_reader = WorldReader(
        lambda: gateway.client('world', cfg.program_id),
        events.subscribe('world'),
        gaps.subscribe('world'),
        feed_status,
        world,
    )
    watchers = chain_watchers(assets)
    actuator = Actuator(
        world,
        [SwapPlanner(watchers), PoolPlanner(), HygienePlanner()],
        executors,
        lambda: gateway.client('actuator', cfg.program_id, cfg.keypair),
    )
    feed = EventFeed(
        lambda: gateway.rpc('feed'),
        LiveFeed(resolve_ws_url(cfg.solana_rpc_url), cfg.program_id),
        FileCursorStore(os.path.join(cfg.state_dir, 'feed_cursor.json')),
        events,
        gaps,
        cfg.program_id,
        status=feed_status,
    )
    scoring = Scoring(world, events.subscribe('scoring'), gaps.subscribe('scoring'), store, rounds)
    components: List[Component] = [
        world_reader,
        actuator,
        feed,
        scoring,
        WeightSetter(rounds, subtensor_factory),
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

    return App(components, Supervisor(components, on_fatal), gateway)


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
