"""Product serving: the axon (miner activation, reserve and confirm from the CLI) and the localhost seam
(the same ops, plus rate and status, for a validator's product offering).

The request ops are lifted unchanged from ``allways.validator.reserve_engine`` and run on the request's
own thread, answering synchronously as before. Their program reads come from the World, so a request costs
no RPC to read; their sends (open_or_request, submit_swap_claim, finalize, vote_activate) still go out
from the request and wait to land, because the caller needs the verdict. Chain checks use serving's own
chain connections. The tick finalizes routed seats this validator won and records swap outcomes."""

from __future__ import annotations

import threading
import time
from functools import partial
from types import SimpleNamespace
from typing import Any, Callable, Dict, List, Optional, Tuple

import bittensor as bt

from allways.constants import FEE_DIVISOR
from allways.solana import pdas
from allways.solana.client import AllwaysSolanaClient
from allways.validator import axon_handlers as handlers
from allways.validator.binding import hotkey_ss58
from allways.validator.reserve_engine import finalize_won_seats
from allways.validator.seam_http import start_seam
from allways.validator_v2.chains.subtensor_view import Head
from allways.validator_v2.feed.events import FeedStatus
from allways.validator_v2.runtime.bus import Latest, Subscription
from allways.validator_v2.runtime.component import Component
from allways.validator_v2.serving.store import OUTCOMES, ServingStore
from allways.validator_v2.store.db import Store
from allways.validator_v2.world.snapshot import ACCOUNT_TYPES, World

SERVING_EVENTS = frozenset({*OUTCOMES, 'SwapFulfilled'})


class WorldClient(AllwaysSolanaClient):
    """The program client for requests: account reads come from the World, sends go out as usual."""

    def __init__(self, world: Latest[World], *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._latest = world
        self._just_sent: Dict[Tuple[str, str], Any] = {}  # read once, live, right after our own send

    def _world(self) -> World:
        world, _ = self._latest.get()
        if world is None:
            raise RuntimeError('no World yet')
        return world

    def _get(self, name: str, pda: Any) -> Any:
        if (name, str(pda)) in self._just_sent:
            return self._just_sent.pop((name, str(pda)))
        if name == 'Config':
            return self._world().config
        if name in ACCOUNT_TYPES:
            return self._world().get(name, pda)
        return super()._get(name, pda)

    def open_or_request(self, miner: Any, from_chain: str, to_chain: str, backing: str = 'sol') -> str:
        """The caller reads the pool straight back for its close time, before the World can have seen it."""
        sig = super().open_or_request(miner, from_chain, to_chain, backing)
        pda = pdas.pool_pda(miner, backing, self.program_id)
        self._just_sent[('Pool', str(pda))] = super()._get('Pool', pda)
        return sig

    def get_all(self, name: str) -> List[Tuple[str, Any]]:
        return list(self._world().all(name)) if name in ACCOUNT_TYPES else super().get_all(name)

    def get_hotkey_binding(self, hotkey: bytes) -> Optional[Any]:
        """The miner the World attributes this hotkey to: bindings it already verified, first bound wins."""
        world, ss58 = self._world(), hotkey_ss58(bytes(hotkey))
        for _, binding in world.all('Binding'):
            if world.hotkey_for(binding.miner) == ss58:
                return SimpleNamespace(miner=binding.miner)
        return None


class _PerThread:
    """One client per request thread: a client's HTTP session isn't thread-safe."""

    def __init__(self, factory: Callable[[], Any]) -> None:
        self._factory, self._local = factory, threading.local()

    def __getattr__(self, name: str) -> Any:
        client = getattr(self._local, 'client', None)
        if client is None:
            client = self._local.client = self._factory()
        return getattr(client, name)


class _View:
    """Exactly what the lifted request ops and axon handlers read off the old validator object."""

    def __init__(self, serving: 'Serving') -> None:
        self._s = serving
        self.solana_client = _PerThread(serving._client_factory)
        self.state_store = serving.store
        self.axon_assets = self.assets = serving.providers
        self.axon_lock = threading.RLock()
        self.solana_config_cache = SimpleNamespace(config=lambda: self.solana_client.get_config())
        self.crank_scheduler = SimpleNamespace(schedule=lambda miner, closes_at: None)  # the actuator cranks pools
        self.config = serving.neuron_config

    @property
    def metagraph(self) -> Any:
        head, _ = self._s.head.get()
        if head is None:
            return SimpleNamespace(hotkeys=[], S=[], n=SimpleNamespace(item=lambda: 0))
        return head.metagraph

    @property
    def held_states(self) -> Optional[Tuple[float, Dict[str, Any]]]:
        """{hotkey: MinerState} of bound miners on the metagraph, which /rate quotes off."""
        world, _ = self._s.world.get()
        if world is None:
            return None
        hotkeys = set(self.metagraph.hotkeys)
        states = {}
        for _, ms in world.all('MinerState'):
            hotkey = world.hotkey_for(ms.miner)
            if hotkey is not None and hotkey in hotkeys:
                states[hotkey] = ms
        return world.taken_at, states

    @property
    def axon_subtensor(self) -> Any:
        return self._s.subtensor

    @property
    def bond_relay(self) -> Any:
        return getattr(self._s.relays.get('tao'), 'relay', None)  # None until its vault client is up

    @property
    def solana_swap_loop(self) -> Any:
        """The swap planner's last report, in the shape /status reads."""
        report, _ = self._s.swap_report.get()
        confs: Dict[str, Dict[str, Dict[str, int]]] = {}
        for key, r in (report or {}).items():
            confs[key] = {
                ('source' if chain == r.from_chain else 'dest'): {'have': have, 'need': need, 'at': int(r.checked_at)}
                for chain, (have, need) in r.legs.items()
            }
        reasons = {k: r.reason for k, r in (report or {}).items() if r.decision == 'reject' and r.reason}
        return SimpleNamespace(
            leg_confs=confs,
            reject_reasons=reasons,
            providers=self.axon_assets,
            fee_divisor=FEE_DIVISOR,
            read_only=False,
        )


class Serving(Component):
    """Not critical: a slow seat finalize or a stuck request never touches the actuator."""

    name = 'serving'
    interval_secs = 1.0
    stall_secs = 120.0

    def __init__(
        self,
        world: Latest[World],
        head: Latest[Head],
        events: Subscription,
        feed_status: Latest[FeedStatus],
        store: Store,
        serving_store: ServingStore,
        client_factory: Callable[[], Any],
        providers: Dict[str, Any],
        swap_report: Latest,
        relays: Dict[str, Any],
        subtensor_factory: Callable[[], Any],
        wallet: Any = None,  # with neuron_config: serve the axon
        neuron_config: Any = None,
        seam: Optional[Tuple[int, str]] = None,  # (port, secret): serve the seam
        clock=time.time,
    ) -> None:
        super().__init__(clock)
        self.world, self.head, self.events, self.feed_status = world, head, events, feed_status
        self.db, self.store, self._client_factory = store, serving_store, client_factory
        self.providers, self.swap_report, self.relays = providers, swap_report, relays
        self._subtensor_factory, self.wallet = subtensor_factory, wallet
        self.neuron_config, self.seam = neuron_config, seam
        self.subtensor: Any = None
        self.view = _View(self)
        self._axon: Any = None
        self._server: Any = None
        row = store.conn().execute("SELECT slot FROM acks WHERE consumer = 'serving'").fetchone()
        self._acked_slot = int(row['slot']) if row else 0

    def setup(self) -> None:
        self.subtensor = self._subtensor_factory()
        if self.seam is not None:
            self._server = start_seam(self.view, *self.seam)
        if self.wallet is not None and self.neuron_config is not None:
            self._serve_axon()

    def _serve_axon(self) -> None:
        axon = bt.Axon(wallet=self.wallet, config=self.neuron_config)
        self.subtensor.serve_axon(netuid=self.neuron_config.netuid, axon=axon)
        for name in ('miner_activate', 'swap_reserve', 'swap_confirm'):
            axon.attach(
                forward_fn=partial(getattr(handlers, f'handle_{name}'), self.view),
                blacklist_fn=partial(getattr(handlers, f'blacklist_{name}'), self.view),
                priority_fn=partial(getattr(handlers, f'priority_{name}'), self.view),
            )
        axon.start()
        self._axon = axon
        bt.logging.info(f'serving: axon {axon} up')

    def tick(self, now: float) -> None:
        world, _ = self.world.get()
        if world is None:
            return
        status, _ = self.feed_status.get()  # read before draining: everything up to its cursor is drained below
        records = [r for r in self.events.drain() if r.name in SERVING_EVENTS and int(r.slot or 0) >= self._acked_slot]
        with self.db.tx() as c:
            for record in records:
                self.store.ingest(record)
            if status is not None and status.cursor is not None:
                Store.ack(c, self.name, *status.cursor)
        if self.store.distinct_routed_pools():
            finalized = finalize_won_seats(self.view, int(now))
            if finalized:
                bt.logging.info(f'serving: finalized routed seat(s) for {len(finalized)} miner(s)')
        self.store.prune(int(now))
