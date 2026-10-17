"""Product serving: the axon (miner activation, reserve and confirm from the CLI) and the localhost seam
(the same ops, plus rate and status, for a validator's product offering).

The component is the request ops' context (``ctx``). Requests run on their own threads: program reads
come from the World (``accounts``), so a request costs no RPC to read; sends go out on ``client`` and
wait to land, because the caller needs the verdict. Chain checks use serving's own chain connections.
The tick finalizes routed seats this validator won and records swap outcomes."""

from __future__ import annotations

import threading
import time
from functools import partial
from types import SimpleNamespace
from typing import Any, Callable, Dict, Optional, Tuple

import bittensor as bt

from allways.validator.chains.subtensor_view import Head
from allways.validator.feed.events import FeedStatus
from allways.validator.runtime.bus import Latest, Subscription
from allways.validator.runtime.component import Component
from allways.validator.scoring.rewards import miner_states
from allways.validator.serving import axon as handlers
from allways.validator.serving.requests import finalize_won_seats
from allways.validator.serving.seam import start_seam
from allways.validator.serving.store import OUTCOMES, ServingStore
from allways.validator.store.db import Store
from allways.validator.world.snapshot import Accounts, World
from allways.vault import BondVaultClient

SERVING_EVENTS = frozenset({*OUTCOMES, 'SwapFulfilled'})


class _PerThread:
    """One client per request thread: a client's HTTP session isn't thread-safe."""

    def __init__(self, factory: Callable[[], Any]) -> None:
        self._factory, self._local = factory, threading.local()

    def __getattr__(self, name: str) -> Any:
        client = getattr(self._local, 'client', None)
        if client is None:
            client = self._local.client = self._factory()
        return getattr(client, name)


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
        program_id: Any = None,
        clock=time.time,
    ) -> None:
        super().__init__(clock)
        self.world, self.head, self.events, self.feed_status = world, head, events, feed_status
        self.db, self.store, self.client = store, serving_store, _PerThread(client_factory)
        self.providers, self.swap_report, self.relays = providers, swap_report, relays
        self._subtensor_factory, self.wallet = subtensor_factory, wallet
        self.neuron_config, self.seam, self.program_id = neuron_config, seam, program_id
        self.netuid = getattr(neuron_config, 'netuid', 0)
        self.subtensor: Any = None
        self.lock = threading.RLock()  # the axon's subtensor socket is not thread-safe
        self._axon: Any = None
        self._server: Any = None
        row = store.conn().execute("SELECT slot FROM acks WHERE consumer = 'serving'").fetchone()
        self._acked_slot = int(row['slot']) if row else 0

    # ── what the request ops read (``ctx``) ─────────────────────────────────

    def accounts(self) -> Accounts:
        world, _ = self.world.get()
        if world is None:
            raise RuntimeError('no World yet')
        return Accounts(world, self.program_id)

    @property
    def metagraph(self) -> Any:
        head, _ = self.head.get()
        if head is None:
            return SimpleNamespace(hotkeys=[], S=[], n=SimpleNamespace(item=lambda: 0))
        return head.metagraph

    def miner_states(self) -> Dict[str, Any]:
        """``{hotkey: MinerState}`` of bound miners on the metagraph: the ones a request may route to."""
        return miner_states(self.accounts().world, set(self.metagraph.hotkeys))

    def report(self) -> Dict[str, Any]:
        """The swap planner's last report: leg confirmations and refusals, by swap key."""
        report, _ = self.swap_report.get()
        return report or {}

    def vault(self) -> Optional[Any]:
        """A read-only client for the TAO hub's vault on serving's own subtensor; None without a relay."""
        relay = getattr(self.relays.get('tao'), 'relay', None)
        if relay is None or self.subtensor is None:
            return None
        return BondVaultClient(self.subtensor, relay.vault.address, metadata=relay.vault.metadata)

    # ── the component ───────────────────────────────────────────────────────

    def setup(self) -> None:
        self.subtensor = self._subtensor_factory()
        if self.seam is not None:
            self._server = start_seam(self, *self.seam)
        if self.wallet is not None and self.neuron_config is not None:
            self._serve_axon()

    def _serve_axon(self) -> None:
        axon = bt.Axon(wallet=self.wallet, config=self.neuron_config)
        self.subtensor.serve_axon(netuid=self.neuron_config.netuid, axon=axon)
        for name in ('miner_activate', 'swap_reserve', 'swap_confirm'):
            axon.attach(
                forward_fn=partial(getattr(handlers, f'handle_{name}'), self),
                blacklist_fn=partial(getattr(handlers, f'blacklist_{name}'), self),
                priority_fn=partial(getattr(handlers, f'priority_{name}'), self),
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
            finalized = finalize_won_seats(self, int(now))
            if finalized:
                bt.logging.info(f'serving: finalized routed seat(s) for {len(finalized)} miner(s)')
        self.store.prune(int(now))
