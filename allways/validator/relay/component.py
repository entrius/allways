"""One relay per vault-custody hub, on its own thread: the only caller of its vault (``engine``)."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Callable, List, Optional

import bittensor as bt

from allways.validator.actuator.planners import intent
from allways.validator.feed.events import HISTORY_HORIZON_SECS, FeedStatus
from allways.validator.hubs import HubSpec
from allways.validator.relay.engine import BondRelay, RelayConfig
from allways.validator.relay.ledger import RelayLedger
from allways.validator.runtime.bus import Latest, Subscription
from allways.validator.runtime.component import Component
from allways.validator.runtime.deadline_queue import DeadlineQueue
from allways.validator.store.db import Store
from allways.validator.world.snapshot import World

RELAY_EVENTS = frozenset({'SwapCompleted', 'SwapTimedOut', 'MinerBackingChanged'})


class HubRelay(Component):
    """Not critical: a lagging relay keeps its hub's entry fuse shut (the heartbeat stays down until its
    first full reconcile is clean), which is the fuse's job, and never delays the actuator."""

    interval_secs = 12.0
    stall_secs = 900.0  # the boot reconcile reads every bound miner's vault state, paced

    def __init__(
        self,
        hub: HubSpec,
        world: Latest[World],
        events: Subscription,
        feed_status: Latest[FeedStatus],
        vault_factory: Callable[[], Any],
        actuator_queue: DeadlineQueue,
        store: Store,
        program_id: Any = None,
        config: Optional[RelayConfig] = None,
        clock=time.time,
    ) -> None:
        super().__init__(clock)
        self.name = f'relay-{hub.chain}'
        self.hub, self.world, self.events, self.feed_status = hub, world, events, feed_status
        self._vault_factory, self.queue, self.store = vault_factory, actuator_queue, store
        self.program_id = program_id
        self.cfg = config or RelayConfig.from_env()
        cadence = self.cfg.fee_cadence_secs
        self.ledger = RelayLedger(
            store, hub.chain, self._settled, boundary=lambda t: (t // cadence) * cadence, clock=lambda: self.clock()
        )
        self.relay: Optional[BondRelay] = None
        self._held: List[Any] = []  # events whose ledger write failed, retried next tick
        row = store.conn().execute('SELECT slot FROM acks WHERE consumer = ?', (self.name,)).fetchone()
        self._acked_slot = int(row['slot']) if row else 0

    def has_pending_debit(self, miner: str) -> bool:
        """Read from the actuator: no new swap backed by this hub for a miner whose slash isn't applied yet."""
        try:
            return bool(self.ledger.open_relay_slashes(self.hub.chain, str(miner)))
        except Exception as e:
            bt.logging.warning(f'{self.name}: pending-debit check failed for {str(miner)[:8]} ({e}); assuming owed')
            return True

    def _settled(self, miner: str) -> Optional[int]:
        hotkey = self.relay.hotkey_for(miner) if self.relay is not None else None
        return self.relay.settled_total(hotkey) if hotkey is not None else None

    def _vote(self, kind: str, subject: Any, payload: Any) -> None:
        self.queue.push(intent(kind, subject, None, self.clock(), payload))

    def _build(self) -> None:
        vault = self._vault_factory()
        self.relay = BondRelay(
            lambda: self.world.get()[0],
            self._vote,
            vault,
            self.ledger,
            backing=self.hub.chain,
            program_id=self.program_id,
            clock=self.clock,
            config=self.cfg,
        )
        bt.logging.info(f'{self.name}: on, vault {getattr(vault, "address", "?")}')

    def tick(self, now: float) -> None:
        world, _ = self.world.get()
        if world is None or world.config is None:
            return
        if self.relay is None:
            self._build()  # a failure backs off and retries: a transient vault fault never disables the hub
        status, _ = self.feed_status.get()  # read before draining: everything up to its cursor is drained below
        records = self._held + [
            r for r in self.events.drain() if r.name in RELAY_EVENTS and int(r.slot or 0) >= self._acked_slot
        ]
        self._held = []
        try:
            with self.store.tx() as c:
                for record in records:
                    try:
                        self.relay.ingest(record)
                    except Exception as e:  # a verdict must never be dropped: hold it and the heartbeat
                        bt.logging.warning(f'{self.name}: could not ingest {record.name}: {e}')
                        self._held.append(record)
                # While a verdict is held the ack stays put, so a restart replays it from the feed.
                if not self._held and status is not None and status.cursor is not None:
                    Store.ack(c, self.name, *status.cursor)
        except Exception:
            self._held = records  # the transaction itself failed: nothing above landed
            raise
        self.relay._ingest_healthy = not self._held
        self.relay.step(int(now))
        boundary = int(self.ledger.get_relay_meta('fee_cadence_boundary') or 0)
        self.ledger.fold(min(int(now) - HISTORY_HORIZON_SECS, boundary))


def ink_vault(subtensor_factory: Callable[[], Any], wallet: Any) -> Callable[[], Any]:
    """The TAO hub's ink! vault client, on its own subtensor connection. Raises ``VaultConfigError`` when
    no vault address is configured: no address, no relay."""
    from allways.vault import BondVaultClient
    from allways.vault.client import resolve_signer, resolve_vault_address

    try:
        config = json.loads((Path.home() / '.allways' / 'config.json').read_text())
    except Exception:
        config = {}
    resolve_vault_address(config)  # env wins over the `alw config` file
    return lambda: BondVaultClient.from_config(subtensor_factory(), config, keypair=resolve_signer(wallet))
