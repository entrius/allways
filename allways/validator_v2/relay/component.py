"""One relay per vault-custody hub: it mirrors the vault onto Solana (attestations, the heartbeat) and
carries Solana verdicts to the vault (slashes, fee true-ups, exits).

The relay engine and its jobs are lifted unchanged from ``allways.validator.relay``. What changes is
what they talk to: Solana reads come from the World, Solana votes go to the actuator's inbox, and the
store is this hub's ``RelayLedger``. The relay thread is the only caller of its vault."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import bittensor as bt

from allways.solana import pdas
from allways.validator.relay.engine import BondRelay, RelayConfig
from allways.validator_v2.actuator.planners import intent
from allways.validator_v2.feed.events import HISTORY_HORIZON_SECS, FeedStatus
from allways.validator_v2.hubs import HubSpec
from allways.validator_v2.relay.ledger import RelayLedger
from allways.validator_v2.runtime.bus import Latest, Subscription
from allways.validator_v2.runtime.component import Component
from allways.validator_v2.runtime.deadline_queue import DeadlineQueue
from allways.validator_v2.store.db import Store
from allways.validator_v2.world.snapshot import World

RELAY_EVENTS = frozenset({'SwapCompleted', 'SwapTimedOut', 'MinerBackingChanged'})


class _WorldSolana:
    """The program as the relay engine sees it: reads answered from the World, votes queued for the
    actuator. A vote is handed over, not confirmed; the engine's own throttle keeps it from re-asking."""

    def __init__(self, world: Latest[World], queue: DeadlineQueue, keypair: Any, program_id: Any) -> None:
        self.world, self.queue, self.keypair, self.program_id = world, queue, keypair, program_id

    def _w(self) -> World:
        world, _ = self.world.get()
        if world is None:
            raise RuntimeError('no World yet')
        return world

    def get_all(self, name: str) -> List[Tuple[str, Any]]:
        return list(self._w().all(name))

    def get_bond_attestation(self, miner: Any, chain: str) -> Optional[Any]:
        return self._w().get('BondAttestation', pdas.bond_attestation_pda(miner, chain, self.program_id))

    def get_miner_state(self, miner: Any) -> Optional[Any]:
        return self._w().get('MinerState', pdas.miner_state_pda(miner, self.program_id))

    def get_config(self) -> Any:
        return self._w().config

    def has_voted(self, *args: Any) -> bool:
        return False  # the heartbeat executor reads the round itself, at send time

    def vote_set_attestation(self, miner: Any, chain: str, balance: int, locked: bool, epoch: int) -> str:
        payload = (str(miner), chain, int(balance), bool(locked), int(epoch))
        self.queue.push(intent('vote_set_attestation', (str(miner), chain), None, time.time(), payload))
        return 'queued'

    def vote_attest_heartbeat(self) -> str:
        self.queue.push(intent('vote_attest_heartbeat', 'config', None, time.time()))
        return 'queued'


class _Relay(BondRelay):
    """The lifted engine, attributing miners off the World's verified bindings."""

    def __init__(self, world: Latest[World], *args: Any, **kwargs: Any) -> None:
        self._world = world
        super().__init__(*args, **kwargs)

    def attribution(self, refresh: bool = False) -> Dict[str, str]:
        world, _ = self._world.get()
        if world is not None:
            self._attribution, self._attribution_fresh = dict(world.attribution), True
        return self._attribution


class _SettledOnce:
    """The vault, with each miner's settled counter read at most once per tick: the attestation and the
    fee ledger both ask for it."""

    def __init__(self, vault: Any) -> None:
        self._vault = vault
        self._settled: Dict[str, Optional[int]] = {}

    def __getattr__(self, name: str) -> Any:
        return getattr(self._vault, name)

    def clear(self) -> None:
        self._settled.clear()

    def get_settled_total(self, hotkey: str) -> Optional[int]:
        if hotkey not in self._settled:
            self._settled[hotkey] = self._vault.get_settled_total(hotkey)
        return self._settled[hotkey]

    def vote_collect_fees_batch(self, entries: Any) -> Any:
        self._settled.clear()
        return self._vault.vote_collect_fees_batch(entries)


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
        keypair: Any,
        program_id: Any = None,
        config: Optional[RelayConfig] = None,
        clock=time.time,
    ) -> None:
        super().__init__(clock)
        self.name = f'relay-{hub.chain}'
        self.hub, self.world, self.events, self.feed_status = hub, world, events, feed_status
        self._vault_factory, self.queue, self.store = vault_factory, actuator_queue, store
        self.keypair, self.program_id = keypair, program_id
        self.cfg = config or RelayConfig.from_env()
        cadence = self.cfg.fee_cadence_secs
        self.ledger = RelayLedger(
            store, hub.chain, self._settled, boundary=lambda t: (t // cadence) * cadence, clock=lambda: self.clock()
        )
        self.relay: Optional[_Relay] = None
        self._vault: Optional[_SettledOnce] = None
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
        return self._vault.get_settled_total(hotkey) if hotkey is not None and self._vault is not None else None

    def _build(self) -> None:
        self._vault = _SettledOnce(self._vault_factory())
        solana = _WorldSolana(self.world, self.queue, self.keypair, self.program_id)
        self.relay = _Relay(
            self.world, solana, self._vault, self.ledger, backing=self.hub.chain, clock=self.clock, config=self.cfg
        )
        bt.logging.info(f'{self.name}: on, vault {getattr(self._vault, "address", "?")}')

    def tick(self, now: float) -> None:
        world, _ = self.world.get()
        if world is None or world.config is None:
            return
        if self.relay is None:
            self._build()  # a failure backs off and retries: a transient vault fault never disables the hub
        self._vault.clear()
        status, _ = self.feed_status.get()  # read before draining: everything up to its cursor is drained below
        records = self._held + [
            r for r in self.events.drain() if r.name in RELAY_EVENTS and int(r.slot or 0) >= self._acked_slot
        ]
        self._held = []
        with self.store.tx() as c:
            for record in records:
                try:
                    self.relay._ingest_event(record)
                except Exception as e:  # a verdict must never be dropped: hold it and the heartbeat
                    bt.logging.warning(f'{self.name}: could not ingest {record.name}: {e}')
                    self._held.append(record)
            if status is not None and status.cursor is not None:
                Store.ack(c, self.name, *status.cursor)
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
