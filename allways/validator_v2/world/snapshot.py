"""World: the program's live accounts plus the short event-derived ledgers, as one consistent snapshot.

At boot the reader restores its last saved state, and the feed's replay from the World's own saved
position brings it current. It scans the program only when there is nothing recent to restore, or after
a history gap the feed could not fill. From then on every change arrives as an event: quotes are
applied straight from ``QuoteSet``, and every other touched account is re-read in one batched
``getMultipleAccounts`` per tick, pinned to the event's slot so a read is never older than the event
that caused it. A closed account reads back null and leaves the World. No component scans the program
on its own."""

from __future__ import annotations

import base64
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Set, Tuple

import bittensor as bt

from allways.solana import layouts, pdas
from allways.validator.binding import build_attribution
from allways.validator.state_store import tx_key
from allways.validator_v2.feed.events import HISTORY_HORIZON_SECS, FeedStatus
from allways.validator_v2.runtime.bus import Latest, Subscription
from allways.validator_v2.runtime.component import Component
from allways.validator_v2.store.db import Store
from allways.validator_v2.world import persist
from allways.validator_v2.world.touches import AccountKey, touches

ACCOUNT_TYPES = ('Swap', 'Reservation', 'Pool', 'MinerState', 'MinerQuote', 'BondAttestation', 'Binding')
MULTIPLE_ACCOUNTS_MAX = 100


@dataclass(frozen=True)
class DepositClaim:
    swap_key: str  # hex
    slot: int  # the earliest claim wins, whatever order the events arrived in
    seen_at: int
    initiated: bool = False


@dataclass(frozen=True)
class World:
    taken_at: float
    slot: int  # every event at or below this slot is reflected
    complete: bool  # False while the feed is still catching up: event-fed ledgers may be missing entries
    config: Any
    accounts: Mapping[str, Mapping[str, Any]]  # account type → pda → decoded account
    attribution: Mapping[str, str] = field(default_factory=dict)  # miner pubkey → verified hotkey ss58
    deposits: Mapping[str, DepositClaim] = field(default_factory=dict)  # tx_key(source tx) → first claim
    fulfillments: Mapping[str, Tuple[str, int]] = field(default_factory=dict)  # swap_key → (to_tx_hash, block_time)

    def all(self, name: str) -> Iterable[Tuple[str, Any]]:
        return self.accounts.get(name, {}).items()

    def get(self, name: str, pda: Any) -> Optional[Any]:
        return self.accounts.get(name, {}).get(str(pda))

    def hotkey_for(self, miner: Any) -> Optional[str]:
        return self.attribution.get(str(miner))


class _BindingSource:
    """Lets build_attribution verify the Binding accounts already in hand instead of scanning again."""

    def __init__(self, bindings: Iterable[Tuple[str, Any]]) -> None:
        self._bindings = list(bindings)

    def get_all(self, name: str) -> List[Tuple[str, Any]]:
        return self._bindings


class WorldReader(Component):
    """Critical: the actuator decides off this World, so a stalled reader means stale decisions."""

    name = 'world'
    interval_secs = 1.0
    stall_secs = 60.0
    critical = True

    def __init__(
        self,
        client_factory: Callable[[], Any],
        events: Subscription,
        gaps: Subscription,
        feed_status: Latest[FeedStatus],
        out: Latest[World],
        store: Store,
        clock=time.time,
    ) -> None:
        super().__init__(clock)
        self._client_factory = client_factory
        self.events = events
        self.gaps = gaps
        self.feed_status = feed_status
        self.out = out
        self.store = store
        persist.declare(store)
        self._client: Any = None
        self._accounts: Dict[str, Dict[str, Any]] = {t: {} for t in ACCOUNT_TYPES}
        self._slot_of: Dict[AccountKey, int] = {}  # the slot each account's current value is from
        self._config: Any = None
        self._attribution: Dict[str, str] = {}
        self._deposits: Dict[str, DepositClaim] = {}
        self._initiated: Dict[str, int] = {}  # swap_key → block_time of its SwapInitiated
        self._fulfillments: Dict[str, Tuple[str, int]] = {}
        self._changes: persist.Changes = {}
        self._scan_slot = 0
        self._slot = 0
        self._need_scan = True

    def setup(self) -> None:
        self._client = self._client_factory()
        self._need_scan = not self._restore()

    def tick(self, now: float) -> None:
        status, _ = self.feed_status.get()  # read before draining: everything up to its cursor is drained below
        if self.gaps.drain():
            self._need_scan = True
        if self._need_scan:
            self._scan()
        reads: Set[AccountKey] = set()
        for record in self.events.drain():
            reads |= self._apply(record)
        if reads:
            self._read(reads)
        self._prune(now)
        persist.save(
            self.store,
            self._changes,
            {'slot': self._slot, 'scan_slot': self._scan_slot, 'saved_at': int(now)},
            status.cursor if status is not None else None,
        )
        self._changes = {}
        # Published every tick: taken_at is how the actuator knows this reader is alive.
        self._publish(now, complete=status is not None and not status.catching_up)

    # ── restore and scan ────────────────────────────────────────────────────

    def _restore(self) -> bool:
        """Load the saved World if it is recent enough for the feed's replay to bring it current."""
        saved_at = self.store.get_meta('world.saved_at')
        if saved_at is None or self.clock() - int(saved_at) > HISTORY_HORIZON_SECS:
            return False
        for row in persist.load(self.store, 'world_accounts'):
            self._put_raw(row['type'], row['pda'], bytes(row['data']), int(row['slot']), record=False)
        for row in persist.load(self.store, 'world_deposits'):
            self._deposits[row['tx_key']] = DepositClaim(
                row['swap_key'], row['slot'], row['seen_at'], bool(row['initiated'])
            )
        self._initiated = {r['swap_key']: r['at'] for r in persist.load(self.store, 'world_initiated')}
        self._fulfillments = {
            r['swap_key']: (r['to_tx_hash'], r['block_time']) for r in persist.load(self.store, 'world_fulfillments')
        }
        self._scan_slot = int(self.store.get_meta('world.scan_slot') or 0)
        self._slot = int(self.store.get_meta('world.slot') or 0)
        self._refresh_attribution()
        bt.logging.info(f'world: restored at slot {self._slot}')
        return self._config is not None

    def _scan(self) -> None:
        """Every account of every tracked type. The slot is read first, so everything the scan returns
        is at least that new and older events must not overwrite it."""
        slot = int(self._client.rpc.get_slot())
        program_id = self._client.program_id
        fresh: Dict[str, Dict[str, Any]] = {}
        for name in ACCOUNT_TYPES:
            fresh[name] = {}
            for pda, raw in self._client.rpc.get_program_accounts(program_id, disc8=layouts.DISCRIMINATORS[name]):
                fresh[name][str(pda)] = raw
        config_pda = str(pdas.config_pda(program_id))
        config_raw = self._client.rpc.get_account_info(config_pda)
        for name, old in self._accounts.items():
            for pda in set(old) - set(fresh[name]):
                self._drop(name, pda, slot)
        for name, accounts in fresh.items():
            for pda, raw in accounts.items():
                self._put_raw(name, pda, raw, slot)
        if config_raw is not None:
            self._put_raw('Config', config_pda, config_raw, slot)
        self._slot_of.clear()
        self._scan_slot = slot
        self._slot = max(self._slot, slot)
        self._refresh_attribution()
        self._need_scan = False
        bt.logging.info(
            f'world: scanned at slot {slot}: ' + ', '.join(f'{t}={len(v)}' for t, v in self._accounts.items())
        )

    # ── state changes ───────────────────────────────────────────────────────

    def _put_raw(self, name: str, pda: str, raw: bytes, slot: int, record: bool = True) -> None:
        decoded = self._client._decode(name, raw)
        if name == 'Config':
            self._config = decoded
        else:
            self._accounts[name][pda] = decoded
        self._slot_of[(name, pda)] = slot
        if record:
            self._changes.setdefault('world_accounts', {})[pda] = (pda, name, slot, raw)

    def _drop(self, name: str, pda: str, slot: int) -> None:
        self._accounts.get(name, {}).pop(pda, None)
        self._slot_of[(name, pda)] = slot
        self._changes.setdefault('world_accounts', {})[pda] = None

    def _newer(self, key: AccountKey, slot: int) -> bool:
        return slot >= max(self._scan_slot, self._slot_of.get(key, 0))

    def _apply(self, record: Any) -> Set[AccountKey]:
        """Fold one event: ledgers always (they are history, not state), accounts only if the event is
        newer than what the World holds. Returns the accounts to re-read."""
        slot = int(record.slot or 0)
        self._slot = max(self._slot, slot)
        self._ledgers(record, slot)
        t = touches(record.name, record.fields, self._client.program_id)
        for pda, quote in t.quote_upserts:
            if self._newer(('MinerQuote', pda), slot):
                raw = layouts.DISCRIMINATORS['MinerQuote'] + layouts.ACCOUNT_LAYOUTS['MinerQuote'].build(quote)
                self._put_raw('MinerQuote', pda, raw, slot)
        for pda in t.quote_removals:
            if self._newer(('MinerQuote', pda), slot):
                self._drop('MinerQuote', pda, slot)
        return {key for key in t.reads if self._newer(key, slot)}

    def _ledgers(self, record: Any, slot: int) -> None:
        f, t = record.fields, int(record.block_time or 0)
        if record.name == 'SwapClaimed':
            key, swap_key = tx_key(str(f['from_tx_hash'])), bytes(f['swap_key']).hex()
            held = self._deposits.get(key)
            if held is None or slot < held.slot:
                self._set_deposit(key, DepositClaim(swap_key, slot, t, swap_key in self._initiated))
        elif record.name == 'SwapInitiated':
            swap_key = bytes(f['swap_key']).hex()
            self._initiated[swap_key] = t
            self._changes.setdefault('world_initiated', {})[swap_key] = (swap_key, t)
            for key, claim in list(self._deposits.items()):
                if claim.swap_key == swap_key and not claim.initiated:
                    self._set_deposit(key, DepositClaim(claim.swap_key, claim.slot, claim.seen_at, True))
        elif record.name == 'SwapFulfilled':
            swap_key, to_tx = bytes(f['swap_key']).hex(), str(f['to_tx_hash'])
            self._fulfillments[swap_key] = (to_tx, t)
            self._changes.setdefault('world_fulfillments', {})[swap_key] = (swap_key, to_tx, t)

    def _set_deposit(self, key: str, claim: DepositClaim) -> None:
        self._deposits[key] = claim
        row = (key, claim.swap_key, claim.slot, claim.seen_at, int(claim.initiated))
        self._changes.setdefault('world_deposits', {})[key] = row

    def _read(self, keys: Set[AccountKey]) -> None:
        ordered = sorted(keys)
        for i in range(0, len(ordered), MULTIPLE_ACCOUNTS_MAX):
            chunk = ordered[i : i + MULTIPLE_ACCOUNTS_MAX]
            result = self._client.rpc._call(
                'getMultipleAccounts',
                [
                    [pda for _, pda in chunk],
                    {'encoding': 'base64', 'commitment': 'confirmed', 'minContextSlot': self._slot},
                ],
            )
            slot = int(result['context']['slot'])
            for (name, pda), value in zip(chunk, result['value']):
                if value is None:
                    self._drop(name, pda, slot)
                else:
                    self._put_raw(name, pda, base64.b64decode(value['data'][0]), slot)
                if name == 'Binding':
                    self._refresh_attribution()

    def _refresh_attribution(self) -> None:
        self._attribution = build_attribution(_BindingSource(self._accounts['Binding'].items()))

    def _prune(self, now: float) -> None:
        floor = now - HISTORY_HORIZON_SECS
        for table, ledger, when in (
            ('world_deposits', self._deposits, lambda c: c.seen_at),
            ('world_fulfillments', self._fulfillments, lambda v: v[1]),
            ('world_initiated', self._initiated, lambda t: t),
        ):
            for key in [k for k, v in ledger.items() if 0 < when(v) < floor]:
                del ledger[key]
                self._changes.setdefault(table, {})[key] = None

    def _publish(self, now: float, complete: bool) -> None:
        self.out.set(
            World(
                taken_at=now,
                slot=self._slot,
                complete=complete,
                config=self._config,
                accounts={t: dict(v) for t, v in self._accounts.items()},
                attribution=dict(self._attribution),
                deposits=dict(self._deposits),
                fulfillments=dict(self._fulfillments),
            )
        )
