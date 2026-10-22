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
import re
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Set, Tuple

import bittensor as bt
from solders.pubkey import Pubkey

from allways.solana import layouts, pdas
from allways.solana.client import swap_key_from_tx_hash
from allways.validator.feed.events import HISTORY_HORIZON_SECS, FeedStatus
from allways.validator.runtime.bus import Latest, Subscription
from allways.validator.runtime.component import Component
from allways.validator.store.db import Store
from allways.validator.world import persist
from allways.validator.world.binding import build_attribution
from allways.validator.world.touches import UNTYPED, AccountKey, touches

_HEX_HASH = re.compile(r'(0x)?[0-9a-f]{64}', re.IGNORECASE)


def tx_key(tx_hash: str) -> str:
    """Ledger key for a tx hash: hex-shaped hashes lowercase (events carry no chain to ask)."""
    return tx_hash.lower() if _HEX_HASH.fullmatch(tx_hash) else tx_hash


ACCOUNT_TYPES = ('Swap', 'Reservation', 'Pool', 'MinerState', 'MinerQuote', 'BondAttestation', 'Binding')
MULTIPLE_ACCOUNTS_MAX = 100
_TYPE_OF = {layouts.DISCRIMINATORS[t]: t for t in (*ACCOUNT_TYPES, 'Config')}
# With nothing changed, the World still saves this often, so its saved-at time stays fresh for a restore.
SAVE_HEARTBEAT_SECS = 30
# A full re-read this often, so a lost event (a dropped notification, an empty getTransaction) can't
# leave an account stale for longer: 8 getProgramAccounts an hour (one per type, and Config).
RESCAN_SECS = 3600.0


@dataclass(frozen=True)
class DepositClaim:
    swap_key: str  # hex
    slot: int  # the earliest claim wins, whatever order the events arrived in
    seen_at: int
    initiated: bool = False


@dataclass(frozen=True)
class Fulfillment:
    to_tx_hash: str
    block_time: int
    miner: str
    settled: bool = False  # the swap completed on-chain: this delivery is spent for good


@dataclass(frozen=True)
class World:
    taken_at: float
    slot: int  # every event at or below this slot is reflected
    complete: bool  # False while the feed is still catching up: event-fed ledgers may be missing entries
    config: Any
    accounts: Mapping[str, Mapping[str, Any]]  # account type → pda → decoded account
    attribution: Mapping[str, str] = field(default_factory=dict)  # miner pubkey → verified hotkey ss58
    deposits: Mapping[str, DepositClaim] = field(default_factory=dict)  # tx_key(source tx) → first claim
    fulfillments: Mapping[str, Fulfillment] = field(default_factory=dict)  # swap_key → reported delivery

    def all(self, name: str) -> Iterable[Tuple[str, Any]]:
        return self.accounts.get(name, {}).items()

    def get(self, name: str, pda: Any) -> Optional[Any]:
        return self.accounts.get(name, {}).get(str(pda))

    def hotkey_for(self, miner: Any) -> Optional[str]:
        return self.attribution.get(str(miner))


class Accounts:
    """The World under the program client's read names, for the code it shares with the CLI (swap intake,
    the request ops). Reads only: sends go through a real client."""

    def __init__(self, world: World, program_id: Any = None) -> None:
        self.world, self.program_id = world, program_id

    def get_all(self, name: str) -> List[Tuple[str, Any]]:
        return list(self.world.all(name))

    def get_config(self) -> Any:
        return self.world.config

    def get_miner_state(self, miner: Any) -> Optional[Any]:
        return self.world.get('MinerState', pdas.miner_state_pda(miner, self.program_id))

    def get_reservation(self, miner: Any, backing: str = 'sol') -> Optional[Any]:
        return self.world.get('Reservation', pdas.reservation_pda(miner, backing, self.program_id))

    def get_pool(self, miner: Any, backing: str = 'sol') -> Optional[Any]:
        return self.world.get('Pool', pdas.pool_pda(miner, backing, self.program_id))

    def get_swap(self, swap_key: Any) -> Optional[Any]:
        return self.world.get('Swap', pdas.swap_pda(swap_key, self.program_id))

    def get_quote(self, miner: Any, from_chain: str, to_chain: str, backing: str = 'sol') -> Optional[Any]:
        return self.world.get('MinerQuote', pdas.quote_pda(miner, from_chain, to_chain, backing, self.program_id))

    def get_quotes_for_direction(self, miner: Any, from_chain: str, to_chain: str) -> List[Any]:
        quotes = (self.get_quote(miner, from_chain, to_chain, backing) for backing in pdas.BACKING_BITS)
        return [q for q in quotes if q is not None]

    def get_bond_attestation(self, miner: Any, chain: str = 'tao') -> Optional[Any]:
        return self.world.get('BondAttestation', pdas.bond_attestation_pda(miner, chain, self.program_id))

    def miner_for(self, hotkey: str) -> Optional[Pubkey]:
        """The miner a hotkey is bound to, off the bindings the World verified (first bound wins)."""
        for miner, bound in self.world.attribution.items():
            if bound == hotkey:
                return Pubkey.from_string(miner)
        return None


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
        self._fulfillments: Dict[str, Fulfillment] = {}
        self._changes: persist.Changes = {}
        self._scan_slot = 0
        self._slot = 0
        self._need_scan = True
        self._scanned_at = 0.0
        self._pending_reads: Set[AccountKey] = set()  # cleared only by a read that landed
        self._requested: Set[AccountKey] = set()  # from other threads, via request_reads
        self._request_lock = threading.Lock()
        self._saved_at = 0.0
        self._saved_cursor: Optional[Tuple[int, str]] = None
        self._dropped = 0

    def setup(self) -> None:
        self._client = self._client_factory()
        self._need_scan = not self._restore()
        self._scanned_at = self.clock()  # a restore is as good as a scan until the next rescan

    def tick(self, now: float) -> None:
        status, _ = self.feed_status.get()  # read before draining: everything up to its cursor is drained below
        if self.gaps.drain() or self.events.dropped != self._dropped:
            self._dropped, self._need_scan = self.events.dropped, True
        if self._need_scan or now - self._scanned_at >= RESCAN_SECS:
            self._scan(routine=not self._need_scan)
            self._scanned_at = now
        for record in self.events.drain():
            try:
                self._pending_reads |= self._apply(record)
            except Exception as e:  # one bad event must not lose the rest of the drain
                bt.logging.warning(f'world: {record.name} at {record.signature} not applied: {e}')
                self._need_scan = True
        with self._request_lock:
            self._pending_reads |= self._requested
            self._requested = set()
        if self._pending_reads:
            # Raises on a failed read: the set is kept for the next tick. An account that came back but would not
            # decode is kept on its own, so it can't hold up the rest.
            self._pending_reads = self._read(set(self._pending_reads))
        self._prune(now)
        cursor = status.cursor if status is not None else None
        if self._changes or cursor != self._saved_cursor or now - self._saved_at >= SAVE_HEARTBEAT_SECS:
            meta = {'slot': self._slot, 'scan_slot': self._scan_slot, 'saved_at': int(now)}
            persist.save(self.store, self._changes, meta, cursor)
            self._changes, self._saved_at, self._saved_cursor = {}, now, cursor
        # Published every tick: taken_at is how the actuator knows this reader is alive.
        self._publish(now, complete=status is not None and not status.catching_up)

    def request_reads(self, keys: Set[AccountKey]) -> None:
        """Thread-safe: re-read these accounts on the next tick (the actuator found its copy stale)."""
        with self._request_lock:
            self._requested |= set(keys)

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
            r['swap_key']: Fulfillment(r['to_tx_hash'], r['block_time'], r['miner'], bool(r['settled']))
            for r in persist.load(self.store, 'world_fulfillments')
        }
        # The saved rows replaced the ledger the accounts above wrote: mark the live swaps' deposits again, so a
        # live swap spends its deposit however the save was ordered.
        for pda, swap in self._accounts.get('Swap', {}).items():
            self._note_initiated(swap, self._slot_of.get(('Swap', pda), 0))
        self._scan_slot = int(self.store.get_meta('world.scan_slot') or 0)
        self._slot = int(self.store.get_meta('world.slot') or 0)
        self._refresh_attribution()
        bt.logging.info(f'world: restored at slot {self._slot}')
        return self._config is not None

    def _scan(self, routine: bool = False) -> None:
        """Every account of every tracked type. Each read must be served at or past the slot read first (a
        node behind it refuses, and the scan fails whole and is retried); the scan then stands at the oldest
        slot a read was served at. An account the World already holds from a newer slot is left as it is."""
        slot = int(self._client.rpc.get_slot())
        program_id = self._client.program_id
        config_pda = str(pdas.config_pda(program_id))
        fresh: Dict[str, Tuple[int, Dict[str, bytes]]] = {}
        for name in (*ACCOUNT_TYPES, 'Config'):
            served_at, accounts = self._client.rpc.get_program_accounts_with_context(
                program_id, disc8=layouts.DISCRIMINATORS[name], min_context_slot=slot
            )
            fresh[name] = (
                int(served_at),
                {str(pda): raw for pda, raw in accounts if name != 'Config' or str(pda) == config_pda},
            )
        scan_slot = min(served_at for served_at, _ in fresh.values())
        missed = 0
        for name, old in self._accounts.items():
            served_at, accounts = fresh[name]
            for pda in set(old) - set(accounts):
                if self._slot_of.get((name, pda), 0) <= served_at:
                    self._drop(name, pda, served_at)
                    missed += 1
            missed += len(set(accounts) - set(old))
        for name, (served_at, accounts) in fresh.items():
            for pda, raw in accounts.items():
                if self._slot_of.get((name, pda), 0) <= served_at:
                    self._put_raw(name, pda, raw, served_at)
        # Only a value newer than the scan still needs its own slot to guard it against older events.
        self._slot_of = {key: at for key, at in self._slot_of.items() if at > scan_slot}
        self._scan_slot = scan_slot
        self._slot = max(self._slot, scan_slot)
        self._refresh_attribution()
        self._need_scan = False
        counts = ', '.join(f'{t}={len(v)}' for t, v in self._accounts.items())
        if not routine:
            bt.logging.info(f'world: scanned at slot {self._scan_slot}: {counts}')
        elif missed:
            bt.logging.warning(f'world: the hourly rescan found {missed} account(s) the events missed ({counts})')
        else:
            bt.logging.debug(f'world: hourly rescan matched the events ({counts})')

    # ── state changes ───────────────────────────────────────────────────────

    def _put_raw(self, name: str, pda: str, raw: bytes, slot: int, record: bool = True) -> None:
        decoded = self._client._decode(name, raw)
        if name == 'Config':
            self._config = decoded
        else:
            self._accounts[name][pda] = decoded
        if name == 'Swap':
            self._note_initiated(decoded, slot)
        self._slot_of[(name, pda)] = slot
        if record:
            self._changes.setdefault('world_accounts', {})[pda] = (pda, name, slot, raw)

    def _note_initiated(self, swap: Any, slot: int) -> None:
        """A live Swap spends its deposit, whether or not this reader saw the SwapInitiated event."""
        status = swap.status if isinstance(swap.status, str) else type(swap.status).__name__
        if status not in ('Active', 'Fulfilled'):
            return
        key, swap_key = tx_key(str(swap.from_tx_hash)), swap_key_from_tx_hash(swap.from_tx_hash).hex()
        held = self._deposits.get(key)
        if held is None:
            self._set_deposit(key, DepositClaim(swap_key, slot, int(self.clock()), True))
        elif not held.initiated:
            self._set_deposit(key, DepositClaim(held.swap_key, held.slot, held.seen_at, True))

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
            swap_key = bytes(f['swap_key']).hex()
            held = self._fulfillments.get(swap_key)
            settled = held is not None and held.settled
            self._set_fulfillment(swap_key, Fulfillment(str(f['to_tx_hash']), t, str(f['miner']), settled))
        elif record.name == 'SwapCompleted':
            # A completion can arrive before its fulfillment (backfill order); the fulfillment keeps the flag.
            swap_key = bytes(f['swap_key']).hex()
            held = self._fulfillments.get(swap_key) or Fulfillment('', t, str(f['miner']))
            self._set_fulfillment(swap_key, Fulfillment(held.to_tx_hash, held.block_time, held.miner, True))

    def _set_fulfillment(self, swap_key: str, f: Fulfillment) -> None:
        self._fulfillments[swap_key] = f
        row = (swap_key, f.to_tx_hash, f.block_time, f.miner, int(f.settled))
        self._changes.setdefault('world_fulfillments', {})[swap_key] = row

    def _set_deposit(self, key: str, claim: DepositClaim) -> None:
        self._deposits[key] = claim
        row = (key, claim.swap_key, claim.slot, claim.seen_at, int(claim.initiated))
        self._changes.setdefault('world_deposits', {})[key] = row

    def _read(self, keys: Set[AccountKey]) -> Set[AccountKey]:
        """Read ``keys`` and apply what came back; returns the accounts that came back undecodable."""
        undecodable: Set[AccountKey] = set()
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
                if name == UNTYPED:
                    self._put_untyped(pda, value, slot)
                    continue
                raw = base64.b64decode(value['data'][0]) if value is not None else b''
                # Anyone can fund any address, so an account that isn't the program's, or isn't of the type this
                # address holds, is no account of ours: absent, exactly as for an untyped read.
                if value is None or value.get('owner') != str(self._client.program_id) or _TYPE_OF.get(raw[:8]) != name:
                    self._drop(name, pda, slot)
                else:
                    try:
                        self._put_raw(name, pda, raw, slot)
                    except Exception as e:
                        bt.logging.warning(f'world: {name} {pda} did not decode, retrying it alone: {e}')
                        undecodable.add((name, pda))
                if name == 'Binding':
                    self._refresh_attribution()
        return undecodable

    def _put_untyped(self, pda: str, value: Optional[dict], slot: int) -> None:
        """An account named only by a truncated tx: ours iff our program owns it, typed by its discriminator."""
        name = None
        if value is not None and value.get('owner') == str(self._client.program_id):
            raw = base64.b64decode(value['data'][0])
            name = _TYPE_OF.get(raw[:8])
            if name is not None:
                self._put_raw(name, pda, raw, slot)
        held = [t for t, accounts in self._accounts.items() if pda in accounts and t != name]
        for t in held:  # closed, or no longer ours: whatever the World held under this address is gone
            self._drop(t, pda, slot)
        if name == 'Binding' or 'Binding' in held:
            self._refresh_attribution()

    def _refresh_attribution(self) -> None:
        self._attribution = build_attribution(_BindingSource(self._accounts['Binding'].items()))

    def _prune(self, now: float) -> None:
        floor = now - HISTORY_HORIZON_SECS
        for table, ledger, when in (
            ('world_deposits', self._deposits, lambda c: c.seen_at),
            ('world_fulfillments', self._fulfillments, lambda v: v.block_time),
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
