"""Scoring's own history: when each miner's rate, active flag, purse and busy state changed, the fills it
completed, and the address bans. The crown replay reads it at an instant and over a window.

One events table holds the four transition kinds the replay merges (``EventKind``), keyed by the miner's
hotkey. Program events write the precise change times; each scoring round anchors it to the World
(``reconcile_live_state`` / ``reconcile_live_quotes``), which also seeds a fresh database. Kept for
``EVENT_RETENTION_SECS`` (fills for ``CLEARING_RETENTION_SECS``), with the latest row per key kept as the
anchor a window starts from."""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import bittensor as bt

from allways.classes import ActivityTransition, MinerActivity, next_activity
from allways.constants import CLEARING_RETENTION_SECS, EVENT_RETENTION_SECS, RATE_PRECISION, RECONCILE_QUIET_SECS
from allways.solana.pdas import BACKING_BITS
from allways.utils.rate import quantize_rate_fixed
from allways.validator.scoring import EventKind
from allways.validator_v2.store.db import Store

OWNER, VERSION = 'scoring', 1
TABLES = [
    (
        'scoring_events',
        'CREATE TABLE IF NOT EXISTS scoring_events (id INTEGER PRIMARY KEY AUTOINCREMENT, at INTEGER NOT NULL, '
        'hotkey TEXT NOT NULL, kind INTEGER NOT NULL, value NUMERIC NOT NULL, from_chain TEXT, to_chain TEXT, '
        'backing TEXT, hub TEXT)',
    ),
    ('scoring_events_at', 'CREATE INDEX IF NOT EXISTS scoring_events_at ON scoring_events (kind, at)'),
    (
        'scoring_fills',
        'CREATE TABLE IF NOT EXISTS scoring_fills (swap_key TEXT PRIMARY KEY, at INTEGER NOT NULL, '
        'hotkey TEXT NOT NULL, from_chain TEXT NOT NULL, to_chain TEXT NOT NULL, backing TEXT NOT NULL, '
        'from_amount TEXT NOT NULL, to_amount TEXT NOT NULL, collateral_amount TEXT NOT NULL, '
        'qualified INTEGER NOT NULL)',
    ),
    ('scoring_bans', 'CREATE TABLE IF NOT EXISTS scoring_bans (hotkey TEXT PRIMARY KEY, at INTEGER NOT NULL)'),
]
LEDGER_SINCE = 'scoring.ledger_since'

ACTIVE, ACTIVITY, RATE, COLLATERAL = (int(k) for k in EventKind)

# Swap lifecycle → the miner's busy machine on the swap's hub.
_FULFILL = {
    'SwapInitiated': ActivityTransition.FULFILL_START,
    'SwapCompleted': ActivityTransition.FULFILL_END,
    'SwapTimedOut': ActivityTransition.FULFILL_END,
    'SwapCancelled': ActivityTransition.FULFILL_END,
}


def _lower(fields: Any, key: str, default: Optional[str] = None) -> Optional[str]:
    value = fields.get(key) if hasattr(fields, 'get') else None
    return str(value).lower() if value else default


class Ledger:
    """Also the read interface the lifted crown functions take as both ``store`` and ``event_index``."""

    def __init__(self, store: Store) -> None:
        self.store = store
        store.declare(OWNER, VERSION, TABLES)

    def _rows(self, sql: str, params: Tuple = ()) -> List[Any]:
        return self.store.conn().execute(sql, params).fetchall()

    # ── writes ──────────────────────────────────────────────────────────────

    def put(
        self,
        at: int,
        hotkey: str,
        kind: int,
        value: float,
        from_chain: Optional[str] = None,
        to_chain: Optional[str] = None,
        backing: Optional[str] = None,
        hub: Optional[str] = None,
    ) -> None:
        if kind == RATE:  # a re-post of the same rate is no transition
            prev = self._rows(
                'SELECT value FROM scoring_events WHERE kind = ? AND hotkey = ? AND from_chain = ? AND to_chain = ? '
                'AND backing = ? ORDER BY at DESC, id DESC LIMIT 1',
                (RATE, hotkey, from_chain, to_chain, backing),
            )
            if prev and prev[0]['value'] == value:
                return
        self.store.conn().execute(
            'INSERT INTO scoring_events (at, hotkey, kind, value, from_chain, to_chain, backing, hub) '
            'VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
            (int(at), hotkey, kind, value, from_chain, to_chain, backing, hub),
        )

    def restamp_expiry(self, hotkey: str, hub: Optional[str], at: int, not_before: int) -> None:
        """Move the miner's latest reservation expiry on ``hub`` to the real deadline. A row that fired before
        ``not_before`` belongs to an earlier reservation and is left alone."""
        self.store.conn().execute(
            'UPDATE scoring_events SET at = ? WHERE id = (SELECT id FROM scoring_events WHERE kind = ? AND '
            'hotkey = ? AND value = ? AND hub IS ? AND at >= ? ORDER BY id DESC LIMIT 1)',
            (int(at), ACTIVITY, hotkey, int(ActivityTransition.RESERVE_EXPIRE), hub, int(not_before)),
        )

    def put_fill(self, swap_key: str, at: int, hotkey: str, lane: Tuple[str, str, str], legs, qualified: bool):
        from_amount, to_amount, collateral_amount = (str(int(x)) for x in legs)
        self.store.conn().execute(
            'INSERT OR IGNORE INTO scoring_fills VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            (swap_key, int(at), hotkey, *lane, from_amount, to_amount, collateral_amount, int(qualified)),
        )

    def ban(self, hotkey: str, at: int) -> None:
        self.store.conn().execute(
            'INSERT INTO scoring_bans VALUES (?, ?) ON CONFLICT(hotkey) DO UPDATE SET at = MAX(at, excluded.at)',
            (hotkey, int(at)),
        )

    def mark_gap(self, complete_since: int) -> None:
        """Fills before ``complete_since`` may be missing: the activity gate reads strikes-only until the
        ledger is a full window past it, instead of zeroing miners whose fill fell in the gap."""
        since = max(int(complete_since), self.clearing_ledger_since(complete_since))
        with self.store.tx() as c:
            Store.set_meta(c, LEDGER_SINCE, str(since))

    def prune(self, now: int) -> None:
        """Older than retention, except each key's latest row (its anchor) and the whole busy timeline of
        a miner still busy (a later close must not be orphaned)."""
        cutoff = now - EVENT_RETENTION_SECS
        busy = list(self.get_activity_state_at(now))
        with self.store.tx() as c:
            c.execute(
                'DELETE FROM scoring_events WHERE at < ? AND kind != ? AND id NOT IN (SELECT MAX(id) FROM '
                'scoring_events WHERE kind != ? GROUP BY kind, hotkey, from_chain, to_chain, backing)',
                (cutoff, ACTIVITY, ACTIVITY),
            )
            marks = ','.join('?' * len(busy))
            c.execute(
                f'DELETE FROM scoring_events WHERE at < ? AND kind = ? AND hotkey NOT IN ({marks})',
                (cutoff, ACTIVITY, *busy),
            )
            c.execute('DELETE FROM scoring_fills WHERE at < ?', (now - CLEARING_RETENTION_SECS,))

    # ── program events ──────────────────────────────────────────────────────

    def ingest(
        self,
        record: Any,
        hotkey: str,
        reservation_ttl: int,
        quote_usable: Optional[bool] = None,
        fill_held_crown: Optional[Callable[..., bool]] = None,
    ) -> bool:
        """Fold one event of a bound miner; False if it changes nothing scoring reads. ``quote_usable`` judges
        a QuoteSet's addresses (None = unjudged); ``fill_held_crown`` judges a fill for the quality slice."""
        name, f, at = record.name, record.fields, int(record.block_time)
        hub = _lower(f, 'collateral_chain')
        if name in ('MinerActivated', 'MinerDeactivated'):
            self.put(at, hotkey, ACTIVE, int(name == 'MinerActivated'))
        elif name == 'PoolResolved':
            # The real deadline isn't on this event: expire at draw + TTL until a fill or extend restamps it.
            if reservation_ttl <= 0:
                return False
            self.put(at, hotkey, ACTIVITY, int(ActivityTransition.RESERVE_START), hub=hub)
            self.put(at + reservation_ttl, hotkey, ACTIVITY, int(ActivityTransition.RESERVE_EXPIRE), hub=hub)
        elif name in ('ReservationFilled', 'ReservationExtended'):
            if int(f['reserved_until']) <= 0:
                return False
            self.restamp_expiry(hotkey, hub, int(f['reserved_until']), not_before=at)
        elif name == 'UnfilledReservationClosed':
            self.restamp_expiry(hotkey, hub, at, not_before=at)
        elif name in _FULFILL:
            self.put(at, hotkey, ACTIVITY, int(_FULFILL[name]), hub=hub)
            if name == 'SwapCompleted':
                self._fill(record, hotkey, at, fill_held_crown)
        elif name in ('CollateralPosted', 'CollateralWithdrawn'):
            self.put(at, hotkey, COLLATERAL, int(f['total']), backing='sol')
        elif name == 'BondAttested':
            balance = int(f['effective_balance']) if f['locked'] else 0  # an unlocked bond backs nothing
            self.put(at, hotkey, COLLATERAL, balance, backing=_lower(f, 'chain', 'sol'))
        elif name in ('QuoteSet', 'QuoteRemoved'):
            lane = (_lower(f, 'from_chain'), _lower(f, 'to_chain'), _lower(f, 'collateral_chain', 'sol'))
            rate = quantize_rate_fixed(int(f['rate'])) / RATE_PRECISION if name == 'QuoteSet' else 0.0
            if name == 'QuoteSet' and quote_usable is False:
                rate = 0.0  # it can never fill: no live rate from the instant it lands
                self.ban(hotkey, at)
                bt.logging.warning(f'{hotkey[:8]}: {lane} quote names an unusable address: rate zeroed, miner banned')
            self.put(at, hotkey, RATE, rate, *lane)
        elif name == 'MinerBackingChanged':
            if f['enabled']:
                return True
            # The purse dropped without naming a direction: zero every live quote under it.
            backing = _lower(f, 'backing', 'sol')
            for (hk, from_chain, to_chain, b), _ in self._live_rates(at).items():
                if hk == hotkey and b == backing:
                    self.put(at, hotkey, RATE, 0.0, from_chain, to_chain, backing)
        else:
            return False
        return True

    def _fill(self, record: Any, hotkey: str, at: int, fill_held_crown: Optional[Callable[..., bool]]) -> None:
        f = record.fields
        lane = (_lower(f, 'from_chain'), _lower(f, 'to_chain'), _lower(f, 'collateral_chain', 'sol'))
        collateral = int(f.get('collateral_amount', 0))
        reserved_at = self.get_last_reserve_start(hotkey, lane[2], at)
        qualified = False
        if fill_held_crown is not None and reserved_at is not None:
            try:
                qualified = bool(fill_held_crown(hotkey, *lane, collateral, reserved_at))
            except Exception as e:  # fail closed: an unjudged fill is unqualified
                bt.logging.warning(f'fill qualification failed for {hotkey[:8]} {lane}: {e}')
        legs = (f['from_amount'], f['to_amount'], collateral)
        self.put_fill(bytes(f['swap_key']).hex(), at, hotkey, lane, legs, qualified)

    # ── anchors from the World (called once per round by the lifted scorer) ──

    def reconcile_live_state(
        self, live_states: Dict[str, Any], now: int, live_bonds: Optional[Dict[str, int]] = None
    ) -> None:
        """Write the chain's active flag and purses where the ledger disagrees, unless a real event touched
        that key in the last ``RECONCILE_QUIET_SECS`` (a fresher event wins). Corrections apply from now."""
        quiet = self._touched(now)
        active = self.get_active_miners_at(now)
        purses = [('sol', {hk: int(ms.collateral) for hk, ms in live_states.items()})]
        if live_bonds is not None:
            purses.append(('tao', {hk: int(b) for hk, b in live_bonds.items()}))
        for hk, ms in live_states.items():
            if (ACTIVE, hk, None) not in quiet and bool(ms.active) != (hk in active):
                self.put(now, hk, ACTIVE, int(bool(ms.active)))
                bt.logging.warning(f'reconcile {hk[:8]}: active corrected to {bool(ms.active)}')
        for backing, live in purses:
            held = self.get_miner_collaterals_at(now, backing=backing)
            for hk, amount in live.items():
                if (COLLATERAL, hk, backing) not in quiet and held.get(hk) != amount:
                    self.put(now, hk, COLLATERAL, amount, backing=backing)
                    bt.logging.warning(f'reconcile {hk[:8]}: {backing} collateral corrected to {amount}')

    def reconcile_live_quotes(self, live_quotes: Dict[Tuple[str, str, str, str], float], now: int) -> None:
        """The quote book's anchor: a lane with no live quote is zeroed, a differing rate corrected, and a
        live lane the ledger never saw (quoted before its history) added. An empty book reads as a failed
        read and changes nothing."""
        if not live_quotes:
            bt.logging.warning('reconcile: empty quote book, skipping')
            return
        quiet = self._touched(now)
        held = self._live_rates(now)
        for lane in set(held) | {k for k, rate in live_quotes.items() if rate > 0}:
            hk, from_chain, to_chain, backing = lane
            live = live_quotes.get(lane, 0.0)
            if (RATE, hk, lane[1:]) not in quiet and held.get(lane, 0.0) != live:
                self.put(now, hk, RATE, live, from_chain, to_chain, backing)
                bt.logging.warning(f'reconcile {hk[:8]}: {lane[1:]} rate corrected to {live}')

    def _touched(self, now: int) -> Set[Tuple[int, str, Any]]:
        rows = self._rows(
            'SELECT DISTINCT kind, hotkey, from_chain, to_chain, backing FROM scoring_events WHERE at > ? AND at <= ?',
            (now - RECONCILE_QUIET_SECS, now),
        )
        out: Set[Tuple[int, str, Any]] = set()
        for r in rows:
            key = {ACTIVE: None, COLLATERAL: r['backing'], RATE: (r['from_chain'], r['to_chain'], r['backing'])}
            out.add((r['kind'], r['hotkey'], key.get(r['kind'])))
        return out

    def _live_rates(self, at: int) -> Dict[Tuple[str, str, str, str], float]:
        rows = self._latest(RATE, at, partition='hotkey, from_chain, to_chain, backing')
        return {(r['hotkey'], r['from_chain'], r['to_chain'], r['backing']): r['value'] for r in rows if r['value'] > 0}

    # ── reads ───────────────────────────────────────────────────────────────

    def _latest(self, kind: int, at: int, where: str = '', params: Tuple = (), partition: str = 'hotkey') -> List[Any]:
        return self._rows(
            f'SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY {partition} ORDER BY at DESC, id DESC) AS rn '
            f'FROM scoring_events WHERE kind = ? AND at <= ?{where}) WHERE rn = 1',
            (kind, int(at), *params),
        )

    def _range(self, kind: int, start: int, end: int, where: str = '', params: Tuple = ()) -> List[Any]:
        return self._rows(
            f'SELECT * FROM scoring_events WHERE kind = ? AND at > ? AND at <= ?{where} ORDER BY at, value, id',
            (kind, int(start), int(end), *params),
        )

    @staticmethod
    def _backing(backing: Optional[str], column: str = 'backing') -> Tuple[str, Tuple]:
        return (f' AND {column} = ?', (backing,)) if backing is not None else ('', ())

    def get_latest_rates_before(
        self, from_chain: str, to_chain: str, block: int, collateral_chain: Optional[str] = None
    ) -> Dict[str, Tuple[float, int]]:
        where, params = self._backing(collateral_chain)
        rows = self._latest(
            RATE, block, ' AND from_chain = ? AND to_chain = ?' + where, (from_chain, to_chain, *params)
        )
        return {r['hotkey']: (r['value'], r['at']) for r in rows}

    def get_rate_events_in_range(
        self, from_chain: str, to_chain: str, start: int, end: int, collateral_chain: Optional[str] = None
    ) -> List[dict]:
        where, params = self._backing(collateral_chain)
        rows = self._range(
            RATE, start, end, ' AND from_chain = ? AND to_chain = ?' + where, (from_chain, to_chain, *params)
        )
        return [{'hotkey': r['hotkey'], 'rate': r['value'], 'block': r['at']} for r in sorted(rows, key=_by_id)]

    def get_active_miners_at(self, at: int) -> Set[str]:
        return {r['hotkey'] for r in self._latest(ACTIVE, at) if r['value']}

    def get_active_events_in_range(self, start: int, end: int) -> List[dict]:
        rows = sorted(self._range(ACTIVE, start, end), key=_by_id)
        return [{'hotkey': r['hotkey'], 'active': bool(r['value']), 'block': r['at']} for r in rows]

    def get_activity_events_in_range(self, start: int, end: int) -> List[dict]:
        rows = self._range(ACTIVITY, start, end)  # same-instant edges in machine order (the kind)
        return [{'hotkey': r['hotkey'], 'kind': int(r['value']), 'block': r['at'], 'hub': r['hub']} for r in rows]

    def get_activity_state_at(self, at: int) -> Dict[str, Dict[str, MinerActivity]]:
        """Per (hotkey, hub) busy state at ``at``; AVAILABLE hubs are omitted. A hub-less edge steps every hub."""
        states: Dict[str, Dict[str, MinerActivity]] = {}
        for r in self._range(ACTIVITY, -1, at):
            per_hub = states.setdefault(r['hotkey'], {})
            for h in [r['hub']] if r['hub'] else list(BACKING_BITS):
                cur = per_hub.get(h, MinerActivity.AVAILABLE)
                nxt = next_activity(cur, ActivityTransition(int(r['value'])))
                per_hub[h] = cur if nxt is None else nxt
        return {
            hk: busy
            for hk, per_hub in states.items()
            if (busy := {h: s for h, s in per_hub.items() if s is not MinerActivity.AVAILABLE})
        }

    def get_miner_collaterals_at(self, at: int, backing: Optional[str] = None) -> Dict[str, int]:
        where, params = self._backing(backing)
        return {r['hotkey']: int(r['value']) for r in self._latest(COLLATERAL, at, where, params)}

    def get_collateral_events_in_range(self, start: int, end: int, backing: Optional[str] = None) -> List[dict]:
        where, params = self._backing(backing)
        rows = sorted(self._range(COLLATERAL, start, end, where, params), key=_by_id)
        return [{'hotkey': r['hotkey'], 'collateral_rao': int(r['value']), 'block': r['at']} for r in rows]

    def get_last_reserve_start(self, hotkey: str, hub: str, before: int) -> Optional[int]:
        rows = self._rows(
            'SELECT at FROM scoring_events WHERE kind = ? AND hotkey = ? AND value = ? AND at < ? '
            'AND (hub IS NULL OR hub = ?) ORDER BY at DESC, id DESC LIMIT 1',
            (ACTIVITY, hotkey, int(ActivityTransition.RESERVE_START), int(before), hub),
        )
        return int(rows[0]['at']) if rows else None

    def quoted_lanes(self) -> Set[Tuple[str, str, str]]:
        rows = self._rows('SELECT DISTINCT from_chain, to_chain, backing FROM scoring_events WHERE kind = ?', (RATE,))
        return {(r['from_chain'], r['to_chain'], r['backing']) for r in rows}

    def get_qualified_lane_volumes(
        self, start: int, end: int
    ) -> Dict[Tuple[str, str, str], Dict[str, Tuple[int, ...]]]:
        """Per lane and hotkey, summed (from, to, collateral) legs of the qualified fills in ``(start, end]``."""
        volumes: Dict[Tuple[str, str, str], Dict[str, Tuple[int, ...]]] = {}
        for r in self._rows('SELECT * FROM scoring_fills WHERE qualified = 1 AND at > ? AND at <= ?', (start, end)):
            lane = volumes.setdefault((r['from_chain'], r['to_chain'], r['backing']), {})
            prev = lane.get(r['hotkey'], (0, 0, 0))
            legs = (int(r['from_amount']), int(r['to_amount']), int(r['collateral_amount']))
            lane[r['hotkey']] = tuple(a + b for a, b in zip(prev, legs))
        return volumes

    def get_recent_fill_hotkeys(self, start: int, end: int) -> Dict[str, Set[str]]:
        out: Dict[str, Set[str]] = {}
        for r in self._rows(
            'SELECT DISTINCT hotkey, backing FROM scoring_fills WHERE at > ? AND at <= ?', (start, end)
        ):
            out.setdefault(r['backing'], set()).add(r['hotkey'])
        return out

    def address_banned_hotkeys(self, start: int, end: int) -> Set[str]:
        return {
            r['hotkey'] for r in self._rows('SELECT hotkey FROM scoring_bans WHERE at > ? AND at <= ?', (start, end))
        }

    def clearing_ledger_since(self, now: int) -> int:
        """When this ledger started recording fills: stamped on first ask."""
        stored = self.store.get_meta(LEDGER_SINCE)
        if stored is not None:
            return int(stored)
        with self.store.tx() as c:
            Store.set_meta(c, LEDGER_SINCE, str(int(now)))
        return int(now)


def _by_id(row: Any) -> Tuple[int, int]:
    return (row['at'], row['id'])
