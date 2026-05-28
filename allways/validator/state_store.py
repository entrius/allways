"""SQLite-backed store for all validator-local state.

Tables: ``pending_confirms`` (axon→forward queue), ``rate_events`` (crown-time
input), ``swap_outcomes`` (credibility ledger), ``active_events`` +
``busy_events`` + ``event_watcher_meta`` + ``bootstrapped_swaps`` (event
watcher persistence — warm restarts hydrate from these instead of replaying
contract history). Single connection guarded by one lock; opened with
``check_same_thread=False``. ``busy_timeout`` is set before
``journal_mode=WAL`` because the WAL flip takes a brief exclusive lock that
concurrent openers would otherwise hit as "database is locked" — the local
dev env runs two validators against the same file.
"""

import sqlite3
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, Optional, Set, Tuple


@dataclass
class PendingConfirm:
    """All data needed to call ``vote_initiate`` once tx confirmations land."""

    miner_hotkey: str
    from_tx_hash: str
    from_chain: str
    to_chain: str
    from_address: str
    to_address: str
    tao_amount: int
    from_amount: int
    to_amount: int
    miner_from_address: str
    miner_to_address: str
    rate_str: str
    reserved_until: int
    # Block the source tx was included in (0 = unknown). Used as a block
    # hint when draining the queue — keeps verification O(1) even if the
    # fixed 150-block fallback scan would have missed the tx.
    from_tx_block: int = 0
    queued_at: float = field(default_factory=time.time)


@dataclass
class ReservationPin:
    """A snapshot of a miner's commitment as of the block its reservation was
    created. ``handle_swap_confirm`` resolves the swap's rate and addresses
    from this pin instead of the live commitment, so a miner moving its rate
    or deposit address after the user reserves cannot shortchange or rob the
    user.

    Stores the full commitment — ``MinerReserved`` does not reveal the swap
    direction, so direction is resolved later from the requested chains.
    """

    miner_hotkey: str
    reserve_block: int
    from_chain: str
    to_chain: str
    rate_str: str
    counter_rate_str: str
    miner_from_address: str
    miner_to_address: str
    reserved_until: int
    created_at: float = field(default_factory=time.time)


@dataclass
class ReservationPinEvent:
    """One transition in the per-direction reservation-pin lifecycle, used by
    the crown-time replay to freeze a miner's crown rate at the value pinned
    when the reservation was created.

    ``kind = 'start'`` carries the pinned rate (``rate`` field, canonical
    units: TAO per BTC in the reservation's stated direction). ``kind = 'end'``
    carries ``rate = 0`` and clears any active pin for that hotkey + direction.
    The pin's lifetime spans (start block, end block]; the credit_interval
    walker uses these events to overlay the pinned rate during the reserved-
    not-busy window. Stored separately from ``rate_events`` so the live rate
    series is unchanged by reservation lifecycle.
    """

    block_num: int
    hotkey: str
    from_chain: str
    to_chain: str
    kind: str  # 'start' or 'end'
    rate: float


class ValidatorStateStore:
    def __init__(
        self,
        db_path: Path | str | None = None,
        current_block_fn: Optional[Callable[[], int]] = None,
    ):
        self.db_path = Path(db_path or Path.home() / '.allways' / 'validator' / 'state.db')
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.Lock()
        self.conn: Optional[sqlite3.Connection] = sqlite3.connect(self.db_path, check_same_thread=False)
        # busy_timeout must be set before journal_mode: the WAL switch takes a
        # brief exclusive lock that a concurrent opener would otherwise hit as
        # an immediate "database is locked" error.
        self.conn.execute('PRAGMA busy_timeout=5000')
        self.conn.execute('PRAGMA journal_mode=WAL')
        self.conn.row_factory = sqlite3.Row
        self.current_block_fn = current_block_fn
        self.init_db()

    # ─── pending_confirms ───────────────────────────────────────────────

    def enqueue(self, item: PendingConfirm) -> None:
        with self.lock:
            conn = self.require_connection()
            conn.execute(
                """
                INSERT OR REPLACE INTO pending_confirms (
                    miner_hotkey, from_tx_hash, from_chain, to_chain,
                    from_address, to_address, tao_amount, from_amount,
                    to_amount, miner_from_address, miner_to_address,
                    rate_str, reserved_until, from_tx_block, queued_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    item.miner_hotkey,
                    item.from_tx_hash,
                    item.from_chain,
                    item.to_chain,
                    item.from_address,
                    item.to_address,
                    item.tao_amount,
                    item.from_amount,
                    item.to_amount,
                    item.miner_from_address,
                    item.miner_to_address,
                    item.rate_str,
                    item.reserved_until,
                    item.from_tx_block,
                    item.queued_at,
                ),
            )
            conn.commit()

    def get_all(self) -> List[PendingConfirm]:
        """Snapshot of pending items, oldest first. Does not purge expired
        entries — call ``purge_expired_pending_confirms`` explicitly."""
        with self.lock:
            conn = self.require_connection()
            rows = conn.execute('SELECT * FROM pending_confirms ORDER BY queued_at').fetchall()
        return [self.row_to_pending(row) for row in rows]

    def remove(self, miner_hotkey: str) -> Optional[PendingConfirm]:
        with self.lock:
            conn = self.require_connection()
            row = conn.execute(
                'SELECT * FROM pending_confirms WHERE miner_hotkey = ?',
                (miner_hotkey,),
            ).fetchone()
            if row is None:
                return None
            conn.execute('DELETE FROM pending_confirms WHERE miner_hotkey = ?', (miner_hotkey,))
            conn.commit()
        return self.row_to_pending(row)

    def update_reserved_until(self, miner_hotkey: str, reserved_until: int) -> None:
        """Refresh the cached reserved_until on an existing pending_confirms row.

        Called after the contract's reservation has been extended on-chain — without
        this, the row's stale value causes ``purge_expired_pending_confirms`` to
        delete a still-live entry the moment the original TTL elapses.
        """
        with self.lock:
            conn = self.require_connection()
            conn.execute(
                'UPDATE pending_confirms SET reserved_until = ? WHERE miner_hotkey = ?',
                (reserved_until, miner_hotkey),
            )
            conn.commit()

    def has(self, miner_hotkey: str) -> bool:
        with self.lock:
            conn = self.require_connection()
            row = conn.execute(
                'SELECT 1 FROM pending_confirms WHERE miner_hotkey = ? LIMIT 1',
                (miner_hotkey,),
            ).fetchone()
        return row is not None

    def pending_size(self) -> int:
        with self.lock:
            conn = self.require_connection()
            count = conn.execute('SELECT COUNT(*) FROM pending_confirms').fetchone()[0]
            return int(count)

    def purge_expired_pending_confirms(self) -> int:
        """Drop pending confirms whose reservation has already expired."""
        if self.current_block_fn is None:
            return 0
        current_block = self.current_block_fn()
        with self.lock:
            conn = self.require_connection()
            cursor = conn.execute('DELETE FROM pending_confirms WHERE reserved_until < ?', (current_block,))
            conn.commit()
            return cursor.rowcount

    @staticmethod
    def row_to_pending(row: sqlite3.Row) -> PendingConfirm:
        # ``from_tx_block`` is a newer column — rows persisted by older code
        # won't have it, so fall back to 0 when the column is missing.
        try:
            from_tx_block = row['from_tx_block']
        except (KeyError, IndexError):
            from_tx_block = 0
        return PendingConfirm(
            miner_hotkey=row['miner_hotkey'],
            from_tx_hash=row['from_tx_hash'],
            from_chain=row['from_chain'],
            to_chain=row['to_chain'],
            from_address=row['from_address'],
            to_address=row['to_address'],
            tao_amount=row['tao_amount'],
            from_amount=row['from_amount'],
            to_amount=row['to_amount'],
            miner_from_address=row['miner_from_address'],
            miner_to_address=row['miner_to_address'],
            rate_str=row['rate_str'],
            reserved_until=row['reserved_until'],
            from_tx_block=int(from_tx_block or 0),
            queued_at=row['queued_at'],
        )

    # ─── reservation_pins ───────────────────────────────────────────────

    def upsert_reservation_pin(self, pin: ReservationPin) -> None:
        """Persist (or overwrite) the commitment snapshot for a miner's
        reservation. Keyed on ``miner_hotkey`` — a miner has at most one live
        reservation, so a fresh ``MinerReserved`` replaces any stale pin."""
        with self.lock:
            conn = self.require_connection()
            conn.execute(
                """
                INSERT OR REPLACE INTO reservation_pins (
                    miner_hotkey, reserve_block, from_chain, to_chain,
                    rate_str, counter_rate_str, miner_from_address,
                    miner_to_address, reserved_until, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    pin.miner_hotkey,
                    pin.reserve_block,
                    pin.from_chain,
                    pin.to_chain,
                    pin.rate_str,
                    pin.counter_rate_str,
                    pin.miner_from_address,
                    pin.miner_to_address,
                    pin.reserved_until,
                    pin.created_at,
                ),
            )
            conn.commit()

    def get_reservation_pin(self, miner_hotkey: str) -> Optional[ReservationPin]:
        with self.lock:
            conn = self.require_connection()
            row = conn.execute(
                'SELECT * FROM reservation_pins WHERE miner_hotkey = ?',
                (miner_hotkey,),
            ).fetchone()
        if row is None:
            return None
        return self.row_to_reservation_pin(row)

    def remove_reservation_pin(self, miner_hotkey: str) -> Optional[ReservationPin]:
        with self.lock:
            conn = self.require_connection()
            row = conn.execute(
                'SELECT * FROM reservation_pins WHERE miner_hotkey = ?',
                (miner_hotkey,),
            ).fetchone()
            if row is None:
                return None
            conn.execute('DELETE FROM reservation_pins WHERE miner_hotkey = ?', (miner_hotkey,))
            conn.commit()
        return self.row_to_reservation_pin(row)

    def update_reservation_pin_reserved_until(self, miner_hotkey: str, reserved_until: int) -> None:
        """Refresh the cached reserved_until on an existing pin row.

        Mirrors ``update_reserved_until`` — called after the contract extends
        the reservation, so ``purge_expired_reservation_pins`` doesn't drop a
        still-live pin at its stale TTL.
        """
        with self.lock:
            conn = self.require_connection()
            conn.execute(
                'UPDATE reservation_pins SET reserved_until = ? WHERE miner_hotkey = ?',
                (reserved_until, miner_hotkey),
            )
            conn.commit()

    def purge_expired_reservation_pins(self) -> int:
        """Drop pins whose reservation has already expired."""
        if self.current_block_fn is None:
            return 0
        current_block = self.current_block_fn()
        with self.lock:
            conn = self.require_connection()
            cursor = conn.execute('DELETE FROM reservation_pins WHERE reserved_until < ?', (current_block,))
            conn.commit()
            return cursor.rowcount

    @staticmethod
    def row_to_reservation_pin(row: sqlite3.Row) -> ReservationPin:
        return ReservationPin(
            miner_hotkey=row['miner_hotkey'],
            reserve_block=row['reserve_block'],
            from_chain=row['from_chain'],
            to_chain=row['to_chain'],
            rate_str=row['rate_str'],
            counter_rate_str=row['counter_rate_str'],
            miner_from_address=row['miner_from_address'],
            miner_to_address=row['miner_to_address'],
            reserved_until=row['reserved_until'],
            created_at=row['created_at'],
        )

    # ─── reservation_pin_events ─────────────────────────────────────────
    #
    # Direction-keyed history of reservation pin start/end transitions.
    # Used by ``replay_crown_time_window`` to overlay the pinned rate
    # during the reserved-not-busy window, closing the bump-after-pin
    # loophole where a miner pinned at a moderate rate could bump live
    # rate to absurd and earn crown at the inflated value.

    def insert_reservation_pin_event(
        self,
        block_num: int,
        hotkey: str,
        from_chain: str,
        to_chain: str,
        kind: str,
        rate: float,
    ) -> None:
        with self.lock:
            conn = self.require_connection()
            conn.execute(
                """
                INSERT INTO reservation_pin_events
                    (block_num, hotkey, from_chain, to_chain, kind, rate)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (block_num, hotkey, (from_chain or '').lower(), (to_chain or '').lower(), kind, float(rate)),
            )
            conn.commit()

    def load_all_reservation_pin_events(self) -> List[dict]:
        with self.lock:
            conn = self.require_connection()
            rows = conn.execute(
                """
                SELECT block_num, hotkey, from_chain, to_chain, kind, rate
                FROM reservation_pin_events
                ORDER BY block_num ASC, id ASC
                """
            ).fetchall()
        return [
            {
                'block_num': r['block_num'],
                'hotkey': r['hotkey'],
                'from_chain': r['from_chain'],
                'to_chain': r['to_chain'],
                'kind': r['kind'],
                'rate': r['rate'],
            }
            for r in rows
        ]

    def get_reservation_pin_events_in_range(
        self,
        from_chain: str,
        to_chain: str,
        start_block: int,
        end_block: int,
    ) -> List[dict]:
        """Pin lifecycle events in ``(start_block, end_block]`` for a direction,
        oldest first."""
        with self.lock:
            conn = self.require_connection()
            rows = conn.execute(
                """
                SELECT id, block_num, hotkey, kind, rate
                FROM reservation_pin_events
                WHERE from_chain = ? AND to_chain = ?
                  AND block_num > ? AND block_num <= ?
                ORDER BY block_num ASC, id ASC
                """,
                ((from_chain or '').lower(), (to_chain or '').lower(), start_block, end_block),
            ).fetchall()
        return [
            {
                'id': r['id'],
                'block': r['block_num'],
                'hotkey': r['hotkey'],
                'kind': r['kind'],
                'rate': r['rate'],
            }
            for r in rows
        ]

    def prune_reservation_pin_events(self, cutoff_block: int) -> None:
        """Drop pin events older than ``cutoff_block``, preserving each
        (hotkey, from_chain, to_chain) tuple's most recent event so a still-
        open pin retains its 'start' anchor for state reconstruction. Mirrors
        the anchor-preservation rule used by ``prune_active_events``.
        """
        if cutoff_block <= 0:
            return
        with self.lock:
            conn = self.require_connection()
            conn.execute(
                """
                DELETE FROM reservation_pin_events
                WHERE block_num < ?
                  AND id NOT IN (
                    SELECT MAX(id) FROM reservation_pin_events
                    GROUP BY hotkey, from_chain, to_chain
                  )
                """,
                (cutoff_block,),
            )
            conn.commit()

    # ─── rate_events ────────────────────────────────────────────────────

    def insert_rate_event(
        self,
        hotkey: str,
        from_chain: str,
        to_chain: str,
        rate: float,
        block: int,
    ) -> bool:
        """Insert a rate event, skipping same-rate duplicates."""
        with self.lock:
            conn = self.require_connection()
            row = conn.execute(
                """
                SELECT rate FROM rate_events
                WHERE hotkey = ? AND from_chain = ? AND to_chain = ?
                ORDER BY block DESC, id DESC
                LIMIT 1
                """,
                (hotkey, from_chain, to_chain),
            ).fetchone()
            if row is not None and row['rate'] == rate:
                return False
            conn.execute(
                'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
                (hotkey, from_chain, to_chain, rate, block),
            )
            conn.commit()
            return True

    def get_latest_rate_before(
        self,
        hotkey: str,
        from_chain: str,
        to_chain: str,
        block: int,
    ) -> Optional[Tuple[float, int]]:
        with self.lock:
            conn = self.require_connection()
            row = conn.execute(
                """
                SELECT rate, block FROM rate_events
                WHERE hotkey = ? AND from_chain = ? AND to_chain = ? AND block <= ?
                ORDER BY block DESC, id DESC
                LIMIT 1
                """,
                (hotkey, from_chain, to_chain, block),
            ).fetchone()
        if row is None:
            return None
        return row['rate'], row['block']

    def get_rate_events_in_range(
        self,
        from_chain: str,
        to_chain: str,
        start_block: int,
        end_block: int,
    ) -> List[dict]:
        """Rate events in ``(start_block, end_block]`` for a direction, oldest first."""
        with self.lock:
            conn = self.require_connection()
            rows = conn.execute(
                """
                SELECT id, hotkey, rate, block FROM rate_events
                WHERE from_chain = ? AND to_chain = ? AND block > ? AND block <= ?
                ORDER BY block ASC, id ASC
                """,
                (from_chain, to_chain, start_block, end_block),
            ).fetchall()
        return [{'id': r['id'], 'hotkey': r['hotkey'], 'rate': r['rate'], 'block': r['block']} for r in rows]

    # ─── swap_outcomes ──────────────────────────────────────────────────

    def insert_swap_outcome(
        self,
        swap_id: int,
        miner_hotkey: str,
        completed: bool,
        resolved_block: int,
        tao_amount: int = 0,
        from_chain: str = '',
        to_chain: str = '',
    ) -> None:
        # Direction is normalized to lowercase on write so the per-direction
        # volume query is robust to upstream case drift. SQLite text
        # comparisons are case-sensitive and DIRECTION_POOLS keys are
        # lowercase.
        with self.lock:
            conn = self.require_connection()
            conn.execute(
                """
                INSERT OR REPLACE INTO swap_outcomes
                    (swap_id, miner_hotkey, completed, resolved_block, tao_amount, from_chain, to_chain)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    swap_id,
                    miner_hotkey,
                    1 if completed else 0,
                    resolved_block,
                    int(tao_amount or 0),
                    (from_chain or '').lower(),
                    (to_chain or '').lower(),
                ),
            )
            conn.commit()

    def get_success_rates_since(self, since_block: int) -> Dict[str, Tuple[int, int]]:
        """Return ``{hotkey: (completed_count, timed_out_count)}`` for outcomes
        resolved at or after ``since_block``."""
        with self.lock:
            conn = self.require_connection()
            rows = conn.execute(
                """
                SELECT miner_hotkey,
                       SUM(completed) AS completed,
                       SUM(1 - completed) AS timed_out
                FROM swap_outcomes
                WHERE resolved_block >= ?
                GROUP BY miner_hotkey
                """,
                (since_block,),
            ).fetchall()
        return {r['miner_hotkey']: (int(r['completed']), int(r['timed_out'])) for r in rows}

    def get_volume_since(self, since_block: int) -> Dict[str, int]:
        """Sum ``tao_amount`` of completed swaps per miner (rao) since
        ``since_block``. Timed-out swaps don't count toward volume.

        Aggregates across directions — kept for callers that don't care about
        direction breakdown. Volume-weighted scoring uses
        ``get_volume_by_direction_since`` so a miner serving one direction
        isn't diluted by network volume on the other direction."""
        with self.lock:
            conn = self.require_connection()
            rows = conn.execute(
                """
                SELECT miner_hotkey, SUM(tao_amount) AS total
                FROM swap_outcomes
                WHERE resolved_block >= ? AND completed = 1
                GROUP BY miner_hotkey
                """,
                (since_block,),
            ).fetchall()
        return {r['miner_hotkey']: int(r['total'] or 0) for r in rows}

    def get_volume_by_direction_since(self, since_block: int, from_chain: str, to_chain: str) -> Dict[str, int]:
        """Per-miner volume (rao) restricted to one swap direction. Outcomes
        missing direction (pre-migration legacy rows) are excluded — they
        contribute no volume credit, same as legacy rows with tao_amount=0.

        Lookup is lowercased to match the normalization applied in
        ``insert_swap_outcome``."""
        with self.lock:
            conn = self.require_connection()
            rows = conn.execute(
                """
                SELECT miner_hotkey, SUM(tao_amount) AS total
                FROM swap_outcomes
                WHERE resolved_block >= ?
                  AND completed = 1
                  AND from_chain = ?
                  AND to_chain = ?
                GROUP BY miner_hotkey
                """,
                (since_block, (from_chain or '').lower(), (to_chain or '').lower()),
            ).fetchall()
        return {r['miner_hotkey']: int(r['total'] or 0) for r in rows}

    def prune_swap_outcomes_older_than(self, cutoff_block: int) -> None:
        if cutoff_block <= 0:
            return
        with self.lock:
            conn = self.require_connection()
            conn.execute('DELETE FROM swap_outcomes WHERE resolved_block < ?', (cutoff_block,))
            conn.commit()

    # ─── event_watcher state ────────────────────────────────────────────

    def insert_active_event(self, block_num: int, hotkey: str, active: bool) -> None:
        with self.lock:
            conn = self.require_connection()
            conn.execute(
                'INSERT INTO active_events (block_num, hotkey, active) VALUES (?, ?, ?)',
                (block_num, hotkey, 1 if active else 0),
            )
            conn.commit()

    def insert_busy_event(self, block_num: int, hotkey: str, delta: int, swap_id: Optional[int] = None) -> None:
        with self.lock:
            conn = self.require_connection()
            conn.execute(
                'INSERT INTO busy_events (block_num, hotkey, delta, swap_id) VALUES (?, ?, ?, ?)',
                (block_num, hotkey, delta, swap_id),
            )
            conn.commit()

    def load_all_active_events(self) -> List[dict]:
        with self.lock:
            conn = self.require_connection()
            rows = conn.execute(
                'SELECT block_num, hotkey, active FROM active_events ORDER BY block_num ASC, id ASC'
            ).fetchall()
        return [{'block_num': r['block_num'], 'hotkey': r['hotkey'], 'active': bool(r['active'])} for r in rows]

    def load_all_busy_events(self) -> List[dict]:
        with self.lock:
            conn = self.require_connection()
            rows = conn.execute(
                'SELECT block_num, hotkey, delta, swap_id FROM busy_events ORDER BY block_num ASC, id ASC'
            ).fetchall()
        return [
            {'block_num': r['block_num'], 'hotkey': r['hotkey'], 'delta': r['delta'], 'swap_id': r['swap_id']}
            for r in rows
        ]

    def get_event_cursor(self) -> Optional[int]:
        with self.lock:
            conn = self.require_connection()
            row = conn.execute('SELECT value FROM event_watcher_meta WHERE key = ?', ('cursor',)).fetchone()
        return int(row['value']) if row is not None else None

    def set_event_cursor(self, block_num: int) -> None:
        with self.lock:
            conn = self.require_connection()
            conn.execute(
                """
                INSERT INTO event_watcher_meta (key, value) VALUES ('cursor', ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (block_num,),
            )
            conn.commit()

    def add_bootstrapped_swap(self, swap_id: int) -> None:
        with self.lock:
            conn = self.require_connection()
            conn.execute('INSERT OR IGNORE INTO bootstrapped_swaps (swap_id) VALUES (?)', (swap_id,))
            conn.commit()

    def remove_bootstrapped_swap(self, swap_id: int) -> None:
        with self.lock:
            conn = self.require_connection()
            conn.execute('DELETE FROM bootstrapped_swaps WHERE swap_id = ?', (swap_id,))
            conn.commit()

    def load_bootstrapped_swaps(self) -> Set[int]:
        with self.lock:
            conn = self.require_connection()
            rows = conn.execute('SELECT swap_id FROM bootstrapped_swaps').fetchall()
        return {int(r['swap_id']) for r in rows}

    def prune_active_events(self, cutoff_block: int) -> None:
        """Drop active events older than ``cutoff_block``, preserving the latest
        row per hotkey as a state-reconstruction anchor (mirrors the in-memory
        prune's anchor-preservation rule)."""
        if cutoff_block <= 0:
            return
        with self.lock:
            conn = self.require_connection()
            conn.execute(
                """
                DELETE FROM active_events
                WHERE block_num < ?
                  AND id NOT IN (SELECT MAX(id) FROM active_events GROUP BY hotkey)
                """,
                (cutoff_block,),
            )
            conn.commit()

    def prune_busy_events(self, cutoff_block: int) -> None:
        """Drop busy events older than ``cutoff_block`` except for hotkeys whose
        SUM(delta) > 0 — those still have an open swap, so we keep their full
        +1/-1 history so a future SwapCompleted's -1 isn't orphaned."""
        if cutoff_block <= 0:
            return
        with self.lock:
            conn = self.require_connection()
            conn.execute(
                """
                DELETE FROM busy_events
                WHERE block_num < ?
                  AND hotkey NOT IN (SELECT hotkey FROM busy_events GROUP BY hotkey HAVING SUM(delta) > 0)
                """,
                (cutoff_block,),
            )
            conn.commit()

    def reset_event_watcher_state(self) -> None:
        """Wipe all event-watcher persistence. Used when the cursor is more than
        a scoring window behind current — the chain has moved past replayable
        history so we fall back to cold bootstrap from the contract."""
        with self.lock:
            conn = self.require_connection()
            conn.execute('DELETE FROM active_events')
            conn.execute('DELETE FROM busy_events')
            conn.execute('DELETE FROM reservation_pin_events')
            conn.execute("DELETE FROM event_watcher_meta WHERE key = 'cursor'")
            conn.execute('DELETE FROM bootstrapped_swaps')
            conn.commit()

    # ─── cross-table maintenance ────────────────────────────────────────

    def delete_hotkey(self, hotkey: str) -> None:
        with self.lock:
            conn = self.require_connection()
            conn.execute('DELETE FROM rate_events WHERE hotkey = ?', (hotkey,))
            conn.execute('DELETE FROM swap_outcomes WHERE miner_hotkey = ?', (hotkey,))
            conn.execute('DELETE FROM reservation_pins WHERE miner_hotkey = ?', (hotkey,))
            conn.execute('DELETE FROM reservation_pin_events WHERE hotkey = ?', (hotkey,))
            conn.commit()

    def prune_events_older_than(self, cutoff_block: int) -> None:
        """Delete rate events older than ``cutoff_block``, preserving the
        latest row per ``(hotkey, from_chain, to_chain)`` as a state-
        reconstruction anchor for ``get_latest_rate_before(window_start)``."""
        with self.lock:
            conn = self.require_connection()
            conn.execute(
                """
                DELETE FROM rate_events
                WHERE block < ?
                  AND id NOT IN (
                      SELECT MAX(id) FROM rate_events
                      GROUP BY hotkey, from_chain, to_chain
                  )
                """,
                (cutoff_block,),
            )
            conn.commit()

    def close(self) -> None:
        with self.lock:
            if self.conn is not None:
                self.conn.close()
                self.conn = None

    def require_connection(self) -> sqlite3.Connection:
        if self.conn is None:
            raise RuntimeError('ValidatorStateStore is closed')
        return self.conn

    def init_db(self) -> None:
        with self.lock:
            conn = self.require_connection()
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS pending_confirms (
                    miner_hotkey          TEXT PRIMARY KEY,
                    from_tx_hash        TEXT NOT NULL,
                    from_chain          TEXT NOT NULL,
                    to_chain            TEXT NOT NULL,
                    from_address        TEXT NOT NULL,
                    to_address          TEXT NOT NULL,
                    tao_amount            INTEGER NOT NULL,
                    from_amount         INTEGER NOT NULL,
                    to_amount           INTEGER NOT NULL,
                    miner_from_address TEXT NOT NULL,
                    miner_to_address    TEXT NOT NULL,
                    rate_str              TEXT NOT NULL,
                    reserved_until        INTEGER NOT NULL,
                    from_tx_block         INTEGER NOT NULL DEFAULT 0,
                    queued_at             REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS rate_events (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    hotkey      TEXT NOT NULL,
                    from_chain  TEXT NOT NULL,
                    to_chain    TEXT NOT NULL,
                    rate        REAL NOT NULL,
                    block       INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_rate_events_block
                    ON rate_events(block);
                CREATE INDEX IF NOT EXISTS idx_rate_events_dir_block
                    ON rate_events(from_chain, to_chain, block);
                CREATE INDEX IF NOT EXISTS idx_rate_events_hotkey
                    ON rate_events(hotkey);

                CREATE TABLE IF NOT EXISTS swap_outcomes (
                    swap_id         INTEGER PRIMARY KEY,
                    miner_hotkey    TEXT NOT NULL,
                    completed       INTEGER NOT NULL,
                    resolved_block  INTEGER NOT NULL,
                    tao_amount      INTEGER NOT NULL DEFAULT 0,
                    from_chain      TEXT NOT NULL DEFAULT '',
                    to_chain        TEXT NOT NULL DEFAULT ''
                );
                CREATE INDEX IF NOT EXISTS idx_swap_outcomes_hotkey
                    ON swap_outcomes(miner_hotkey);
                CREATE INDEX IF NOT EXISTS idx_swap_outcomes_resolved_block
                    ON swap_outcomes(resolved_block);

                CREATE TABLE IF NOT EXISTS reservation_pins (
                    miner_hotkey        TEXT PRIMARY KEY,
                    reserve_block       INTEGER NOT NULL,
                    from_chain          TEXT NOT NULL,
                    to_chain            TEXT NOT NULL,
                    rate_str            TEXT NOT NULL,
                    counter_rate_str    TEXT NOT NULL,
                    miner_from_address  TEXT NOT NULL,
                    miner_to_address    TEXT NOT NULL,
                    reserved_until      INTEGER NOT NULL,
                    created_at          REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_reservation_pins_reserved_until
                    ON reservation_pins(reserved_until);

                CREATE TABLE IF NOT EXISTS active_events (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    block_num   INTEGER NOT NULL,
                    hotkey      TEXT NOT NULL,
                    active      INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_active_events_block
                    ON active_events(block_num);
                CREATE INDEX IF NOT EXISTS idx_active_events_hotkey
                    ON active_events(hotkey);

                CREATE TABLE IF NOT EXISTS busy_events (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    block_num   INTEGER NOT NULL,
                    hotkey      TEXT NOT NULL,
                    delta       INTEGER NOT NULL,
                    swap_id     INTEGER
                );
                CREATE INDEX IF NOT EXISTS idx_busy_events_block
                    ON busy_events(block_num);
                CREATE INDEX IF NOT EXISTS idx_busy_events_hotkey
                    ON busy_events(hotkey);

                CREATE TABLE IF NOT EXISTS reservation_pin_events (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    block_num   INTEGER NOT NULL,
                    hotkey      TEXT NOT NULL,
                    from_chain  TEXT NOT NULL,
                    to_chain    TEXT NOT NULL,
                    kind        TEXT NOT NULL,
                    rate        REAL NOT NULL DEFAULT 0
                );
                CREATE INDEX IF NOT EXISTS idx_reservation_pin_events_block
                    ON reservation_pin_events(block_num);
                CREATE INDEX IF NOT EXISTS idx_reservation_pin_events_hotkey
                    ON reservation_pin_events(hotkey);
                CREATE INDEX IF NOT EXISTS idx_reservation_pin_events_dir_block
                    ON reservation_pin_events(from_chain, to_chain, block_num);

                CREATE TABLE IF NOT EXISTS event_watcher_meta (
                    key     TEXT PRIMARY KEY,
                    value   INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS bootstrapped_swaps (
                    swap_id INTEGER PRIMARY KEY
                );
                """
            )
            # Ensure newer columns exist on DBs created by older validator
            # versions. SQLite has no ``ADD COLUMN IF NOT EXISTS`` (<3.35), and
            # the PRAGMA-then-ALTER pattern races when two validators share the
            # same DB file: both read "column missing" and both try to add it.
            # Catching the duplicate-column error is the simplest correct form.
            for table, column, ddl in (
                ('pending_confirms', 'from_tx_block', 'INTEGER NOT NULL DEFAULT 0'),
                ('swap_outcomes', 'tao_amount', 'INTEGER NOT NULL DEFAULT 0'),
                ('swap_outcomes', 'from_chain', "TEXT NOT NULL DEFAULT ''"),
                ('swap_outcomes', 'to_chain', "TEXT NOT NULL DEFAULT ''"),
            ):
                try:
                    conn.execute(f'ALTER TABLE {table} ADD COLUMN {column} {ddl}')
                except sqlite3.OperationalError as e:
                    if 'duplicate column' not in str(e).lower():
                        raise
            conn.commit()
