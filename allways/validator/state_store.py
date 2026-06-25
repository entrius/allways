"""SQLite-backed store for all validator-local state.

Tables: ``rate_events`` + ``active_events`` + ``busy_events`` +
``collateral_events`` (the crown-time event series, sourced from Solana program
events via ``SolanaEventIndex`` and keyed by unix ``blockTime``),
``solana_event_meta`` (the event-ingest cursor), and ``reservation_pins`` (the
axon reserve path's commitment snapshot, kept until the Phase-9 repoint). Single
connection guarded by one lock; opened with ``check_same_thread=False``.
``busy_timeout`` is set before ``journal_mode=WAL`` because the WAL flip takes a
brief exclusive lock that concurrent openers would otherwise hit as "database is
locked" — the local dev env runs two validators against the same file.
"""

import sqlite3
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, Optional, Set, Tuple


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

    # ─── reservation_pins ───────────────────────────────────────────────

    def upsert_reservation_pin(self, pin: ReservationPin) -> None:
        """Persist (or overwrite) the commitment snapshot for a miner's
        reservation. Keyed on ``miner_hotkey`` — a miner has at most one live
        reservation, so a fresh ``MinerReserved`` replaces any stale pin."""
        self._execute(
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

    def get_reservation_pin(self, miner_hotkey: str) -> Optional[ReservationPin]:
        row = self._fetchone(
            'SELECT * FROM reservation_pins WHERE miner_hotkey = ?',
            (miner_hotkey,),
        )
        return self.row_to_reservation_pin(row) if row is not None else None

    def remove_reservation_pin(self, miner_hotkey: str) -> Optional[ReservationPin]:
        row = self._fetch_and_delete(
            'SELECT * FROM reservation_pins WHERE miner_hotkey = ?',
            'DELETE FROM reservation_pins WHERE miner_hotkey = ?',
            (miner_hotkey,),
        )
        return self.row_to_reservation_pin(row) if row is not None else None

    def get_expired_reservation_pins(self) -> List[ReservationPin]:
        """Pins whose reservation has lapsed as of the current block.

        Read before ``purge_expired_reservation_pins`` so the caller can emit a
        scoring pin-end event per expired pin — otherwise the crown overlay's
        'start' outlives the on-chain reservation and keeps earning crown at the
        pinned rate after expiry.
        """
        if self.current_block_fn is None:
            return []
        rows = self._fetchall(
            'SELECT * FROM reservation_pins WHERE reserved_until < ?',
            (self.current_block_fn(),),
        )
        return [self.row_to_reservation_pin(row) for row in rows]

    def purge_expired_reservation_pins(self) -> int:
        """Drop pins whose reservation has already expired."""
        if self.current_block_fn is None:
            return 0
        return self._execute_returning_rowcount(
            'DELETE FROM reservation_pins WHERE reserved_until < ?',
            (self.current_block_fn(),),
        )

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
        row = self._fetchone(
            """
            SELECT rate, block FROM rate_events
            WHERE hotkey = ? AND from_chain = ? AND to_chain = ? AND block <= ?
            ORDER BY block DESC, id DESC
            LIMIT 1
            """,
            (hotkey, from_chain, to_chain, block),
        )
        return (row['rate'], row['block']) if row is not None else None

    def get_latest_rates_before(
        self,
        from_chain: str,
        to_chain: str,
        block: int,
    ) -> Dict[str, Tuple[float, int]]:
        """Batched form of get_latest_rate_before — one query per direction
        instead of one per (hotkey, direction). Returns {hotkey: (rate, block)}
        for every hotkey that has at least one rate event in that direction
        at-or-before ``block``. Caller filters by membership in the
        rewardable set after.

        Ordering matches the single-row form: ``block DESC, id DESC`` so a
        same-block re-emit (id is monotonic) picks the latest write.
        """
        with self.lock:
            conn = self.require_connection()
            rows = conn.execute(
                """
                SELECT hotkey, rate, block FROM (
                    SELECT hotkey, rate, block,
                           ROW_NUMBER() OVER (
                               PARTITION BY hotkey
                               ORDER BY block DESC, id DESC
                           ) AS rn
                    FROM rate_events
                    WHERE from_chain = ? AND to_chain = ? AND block <= ?
                ) WHERE rn = 1
                """,
                (from_chain, to_chain, block),
            ).fetchall()
        return {r['hotkey']: (r['rate'], r['block']) for r in rows}

    def get_rate_events_in_range(
        self,
        from_chain: str,
        to_chain: str,
        start_block: int,
        end_block: int,
    ) -> List[dict]:
        """Rate events in ``(start_block, end_block]`` for a direction, oldest first."""
        rows = self._fetchall(
            """
            SELECT id, hotkey, rate, block FROM rate_events
            WHERE from_chain = ? AND to_chain = ? AND block > ? AND block <= ?
            ORDER BY block ASC, id ASC
            """,
            (from_chain, to_chain, start_block, end_block),
        )
        return [{'id': r['id'], 'hotkey': r['hotkey'], 'rate': r['rate'], 'block': r['block']} for r in rows]


    # ─── crown event tables (Solana-sourced via SolanaEventIndex) ───────

    def insert_active_event(self, block_num: int, hotkey: str, active: bool) -> None:
        self._execute(
            'INSERT INTO active_events (block_num, hotkey, active) VALUES (?, ?, ?)',
            (block_num, hotkey, 1 if active else 0),
        )

    def insert_busy_event(self, block_num: int, hotkey: str, delta: int, swap_id: Optional[int] = None) -> None:
        self._execute(
            'INSERT INTO busy_events (block_num, hotkey, delta, swap_id) VALUES (?, ?, ?, ?)',
            (block_num, hotkey, delta, swap_id),
        )

    def load_all_active_events(self) -> List[dict]:
        rows = self._fetchall('SELECT block_num, hotkey, active FROM active_events ORDER BY block_num ASC, id ASC')
        return [{'block_num': r['block_num'], 'hotkey': r['hotkey'], 'active': bool(r['active'])} for r in rows]

    def load_all_busy_events(self) -> List[dict]:
        rows = self._fetchall(
            'SELECT block_num, hotkey, delta, swap_id FROM busy_events ORDER BY block_num ASC, id ASC'
        )
        return [
            {'block_num': r['block_num'], 'hotkey': r['hotkey'], 'delta': r['delta'], 'swap_id': r['swap_id']}
            for r in rows
        ]

    def insert_collateral_event(self, block_num: int, hotkey: str, collateral_rao: int) -> None:
        self._execute(
            'INSERT INTO collateral_events (block_num, hotkey, collateral_rao) VALUES (?, ?, ?)',
            (block_num, hotkey, int(collateral_rao)),
        )

    def load_all_collateral_events(self) -> List[dict]:
        rows = self._fetchall(
            'SELECT block_num, hotkey, collateral_rao FROM collateral_events ORDER BY block_num ASC, id ASC'
        )
        return [
            {'block_num': r['block_num'], 'hotkey': r['hotkey'], 'collateral_rao': int(r['collateral_rao'])}
            for r in rows
        ]

    # ─── crown read interface (B3.4 SolanaEventIndex) ───────────────────
    #
    # At-time + in-range queries over the active/busy/collateral event tables,
    # the SQL twins of the rate_events readers above. ``block_num`` here is a
    # unix ``blockTime`` (seconds), not a substrate block — the Solana crown
    # axis. ``SolanaEventIndex`` wraps these into the read interface scoring's
    # crown replay consumes.

    def get_active_events_in_range(self, start_time: int, end_time: int) -> List[dict]:
        """Active-flag transitions in ``(start_time, end_time]``, oldest first."""
        rows = self._fetchall(
            """
            SELECT id, block_num, hotkey, active FROM active_events
            WHERE block_num > ? AND block_num <= ?
            ORDER BY block_num ASC, id ASC
            """,
            (start_time, end_time),
        )
        return [{'hotkey': r['hotkey'], 'active': bool(r['active']), 'block': r['block_num']} for r in rows]

    def get_active_state_at(self, at_time: int) -> Set[str]:
        """Active set at ``at_time`` — latest transition per hotkey at-or-before
        ``at_time``, keeping those whose latest flag is True."""
        rows = self._fetchall(
            """
            SELECT hotkey, active FROM (
                SELECT hotkey, active,
                       ROW_NUMBER() OVER (PARTITION BY hotkey ORDER BY block_num DESC, id DESC) AS rn
                FROM active_events WHERE block_num <= ?
            ) WHERE rn = 1
            """,
            (at_time,),
        )
        return {r['hotkey'] for r in rows if r['active']}

    def get_busy_events_in_range(self, start_time: int, end_time: int) -> List[dict]:
        """Busy ±1 deltas in ``(start_time, end_time]``, oldest first."""
        rows = self._fetchall(
            """
            SELECT id, block_num, hotkey, delta FROM busy_events
            WHERE block_num > ? AND block_num <= ?
            ORDER BY block_num ASC, id ASC
            """,
            (start_time, end_time),
        )
        return [{'hotkey': r['hotkey'], 'delta': r['delta'], 'block': r['block_num']} for r in rows]

    def get_busy_counts_at(self, at_time: int) -> Dict[str, int]:
        """Per-hotkey open-swap count at ``at_time`` (running sum of deltas),
        keeping only hotkeys still busy (sum > 0)."""
        rows = self._fetchall(
            """
            SELECT hotkey, SUM(delta) AS total FROM busy_events
            WHERE block_num <= ?
            GROUP BY hotkey HAVING total > 0
            """,
            (at_time,),
        )
        return {r['hotkey']: int(r['total']) for r in rows}

    def get_collateral_events_in_range(self, start_time: int, end_time: int) -> List[dict]:
        """Collateral transitions in ``(start_time, end_time]``, oldest first.
        ``collateral_rao`` is the post-event total."""
        rows = self._fetchall(
            """
            SELECT id, block_num, hotkey, collateral_rao FROM collateral_events
            WHERE block_num > ? AND block_num <= ?
            ORDER BY block_num ASC, id ASC
            """,
            (start_time, end_time),
        )
        return [
            {'hotkey': r['hotkey'], 'collateral_rao': int(r['collateral_rao']), 'block': r['block_num']}
            for r in rows
        ]

    def get_collaterals_at(self, at_time: int) -> Dict[str, int]:
        """Per-hotkey posted collateral at ``at_time`` — latest transition
        at-or-before ``at_time``. Hotkeys with no event are absent (caller
        treats as unknown, not zero)."""
        rows = self._fetchall(
            """
            SELECT hotkey, collateral_rao FROM (
                SELECT hotkey, collateral_rao,
                       ROW_NUMBER() OVER (PARTITION BY hotkey ORDER BY block_num DESC, id DESC) AS rn
                FROM collateral_events WHERE block_num <= ?
            ) WHERE rn = 1
            """,
            (at_time,),
        )
        return {r['hotkey']: int(r['collateral_rao']) for r in rows}

    def get_solana_event_cursor(self) -> Optional[str]:
        """Last ingested Solana tx signature (the SolanaEventIngest cursor).
        ``None`` on a fresh DB so the first poll starts from the prune horizon."""
        row = self._fetchone('SELECT value FROM solana_event_meta WHERE key = ?', ('cursor',))
        return row['value'] if row is not None else None

    def set_solana_event_cursor(self, signature: str) -> None:
        self._execute(
            """
            INSERT INTO solana_event_meta (key, value) VALUES ('cursor', ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (signature,),
        )

    def prune_active_events(self, cutoff_block: int) -> None:
        """Drop active events older than ``cutoff_block``, preserving the latest
        row per hotkey as a state-reconstruction anchor (mirrors the in-memory
        prune's anchor-preservation rule)."""
        if cutoff_block <= 0:
            return
        self._execute(
            """
            DELETE FROM active_events
            WHERE block_num < ?
              AND id NOT IN (SELECT MAX(id) FROM active_events GROUP BY hotkey)
            """,
            (cutoff_block,),
        )

    def prune_busy_events(self, cutoff_block: int) -> None:
        """Drop busy events older than ``cutoff_block`` except for hotkeys whose
        SUM(delta) > 0 — those still have an open swap, so we keep their full
        +1/-1 history so a future SwapCompleted's -1 isn't orphaned."""
        if cutoff_block <= 0:
            return
        self._execute(
            """
            DELETE FROM busy_events
            WHERE block_num < ?
              AND hotkey NOT IN (SELECT hotkey FROM busy_events GROUP BY hotkey HAVING SUM(delta) > 0)
            """,
            (cutoff_block,),
        )

    def prune_collateral_events(self, cutoff_block: int) -> None:
        """Drop collateral events older than ``cutoff_block``, preserving the
        latest row per hotkey as a reconstruction anchor (mirrors
        ``prune_active_events``)."""
        if cutoff_block <= 0:
            return
        self._execute(
            """
            DELETE FROM collateral_events
            WHERE block_num < ?
              AND id NOT IN (SELECT MAX(id) FROM collateral_events GROUP BY hotkey)
            """,
            (cutoff_block,),
        )


    # ─── cross-table maintenance ────────────────────────────────────────

    def delete_hotkey(self, hotkey: str) -> None:
        with self.lock:
            conn = self.require_connection()
            conn.execute('DELETE FROM rate_events WHERE hotkey = ?', (hotkey,))
            conn.execute('DELETE FROM reservation_pins WHERE miner_hotkey = ?', (hotkey,))
            conn.commit()

    def prune_events_older_than(self, cutoff_block: int) -> None:
        """Delete rate events older than ``cutoff_block``, preserving the
        latest row per ``(hotkey, from_chain, to_chain)`` as a state-
        reconstruction anchor for ``get_latest_rate_before(window_start)``."""
        self._execute(
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

    def close(self) -> None:
        with self.lock:
            if self.conn is not None:
                self.conn.close()
                self.conn = None

    def require_connection(self) -> sqlite3.Connection:
        if self.conn is None:
            raise RuntimeError('ValidatorStateStore is closed')
        return self.conn

    # ─── crud helpers ───────────────────────────────────────────────────
    # Single-statement boilerplate. Methods that hold the lock across
    # multiple statements (insert_rate_event, delete_hotkey) bypass these.

    def _execute(self, sql: str, params: Tuple = ()) -> None:
        """Single-statement write under lock with commit."""
        with self.lock:
            conn = self.require_connection()
            conn.execute(sql, params)
            conn.commit()

    def _execute_returning_rowcount(self, sql: str, params: Tuple = ()) -> int:
        """Single-statement write under lock; returns affected row count."""
        with self.lock:
            conn = self.require_connection()
            cursor = conn.execute(sql, params)
            conn.commit()
            return cursor.rowcount

    def _fetchone(self, sql: str, params: Tuple = ()) -> Optional[sqlite3.Row]:
        """Read a single row under lock. Caller is responsible for mapping
        the row to a domain type (often via a ``row_to_X`` helper)."""
        with self.lock:
            conn = self.require_connection()
            return conn.execute(sql, params).fetchone()

    def _fetchall(self, sql: str, params: Tuple = ()) -> List[sqlite3.Row]:
        """Read all matching rows under lock. Caller maps."""
        with self.lock:
            conn = self.require_connection()
            return conn.execute(sql, params).fetchall()

    def _fetch_and_delete(self, select_sql: str, delete_sql: str, params: Tuple) -> Optional[sqlite3.Row]:
        """Atomic snapshot-then-delete under a single lock acquisition.
        Returns the pre-delete row, or None if no row matched."""
        with self.lock:
            conn = self.require_connection()
            row = conn.execute(select_sql, params).fetchone()
            if row is None:
                return None
            conn.execute(delete_sql, params)
            conn.commit()
            return row

    def init_db(self) -> None:
        with self.lock:
            conn = self.require_connection()
            conn.executescript(
                """
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

                CREATE TABLE IF NOT EXISTS collateral_events (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    block_num       INTEGER NOT NULL,
                    hotkey          TEXT NOT NULL,
                    collateral_rao  INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_collateral_events_block
                    ON collateral_events(block_num);
                CREATE INDEX IF NOT EXISTS idx_collateral_events_hotkey
                    ON collateral_events(hotkey);

                CREATE TABLE IF NOT EXISTS solana_event_meta (
                    key     TEXT PRIMARY KEY,
                    value   TEXT NOT NULL
                );
                """
            )
            conn.commit()
