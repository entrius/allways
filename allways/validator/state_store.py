"""SQLite-backed store for all validator-local state.

Tables: ``rate_events`` + ``active_events`` + ``activity_events`` +
``collateral_events`` (the crown-time event series, sourced from Solana program
events via ``SolanaEventIndex`` and keyed by unix ``blockTime``),
``clearing_rates`` (per-swap realized legs from ``SwapCompleted``, backing the
windowed volume read), ``swap_outcomes`` (terminal completed/timed_out
truth per swap_key, backing the seam's stage disambiguation after the swap PDA
closes), ``routed_requests`` (queued on-behalf reservation details awaiting
finalize — the one table NOT rebuildable from chain), and
``solana_event_meta`` (the event-ingest cursor).
Single connection guarded by one lock; opened with ``check_same_thread=False``.
``busy_timeout`` is set before ``journal_mode=WAL`` because the WAL flip takes a
brief exclusive lock that concurrent openers would otherwise hit as "database is
locked" — the local dev env runs two validators against the same file.
"""

import sqlite3
import threading
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence, Set, Tuple

from allways.classes import ActivityTransition, MinerActivity, next_activity


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

    def insert_activity_event(self, block_num: int, hotkey: str, transition: ActivityTransition) -> None:
        """Record one edge of a miner's ``MinerActivity`` machine (RESERVE_START,
        FULFILL_START, FULFILL_END, or the synthetic RESERVE_EXPIRE)."""
        self._execute(
            'INSERT INTO activity_events (block_num, hotkey, kind) VALUES (?, ?, ?)',
            (block_num, hotkey, int(transition)),
        )

    def load_all_active_events(self) -> List[dict]:
        rows = self._fetchall('SELECT block_num, hotkey, active FROM active_events ORDER BY block_num ASC, id ASC')
        return [{'block_num': r['block_num'], 'hotkey': r['hotkey'], 'active': bool(r['active'])} for r in rows]

    def load_all_activity_events(self) -> List[dict]:
        rows = self._fetchall('SELECT block_num, hotkey, kind FROM activity_events ORDER BY block_num ASC, id ASC')
        return [{'block_num': r['block_num'], 'hotkey': r['hotkey'], 'kind': r['kind']} for r in rows]

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
    # At-time + in-range queries over the active/activity/collateral event tables,
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

    def get_activity_events_in_range(self, start_time: int, end_time: int) -> List[dict]:
        """Activity transitions in ``(start_time, end_time]``. Ordered ``block_num,
        kind`` so coincident-instant edges replay in machine-precedence order
        (closers/openers before a reservation lapse)."""
        rows = self._fetchall(
            """
            SELECT id, block_num, hotkey, kind FROM activity_events
            WHERE block_num > ? AND block_num <= ?
            ORDER BY block_num ASC, kind ASC, id ASC
            """,
            (start_time, end_time),
        )
        return [{'hotkey': r['hotkey'], 'kind': r['kind'], 'block': r['block_num']} for r in rows]

    def get_activity_state_at(self, at_time: int) -> Dict[str, MinerActivity]:
        """Per-hotkey ``MinerActivity`` at ``at_time``, reduced over each miner's
        transition timeline. Only non-AVAILABLE miners are returned (callers
        default the rest to AVAILABLE)."""
        rows = self._fetchall(
            """
            SELECT block_num, hotkey, kind FROM activity_events
            WHERE block_num <= ?
            ORDER BY block_num ASC, kind ASC, id ASC
            """,
            (at_time,),
        )
        return self._reduce_activity(rows)

    @staticmethod
    def _reduce_activity(rows: Sequence[sqlite3.Row]) -> Dict[str, MinerActivity]:
        """Fold ordered transition rows into ``{hotkey: state}`` for non-AVAILABLE
        miners. An undefined transition holds the current state (defensive)."""
        states: Dict[str, MinerActivity] = {}
        for r in rows:
            hk = r['hotkey']
            cur = states.get(hk, MinerActivity.AVAILABLE)
            nxt = next_activity(cur, ActivityTransition(r['kind']))
            states[hk] = cur if nxt is None else nxt
        return {hk: st for hk, st in states.items() if st is not MinerActivity.AVAILABLE}

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
            {'hotkey': r['hotkey'], 'collateral_rao': int(r['collateral_rao']), 'block': r['block_num']} for r in rows
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

    # ─── clearing_rates (per-swap realized legs) ────────────────────────

    def insert_clearing_rate(
        self,
        block_num: int,
        hotkey: str,
        from_chain: str,
        to_chain: str,
        from_amount: int,
        to_amount: int,
    ) -> None:
        """Persist one completed swap's realized legs. ``block_num`` is the unix
        ``blockTime``; the legs are stored as decimal strings (u128-safe)."""
        self._execute(
            """
            INSERT INTO clearing_rates (block_num, hotkey, from_chain, to_chain, from_amount, to_amount)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (block_num, hotkey, from_chain, to_chain, str(int(from_amount)), str(int(to_amount))),
        )

    def get_clearing_volumes(self, start_time: int, end_time: int) -> Dict[Tuple[str, str], Dict[str, Tuple[int, int]]]:
        """``{(from_chain, to_chain): {hotkey: (from_amount_sum, to_amount_sum)}}``
        over ``(start_time, end_time]`` — the windowed realized-volume read the
        reward weighting consumes. Summed in Python: the legs are stored as TEXT
        (u128-safe) and SQL SUM would coerce them to float."""
        rows = self._fetchall(
            """
            SELECT from_chain, to_chain, hotkey, from_amount, to_amount FROM clearing_rates
            WHERE block_num > ? AND block_num <= ?
            """,
            (start_time, end_time),
        )
        volumes: Dict[Tuple[str, str], Dict[str, Tuple[int, int]]] = {}
        for r in rows:
            direction = volumes.setdefault((r['from_chain'], r['to_chain']), {})
            from_sum, to_sum = direction.get(r['hotkey'], (0, 0))
            direction[r['hotkey']] = (from_sum + int(r['from_amount']), to_sum + int(r['to_amount']))
        return volumes

    def prune_clearing_rates(self, cutoff_block: int) -> None:
        """Drop clearing-rate rows older than ``cutoff_block``. No anchor row is
        preserved — each row is an independent sample, not a state-reconstruction
        baseline (unlike rate/active/collateral events)."""
        if cutoff_block <= 0:
            return
        self._execute('DELETE FROM clearing_rates WHERE block_num < ?', (cutoff_block,))

    # ─── swap_outcomes (terminal per-swap truth for the seam) ───────────

    def record_swap_outcome(self, swap_key: str, outcome: str, block_time: int) -> None:
        """Persist a swap's terminal outcome (``completed`` | ``timed_out``) keyed by
        swap_key hex. Upsert: a cursor-reset re-ingest of the same event is a no-op."""
        self._execute(
            """
            INSERT INTO swap_outcomes (swap_key, outcome, block_time) VALUES (?, ?, ?)
            ON CONFLICT(swap_key) DO UPDATE SET outcome = excluded.outcome, block_time = excluded.block_time
            """,
            (swap_key, outcome, block_time),
        )

    def get_swap_outcome(self, swap_key: str) -> Optional[str]:
        row = self._fetchone('SELECT outcome FROM swap_outcomes WHERE swap_key = ?', (swap_key,))
        return row['outcome'] if row is not None else None

    def prune_swap_outcomes(self, cutoff_block: int) -> None:
        """Drop outcome rows older than ``cutoff_block``. No anchor row — each row is
        an independent terminal fact, only queried while the offering still polls."""
        if cutoff_block <= 0:
            return
        self._execute('DELETE FROM swap_outcomes WHERE block_time < ?', (cutoff_block,))

    # ─── routed_requests (on-behalf reservation queue) ──────────────────
    # The ONLY table not rebuildable from chain events: a routed user's details
    # exist nowhere else until the won seat is finalized on-chain.

    def upsert_routed_request(
        self,
        miner: str,
        from_chain: str,
        to_chain: str,
        user_pubkey: str,
        user_from_addr: str,
        user_to_addr: str,
        from_amount: int,
        created_at: int,
    ) -> None:
        """Persist one routed reservation request. A retry from the same user for the
        same (miner, direction) refreshes addresses/amount but keeps its original
        ``created_at`` — a retry never loses its FIFO position."""
        self._execute(
            """
            INSERT INTO routed_requests
                (miner, from_chain, to_chain, user_pubkey, user_from_addr, user_to_addr, from_amount, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(miner, from_chain, to_chain, user_pubkey) DO UPDATE SET
                user_from_addr = excluded.user_from_addr,
                user_to_addr = excluded.user_to_addr,
                from_amount = excluded.from_amount
            """,
            (miner, from_chain, to_chain, user_pubkey, user_from_addr, user_to_addr, str(int(from_amount)), created_at),
        )

    def pending_routed_requests(self, miner: str, from_chain: str, to_chain: str) -> List[dict]:
        """A miner-direction's queued requests, oldest first (the FIFO order
        ``draw_pool_winner`` selects from)."""
        rows = self._fetchall(
            """
            SELECT user_pubkey, user_from_addr, user_to_addr, from_amount, created_at FROM routed_requests
            WHERE miner = ? AND from_chain = ? AND to_chain = ?
            ORDER BY created_at ASC, id ASC
            """,
            (miner, from_chain, to_chain),
        )
        return [
            {
                'user_pubkey': r['user_pubkey'],
                'user_from_addr': r['user_from_addr'],
                'user_to_addr': r['user_to_addr'],
                'from_amount': int(r['from_amount']),
                'created_at': r['created_at'],
            }
            for r in rows
        ]

    def distinct_routed_pools(self) -> List[Tuple[str, str, str]]:
        """The (miner, from_chain, to_chain) keys with pending requests — the
        finalize sweep's iteration set."""
        rows = self._fetchall('SELECT DISTINCT miner, from_chain, to_chain FROM routed_requests')
        return [(r['miner'], r['from_chain'], r['to_chain']) for r in rows]

    def delete_routed_requests(self, miner: str, from_chain: str, to_chain: str) -> None:
        """Drop a miner-direction's whole queue — called on any terminal outcome
        (finalized, lost, expired). Non-selected users re-request via their client."""
        self._execute(
            'DELETE FROM routed_requests WHERE miner = ? AND from_chain = ? AND to_chain = ?',
            (miner, from_chain, to_chain),
        )

    def prune_routed_requests(self, cutoff_time: int) -> None:
        """Staleness backstop: drop rows older than ``cutoff_time`` so a dead
        miner (pool never drawn, reservation never seen) can't pin a queue."""
        self._execute('DELETE FROM routed_requests WHERE created_at < ?', (cutoff_time,))

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

    def prune_activity_events(self, cutoff_block: int) -> None:
        """Drop activity transitions older than ``cutoff_block`` except for hotkeys
        still mid-reservation/swap (reduced state != AVAILABLE) — their full
        timeline is kept so a later FULFILL_END / RESERVE_EXPIRE isn't orphaned.
        Read + reduce + delete under one lock so no writer interleaves."""
        if cutoff_block <= 0:
            return
        with self.lock:
            conn = self.require_connection()
            all_rows = conn.execute(
                'SELECT block_num, hotkey, kind FROM activity_events ORDER BY block_num ASC, kind ASC, id ASC'
            ).fetchall()
            open_hotkeys = set(self._reduce_activity(all_rows))
            if open_hotkeys:
                placeholders = ','.join('?' * len(open_hotkeys))
                conn.execute(
                    f'DELETE FROM activity_events WHERE block_num < ? AND hotkey NOT IN ({placeholders})',
                    (cutoff_block, *open_hotkeys),
                )
            else:
                conn.execute('DELETE FROM activity_events WHERE block_num < ?', (cutoff_block,))
            conn.commit()

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
            # The pre-B3.5 scoring ledger squatted this name; IF NOT EXISTS keeps its dead schema.
            cols = [row[1] for row in conn.execute('PRAGMA table_info(swap_outcomes)')]
            if cols and 'outcome' not in cols:
                conn.execute('DROP TABLE swap_outcomes')
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

                -- MinerActivity transitions (D4): kind is an ActivityTransition
                -- value; the crown replay reduces these into per-instant state so
                -- a reserved/fulfilling miner forfeits crown (REWARD_MINER_STATES).
                CREATE TABLE IF NOT EXISTS activity_events (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    block_num   INTEGER NOT NULL,
                    hotkey      TEXT NOT NULL,
                    kind        INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_activity_events_block
                    ON activity_events(block_num);
                CREATE INDEX IF NOT EXISTS idx_activity_events_hotkey
                    ON activity_events(hotkey);

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

                -- Per-swap realized legs from SwapCompleted. One row per completed
                -- swap; the windowed volume read (fill_ratio's input) sums these.
                -- from_amount/to_amount are TEXT because the on-chain legs are
                -- u128 and overflow SQLite's signed-64 INTEGER.
                CREATE TABLE IF NOT EXISTS clearing_rates (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    block_num   INTEGER NOT NULL,
                    hotkey      TEXT NOT NULL,
                    from_chain  TEXT NOT NULL,
                    to_chain    TEXT NOT NULL,
                    from_amount TEXT NOT NULL,
                    to_amount   TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_clearing_rates_dir_block
                    ON clearing_rates(from_chain, to_chain, block_num);

                -- Terminal outcome per swap (SwapCompleted | SwapTimedOut), keyed by
                -- swap_key hex. Terminal swap PDAs are closed on-chain, so this is the
                -- seam's only way to tell a slash from a completion after close. Not
                -- the old B3.5 scoring ledger — scoring reads on-chain counters.
                CREATE TABLE IF NOT EXISTS swap_outcomes (
                    swap_key    TEXT PRIMARY KEY,
                    outcome     TEXT NOT NULL,
                    block_time  INTEGER NOT NULL
                );

                -- Routed reservation requests awaiting their draw (on-behalf flow).
                -- The ONLY table not rebuildable from chain events: the user's
                -- details live here alone until finalize_reservation publishes
                -- them. from_amount is TEXT (u128-safe), created_at is the FIFO key.
                CREATE TABLE IF NOT EXISTS routed_requests (
                    id             INTEGER PRIMARY KEY AUTOINCREMENT,
                    miner          TEXT NOT NULL,
                    from_chain     TEXT NOT NULL,
                    to_chain       TEXT NOT NULL,
                    user_pubkey    TEXT NOT NULL,
                    user_from_addr TEXT NOT NULL,
                    user_to_addr   TEXT NOT NULL,
                    from_amount    TEXT NOT NULL,
                    created_at     INTEGER NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_routed_requests_key
                    ON routed_requests(miner, from_chain, to_chain, user_pubkey);

                CREATE TABLE IF NOT EXISTS solana_event_meta (
                    key     TEXT PRIMARY KEY,
                    value   TEXT NOT NULL
                );
                """
            )
            conn.commit()
