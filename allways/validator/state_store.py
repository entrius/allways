"""Single SQLite-backed store for all validator-local state.

Consolidates what used to be two files (``rate_state.db`` + ``pending_confirms.db``)
into one ``state.db`` with a single connection, a single lock, and one class
holding every table the validator owns:

- ``pending_confirms`` — user swap confirmations awaiting tx confirmations;
  written by axon handler thread, drained by forward loop thread.
- ``rate_events`` — per-miner per-direction rate history used by the V1
  crown-time scoring replay. Deduped on insert (same-rate observations are
  dropped) and bounded by ``EVENT_RETENTION_BLOCKS`` on prune.
- ``collateral_events`` — per-miner collateral history. One row per observed
  change. Pruned alongside rate_events.
- ``swap_outcomes`` — rolling credibility ledger keyed by ``swap_id``. Pruned
  on each scoring pass to a ~30 day window, and fully removed when a hotkey
  deregisters. Read during scoring via ``get_success_rates_since(block)`` to
  compute ``success_rate ** SUCCESS_EXPONENT`` over recent behavior only.

Threading: one ``sqlite3.Connection`` opened with ``check_same_thread=False``
behind a ``threading.Lock``. ``busy_timeout`` set before ``journal_mode=WAL``
so concurrent openers wait on the init lock instead of erroring out.
"""

import sqlite3
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple


@dataclass
class PendingConfirm:
    """All data needed to call ``vote_initiate`` once tx confirmations are met."""

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
    queued_at: float = field(default_factory=time.time)


class ValidatorStateStore:
    """Single-connection SQLite store owning every validator-local table."""

    def __init__(
        self,
        db_path: Path | str | None = None,
        current_block_fn: Optional[Callable[[], int]] = None,
    ):
        self.db_path = Path(db_path or Path.home() / '.allways' / 'validator' / 'state.db')
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.Lock()
        self.conn: Optional[sqlite3.Connection] = sqlite3.connect(self.db_path, check_same_thread=False)
        # busy_timeout must be set BEFORE journal_mode: setting WAL mode takes a
        # brief exclusive lock that a concurrent opener will otherwise hit as an
        # immediate "database is locked" error (dev env runs two validators
        # against the same SQLite file).
        self.conn.execute('PRAGMA busy_timeout=5000')
        self.conn.execute('PRAGMA journal_mode=WAL')
        self.conn.row_factory = sqlite3.Row
        self.current_block_fn = current_block_fn
        self.init_db()

    # ─── pending_confirms ───────────────────────────────────────────────

    def enqueue(self, item: PendingConfirm) -> None:
        """Add or replace a pending confirm. Keyed by ``miner_hotkey``."""
        with self.lock:
            conn = self.require_connection()
            conn.execute(
                """
                INSERT OR REPLACE INTO pending_confirms (
                    miner_hotkey, from_tx_hash, from_chain, to_chain,
                    from_address, to_address, tao_amount, from_amount,
                    to_amount, miner_from_address, miner_to_address,
                    rate_str, reserved_until, queued_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    item.queued_at,
                ),
            )
            conn.commit()

    def get_all(self) -> List[PendingConfirm]:
        """Return a snapshot of all pending items, oldest first.

        Read-only — does not purge expired entries. Call ``purge_expired``
        explicitly from the forward loop once per tick instead of side-
        effecting every read.
        """
        with self.lock:
            conn = self.require_connection()
            rows = conn.execute('SELECT * FROM pending_confirms ORDER BY queued_at').fetchall()
        return [self.row_to_pending(row) for row in rows]

    def remove(self, miner_hotkey: str) -> Optional[PendingConfirm]:
        """Remove and return a specific entry."""
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
        """Drop pending confirms whose reservation has already expired.

        Returns the number of rows removed. Meant to be called once per
        forward-loop tick — the forward loop knows when it's safe to mutate.
        """
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
            queued_at=row['queued_at'],
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
        """Insert a rate event, skipping same-rate duplicates.

        The validator no longer throttles how often rate events land — per-block
        commitment polling picks up every chain-accepted change, and shorter
        inter-rate gaps become real crown intervals instead of getting collapsed
        into the previous one. The only gate is same-rate dedupe to keep the
        table from bloating on identical observations.
        """
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
        """Most recent rate for ``hotkey``+direction at or before ``block``."""
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
    ) -> None:
        """Insert or replace a swap outcome row. Idempotent on ``swap_id``."""
        with self.lock:
            conn = self.require_connection()
            conn.execute(
                """
                INSERT OR REPLACE INTO swap_outcomes (swap_id, miner_hotkey, completed, resolved_block)
                VALUES (?, ?, ?, ?)
                """,
                (swap_id, miner_hotkey, 1 if completed else 0, resolved_block),
            )
            conn.commit()

    def get_success_rates_since(self, since_block: int) -> Dict[str, Tuple[int, int]]:
        """Return ``{hotkey: (completed_count, timed_out_count)}`` for outcomes
        resolved at or after ``since_block``. Callers pass the rolling
        credibility window start so miners can rehabilitate over time."""
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

    def prune_swap_outcomes_older_than(self, cutoff_block: int) -> None:
        """Bound swap_outcomes growth by dropping rows resolved before the
        credibility window start. Called from the scoring pass alongside the
        rate-events prune."""
        if cutoff_block <= 0:
            return
        with self.lock:
            conn = self.require_connection()
            conn.execute('DELETE FROM swap_outcomes WHERE resolved_block < ?', (cutoff_block,))
            conn.commit()

    # ─── cross-table maintenance ────────────────────────────────────────

    def delete_hotkey(self, hotkey: str) -> None:
        """Dereg purge: remove the hotkey from rate/outcomes tables."""
        with self.lock:
            conn = self.require_connection()
            conn.execute('DELETE FROM rate_events WHERE hotkey = ?', (hotkey,))
            conn.execute('DELETE FROM swap_outcomes WHERE miner_hotkey = ?', (hotkey,))
            conn.commit()

    def prune_events_older_than(self, cutoff_block: int) -> None:
        """Delete rate events older than ``cutoff_block``ㅡwith one exception.

        The single most recent row per ``(hotkey, from_chain, to_chain)`` is
        always preserved, even if it's older than the cutoff. Without this,
        a miner who posts a rate once and never updates it would eventually
        have their only row pruned, and ``get_latest_rate_before(window_start)``
        at the next scoring pass would find nothing, dropping them from crown
        attribution entirely.

        Never touches ``swap_outcomes`` or ``pending_confirms`` — those have
        their own lifetimes.
        """
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
                    resolved_block  INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_swap_outcomes_hotkey
                    ON swap_outcomes(miner_hotkey);
                CREATE INDEX IF NOT EXISTS idx_swap_outcomes_resolved_block
                    ON swap_outcomes(resolved_block);
                """
            )
            conn.commit()
