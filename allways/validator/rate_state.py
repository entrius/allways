"""SQLite-backed store for V1 crown-time scoring.

Three tables:

- ``rate_events``: history of miner rate commitments per (hotkey, from_chain,
  to_chain). Inserts are throttled at ``RATE_UPDATE_MIN_INTERVAL_BLOCKS`` per
  hotkey+direction to prevent rate-war griefing, and deduplicated against the
  last known rate so no-op events are dropped.

- ``collateral_events``: history of miner collateral balances. The smart
  contract stores collateral as a single per-miner balance (not per-direction),
  so rows have no direction columns.

- ``swap_outcomes``: all-time credibility ledger, keyed by ``swap_id``. Never
  time-pruned; only removed when a hotkey deregisters. Read during scoring to
  compute ``success_rate ** SUCCESS_EXPONENT``.

Threading: one ``sqlite3.Connection`` opened with ``check_same_thread=False``
behind a ``threading.Lock``. Mirrors the pattern used by PendingConfirmQueue.
"""

import sqlite3
import threading
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from allways.constants import RATE_UPDATE_MIN_INTERVAL_BLOCKS


class RateStateStore:
    """Thread-safe SQLite store for V1 scoring state."""

    def __init__(self, db_path: Path | str | None = None):
        self._db_path = Path(db_path or Path.home() / '.allways' / 'validator' / 'rate_state.db')
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn: Optional[sqlite3.Connection] = sqlite3.connect(self._db_path, check_same_thread=False)
        self._conn.execute('PRAGMA journal_mode=WAL')
        self._conn.execute('PRAGMA busy_timeout=5000')
        self._conn.row_factory = sqlite3.Row
        self._init_db()

    def insert_rate_event(
        self,
        hotkey: str,
        from_chain: str,
        to_chain: str,
        rate: float,
        block: int,
    ) -> bool:
        """Insert a rate event if throttle + change conditions pass.

        Returns ``True`` if the row was inserted, ``False`` if rejected for
        being within the per-hotkey+direction throttle window or because the
        rate is unchanged from the last accepted event.
        """
        with self._lock:
            conn = self._require_connection()
            row = conn.execute(
                """
                SELECT rate, block FROM rate_events
                WHERE hotkey = ? AND from_chain = ? AND to_chain = ?
                ORDER BY block DESC, id DESC
                LIMIT 1
                """,
                (hotkey, from_chain, to_chain),
            ).fetchone()
            if row is not None:
                if block - row['block'] < RATE_UPDATE_MIN_INTERVAL_BLOCKS:
                    return False
                if row['rate'] == rate:
                    return False
            conn.execute(
                'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
                (hotkey, from_chain, to_chain, rate, block),
            )
            conn.commit()
            return True

    def insert_collateral_event(self, hotkey: str, collateral_rao: int, block: int) -> bool:
        """Insert a collateral event if the value changed since the last row.

        Returns ``True`` on insert, ``False`` if the collateral matches the
        last known value for this hotkey.
        """
        with self._lock:
            conn = self._require_connection()
            row = conn.execute(
                """
                SELECT collateral_rao FROM collateral_events
                WHERE hotkey = ?
                ORDER BY block DESC, id DESC
                LIMIT 1
                """,
                (hotkey,),
            ).fetchone()
            if row is not None and row['collateral_rao'] == collateral_rao:
                return False
            conn.execute(
                'INSERT INTO collateral_events (hotkey, collateral_rao, block) VALUES (?, ?, ?)',
                (hotkey, collateral_rao, block),
            )
            conn.commit()
            return True

    def insert_swap_outcome(
        self,
        swap_id: int,
        miner_hotkey: str,
        completed: bool,
        resolved_block: int,
    ) -> None:
        """Insert or replace a swap outcome row. Idempotent on ``swap_id``."""
        with self._lock:
            conn = self._require_connection()
            conn.execute(
                """
                INSERT OR REPLACE INTO swap_outcomes (swap_id, miner_hotkey, completed, resolved_block)
                VALUES (?, ?, ?, ?)
                """,
                (swap_id, miner_hotkey, 1 if completed else 0, resolved_block),
            )
            conn.commit()

    def get_latest_rate_before(
        self,
        hotkey: str,
        from_chain: str,
        to_chain: str,
        block: int,
    ) -> Optional[Tuple[float, int]]:
        """Most recent rate for ``hotkey``+direction at or before ``block``."""
        with self._lock:
            conn = self._require_connection()
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

    def get_latest_collateral_before(self, hotkey: str, block: int) -> Optional[Tuple[int, int]]:
        """Most recent collateral for ``hotkey`` at or before ``block``."""
        with self._lock:
            conn = self._require_connection()
            row = conn.execute(
                """
                SELECT collateral_rao, block FROM collateral_events
                WHERE hotkey = ? AND block <= ?
                ORDER BY block DESC, id DESC
                LIMIT 1
                """,
                (hotkey, block),
            ).fetchone()
        if row is None:
            return None
        return row['collateral_rao'], row['block']

    def get_rate_events_in_range(
        self,
        from_chain: str,
        to_chain: str,
        start_block: int,
        end_block: int,
    ) -> List[dict]:
        """Rate events in ``(start_block, end_block]`` for a direction, oldest first."""
        with self._lock:
            conn = self._require_connection()
            rows = conn.execute(
                """
                SELECT id, hotkey, rate, block FROM rate_events
                WHERE from_chain = ? AND to_chain = ? AND block > ? AND block <= ?
                ORDER BY block ASC, id ASC
                """,
                (from_chain, to_chain, start_block, end_block),
            ).fetchall()
        return [{'id': r['id'], 'hotkey': r['hotkey'], 'rate': r['rate'], 'block': r['block']} for r in rows]

    def get_collateral_events_in_range(self, start_block: int, end_block: int) -> List[dict]:
        """Collateral events in ``(start_block, end_block]``, oldest first."""
        with self._lock:
            conn = self._require_connection()
            rows = conn.execute(
                """
                SELECT id, hotkey, collateral_rao, block FROM collateral_events
                WHERE block > ? AND block <= ?
                ORDER BY block ASC, id ASC
                """,
                (start_block, end_block),
            ).fetchall()
        return [
            {
                'id': r['id'],
                'hotkey': r['hotkey'],
                'collateral_rao': r['collateral_rao'],
                'block': r['block'],
            }
            for r in rows
        ]

    def get_all_time_success_rates(self) -> Dict[str, Tuple[int, int]]:
        """Return ``{hotkey: (completed_count, timed_out_count)}`` over all outcomes."""
        with self._lock:
            conn = self._require_connection()
            rows = conn.execute(
                """
                SELECT miner_hotkey,
                       SUM(completed) AS completed,
                       SUM(1 - completed) AS timed_out
                FROM swap_outcomes
                GROUP BY miner_hotkey
                """
            ).fetchall()
        return {r['miner_hotkey']: (int(r['completed']), int(r['timed_out'])) for r in rows}

    def delete_hotkey(self, hotkey: str) -> None:
        """Dereg purge: remove the hotkey from all three tables."""
        with self._lock:
            conn = self._require_connection()
            conn.execute('DELETE FROM rate_events WHERE hotkey = ?', (hotkey,))
            conn.execute('DELETE FROM collateral_events WHERE hotkey = ?', (hotkey,))
            conn.execute('DELETE FROM swap_outcomes WHERE miner_hotkey = ?', (hotkey,))
            conn.commit()

    def prune_events_older_than(self, cutoff_block: int) -> None:
        """Delete ``rate_events`` and ``collateral_events`` older than ``cutoff_block``.

        Never touches ``swap_outcomes`` — those are retained forever for
        credibility scoring.
        """
        with self._lock:
            conn = self._require_connection()
            conn.execute('DELETE FROM rate_events WHERE block < ?', (cutoff_block,))
            conn.execute('DELETE FROM collateral_events WHERE block < ?', (cutoff_block,))
            conn.commit()

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    def _init_db(self) -> None:
        with self._lock:
            conn = self._require_connection()
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

                CREATE TABLE IF NOT EXISTS collateral_events (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    hotkey          TEXT NOT NULL,
                    collateral_rao  INTEGER NOT NULL,
                    block           INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_collateral_events_block
                    ON collateral_events(block);
                CREATE INDEX IF NOT EXISTS idx_collateral_events_hotkey_block
                    ON collateral_events(hotkey, block);

                CREATE TABLE IF NOT EXISTS swap_outcomes (
                    swap_id         INTEGER PRIMARY KEY,
                    miner_hotkey    TEXT NOT NULL,
                    completed       INTEGER NOT NULL,
                    resolved_block  INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_swap_outcomes_hotkey
                    ON swap_outcomes(miner_hotkey);
                """
            )
            conn.commit()

    def _require_connection(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError('RateStateStore is closed')
        return self._conn
