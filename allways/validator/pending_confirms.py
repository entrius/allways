"""Thread-safe queue for swap confirmations awaiting source tx confirmations.

Written by axon handler thread (handle_swap_confirm), read by forward loop thread
(_process_pending_confirms). Keyed by miner_hotkey since reservations are 1:1 per miner.

The queue stores entries in a local SQLite database so pending confirmations
survive validator process restarts.
"""

import sqlite3
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, List, Optional


@dataclass
class PendingConfirm:
    """All data needed to call vote_initiate once tx confirmations are met."""

    miner_hotkey: str
    source_tx_hash: str
    source_chain: str
    dest_chain: str
    source_address: str
    dest_address: str
    tao_amount: int
    source_amount: int
    dest_amount: int
    miner_deposit_address: str
    miner_dest_address: str
    rate_str: str
    reserved_until: int
    queued_at: float = field(default_factory=time.time)


class PendingConfirmQueue:
    """Thread-safe queue of unconfirmed swap confirmations.

    Keyed by miner_hotkey — one reservation per miner at a time,
    so re-submissions for the same miner overwrite the previous entry.
    """

    def __init__(
        self,
        db_path: Path | str | None = None,
        current_block_fn: Optional[Callable[[], int]] = None,
    ):
        self._db_path = Path(db_path or Path.home() / '.allways' / 'validator' / 'pending_confirms.db')
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self._db_path, check_same_thread=False)
        self._conn.execute('PRAGMA journal_mode=WAL')
        self._conn.execute('PRAGMA busy_timeout=5000')
        self._conn.row_factory = sqlite3.Row
        self._init_db()
        self._current_block_fn = current_block_fn

    def enqueue(self, item: PendingConfirm) -> None:
        """Add or replace a pending confirm."""
        with self._lock:
            conn = self._require_connection()
            conn.execute(
                """
                INSERT OR REPLACE INTO pending_confirms (
                    miner_hotkey,
                    source_tx_hash,
                    source_chain,
                    dest_chain,
                    source_address,
                    dest_address,
                    tao_amount,
                    source_amount,
                    dest_amount,
                    miner_deposit_address,
                    miner_dest_address,
                    rate_str,
                    reserved_until,
                    queued_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    item.miner_hotkey,
                    item.source_tx_hash,
                    item.source_chain,
                    item.dest_chain,
                    item.source_address,
                    item.dest_address,
                    item.tao_amount,
                    item.source_amount,
                    item.dest_amount,
                    item.miner_deposit_address,
                    item.miner_dest_address,
                    item.rate_str,
                    item.reserved_until,
                    item.queued_at,
                ),
            )
            conn.commit()

    def get_all(self) -> List[PendingConfirm]:
        """Return a snapshot of all pending items."""
        with self._lock:
            conn = self._require_connection()
            self._purge_expired(conn)
            rows = conn.execute('SELECT * FROM pending_confirms ORDER BY queued_at').fetchall()
        return [self._row_to_item(row) for row in rows]

    def remove(self, miner_hotkey: str) -> Optional[PendingConfirm]:
        """Remove and return a specific entry."""
        with self._lock:
            conn = self._require_connection()
            row = conn.execute(
                'SELECT * FROM pending_confirms WHERE miner_hotkey = ?',
                (miner_hotkey,),
            ).fetchone()
            if row is None:
                return None
            conn.execute('DELETE FROM pending_confirms WHERE miner_hotkey = ?', (miner_hotkey,))
            conn.commit()
        return self._row_to_item(row)

    def has(self, miner_hotkey: str) -> bool:
        with self._lock:
            conn = self._require_connection()
            self._purge_expired(conn)
            row = conn.execute(
                'SELECT 1 FROM pending_confirms WHERE miner_hotkey = ? LIMIT 1',
                (miner_hotkey,),
            ).fetchone()
        return row is not None

    def size(self) -> int:
        with self._lock:
            conn = self._require_connection()
            self._purge_expired(conn)
            count = conn.execute('SELECT COUNT(*) FROM pending_confirms').fetchone()[0]
            return int(count)

    def _purge_expired(self, conn: sqlite3.Connection) -> None:
        if self._current_block_fn is None:
            return
        current_block = self._current_block_fn()
        conn.execute('DELETE FROM pending_confirms WHERE reserved_until < ?', (current_block,))
        conn.commit()

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    def _init_db(self) -> None:
        with self._lock:
            conn = self._require_connection()
            conn.execute("""
                CREATE TABLE IF NOT EXISTS pending_confirms (
                    miner_hotkey TEXT PRIMARY KEY,
                    source_tx_hash TEXT NOT NULL,
                    source_chain TEXT NOT NULL,
                    dest_chain TEXT NOT NULL,
                    source_address TEXT NOT NULL,
                    dest_address TEXT NOT NULL,
                    tao_amount INTEGER NOT NULL,
                    source_amount INTEGER NOT NULL,
                    dest_amount INTEGER NOT NULL,
                    miner_deposit_address TEXT NOT NULL,
                    miner_dest_address TEXT NOT NULL,
                    rate_str TEXT NOT NULL,
                    reserved_until INTEGER NOT NULL,
                    queued_at REAL NOT NULL
                )
            """)
            conn.commit()

    @staticmethod
    def _row_to_item(row: sqlite3.Row) -> PendingConfirm:
        return PendingConfirm(
            miner_hotkey=row['miner_hotkey'],
            source_tx_hash=row['source_tx_hash'],
            source_chain=row['source_chain'],
            dest_chain=row['dest_chain'],
            source_address=row['source_address'],
            dest_address=row['dest_address'],
            tao_amount=row['tao_amount'],
            source_amount=row['source_amount'],
            dest_amount=row['dest_amount'],
            miner_deposit_address=row['miner_deposit_address'],
            miner_dest_address=row['miner_dest_address'],
            rate_str=row['rate_str'],
            reserved_until=row['reserved_until'],
            queued_at=row['queued_at'],
        )

    def _require_connection(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError('PendingConfirmQueue is closed')
        return self._conn
