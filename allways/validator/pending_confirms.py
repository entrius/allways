"""Thread-safe queue for swap confirmations awaiting source tx confirmations.

Written by axon handler thread (handle_swap_confirm), read by forward loop thread
(_process_pending_confirms). Keyed by miner_hotkey since reservations are 1:1 per miner.

This module owns the queue abstraction and its storage backends. The validator
uses a local SQLite-backed store in production so pending confirmations survive
validator process restarts.
"""

import sqlite3
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, List, Optional, Protocol

MAX_QUEUE_SIZE = 50


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
        store: Optional['PendingConfirmStore'] = None,
        current_block_fn: Optional[Callable[[], int]] = None,
    ):
        self._store = store or SqlitePendingConfirmStore()
        self._current_block_fn = current_block_fn

    def enqueue(self, item: PendingConfirm) -> bool:
        """Add or replace a pending confirm. Returns False if queue is full (and not an overwrite)."""
        return self._store.upsert(item)

    def get_all(self) -> List[PendingConfirm]:
        """Return a snapshot of all pending items."""
        self._purge_expired()
        return self._store.list_all()

    def remove(self, miner_hotkey: str) -> Optional[PendingConfirm]:
        """Remove and return a specific entry."""
        return self._store.remove(miner_hotkey)

    def has(self, miner_hotkey: str) -> bool:
        self._purge_expired()
        return self._store.has(miner_hotkey)

    def size(self) -> int:
        self._purge_expired()
        return self._store.size()

    def _purge_expired(self) -> None:
        if self._current_block_fn is None:
            return
        self._store.purge_expired(self._current_block_fn())


class PendingConfirmStore(Protocol):
    """Persistence interface for pending confirms."""

    def upsert(self, item: PendingConfirm) -> bool:
        """Add or replace a pending confirm. False means the queue is full."""

    def list_all(self) -> List[PendingConfirm]:
        """Return all pending confirms."""

    def remove(self, miner_hotkey: str) -> Optional[PendingConfirm]:
        """Remove and return a pending confirm."""

    def has(self, miner_hotkey: str) -> bool:
        """Return whether an entry exists for the miner."""

    def size(self) -> int:
        """Return the number of queued items."""

    def purge_expired(self, current_block: int) -> None:
        """Delete entries whose reservation has expired."""

class SqlitePendingConfirmStore:
    """Durable local SQLite store for pending confirms."""

    def __init__(self, db_path: Path | str | None = None):
        self._db_path = Path(db_path or Path.home() / '.allways' / 'validator' / 'pending_confirms.db')
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path)
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA busy_timeout=5000')
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        with self._get_connection() as conn:
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

    def upsert(self, item: PendingConfirm) -> bool:
        with self._lock:
            with self._get_connection() as conn:
                exists = conn.execute(
                    'SELECT 1 FROM pending_confirms WHERE miner_hotkey = ?',
                    (item.miner_hotkey,),
                ).fetchone()
                if exists is None:
                    count = conn.execute('SELECT COUNT(*) FROM pending_confirms').fetchone()[0]
                    if count >= MAX_QUEUE_SIZE:
                        return False

                fields = (
                    'miner_hotkey',
                    'source_tx_hash',
                    'source_chain',
                    'dest_chain',
                    'source_address',
                    'dest_address',
                    'tao_amount',
                    'source_amount',
                    'dest_amount',
                    'miner_deposit_address',
                    'miner_dest_address',
                    'rate_str',
                    'reserved_until',
                    'queued_at',
                )
                columns = ', '.join(fields)
                placeholders = ', '.join('?' for _ in fields)
                updates = ', '.join(
                    f'{field} = excluded.{field}'
                    for field in fields
                    if field != 'miner_hotkey'
                )
                values = tuple(getattr(item, field) for field in fields)

                conn.execute(
                    f"""
                    INSERT INTO pending_confirms ({columns})
                    VALUES ({placeholders})
                    ON CONFLICT (miner_hotkey)
                    DO UPDATE SET {updates}
                    """,
                    values,
                )
                conn.commit()
                return True

    def list_all(self) -> List[PendingConfirm]:
        with self._get_connection() as conn:
            rows = conn.execute('SELECT * FROM pending_confirms ORDER BY queued_at').fetchall()
        return [self._row_to_item(row) for row in rows]

    def remove(self, miner_hotkey: str) -> Optional[PendingConfirm]:
        with self._lock:
            with self._get_connection() as conn:
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
        with self._get_connection() as conn:
            row = conn.execute(
                'SELECT 1 FROM pending_confirms WHERE miner_hotkey = ?',
                (miner_hotkey,),
            ).fetchone()
        return row is not None

    def size(self) -> int:
        with self._get_connection() as conn:
            return int(conn.execute('SELECT COUNT(*) FROM pending_confirms').fetchone()[0])

    def purge_expired(self, current_block: int) -> None:
        with self._lock:
            with self._get_connection() as conn:
                conn.execute(
                    'DELETE FROM pending_confirms WHERE reserved_until < ?',
                    (current_block,),
                )
                conn.commit()
