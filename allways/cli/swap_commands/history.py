"""Local swap history — SQLite persistence for completed/timed-out swaps."""

import sqlite3
import time
from typing import Optional

from allways.classes import Swap
from allways.cli.swap_commands.helpers import ALLWAYS_DIR

HISTORY_DB = ALLWAYS_DIR / 'swap_history.db'

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS swap_history (
    swap_id         INTEGER PRIMARY KEY,
    status          INTEGER NOT NULL,
    source_chain    TEXT NOT NULL,
    dest_chain      TEXT NOT NULL,
    source_amount   INTEGER NOT NULL,
    dest_amount     INTEGER NOT NULL,
    tao_amount      INTEGER NOT NULL,
    rate            TEXT NOT NULL DEFAULT '',
    user_source_address TEXT NOT NULL DEFAULT '',
    user_dest_address   TEXT NOT NULL DEFAULT '',
    miner_hotkey    TEXT NOT NULL DEFAULT '',
    source_tx_hash  TEXT NOT NULL DEFAULT '',
    dest_tx_hash    TEXT NOT NULL DEFAULT '',
    initiated_block INTEGER NOT NULL DEFAULT 0,
    fulfilled_block INTEGER NOT NULL DEFAULT 0,
    completed_block INTEGER NOT NULL DEFAULT 0,
    timeout_block   INTEGER NOT NULL DEFAULT 0,
    saved_at        REAL NOT NULL
)
"""


def _get_conn() -> sqlite3.Connection:
    ALLWAYS_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(HISTORY_DB)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.row_factory = sqlite3.Row
    conn.execute(_CREATE_TABLE)
    return conn


def save_swap(swap: Swap) -> None:
    """Save a resolved swap to local history. Overwrites if swap_id already exists."""
    conn = _get_conn()
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO swap_history (
                swap_id, status, source_chain, dest_chain,
                source_amount, dest_amount, tao_amount, rate,
                user_source_address, user_dest_address, miner_hotkey,
                source_tx_hash, dest_tx_hash,
                initiated_block, fulfilled_block, completed_block, timeout_block,
                saved_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                swap.id,
                int(swap.status),
                swap.source_chain,
                swap.dest_chain,
                swap.source_amount,
                swap.dest_amount,
                swap.tao_amount,
                swap.rate,
                swap.user_source_address,
                swap.user_dest_address,
                swap.miner_hotkey,
                swap.source_tx_hash,
                swap.dest_tx_hash,
                swap.initiated_block,
                swap.fulfilled_block,
                swap.completed_block,
                swap.timeout_block,
                time.time(),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def load_history(
    status: Optional[int] = None,
    limit: int = 50,
) -> list[dict]:
    """Load swap history rows. Returns list of dicts, newest first."""
    conn = _get_conn()
    try:
        query = 'SELECT * FROM swap_history'
        params: list = []
        if status is not None:
            query += ' WHERE status = ?'
            params.append(status)
        query += ' ORDER BY saved_at DESC LIMIT ?'
        params.append(limit)
        rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def load_swap(swap_id: int) -> Optional[dict]:
    """Load a single swap from history by ID."""
    conn = _get_conn()
    try:
        row = conn.execute('SELECT * FROM swap_history WHERE swap_id = ?', (swap_id,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()
