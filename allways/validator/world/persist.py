"""The World's tables: accounts as raw bytes (decoded with the program layouts on load), and the ledgers.

Raw bytes keep this schema-free: a program upgrade that changes an account layout changes nothing here."""

from __future__ import annotations

from typing import Dict, Iterable, Optional, Tuple

from allways.validator.store.db import Store

OWNER, VERSION = 'world', 2
TABLES = [
    (
        'world_accounts',
        'CREATE TABLE IF NOT EXISTS world_accounts '
        '(pda TEXT PRIMARY KEY, type TEXT NOT NULL, slot INTEGER NOT NULL, data BLOB NOT NULL)',
    ),
    (
        'world_deposits',
        'CREATE TABLE IF NOT EXISTS world_deposits (tx_key TEXT PRIMARY KEY, swap_key TEXT NOT NULL, '
        'slot INTEGER NOT NULL, seen_at INTEGER NOT NULL, initiated INTEGER NOT NULL)',
    ),
    ('world_initiated', 'CREATE TABLE IF NOT EXISTS world_initiated (swap_key TEXT PRIMARY KEY, at INTEGER NOT NULL)'),
    (
        'world_fulfillments',
        'CREATE TABLE IF NOT EXISTS world_fulfillments (swap_key TEXT PRIMARY KEY, to_tx_hash TEXT NOT NULL, '
        'block_time INTEGER NOT NULL, miner TEXT NOT NULL, settled INTEGER NOT NULL)',
    ),
    (
        'world_routes',
        'CREATE TABLE IF NOT EXISTS world_routes (swap_key TEXT PRIMARY KEY, chain TEXT NOT NULL, '
        'sender TEXT NOT NULL, recipient TEXT NOT NULL, seen_at INTEGER NOT NULL)',
    ),
]

Row = Tuple  # one row's values, in table column order
Changes = Dict[str, Dict[str, Optional[Row]]]  # table → key → row (None = delete)

_UPSERT = {
    'world_accounts': 'INSERT OR REPLACE INTO world_accounts (pda, type, slot, data) VALUES (?, ?, ?, ?)',
    'world_deposits': 'INSERT OR REPLACE INTO world_deposits VALUES (?, ?, ?, ?, ?)',
    'world_initiated': 'INSERT OR REPLACE INTO world_initiated VALUES (?, ?)',
    'world_fulfillments': 'INSERT OR REPLACE INTO world_fulfillments VALUES (?, ?, ?, ?, ?)',
    'world_routes': 'INSERT OR REPLACE INTO world_routes VALUES (?, ?, ?, ?, ?)',
}
_KEY = {
    'world_accounts': 'pda',
    'world_deposits': 'tx_key',
    'world_initiated': 'swap_key',
    'world_fulfillments': 'swap_key',
    'world_routes': 'swap_key',
}


def declare(store: Store) -> None:
    store.declare(OWNER, VERSION, TABLES)


def save(store: Store, changes: Changes, meta: Dict[str, object], ack: Optional[Tuple[int, str]]) -> None:
    """One transaction: every changed row, the World's meta, and (if given) its feed position."""
    with store.tx() as c:
        for table, rows in changes.items():
            for key, row in rows.items():
                if row is None:
                    c.execute(f'DELETE FROM {table} WHERE {_KEY[table]} = ?', (key,))
                else:
                    c.execute(_UPSERT[table], row)
        for key, value in meta.items():
            Store.set_meta(c, f'world.{key}', str(value))
        if ack is not None:
            Store.ack(c, OWNER, ack[0], ack[1])


def load(store: Store, table: str) -> Iterable:
    return store.conn().execute(f'SELECT * FROM {table}').fetchall()
