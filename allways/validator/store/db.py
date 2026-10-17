"""The validator's one SQLite file. It holds only what the chain can't cheaply give back.

There is no central schema: each component declares the tables it owns (``Store.declare``) and is the
only writer of them. Every thread gets its own connection (WAL, so readers never block the writer), and
a component writes its state and its feed position in one transaction (``Store.tx``), so after a crash
the two always agree. The store starts empty on a fresh disk and refills from the chain, so there is no
migration history: a table whose shape changes is dropped and refilled, never migrated in place."""

from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from typing import Iterator, List, Optional, Tuple

_META = 'CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)'
# How far each event consumer has durably applied the feed: the feed resumes from the oldest of these.
_ACKS = 'CREATE TABLE IF NOT EXISTS acks (consumer TEXT PRIMARY KEY, slot INTEGER NOT NULL, signature TEXT NOT NULL)'
# Shape version per owned table set; a mismatch drops and recreates the set (the chain refills it).
_SHAPES = 'CREATE TABLE IF NOT EXISTS shapes (owner TEXT PRIMARY KEY, version INTEGER NOT NULL)'


class Store:
    def __init__(self, path: str) -> None:
        self.path = path
        self._local = threading.local()
        with self.tx() as c:
            for ddl in (_META, _ACKS, _SHAPES):
                c.execute(ddl)

    def conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, 'conn', None)
        if conn is None:
            conn = sqlite3.connect(self.path, isolation_level=None)  # explicit transactions only
            conn.row_factory = sqlite3.Row
            conn.execute('PRAGMA busy_timeout=5000')  # before WAL: the switch itself can wait on a lock
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA synchronous=NORMAL')
            self._local.conn = conn
        return conn

    @contextmanager
    def tx(self) -> Iterator[sqlite3.Connection]:
        conn = self.conn()
        conn.execute('BEGIN IMMEDIATE')
        try:
            yield conn
        except BaseException:
            conn.execute('ROLLBACK')
            raise
        conn.execute('COMMIT')

    def declare(self, owner: str, version: int, tables: List[Tuple[str, str]]) -> bool:
        """Create ``owner``'s tables ((name, ddl) pairs). If their declared ``version`` changed, drop and
        recreate them instead of migrating. Returns True when the tables were (re)created empty."""
        with self.tx() as c:
            row = c.execute('SELECT version FROM shapes WHERE owner = ?', (owner,)).fetchone()
            fresh = row is None or int(row['version']) != version
            if fresh:
                for name, _ in tables:
                    c.execute(f'DROP TABLE IF EXISTS {name}')
                c.execute('INSERT OR REPLACE INTO shapes (owner, version) VALUES (?, ?)', (owner, version))
            for _, ddl in tables:
                c.execute(ddl)
            return fresh

    def get_meta(self, key: str) -> Optional[str]:
        row = self.conn().execute('SELECT value FROM meta WHERE key = ?', (key,)).fetchone()
        return None if row is None else row['value']

    @staticmethod
    def set_meta(c: sqlite3.Connection, key: str, value: str) -> None:
        c.execute('INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)', (key, str(value)))

    @staticmethod
    def ack(c: sqlite3.Connection, consumer: str, slot: int, signature: str) -> None:
        """Record, inside the consumer's own transaction, that it has applied every event up to here."""
        c.execute(
            'INSERT OR REPLACE INTO acks (consumer, slot, signature) VALUES (?, ?, ?)', (consumer, int(slot), signature)
        )

    def resume_point(self, consumers: List[str]) -> Optional[str]:
        """The signature the feed resumes from: the oldest position among ``consumers``. None (from the
        horizon) if any of them has never acknowledged anything."""
        if not consumers:
            return None
        marks = ','.join('?' * len(consumers))
        rows = self.conn().execute(f'SELECT consumer, slot, signature FROM acks WHERE consumer IN ({marks})', consumers)
        acks = {r['consumer']: (r['slot'], r['signature']) for r in rows}
        if set(acks) != set(consumers):
            return None
        return min(acks.values())[1]
