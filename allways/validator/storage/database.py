"""Postgres connection factory for the validator's dashboard-write path."""

import os
from typing import Any, Optional

import bittensor as bt

try:
    import psycopg

    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False
    bt.logging.warning('psycopg not installed. Validator DB storage will be disabled.')


# Hard ceiling on any individual write — the dashboard is a non-critical
# side channel and must never stall the forward loop. Server-side
# statement_timeout cancels anything that blocks past this.
STATEMENT_TIMEOUT_MS = 2000
# Single connect attempt with the same 2s ceiling. If Postgres isn't
# reachable, this attempt fails fast; DatabaseStorage retries lazily on a
# later write (rate-limited), so a slow-booting or restarted aw-db only
# pauses dashboard writes instead of disabling them for the process.
CONNECT_TIMEOUT_SEC = 2

# psycopg exception class names that mean the connection itself is dead or unusable
# (server restart, network drop, closed connection) — DatabaseStorage drops and lazily
# reconnects on these. Matched by MRO class name rather than isinstance so the check
# (and its tests) work even where psycopg isn't importable.
_CONNECTION_ERROR_NAMES = frozenset({'OperationalError', 'InterfaceError'})


def is_connection_failure(ex: Exception) -> bool:
    return any(c.__name__ in _CONNECTION_ERROR_NAMES for c in type(ex).__mro__)


def create_database_connection() -> Optional[Any]:
    """Build a psycopg connection from DB_* env vars.

    Single attempt — no retry, no backoff. Returns the connection on
    success, None on any failure. Caller logs the outcome.
    """
    if not POSTGRES_AVAILABLE:
        bt.logging.error('psycopg not installed; cannot connect to Postgres')
        return None

    try:
        db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', 5432)),
            'user': os.getenv('DB_USERNAME', 'allways'),
            'password': os.getenv('DB_PASSWORD', 'allways'),
            'dbname': os.getenv('DB_NAME', 'allways'),
            'connect_timeout': CONNECT_TIMEOUT_SEC,
            'options': f'-c statement_timeout={STATEMENT_TIMEOUT_MS}',
        }

        connection = psycopg.connect(**db_config)
        connection.autocommit = False
        connection.prepare_threshold = 0
        return connection

    except Exception as e:
        bt.logging.error(f'Postgres connect failed: {e}')
        return None
