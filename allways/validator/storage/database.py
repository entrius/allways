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
# reachable at validator boot, storage stays disabled for the lifetime of
# the process; caller (DatabaseStorage.__init__) logs the outcome and
# proceeds with writes disabled.
CONNECT_TIMEOUT_SEC = 2


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
