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


def create_database_connection() -> Optional[Any]:
    """Build a psycopg connection from DB_* env vars.

    Env var names mirror alw-utils/sync-validator-state so validators that
    already have a `.env` for that daemon can flip STORE_DB_RESULTS=true
    without renaming anything.
    """
    if not POSTGRES_AVAILABLE:
        bt.logging.error('Cannot create database connection: psycopg not installed')
        return None

    try:
        db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', 5432)),
            'user': os.getenv('DB_USERNAME', 'allways'),
            'password': os.getenv('DB_PASSWORD', 'allways'),
            'dbname': os.getenv('DB_NAME', 'allways'),
        }

        connection = psycopg.connect(**db_config)
        connection.autocommit = False
        connection.prepare_threshold = 0
        bt.logging.success('Connected to Postgres for validator dashboard storage')
        return connection

    except psycopg.Error as e:
        bt.logging.error(f'Failed to connect to database: {e}')
        return None
    except Exception as e:
        bt.logging.error(f'Unexpected error connecting to database: {e}')
        return None
