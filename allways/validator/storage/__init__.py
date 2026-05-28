"""Validator-side Postgres storage for dashboard data.

Writes the same `crown_holders` / `rate_history` / `sync_cursor` rows that
alw-utils/sync-validator-state writes today — but from inside the validator
during the scoring run, so there is no Python re-implementation of the
scoring walker to drift from the validator's truth.

Called from the validator's scoring run (`calculate_miner_rewards`) and the
per-forward live-crown snapshot; gated by `STORE_DB_RESULTS`.
"""

from .database import create_database_connection
from .repository import Repository
from .storage import DatabaseStorage, StorageResult

__all__ = ['create_database_connection', 'Repository', 'DatabaseStorage', 'StorageResult']
