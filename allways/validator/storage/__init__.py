"""Validator-side Postgres storage for dashboard data.

Writes the same `crown_holders` / `rate_history` / `sync_cursor` rows that
alw-utils/sync-validator-state writes today — but from inside the validator
during the scoring run, so there is no Python re-implementation of the
scoring walker to drift from the validator's truth.

Self-contained: nothing here is imported by scoring or forward yet. The
call site is wired up in a follow-up.
"""

from .database import create_database_connection
from .repository import Repository
from .storage import DatabaseStorage, StorageResult

__all__ = ['create_database_connection', 'Repository', 'DatabaseStorage', 'StorageResult']
