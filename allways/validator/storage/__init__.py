"""Validator-side Postgres storage for dashboard data.

Writes the `crown_holders` / `miner_scores` ledgers, their live tips
(`current_crown_holders` / `current_miner_scores`), and the `sync_cursor`
watermark from inside the validator during the scoring run — so there is no
Python re-implementation of the scoring walker to drift from the validator's
truth. `rate_history` belongs to the indexer (real-time, per QuoteSet event).

Called from the validator's scoring run (`calculate_miner_rewards`) and the
per-forward live snapshots; gated by `STORE_DB_RESULTS`.
"""

from .database import create_database_connection
from .repository import Repository
from .storage import DatabaseStorage, StorageResult

__all__ = ['create_database_connection', 'Repository', 'DatabaseStorage', 'StorageResult']
