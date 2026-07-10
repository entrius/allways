"""Repository class wrapping the validator's dashboard-write queries.

Mirrors gittensor/gittensor/validator/storage/repository.py — bulk methods
take `commit=False` so an orchestrator can compose multiple writes inside a
single transaction.
"""

import logging
from contextlib import contextmanager
from typing import Dict, List, Tuple

from .queries import (
    BULK_UPSERT_CROWN_HOLDERS,
    BULK_UPSERT_CURRENT_CROWN_HOLDERS,
    BULK_UPSERT_RATE_HISTORY,
    DELETE_CROWN_IN_RANGE,
    DELETE_CURRENT_CROWN_BY_DIRECTION,
    SET_SYNC_CURSOR,
)


class BaseRepository:
    def __init__(self, db_connection):
        self.db = db_connection
        self.logger = logging.getLogger(self.__class__.__name__)

    @contextmanager
    def get_cursor(self):
        cursor = self.db.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    def execute_command(self, query: str, params: tuple = (), commit: bool = True) -> bool:
        try:
            with self.get_cursor() as cursor:
                cursor.execute(query, params)
                if commit:
                    self.db.commit()
                return True
        except Exception as e:
            if commit:
                self.db.rollback()
            self.logger.error(f'Error executing command: {e}')
            return False


class Repository(BaseRepository):
    """Bulk write methods for the validator's dashboard-write path."""

    def store_rate_history_bulk(
        self,
        rows: List[Tuple[str, str, str, float, int]],
        commit: bool = True,
    ) -> int:
        """Upsert rate quotes. Rows: (hotkey, from_chain, to_chain, rate, ts)."""
        if not rows:
            return 0
        try:
            with self.get_cursor() as cursor:
                cursor.executemany(BULK_UPSERT_RATE_HISTORY, rows)
                if commit:
                    self.db.commit()
                return len(rows)
        except Exception as e:
            if commit:
                self.db.rollback()
            self.logger.error(f'Error in bulk rate_history storage: {e}')
            return 0

    def delete_crown_in_range(
        self,
        from_chain: str,
        to_chain: str,
        lo_ts: int,
        hi_ts: int,
        commit: bool = True,
    ) -> bool:
        """Wipe crown_holders intervals starting in [lo_ts, hi_ts) for one direction."""
        if hi_ts <= lo_ts:
            return True
        return self.execute_command(
            DELETE_CROWN_IN_RANGE,
            (from_chain, to_chain, lo_ts, hi_ts),
            commit=commit,
        )

    def store_crown_holders_bulk(
        self,
        rows: List[Tuple[int, int, str, str, str, float, float]],
        commit: bool = True,
    ) -> int:
        """Upsert crown intervals. Rows: (started_at, ended_at, from_chain, to_chain, hotkey, credit, rate)."""
        if not rows:
            return 0
        try:
            with self.get_cursor() as cursor:
                cursor.executemany(BULK_UPSERT_CROWN_HOLDERS, rows)
                if commit:
                    self.db.commit()
                return len(rows)
        except Exception as e:
            if commit:
                self.db.rollback()
            self.logger.error(f'Error in bulk crown_holders storage: {e}')
            return 0

    def set_sync_cursor(self, name: str, value: int, commit: bool = True) -> bool:
        """Advance a named watermark. Commit in the same transaction as the
        data the cursor describes so partial writes don't desync the
        dashboard's freshness signal."""
        return self.execute_command(SET_SYNC_CURSOR, (name, value), commit=commit)

    def replace_current_crown(
        self,
        rows_by_direction: Dict[Tuple[str, str], List[Tuple[str, str, str, float, float, int]]],
        commit: bool = True,
    ) -> int:
        """Replace current_crown_holders rows for each given direction.
        Row tail int is the snapshot unix ts (was block).

        Per direction: delete all existing rows, then insert the supplied
        winners. Wrapped in one transaction so a tied k-way holder set is
        never partially visible. An empty row list for a direction clears
        it (no qualified holder at the current instant)."""
        if not rows_by_direction:
            return 0
        total = 0
        try:
            with self.get_cursor() as cursor:
                for (from_chain, to_chain), rows in rows_by_direction.items():
                    cursor.execute(DELETE_CURRENT_CROWN_BY_DIRECTION, (from_chain, to_chain))
                    if rows:
                        cursor.executemany(BULK_UPSERT_CURRENT_CROWN_HOLDERS, rows)
                        total += len(rows)
                if commit:
                    self.db.commit()
            return total
        except Exception as e:
            if commit:
                self.db.rollback()
            self.logger.error(f'Error in current_crown_holders replace: {e}')
            return 0
