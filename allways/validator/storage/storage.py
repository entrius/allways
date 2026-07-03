"""DatabaseStorage orchestrator — composes Repository writes into one txn.

Gated by the STORE_DB_RESULTS env var. When disabled (default), the
validator runs unchanged; the storage methods short-circuit to a
disabled-state result.
"""

import os
from dataclasses import dataclass, field
from typing import Dict, List, Tuple

import bittensor as bt

from .database import create_database_connection
from .repository import Repository


@dataclass
class StorageResult:
    success: bool
    errors: List[str] = field(default_factory=list)
    stored_counts: Dict[str, int] = field(default_factory=dict)


def _flag_enabled() -> bool:
    return os.getenv('STORE_DB_RESULTS', '').lower() in ('1', 'true', 'yes')


class DatabaseStorage:
    """Single connection per validator instance; one transactional flush per
    scoring round. Caller decides what to pass in.

    Methods accept already-shaped row tuples — the validator's scoring code
    is responsible for producing them, not this class. That keeps the
    storage layer ignorant of scoring internals.
    """

    def __init__(self):
        self.logger = bt.logging
        self.db_connection = None
        self.repo = None

        if not _flag_enabled():
            bt.logging.info('STORE_DB_RESULTS not set — validator DB storage disabled')
            return

        bt.logging.info('STORE_DB_RESULTS=1 — connecting to Postgres for dashboard writes')
        self.db_connection = create_database_connection()
        if self.db_connection is not None:
            self.repo = Repository(self.db_connection)
            bt.logging.success('Validator DB storage enabled')
        else:
            bt.logging.error(
                'STORE_DB_RESULTS=1 but Postgres connection failed — dashboard writes disabled for this process'
            )

    def is_enabled(self) -> bool:
        return self.db_connection is not None and self.repo is not None

    def flush_scoring_window(
        self,
        rate_rows: List[Tuple[str, str, str, float, int]],
        crown_rows_by_direction: Dict[Tuple[str, str], List[Tuple[int, int, str, str, str, float, float]]],
        crown_window_bounds_by_direction: Dict[Tuple[str, str], Tuple[int, int]],
        rate_snapshot_max_ts: int,
        crown_holders_max_ts: int,
    ) -> StorageResult:
        """All-or-nothing flush for one scoring window.

        - `rate_rows`: new rate quotes seen this round.
        - `crown_rows_by_direction`: recomputed crown rows, keyed by (from, to).
        - `crown_window_bounds_by_direction`: [lo, hi) unix-second range to wipe
          before re-upserting, keyed by (from, to). Must match the rows above.
        - The two `_max_ts` values advance the corresponding sync_cursor
          watermarks so the dashboard can render an "as-of <unix ts>" freshness
          signal.

        Failure on any write rolls back the whole window — the cursor is
        never left ahead of (or behind) the rows it describes.
        """
        if not self.is_enabled():
            return StorageResult(success=False, errors=['Validator DB storage not enabled'])

        result = StorageResult(success=True)

        try:
            assert self.db_connection is not None and self.repo is not None
            self.db_connection.autocommit = False

            with self.db_connection.pipeline():
                result.stored_counts['rate_history'] = self.repo.store_rate_history_bulk(rate_rows, commit=False)

                crown_inserted = 0
                for direction, rows in crown_rows_by_direction.items():
                    from_chain, to_chain = direction
                    lo, hi = crown_window_bounds_by_direction[direction]
                    self.repo.delete_crown_in_range(from_chain, to_chain, lo, hi, commit=False)
                    crown_inserted += self.repo.store_crown_holders_bulk(rows, commit=False)
                result.stored_counts['crown_holders'] = crown_inserted

                self.repo.set_sync_cursor('rate_snapshot_max_ts', rate_snapshot_max_ts, commit=False)
                self.repo.set_sync_cursor('crown_holders_max_ts', crown_holders_max_ts, commit=False)

            self.db_connection.commit()
            self.db_connection.autocommit = True

        except Exception as ex:
            if self.db_connection is not None:
                self.db_connection.rollback()
                self.db_connection.autocommit = True
            error_msg = f'Failed to flush scoring window to DB: {ex}'
            result.success = False
            result.errors.append(error_msg)
            self.logger.error(error_msg)

        return result

    def flush_halt_window(
        self,
        directions: List[Tuple[str, str]],
        window_start: int,
        window_end: int,
        max_ts: int,
    ) -> StorageResult:
        """Halt-aware counterpart to flush_scoring_window.

        During a halt, no miner earns crown and the pool recycles
        (see ``build_halted_rewards``). Mirror that on the dashboard by
        deleting any pre-existing crown_holders rows in the halted
        window and advancing the cursor — leaves no stale "current
        holder" implication on the historical grid. Rate events that
        fired during halt are *not* written: the daemon never wrote
        them either, and rate_history during a recycle has no scoring
        meaning.

        ``[window_start, window_end)`` is the unix-second range to clear per
        direction. ``max_ts`` advances both sync_cursor watermarks.
        """
        if not self.is_enabled():
            return StorageResult(success=False, errors=['Validator DB storage not enabled'])

        result = StorageResult(success=True)
        try:
            assert self.db_connection is not None and self.repo is not None
            self.db_connection.autocommit = False

            with self.db_connection.pipeline():
                for from_chain, to_chain in directions:
                    self.repo.delete_crown_in_range(from_chain, to_chain, window_start, window_end, commit=False)
                self.repo.set_sync_cursor('rate_snapshot_max_ts', max_ts, commit=False)
                self.repo.set_sync_cursor('crown_holders_max_ts', max_ts, commit=False)

            self.db_connection.commit()
            self.db_connection.autocommit = True

        except Exception as ex:
            if self.db_connection is not None:
                try:
                    self.db_connection.rollback()
                except Exception:
                    pass
                self.db_connection.autocommit = True
            error_msg = f'Failed to flush halt window to DB: {ex}'
            result.success = False
            result.errors.append(error_msg)
            self.logger.error(error_msg)

        return result

    def upsert_current_crown_snapshot(
        self,
        rows_by_direction: Dict[Tuple[str, str], List[Tuple[str, str, str, float, float, int]]],
    ) -> StorageResult:
        """Replace current_crown_holders rows for the given directions.

        Called per forward step (~12s) — the dashboard's live "who holds
        the crown right now" surface. Distinct cadence from
        ``flush_scoring_window``, which writes the historical interval
        ledger at round end (~1h).

        Row format per direction: ``(from_chain, to_chain, hotkey, credit,
        rate, ts)``. Empty list for a direction means "no qualifying
        holder right now" — that direction's rows are cleared.
        """
        if not self.is_enabled():
            return StorageResult(success=False, errors=['Validator DB storage not enabled'])

        result = StorageResult(success=True)
        try:
            assert self.db_connection is not None and self.repo is not None
            self.db_connection.autocommit = False
            count = self.repo.replace_current_crown(rows_by_direction, commit=False)
            self.db_connection.commit()
            self.db_connection.autocommit = True
            result.stored_counts['current_crown_holders'] = count
        except Exception as ex:
            if self.db_connection is not None:
                try:
                    self.db_connection.rollback()
                except Exception:
                    pass
                self.db_connection.autocommit = True
            error_msg = f'Failed to upsert current_crown_holders: {ex}'
            result.success = False
            result.errors.append(error_msg)
            self.logger.error(error_msg)

        return result

    def close(self):
        if self.db_connection:
            self.db_connection.close()
