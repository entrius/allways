"""DatabaseStorage orchestrator — composes Repository writes into one txn.

Gated by the STORE_DB_RESULTS env var. When disabled (default), the
validator runs unchanged; the storage methods short-circuit to a
disabled-state result.
"""

import os
import time
from dataclasses import dataclass, field
from typing import Dict, List, Tuple

import bittensor as bt

from .database import create_database_connection, is_connection_failure
from .repository import Repository

# Floor between reconnect attempts after a dropped/failed connection. Each attempt can
# stall up to CONNECT_TIMEOUT_SEC, so a down aw-db costs one bounded stall per interval,
# not one per write. The warning is rate-limited by the same clock.
RECONNECT_MIN_INTERVAL_SEC = 30.0


@dataclass
class StorageResult:
    success: bool
    errors: List[str] = field(default_factory=list)
    stored_counts: Dict[str, int] = field(default_factory=dict)


def _flag_enabled() -> bool:
    return os.getenv('STORE_DB_RESULTS', '').lower() in ('1', 'true', 'yes')


class DatabaseStorage:
    """Single connection per validator instance; one transactional flush per
    scoring round. Caller decides what to pass in. A dead connection (aw-db
    restart, boot-order race) is dropped and lazily re-established on a later
    write — failures are logged, never raised into the forward loop.

    Methods accept already-shaped row tuples — the validator's scoring code
    is responsible for producing them, not this class. That keeps the
    storage layer ignorant of scoring internals.
    """

    def __init__(self):
        self.logger = bt.logging
        self.db_connection = None
        self.repo = None
        self._enabled = _flag_enabled()
        self._last_reconnect_attempt = 0.0

        if not self._enabled:
            bt.logging.info('STORE_DB_RESULTS not set — validator DB storage disabled')
            return

        bt.logging.info('STORE_DB_RESULTS=1 — connecting to Postgres for dashboard writes')
        if self._connect():
            bt.logging.success('Validator DB storage enabled')
        else:
            bt.logging.error(
                'STORE_DB_RESULTS=1 but Postgres connection failed — dashboard writes paused until reconnect'
            )

    def is_enabled(self) -> bool:
        """Storage is configured (STORE_DB_RESULTS set) — the callers' gate. Deliberately NOT
        "connection currently live": the write methods own connection state (drop + lazy
        redial), so a dead connection must still reach them or reconnect would be unreachable."""
        return self._enabled

    def _connected(self) -> bool:
        return self.db_connection is not None and self.repo is not None

    def _connect(self) -> bool:
        self.db_connection = create_database_connection()
        self.repo = Repository(self.db_connection) if self.db_connection is not None else None
        return self._connected()

    def _ensure_connection(self) -> bool:
        """Lazy reconnect for a dropped (or never-established) connection, at most once per
        RECONNECT_MIN_INTERVAL_SEC. Called at the top of every write so a restarted aw-db
        heals without a validator restart."""
        if not self._enabled:
            return False
        if self._connected():
            return True
        now = time.monotonic()
        if now - self._last_reconnect_attempt < RECONNECT_MIN_INTERVAL_SEC:
            return False
        self._last_reconnect_attempt = now
        bt.logging.warning('Dashboard DB connection is down — attempting reconnect')
        if self._connect():
            bt.logging.success('Dashboard DB connection re-established')
            return True
        return False

    def _drop_connection(self) -> None:
        try:
            if self.db_connection is not None:
                self.db_connection.close()
        except Exception:
            pass
        self.db_connection = None
        self.repo = None

    def _handle_write_failure(self, ex: Exception, context: str) -> str:
        """Best-effort rollback, then drop the connection on connection-class failures so the
        next write reconnects. Never raises — dashboard writes must not kill the forward loop."""
        if self.db_connection is not None:
            try:
                self.db_connection.rollback()
                self.db_connection.autocommit = True
            except Exception:
                pass
        if is_connection_failure(ex):
            self._drop_connection()
        msg = f'{context}: {ex}'
        self.logger.error(msg)
        return msg

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
        if not self._ensure_connection():
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
            result.success = False
            result.errors.append(self._handle_write_failure(ex, 'Failed to flush scoring window to DB'))

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
        if not self._ensure_connection():
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
            result.success = False
            result.errors.append(self._handle_write_failure(ex, 'Failed to flush halt window to DB'))

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
        if not self._ensure_connection():
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
            result.success = False
            result.errors.append(self._handle_write_failure(ex, 'Failed to upsert current_crown_holders'))

        return result

    def close(self):
        if self.db_connection:
            self.db_connection.close()
