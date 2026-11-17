"""DatabaseStorage reconnect-on-failure.

A slow-booting or restarted aw-db must pause dashboard writes, not disable them for the
process lifetime: a connection-class failure drops the connection and a later write lazily
reconnects (rate-limited). Writes stay non-fatal to the forward loop throughout.

``is_enabled()`` is the callers' gate and means "configured", not "connection live" — otherwise
the gates in forward.py/scoring.py would make the reconnect unreachable.

Mocks the psycopg layer entirely (psycopg isn't a test dependency): failures are raised as a
test-local ``OperationalError``, which ``is_connection_failure`` matches by class name.
"""

import contextlib

from allways.validator.export.storage import storage as storage_mod
from allways.validator.export.storage.storage import DatabaseStorage


class OperationalError(Exception):
    """Name-matched by is_connection_failure, same as psycopg's."""


class FakeConnection:
    def __init__(self):
        self.autocommit = True
        self.failing = False
        self.closed = False
        self.commits = 0

    def _maybe_fail(self):
        if self.failing:
            raise OperationalError('server closed the connection unexpectedly')

    def commit(self):
        self._maybe_fail()
        self.commits += 1

    def rollback(self):
        self._maybe_fail()

    def pipeline(self):
        self._maybe_fail()
        return contextlib.nullcontext()

    def close(self):
        self.closed = True


class FakeRepo:
    def __init__(self, conn):
        self.conn = conn

    def replace_current_crown(self, rows_by_direction, commit=False):
        self.conn._maybe_fail()
        return len(rows_by_direction)

    def replace_current_miner_scores(self, rows, commit=False):
        self.conn._maybe_fail()
        return len(rows)

    def store_direction_pools_bulk(self, rows, commit=False):
        self.conn._maybe_fail()
        return len(rows)

    def delete_crown_in_range(self, from_chain, to_chain, backing, lo, hi, commit=False):
        self.conn._maybe_fail()

    def trim_crown_tails(self, from_chain, to_chain, backing, at_ts, commit=False):
        self.conn._maybe_fail()

    def set_sync_cursor(self, key, value, commit=False):
        self.conn._maybe_fail()


def make_storage(monkeypatch, connections):
    """DatabaseStorage wired to pop successive connections from ``connections``
    (None = a failed connect attempt) instead of dialing Postgres."""
    monkeypatch.setenv('STORE_DB_RESULTS', '1')
    monkeypatch.setattr(storage_mod, 'create_database_connection', lambda: connections.pop(0))
    monkeypatch.setattr(storage_mod, 'Repository', FakeRepo)
    return DatabaseStorage()


def force_retry_window(storage):
    """Rewind the reconnect rate limiter so the next write is allowed to redial."""
    storage._last_reconnect_attempt = 0.0


def test_write_failure_does_not_raise_and_drops_connection(monkeypatch):
    conn = FakeConnection()
    storage = make_storage(monkeypatch, [conn])
    assert storage.is_enabled()

    conn.failing = True  # aw-db restarted: repo call AND the rollback both raise
    result = storage.upsert_current_crown_snapshot({})  # must not propagate into the forward loop
    assert not result.success and result.errors
    assert conn.closed and storage.db_connection is None
    assert storage.is_enabled()  # still configured — the callers' gate must stay open to reach the redial


def test_reconnects_on_next_write_after_drop(monkeypatch):
    dead, fresh = FakeConnection(), FakeConnection()
    storage = make_storage(monkeypatch, [dead, fresh])
    dead.failing = True
    assert not storage.upsert_current_crown_snapshot({}).success

    force_retry_window(storage)
    result = storage.upsert_current_crown_snapshot({('sol', 'btc', 'sol'): []})
    assert result.success and result.stored_counts['current_crown_holders'] == 1
    assert storage.db_connection is fresh and fresh.commits == 1


def test_boot_time_connect_failure_heals_on_later_write(monkeypatch):
    # The full-e2e boot-order landmine: aw-db up after the validator.
    fresh = FakeConnection()
    storage = make_storage(monkeypatch, [None, fresh])
    assert storage.is_enabled() and storage.db_connection is None  # configured, not yet connected

    force_retry_window(storage)
    assert storage.upsert_current_crown_snapshot({}).success
    assert storage.db_connection is fresh


def test_disabled_flag_never_dials(monkeypatch):
    monkeypatch.delenv('STORE_DB_RESULTS', raising=False)
    monkeypatch.setattr(storage_mod, 'create_database_connection', lambda: FakeConnection())
    storage = DatabaseStorage()
    assert not storage.is_enabled()
    assert not storage.upsert_current_crown_snapshot({}).success


def test_scoring_flush_trims_pre_window_tails(monkeypatch):
    """flush_scoring_window clamps intervals that run into the window from
    before its start. The started_at-keyed wipe can't reach them, and a
    surviving tail overlaps the recomputed rows and double-counts crown time
    (seen as /crown/time shareOfWindow > 1 after a validator restart, whose
    initial round anchors behind what the old process last flushed)."""
    conn = FakeConnection()
    storage = make_storage(monkeypatch, [conn])
    calls = []
    storage.repo.trim_crown_tails = lambda f, t, b, at, commit=False: calls.append(('trim', f, t, b, at))
    storage.repo.delete_crown_in_range = lambda f, t, b, lo, hi, commit=False: calls.append(('delete', f, t, b, lo, hi))
    storage.repo.store_crown_holders_bulk = lambda rows, commit=False: len(rows)
    storage.repo.store_miner_scores_bulk = lambda rows, commit=False: len(rows)
    lane = ('sol', 'btc', 'sol')
    result = storage.flush_scoring_window(
        crown_rows_by_direction={lane: []},
        crown_window_bounds_by_direction={lane: (1_000, 4_600)},
        miner_score_rows=[],
        crown_holders_max_ts=4_600,
    )
    assert result.success
    # Trim runs before the wipe, clamped at the same window edge the wipe keys on.
    assert calls == [('trim', 'sol', 'btc', 'sol', 1_000), ('delete', 'sol', 'btc', 'sol', 1_000, 4_600)]
