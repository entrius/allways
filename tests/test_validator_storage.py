"""TODO-14 — DatabaseStorage reconnect-on-failure.

A slow-booting or restarted aw-db must pause dashboard writes, not disable them for the
process lifetime: a connection-class failure drops the connection and a later write lazily
reconnects (rate-limited). Writes stay non-fatal to the forward loop throughout.

Mocks the psycopg layer entirely (psycopg isn't a test dependency): failures are raised as a
test-local ``OperationalError``, which ``is_connection_failure`` matches by class name.
"""

from allways.validator.storage import storage as storage_mod
from allways.validator.storage.database import is_connection_failure
from allways.validator.storage.storage import DatabaseStorage


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

    def close(self):
        self.closed = True


class FakeRepo:
    def __init__(self, conn):
        self.conn = conn

    def replace_current_crown(self, rows_by_direction, commit=False):
        self.conn._maybe_fail()
        return len(rows_by_direction)


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


def test_is_connection_failure_matches_by_class_name():
    assert is_connection_failure(OperationalError('boom'))
    assert not is_connection_failure(ValueError('boom'))


def test_write_failure_does_not_raise_and_drops_connection(monkeypatch):
    conn = FakeConnection()
    storage = make_storage(monkeypatch, [conn])
    assert storage.is_enabled()

    conn.failing = True  # aw-db restarted: repo call AND the rollback both raise
    result = storage.upsert_current_crown_snapshot({})  # must not propagate into the forward loop
    assert not result.success and result.errors
    assert conn.closed and not storage.is_enabled()


def test_reconnects_on_next_write_after_drop(monkeypatch):
    dead, fresh = FakeConnection(), FakeConnection()
    storage = make_storage(monkeypatch, [dead, fresh])
    dead.failing = True
    assert not storage.upsert_current_crown_snapshot({}).success

    force_retry_window(storage)
    result = storage.upsert_current_crown_snapshot({('sol', 'btc'): []})
    assert result.success and result.stored_counts['current_crown_holders'] == 1
    assert storage.db_connection is fresh and fresh.commits == 1


def test_reconnect_attempts_are_rate_limited(monkeypatch):
    dead = FakeConnection()
    connections = [dead, None]  # a third pop would raise IndexError — proves no extra redial
    storage = make_storage(monkeypatch, connections)
    dead.failing = True
    assert not storage.upsert_current_crown_snapshot({}).success  # drops the connection
    assert not storage.upsert_current_crown_snapshot({}).success  # first redial (fails: None)

    # Within the rate-limit window after a failed redial: degrades to not-enabled without dialing.
    result = storage.upsert_current_crown_snapshot({})
    assert not result.success and 'not enabled' in result.errors[0]


def test_boot_time_connect_failure_heals_on_later_write(monkeypatch):
    # The full-e2e boot-order landmine: aw-db up after the validator.
    fresh = FakeConnection()
    storage = make_storage(monkeypatch, [None, fresh])
    assert not storage.is_enabled()

    force_retry_window(storage)
    assert storage.upsert_current_crown_snapshot({}).success
    assert storage.is_enabled() and storage.db_connection is fresh


def test_non_connection_failure_keeps_connection(monkeypatch):
    conn = FakeConnection()
    storage = make_storage(monkeypatch, [conn])

    def raise_value_error(rows_by_direction, commit=False):
        raise ValueError('bad row shape')

    storage.repo.replace_current_crown = raise_value_error
    result = storage.upsert_current_crown_snapshot({})
    assert not result.success
    assert storage.is_enabled() and storage.db_connection is conn  # only connection-class errors drop


def test_disabled_flag_never_dials(monkeypatch):
    monkeypatch.delenv('STORE_DB_RESULTS', raising=False)
    monkeypatch.setattr(storage_mod, 'create_database_connection', lambda: FakeConnection())
    storage = DatabaseStorage()
    assert not storage.is_enabled()
    assert not storage.upsert_current_crown_snapshot({}).success
