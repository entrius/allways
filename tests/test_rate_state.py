import threading
from pathlib import Path

import pytest

from allways.validator.state_store import ValidatorStateStore


def make_store(tmp_path: Path) -> ValidatorStateStore:
    return ValidatorStateStore(db_path=tmp_path / 'state.db')


class TestValidatorStateStoreSchema:
    def test_init_creates_all_tables_and_indexes(self, tmp_path: Path):
        store = make_store(tmp_path)
        conn = store.require_connection()

        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        assert {'rate_events', 'active_events', 'activity_events', 'collateral_events'}.issubset(tables)

        indexes = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        }
        assert 'idx_rate_events_block' in indexes
        assert 'idx_rate_events_dir_block' in indexes
        assert 'idx_rate_events_hotkey' in indexes

        store.close()


class TestInsertRateEvent:
    def test_first_event_accepted(self, tmp_path: Path):
        store = make_store(tmp_path)
        assert store.insert_rate_event('hk1', 'tao', 'btc', 0.00015, block=100) is True
        store.close()

    def test_rate_change_next_block_is_accepted(self, tmp_path: Path):
        """No throttle — a rate change one block later lands immediately."""
        store = make_store(tmp_path)
        assert store.insert_rate_event('hk1', 'tao', 'btc', 0.00015, block=100) is True
        assert store.insert_rate_event('hk1', 'tao', 'btc', 0.00016, block=101) is True
        events = store.get_rate_events_in_range('tao', 'btc', start_block=99, end_block=200)
        assert [e['rate'] for e in events] == [0.00015, 0.00016]
        store.close()

    def test_rejected_when_rate_unchanged(self, tmp_path: Path):
        store = make_store(tmp_path)
        assert store.insert_rate_event('hk1', 'tao', 'btc', 0.00015, block=100) is True
        assert store.insert_rate_event('hk1', 'tao', 'btc', 0.00015, block=200) is False
        store.close()

    def test_accepted_when_rate_changes(self, tmp_path: Path):
        store = make_store(tmp_path)
        assert store.insert_rate_event('hk1', 'tao', 'btc', 0.00015, block=100) is True
        assert store.insert_rate_event('hk1', 'tao', 'btc', 0.00020, block=200) is True
        events = store.get_rate_events_in_range('tao', 'btc', start_block=99, end_block=300)
        assert len(events) == 2
        store.close()

    def test_direction_isolation(self, tmp_path: Path):
        """Dedupe is per (hotkey, from, to) — different directions don't conflict."""
        store = make_store(tmp_path)
        assert store.insert_rate_event('hk1', 'tao', 'btc', 0.00015, block=100) is True
        # Same hotkey, other direction — same-rate dedupe only checks its own direction
        assert store.insert_rate_event('hk1', 'btc', 'tao', 6500.0, block=105) is True
        store.close()


class TestGetLatestRateBefore:
    def test_returns_none_when_empty(self, tmp_path: Path):
        store = make_store(tmp_path)
        assert store.get_latest_rate_before('hk1', 'tao', 'btc', block=100) is None
        store.close()

    def test_returns_most_recent_at_or_before(self, tmp_path: Path):
        store = make_store(tmp_path)
        store.insert_rate_event('hk1', 'tao', 'btc', 0.00015, block=100)
        store.insert_rate_event('hk1', 'tao', 'btc', 0.00020, block=300)
        assert store.get_latest_rate_before('hk1', 'tao', 'btc', block=250) == (0.00015, 100)
        assert store.get_latest_rate_before('hk1', 'tao', 'btc', block=300) == (0.00020, 300)
        store.close()


class TestGetRateEventsInRange:
    def test_boundary_exclusive_start_inclusive_end(self, tmp_path: Path):
        store = make_store(tmp_path)
        # Insert 3 distinct events at different blocks (use distinct rates + past throttle).
        store.insert_rate_event('hk1', 'tao', 'btc', 0.00010, block=100)
        store.insert_rate_event('hk1', 'tao', 'btc', 0.00020, block=200)
        store.insert_rate_event('hk1', 'tao', 'btc', 0.00030, block=300)

        events = store.get_rate_events_in_range('tao', 'btc', start_block=100, end_block=300)
        # block > 100 AND block <= 300 → blocks 200 and 300
        assert [e['block'] for e in events] == [200, 300]
        store.close()

    def test_filters_by_direction(self, tmp_path: Path):
        store = make_store(tmp_path)
        store.insert_rate_event('hk1', 'tao', 'btc', 0.00015, block=100)
        store.insert_rate_event('hk1', 'btc', 'tao', 6500.0, block=100)

        tao_btc = store.get_rate_events_in_range('tao', 'btc', 0, 200)
        btc_tao = store.get_rate_events_in_range('btc', 'tao', 0, 200)

        assert len(tao_btc) == 1 and tao_btc[0]['rate'] == 0.00015
        assert len(btc_tao) == 1 and btc_tao[0]['rate'] == 6500.0
        store.close()


class TestDeleteHotkey:
    def test_removes_rate_events_for_hotkey(self, tmp_path: Path):
        store = make_store(tmp_path)
        store.insert_rate_event('hk1', 'tao', 'btc', 0.00015, block=100)
        store.insert_rate_event('hk2', 'tao', 'btc', 0.00016, block=100)

        store.delete_hotkey('hk1')

        assert store.get_latest_rate_before('hk1', 'tao', 'btc', block=200) is None
        # hk2 untouched
        assert store.get_latest_rate_before('hk2', 'tao', 'btc', block=200) is not None
        store.close()


class TestPrune:
    def test_prune_preserves_latest_row_per_direction(self, tmp_path: Path):
        """A miner's single rate row must survive even when it's older than
        the cutoff — otherwise get_latest_rate_before at window_start would
        find nothing and the miner falls out of scoring entirely."""
        store = make_store(tmp_path)
        store.insert_rate_event('hk1', 'tao', 'btc', 0.00015, block=100)

        # Cutoff is way past block 100, but the row is the only anchor.
        store.prune_events_older_than(cutoff_block=5_000)

        assert store.get_latest_rate_before('hk1', 'tao', 'btc', block=10_000) == (0.00015, 100)
        store.close()

    def test_prune_drops_older_rows_when_newer_exists(self, tmp_path: Path):
        """When a direction has multiple rows, rows older than the cutoff
        get pruned as long as a newer row survives as the anchor."""
        store = make_store(tmp_path)
        store.insert_rate_event('hk1', 'tao', 'btc', 0.00010, block=100)
        store.insert_rate_event('hk1', 'tao', 'btc', 0.00020, block=200)
        store.insert_rate_event('hk1', 'tao', 'btc', 0.00030, block=6_000)

        store.prune_events_older_than(cutoff_block=5_000)

        # blocks 100 and 200 drop; block 6000 survives.
        events = store.get_rate_events_in_range('tao', 'btc', start_block=0, end_block=10_000)
        assert [e['block'] for e in events] == [6_000]
        store.close()

    def test_prune_preserves_latest_per_direction_independently(self, tmp_path: Path):
        """Preservation is keyed on (hotkey, from_chain, to_chain) — each
        direction keeps its own anchor row."""
        store = make_store(tmp_path)
        store.insert_rate_event('hk1', 'tao', 'btc', 0.00015, block=100)
        store.insert_rate_event('hk1', 'btc', 'tao', 6500.0, block=100)

        store.prune_events_older_than(cutoff_block=5_000)

        assert store.get_latest_rate_before('hk1', 'tao', 'btc', block=10_000) == (0.00015, 100)
        assert store.get_latest_rate_before('hk1', 'btc', 'tao', block=10_000) == (6500.0, 100)
        store.close()


class TestConcurrency:
    def test_concurrent_writes_threadsafe(self, tmp_path: Path):
        store = make_store(tmp_path)
        errors: list[Exception] = []

        def writer(thread_idx: int):
            try:
                for i in range(100):
                    # active_events has no dedup, so every write lands a row —
                    # a clean count of concurrent inserts.
                    store.insert_active_event(1000 + i, f'hk{thread_idx}', bool(i % 2))
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=writer, args=(i,)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []

        conn = store.require_connection()
        count = conn.execute('SELECT COUNT(*) FROM active_events').fetchone()[0]
        assert count == 400
        store.close()


class TestClose:
    def test_close_is_idempotent_and_blocks_further_ops(self, tmp_path: Path):
        store = make_store(tmp_path)
        store.insert_rate_event('hk1', 'tao', 'btc', 0.00015, block=100)

        store.close()
        store.close()  # second close is a no-op

        with pytest.raises(RuntimeError):
            store.insert_rate_event('hk1', 'tao', 'btc', 0.00020, block=200)
