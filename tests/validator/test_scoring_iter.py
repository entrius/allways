"""Per-block crown replay iter tests — verifies that the iter wrapper and the
aggregate wrapper share the same underlying state machine."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from allways.validator.scoring import (
    _walk_replay_events,
    replay_crown_time_window,
    replay_crown_time_window_iter,
)
from allways.validator.state_store import ValidatorStateStore
from tests.test_scoring_v1 import make_watcher, seed_active


def _insert_rate(
    conn: sqlite3.Connection, hotkey: str, from_chain: str, to_chain: str, rate: float, block: int
) -> None:
    conn.execute(
        'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
        (hotkey, from_chain, to_chain, rate, block),
    )


class TestIterShape:
    def test_iter_yields_one_entry_per_block(self, tmp_path: Path):
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active={'hk_a'})
        _insert_rate(store.require_connection(), 'hk_a', 'btc', 'tao', 100.0, 0)
        store.require_connection().commit()

        records = list(
            replay_crown_time_window_iter(
                store=store,
                event_watcher=watcher,
                from_chain='btc',
                to_chain='tao',
                window_start=100,
                window_end=200,
                rewardable_hotkeys={'hk_a'},
            )
        )

        assert len(records) == 100
        assert [r[0] for r in records] == list(range(100, 200))
        assert all(r[1] == ['hk_a'] for r in records)
        assert all(r[2] == 100.0 for r in records)
        store.close()

    def test_iter_empty_window_yields_nothing(self, tmp_path: Path):
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active={'hk_a'})
        records = list(
            replay_crown_time_window_iter(
                store=store,
                event_watcher=watcher,
                from_chain='btc',
                to_chain='tao',
                window_start=500,
                window_end=500,
                rewardable_hotkeys={'hk_a'},
            )
        )
        assert records == []
        store.close()

    def test_iter_no_rewardable_emits_empty_holders_each_block(self, tmp_path: Path):
        """When no hotkey qualifies, every block still emits a record with
        empty holders — caller decides how to encode that (we rely on
        absence in Postgres, but the generator faithfully reports every
        block of the window)."""
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active=set())
        records = list(
            replay_crown_time_window_iter(
                store=store,
                event_watcher=watcher,
                from_chain='btc',
                to_chain='tao',
                window_start=100,
                window_end=110,
                rewardable_hotkeys=set(),
            )
        )
        assert len(records) == 10
        assert all(r[1] == [] for r in records)
        assert all(r[2] == 0.0 for r in records)
        store.close()


class TestIterAggregateParity:
    """SUM(credit) derived from iter must equal replay_crown_time_window totals
    bit-for-bit — the validator scoring path and the database mirror are not
    just close, they share the underlying ``_walk_replay_events`` generator."""

    def _credit_from_iter(self, records) -> dict[str, float]:
        credit: dict[str, float] = {}
        for _block, holders, _rate in records:
            if not holders:
                continue
            per = 1.0 / len(holders)
            for hk in holders:
                credit[hk] = credit.get(hk, 0.0) + per
        return credit

    def test_two_miners_alternate_leadership(self, tmp_path: Path):
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active={'hk_a', 'hk_b'})
        conn = store.require_connection()
        _insert_rate(conn, 'hk_a', 'btc', 'tao', 100.0, 0)
        _insert_rate(conn, 'hk_b', 'btc', 'tao', 200.0, 0)
        _insert_rate(conn, 'hk_a', 'btc', 'tao', 300.0, 600)
        conn.commit()

        kwargs = dict(
            store=store,
            event_watcher=watcher,
            from_chain='btc',
            to_chain='tao',
            window_start=100,
            window_end=1100,
            rewardable_hotkeys={'hk_a', 'hk_b'},
        )
        aggregate = replay_crown_time_window(**kwargs)
        from_iter = self._credit_from_iter(list(replay_crown_time_window_iter(**kwargs)))

        assert aggregate == from_iter == {'hk_a': 500.0, 'hk_b': 500.0}
        store.close()

    def test_tie_split_credit_matches(self, tmp_path: Path):
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active={'hk_a', 'hk_b'})
        conn = store.require_connection()
        _insert_rate(conn, 'hk_a', 'tao', 'btc', 0.00020, 0)
        _insert_rate(conn, 'hk_b', 'tao', 'btc', 0.00020, 0)
        conn.commit()

        kwargs = dict(
            store=store,
            event_watcher=watcher,
            from_chain='tao',
            to_chain='btc',
            window_start=100,
            window_end=1100,
            rewardable_hotkeys={'hk_a', 'hk_b'},
        )
        aggregate = replay_crown_time_window(**kwargs)
        from_iter = self._credit_from_iter(list(replay_crown_time_window_iter(**kwargs)))
        assert aggregate == from_iter == {'hk_a': 500.0, 'hk_b': 500.0}

        # Per-block, both miners are listed as holders with rate 0.00020.
        records = list(replay_crown_time_window_iter(**kwargs))
        assert all(set(r[1]) == {'hk_a', 'hk_b'} for r in records)
        store.close()

    def test_busy_window_runner_up_matches(self, tmp_path: Path):
        """A drops crown to B for [400, 800] — both aggregate and iter agree
        on the credit split."""
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active={'hk_a', 'hk_b'})
        conn = store.require_connection()
        _insert_rate(conn, 'hk_a', 'btc', 'tao', 300.0, 0)
        _insert_rate(conn, 'hk_b', 'btc', 'tao', 200.0, 0)
        conn.commit()
        watcher.apply_event(400, 'SwapInitiated', {'swap_id': 1, 'miner': 'hk_a'})
        watcher.apply_event(800, 'SwapCompleted', {'swap_id': 1, 'miner': 'hk_a'})

        kwargs = dict(
            store=store,
            event_watcher=watcher,
            from_chain='btc',
            to_chain='tao',
            window_start=100,
            window_end=1100,
            rewardable_hotkeys={'hk_a', 'hk_b'},
        )
        aggregate = replay_crown_time_window(**kwargs)
        from_iter = self._credit_from_iter(list(replay_crown_time_window_iter(**kwargs)))
        # A held [100, 400] + [800, 1100] = 600 blocks; B held [400, 800] = 400.
        assert aggregate == from_iter == {'hk_a': 600.0, 'hk_b': 400.0}
        store.close()


class TestNoHolderEncoding:
    def test_all_busy_emits_empty_holders(self, tmp_path: Path):
        """When the best-rate miner is busy and no runner-up qualifies, iter
        emits empty-holder records — the sync utility translates those to
        "no rows for this block" in the Postgres mirror."""
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active={'hk_a'})
        conn = store.require_connection()
        _insert_rate(conn, 'hk_a', 'btc', 'tao', 300.0, 0)
        conn.commit()
        watcher.apply_event(400, 'SwapInitiated', {'swap_id': 1, 'miner': 'hk_a'})
        watcher.apply_event(800, 'SwapCompleted', {'swap_id': 1, 'miner': 'hk_a'})

        records = list(
            replay_crown_time_window_iter(
                store=store,
                event_watcher=watcher,
                from_chain='btc',
                to_chain='tao',
                window_start=100,
                window_end=1100,
                rewardable_hotkeys={'hk_a'},
            )
        )
        busy_block = next(r for r in records if r[0] == 500)
        assert busy_block == (500, [], 0.0)
        held_block = next(r for r in records if r[0] == 300)
        assert held_block == (300, ['hk_a'], 300.0)
        store.close()

    def test_active_flag_off_emits_empty_holders(self, tmp_path: Path):
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active={'hk_a'})
        conn = store.require_connection()
        _insert_rate(conn, 'hk_a', 'btc', 'tao', 300.0, 0)
        conn.commit()
        # MinerActivated(active=False) at block 500
        seed_active(watcher, 'hk_a', active=False, block=500)

        records = list(
            replay_crown_time_window_iter(
                store=store,
                event_watcher=watcher,
                from_chain='btc',
                to_chain='tao',
                window_start=100,
                window_end=1000,
                rewardable_hotkeys={'hk_a'},
            )
        )
        # Pre-deactivation block held; post-deactivation block does not.
        assert next(r for r in records if r[0] == 400) == (400, ['hk_a'], 300.0)
        assert next(r for r in records if r[0] == 600) == (600, [], 0.0)
        store.close()


class TestWalkReplayEventsIntervalShape:
    def test_intervals_partition_the_window(self, tmp_path: Path):
        """The merged walker emits non-overlapping intervals that cover
        ``[window_start, window_end)`` exactly."""
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active={'hk_a', 'hk_b'})
        conn = store.require_connection()
        _insert_rate(conn, 'hk_a', 'btc', 'tao', 100.0, 0)
        _insert_rate(conn, 'hk_b', 'btc', 'tao', 200.0, 0)
        _insert_rate(conn, 'hk_a', 'btc', 'tao', 300.0, 400)
        conn.commit()

        intervals = list(
            _walk_replay_events(
                store=store,
                event_watcher=watcher,
                from_chain='btc',
                to_chain='tao',
                window_start=100,
                window_end=1000,
                rewardable_hotkeys={'hk_a', 'hk_b'},
            )
        )
        assert intervals[0][0] == 100
        assert intervals[-1][1] == 1000
        for prev, nxt in zip(intervals, intervals[1:]):
            assert prev[1] == nxt[0]
        store.close()


class TestReadOnlyMode:
    def test_readonly_can_read_existing_db(self, tmp_path: Path):
        writer = ValidatorStateStore(db_path=tmp_path / 'state.db')
        _insert_rate(writer.require_connection(), 'hk_a', 'btc', 'tao', 123.0, 7)
        writer.require_connection().commit()
        writer.close()

        reader = ValidatorStateStore(db_path=tmp_path / 'state.db', readonly=True)
        rows = reader.get_rate_events_since_id(0)
        assert len(rows) == 1
        assert rows[0]['rate'] == 123.0
        assert rows[0]['hotkey'] == 'hk_a'
        reader.close()

    def test_readonly_blocks_writes(self, tmp_path: Path):
        writer = ValidatorStateStore(db_path=tmp_path / 'state.db')
        writer.close()

        reader = ValidatorStateStore(db_path=tmp_path / 'state.db', readonly=True)
        with pytest.raises(sqlite3.OperationalError):
            _insert_rate(reader.require_connection(), 'hk_a', 'btc', 'tao', 1.0, 1)
            reader.require_connection().commit()
        reader.close()
