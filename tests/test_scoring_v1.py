"""C5 — crown-time scoring replay tests."""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import numpy as np

from allways.constants import RECYCLE_UID, SUCCESS_EXPONENT
from allways.validator.event_watcher import CollateralEvent, ContractEventWatcher
from allways.validator.forward import (
    _success_rate,
    calculate_miner_rewards,
    crown_holders_at_instant,
    replay_crown_time_window,
)
from allways.validator.state_store import ValidatorStateStore

POOL_TAO_BTC = 0.04
POOL_BTC_TAO = 0.04
MIN_COLLATERAL = 100_000_000  # 0.1 TAO

METADATA_PATH = Path(__file__).parent.parent / 'allways' / 'metadata' / 'allways_swap_manager.json'


def _make_metagraph(hotkeys: list[str]) -> SimpleNamespace:
    n = SimpleNamespace(item=lambda: len(hotkeys))
    return SimpleNamespace(n=n, hotkeys=list(hotkeys))


def _make_watcher(store: ValidatorStateStore, active: set[str]) -> ContractEventWatcher:
    w = ContractEventWatcher(
        substrate=MagicMock(),
        contract_address='5contract',
        metadata_path=METADATA_PATH,
        state_store=store,
        default_min_collateral=MIN_COLLATERAL,
    )
    w.min_collateral = MIN_COLLATERAL
    w.active_miners = set(active)
    return w


def _seed_collateral(watcher: ContractEventWatcher, hotkey: str, collateral_rao: int, block: int) -> None:
    """Insert a collateral event directly into the watcher's in-memory state."""
    watcher.collateral[hotkey] = collateral_rao
    watcher.collateral_events.append(CollateralEvent(hotkey=hotkey, collateral_rao=collateral_rao, block=block))


def _make_validator(tmp_path: Path, hotkeys: list[str], block: int = 10_000) -> SimpleNamespace:
    store = ValidatorStateStore(db_path=tmp_path / 'state.db')
    watcher = _make_watcher(store, active=set(hotkeys))
    return SimpleNamespace(
        block=block,
        metagraph=_make_metagraph(hotkeys),
        state_store=store,
        event_watcher=watcher,
    )


def _pad_hotkeys_to_cover_recycle(seeds: list[str]) -> list[str]:
    """Ensure the metagraph is large enough that RECYCLE_UID is in-bounds."""
    hotkeys = list(seeds)
    while len(hotkeys) <= RECYCLE_UID:
        hotkeys.append(f'hk_filler_{len(hotkeys)}')
    return hotkeys


class TestSuccessRateHelper:
    def test_none_is_optimistic(self):
        assert _success_rate(None) == 1.0

    def test_zero_total_is_optimistic(self):
        assert _success_rate((0, 0)) == 1.0

    def test_ratio_is_completed_over_total(self):
        assert _success_rate((8, 2)) == 0.8


class TestCrownHoldersHelper:
    def test_excludes_rate_zero(self):
        rates = {'a': 0.0, 'b': 0.00015}
        collaterals = {'a': MIN_COLLATERAL, 'b': MIN_COLLATERAL}
        assert crown_holders_at_instant(rates, collaterals, MIN_COLLATERAL, {'a', 'b'}) == ['b']

    def test_excludes_below_min_collateral(self):
        rates = {'a': 0.00020, 'b': 0.00015}
        collaterals = {'a': MIN_COLLATERAL - 1, 'b': MIN_COLLATERAL}
        assert crown_holders_at_instant(rates, collaterals, MIN_COLLATERAL, {'a', 'b'}) == ['b']

    def test_excludes_not_eligible(self):
        rates = {'a': 0.00020, 'b': 0.00015}
        collaterals = {'a': MIN_COLLATERAL, 'b': MIN_COLLATERAL}
        assert crown_holders_at_instant(rates, collaterals, MIN_COLLATERAL, {'b'}) == ['b']

    def test_tied_best_rate_returns_all(self):
        rates = {'a': 0.00020, 'b': 0.00020}
        collaterals = {'a': MIN_COLLATERAL, 'b': MIN_COLLATERAL}
        holders = set(crown_holders_at_instant(rates, collaterals, MIN_COLLATERAL, {'a', 'b'}))
        assert holders == {'a', 'b'}


class TestReplayCrownTime:
    def test_single_miner_holds_full_window(self, tmp_path: Path):
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = _make_watcher(store, active={'hk_a'})
        conn = store._require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_a', 'tao', 'btc', 0.00015, 0),
        )
        conn.commit()
        _seed_collateral(watcher, 'hk_a', MIN_COLLATERAL, 0)

        crown = replay_crown_time_window(
            store=store,
            event_watcher=watcher,
            from_chain='tao',
            to_chain='btc',
            window_start=100,
            window_end=1100,
            eligible_hotkeys={'hk_a'},
            min_collateral=MIN_COLLATERAL,
        )
        assert crown == {'hk_a': 1000.0}
        store.close()

    def test_two_miners_alternate_rate_leadership(self, tmp_path: Path):
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = _make_watcher(store, active={'hk_a', 'hk_b'})
        conn = store._require_connection()
        for row in (
            ('hk_a', 'tao', 'btc', 0.00010, 0),
            ('hk_b', 'tao', 'btc', 0.00020, 0),
        ):
            conn.execute(
                'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
                row,
            )
        # Mid-window, A jumps to the top.
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_a', 'tao', 'btc', 0.00030, 600),
        )
        conn.commit()
        _seed_collateral(watcher, 'hk_a', MIN_COLLATERAL, 0)
        _seed_collateral(watcher, 'hk_b', MIN_COLLATERAL, 0)

        crown = replay_crown_time_window(
            store=store,
            event_watcher=watcher,
            from_chain='tao',
            to_chain='btc',
            window_start=100,
            window_end=1100,
            eligible_hotkeys={'hk_a', 'hk_b'},
            min_collateral=MIN_COLLATERAL,
        )
        # B leads blocks (100, 600] → 500 blocks, A leads (600, 1100] → 500 blocks
        assert crown == {'hk_b': 500.0, 'hk_a': 500.0}
        store.close()

    def test_tie_splits_credit_evenly(self, tmp_path: Path):
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = _make_watcher(store, active={'hk_a', 'hk_b'})
        conn = store._require_connection()
        for hk in ('hk_a', 'hk_b'):
            conn.execute(
                'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
                (hk, 'tao', 'btc', 0.00020, 0),
            )
            _seed_collateral(watcher, hk, MIN_COLLATERAL, 0)
        conn.commit()

        crown = replay_crown_time_window(
            store=store,
            event_watcher=watcher,
            from_chain='tao',
            to_chain='btc',
            window_start=100,
            window_end=1100,
            eligible_hotkeys={'hk_a', 'hk_b'},
            min_collateral=MIN_COLLATERAL,
        )
        assert crown == {'hk_a': 500.0, 'hk_b': 500.0}
        store.close()

    def test_collateral_drop_mid_window_forfeits_remaining_interval(self, tmp_path: Path):
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = _make_watcher(store, active={'hk_a'})
        conn = store._require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_a', 'tao', 'btc', 0.00020, 0),
        )
        conn.commit()
        # Initial collateral at block 0, drop at block 600
        _seed_collateral(watcher, 'hk_a', MIN_COLLATERAL, 0)
        _seed_collateral(watcher, 'hk_a', MIN_COLLATERAL - 1, 600)

        crown = replay_crown_time_window(
            store=store,
            event_watcher=watcher,
            from_chain='tao',
            to_chain='btc',
            window_start=100,
            window_end=1100,
            eligible_hotkeys={'hk_a'},
            min_collateral=MIN_COLLATERAL,
        )
        assert crown == {'hk_a': 500.0}
        store.close()

    def test_window_start_state_reconstruction_from_pre_window_events(self, tmp_path: Path):
        """A miner posted before window_start and never updated — replay reads initial state."""
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = _make_watcher(store, active={'hk_a'})
        conn = store._require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_a', 'tao', 'btc', 0.00020, 5_000),
        )
        conn.commit()
        _seed_collateral(watcher, 'hk_a', MIN_COLLATERAL, 5_000)

        crown = replay_crown_time_window(
            store=store,
            event_watcher=watcher,
            from_chain='tao',
            to_chain='btc',
            window_start=10_000,
            window_end=11_000,
            eligible_hotkeys={'hk_a'},
            min_collateral=MIN_COLLATERAL,
        )
        assert crown == {'hk_a': 1000.0}
        store.close()


class TestCalculateMinerRewards:
    def test_empty_direction_recycles_full_pool(self, tmp_path: Path):
        hotkeys = _pad_hotkeys_to_cover_recycle(['hk_a'])
        v = _make_validator(tmp_path, hotkeys=hotkeys)

        rewards, uids = calculate_miner_rewards(v)

        assert set(uids) == set(range(len(hotkeys)))
        assert rewards[RECYCLE_UID] == 1.0
        assert rewards[0] == 0.0
        np.testing.assert_allclose(rewards.sum(), 1.0, atol=1e-6)
        v.state_store.close()

    def test_single_miner_full_pool_with_perfect_success(self, tmp_path: Path):
        hotkeys = _pad_hotkeys_to_cover_recycle(['hk_a'])
        v = _make_validator(tmp_path, hotkeys=hotkeys)
        conn = v.state_store._require_connection()
        for direction in (('tao', 'btc'), ('btc', 'tao')):
            conn.execute(
                'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
                ('hk_a', direction[0], direction[1], 0.00020, 0),
            )
        conn.commit()
        _seed_collateral(v.event_watcher, 'hk_a', MIN_COLLATERAL, 0)
        v.state_store.insert_swap_outcome(swap_id=1, miner_hotkey='hk_a', completed=True, resolved_block=100)

        rewards, _ = calculate_miner_rewards(v)

        np.testing.assert_allclose(rewards[0], POOL_TAO_BTC + POOL_BTC_TAO, atol=1e-6)
        np.testing.assert_allclose(rewards.sum(), 1.0, atol=1e-6)
        v.state_store.close()

    def test_partial_success_reduces_reward_by_cube(self, tmp_path: Path):
        hotkeys = _pad_hotkeys_to_cover_recycle(['hk_a'])
        v = _make_validator(tmp_path, hotkeys=hotkeys)
        conn = v.state_store._require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_a', 'tao', 'btc', 0.00020, 0),
        )
        conn.commit()
        _seed_collateral(v.event_watcher, 'hk_a', MIN_COLLATERAL, 0)
        for i in range(8):
            v.state_store.insert_swap_outcome(i + 1, 'hk_a', True, 100 + i)
        for i in range(2):
            v.state_store.insert_swap_outcome(100 + i, 'hk_a', False, 200 + i)

        rewards, _ = calculate_miner_rewards(v)

        expected = POOL_TAO_BTC * (0.8**SUCCESS_EXPONENT)
        np.testing.assert_allclose(rewards[0], expected, atol=1e-6)
        np.testing.assert_allclose(rewards.sum(), 1.0, atol=1e-6)
        v.state_store.close()

    def test_dereg_mid_window_forfeits_credit(self, tmp_path: Path):
        # hk_a was the best rate miner but is no longer in the metagraph
        hotkeys = _pad_hotkeys_to_cover_recycle(['hk_b'])
        v = _make_validator(tmp_path, hotkeys=hotkeys)
        # Ensure hk_a is still marked active on the watcher even if out of metagraph
        v.event_watcher.active_miners.add('hk_a')
        conn = v.state_store._require_connection()
        for hk, rate in (('hk_a', 0.00030), ('hk_b', 0.00020)):
            conn.execute(
                'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
                (hk, 'tao', 'btc', rate, 0),
            )
            _seed_collateral(v.event_watcher, hk, MIN_COLLATERAL, 0)
        conn.commit()

        rewards, _ = calculate_miner_rewards(v)

        # hk_a isn't in metagraph so hk_b (uid 0) becomes the crown holder.
        np.testing.assert_allclose(rewards[0], POOL_TAO_BTC, atol=1e-6)
        v.state_store.close()

    def test_recycle_uid_out_of_bounds_falls_back_to_zero(self, tmp_path: Path):
        hotkeys = ['hk_a', 'hk_b']
        v = _make_validator(tmp_path, hotkeys=hotkeys)

        rewards, _ = calculate_miner_rewards(v)

        assert rewards[0] == 1.0
        assert len(rewards) == 2
        v.state_store.close()

    def test_empty_metagraph_returns_empty(self, tmp_path: Path):
        v = _make_validator(tmp_path, hotkeys=[])
        rewards, uids = calculate_miner_rewards(v)
        assert rewards.size == 0
        assert uids == set()
        v.state_store.close()

    def test_inactive_miner_gets_no_credit(self, tmp_path: Path):
        """Even with best rate and collateral, a deactivated miner earns nothing."""
        hotkeys = _pad_hotkeys_to_cover_recycle(['hk_a'])
        v = _make_validator(tmp_path, hotkeys=hotkeys)
        # Remove hk_a from active set — simulates MinerActivated(false) event
        v.event_watcher.active_miners.discard('hk_a')
        conn = v.state_store._require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_a', 'tao', 'btc', 0.00020, 0),
        )
        conn.commit()
        _seed_collateral(v.event_watcher, 'hk_a', MIN_COLLATERAL, 0)

        rewards, _ = calculate_miner_rewards(v)

        # No eligible crown holders → full pool recycles
        assert rewards[0] == 0.0
        assert rewards[RECYCLE_UID] == 1.0
        v.state_store.close()
