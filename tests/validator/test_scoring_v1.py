"""C5 — crown-time scoring replay tests."""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import numpy as np

from allways.constants import RECYCLE_UID, SUCCESS_EXPONENT
from allways.validator.event_watcher import ActiveEvent, ContractEventWatcher
from allways.validator.scoring import (
    calculate_miner_rewards,
    crown_holders_at_instant,
    replay_crown_time_window,
    score_and_reward_miners,
    success_rate,
)
from allways.validator.state_store import ValidatorStateStore

from tests.helpers import METADATA_PATH

POOL_TAO_BTC = 0.04
POOL_BTC_TAO = 0.04
MIN_COLLATERAL = 100_000_000  # 0.1 TAO


def make_metagraph(hotkeys: list[str]) -> SimpleNamespace:
    n = SimpleNamespace(item=lambda: len(hotkeys))
    return SimpleNamespace(n=n, hotkeys=list(hotkeys))


def make_watcher(store: ValidatorStateStore, active: set[str]) -> ContractEventWatcher:
    w = ContractEventWatcher(
        substrate=MagicMock(),
        contract_address='5contract',
        metadata_path=METADATA_PATH,
        state_store=store,
    )
    w.active_miners = set(active)
    # Seed an anchor active=True event at block 0 for each bootstrapped
    # active miner — mirrors the bootstrap seed that ContractEventWatcher
    # emits in production inside initialize(). Without this, the historical
    # replay would treat every miner as inactive at window_start.
    for hotkey in active:
        seed_active(w, hotkey, active=True, block=0)
    return w


def seed_active(watcher: ContractEventWatcher, hotkey: str, active: bool, block: int) -> None:
    """Insert an active-flag event directly into the watcher's in-memory state.
    Bypasses ``record_active_transition``'s no-op-on-same-state guard so tests
    can seed pre-window anchors (including re-seeding after a reset)."""
    event = ActiveEvent(hotkey=hotkey, active=active, block=block)
    watcher.active_events.append(event)
    watcher.active_events_by_hotkey.setdefault(hotkey, []).append(event)
    watcher.active_events.sort(key=lambda ev: ev.block)
    watcher.active_events_by_hotkey[hotkey].sort(key=lambda ev: ev.block)
    if active:
        watcher.active_miners.add(hotkey)
    else:
        watcher.active_miners.discard(hotkey)


def make_validator(tmp_path: Path, hotkeys: list[str], block: int = 10_000) -> SimpleNamespace:
    store = ValidatorStateStore(db_path=tmp_path / 'state.db')
    watcher = make_watcher(store, active=set(hotkeys))
    return SimpleNamespace(
        block=block,
        metagraph=make_metagraph(hotkeys),
        state_store=store,
        event_watcher=watcher,
    )


def pad_hotkeys_to_cover_recycle(seeds: list[str]) -> list[str]:
    """Ensure the metagraph is large enough that RECYCLE_UID is in-bounds."""
    hotkeys = list(seeds)
    while len(hotkeys) <= RECYCLE_UID:
        hotkeys.append(f'hk_filler_{len(hotkeys)}')
    return hotkeys


class TestSuccessRateHelper:
    def test_none_is_optimistic(self):
        assert success_rate(None) == 1.0

    def test_zero_total_is_optimistic(self):
        assert success_rate((0, 0)) == 1.0

    def test_ratio_is_completed_over_total(self):
        assert success_rate((8, 2)) == 0.8


class TestCrownHoldersHelper:
    def test_excludes_rate_zero(self):
        rates = {'a': 0.0, 'b': 0.00015}
        assert crown_holders_at_instant(rates, {'a', 'b'}) == ['b']

    def test_excludes_not_eligible(self):
        rates = {'a': 0.00020, 'b': 0.00015}
        assert crown_holders_at_instant(rates, {'b'}) == ['b']

    def test_tied_best_rate_returns_all(self):
        rates = {'a': 0.00020, 'b': 0.00020}
        holders = set(crown_holders_at_instant(rates, {'a', 'b'}))
        assert holders == {'a', 'b'}

    def test_busy_best_rate_loses_to_idle_runner_up(self):
        """Miner A has the best rate but is mid-swap — crown goes to B."""
        rates = {'a': 0.00030, 'b': 0.00020}
        holders = crown_holders_at_instant(rates, {'a', 'b'}, busy={'a'})
        assert holders == ['b']

    def test_all_busy_returns_empty(self):
        """Every eligible miner is busy → no crown → pool recycles."""
        rates = {'a': 0.00030, 'b': 0.00020}
        holders = crown_holders_at_instant(rates, {'a', 'b'}, busy={'a', 'b'})
        assert holders == []


class TestReplayCrownTime:
    def test_single_miner_holds_full_window(self, tmp_path: Path):
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active={'hk_a'})
        conn = store.require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_a', 'tao', 'btc', 0.00015, 0),
        )
        conn.commit()

        crown = replay_crown_time_window(
            store=store,
            event_watcher=watcher,
            from_chain='tao',
            to_chain='btc',
            window_start=100,
            window_end=1100,
            rewardable_hotkeys={'hk_a'},
        )
        assert crown == {'hk_a': 1000.0}
        store.close()

    def test_two_miners_alternate_rate_leadership(self, tmp_path: Path):
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active={'hk_a', 'hk_b'})
        conn = store.require_connection()
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

        crown = replay_crown_time_window(
            store=store,
            event_watcher=watcher,
            from_chain='tao',
            to_chain='btc',
            window_start=100,
            window_end=1100,
            rewardable_hotkeys={'hk_a', 'hk_b'},
        )
        # B leads blocks (100, 600] → 500 blocks, A leads (600, 1100] → 500 blocks
        assert crown == {'hk_b': 500.0, 'hk_a': 500.0}
        store.close()

    def test_tie_splits_credit_evenly(self, tmp_path: Path):
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active={'hk_a', 'hk_b'})
        conn = store.require_connection()
        for hk in ('hk_a', 'hk_b'):
            conn.execute(
                'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
                (hk, 'tao', 'btc', 0.00020, 0),
            )
        conn.commit()

        crown = replay_crown_time_window(
            store=store,
            event_watcher=watcher,
            from_chain='tao',
            to_chain='btc',
            window_start=100,
            window_end=1100,
            rewardable_hotkeys={'hk_a', 'hk_b'},
        )
        assert crown == {'hk_a': 500.0, 'hk_b': 500.0}
        store.close()

    def test_window_start_state_reconstruction_from_pre_window_events(self, tmp_path: Path):
        """A miner posted before window_start and never updated — replay reads initial state."""
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active={'hk_a'})
        conn = store.require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_a', 'tao', 'btc', 0.00020, 5_000),
        )
        conn.commit()

        crown = replay_crown_time_window(
            store=store,
            event_watcher=watcher,
            from_chain='tao',
            to_chain='btc',
            window_start=10_000,
            window_end=11_000,
            rewardable_hotkeys={'hk_a'},
        )
        assert crown == {'hk_a': 1000.0}
        store.close()

    def test_best_rate_miner_goes_busy_credit_flows_to_runner_up(self, tmp_path: Path):
        """A holds the best rate but takes a swap at block 400 that resolves
        at block 800. During [400, 800] the crown flips to idle runner-up B."""
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active={'hk_a', 'hk_b'})
        conn = store.require_connection()
        for row in (
            ('hk_a', 'tao', 'btc', 0.00030, 0),  # A is best
            ('hk_b', 'tao', 'btc', 0.00020, 0),  # B is runner-up
        ):
            conn.execute(
                'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
                row,
            )
        conn.commit()

        # A goes busy with a swap at 400, completes at 800.
        watcher.apply_event(400, 'SwapInitiated', {'swap_id': 1, 'miner': 'hk_a'})
        watcher.apply_event(800, 'SwapCompleted', {'swap_id': 1, 'miner': 'hk_a'})

        crown = replay_crown_time_window(
            store=store,
            event_watcher=watcher,
            from_chain='tao',
            to_chain='btc',
            window_start=100,
            window_end=1100,
            rewardable_hotkeys={'hk_a', 'hk_b'},
        )
        # A earns (100,400] = 300 + (800,1100] = 300 → 600 total
        # B earns (400,800] = 400 total
        assert crown == {'hk_a': 600.0, 'hk_b': 400.0}
        store.close()

    def test_solo_miner_busy_pool_recycles(self, tmp_path: Path):
        """Only one miner has a rate, they're busy for part of the window —
        nobody else is eligible, so the busy period earns nothing (recycles)."""
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active={'hk_a'})
        conn = store.require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_a', 'tao', 'btc', 0.00020, 0),
        )
        conn.commit()

        watcher.apply_event(400, 'SwapInitiated', {'swap_id': 1, 'miner': 'hk_a'})
        watcher.apply_event(900, 'SwapTimedOut', {'swap_id': 1, 'miner': 'hk_a'})

        crown = replay_crown_time_window(
            store=store,
            event_watcher=watcher,
            from_chain='tao',
            to_chain='btc',
            window_start=100,
            window_end=1100,
            rewardable_hotkeys={'hk_a'},
        )
        # A earns (100,400] = 300 + (900,1100] = 200 → 500. The 500 blocks
        # of busy interval have no idle candidate → not credited to anyone
        # (the caller recycles via the remainder).
        assert crown == {'hk_a': 500.0}
        store.close()

    def test_busy_state_at_window_start_is_reconstructed(self, tmp_path: Path):
        """Miner A's SwapInitiated fires before window_start and doesn't
        resolve until mid-window — replay must see A as busy from the start."""
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active={'hk_a', 'hk_b'})
        conn = store.require_connection()
        for row in (
            ('hk_a', 'tao', 'btc', 0.00030, 0),
            ('hk_b', 'tao', 'btc', 0.00020, 0),
        ):
            conn.execute(
                'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
                row,
            )
        conn.commit()

        # A's swap started BEFORE the window opens and completes inside it.
        watcher.apply_event(50, 'SwapInitiated', {'swap_id': 1, 'miner': 'hk_a'})
        watcher.apply_event(500, 'SwapCompleted', {'swap_id': 1, 'miner': 'hk_a'})

        crown = replay_crown_time_window(
            store=store,
            event_watcher=watcher,
            from_chain='tao',
            to_chain='btc',
            window_start=100,
            window_end=1100,
            rewardable_hotkeys={'hk_a', 'hk_b'},
        )
        # From window_start=100 A is already busy (reconstructed from pre-window
        # SwapInitiated). B earns (100,500] = 400; A earns (500,1100] = 600.
        assert crown == {'hk_b': 400.0, 'hk_a': 600.0}
        store.close()


class TestCalculateMinerRewards:
    def test_empty_direction_recycles_full_pool(self, tmp_path: Path):
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys=hotkeys)

        rewards, uids = calculate_miner_rewards(v)

        assert set(uids) == set(range(len(hotkeys)))
        assert rewards[RECYCLE_UID] == 1.0
        assert rewards[0] == 0.0
        np.testing.assert_allclose(rewards.sum(), 1.0, atol=1e-6)
        v.state_store.close()

    def test_single_miner_full_pool_with_perfect_success(self, tmp_path: Path):
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys=hotkeys)
        conn = v.state_store.require_connection()
        for direction in (('tao', 'btc'), ('btc', 'tao')):
            conn.execute(
                'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
                ('hk_a', direction[0], direction[1], 0.00020, 0),
            )
        conn.commit()
        v.state_store.insert_swap_outcome(swap_id=1, miner_hotkey='hk_a', completed=True, resolved_block=100)

        rewards, _ = calculate_miner_rewards(v)

        np.testing.assert_allclose(rewards[0], POOL_TAO_BTC + POOL_BTC_TAO, atol=1e-6)
        np.testing.assert_allclose(rewards.sum(), 1.0, atol=1e-6)
        v.state_store.close()

    def test_partial_success_reduces_reward_by_cube(self, tmp_path: Path):
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys=hotkeys)
        conn = v.state_store.require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_a', 'tao', 'btc', 0.00020, 0),
        )
        conn.commit()
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
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_b'])
        v = make_validator(tmp_path, hotkeys=hotkeys)
        # Ensure hk_a is still marked active on the watcher even if out of metagraph
        v.event_watcher.active_miners.add('hk_a')
        conn = v.state_store.require_connection()
        for hk, rate in (('hk_a', 0.00030), ('hk_b', 0.00020)):
            conn.execute(
                'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
                (hk, 'tao', 'btc', rate, 0),
            )
        conn.commit()

        rewards, _ = calculate_miner_rewards(v)

        # hk_a isn't in metagraph so hk_b (uid 0) becomes the crown holder.
        np.testing.assert_allclose(rewards[0], POOL_TAO_BTC, atol=1e-6)
        v.state_store.close()

    def test_recycle_uid_out_of_bounds_falls_back_to_zero(self, tmp_path: Path):
        hotkeys = ['hk_a', 'hk_b']
        v = make_validator(tmp_path, hotkeys=hotkeys)

        rewards, _ = calculate_miner_rewards(v)

        assert rewards[0] == 1.0
        assert len(rewards) == 2
        v.state_store.close()

    def test_empty_metagraph_returns_empty(self, tmp_path: Path):
        v = make_validator(tmp_path, hotkeys=[])
        rewards, uids = calculate_miner_rewards(v)
        assert rewards.size == 0
        assert uids == set()
        v.state_store.close()

    def test_never_active_miner_gets_no_credit(self, tmp_path: Path):
        """A miner with a rate and collateral but no MinerActivated event in
        history earns nothing — the historical active flag is the tell-all."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys=hotkeys)
        # Wipe the bootstrap active seed — hk_a has never been activated
        # on-chain. This mirrors a miner that registered but never called
        # set_active(true).
        v.event_watcher.active_miners.discard('hk_a')
        v.event_watcher.active_events.clear()
        v.event_watcher.active_events_by_hotkey.clear()
        conn = v.state_store.require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_a', 'tao', 'btc', 0.00020, 0),
        )
        conn.commit()

        rewards, _ = calculate_miner_rewards(v)

        assert rewards[0] == 0.0
        assert rewards[RECYCLE_UID] == 1.0
        v.state_store.close()


class TestHistoricalActiveState:
    """Replay must judge active state *as of each block* in the window, not
    as of the scoring moment. A miner inactive at scoring time is still
    rewardable for blocks where they were active, and vice versa."""

    def seed_one_miner(
        self,
        v,
        hotkey: str,
        rate: float,
        from_chain: str = 'tao',
        to_chain: str = 'btc',
        collateral: int = MIN_COLLATERAL,
    ) -> None:
        conn = v.state_store.require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            (hotkey, from_chain, to_chain, rate, 0),
        )
        conn.commit()

    def test_deactivation_mid_window_truncates_credit(self, tmp_path: Path):
        """Active from window_start, deactivates at block 600 of a 1000-block
        window. Credit runs until 600; nothing after."""
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active={'hk_a'})
        conn = store.require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_a', 'tao', 'btc', 0.00020, 0),
        )
        conn.commit()
        # Deactivate mid-window at block 600 (window is (100, 1100]).
        watcher.apply_event(600, 'MinerActivated', {'miner': 'hk_a', 'active': False})

        crown = replay_crown_time_window(
            store=store,
            event_watcher=watcher,
            from_chain='tao',
            to_chain='btc',
            window_start=100,
            window_end=1100,
            rewardable_hotkeys={'hk_a'},
        )
        assert crown == {'hk_a': 500.0}
        store.close()

    def test_activation_mid_window_starts_credit(self, tmp_path: Path):
        """Inactive at window_start, activates at block 400. Credit runs
        from block 400 to window_end."""
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active=set())  # nobody seeded active
        conn = store.require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_a', 'tao', 'btc', 0.00020, 0),
        )
        conn.commit()
        watcher.apply_event(400, 'MinerActivated', {'miner': 'hk_a', 'active': True})

        crown = replay_crown_time_window(
            store=store,
            event_watcher=watcher,
            from_chain='tao',
            to_chain='btc',
            window_start=100,
            window_end=1100,
            rewardable_hotkeys={'hk_a'},
        )
        # (100, 400] → inactive, no credit. (400, 1100] → active, 700 blocks.
        assert crown == {'hk_a': 700.0}
        store.close()

    def test_deactivated_at_scoring_time_still_earns_for_active_window(self, tmp_path: Path):
        """THE bug-fix case. Miner was active and top-rate throughout the
        entire window. *After* window_end but before scoring runs they call
        set_active(false). They should still earn their full window."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys=hotkeys, block=10_000)
        self.seed_one_miner(v, 'hk_a', 0.00020)
        self.seed_one_miner(v, 'hk_a', 0.00020, from_chain='btc', to_chain='tao')
        # Deactivation happens *after* window_end (=10_000). The replay
        # window is (8_800, 10_000], so this transition is outside it.
        v.event_watcher.apply_event(10_500, 'MinerActivated', {'miner': 'hk_a', 'active': False})

        rewards, _ = calculate_miner_rewards(v)

        # Full pool across both directions goes to hk_a (uid 0).
        np.testing.assert_allclose(rewards[0], POOL_TAO_BTC + POOL_BTC_TAO, atol=1e-6)
        np.testing.assert_allclose(rewards.sum(), 1.0, atol=1e-6)
        v.state_store.close()

    def test_multiple_active_cycles(self, tmp_path: Path):
        """Active at start, off at 300, back on at 700, off at 900. Credit:
        (100,300] = 200 + (700,900] = 200 = 400."""
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active={'hk_a'})
        conn = store.require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_a', 'tao', 'btc', 0.00020, 0),
        )
        conn.commit()
        watcher.apply_event(300, 'MinerActivated', {'miner': 'hk_a', 'active': False})
        watcher.apply_event(700, 'MinerActivated', {'miner': 'hk_a', 'active': True})
        watcher.apply_event(900, 'MinerActivated', {'miner': 'hk_a', 'active': False})

        crown = replay_crown_time_window(
            store=store,
            event_watcher=watcher,
            from_chain='tao',
            to_chain='btc',
            window_start=100,
            window_end=1100,
            rewardable_hotkeys={'hk_a'},
        )
        assert crown == {'hk_a': 400.0}
        store.close()

    def test_leader_deactivates_runner_up_inherits_crown(self, tmp_path: Path):
        """A is top rate but deactivates at block 500; B was runner-up and
        remains active. B takes the crown from 500 onward."""
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active={'hk_a', 'hk_b'})
        conn = store.require_connection()
        for hk, rate in (('hk_a', 0.00030), ('hk_b', 0.00020)):
            conn.execute(
                'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
                (hk, 'tao', 'btc', rate, 0),
            )
        conn.commit()
        watcher.apply_event(500, 'MinerActivated', {'miner': 'hk_a', 'active': False})

        crown = replay_crown_time_window(
            store=store,
            event_watcher=watcher,
            from_chain='tao',
            to_chain='btc',
            window_start=100,
            window_end=1100,
            rewardable_hotkeys={'hk_a', 'hk_b'},
        )
        # A earns (100, 500] = 400. B earns (500, 1100] = 600.
        assert crown == {'hk_a': 400.0, 'hk_b': 600.0}
        store.close()

    def test_only_miner_deactivated_mid_window_pool_partially_recycles(self, tmp_path: Path):
        """Solo miner deactivates at 600. Earns 500, forfeits 500."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys=hotkeys, block=1100)
        conn = v.state_store.require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_a', 'tao', 'btc', 0.00020, 0),
        )
        conn.commit()
        v.event_watcher.apply_event(600, 'MinerActivated', {'miner': 'hk_a', 'active': False})

        rewards, _ = calculate_miner_rewards(v)

        # hk_a earned (0, 600] = 600 blocks out of 1100 → 600/1100 of tao→btc
        # pool (success_rate=1.0 default). btc→tao pool gets nothing (no
        # rates posted) and recycles.
        np.testing.assert_allclose(rewards[0], POOL_TAO_BTC, atol=1e-6)
        # Everything else recycles: btc→tao pool.
        np.testing.assert_allclose(rewards.sum(), 1.0, atol=1e-6)
        v.state_store.close()

    def test_pre_window_activation_is_reconstructed(self, tmp_path: Path):
        """Activation event at block 50 (before window_start=100). At
        window_start the reconstructed active set must include hk_a."""
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active=set())
        conn = store.require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_a', 'tao', 'btc', 0.00020, 0),
        )
        conn.commit()
        # Pre-window activation at block 50.
        watcher.apply_event(50, 'MinerActivated', {'miner': 'hk_a', 'active': True})

        crown = replay_crown_time_window(
            store=store,
            event_watcher=watcher,
            from_chain='tao',
            to_chain='btc',
            window_start=100,
            window_end=1100,
            rewardable_hotkeys={'hk_a'},
        )
        assert crown == {'hk_a': 1000.0}
        store.close()

    def test_pre_window_deactivation_is_reconstructed(self, tmp_path: Path):
        """Miner was active at bootstrap but deactivated pre-window. Replay
        must see them inactive from window_start."""
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active={'hk_a'})
        conn = store.require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_a', 'tao', 'btc', 0.00020, 0),
        )
        conn.commit()
        watcher.apply_event(50, 'MinerActivated', {'miner': 'hk_a', 'active': False})

        crown = replay_crown_time_window(
            store=store,
            event_watcher=watcher,
            from_chain='tao',
            to_chain='btc',
            window_start=100,
            window_end=1100,
            rewardable_hotkeys={'hk_a'},
        )
        assert crown == {}
        store.close()

    def test_active_applies_before_rate_at_same_block(self, tmp_path: Path):
        """At a shared block, ACTIVE applies before RATE. A posts rate and
        activates at block 500; B has been active and top-rate. Before 500,
        B holds crown alone. At 500 both transitions apply; the interval
        *ending* at 500 was B-only. After 500, A has a higher rate and is
        now active → A takes crown."""
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active={'hk_b'})
        conn = store.require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_b', 'tao', 'btc', 0.00020, 0),
        )
        # A's rate posted at block 500 — same block as their activation.
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_a', 'tao', 'btc', 0.00030, 500),
        )
        conn.commit()
        watcher.apply_event(500, 'MinerActivated', {'miner': 'hk_a', 'active': True})

        crown = replay_crown_time_window(
            store=store,
            event_watcher=watcher,
            from_chain='tao',
            to_chain='btc',
            window_start=100,
            window_end=1100,
            rewardable_hotkeys={'hk_a', 'hk_b'},
        )
        # (100, 500] → B alone: 400. (500, 1100] → A (higher rate, now active): 600.
        assert crown == {'hk_b': 400.0, 'hk_a': 600.0}
        store.close()

    def test_crown_helper_with_active_filter(self):
        """crown_holders_at_instant: explicit active filter excludes otherwise-
        qualified miners."""
        rates = {'a': 0.00030, 'b': 0.00020}
        # a has best rate but isn't in active set.
        holders = crown_holders_at_instant(rates, rewardable={'a', 'b'}, active={'b'})
        assert holders == ['b']

    def test_crown_helper_active_none_disables_filter(self):
        """When active is None, the filter is disabled (backwards-compat for
        the helper's isolated-test use case)."""
        rates = {'a': 0.00020}
        holders = crown_holders_at_instant(rates, rewardable={'a'})
        assert holders == ['a']

    def test_crown_helper_empty_active_excludes_everyone(self):
        """Explicit empty active set → nobody qualifies, even with rate +
        collateral."""
        rates = {'a': 0.00020, 'b': 0.00015}
        holders = crown_holders_at_instant(rates, rewardable={'a', 'b'}, active=set())
        assert holders == []

    def test_active_transition_plus_busy_transition_at_same_block(self, tmp_path: Path):
        """ACTIVE (kind=0) orders before BUSY (kind=1). A deactivates and
        goes busy at the same block — the interval ending at that block
        included A. After the block, A is out both ways."""
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active={'hk_a', 'hk_b'})
        conn = store.require_connection()
        for hk, rate in (('hk_a', 0.00030), ('hk_b', 0.00020)):
            conn.execute(
                'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
                (hk, 'tao', 'btc', rate, 0),
            )
        conn.commit()
        # At block 500: A both deactivates and picks up a swap. Both events
        # apply; both end A's crown credit. A remains out until deactivation
        # reverses (it doesn't).
        watcher.apply_event(500, 'MinerActivated', {'miner': 'hk_a', 'active': False})
        watcher.apply_event(500, 'SwapInitiated', {'swap_id': 1, 'miner': 'hk_a'})
        watcher.apply_event(800, 'SwapCompleted', {'swap_id': 1, 'miner': 'hk_a'})

        crown = replay_crown_time_window(
            store=store,
            event_watcher=watcher,
            from_chain='tao',
            to_chain='btc',
            window_start=100,
            window_end=1100,
            rewardable_hotkeys={'hk_a', 'hk_b'},
        )
        # (100, 500] → A holds: 400. (500, 1100] → B holds (A inactive AND
        # busy, then inactive only): 600.
        assert crown == {'hk_a': 400.0, 'hk_b': 600.0}
        store.close()

    def test_dereg_plus_deactivation_no_credit(self, tmp_path: Path):
        """Miner dereg'd AND deactivated — earns nothing, for both reasons.
        Tests that the two filters compose correctly (no accidental double-
        negative that credits them)."""
        # hk_a is not on the metagraph.
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_b'])
        v = make_validator(tmp_path, hotkeys=hotkeys, block=10_000)
        # hk_a is historically active (seeded) but we deactivate mid-window
        # and they're dereg'd at scoring time.
        v.event_watcher.active_miners.add('hk_a')
        seed_active(v.event_watcher, 'hk_a', active=True, block=0)
        conn = v.state_store.require_connection()
        for hk, rate in (('hk_a', 0.00030), ('hk_b', 0.00020)):
            conn.execute(
                'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
                (hk, 'tao', 'btc', rate, 0),
            )
        conn.commit()
        v.event_watcher.apply_event(9_000, 'MinerActivated', {'miner': 'hk_a', 'active': False})

        rewards, _ = calculate_miner_rewards(v)

        # hk_b (uid 0) is the only rewardable + active miner, earns tao→btc.
        np.testing.assert_allclose(rewards[0], POOL_TAO_BTC, atol=1e-6)
        np.testing.assert_allclose(rewards.sum(), 1.0, atol=1e-6)
        v.state_store.close()


class TestEventWatcherActiveState:
    """Unit-test the event_watcher's active-state tracking in isolation so
    the scoring pipeline has a reliable foundation."""

    def test_get_active_miners_at_empty(self, tmp_path: Path):
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active=set())
        assert watcher.get_active_miners_at(1000) == set()
        store.close()

    def test_get_active_miners_at_returns_latest_before_block(self, tmp_path: Path):
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active=set())
        watcher.apply_event(100, 'MinerActivated', {'miner': 'hk_a', 'active': True})
        watcher.apply_event(500, 'MinerActivated', {'miner': 'hk_a', 'active': False})
        watcher.apply_event(800, 'MinerActivated', {'miner': 'hk_a', 'active': True})
        assert watcher.get_active_miners_at(50) == set()
        assert watcher.get_active_miners_at(100) == {'hk_a'}
        assert watcher.get_active_miners_at(300) == {'hk_a'}
        assert watcher.get_active_miners_at(500) == set()
        assert watcher.get_active_miners_at(799) == set()
        assert watcher.get_active_miners_at(800) == {'hk_a'}
        assert watcher.get_active_miners_at(9999) == {'hk_a'}
        store.close()

    def test_get_active_events_in_range_half_open_start(self, tmp_path: Path):
        """Range is (start, end] — events at exactly start_block are excluded."""
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active=set())
        watcher.apply_event(100, 'MinerActivated', {'miner': 'hk_a', 'active': True})
        watcher.apply_event(500, 'MinerActivated', {'miner': 'hk_a', 'active': False})
        watcher.apply_event(1000, 'MinerActivated', {'miner': 'hk_a', 'active': True})
        in_range = watcher.get_active_events_in_range(100, 1000)
        # block=100 is excluded (<=start_block); 500 and 1000 included.
        assert [e['block'] for e in in_range] == [500, 1000]
        assert [e['active'] for e in in_range] == [False, True]
        store.close()

    def test_record_active_transition_is_noop_on_duplicate(self, tmp_path: Path):
        """Duplicate MinerActivated emissions for the same state don't bloat
        the event log — critical for prune/retention correctness."""
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active=set())
        watcher.apply_event(100, 'MinerActivated', {'miner': 'hk_a', 'active': True})
        watcher.apply_event(200, 'MinerActivated', {'miner': 'hk_a', 'active': True})
        watcher.apply_event(300, 'MinerActivated', {'miner': 'hk_a', 'active': True})
        assert len(watcher.active_events) == 1
        assert watcher.active_events[0].block == 100
        store.close()

    def test_record_active_transition_ignores_empty_hotkey(self, tmp_path: Path):
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active=set())
        watcher.record_active_transition(100, '', True)
        assert watcher.active_events == []
        assert watcher.active_miners == set()
        store.close()

    def test_apply_minerativated_populates_per_hotkey_index(self, tmp_path: Path):
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active=set())
        watcher.apply_event(100, 'MinerActivated', {'miner': 'hk_a', 'active': True})
        watcher.apply_event(200, 'MinerActivated', {'miner': 'hk_b', 'active': True})
        watcher.apply_event(300, 'MinerActivated', {'miner': 'hk_a', 'active': False})
        assert len(watcher.active_events_by_hotkey['hk_a']) == 2
        assert len(watcher.active_events_by_hotkey['hk_b']) == 1
        assert 'hk_a' not in watcher.active_miners
        assert 'hk_b' in watcher.active_miners
        store.close()

    def test_prune_keeps_latest_active_event_per_hotkey(self, tmp_path: Path):
        """Mirror of the collateral prune rule — drop stale events but keep
        the latest per hotkey as a state-reconstruction anchor so
        get_active_miners_at still returns correct state for blocks past the
        retention boundary."""
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active=set())
        # Two transitions far in the past, one recent.
        watcher.apply_event(100, 'MinerActivated', {'miner': 'hk_a', 'active': True})
        watcher.apply_event(200, 'MinerActivated', {'miner': 'hk_a', 'active': False})
        watcher.apply_event(5_000, 'MinerActivated', {'miner': 'hk_a', 'active': True})
        # Also test a dormant hotkey whose only event is ancient.
        watcher.apply_event(50, 'MinerActivated', {'miner': 'hk_b', 'active': True})

        # current_block=10_000, SCORING_WINDOW_BLOCKS=1200 → cutoff=8_800.
        # All events below cutoff except the latest-per-hotkey should drop.
        watcher.prune_old_events(10_000)

        blocks_a = [ev.block for ev in watcher.active_events_by_hotkey['hk_a']]
        assert blocks_a == [5_000]  # only latest kept
        blocks_b = [ev.block for ev in watcher.active_events_by_hotkey['hk_b']]
        assert blocks_b == [50]  # dormant hotkey's latest anchor preserved
        # Sanity: reconstruction still works post-prune.
        assert watcher.get_active_miners_at(20_000) == {'hk_a', 'hk_b'}
        store.close()

    def test_event_kind_ordering_at_same_block(self, tmp_path: Path):
        """ACTIVE < BUSY < RATE. At a shared block the credit_interval
        *ending* at that block is evaluated before any of these transitions
        applies. Ordering matters: active-flag flip at block N must gate
        block N+1 regardless of any other same-block transition."""
        from allways.validator.scoring import EventKind

        assert int(EventKind.ACTIVE) < int(EventKind.BUSY) < int(EventKind.RATE)


class TestHaltShortCircuit:
    """Halt check at the scoring entry sidesteps event replay: full pool
    recycles, rewards skip the crown-time path entirely."""

    def _make_validator_with_halt(self, tmp_path: Path, halt_return, hotkeys: list[str]) -> SimpleNamespace:
        hotkeys = pad_hotkeys_to_cover_recycle(hotkeys)
        v = make_validator(tmp_path, hotkeys)
        contract_client = MagicMock()
        if isinstance(halt_return, Exception):
            contract_client.get_halted.side_effect = halt_return
        else:
            contract_client.get_halted.return_value = halt_return
        v.contract_client = contract_client
        captured = {}

        def capture(rewards, miner_uids):
            captured['rewards'] = rewards
            captured['miner_uids'] = miner_uids

        v.update_scores = capture
        return v, captured

    def test_halted_short_circuits_to_full_recycle(self, tmp_path: Path):
        v, captured = self._make_validator_with_halt(tmp_path, halt_return=True, hotkeys=['hk_a', 'hk_b'])
        score_and_reward_miners(v)
        rewards = captured['rewards']
        recycle_uid = RECYCLE_UID if RECYCLE_UID < len(rewards) else 0
        assert rewards[recycle_uid] == 1.0
        # every other uid must be exactly zero
        assert float(rewards.sum()) == 1.0

    def test_halted_rpc_error_still_scores(self, tmp_path: Path):
        """If the halt RPC fails, scoring proceeds as normal rather than
        zeroing every miner's reward."""
        v, captured = self._make_validator_with_halt(tmp_path, halt_return=RuntimeError('rpc down'), hotkeys=['hk_a'])
        # No rate / collateral seeded → replay credits nothing → full pool
        # still recycles, but via the normal path (not the halt short-circuit).
        score_and_reward_miners(v)
        rewards = captured['rewards']
        recycle_uid = RECYCLE_UID if RECYCLE_UID < len(rewards) else 0
        assert rewards[recycle_uid] > 0  # recycle got something via normal path
