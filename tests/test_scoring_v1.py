"""C5 — crown-time scoring replay tests."""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import numpy as np

from allways.constants import (
    MAX_SCORING_BACKFILL_BLOCKS,
    RECYCLE_UID,
    SCORING_WINDOW_BLOCKS,
    SUCCESS_EXPONENT,
)
from allways.validator.event_watcher import ActiveEvent, ContractEventWatcher
from allways.validator.scoring import (
    calculate_miner_rewards,
    credibility_ramp,
    crown_holders_at_instant,
    due_for_scoring,
    replay_crown_time_window,
    score_and_reward_miners,
    scoring_window_bounds,
    success_rate,
)
from allways.validator.state_store import ValidatorStateStore

POOL_TAO_BTC = 0.5
POOL_BTC_TAO = 0.5
MIN_COLLATERAL = 100_000_000  # 0.1 TAO

METADATA_PATH = Path(__file__).parent.parent / 'allways' / 'metadata' / 'allways_swap_manager.json'


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


def make_validator(
    tmp_path: Path,
    hotkeys: list[str],
    block: int = 10_000,
    *,
    max_swap_amount: int = 0,
    collaterals: dict[str, int] | None = None,
    baseline_credibility: bool = True,
) -> SimpleNamespace:
    """Build a SimpleNamespace stand-in for the validator.

    Defaults bypass capacity weighting (``max_swap_amount=0`` → capacity_factor
    returns 1.0 fail-safe) and provide a zero-collateral stub. Tests that
    exercise capacity weighting pass explicit ``max_swap_amount`` and
    ``collaterals`` overrides.

    ``baseline_credibility`` pre-seeds enough completed swap outcomes per
    hotkey to keep the credibility ramp at 1.0 — every test that isn't
    specifically exercising the ramp gets a "fully credible" miner by
    default. Credibility tests pass ``False`` to start from zero.
    """
    from allways.constants import CREDIBILITY_RAMP_OBSERVATIONS

    store = ValidatorStateStore(db_path=tmp_path / 'state.db')
    watcher = make_watcher(store, active=set(hotkeys))
    collaterals = collaterals or {}
    bounds_cache = MagicMock()
    bounds_cache.max_swap_amount.return_value = max_swap_amount
    contract_client = MagicMock()
    contract_client.get_miner_collateral.side_effect = lambda hk: collaterals.get(hk, 0)
    database_storage = MagicMock()
    database_storage.is_enabled.return_value = False
    if baseline_credibility:
        for hk_idx, hk in enumerate(hotkeys):
            for i in range(CREDIBILITY_RAMP_OBSERVATIONS):
                store.insert_swap_outcome(
                    swap_id=-(hk_idx * CREDIBILITY_RAMP_OBSERVATIONS + i + 1),
                    miner_hotkey=hk,
                    completed=True,
                    resolved_block=0,
                )
    return SimpleNamespace(
        block=block,
        # Seed one window back so scoring_window_bounds yields the same
        # [block - SCORING_WINDOW_BLOCKS, block] window these tests assume.
        last_scored_block=max(0, block - SCORING_WINDOW_BLOCKS),
        metagraph=make_metagraph(hotkeys),
        state_store=store,
        event_watcher=watcher,
        bounds_cache=bounds_cache,
        contract_client=contract_client,
        database_storage=database_storage,
    )


def pad_hotkeys_to_cover_recycle(seeds: list[str]) -> list[str]:
    """Ensure the metagraph is large enough that RECYCLE_UID is in-bounds."""
    hotkeys = list(seeds)
    while len(hotkeys) <= RECYCLE_UID:
        hotkeys.append(f'hk_filler_{len(hotkeys)}')
    return hotkeys


class TestSuccessRateHelper:
    def test_none_is_pessimistic(self):
        """Zero observations earns no trust — the credibility hole closed."""
        assert success_rate(None) == 0.0

    def test_zero_total_is_pessimistic(self):
        assert success_rate((0, 0)) == 0.0

    def test_raw_ratio_ignores_ramp(self):
        """success_rate is the raw completed/closed ratio — no ramp folded in."""
        assert success_rate((1, 0)) == 1.0
        assert success_rate((5, 0)) == 1.0
        assert success_rate((8, 2)) == 0.8
        assert success_rate((90, 10)) == 0.9

    def test_timed_out_swaps_drop_raw_rate(self):
        # 5 completed + 5 timed_out → raw = 0.5
        assert success_rate((5, 5)) == 0.5


class TestCredibilityRampHelper:
    def test_none_is_zero(self):
        assert credibility_ramp(None) == 0.0
        assert credibility_ramp((0, 0)) == 0.0

    def test_scales_linearly_below_threshold(self):
        """1 closed swap → ramp 0.1; 5 closed → 0.5. Counts tolerated timeouts too."""
        assert credibility_ramp((1, 0)) == 0.1
        assert credibility_ramp((5, 0)) == 0.5
        assert credibility_ramp((2, 3)) == 0.5  # 3 timeouts tolerated

    def test_caps_at_full_ramp(self):
        assert credibility_ramp((10, 0)) == 1.0

    def test_excess_timeouts_hard_zero(self):
        """More than CREDIBILITY_MAX_TIMEOUTS (3) timeouts → 0, regardless of volume."""
        assert credibility_ramp((8, 3)) == 1.0  # 3 tolerated, fully ramped
        assert credibility_ramp((8, 4)) == 0.0  # 4th timeout zeros it
        assert credibility_ramp((90, 10)) == 0.0  # high volume can't rescue it
        assert credibility_ramp((0, 4)) == 0.0


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

    def test_lower_rate_wins_flips_sort(self):
        """For tao→btc the rate is TAO per BTC and lower = better. The
        helper picks the smallest qualifying rate, falling through to the
        next-smallest when the smallest is busy."""
        rates = {'a': 250.0, 'b': 251.0}
        assert crown_holders_at_instant(rates, {'a', 'b'}, lower_rate_wins=True) == ['a']
        # Smallest miner is busy → next-smallest takes the crown.
        assert crown_holders_at_instant(rates, {'a', 'b'}, busy={'a'}, lower_rate_wins=True) == ['b']

    def test_executable_check_drops_sentinel_and_falls_through(self):
        """Regression for #392: a miner posting 1e10 TAO/BTC wins the rate
        sort but is not routable, so the executability gate kicks them out
        and the crown drops to the next-best sane rate."""
        rates = {'sentinel': 1e10, 'sane': 326.0}

        # Reject the sentinel rate, accept anything ≤ 1e6 (well above sane).
        def check(r):
            return r <= 1e6

        assert crown_holders_at_instant(rates, {'sentinel', 'sane'}, executable_rate_check=check) == ['sane']

    def test_executable_check_none_preserves_legacy_behavior(self):
        """When no check is supplied, the old qualification rules apply —
        sentinel still wins. Ensures existing callers aren't surprised."""
        rates = {'sentinel': 1e10, 'sane': 326.0}
        assert crown_holders_at_instant(rates, {'sentinel', 'sane'}) == ['sentinel']


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
            ('hk_a', 'btc', 'tao', 100.0, 0),
            ('hk_b', 'btc', 'tao', 200.0, 0),
        ):
            conn.execute(
                'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
                row,
            )
        # Mid-window, A jumps to the top.
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_a', 'btc', 'tao', 300.0, 600),
        )
        conn.commit()

        crown = replay_crown_time_window(
            store=store,
            event_watcher=watcher,
            from_chain='btc',
            to_chain='tao',
            window_start=100,
            window_end=1100,
            rewardable_hotkeys={'hk_a', 'hk_b'},
        )
        # B leads blocks (100, 600] → 500 blocks, A leads (600, 1100] → 500 blocks
        assert crown == {'hk_b': 500.0, 'hk_a': 500.0}
        store.close()

    def test_sentinel_rate_earns_no_crown_when_bounds_set(self, tmp_path: Path):
        """Regression for #392: a miner posting an unexecutable sentinel
        rate (1e10 TAO/BTC) wins the rate sort but cannot route any
        positive integer sat into ``[min_swap, max_swap]`` — they should
        earn zero crown, and the sane miner takes the entire window."""
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active={'hk_sentinel', 'hk_sane'})
        conn = store.require_connection()
        for row in (
            ('hk_sentinel', 'btc', 'tao', 1e10, 0),
            ('hk_sane', 'btc', 'tao', 326.0, 0),
        ):
            conn.execute(
                'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
                row,
            )
        conn.commit()

        crown = replay_crown_time_window(
            store=store,
            event_watcher=watcher,
            from_chain='btc',
            to_chain='tao',
            window_start=100,
            window_end=1100,
            rewardable_hotkeys={'hk_sentinel', 'hk_sane'},
            min_swap_rao=100_000_000,  # 0.1 TAO
            max_swap_rao=500_000_000,  # 0.5 TAO
        )
        assert crown == {'hk_sane': 1000.0}
        store.close()

    def test_sentinel_rate_still_wins_when_bounds_unset(self, tmp_path: Path):
        """Without configured bounds the executability filter is permissive
        — preserves legacy behavior on chains/networks that haven't yet
        set ``min_swap_amount`` / ``max_swap_amount``."""
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active={'hk_sentinel', 'hk_sane'})
        conn = store.require_connection()
        for row in (
            ('hk_sentinel', 'btc', 'tao', 1e10, 0),
            ('hk_sane', 'btc', 'tao', 326.0, 0),
        ):
            conn.execute(
                'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
                row,
            )
        conn.commit()

        crown = replay_crown_time_window(
            store=store,
            event_watcher=watcher,
            from_chain='btc',
            to_chain='tao',
            window_start=100,
            window_end=1100,
            rewardable_hotkeys={'hk_sentinel', 'hk_sane'},
        )
        assert crown == {'hk_sentinel': 1000.0}
        store.close()

    def test_zero_rate_optout_hands_crown_to_still_offering_miner(self, tmp_path: Path):
        """Regression for #379: a recorded zero-rate opt-out ends crown credit
        for the leaving miner and hands the rest of the window to the miner who
        is still offering the direction — instead of the stale positive rate
        holding the crown for the whole window."""
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active={'hk_a', 'hk_b'})
        conn = store.require_connection()
        for row in (
            ('hk_a', 'btc', 'tao', 200.0, 0),
            ('hk_b', 'btc', 'tao', 150.0, 0),
            # hk_a opts out mid-window — the zero terminator scoring now sees.
            ('hk_a', 'btc', 'tao', 0.0, 600),
        ):
            conn.execute(
                'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
                row,
            )
        conn.commit()

        crown = replay_crown_time_window(
            store=store,
            event_watcher=watcher,
            from_chain='btc',
            to_chain='tao',
            window_start=100,
            window_end=1100,
            rewardable_hotkeys={'hk_a', 'hk_b'},
        )
        # A leads (100, 600] → 500 blocks; after opt-out B holds (600, 1100] → 500.
        assert crown == {'hk_a': 500.0, 'hk_b': 500.0}
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
            ('hk_a', 'btc', 'tao', 300.0, 0),  # A is best
            ('hk_b', 'btc', 'tao', 200.0, 0),  # B is runner-up
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
            from_chain='btc',
            to_chain='tao',
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
            ('hk_a', 'btc', 'tao', 300.0, 0),
            ('hk_b', 'btc', 'tao', 200.0, 0),
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
            from_chain='btc',
            to_chain='tao',
            window_start=100,
            window_end=1100,
            rewardable_hotkeys={'hk_a', 'hk_b'},
        )
        # From window_start=100 A is already busy (reconstructed from pre-window
        # SwapInitiated). B earns (100,500] = 400; A earns (500,1100] = 600.
        assert crown == {'hk_b': 400.0, 'hk_a': 600.0}
        store.close()


class TestPinnedRateDuringReservation:
    """Crown calculation must use the pinned rate during the reserved-not-busy
    window, not the live rate. Closes the bump-after-pin loophole."""

    def test_live_rate_bump_after_pin_does_not_earn_crown(self, tmp_path: Path):
        """A reserved miner who bumps their live rate to an outlier value
        earns crown at the pinned rate they had at reservation time, not the
        post-pin live rate."""
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active={'hk_a', 'hk_b'})
        conn = store.require_connection()
        # Pre-window rates: A and B both at 200.
        for hk in ('hk_a', 'hk_b'):
            conn.execute(
                'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
                (hk, 'btc', 'tao', 200.0, 0),
            )
        conn.commit()

        # A is reserved at block 300 — pin captures the live rate (200).
        watcher._record_reservation_pin_event(
            block_num=300,
            hotkey='hk_a',
            from_chain='btc',
            to_chain='tao',
            kind='start',
            rate=200.0,
        )
        # A bumps live rate to 25004 at block 400 — would normally dominate
        # the crown ranking. With the pinned-rate overlay, this update is
        # ignored for crown purposes until the reservation ends.
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_a', 'btc', 'tao', 25004.0, 400),
        )
        conn.commit()

        crown = replay_crown_time_window(
            store=store,
            event_watcher=watcher,
            from_chain='btc',
            to_chain='tao',
            window_start=100,
            window_end=1100,
            rewardable_hotkeys={'hk_a', 'hk_b'},
        )
        # Without the fix, A's 25004 live rate would dominate from block 400
        # and A would earn ~600 blocks of crown. With the fix, A's effective
        # rate stays pinned at 200, tying with B. Both earn half of the
        # 1000-block window.
        assert crown == {'hk_a': 500.0, 'hk_b': 500.0}
        store.close()

    def test_pin_end_lets_live_rate_take_over(self, tmp_path: Path):
        """Once the reservation ends (RESERVED_END), the live rate updates
        that happened during the pin take effect for crown ranking."""
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active={'hk_a', 'hk_b'})
        conn = store.require_connection()
        for hk in ('hk_a', 'hk_b'):
            conn.execute(
                'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
                (hk, 'btc', 'tao', 200.0, 0),
            )
        # A bumps to 300 at block 400 — buffered behind the pin.
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_a', 'btc', 'tao', 300.0, 400),
        )
        conn.commit()

        watcher._record_reservation_pin_event(
            block_num=300,
            hotkey='hk_a',
            from_chain='btc',
            to_chain='tao',
            kind='start',
            rate=200.0,
        )
        # Reservation ends at block 600 — A's live rate of 300 takes over.
        watcher._record_reservation_pin_event(
            block_num=600,
            hotkey='hk_a',
            from_chain='btc',
            to_chain='tao',
            kind='end',
            rate=0.0,
        )

        crown = replay_crown_time_window(
            store=store,
            event_watcher=watcher,
            from_chain='btc',
            to_chain='tao',
            window_start=100,
            window_end=1100,
            rewardable_hotkeys={'hk_a', 'hk_b'},
        )
        # (100, 600]: A pinned at 200, B at 200 — tie, each earns 250.
        # (600, 1100]: A live at 300, B still 200 — A wins 500.
        assert crown == {'hk_a': 750.0, 'hk_b': 250.0}
        store.close()

    def test_pin_only_applies_to_pinned_direction(self, tmp_path: Path):
        """A reservation pin is direction-specific — pinning btc→tao must not
        affect crown ranking in the tao→btc direction."""
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active={'hk_a', 'hk_b'})
        conn = store.require_connection()
        # Pre-window: both miners quote tao→btc at 0.005.
        for hk in ('hk_a', 'hk_b'):
            conn.execute(
                'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
                (hk, 'tao', 'btc', 0.005, 0),
            )
        # Mid-window: A improves their tao→btc rate (lower wins).
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_a', 'tao', 'btc', 0.003, 400),
        )
        conn.commit()

        # Pin A in the btc→tao direction only — should not affect tao→btc
        # crown ranking.
        watcher._record_reservation_pin_event(
            block_num=300,
            hotkey='hk_a',
            from_chain='btc',
            to_chain='tao',
            kind='start',
            rate=200.0,
        )

        crown = replay_crown_time_window(
            store=store,
            event_watcher=watcher,
            from_chain='tao',
            to_chain='btc',
            window_start=100,
            window_end=1100,
            rewardable_hotkeys={'hk_a', 'hk_b'},
        )
        # In tao→btc the pin is invisible. (100, 400]: tied at 0.005 → split.
        # (400, 1100]: A wins at 0.003 (lower) → 700 to A.
        assert crown == {'hk_a': 150.0 + 700.0, 'hk_b': 150.0}
        store.close()

    def test_pin_active_at_window_start_is_reconstructed(self, tmp_path: Path):
        """A pin laid down before window_start must overlay the live rate from
        the first interval — same anchor-reconstruction guarantee that
        rates/busy/active have."""
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active={'hk_a', 'hk_b'})
        conn = store.require_connection()
        for hk in ('hk_a', 'hk_b'):
            conn.execute(
                'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
                (hk, 'btc', 'tao', 200.0, 0),
            )
        # A bumps to 25004 BEFORE the window opens (block 50). Without the
        # pin, A would dominate from window_start.
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_a', 'btc', 'tao', 25004.0, 50),
        )
        conn.commit()

        # Pin laid down at block 30 (also before window_start) captures
        # the rate as of that block: 200. Pin remains open at window_start.
        watcher._record_reservation_pin_event(
            block_num=30,
            hotkey='hk_a',
            from_chain='btc',
            to_chain='tao',
            kind='start',
            rate=200.0,
        )

        crown = replay_crown_time_window(
            store=store,
            event_watcher=watcher,
            from_chain='btc',
            to_chain='tao',
            window_start=100,
            window_end=1100,
            rewardable_hotkeys={'hk_a', 'hk_b'},
        )
        # A's effective rate stays pinned at 200 across the entire window;
        # B is also at 200. They tie, each earns half.
        assert crown == {'hk_a': 500.0, 'hk_b': 500.0}
        store.close()

    def test_busy_still_excludes_pinned_miner_from_crown(self, tmp_path: Path):
        """The pinned-rate overlay does not override the busy gate — a pinned
        miner whose SwapInitiated fires earns no crown while busy. Sanity
        check that pinning and busy compose correctly."""
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active={'hk_a', 'hk_b'})
        conn = store.require_connection()
        for hk in ('hk_a', 'hk_b'):
            conn.execute(
                'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
                (hk, 'btc', 'tao', 200.0, 0),
            )
        conn.commit()

        # A pinned at 200 at block 300. Then SwapInitiated at 500 → busy.
        # Pin should end at SwapInitiated (event_watcher emits the 'end' in
        # production); we simulate that here.
        watcher._record_reservation_pin_event(
            block_num=300,
            hotkey='hk_a',
            from_chain='btc',
            to_chain='tao',
            kind='start',
            rate=200.0,
        )
        watcher._record_reservation_pin_event(
            block_num=500,
            hotkey='hk_a',
            from_chain='btc',
            to_chain='tao',
            kind='end',
            rate=0.0,
        )
        watcher.apply_busy_delta(500, 'hk_a', +1)
        watcher.apply_busy_delta(900, 'hk_a', -1)

        crown = replay_crown_time_window(
            store=store,
            event_watcher=watcher,
            from_chain='btc',
            to_chain='tao',
            window_start=100,
            window_end=1100,
            rewardable_hotkeys={'hk_a', 'hk_b'},
        )
        # (100, 500]: A pinned, both at 200, tie → 200 each.
        # (500, 900]: A busy → B alone → 400.
        # (900, 1100]: A back at live 200, tie → 100 each.
        assert crown == {'hk_a': 200.0 + 100.0, 'hk_b': 200.0 + 400.0 + 100.0}
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
        # Opt out of the fixture's baseline credibility seed so the test's
        # explicit 8-completed-2-timed-out profile is the only credibility data.
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys=hotkeys, baseline_credibility=False)
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
        for hk, rate in (('hk_a', 300.0), ('hk_b', 200.0)):
            conn.execute(
                'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
                (hk, 'btc', 'tao', rate, 0),
            )
        conn.commit()

        rewards, _ = calculate_miner_rewards(v)

        # hk_a isn't in metagraph so hk_b (uid 0) becomes the crown holder.
        np.testing.assert_allclose(rewards[0], POOL_BTC_TAO, atol=1e-6)
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
        for hk, rate in (('hk_a', 300.0), ('hk_b', 200.0)):
            conn.execute(
                'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
                (hk, 'btc', 'tao', rate, 0),
            )
        conn.commit()
        watcher.apply_event(500, 'MinerActivated', {'miner': 'hk_a', 'active': False})

        crown = replay_crown_time_window(
            store=store,
            event_watcher=watcher,
            from_chain='btc',
            to_chain='tao',
            window_start=100,
            window_end=1100,
            rewardable_hotkeys={'hk_a', 'hk_b'},
        )
        # A earns (100, 500] = 400. B earns (500, 1100] = 600.
        assert crown == {'hk_a': 400.0, 'hk_b': 600.0}
        store.close()

    def test_only_miner_deactivated_mid_window_pool_partially_recycles(self, tmp_path: Path):
        """Solo miner active at window_start, deactivates mid-window."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys=hotkeys, block=1100)
        conn = v.state_store.require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_a', 'tao', 'btc', 0.00020, 0),
        )
        conn.commit()
        # Deactivate inside the [block - SCORING_WINDOW_BLOCKS, block] window so
        # hk_a holds crown for part of it (sole crowned miner → full pool).
        v.event_watcher.apply_event(950, 'MinerActivated', {'miner': 'hk_a', 'active': False})

        rewards, _ = calculate_miner_rewards(v)

        # hk_a is the only miner with a rate, so it takes the entire tao→btc
        # pool for the blocks it held crown. btc→tao pool gets nothing (no
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
            ('hk_b', 'btc', 'tao', 200.0, 0),
        )
        # A's rate posted at block 500 — same block as their activation.
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_a', 'btc', 'tao', 300.0, 500),
        )
        conn.commit()
        watcher.apply_event(500, 'MinerActivated', {'miner': 'hk_a', 'active': True})

        crown = replay_crown_time_window(
            store=store,
            event_watcher=watcher,
            from_chain='btc',
            to_chain='tao',
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
        for hk, rate in (('hk_a', 300.0), ('hk_b', 200.0)):
            conn.execute(
                'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
                (hk, 'btc', 'tao', rate, 0),
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
            from_chain='btc',
            to_chain='tao',
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
        for hk, rate in (('hk_a', 300.0), ('hk_b', 200.0)):
            conn.execute(
                'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
                (hk, 'btc', 'tao', rate, 0),
            )
        conn.commit()
        v.event_watcher.apply_event(9_000, 'MinerActivated', {'miner': 'hk_a', 'active': False})

        rewards, _ = calculate_miner_rewards(v)

        # hk_b (uid 0) is the only rewardable + active miner, earns btc→tao.
        np.testing.assert_allclose(rewards[0], POOL_BTC_TAO, atol=1e-6)
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


class TestCapacityFactorHelper:
    """Direct unit tests for the capacity_factor pure function."""

    def test_at_max_swap_is_full(self):
        from allways.validator.scoring import capacity_factor

        assert capacity_factor(500_000_000, 500_000_000) == 1.0

    def test_half_max_swap_is_half(self):
        from allways.validator.scoring import capacity_factor

        assert capacity_factor(250_000_000, 500_000_000) == 0.5

    def test_quarter_max_swap_is_quarter(self):
        from allways.validator.scoring import capacity_factor

        assert capacity_factor(125_000_000, 500_000_000) == 0.25

    def test_above_max_swap_caps_at_one(self):
        from allways.validator.scoring import capacity_factor

        assert capacity_factor(2_000_000_000, 500_000_000) == 1.0

    def test_zero_collateral_is_zero(self):
        from allways.validator.scoring import capacity_factor

        assert capacity_factor(0, 500_000_000) == 0.0

    def test_negative_collateral_is_zero(self):
        """Defensive — contract guarantees >=0 but the helper handles it."""
        from allways.validator.scoring import capacity_factor

        assert capacity_factor(-1, 500_000_000) == 0.0

    def test_zero_max_swap_is_fail_safe_one(self):
        """Bounds cache cold start / RPC failure returns 0; factor should
        default to 1.0 so the first scoring pass doesn't zero everyone."""
        from allways.validator.scoring import capacity_factor

        assert capacity_factor(100_000_000, 0) == 1.0

    def test_negative_max_swap_is_fail_safe_one(self):
        from allways.validator.scoring import capacity_factor

        assert capacity_factor(100_000_000, -1) == 1.0


class TestVolumeFactorHelper:
    """Direct unit tests for the volume_factor pure function.

    Signature: volume_factor(vol_rao, total_volume_rao, crown_share, alpha).
    """

    def test_idle_crown_loses_alpha(self):
        """vol=0 of total=1 → vol_share=0, participation=0 → factor = (1-α)."""
        from allways.validator.scoring import volume_factor

        assert volume_factor(0, 1_000, crown_share=1.0, alpha=0.5) == 0.5

    def test_matching_volume_keeps_full_reward(self):
        from allways.validator.scoring import volume_factor

        # 500/1000 = 0.5 vol_share, crown_share 0.5 → participation 1.0.
        assert volume_factor(500, 1_000, crown_share=0.5, alpha=0.5) == 1.0

    def test_over_serving_capped_at_one(self):
        """Anti-wash-trade: high volume / low crown stays at 1.0."""
        from allways.validator.scoring import volume_factor

        assert volume_factor(900, 1_000, crown_share=0.1, alpha=0.5) == 1.0

    def test_partial_mismatch_interpolates(self):
        """vol_share/crown_share = 0.5 → factor = 0.5 + 0.5*0.5 = 0.75."""
        from allways.validator.scoring import volume_factor

        assert volume_factor(250, 1_000, crown_share=0.5, alpha=0.5) == 0.75

    def test_zero_crown_share_is_moot(self):
        from allways.validator.scoring import volume_factor

        assert volume_factor(500, 1_000, crown_share=0.0, alpha=0.5) == 1.0

    def test_idle_network_short_circuits_to_one(self):
        """total_volume == 0 → factor = 1.0 (no penalty for a quiet window)."""
        from allways.validator.scoring import volume_factor

        assert volume_factor(0, 0, crown_share=1.0, alpha=0.5) == 1.0

    def test_alpha_zero_disables_volume_weighting(self):
        from allways.validator.scoring import volume_factor

        for vol in (0, 250, 500, 750, 1_000):
            assert volume_factor(vol, 1_000, crown_share=1.0, alpha=0.0) == 1.0

    def test_alpha_one_is_pure_volume_share(self):
        from allways.validator.scoring import volume_factor

        assert volume_factor(0, 1_000, crown_share=1.0, alpha=1.0) == 0.0
        assert volume_factor(250, 1_000, crown_share=0.5, alpha=1.0) == 0.5

    def test_alpha_03_softer_penalty(self):
        from allways.validator.scoring import volume_factor

        assert volume_factor(0, 1_000, crown_share=1.0, alpha=0.3) == 0.7
        np.testing.assert_allclose(volume_factor(500, 1_000, crown_share=1.0, alpha=0.3), 0.85, atol=1e-6)


class TestCapacityWeighting:
    """End-to-end capacity weighting via calculate_miner_rewards."""

    def seed_tao_btc_crown(self, v: SimpleNamespace, hotkey: str, rate: float = 0.00020) -> None:
        conn = v.state_store.require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            (hotkey, 'tao', 'btc', rate, 0),
        )
        conn.commit()

    def test_full_capacity_pays_baseline(self, tmp_path: Path):
        """Miner with collateral = max_swap earns the full per-direction pool."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(
            tmp_path,
            hotkeys,
            max_swap_amount=500_000_000,
            collaterals={'hk_a': 500_000_000},
        )
        self.seed_tao_btc_crown(v, 'hk_a')
        rewards, _ = calculate_miner_rewards(v)
        # hk_a holds 100% of tao→btc crown, full capacity, sr=1.0, no volume penalty.
        np.testing.assert_allclose(rewards[0], POOL_TAO_BTC, atol=1e-6)
        v.state_store.close()

    def test_quarter_capacity_pays_quarter(self, tmp_path: Path):
        """Collateral at 1/4 of max swap → 1/4 reward, 3/4 recycles."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(
            tmp_path,
            hotkeys,
            max_swap_amount=500_000_000,
            collaterals={'hk_a': 125_000_000},
        )
        self.seed_tao_btc_crown(v, 'hk_a')
        rewards, _ = calculate_miner_rewards(v)
        np.testing.assert_allclose(rewards[0], POOL_TAO_BTC * 0.25, atol=1e-6)
        # Pool conservation: hk_a got 0.125, btc→tao bucket fully recycles (0.5),
        # plus capacity shortfall on tao→btc (0.375) → recycle = 0.875.
        recycle_uid = RECYCLE_UID if RECYCLE_UID < len(rewards) else 0
        np.testing.assert_allclose(rewards[recycle_uid], 0.875, atol=1e-6)
        np.testing.assert_allclose(rewards.sum(), 1.0, atol=1e-6)
        v.state_store.close()

    def test_over_max_caps_at_full(self, tmp_path: Path):
        """Collateral 2x max_swap → factor still 1.0, no bonus."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(
            tmp_path,
            hotkeys,
            max_swap_amount=500_000_000,
            collaterals={'hk_a': 1_000_000_000},
        )
        self.seed_tao_btc_crown(v, 'hk_a')
        rewards, _ = calculate_miner_rewards(v)
        np.testing.assert_allclose(rewards[0], POOL_TAO_BTC, atol=1e-6)
        v.state_store.close()

    def test_zero_collateral_zeros_reward(self, tmp_path: Path):
        """Collateral 0 with max_swap set → factor 0 → no reward, full recycle."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(
            tmp_path,
            hotkeys,
            max_swap_amount=500_000_000,
            collaterals={'hk_a': 0},
        )
        self.seed_tao_btc_crown(v, 'hk_a')
        rewards, _ = calculate_miner_rewards(v)
        recycle_uid = RECYCLE_UID if RECYCLE_UID < len(rewards) else 0
        assert rewards[0] == 0.0
        np.testing.assert_allclose(rewards[recycle_uid], 1.0, atol=1e-6)
        v.state_store.close()

    def test_unequal_collateral_proportional_rewards(self, tmp_path: Path):
        """Two miners tie crown, unequal collateral → rewards scale linearly."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a', 'hk_b'])
        v = make_validator(
            tmp_path,
            hotkeys,
            max_swap_amount=500_000_000,
            collaterals={'hk_a': 500_000_000, 'hk_b': 100_000_000},
        )
        conn = v.state_store.require_connection()
        for hk in ('hk_a', 'hk_b'):
            conn.execute(
                'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
                (hk, 'tao', 'btc', 0.00020, 0),
            )
        conn.commit()
        rewards, _ = calculate_miner_rewards(v)
        # Both split crown 50/50. A's capacity = 1.0, B's = 0.2. Direction pool = 0.5.
        # A earns 0.5 * 0.5 * 1.0 = 0.25; B earns 0.5 * 0.5 * 0.2 = 0.05.
        np.testing.assert_allclose(rewards[0], 0.25, atol=1e-6)
        np.testing.assert_allclose(rewards[1], 0.05, atol=1e-6)
        v.state_store.close()

    def test_cold_start_max_swap_zero_is_fail_safe(self, tmp_path: Path):
        """max_swap_amount=0 (default) bypasses capacity weighting entirely.
        Critical: a freshly restarted validator must not zero every miner."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys)  # defaults: max_swap=0, no collateral lookup
        self.seed_tao_btc_crown(v, 'hk_a')
        rewards, _ = calculate_miner_rewards(v)
        # Fail-safe path: capacity_factor = 1.0 regardless of collateral.
        np.testing.assert_allclose(rewards[0], POOL_TAO_BTC, atol=1e-6)
        v.state_store.close()

    def test_collateral_rpc_failure_is_logged_not_fatal(self, tmp_path: Path):
        """A failing get_miner_collateral logs and treats as 0 → factor 0 →
        miner's reward zeroes but the scoring pass completes."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys, max_swap_amount=500_000_000)
        v.contract_client.get_miner_collateral.side_effect = RuntimeError('rpc down')
        self.seed_tao_btc_crown(v, 'hk_a')
        rewards, _ = calculate_miner_rewards(v)
        assert rewards[0] == 0.0
        v.state_store.close()

    def test_collateral_read_cached_within_pass(self, tmp_path: Path):
        """A miner holding crown in both directions has collateral fetched once,
        not per-direction. Keeps the RPC budget bounded."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(
            tmp_path,
            hotkeys,
            max_swap_amount=500_000_000,
            collaterals={'hk_a': 500_000_000},
        )
        conn = v.state_store.require_connection()
        for from_c, to_c in (('tao', 'btc'), ('btc', 'tao')):
            conn.execute(
                'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
                ('hk_a', from_c, to_c, 0.00020 if from_c == 'tao' else 200.0, 0),
            )
        conn.commit()
        calculate_miner_rewards(v)
        # hk_a held crown in both directions → exactly one RPC for collateral.
        assert v.contract_client.get_miner_collateral.call_count == 1
        v.state_store.close()

    def test_max_swap_amount_rpc_failure_falls_back_to_unity(self, tmp_path: Path):
        """bounds_cache failure → max_swap=0 → capacity_factor fail-safes to 1.0
        so a transient RPC blip can't zero every miner on a scoring pass."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys, collaterals={'hk_a': 100_000_000})
        v.bounds_cache.max_swap_amount.side_effect = RuntimeError('rpc down')
        self.seed_tao_btc_crown(v, 'hk_a')
        rewards, _ = calculate_miner_rewards(v)
        # Fail-safe: capacity factor 1.0 → hk_a earns the full tao→btc pool.
        np.testing.assert_allclose(rewards[0], POOL_TAO_BTC, atol=1e-6)
        v.state_store.close()


class TestWeightingTraceRecorders:
    """The three record_* methods on WeightingTrace own their own math —
    direct unit coverage so changes there don't have to be inferred from
    integration tests."""

    def test_record_capacity_sets_both_fields(self):
        from allways.validator.scoring_trace import WeightingTrace

        wt = WeightingTrace()
        wt.record_capacity(collateral=250_000_000, factor=0.5)
        assert wt.collateral == 250_000_000
        assert wt.capacity_factor == 0.5

    def test_record_volume_computes_share_and_participation(self):
        from allways.validator.scoring_trace import WeightingTrace

        wt = WeightingTrace()
        # vol 250 of total 1000 = 25% share; crown_share 50% → participation 50%.
        wt.record_volume(vol_rao=250, total_volume_rao=1_000, crown_share=0.5, factor=0.75)
        assert wt.volume_rao == 250
        assert wt.volume_share == 0.25
        assert wt.crown_share == 0.5
        assert wt.participation == 0.5
        assert wt.volume_factor == 0.75

    def test_record_volume_idle_network_zeros_share(self):
        """total_volume == 0 → volume_share = 0, participation defaults to 1.0
        only when crown_share also 0; otherwise participation = 0."""
        from allways.validator.scoring_trace import WeightingTrace

        wt = WeightingTrace()
        wt.record_volume(vol_rao=0, total_volume_rao=0, crown_share=1.0, factor=1.0)
        assert wt.volume_share == 0.0
        assert wt.participation == 0.0  # vol_share / crown_share = 0/1 = 0
        assert wt.volume_factor == 1.0  # set by caller (idle-network short-circuit)

    def test_record_volume_caps_participation_at_one(self):
        """Over-serving: vol_share/crown_share > 1 → participation capped."""
        from allways.validator.scoring_trace import WeightingTrace

        wt = WeightingTrace()
        wt.record_volume(vol_rao=900, total_volume_rao=1_000, crown_share=0.1, factor=1.0)
        assert wt.participation == 1.0  # min(1.0, 0.9/0.1)

    def test_record_credibility_computes_ramp(self):
        from allways.validator.scoring_trace import WeightingTrace

        wt = WeightingTrace()
        wt.record_credibility(closed_swaps=5, ramp_target=10)
        assert wt.closed_swaps == 5
        assert wt.credibility_ramp == 0.5

    def test_record_credibility_caps_at_one(self):
        from allways.validator.scoring_trace import WeightingTrace

        wt = WeightingTrace()
        wt.record_credibility(closed_swaps=20, ramp_target=10)
        assert wt.credibility_ramp == 1.0

    def test_record_credibility_zero_target_is_unity(self):
        """Defensive: ramp_target=0 → divide-by-zero guarded → 1.0."""
        from allways.validator.scoring_trace import WeightingTrace

        wt = WeightingTrace()
        wt.record_credibility(closed_swaps=5, ramp_target=0)
        assert wt.credibility_ramp == 1.0

    def test_record_credibility_excess_timeouts_zero(self):
        """Trace mirrors the reward rule: >3 timeouts → ramp shown as 0."""
        from allways.validator.scoring_trace import WeightingTrace

        wt = WeightingTrace()
        wt.record_credibility(closed_swaps=100, ramp_target=10, timed_out=4)
        assert wt.closed_swaps == 100
        assert wt.credibility_ramp == 0.0


class TestVolumeWeighting:
    """End-to-end volume weighting via calculate_miner_rewards."""

    def seed_tao_btc_crown(self, v: SimpleNamespace, hotkey: str, rate: float = 0.00020) -> None:
        conn = v.state_store.require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            (hotkey, 'tao', 'btc', rate, 0),
        )
        conn.commit()

    def insert_volume(
        self,
        v: SimpleNamespace,
        miner_hotkey: str,
        tao_amount: int,
        swap_id: int = 1,
        # Within the default validator's [block - SCORING_WINDOW_BLOCKS, block]
        # scoring window so the volume actually counts.
        resolved_block: int = 9_900,
        completed: bool = True,
        from_chain: str = 'tao',
        to_chain: str = 'btc',
    ) -> None:
        v.state_store.insert_swap_outcome(
            swap_id=swap_id,
            miner_hotkey=miner_hotkey,
            completed=completed,
            resolved_block=resolved_block,
            tao_amount=tao_amount,
            from_chain=from_chain,
            to_chain=to_chain,
        )

    def test_idle_network_no_penalty(self, tmp_path: Path):
        """Total network volume = 0 → factor 1.0 for all crown earners."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys)
        self.seed_tao_btc_crown(v, 'hk_a')
        rewards, _ = calculate_miner_rewards(v)
        # No swaps → factor = 1.0 → full crown reward.
        np.testing.assert_allclose(rewards[0], POOL_TAO_BTC, atol=1e-6)
        v.state_store.close()

    def test_idle_crown_holder_loses_alpha(self, tmp_path: Path):
        """A holds 100% crown, B serves 100% volume → A factor = (1 - α)."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a', 'hk_b'])
        v = make_validator(tmp_path, hotkeys)
        self.seed_tao_btc_crown(v, 'hk_a')
        # B doesn't post a rate → never holds crown.
        self.insert_volume(v, 'hk_b', tao_amount=1_000_000_000, swap_id=1)
        rewards, _ = calculate_miner_rewards(v)
        # A's vol_share = 0, crown_share = 1.0 → participation = 0 → factor = 0.5.
        # B has crown_share = 0 → no crown reward to multiply.
        np.testing.assert_allclose(rewards[0], POOL_TAO_BTC * 0.5, atol=1e-6)
        assert rewards[1] == 0.0
        v.state_store.close()

    def test_matched_crown_and_volume_full_reward(self, tmp_path: Path):
        """Equal crown + equal volume → factor 1.0 for both."""
        from allways.constants import VOLUME_WEIGHT_ALPHA  # noqa: F401 — keep imports tidy

        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a', 'hk_b'])
        v = make_validator(tmp_path, hotkeys)
        conn = v.state_store.require_connection()
        for hk in ('hk_a', 'hk_b'):
            conn.execute(
                'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
                (hk, 'tao', 'btc', 0.00020, 0),
            )
        conn.commit()
        self.insert_volume(v, 'hk_a', tao_amount=500_000_000, swap_id=1)
        self.insert_volume(v, 'hk_b', tao_amount=500_000_000, swap_id=2)
        rewards, _ = calculate_miner_rewards(v)
        # Both 50/50 on crown and volume → participation 1.0 → factor 1.0 each.
        np.testing.assert_allclose(rewards[0], POOL_TAO_BTC * 0.5, atol=1e-6)
        np.testing.assert_allclose(rewards[1], POOL_TAO_BTC * 0.5, atol=1e-6)
        v.state_store.close()

    def test_over_serving_capped_no_bonus(self, tmp_path: Path):
        """A holds 100% crown but B serves 9× more volume → A's factor still > 0,
        B gets nothing (no crown). Verifies cap is one-sided: high volume can't
        amplify a low crown holder."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a', 'hk_b'])
        v = make_validator(tmp_path, hotkeys)
        # btc→tao: higher rate wins (canonical direction). A wins, B loses.
        conn = v.state_store.require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_a', 'btc', 'tao', 200.0, 0),
        )
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_b', 'btc', 'tao', 100.0, 0),
        )
        conn.commit()
        self.insert_volume(v, 'hk_a', tao_amount=100_000_000, swap_id=1, from_chain='btc', to_chain='tao')
        self.insert_volume(v, 'hk_b', tao_amount=900_000_000, swap_id=2, from_chain='btc', to_chain='tao')
        rewards, _ = calculate_miner_rewards(v)
        # A: crown_share = 1.0, vol_share = 0.1, participation = 0.1 → factor = 0.55
        np.testing.assert_allclose(rewards[0], POOL_BTC_TAO * 0.55, atol=1e-6)
        # B: crown_share = 0 → factor moot, no reward to multiply.
        assert rewards[1] == 0.0
        v.state_store.close()

    def test_partial_mismatch_interpolates(self, tmp_path: Path):
        """A holds 80% crown / 20% volume, B holds 20% crown / 80% volume.
        Uses btc→tao (higher rate wins) so the rate-direction mapping is
        self-evident in the test."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a', 'hk_b'])
        v = make_validator(tmp_path, hotkeys)
        conn = v.state_store.require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_a', 'btc', 'tao', 200.0, 0),
        )
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_b', 'btc', 'tao', 150.0, 0),
        )
        conn.commit()
        # Window is (9700, 10000]. A busy block 9_800..9_860 (60 blocks within
        # the window) so B holds crown 20% of window. We can't use a
        # SwapCompleted event here because the direction lookup needs an
        # active swap entry in the tracker — easier to just record the
        # outcome and the busy delta directly.
        v.event_watcher.apply_busy_delta(9_800, 'hk_a', +1)
        v.event_watcher.apply_busy_delta(9_860, 'hk_a', -1)
        self.insert_volume(v, 'hk_a', tao_amount=200_000_000, swap_id=1, from_chain='btc', to_chain='tao')
        self.insert_volume(v, 'hk_b', tao_amount=800_000_000, swap_id=2, from_chain='btc', to_chain='tao')
        rewards, _ = calculate_miner_rewards(v)
        # Crown: A=240/300=0.8, B=60/300=0.2. Volume: A=0.2, B=0.8.
        # A participation = 0.2/0.8 = 0.25 → factor 0.625.
        # B participation = min(1.0, 0.8/0.2) = 1.0 → factor 1.0.
        np.testing.assert_allclose(rewards[0], POOL_BTC_TAO * 0.8 * 0.625, atol=1e-6)
        np.testing.assert_allclose(rewards[1], POOL_BTC_TAO * 0.2 * 1.0, atol=1e-6)
        v.state_store.close()

    def test_timed_out_swaps_dont_count_as_volume(self, tmp_path: Path):
        """SwapTimedOut hits credibility, not volume."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys, baseline_credibility=False)
        self.seed_tao_btc_crown(v, 'hk_a')
        self.insert_volume(
            v,
            'hk_a',
            tao_amount=1_000_000_000,
            swap_id=1,
            completed=False,
        )
        rewards, _ = calculate_miner_rewards(v)
        # Volume aggregator returns 0 → idle network short-circuit → factor 1.0.
        # sr from (0 completed, 1 timed_out) = 0 → cubed = 0 → reward 0 via
        # credibility, confirming the timed-out swap didn't sneak through as
        # volume credit.
        assert rewards[0] == 0.0
        v.state_store.close()

    def test_volume_split_per_direction(self, tmp_path: Path):
        """Per-direction volume queries isolate each market. A miner with
        volume in both directions shows up in both per-direction sums but
        each query only returns that direction's slice. The aggregate
        ``get_volume_since`` still sums both, kept for non-scoring callers."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys, baseline_credibility=False)
        self.seed_tao_btc_crown(v, 'hk_a')
        self.insert_volume(v, 'hk_a', tao_amount=300_000_000, swap_id=1, from_chain='tao', to_chain='btc')
        self.insert_volume(v, 'hk_a', tao_amount=200_000_000, swap_id=2, from_chain='btc', to_chain='tao')
        assert v.state_store.get_volume_by_direction_since(0, 'tao', 'btc') == {'hk_a': 300_000_000}
        assert v.state_store.get_volume_by_direction_since(0, 'btc', 'tao') == {'hk_a': 200_000_000}
        assert v.state_store.get_volume_since(0) == {'hk_a': 500_000_000}
        v.state_store.close()

    def test_per_direction_volume_isolates_markets(self, tmp_path: Path):
        """A holds 100% of btc→tao crown and 100% of btc→tao volume. B has
        no crown but serves heavy tao→btc volume. A's reward must not be
        penalized for sitting out the tao→btc market — its denominator is
        btc→tao only."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a', 'hk_b'])
        v = make_validator(tmp_path, hotkeys)
        conn = v.state_store.require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_a', 'btc', 'tao', 200.0, 0),
        )
        conn.commit()
        # A serves all of btc→tao. B floods tao→btc but earns no crown there.
        self.insert_volume(v, 'hk_a', tao_amount=100_000_000, swap_id=1, from_chain='btc', to_chain='tao')
        self.insert_volume(v, 'hk_b', tao_amount=9_000_000_000, swap_id=2, from_chain='tao', to_chain='btc')
        rewards, _ = calculate_miner_rewards(v)
        # Old direction-blind logic: A vol_share = 0.011 of total network,
        # crown_share = 1.0 → factor 0.5055 → reward ≈ POOL_BTC_TAO * 0.5055.
        # New per-direction logic: A is sole btc→tao server, factor = 1.0.
        np.testing.assert_allclose(rewards[0], POOL_BTC_TAO, atol=1e-6)
        assert rewards[1] == 0.0
        v.state_store.close()

    def test_legacy_rows_with_zero_tao_amount_tolerated(self, tmp_path: Path):
        """Pre-migration rows have tao_amount = 0 in the schema default — they
        just don't count toward future volume aggregation."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys, baseline_credibility=False)
        conn = v.state_store.require_connection()
        conn.execute(
            'INSERT INTO swap_outcomes (swap_id, miner_hotkey, completed, resolved_block, tao_amount) VALUES (?, ?, ?, ?, ?)',
            (1, 'hk_a', 1, 9_000, 0),
        )
        conn.commit()
        volumes = v.state_store.get_volume_since(0)
        assert volumes == {'hk_a': 0}
        # Direction is empty → excluded from per-direction sums.
        assert v.state_store.get_volume_by_direction_since(0, 'tao', 'btc') == {}
        v.state_store.close()


class TestCapacityVolumeInteraction:
    """Capacity + volume are independent multipliers — verify they compose."""

    def test_both_factors_compose_multiplicatively(self, tmp_path: Path):
        """Single miner, capacity 0.5, idle on volume → reward = pool * 0.5 * 0.5."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a', 'hk_b'])
        v = make_validator(
            tmp_path,
            hotkeys,
            max_swap_amount=500_000_000,
            collaterals={'hk_a': 250_000_000},
        )
        conn = v.state_store.require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_a', 'tao', 'btc', 0.00020, 0),
        )
        conn.commit()
        # B serves some volume in A's market (tao→btc) so A's vol_share = 0.
        v.state_store.insert_swap_outcome(
            swap_id=1,
            miner_hotkey='hk_b',
            completed=True,
            resolved_block=9_900,  # within the default validator's scoring window
            tao_amount=500_000_000,
            from_chain='tao',
            to_chain='btc',
        )
        rewards, _ = calculate_miner_rewards(v)
        # A: pool 0.5 × crown 1.0 × sr 1.0 × capacity 0.5 × volume_factor 0.5
        np.testing.assert_allclose(rewards[0], 0.5 * 1.0 * 1.0 * 0.5 * 0.5, atol=1e-6)
        v.state_store.close()

    def test_full_pool_conservation_with_all_factors(self, tmp_path: Path):
        """Reward sum + recycle = 1.0 always, regardless of capacity/volume shortfalls."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a', 'hk_b'])
        v = make_validator(
            tmp_path,
            hotkeys,
            max_swap_amount=500_000_000,
            collaterals={'hk_a': 100_000_000, 'hk_b': 500_000_000},
        )
        conn = v.state_store.require_connection()
        for hk, rate in (('hk_a', 0.00020), ('hk_b', 200.0)):
            from_c, to_c = ('tao', 'btc') if rate < 1 else ('btc', 'tao')
            conn.execute(
                'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
                (hk, from_c, to_c, rate, 0),
            )
        conn.commit()
        v.state_store.insert_swap_outcome(
            swap_id=1,
            miner_hotkey='hk_b',
            completed=True,
            resolved_block=9_900,  # within the default validator's scoring window
            tao_amount=400_000_000,
            from_chain='btc',
            to_chain='tao',
        )
        rewards, _ = calculate_miner_rewards(v)
        np.testing.assert_allclose(rewards.sum(), 1.0, atol=1e-6)
        v.state_store.close()


class TestCredibilityRamp:
    """End-to-end ramp behavior via calculate_miner_rewards.

    Pessimistic-default + linear ramp closes the new-miner free-emission hole
    without a hard cliff. A genuinely active miner crosses the ramp in 10
    closed swaps and is then indistinguishable from a long-running miner with
    the same raw success rate.
    """

    def seed_btc_tao_crown(self, v: SimpleNamespace, hotkey: str, rate: float = 200.0) -> None:
        conn = v.state_store.require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            (hotkey, 'btc', 'tao', rate, 0),
        )
        conn.commit()

    def test_zero_observations_earns_nothing(self, tmp_path: Path):
        """A brand-new miner with no closed swaps earns nothing even with
        full crown — the old optimistic-default hole."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys, baseline_credibility=False)
        self.seed_btc_tao_crown(v, 'hk_a')
        rewards, _ = calculate_miner_rewards(v)
        assert rewards[0] == 0.0
        v.state_store.close()

    def test_one_completed_earns_tenth(self, tmp_path: Path):
        """1/1 completed → raw 1.0, ramp 0.1 → 1.0³ × 0.1 = 0.1 of the pool."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys, baseline_credibility=False)
        self.seed_btc_tao_crown(v, 'hk_a')
        v.state_store.insert_swap_outcome(swap_id=1, miner_hotkey='hk_a', completed=True, resolved_block=100)
        rewards, _ = calculate_miner_rewards(v)
        np.testing.assert_allclose(rewards[0], POOL_BTC_TAO * 0.1, atol=1e-6)
        v.state_store.close()

    def test_five_completed_half_of_pool(self, tmp_path: Path):
        """5/5 completed → raw 1.0, ramp 0.5 → 1.0³ × 0.5 = half the pool."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys, baseline_credibility=False)
        self.seed_btc_tao_crown(v, 'hk_a')
        for i in range(5):
            v.state_store.insert_swap_outcome(
                swap_id=i + 1, miner_hotkey='hk_a', completed=True, resolved_block=100 + i
            )
        rewards, _ = calculate_miner_rewards(v)
        np.testing.assert_allclose(rewards[0], POOL_BTC_TAO * 0.5, atol=1e-6)
        v.state_store.close()

    def test_full_ramp_at_threshold(self, tmp_path: Path):
        """10/10 completed → sr = 1.0, full pool earned."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys, baseline_credibility=False)
        self.seed_btc_tao_crown(v, 'hk_a')
        for i in range(10):
            v.state_store.insert_swap_outcome(
                swap_id=i + 1, miner_hotkey='hk_a', completed=True, resolved_block=100 + i
            )
        rewards, _ = calculate_miner_rewards(v)
        np.testing.assert_allclose(rewards[0], POOL_BTC_TAO, atol=1e-6)
        v.state_store.close()

    def test_tolerated_timeouts_advance_ramp_but_hurt_raw_rate(self, tmp_path: Path):
        """7 completed + 3 timed_out (within the 3-timeout tolerance) → ramp = 1.0,
        raw = 0.7 → 0.7³ × 1.0."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys, baseline_credibility=False)
        self.seed_btc_tao_crown(v, 'hk_a')
        for i in range(7):
            v.state_store.insert_swap_outcome(
                swap_id=i + 1, miner_hotkey='hk_a', completed=True, resolved_block=100 + i
            )
        for i in range(3):
            v.state_store.insert_swap_outcome(
                swap_id=100 + i, miner_hotkey='hk_a', completed=False, resolved_block=200 + i
            )
        rewards, _ = calculate_miner_rewards(v)
        np.testing.assert_allclose(rewards[0], POOL_BTC_TAO * 0.7**3, atol=1e-6)
        v.state_store.close()

    def test_excess_timeouts_zero_reward(self, tmp_path: Path):
        """5 completed + 5 timed_out → 5 timeouts > 3 → credibility hard-zeroed →
        reward 0 despite a high raw fulfillment count."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys, baseline_credibility=False)
        self.seed_btc_tao_crown(v, 'hk_a')
        for i in range(5):
            v.state_store.insert_swap_outcome(
                swap_id=i + 1, miner_hotkey='hk_a', completed=True, resolved_block=100 + i
            )
        for i in range(5):
            v.state_store.insert_swap_outcome(
                swap_id=100 + i, miner_hotkey='hk_a', completed=False, resolved_block=200 + i
            )
        rewards, _ = calculate_miner_rewards(v)
        np.testing.assert_allclose(rewards[0], 0.0, atol=1e-6)
        v.state_store.close()

    def test_unearned_ramp_portion_recycles(self, tmp_path: Path):
        """Ramped-down reward doesn't transfer to other miners — it recycles."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys, baseline_credibility=False)
        self.seed_btc_tao_crown(v, 'hk_a')
        v.state_store.insert_swap_outcome(swap_id=1, miner_hotkey='hk_a', completed=True, resolved_block=100)
        rewards, _ = calculate_miner_rewards(v)
        recycle_uid = RECYCLE_UID if RECYCLE_UID < len(rewards) else 0
        # tao→btc pool fully recycles (no holder), btc→tao gives A 0.05;
        # the remaining 0.45 + 0.5 = 0.95 recycles.
        np.testing.assert_allclose(rewards[recycle_uid], 1.0 - POOL_BTC_TAO * 0.1, atol=1e-6)
        np.testing.assert_allclose(rewards.sum(), 1.0, atol=1e-6)
        v.state_store.close()


class TestStateStoreVolumeMigration:
    """The tao_amount column was added in a schema migration. Ensure the
    ALTER path is idempotent and aggregation tolerates legacy data."""

    def test_idempotent_alter_on_reopen(self, tmp_path: Path):
        """Opening the DB twice must not fail on duplicate ALTER."""
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        store.close()
        # Re-open — init_db runs again, ALTER must be caught.
        store2 = ValidatorStateStore(db_path=tmp_path / 'state.db')
        store2.close()

    def test_insert_swap_outcome_persists_tao_amount(self, tmp_path: Path):
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        store.insert_swap_outcome(
            swap_id=42,
            miner_hotkey='hk_a',
            completed=True,
            resolved_block=1_000,
            tao_amount=123_456_789,
        )
        row = (
            store.require_connection()
            .execute(
                'SELECT tao_amount FROM swap_outcomes WHERE swap_id = ?',
                (42,),
            )
            .fetchone()
        )
        assert row['tao_amount'] == 123_456_789
        store.close()

    def test_get_volume_since_excludes_timed_out(self, tmp_path: Path):
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        store.insert_swap_outcome(
            swap_id=1,
            miner_hotkey='hk_a',
            completed=True,
            resolved_block=1_000,
            tao_amount=100_000_000,
        )
        store.insert_swap_outcome(
            swap_id=2,
            miner_hotkey='hk_a',
            completed=False,
            resolved_block=1_100,
            tao_amount=999_999_999,
        )
        assert store.get_volume_since(0) == {'hk_a': 100_000_000}
        store.close()

    def test_get_volume_since_respects_cutoff(self, tmp_path: Path):
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        store.insert_swap_outcome(
            swap_id=1,
            miner_hotkey='hk_a',
            completed=True,
            resolved_block=500,
            tao_amount=100_000_000,
        )
        store.insert_swap_outcome(
            swap_id=2,
            miner_hotkey='hk_a',
            completed=True,
            resolved_block=1_500,
            tao_amount=200_000_000,
        )
        # since=1_000 → only the later swap counts.
        assert store.get_volume_since(1_000) == {'hk_a': 200_000_000}
        store.close()


class TestEventWatcherPassesTaoAmount:
    """SwapCompleted events carry tao_amount; it must reach the state store."""

    def test_swap_completed_persists_tao_amount(self, tmp_path: Path):
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active={'hk_a'})
        # Pre-arm with the matching SwapInitiated so the busy delta is consistent.
        watcher.apply_event(100, 'SwapInitiated', {'swap_id': 7, 'miner': 'hk_a'})
        watcher.apply_event(
            200,
            'SwapCompleted',
            {'swap_id': 7, 'miner': 'hk_a', 'tao_amount': 250_000_000},
        )
        assert store.get_volume_since(0) == {'hk_a': 250_000_000}
        store.close()

    def test_swap_completed_without_tao_amount_defaults_to_zero(self, tmp_path: Path):
        """Tests that don't set tao_amount still work (backwards compat with
        the older test fixtures that didn't pass the field)."""
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active={'hk_a'})
        watcher.apply_event(100, 'SwapInitiated', {'swap_id': 7, 'miner': 'hk_a'})
        watcher.apply_event(200, 'SwapCompleted', {'swap_id': 7, 'miner': 'hk_a'})
        # Row exists but tao_amount is 0 → excluded from any positive-volume
        # network.
        row = (
            store.require_connection()
            .execute(
                'SELECT tao_amount FROM swap_outcomes WHERE swap_id = 7',
            )
            .fetchone()
        )
        assert row['tao_amount'] == 0
        store.close()


class TestScoringCadenceAndWindow:
    """Block-based scoring gate + cursor-anchored, gap-free window tiling —
    guards the step-vs-block fix (a multi-block forward pass made the old
    step-count gate fire too rarely and leave most blocks unscored)."""

    def test_gate_forces_first_pass_on_fresh_process(self):
        # initial_scoring_done False → fire regardless of block delta.
        assert due_for_scoring(current_block=5, last_scored_block=4, initial_scoring_done=False)

    def test_gate_fires_on_block_delta_not_step_count(self):
        # Exactly one window elapsed → due; one block short → not yet.
        assert due_for_scoring(1000, 1000 - SCORING_WINDOW_BLOCKS, True)
        assert not due_for_scoring(1000, 1000 - SCORING_WINDOW_BLOCKS + 1, True)

    def test_gate_overshoot_fires(self):
        # A multi-block forward pass can overshoot the boundary — still fires.
        assert due_for_scoring(1000, 1000 - SCORING_WINDOW_BLOCKS - 5, True)

    def test_consecutive_windows_tile_with_no_gap(self):
        # Round N's window_end must equal round N+1's window_start so every
        # block is covered exactly once.
        start1, end1 = scoring_window_bounds(current_block=1000, last_scored_block=400)
        assert (start1, end1) == (400, 1000)
        # Cursor advances to end1; next round fires a window later.
        start2, end2 = scoring_window_bounds(current_block=1600, last_scored_block=end1)
        assert start2 == end1  # tiles — no gap
        assert (start2, end2) == (1000, 1600)

    def test_overshoot_does_not_open_a_gap(self):
        # Forward straddled the boundary: fires at last+window+5. window_start
        # stays anchored to last_scored, so the 5 extra blocks are still scored.
        last = 1000
        start, end = scoring_window_bounds(current_block=last + SCORING_WINDOW_BLOCKS + 5, last_scored_block=last)
        assert start == last  # no gap despite the overshoot

    def test_backfill_is_capped_after_a_stall(self):
        # last_scored far behind (long outage) → window_start clamps to the
        # cap, not the stale cursor, so one round can't replay an unbounded span.
        start, end = scoring_window_bounds(current_block=100_000, last_scored_block=0)
        assert start == 100_000 - MAX_SCORING_BACKFILL_BLOCKS
        assert end == 100_000

    def test_fresh_seed_scores_one_trailing_window(self):
        # Seed = block - SCORING_WINDOW_BLOCKS (validator __init__) → first
        # round covers exactly one trailing window, like the old behavior.
        block = 50_000
        seed = max(0, block - SCORING_WINDOW_BLOCKS)
        start, end = scoring_window_bounds(current_block=block, last_scored_block=seed)
        assert (start, end) == (block - SCORING_WINDOW_BLOCKS, block)
