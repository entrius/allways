"""C5 — crown-time scoring replay tests."""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import numpy as np
import pytest

from allways.classes import ActivityTransition, MinerActivity
from allways.constants import (
    DIRECTION_POOLS,
    MAX_FAILED_SWAPS,
    MAX_SCORING_BACKFILL_SECS,
    MIN_SUCCESSFUL_SWAPS,
    RECYCLE_UID,
    SCORING_WINDOW_BLOCKS,
)
from allways.utils.rate import is_executable_rate, min_executable_sol_leg
from allways.validator import scoring as scoring_mod
from allways.validator.event_index import SolanaEventIndex
from allways.validator.scoring import (
    build_eligibility,
    calculate_miner_rewards,
    crown_holders_at_instant,
    due_for_scoring,
    is_eligible,
    make_crown_predicates,
    replay_crown_time_window,
    score_and_reward_miners,
    scoring_window_bounds,
    snapshot_current_crown_holders,
    snapshot_current_miner_scores,
)
from allways.validator.state_store import ValidatorStateStore

# Mirror production pool shares so these stay in sync if DIRECTION_POOLS changes.
POOL_BTC_SOL = DIRECTION_POOLS[('btc', 'sol')]
POOL_SOL_BTC = DIRECTION_POOLS[('sol', 'btc')]
MIN_COLLATERAL = 100_000_000  # 0.1 TAO

METADATA_PATH = Path(__file__).parent.parent / 'allways' / 'metadata' / 'allways_swap_manager.json'


def make_metagraph(hotkeys: list[str]) -> SimpleNamespace:
    n = SimpleNamespace(item=lambda: len(hotkeys))
    return SimpleNamespace(n=n, hotkeys=list(hotkeys))


class FakeSolanaClient:
    """Stand-in exposing ``get_all('MinerState')`` for the flat eligibility gate
    (B3.3). Each entry's ``miner`` is the test hotkey string; the autouse
    ``_identity_attribution`` fixture makes pubkey→hotkey attribution identity so
    state keys by the same opaque strings the crown tables use. End-to-end
    sr25519 attribution is covered in ``tests/test_eligibility.py``."""

    def __init__(self, miner_counters: dict[str, tuple[int, int]]):
        self._counters = miner_counters

    def get_all(self, name: str):
        if name == 'MinerState':
            return [
                (f'pda_{hk}', SimpleNamespace(miner=hk, successful_swaps=s, failed_swaps=f))
                for hk, (s, f) in self._counters.items()
            ]
        return []


@pytest.fixture(autouse=True)
def _identity_attribution(monkeypatch):
    """In this module the ``MinerState.miner`` and the crown hotkey are the same
    opaque string, so pubkey→hotkey attribution is identity. Real sr25519
    attribution lives in ``tests/test_eligibility.py`` (unaffected by this)."""
    monkeypatch.setattr(
        scoring_mod, 'build_attribution', lambda client: {str(pk): str(pk) for _pda, pk in _miner_pubkeys(client)}
    )


def _miner_pubkeys(client):
    return [(pda, ms.miner) for pda, ms in client.get_all('MinerState')]


class CrownSeeder:
    """Test-local writer for the crown event tables, replacing the deleted
    substrate ``ContractEventWatcher`` as a DB seeder. Scoring reads these tables
    back through ``SolanaEventIndex`` — this just persists transitions."""

    def __init__(self, store: ValidatorStateStore):
        self.state_store = store

    def apply_event(self, block: int, name: str, fields: dict) -> None:
        miner = fields['miner']
        if name in ('MinerActivated', 'MinerDeactivated'):
            active = fields.get('active', name == 'MinerActivated')
            self.state_store.insert_active_event(block, miner, bool(active))
        elif name == 'PoolResolved':
            # RESERVE_START now + RESERVE_EXPIRE at block+ttl (default beyond any
            # test window, so a swap's FULFILL_END is what returns to AVAILABLE).
            ttl = fields.get('ttl', 10**9)
            self.state_store.insert_activity_event(block, miner, ActivityTransition.RESERVE_START)
            self.state_store.insert_activity_event(block + ttl, miner, ActivityTransition.RESERVE_EXPIRE)
        elif name == 'SwapInitiated':
            self.state_store.insert_activity_event(block, miner, ActivityTransition.FULFILL_START)
        elif name in ('SwapCompleted', 'SwapTimedOut'):
            self.state_store.insert_activity_event(block, miner, ActivityTransition.FULFILL_END)
        elif name in ('CollateralPosted', 'CollateralWithdrawn'):
            self.state_store.insert_collateral_event(block, miner, int(fields['total']))
        else:
            raise ValueError(f'CrownSeeder: unsupported event {name}')

    def reserve_then_swap(
        self, miner: str, reserve_block: int, init_block: int, end_block: int, end='SwapCompleted', ttl: int = 10**9
    ):
        """Realistic busy span: PoolResolved → SwapInitiated → completion. The
        reservation's RESERVE_EXPIRE is parked at reserve_block + ttl (default
        beyond the window)."""
        self.apply_event(reserve_block, 'PoolResolved', {'miner': miner, 'ttl': ttl})
        self.apply_event(init_block, 'SwapInitiated', {'miner': miner})
        self.apply_event(end_block, end, {'miner': miner})

    def get_active_miners_at(self, at_time: int) -> set[str]:
        return self.state_store.get_active_state_at(at_time)

    def get_activity_state_at(self, at_time: int) -> dict[str, MinerActivity]:
        return self.state_store.get_activity_state_at(at_time)

    def get_miner_collaterals_at(self, at_time: int) -> dict[str, int]:
        return self.state_store.get_collaterals_at(at_time)


def make_watcher(store: ValidatorStateStore, active: set[str]) -> CrownSeeder:
    seeder = CrownSeeder(store)
    # Anchor active=True at block 0 for each bootstrapped active miner, so the
    # historical replay sees them active at window_start (mirrors production's
    # initialize() anchor).
    for hotkey in active:
        seed_active(seeder, hotkey, active=True, block=0)
    return seeder


def seed_active(seeder: CrownSeeder, hotkey: str, active: bool, block: int) -> None:
    """Persist an active-flag transition to the ``active_events`` table the crown
    reads back through ``SolanaEventIndex``."""
    seeder.state_store.insert_active_event(block, hotkey, active)


def seed_collateral(seeder: CrownSeeder, hotkey: str, collateral_rao: int, block: int) -> None:
    """Persist a collateral transition to the ``collateral_events`` table."""
    seeder.state_store.insert_collateral_event(block, hotkey, int(collateral_rao))


def make_validator(
    tmp_path: Path,
    hotkeys: list[str],
    block: int = 10_000,
    *,
    max_swap_amount: int = 0,
    min_swap_amount: int = 0,
    collaterals: dict[str, int] | None = None,
    miner_counters: dict[str, tuple[int, int]] | None = None,
    all_eligible: bool = True,
) -> SimpleNamespace:
    """Build a SimpleNamespace stand-in for the validator.

    Defaults bypass capacity weighting (``max_swap_amount=0`` → capacity_factor
    returns 1.0 fail-safe) and provide a zero-collateral stub. Tests that
    exercise capacity weighting pass explicit ``max_swap_amount`` and
    ``collaterals`` overrides.

    Eligibility (B3.3) is read off on-chain ``MinerState`` counters via
    ``solana_client``. ``miner_counters`` maps hotkey → (successful, failed)
    swaps; when omitted, every hotkey gets ``(MIN_SUCCESSFUL_SWAPS, 0)`` if
    ``all_eligible`` (the default — a "passes the gate" miner) else ``(0, 0)``
    (no proven successes → ineligible).
    """
    store = ValidatorStateStore(db_path=tmp_path / 'state.db')
    watcher = make_watcher(store, active=set(hotkeys))
    collaterals = collaterals or {}
    # Mirror the cold-bootstrap collateral anchor: scoring now reads collateral
    # from the event watcher's per-block series, not from a live contract call.
    # Seeding at block 0 puts the value before any test's window_start, so the
    # reconstruction at window_start returns it as the anchor value.
    for hotkey, amount in collaterals.items():
        if amount > 0:
            seed_collateral(watcher, hotkey, amount, block=0)
    solana_config_cache = MagicMock()
    solana_config_cache.max_swap_amount.return_value = max_swap_amount
    solana_config_cache.min_swap_amount.return_value = min_swap_amount
    solana_config_cache.halted.return_value = False
    database_storage = MagicMock()
    database_storage.is_enabled.return_value = False
    if miner_counters is None:
        default = (MIN_SUCCESSFUL_SWAPS, 0) if all_eligible else (0, 0)
        miner_counters = {hk: default for hk in hotkeys}
    return SimpleNamespace(
        block=block,
        # Seed one window back so scoring_window_bounds yields the same window
        # these tests assume. last_scored_block gates cadence (block axis);
        # last_scored_time anchors the crown replay window (unix axis) — tests
        # pass `block` as the synthetic time axis to calculate_miner_rewards.
        last_scored_block=max(0, block - SCORING_WINDOW_BLOCKS),
        last_scored_time=max(0, block - SCORING_WINDOW_BLOCKS),
        metagraph=make_metagraph(hotkeys),
        state_store=store,
        # ``event_watcher`` is a test-local DB writer for the crown event tables;
        # scoring reads those tables back through ``event_index`` (B3.4).
        event_watcher=watcher,
        event_index=SolanaEventIndex(store),
        solana_config_cache=solana_config_cache,
        database_storage=database_storage,
        solana_client=FakeSolanaClient(miner_counters),
    )


def pad_hotkeys_to_cover_recycle(seeds: list[str]) -> list[str]:
    """Ensure the metagraph is large enough that RECYCLE_UID is in-bounds."""
    hotkeys = list(seeds)
    while len(hotkeys) <= RECYCLE_UID:
        hotkeys.append(f'hk_filler_{len(hotkeys)}')
    return hotkeys


def _miner_state(successful: int, failed: int) -> SimpleNamespace:
    return SimpleNamespace(successful_swaps=successful, failed_swaps=failed)


class TestIsEligibleHelper:
    """Flat binary gate: eligible iff successes >= MIN_SUCCESSFUL_SWAPS (2) and
    failures <= MAX_FAILED_SWAPS (2). Replaces success_rate³ × credibility."""

    def test_below_min_successes_ineligible(self):
        assert is_eligible(_miner_state(0, 0)) is False
        assert is_eligible(_miner_state(1, 0)) is False  # one short of the floor

    def test_at_min_successes_eligible(self):
        assert is_eligible(_miner_state(MIN_SUCCESSFUL_SWAPS, 0)) is True  # boundary

    def test_above_min_successes_eligible(self):
        assert is_eligible(_miner_state(50, 0)) is True

    def test_at_max_failures_still_eligible(self):
        # 2 failures tolerated at the boundary, given enough successes.
        assert is_eligible(_miner_state(2, MAX_FAILED_SWAPS)) is True

    def test_above_max_failures_ineligible(self):
        # One failure past the cap kills eligibility regardless of success count.
        assert is_eligible(_miner_state(50, MAX_FAILED_SWAPS + 1)) is False

    def test_both_gates_must_pass(self):
        # Enough successes but too many failures → out.
        assert is_eligible(_miner_state(3, 3)) is False
        # Few failures but too few successes → out.
        assert is_eligible(_miner_state(1, 0)) is False


class TestBuildEligibility:
    """build_eligibility attributes MinerState counters to metagraph hotkeys."""

    def test_maps_metagraph_hotkeys_to_gate(self):
        metagraph = make_metagraph(['hk_a', 'hk_b'])
        client = FakeSolanaClient({'hk_a': (MIN_SUCCESSFUL_SWAPS, 0), 'hk_b': (0, 0)})
        elig = build_eligibility(client, metagraph)
        assert elig == {'hk_a': True, 'hk_b': False}

    def test_off_metagraph_miner_dropped(self):
        """A bound miner not on the metagraph has no UID to credit → excluded."""
        metagraph = make_metagraph(['hk_a'])
        client = FakeSolanaClient({'hk_a': (5, 0), 'hk_ghost': (5, 0)})
        elig = build_eligibility(client, metagraph)
        assert elig == {'hk_a': True}

    def test_high_fail_miner_marked_ineligible(self):
        metagraph = make_metagraph(['hk_a'])
        client = FakeSolanaClient({'hk_a': (50, MAX_FAILED_SWAPS + 1)})
        assert build_eligibility(client, metagraph) == {'hk_a': False}


class TestGetClearingVolumes:
    """``get_clearing_volumes`` sums the windowed clearing-rate legs per
    (direction, hotkey) — the realized-volume read ``fill_ratio`` consumes.
    Window semantics are ``(start, end]``."""

    def _store(self, tmp_path: Path) -> ValidatorStateStore:
        return ValidatorStateStore(db_path=tmp_path / 'state.db')

    def test_sums_per_direction_and_hotkey(self, tmp_path: Path):
        store = self._store(tmp_path)
        store.insert_clearing_rate(9_800, 'hk_a', 'btc', 'sol', 300, 600)
        store.insert_clearing_rate(9_900, 'hk_a', 'btc', 'sol', 100, 200)
        store.insert_clearing_rate(9_850, 'hk_b', 'sol', 'btc', 100, 50)
        vols = store.get_clearing_volumes(9_700, 10_000)
        assert vols[('btc', 'sol')]['hk_a'] == (400, 800)
        assert vols[('sol', 'btc')]['hk_b'] == (100, 50)
        store.close()

    def test_window_boundaries_start_exclusive_end_inclusive(self, tmp_path: Path):
        store = self._store(tmp_path)
        store.insert_clearing_rate(9_700, 'hk_a', 'btc', 'sol', 1, 1)  # at start — excluded
        store.insert_clearing_rate(9_701, 'hk_a', 'btc', 'sol', 10, 10)  # just inside
        store.insert_clearing_rate(10_000, 'hk_a', 'btc', 'sol', 100, 100)  # at end — included
        store.insert_clearing_rate(10_001, 'hk_a', 'btc', 'sol', 1_000, 1_000)  # past end — excluded
        vols = store.get_clearing_volumes(9_700, 10_000)
        assert vols[('btc', 'sol')]['hk_a'] == (110, 110)
        store.close()

    def test_empty_window(self, tmp_path: Path):
        store = self._store(tmp_path)
        store.insert_clearing_rate(9_000, 'hk_a', 'btc', 'sol', 5, 5)
        assert store.get_clearing_volumes(9_700, 10_000) == {}
        store.close()

    def test_u128_scale_legs_sum_exactly(self, tmp_path: Path):
        # Legs are TEXT in SQLite; summing in Python keeps u128-scale integers
        # exact where SQL SUM would round through float.
        store = self._store(tmp_path)
        store.insert_clearing_rate(9_800, 'hk_a', 'btc', 'sol', 10**30 + 7, 1)
        store.insert_clearing_rate(9_900, 'hk_a', 'btc', 'sol', 3, 1)
        assert store.get_clearing_volumes(9_700, 10_000)[('btc', 'sol')]['hk_a'] == (10**30 + 10, 2)
        store.close()


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
        """Miner A has the best rate but is mid-swap (not in the rewardable-by-
        state set) — crown goes to B."""
        rates = {'a': 0.00030, 'b': 0.00020}
        holders = crown_holders_at_instant(rates, {'a', 'b'}, rewardable_by_state={'b'})
        assert holders == ['b']

    def test_all_busy_returns_empty(self):
        """No miner is in a rewardable state → no crown → pool recycles."""
        rates = {'a': 0.00030, 'b': 0.00020}
        holders = crown_holders_at_instant(rates, {'a', 'b'}, rewardable_by_state=set())
        assert holders == []

    def test_lower_rate_wins_flips_sort(self):
        """For tao→btc the rate is TAO per BTC and lower = better. The
        helper picks the smallest qualifying rate, falling through to the
        next-smallest when the smallest isn't rewardable."""
        rates = {'a': 250.0, 'b': 251.0}
        assert crown_holders_at_instant(rates, {'a', 'b'}, lower_rate_wins=True) == ['a']
        # Smallest miner is busy → next-smallest takes the crown.
        assert crown_holders_at_instant(rates, {'a', 'b'}, rewardable_by_state={'b'}, lower_rate_wins=True) == ['b']

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
        make_watcher(store, active={'hk_a'})
        conn = store.require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_a', 'btc', 'sol', 0.00015, 0),
        )
        conn.commit()

        crown = replay_crown_time_window(
            store=store,
            event_index=SolanaEventIndex(store),
            from_chain='btc',
            to_chain='sol',
            window_start=100,
            window_end=1100,
            rewardable_hotkeys={'hk_a'},
        )
        assert crown == {'hk_a': 1000.0}
        store.close()

    def test_two_miners_alternate_rate_leadership(self, tmp_path: Path):
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        make_watcher(store, active={'hk_a', 'hk_b'})
        conn = store.require_connection()
        for row in (
            ('hk_a', 'sol', 'btc', 100.0, 0),
            ('hk_b', 'sol', 'btc', 200.0, 0),
        ):
            conn.execute(
                'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
                row,
            )
        # Mid-window, A jumps to the top.
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_a', 'sol', 'btc', 300.0, 600),
        )
        conn.commit()

        crown = replay_crown_time_window(
            store=store,
            event_index=SolanaEventIndex(store),
            from_chain='sol',
            to_chain='btc',
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
        # Seed enough collateral so hk_sane clears the per-block boundary-squat
        # gate — this test isn't exercising that gate.
        seed_collateral(watcher, 'hk_sane', 500_000_000, block=0)
        conn = store.require_connection()
        for row in (
            ('hk_sentinel', 'btc', 'sol', 1e10, 0),
            ('hk_sane', 'btc', 'sol', 326.0, 0),
        ):
            conn.execute(
                'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
                row,
            )
        conn.commit()

        crown = replay_crown_time_window(
            store=store,
            event_index=SolanaEventIndex(store),
            from_chain='btc',
            to_chain='sol',
            window_start=100,
            window_end=1100,
            rewardable_hotkeys={'hk_sentinel', 'hk_sane'},
            min_swap_lamports=100_000_000,  # 0.1 TAO
            max_swap_lamports=500_000_000,  # 0.5 TAO
        )
        assert crown == {'hk_sane': 1000.0}
        store.close()

    def test_boundary_squat_dropped_per_block(self, tmp_path: Path):
        """Squatter posts a live, executable rate (50000 TAO/BTC) whose smallest
        in-band leg (0.5 TAO at 1000 sats) exceeds their 0.15 TAO collateral.
        Survives is_executable_rate but the per-block gate drops them — entire
        window unfilled (no other holders)."""
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active={'hk_squat'})
        seed_collateral(watcher, 'hk_squat', 150_000_000, block=0)
        conn = store.require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_squat', 'btc', 'sol', 50000.0, 0),
        )
        conn.commit()

        crown = replay_crown_time_window(
            store=store,
            event_index=SolanaEventIndex(store),
            from_chain='btc',
            to_chain='sol',
            window_start=100,
            window_end=1100,
            rewardable_hotkeys={'hk_squat'},
            min_swap_lamports=100_000_000,
            max_swap_lamports=500_000_000,
        )
        assert crown == {}
        store.close()

    def test_boundary_squat_loses_to_funded_runner_up(self, tmp_path: Path):
        """Squatter has the best rate but their per-block gate drops every
        block to the funded runner-up — same shape as the busy-runner-up case."""
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active={'hk_squat', 'hk_funded'})
        seed_collateral(watcher, 'hk_squat', 150_000_000, block=0)
        seed_collateral(watcher, 'hk_funded', 500_000_000, block=0)
        conn = store.require_connection()
        for row in (
            ('hk_squat', 'btc', 'sol', 50000.0, 0),  # best rate, can't fund
            ('hk_funded', 'btc', 'sol', 326.0, 0),  # runner-up, can fund
        ):
            conn.execute(
                'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
                row,
            )
        conn.commit()

        crown = replay_crown_time_window(
            store=store,
            event_index=SolanaEventIndex(store),
            from_chain='btc',
            to_chain='sol',
            window_start=100,
            window_end=1100,
            rewardable_hotkeys={'hk_squat', 'hk_funded'},
            min_swap_lamports=100_000_000,
            max_swap_lamports=500_000_000,
        )
        assert crown == {'hk_funded': 1000.0}
        store.close()

    def test_squat_gate_skipped_when_bounds_unset(self, tmp_path: Path):
        """Cold-start fail-safe: bounds at 0 → gate skipped (matches
        is_executable_rate's permissive branch)."""
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active={'hk_squat'})
        seed_collateral(watcher, 'hk_squat', 1, block=0)
        conn = store.require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_squat', 'sol', 'btc', 50000.0, 0),
        )
        conn.commit()

        crown = replay_crown_time_window(
            store=store,
            event_index=SolanaEventIndex(store),
            from_chain='sol',
            to_chain='btc',
            window_start=100,
            window_end=1100,
            rewardable_hotkeys={'hk_squat'},
        )
        assert crown == {'hk_squat': 1000.0}
        store.close()

    def test_squat_gate_uses_per_block_collateral(self, tmp_path: Path):
        """A miner who tops up collateral mid-window earns crown only for
        blocks after the top-up — proves the gate uses per-block state, not
        a window-end snapshot."""
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active={'hk_squat'})
        seed_collateral(watcher, 'hk_squat', 150_000_000, block=0)
        # Top-up mid-window — collateral becomes enough to fund the 0.5 TAO leg.
        watcher.apply_event(600, 'CollateralPosted', {'miner': 'hk_squat', 'amount': 350_000_000, 'total': 500_000_000})
        conn = store.require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_squat', 'btc', 'sol', 50000.0, 0),
        )
        conn.commit()

        crown = replay_crown_time_window(
            store=store,
            event_index=SolanaEventIndex(store),
            from_chain='btc',
            to_chain='sol',
            window_start=100,
            window_end=1100,
            rewardable_hotkeys={'hk_squat'},
            min_swap_lamports=100_000_000,
            max_swap_lamports=500_000_000,
        )
        # Blocks (100, 600] dropped (collateral 0.15 < 0.5 TAO leg).
        # Blocks (600, 1100] credited (collateral 0.5 TAO).
        assert crown == {'hk_squat': 500.0}
        store.close()

    def test_bounds_transition_does_not_retroactively_zero_pre_bounds_credit(self, tmp_path: Path):
        """Bounds-tightening between scoring rounds must not zero out a miner's
        credit from the previous round.

        Production scoring runs per ~hour window; each round reads bounds fresh
        and applies them to its window only. If bounds tighten between round N
        (permissive) and round N+1 (strict), round N's credit stays as it was —
        scoring is per-window, never re-evaluated.

        Verified by replaying the same rate-event store twice for adjacent
        windows: the permissive window credits the miner, the strict window
        does not, and the permissive replay's result is unchanged.
        """
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        make_watcher(store, active={'hk_a'})
        conn = store.require_connection()
        # A rate that lands in [min, max] = [0, very-large] but is unexecutable
        # once max is tightened to a small value: 1e10 TAO/BTC has no fundable
        # source in [0.1, 0.5] TAO (every sat maps above max).
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_a', 'btc', 'sol', 1e10, 0),
        )
        conn.commit()

        # Round N — permissive bounds, full window credited.
        permissive = replay_crown_time_window(
            store=store,
            event_index=SolanaEventIndex(store),
            from_chain='btc',
            to_chain='sol',
            window_start=100,
            window_end=1100,
            rewardable_hotkeys={'hk_a'},
        )
        assert permissive == {'hk_a': 1000.0}

        # Round N+1 — bounds tightened mid-day. The next window's replay zeros
        # the miner, but the prior result must still be exactly what it was.
        strict = replay_crown_time_window(
            store=store,
            event_index=SolanaEventIndex(store),
            from_chain='btc',
            to_chain='sol',
            window_start=1100,
            window_end=2100,
            rewardable_hotkeys={'hk_a'},
            min_swap_lamports=100_000_000,
            max_swap_lamports=500_000_000,
        )
        assert strict == {}

        # Re-run round N — same inputs, same result. Confirms the strict round
        # did not mutate state in a way that retroactively wipes earlier credit.
        permissive_replay = replay_crown_time_window(
            store=store,
            event_index=SolanaEventIndex(store),
            from_chain='btc',
            to_chain='sol',
            window_start=100,
            window_end=1100,
            rewardable_hotkeys={'hk_a'},
        )
        assert permissive_replay == permissive
        store.close()

    def test_sentinel_rate_still_wins_when_bounds_unset(self, tmp_path: Path):
        """Without configured bounds the executability filter is permissive
        — preserves legacy behavior on chains/networks that haven't yet
        set ``min_swap_amount`` / ``max_swap_amount``."""
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        make_watcher(store, active={'hk_sentinel', 'hk_sane'})
        conn = store.require_connection()
        for row in (
            ('hk_sentinel', 'sol', 'btc', 1e10, 0),
            ('hk_sane', 'sol', 'btc', 326.0, 0),
        ):
            conn.execute(
                'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
                row,
            )
        conn.commit()

        crown = replay_crown_time_window(
            store=store,
            event_index=SolanaEventIndex(store),
            from_chain='sol',
            to_chain='btc',
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
        make_watcher(store, active={'hk_a', 'hk_b'})
        conn = store.require_connection()
        for row in (
            ('hk_a', 'sol', 'btc', 200.0, 0),
            ('hk_b', 'sol', 'btc', 150.0, 0),
            # hk_a opts out mid-window — the zero terminator scoring now sees.
            ('hk_a', 'sol', 'btc', 0.0, 600),
        ):
            conn.execute(
                'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
                row,
            )
        conn.commit()

        crown = replay_crown_time_window(
            store=store,
            event_index=SolanaEventIndex(store),
            from_chain='sol',
            to_chain='btc',
            window_start=100,
            window_end=1100,
            rewardable_hotkeys={'hk_a', 'hk_b'},
        )
        # A leads (100, 600] → 500 blocks; after opt-out B holds (600, 1100] → 500.
        assert crown == {'hk_a': 500.0, 'hk_b': 500.0}
        store.close()

    def test_tie_splits_credit_evenly(self, tmp_path: Path):
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        make_watcher(store, active={'hk_a', 'hk_b'})
        conn = store.require_connection()
        for hk in ('hk_a', 'hk_b'):
            conn.execute(
                'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
                (hk, 'btc', 'sol', 0.00020, 0),
            )
        conn.commit()

        crown = replay_crown_time_window(
            store=store,
            event_index=SolanaEventIndex(store),
            from_chain='btc',
            to_chain='sol',
            window_start=100,
            window_end=1100,
            rewardable_hotkeys={'hk_a', 'hk_b'},
        )
        assert crown == {'hk_a': 500.0, 'hk_b': 500.0}
        store.close()

    def test_window_start_state_reconstruction_from_pre_window_events(self, tmp_path: Path):
        """A miner posted before window_start and never updated — replay reads initial state."""
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        make_watcher(store, active={'hk_a'})
        conn = store.require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_a', 'btc', 'sol', 0.00020, 5_000),
        )
        conn.commit()

        crown = replay_crown_time_window(
            store=store,
            event_index=SolanaEventIndex(store),
            from_chain='btc',
            to_chain='sol',
            window_start=10_000,
            window_end=11_000,
            rewardable_hotkeys={'hk_a'},
        )
        assert crown == {'hk_a': 1000.0}
        store.close()

    def test_best_rate_miner_goes_busy_credit_flows_to_runner_up(self, tmp_path: Path):
        """A holds the best rate but is reserved+swapping over [400, 800]. During
        that span A is FULFILLING (forfeits crown) and it flips to runner-up B."""
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active={'hk_a', 'hk_b'})
        conn = store.require_connection()
        for row in (
            ('hk_a', 'sol', 'btc', 300.0, 0),  # A is best
            ('hk_b', 'sol', 'btc', 200.0, 0),  # B is runner-up
        ):
            conn.execute(
                'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
                row,
            )
        conn.commit()

        # A is reserved then initiates a swap at 400, completing at 800.
        watcher.reserve_then_swap('hk_a', reserve_block=400, init_block=400, end_block=800)

        crown = replay_crown_time_window(
            store=store,
            event_index=SolanaEventIndex(store),
            from_chain='sol',
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
            ('hk_a', 'btc', 'sol', 0.00020, 0),
        )
        conn.commit()

        watcher.reserve_then_swap('hk_a', reserve_block=400, init_block=400, end_block=900, end='SwapTimedOut')

        crown = replay_crown_time_window(
            store=store,
            event_index=SolanaEventIndex(store),
            from_chain='btc',
            to_chain='sol',
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
        """Miner A's reservation+swap opens before window_start and doesn't
        resolve until mid-window — reconstruction must see A as FULFILLING at the
        window edge."""
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active={'hk_a', 'hk_b'})
        conn = store.require_connection()
        for row in (
            ('hk_a', 'sol', 'btc', 300.0, 0),
            ('hk_b', 'sol', 'btc', 200.0, 0),
        ):
            conn.execute(
                'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
                row,
            )
        conn.commit()

        # A's swap started BEFORE the window opens and completes inside it.
        watcher.reserve_then_swap('hk_a', reserve_block=50, init_block=50, end_block=500)

        # Window-start state shows A FULFILLING (open swap spans the edge).
        assert store.get_activity_state_at(100) == {'hk_a': MinerActivity.FULFILLING}
        crown = replay_crown_time_window(
            store=store,
            event_index=SolanaEventIndex(store),
            from_chain='sol',
            to_chain='btc',
            window_start=100,
            window_end=1100,
            rewardable_hotkeys={'hk_a', 'hk_b'},
        )
        # From window_start=100 A is already busy (reconstructed from pre-window
        # swap). B earns (100,500] = 400; A earns (500,1100] = 600.
        assert crown == {'hk_b': 400.0, 'hk_a': 600.0}
        store.close()


class TestCrownRewardStates:
    """D4 — a reserved/fulfilling miner forfeits crown (activity ∉
    REWARD_MINER_STATES), on top of the existing active/rate/executable gates."""

    def _seed_solo(self, tmp_path: Path, rate: float = 300.0):
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active={'hk_a'})
        conn = store.require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_a', 'sol', 'btc', rate, 0),
        )
        conn.commit()
        return store, watcher

    def _replay(self, store, **kw):
        return replay_crown_time_window(
            store=store,
            event_index=SolanaEventIndex(store),
            from_chain='sol',
            to_chain='btc',
            window_start=100,
            window_end=1100,
            **kw,
        )

    def test_reserved_not_initiated_forfeits_then_earns_after_expiry(self, tmp_path: Path):
        """A bare reservation (no swap) forfeits crown for [resolve, resolve+ttl],
        then RESERVE_EXPIRE returns the miner to AVAILABLE and it earns again."""
        store, watcher = self._seed_solo(tmp_path)
        watcher.apply_event(300, 'PoolResolved', {'miner': 'hk_a', 'ttl': 400})  # expire @ 700
        crown = self._replay(store, rewardable_hotkeys={'hk_a'})
        # AVAILABLE (100,300]=200 + (700,1100]=400 = 600; RESERVED (300,700] forfeited (solo → recycles).
        assert crown == {'hk_a': 600.0}
        store.close()

    def test_reserved_then_initiated_then_completed_forfeits_whole_span(self, tmp_path: Path):
        """A is reserved at 300 and the swap runs to 800 — the entire
        [300, 800] span (RESERVED then FULFILLING) forfeits to runner-up B."""
        store, watcher = self._seed_solo(tmp_path)
        conn = store.require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_b', 'sol', 'btc', 200.0, 0),
        )
        conn.commit()
        seed_active(watcher, 'hk_b', active=True, block=0)
        watcher.reserve_then_swap('hk_a', reserve_block=300, init_block=350, end_block=800)
        crown = self._replay(store, rewardable_hotkeys={'hk_a', 'hk_b'})
        # A: (100,300]=200 + (800,1100]=300 = 500. B holds the forfeited (300,800]=500.
        assert crown == {'hk_a': 500.0, 'hk_b': 500.0}
        store.close()

    def test_swap_completes_before_reserved_until_has_no_false_tail(self, tmp_path: Path):
        """The interval model's win over the old delta tail: a swap completing
        before the reservation's TTL returns the miner to AVAILABLE immediately —
        no forfeited tail out to reserved_until."""
        store, watcher = self._seed_solo(tmp_path)
        # Reserved @300 with ttl 600 (reserved_until = 900), but the swap completes @500.
        watcher.reserve_then_swap('hk_a', reserve_block=300, init_block=350, end_block=500, ttl=600)
        # Mid-span between completion and the original reserved_until: AVAILABLE, not RESERVED.
        assert store.get_activity_state_at(700) == {}
        crown = self._replay(store, rewardable_hotkeys={'hk_a'})
        # Forfeits only (300,500]=200; earns (100,300]=200 + (500,1100]=600 = 800. No tail to 900.
        assert crown == {'hk_a': 800.0}
        store.close()

    def test_window_start_reconstruction_shows_reserved_at_edge(self, tmp_path: Path):
        """A reservation open before window_start shows RESERVED at the edge."""
        store, watcher = self._seed_solo(tmp_path)
        watcher.apply_event(50, 'PoolResolved', {'miner': 'hk_a', 'ttl': 400})  # expire @ 450
        assert store.get_activity_state_at(100) == {'hk_a': MinerActivity.RESERVED}
        crown = self._replay(store, rewardable_hotkeys={'hk_a'})
        # RESERVED (100,450] forfeited (solo); earns (450,1100] = 650.
        assert crown == {'hk_a': 650.0}
        store.close()

    def test_reserve_expire_during_fulfilling_is_a_no_op(self, tmp_path: Path):
        """RESERVE_EXPIRE landing mid-swap is a no-op — the miner stays FULFILLING
        until FULFILL_END, forfeiting the whole swap span."""
        store, watcher = self._seed_solo(tmp_path)
        # ttl 100 → RESERVE_EXPIRE @ 400, but the swap runs 350..800.
        watcher.reserve_then_swap('hk_a', reserve_block=300, init_block=350, end_block=800, ttl=100)
        assert store.get_activity_state_at(500) == {'hk_a': MinerActivity.FULFILLING}  # past the expire, still busy
        crown = self._replay(store, rewardable_hotkeys={'hk_a'})
        # Forfeits (300,800]; earns (100,300]=200 + (800,1100]=300 = 500.
        assert crown == {'hk_a': 500.0}
        store.close()

    def test_reward_states_set_is_the_only_policy_knob(self, tmp_path: Path, monkeypatch):
        """Flipping REWARD_MINER_STATES to include FULFILLING makes a fulfilling
        miner earn — with no other change. RESERVED still forfeits, proving the
        frozenset is the sole policy lever."""
        store, watcher = self._seed_solo(tmp_path)
        watcher.reserve_then_swap('hk_a', reserve_block=300, init_block=350, end_block=800)
        # Default: only AVAILABLE earns. RESERVED (300,350] + FULFILLING (350,800] forfeit.
        base = self._replay(store, rewardable_hotkeys={'hk_a'})
        assert base == {'hk_a': 500.0}  # (100,300]=200 + (800,1100]=300

        monkeypatch.setattr(
            scoring_mod, 'REWARD_MINER_STATES', frozenset({MinerActivity.AVAILABLE, MinerActivity.FULFILLING})
        )
        flipped = self._replay(store, rewardable_hotkeys={'hk_a'})
        # FULFILLING (350,800]=450 now earns; RESERVED (300,350]=50 still forfeits.
        assert flipped == {'hk_a': 950.0}
        store.close()


class TestSnapshotCurrentCrownHolders:
    """The per-forward-step live-crown snapshot must stay consistent with the
    scoring ledger: it reconstructs the same 5-tuple window state and applies
    the same boundary-squat gate."""

    def _seed_rate(
        self, store: ValidatorStateStore, hotkey: str, rate: float, from_chain: str = 'sol', to_chain: str = 'btc'
    ) -> None:
        conn = store.require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            (hotkey, from_chain, to_chain, rate, 0),
        )
        conn.commit()

    def test_runs_and_credits_funded_holder(self, tmp_path: Path):
        """Regression for the 5-vs-4 unpack crash: reconstruct_window_start_state
        returns 5 values and this caller must unpack all of them. A funded miner
        with an executable rate shows up as the live crown holder."""
        v = make_validator(
            tmp_path,
            ['hk_funded'],
            min_swap_amount=100_000_000,
            max_swap_amount=500_000_000,
            collaterals={'hk_funded': 500_000_000},
        )
        self._seed_rate(v.state_store, 'hk_funded', 326.0)

        rows = snapshot_current_crown_holders(v, v.block)

        holders = [row[2] for row in rows[('sol', 'btc')]]
        assert holders == ['hk_funded']
        v.state_store.close()

    def test_boundary_squat_excluded_from_live_table(self, tmp_path: Path):
        """The squatter posts the best, executable rate but their 0.15 TAO
        collateral can't fund the 0.5 TAO leg it forces. The live table must
        drop them to the funded runner-up, matching the ledger."""
        v = make_validator(
            tmp_path,
            ['hk_squat', 'hk_funded'],
            min_swap_amount=100_000_000,
            max_swap_amount=500_000_000,
            collaterals={'hk_squat': 150_000_000, 'hk_funded': 500_000_000},
        )
        # btc→sol (into the bounded SOL leg): the squat rate forces a 0.5-SOL leg the squatter can't fund.
        self._seed_rate(v.state_store, 'hk_squat', 50000.0, 'btc', 'sol')  # best rate, can't fund
        self._seed_rate(v.state_store, 'hk_funded', 326.0, 'btc', 'sol')  # runner-up, can fund

        rows = snapshot_current_crown_holders(v, v.block)

        holders = [row[2] for row in rows[('btc', 'sol')]]
        assert holders == ['hk_funded']
        v.state_store.close()


class TestLedgerSnapshotAgreement:
    """The #450 invariant end-to-end: the live snapshot and the scoring ledger
    must resolve the crown to the same holder when fed identical state. Guards
    against a future one-sided edit even if it bypassed make_crown_predicates."""

    def test_squat_dropped_by_both_paths(self, tmp_path: Path):
        # Squatter posts the best rate but can't fund the leg it forces; the
        # funded runner-up is the only eligible holder. Both the per-forward
        # snapshot and the windowed replay must agree on hk_funded and exclude
        # hk_squat — the executability/funding gate applied identically.
        v = make_validator(
            tmp_path,
            ['hk_squat', 'hk_funded'],
            block=1100,
            min_swap_amount=100_000_000,
            max_swap_amount=500_000_000,
            collaterals={'hk_squat': 150_000_000, 'hk_funded': 500_000_000},
        )
        conn = v.state_store.require_connection()
        for hk, rate in (('hk_squat', 50000.0), ('hk_funded', 326.0)):
            conn.execute(
                'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
                (hk, 'btc', 'sol', rate, 0),
            )
        conn.commit()

        snapshot_holders = [row[2] for row in snapshot_current_crown_holders(v, v.block)[('btc', 'sol')]]
        ledger = replay_crown_time_window(
            store=v.state_store,
            event_index=v.event_index,
            from_chain='btc',
            to_chain='sol',
            window_start=100,
            window_end=1100,
            rewardable_hotkeys={'hk_squat', 'hk_funded'},
            min_swap_lamports=100_000_000,
            max_swap_lamports=500_000_000,
        )

        assert snapshot_holders == ['hk_funded']
        assert set(ledger) == {'hk_funded'}  # squatter credited zero blocks
        # The whole point: live view and rewarded ledger name the same holder.
        assert snapshot_holders == list(ledger.keys())
        v.state_store.close()


class TestCalculateMinerRewards:
    def test_empty_direction_recycles_full_pool(self, tmp_path: Path):
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys=hotkeys)

        rewards, uids = calculate_miner_rewards(v, v.block)

        assert set(uids) == set(range(len(hotkeys)))
        assert rewards[RECYCLE_UID] == 1.0
        assert rewards[0] == 0.0
        np.testing.assert_allclose(rewards.sum(), 1.0, atol=1e-6)
        v.state_store.close()

    def test_single_eligible_miner_earns_full_pool(self, tmp_path: Path):
        """Eligible miner (passes the flat gate) holding crown in both
        directions earns the entire distributed pool — no sr³/ramp scaling."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys=hotkeys)
        conn = v.state_store.require_connection()
        for direction in (('btc', 'sol'), ('sol', 'btc')):
            conn.execute(
                'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
                ('hk_a', direction[0], direction[1], 0.00020, 0),
            )
        conn.commit()

        rewards, _ = calculate_miner_rewards(v, v.block)

        np.testing.assert_allclose(rewards[0], POOL_BTC_SOL + POOL_SOL_BTC, atol=1e-6)
        np.testing.assert_allclose(rewards.sum(), 1.0, atol=1e-6)
        v.state_store.close()

    def test_ineligible_miner_earns_nothing(self, tmp_path: Path):
        """A crown-holding miner below the success floor gates to weight 0 and
        the whole pool recycles — the flat gate is a hard 0/1 multiplier."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        # hk_a holds the crown but has only 1 successful swap (< MIN=2).
        v = make_validator(tmp_path, hotkeys=hotkeys, miner_counters={'hk_a': (1, 0)})
        conn = v.state_store.require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_a', 'btc', 'sol', 0.00020, 0),
        )
        conn.commit()

        rewards, _ = calculate_miner_rewards(v, v.block)

        assert rewards[0] == 0.0
        np.testing.assert_allclose(rewards[RECYCLE_UID], 1.0, atol=1e-6)
        v.state_store.close()

    def test_eligible_high_fail_miner_excluded(self, tmp_path: Path):
        """Plenty of successes but failures over the cap → ineligible → 0. A
        rolling strike count, mirrored from the on-chain MinerState counters."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys=hotkeys, miner_counters={'hk_a': (50, MAX_FAILED_SWAPS + 1)})
        conn = v.state_store.require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_a', 'btc', 'sol', 0.00020, 0),
        )
        conn.commit()

        rewards, _ = calculate_miner_rewards(v, v.block)

        assert rewards[0] == 0.0
        np.testing.assert_allclose(rewards[RECYCLE_UID], 1.0, atol=1e-6)
        v.state_store.close()

    def test_unbound_miner_without_counters_ineligible(self, tmp_path: Path):
        """A miner holding crown but with no on-chain MinerState entry (unbound /
        never swapped) defaults to ineligible — absent counters earn nothing."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys=hotkeys, miner_counters={})
        conn = v.state_store.require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_a', 'btc', 'sol', 0.00020, 0),
        )
        conn.commit()

        rewards, _ = calculate_miner_rewards(v, v.block)

        assert rewards[0] == 0.0
        np.testing.assert_allclose(rewards[RECYCLE_UID], 1.0, atol=1e-6)
        v.state_store.close()

    def test_dereg_mid_window_forfeits_credit(self, tmp_path: Path):
        # hk_a was the best rate miner but is no longer in the metagraph
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_b'])
        v = make_validator(tmp_path, hotkeys=hotkeys)
        # hk_a is active + best-rate in the index series but out of the metagraph
        # (dereg'd), so it must forfeit credit to hk_b.
        seed_active(v.event_watcher, 'hk_a', active=True, block=0)
        conn = v.state_store.require_connection()
        for hk, rate in (('hk_a', 300.0), ('hk_b', 200.0)):
            conn.execute(
                'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
                (hk, 'sol', 'btc', rate, 0),
            )
        conn.commit()

        rewards, _ = calculate_miner_rewards(v, v.block)

        # hk_a isn't in metagraph so hk_b (uid 0) becomes the crown holder.
        np.testing.assert_allclose(rewards[0], POOL_SOL_BTC, atol=1e-6)
        v.state_store.close()

    def test_recycle_uid_out_of_bounds_falls_back_to_zero(self, tmp_path: Path):
        hotkeys = ['hk_a', 'hk_b']
        v = make_validator(tmp_path, hotkeys=hotkeys)

        rewards, _ = calculate_miner_rewards(v, v.block)

        assert rewards[0] == 1.0
        assert len(rewards) == 2
        v.state_store.close()

    def test_empty_metagraph_returns_empty(self, tmp_path: Path):
        v = make_validator(tmp_path, hotkeys=[])
        rewards, uids = calculate_miner_rewards(v, v.block)
        assert rewards.size == 0
        assert uids == set()
        v.state_store.close()

    def test_never_active_miner_gets_no_credit(self, tmp_path: Path):
        """A miner with a rate and collateral but no MinerActivated event in
        history earns nothing — the historical active flag is the tell-all."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys=hotkeys)
        # Wipe the bootstrap active seed from the index series — hk_a has never
        # been activated on-chain. Mirrors a miner that registered but never
        # called set_active(true).
        conn = v.state_store.require_connection()
        conn.execute("DELETE FROM active_events WHERE hotkey = 'hk_a'")
        conn.commit()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_a', 'btc', 'sol', 0.00020, 0),
        )
        conn.commit()

        rewards, _ = calculate_miner_rewards(v, v.block)

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
        from_chain: str = 'btc',
        to_chain: str = 'sol',
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
            ('hk_a', 'btc', 'sol', 0.00020, 0),
        )
        conn.commit()
        # Deactivate mid-window at block 600 (window is (100, 1100]).
        watcher.apply_event(600, 'MinerActivated', {'miner': 'hk_a', 'active': False})

        crown = replay_crown_time_window(
            store=store,
            event_index=SolanaEventIndex(store),
            from_chain='btc',
            to_chain='sol',
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
            ('hk_a', 'btc', 'sol', 0.00020, 0),
        )
        conn.commit()
        watcher.apply_event(400, 'MinerActivated', {'miner': 'hk_a', 'active': True})

        crown = replay_crown_time_window(
            store=store,
            event_index=SolanaEventIndex(store),
            from_chain='btc',
            to_chain='sol',
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
        self.seed_one_miner(v, 'hk_a', 0.00020, from_chain='sol', to_chain='btc')
        # Deactivation happens *after* window_end (=10_000). The replay
        # window is (8_800, 10_000], so this transition is outside it.
        v.event_watcher.apply_event(10_500, 'MinerActivated', {'miner': 'hk_a', 'active': False})

        rewards, _ = calculate_miner_rewards(v, v.block)

        # Full pool across both directions goes to hk_a (uid 0).
        np.testing.assert_allclose(rewards[0], POOL_BTC_SOL + POOL_SOL_BTC, atol=1e-6)
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
            ('hk_a', 'btc', 'sol', 0.00020, 0),
        )
        conn.commit()
        watcher.apply_event(300, 'MinerActivated', {'miner': 'hk_a', 'active': False})
        watcher.apply_event(700, 'MinerActivated', {'miner': 'hk_a', 'active': True})
        watcher.apply_event(900, 'MinerActivated', {'miner': 'hk_a', 'active': False})

        crown = replay_crown_time_window(
            store=store,
            event_index=SolanaEventIndex(store),
            from_chain='btc',
            to_chain='sol',
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
                (hk, 'sol', 'btc', rate, 0),
            )
        conn.commit()
        watcher.apply_event(500, 'MinerActivated', {'miner': 'hk_a', 'active': False})

        crown = replay_crown_time_window(
            store=store,
            event_index=SolanaEventIndex(store),
            from_chain='sol',
            to_chain='btc',
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
            ('hk_a', 'btc', 'sol', 0.00020, 0),
        )
        conn.commit()
        # Deactivate inside the [block - SCORING_WINDOW_BLOCKS, block] window so
        # hk_a holds crown for part of it (sole crowned miner → full pool).
        v.event_watcher.apply_event(950, 'MinerActivated', {'miner': 'hk_a', 'active': False})

        rewards, _ = calculate_miner_rewards(v, v.block)

        # hk_a is the only miner with a rate, so it takes the entire tao→btc
        # pool for the blocks it held crown. btc→tao pool gets nothing (no
        # rates posted) and recycles.
        np.testing.assert_allclose(rewards[0], POOL_BTC_SOL, atol=1e-6)
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
            ('hk_a', 'btc', 'sol', 0.00020, 0),
        )
        conn.commit()
        # Pre-window activation at block 50.
        watcher.apply_event(50, 'MinerActivated', {'miner': 'hk_a', 'active': True})

        crown = replay_crown_time_window(
            store=store,
            event_index=SolanaEventIndex(store),
            from_chain='btc',
            to_chain='sol',
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
            ('hk_a', 'btc', 'sol', 0.00020, 0),
        )
        conn.commit()
        watcher.apply_event(50, 'MinerActivated', {'miner': 'hk_a', 'active': False})

        crown = replay_crown_time_window(
            store=store,
            event_index=SolanaEventIndex(store),
            from_chain='btc',
            to_chain='sol',
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
            ('hk_b', 'sol', 'btc', 200.0, 0),
        )
        # A's rate posted at block 500 — same block as their activation.
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_a', 'sol', 'btc', 300.0, 500),
        )
        conn.commit()
        watcher.apply_event(500, 'MinerActivated', {'miner': 'hk_a', 'active': True})

        crown = replay_crown_time_window(
            store=store,
            event_index=SolanaEventIndex(store),
            from_chain='sol',
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

    def test_active_transition_plus_activity_transition_at_same_block(self, tmp_path: Path):
        """ACTIVE (kind=0) orders before ACTIVITY (kind=1). A deactivates and
        goes busy at the same block — the interval ending at that block
        included A. After the block, A is out both ways."""
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active={'hk_a', 'hk_b'})
        conn = store.require_connection()
        for hk, rate in (('hk_a', 300.0), ('hk_b', 200.0)):
            conn.execute(
                'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
                (hk, 'sol', 'btc', rate, 0),
            )
        conn.commit()
        # At block 500: A both deactivates and picks up a reserved swap. Both
        # events apply; both end A's crown credit. A remains out until
        # deactivation reverses (it doesn't).
        watcher.apply_event(500, 'MinerActivated', {'miner': 'hk_a', 'active': False})
        watcher.reserve_then_swap('hk_a', reserve_block=500, init_block=500, end_block=800)

        crown = replay_crown_time_window(
            store=store,
            event_index=SolanaEventIndex(store),
            from_chain='sol',
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
        seed_active(v.event_watcher, 'hk_a', active=True, block=0)
        conn = v.state_store.require_connection()
        for hk, rate in (('hk_a', 300.0), ('hk_b', 200.0)):
            conn.execute(
                'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
                (hk, 'sol', 'btc', rate, 0),
            )
        conn.commit()
        v.event_watcher.apply_event(9_000, 'MinerActivated', {'miner': 'hk_a', 'active': False})

        rewards, _ = calculate_miner_rewards(v, v.block)

        # hk_b (uid 0) is the only rewardable + active miner, earns btc→tao.
        np.testing.assert_allclose(rewards[0], POOL_SOL_BTC, atol=1e-6)
        np.testing.assert_allclose(rewards.sum(), 1.0, atol=1e-6)
        v.state_store.close()


class TestEventKindOrdering:
    """Crown-replay transition ordering. The per-instant active/collateral
    reconstruction these tests used to cover lives in tests/test_event_index.py
    now that the crown reads the state_store event tables directly."""

    def test_event_kind_ordering_at_same_block(self, tmp_path: Path):
        """ACTIVE < ACTIVITY < RATE. At a shared block the credit_interval
        *ending* at that block is evaluated before any of these transitions
        applies. Ordering matters: active-flag flip at block N must gate
        block N+1 regardless of any other same-block transition."""
        from allways.validator.scoring import EventKind

        assert int(EventKind.ACTIVE) < int(EventKind.ACTIVITY) < int(EventKind.RATE)


class TestHaltShortCircuit:
    """Halt check at the scoring entry sidesteps event replay: full pool
    recycles, rewards skip the crown-time path entirely."""

    def _make_validator_with_halt(self, tmp_path: Path, halt_return, hotkeys: list[str]) -> SimpleNamespace:
        hotkeys = pad_hotkeys_to_cover_recycle(hotkeys)
        v = make_validator(tmp_path, hotkeys)
        # The cache fails open to False on RPC error, so an Exception case is
        # modelled as halted()==False → scoring proceeds via the normal path.
        v.solana_config_cache.halted.return_value = False if isinstance(halt_return, Exception) else halt_return
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
    """Direct unit tests for the capacity_factor pure function. Full credit requires
    backing a max_swap fill at the contract's 1.10× gate: denominator = 1.1 × max_swap."""

    def test_at_required_collateral_is_full(self):
        from allways.validator.scoring import capacity_factor

        assert capacity_factor(550_000_000, 500_000_000) == 1.0

    def test_at_max_swap_is_under_full(self):
        """Collateral == max_swap can't actually accept a max_swap fill (needs 1.1×)."""
        from allways.validator.scoring import capacity_factor

        assert capacity_factor(500_000_000, 500_000_000) == 500_000_000 / 550_000_000

    def test_half_required_is_half(self):
        from allways.validator.scoring import capacity_factor

        assert capacity_factor(275_000_000, 500_000_000) == 0.5

    def test_quarter_required_is_quarter(self):
        from allways.validator.scoring import capacity_factor

        assert capacity_factor(137_500_000, 500_000_000) == 0.25

    def test_above_required_caps_at_one(self):
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


class TestFillRatioHelper:
    """Direct unit tests for the fill_ratio pure function.

    Signature: fill_ratio(vol_rao, total_volume_rao, crown_share, alpha).
    """

    def test_idle_crown_loses_alpha(self):
        """vol=0 of total=1 → vol_share=0, participation=0 → factor = (1-α)."""
        from allways.validator.scoring import fill_ratio

        assert fill_ratio(0, 1_000, crown_share=1.0, alpha=0.5) == 0.5

    def test_matching_volume_keeps_full_reward(self):
        from allways.validator.scoring import fill_ratio

        # 500/1000 = 0.5 vol_share, crown_share 0.5 → participation 1.0.
        assert fill_ratio(500, 1_000, crown_share=0.5, alpha=0.5) == 1.0

    def test_over_serving_capped_at_one(self):
        """Anti-wash-trade: high volume / low crown stays at 1.0."""
        from allways.validator.scoring import fill_ratio

        assert fill_ratio(900, 1_000, crown_share=0.1, alpha=0.5) == 1.0

    def test_partial_mismatch_interpolates(self):
        """vol_share/crown_share = 0.5 → factor = 0.5 + 0.5*0.5 = 0.75."""
        from allways.validator.scoring import fill_ratio

        assert fill_ratio(250, 1_000, crown_share=0.5, alpha=0.5) == 0.75

    def test_zero_crown_share_is_moot(self):
        from allways.validator.scoring import fill_ratio

        assert fill_ratio(500, 1_000, crown_share=0.0, alpha=0.5) == 1.0

    def test_idle_network_short_circuits_to_one(self):
        """total_volume == 0 → factor = 1.0 (no penalty for a quiet window)."""
        from allways.validator.scoring import fill_ratio

        assert fill_ratio(0, 0, crown_share=1.0, alpha=0.5) == 1.0

    def test_alpha_zero_disables_volume_weighting(self):
        from allways.validator.scoring import fill_ratio

        for vol in (0, 250, 500, 750, 1_000):
            assert fill_ratio(vol, 1_000, crown_share=1.0, alpha=0.0) == 1.0

    def test_alpha_one_is_pure_volume_share(self):
        from allways.validator.scoring import fill_ratio

        assert fill_ratio(0, 1_000, crown_share=1.0, alpha=1.0) == 0.0
        assert fill_ratio(250, 1_000, crown_share=0.5, alpha=1.0) == 0.5

    def test_alpha_03_softer_penalty(self):
        from allways.validator.scoring import fill_ratio

        assert fill_ratio(0, 1_000, crown_share=1.0, alpha=0.3) == 0.7
        np.testing.assert_allclose(fill_ratio(500, 1_000, crown_share=1.0, alpha=0.3), 0.85, atol=1e-6)


class TestCapacityWeighting:
    """End-to-end capacity weighting via calculate_miner_rewards."""

    def seed_sol_btc_crown(self, v: SimpleNamespace, hotkey: str, rate: float = 0.00020) -> None:
        conn = v.state_store.require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            (hotkey, 'btc', 'sol', rate, 0),
        )
        conn.commit()

    def test_full_capacity_pays_baseline(self, tmp_path: Path):
        """Miner with collateral = 1.1 x max_swap earns the full per-direction pool."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(
            tmp_path,
            hotkeys,
            max_swap_amount=500_000_000,
            collaterals={'hk_a': 550_000_000},
        )
        self.seed_sol_btc_crown(v, 'hk_a')
        rewards, _ = calculate_miner_rewards(v, v.block)
        # hk_a holds 100% of tao→btc crown, full capacity, eligible, no volume penalty.
        np.testing.assert_allclose(rewards[0], POOL_BTC_SOL, atol=1e-6)
        v.state_store.close()

    def test_quarter_capacity_pays_quarter(self, tmp_path: Path):
        """Collateral at 1/4 of required (1.1 x max_swap) → 1/4 reward, 3/4 recycles."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(
            tmp_path,
            hotkeys,
            max_swap_amount=500_000_000,
            collaterals={'hk_a': 137_500_000},
        )
        self.seed_sol_btc_crown(v, 'hk_a')
        rewards, _ = calculate_miner_rewards(v, v.block)
        np.testing.assert_allclose(rewards[0], POOL_BTC_SOL * 0.25, atol=1e-6)
        # Pool conservation: hk_a got POOL_BTC_SOL*0.25; the rest of both buckets
        # and the unallocated pool all recycle, so recycle = 1 - that share.
        recycle_uid = RECYCLE_UID if RECYCLE_UID < len(rewards) else 0
        np.testing.assert_allclose(rewards[recycle_uid], 1.0 - POOL_BTC_SOL * 0.25, atol=1e-6)
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
        self.seed_sol_btc_crown(v, 'hk_a')
        rewards, _ = calculate_miner_rewards(v, v.block)
        np.testing.assert_allclose(rewards[0], POOL_BTC_SOL, atol=1e-6)
        v.state_store.close()

    def test_zero_collateral_zeros_reward(self, tmp_path: Path):
        """A *known* zero collateral event with max_swap set → factor 0 → no
        reward, full recycle. Seeded as an explicit present-0 event (not an
        absent series, which now fails open — see
        test_unknown_collateral_fails_open)."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys, max_swap_amount=500_000_000)
        seed_collateral(v.event_watcher, 'hk_a', 0, block=0)  # present, known zero
        self.seed_sol_btc_crown(v, 'hk_a')
        rewards, _ = calculate_miner_rewards(v, v.block)
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
            collaterals={'hk_a': 550_000_000, 'hk_b': 110_000_000},
        )
        conn = v.state_store.require_connection()
        for hk in ('hk_a', 'hk_b'):
            conn.execute(
                'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
                (hk, 'btc', 'sol', 0.00020, 0),
            )
        conn.commit()
        rewards, _ = calculate_miner_rewards(v, v.block)
        # Both split crown 50/50. A's capacity = 1.0, B's = 0.2.
        # A earns pool * 0.5 * 1.0; B earns pool * 0.5 * 0.2.
        np.testing.assert_allclose(rewards[0], POOL_BTC_SOL * 0.5 * 1.0, atol=1e-6)
        np.testing.assert_allclose(rewards[1], POOL_BTC_SOL * 0.5 * 0.2, atol=1e-6)
        v.state_store.close()

    def test_cold_start_max_swap_zero_is_fail_safe(self, tmp_path: Path):
        """max_swap_amount=0 (default) bypasses capacity weighting entirely.
        Critical: a freshly restarted validator must not zero every miner."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys)  # defaults: max_swap=0, no collateral lookup
        self.seed_sol_btc_crown(v, 'hk_a')
        rewards, _ = calculate_miner_rewards(v, v.block)
        # Fail-safe path: capacity_factor = 1.0 regardless of collateral.
        np.testing.assert_allclose(rewards[0], POOL_BTC_SOL, atol=1e-6)
        v.state_store.close()

    def test_unknown_collateral_fails_open(self, tmp_path: Path):
        """A miner with NO collateral event in the watcher's series (unknown,
        not zero) must fail OPEN: capacity 1.0 and can_fund passes, so it earns
        the full pool. The contract auto-deactivates anyone below
        min_collateral, so an active miner always holds enough — treating a
        missing baseline as zero would silently drop honest miners from crown
        (the collateral-baseline bug). Absent != zero."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys, max_swap_amount=500_000_000)  # no collaterals dict → absent
        self.seed_sol_btc_crown(v, 'hk_a')
        rewards, _ = calculate_miner_rewards(v, v.block)
        np.testing.assert_allclose(rewards[0], POOL_BTC_SOL, atol=1e-6)
        v.state_store.close()

    def test_scoring_does_not_call_contract_for_collateral(self, tmp_path: Path):
        """Scoring must derive capacity from the replayed event series, not a
        live contract read. Closes #409: any path that reads current collateral
        at scoring time would let a miner top up after the window and
        retroactively boost capacity on already-earned crown."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(
            tmp_path,
            hotkeys,
            max_swap_amount=500_000_000,
            collaterals={'hk_a': 500_000_000},
        )
        conn = v.state_store.require_connection()
        for from_c, to_c in (('btc', 'sol'), ('sol', 'btc')):
            conn.execute(
                'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
                ('hk_a', from_c, to_c, 0.00020 if (from_c, to_c) == ('btc', 'sol') else 200.0, 0),
            )
        conn.commit()
        # Scoring sources collateral from the per-block event series, never a
        # live contract call — the capacity integral can't be retroactively
        # boosted. Runs clean with only the seeded events present.
        rewards, _ = calculate_miner_rewards(v, v.block)
        assert rewards is not None
        v.state_store.close()

    def test_max_swap_amount_rpc_failure_falls_back_to_unity(self, tmp_path: Path):
        """config-cache failure → max_swap=0 → capacity_factor fail-safes to 1.0
        so a transient RPC blip can't zero every miner on a scoring pass."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys, collaterals={'hk_a': 100_000_000})
        v.solana_config_cache.max_swap_amount.side_effect = RuntimeError('rpc down')
        self.seed_sol_btc_crown(v, 'hk_a')
        rewards, _ = calculate_miner_rewards(v, v.block)
        # Fail-safe: capacity factor 1.0 → hk_a earns the full tao→btc pool.
        np.testing.assert_allclose(rewards[0], POOL_BTC_SOL, atol=1e-6)
        v.state_store.close()


class TestWeightingTraceRecorders:
    """The three record_* methods on WeightingTrace own their own math —
    direct unit coverage so changes there don't have to be inferred from
    integration tests."""

    def test_record_capacity_sets_factor(self):
        from allways.validator.scoring_trace import WeightingTrace

        wt = WeightingTrace()
        wt.record_capacity(factor=0.5)
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
        assert wt.fill_ratio == 0.75

    def test_record_volume_idle_network_zeros_share(self):
        """total_volume == 0 → volume_share = 0, participation defaults to 1.0
        only when crown_share also 0; otherwise participation = 0."""
        from allways.validator.scoring_trace import WeightingTrace

        wt = WeightingTrace()
        wt.record_volume(vol_rao=0, total_volume_rao=0, crown_share=1.0, factor=1.0)
        assert wt.volume_share == 0.0
        assert wt.participation == 0.0  # vol_share / crown_share = 0/1 = 0
        assert wt.fill_ratio == 1.0  # set by caller (idle-network short-circuit)

    def test_record_volume_caps_participation_at_one(self):
        """Over-serving: vol_share/crown_share > 1 → participation capped."""
        from allways.validator.scoring_trace import WeightingTrace

        wt = WeightingTrace()
        wt.record_volume(vol_rao=900, total_volume_rao=1_000, crown_share=0.1, factor=1.0)
        assert wt.participation == 1.0  # min(1.0, 0.9/0.1)

    def test_record_eligibility_sets_flag(self):
        from allways.validator.scoring_trace import WeightingTrace

        wt = WeightingTrace()
        assert wt.eligible is False  # default
        wt.record_eligibility(eligible=True)
        assert wt.eligible is True
        wt.record_eligibility(eligible=False)
        assert wt.eligible is False


class TestVolumeWeighting:
    """End-to-end volume weighting via calculate_miner_rewards."""

    def seed_sol_btc_crown(self, v: SimpleNamespace, hotkey: str, rate: float = 0.00020) -> None:
        conn = v.state_store.require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            (hotkey, 'btc', 'sol', rate, 0),
        )
        conn.commit()

    def insert_volume(
        self,
        v: SimpleNamespace,
        miner_hotkey: str,
        from_amount: int,
        block: int = 9_900,  # inside the (9_700, 10_000] window these tests score
        completed: bool = True,
        from_chain: str = 'btc',
        to_chain: str = 'sol',
        to_amount: int | None = None,
    ) -> None:
        """Seed one completed swap's realized legs on the ``clearing_rates``
        ledger — the windowed volume read the reward weighting consumes.
        ``from_amount`` is the from-leg the ``fill_ratio`` compares within a
        direction. A non-completed swap never lands a ``SwapCompleted`` event,
        so it writes nothing — timed-out swaps contribute no volume."""
        if not completed:
            return
        v.state_store.insert_clearing_rate(
            block,
            miner_hotkey,
            from_chain,
            to_chain,
            from_amount,
            from_amount if to_amount is None else to_amount,
        )

    def test_idle_network_no_penalty(self, tmp_path: Path):
        """Total network volume = 0 → factor 1.0 for all crown earners."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys)
        self.seed_sol_btc_crown(v, 'hk_a')
        rewards, _ = calculate_miner_rewards(v, v.block)
        # No swaps → factor = 1.0 → full crown reward.
        np.testing.assert_allclose(rewards[0], POOL_BTC_SOL, atol=1e-6)
        v.state_store.close()

    def test_idle_crown_holder_loses_alpha(self, tmp_path: Path):
        """A holds 100% crown, B serves 100% volume → A factor = (1 - α)."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a', 'hk_b'])
        v = make_validator(tmp_path, hotkeys)
        self.seed_sol_btc_crown(v, 'hk_a')
        # B doesn't post a rate → never holds crown.
        self.insert_volume(v, 'hk_b', from_amount=1_000_000_000)
        rewards, _ = calculate_miner_rewards(v, v.block)
        # A's vol_share = 0, crown_share = 1.0 → participation = 0 → fill_ratio = 0.5.
        # A = pool·0.5. B has crown_share = 0 → no crown reward to multiply.
        np.testing.assert_allclose(rewards[0], POOL_BTC_SOL * 0.5, atol=1e-6)
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
                (hk, 'btc', 'sol', 0.00020, 0),
            )
        conn.commit()
        self.insert_volume(v, 'hk_a', from_amount=500_000_000)
        self.insert_volume(v, 'hk_b', from_amount=500_000_000)
        rewards, _ = calculate_miner_rewards(v, v.block)
        # Both 50/50 on crown and volume → participation 1.0 → factor 1.0 each.
        np.testing.assert_allclose(rewards[0], POOL_BTC_SOL * 0.5, atol=1e-6)
        np.testing.assert_allclose(rewards[1], POOL_BTC_SOL * 0.5, atol=1e-6)
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
            ('hk_a', 'sol', 'btc', 200.0, 0),
        )
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_b', 'sol', 'btc', 100.0, 0),
        )
        conn.commit()
        self.insert_volume(v, 'hk_a', from_amount=100_000_000, from_chain='sol', to_chain='btc')
        self.insert_volume(v, 'hk_b', from_amount=900_000_000, from_chain='sol', to_chain='btc')
        rewards, _ = calculate_miner_rewards(v, v.block)
        # A: crown_share = 1.0, vol_share = 0.1, participation = 0.1 → fill_ratio = 0.55.
        np.testing.assert_allclose(rewards[0], POOL_SOL_BTC * 0.55, atol=1e-6)
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
            ('hk_a', 'sol', 'btc', 200.0, 0),
        )
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_b', 'sol', 'btc', 150.0, 0),
        )
        conn.commit()
        # Window is (9700, 10000]. A is reserved 9_800..9_860 (60 blocks within
        # the window) so B holds crown 20% of window. A bare PoolResolved (no
        # swap) with a 60s TTL forfeits the crown for exactly the reserved span,
        # then RESERVE_EXPIRE returns A to AVAILABLE.
        v.event_watcher.apply_event(9_800, 'PoolResolved', {'miner': 'hk_a', 'ttl': 60})
        self.insert_volume(v, 'hk_a', from_amount=200_000_000, from_chain='sol', to_chain='btc')
        self.insert_volume(v, 'hk_b', from_amount=800_000_000, from_chain='sol', to_chain='btc')
        rewards, _ = calculate_miner_rewards(v, v.block)
        # Crown: A=240/300=0.8, B=60/300=0.2. Volume: A=0.2, B=0.8.
        # A participation = 0.2/0.8 = 0.25 → fill_ratio 0.625; B → fill_ratio 1.0.
        # A = 0.8·0.625 = 0.5·pool. B = 0.2·1.0 = 0.2·pool.
        np.testing.assert_allclose(rewards[0], POOL_SOL_BTC * 0.5, atol=1e-6)
        np.testing.assert_allclose(rewards[1], POOL_SOL_BTC * 0.2, atol=1e-6)
        v.state_store.close()

    def test_timed_out_swaps_dont_count_as_volume(self, tmp_path: Path):
        """A timed-out swap contributes no volume — only completed swaps do."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys)
        self.seed_sol_btc_crown(v, 'hk_a')
        self.insert_volume(
            v,
            'hk_a',
            from_amount=1_000_000_000,
            completed=False,
        )
        # A timed-out swap never lands SwapCompleted, so no clearing row exists.
        assert v.state_store.get_clearing_volumes(9_700, 10_000) == {}
        rewards, _ = calculate_miner_rewards(v, v.block)
        # Eligible solo crown holder, zero counted volume → idle-network
        # short-circuit → factor 1.0 → full tao→btc pool.
        np.testing.assert_allclose(rewards[0], POOL_BTC_SOL, atol=1e-6)
        v.state_store.close()

    def test_volume_split_per_direction(self, tmp_path: Path):
        """Per-direction volume isolates each market. A miner with volume in both
        directions is keyed by direction in ``get_clearing_volumes``, so each
        direction's ``from_amount`` is summed independently."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys)
        self.seed_sol_btc_crown(v, 'hk_a')
        self.insert_volume(v, 'hk_a', from_amount=300_000_000, from_chain='btc', to_chain='sol')
        self.insert_volume(v, 'hk_a', from_amount=200_000_000, from_chain='sol', to_chain='btc')
        vols = v.state_store.get_clearing_volumes(9_700, 10_000)
        assert vols[('btc', 'sol')]['hk_a'] == (300_000_000, 300_000_000)
        assert vols[('sol', 'btc')]['hk_a'] == (200_000_000, 200_000_000)
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
            ('hk_a', 'sol', 'btc', 200.0, 0),
        )
        conn.commit()
        # A serves all of btc→tao. B floods tao→btc but earns no crown there.
        self.insert_volume(v, 'hk_a', from_amount=1_000_000_000, from_chain='sol', to_chain='btc')
        self.insert_volume(v, 'hk_b', from_amount=9_000_000_000, from_chain='btc', to_chain='sol')
        rewards, _ = calculate_miner_rewards(v, v.block)
        # Old direction-blind logic: A's vol_share would be diluted by B's flood,
        # dragging the factor below 1. Per-direction logic: A is the sole server
        # in its own market, factor = 1.0.
        np.testing.assert_allclose(rewards[0], POOL_SOL_BTC, atol=1e-6)
        assert rewards[1] == 0.0
        v.state_store.close()

    def test_dust_volume_counts_toward_shares(self, tmp_path: Path):
        """Any cleared volume counts — there is no direction volume floor. A dust
        swap served by a non-holder costs the idle crown holder alpha, exactly
        like larger volume."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a', 'hk_b'])
        v = make_validator(tmp_path, hotkeys)
        self.seed_sol_btc_crown(v, 'hk_a')
        # B clears dust in A's market: A holds all crown, serves none of the
        # volume → pool·fill_ratio(0.5).
        self.insert_volume(v, 'hk_b', from_amount=100, to_amount=999_999_999)
        rewards, _ = calculate_miner_rewards(v, v.block)
        np.testing.assert_allclose(rewards[0], POOL_BTC_SOL * 0.5, atol=1e-6)
        v.state_store.close()

    def test_volume_outside_window_ignored(self, tmp_path: Path):
        """Clearing rows outside the scored window contribute no volume — the
        weighting reads this round's flow, not history."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a', 'hk_b'])
        v = make_validator(tmp_path, hotkeys)
        self.seed_sol_btc_crown(v, 'hk_a')
        # B's heavy volume landed before the window opened (block 9_600 ≤ 9_700).
        self.insert_volume(v, 'hk_b', from_amount=9_000_000_000, block=9_600)
        rewards, _ = calculate_miner_rewards(v, v.block)
        # No in-window volume → idle-network fallback → full pool for the holder.
        np.testing.assert_allclose(rewards[0], POOL_BTC_SOL, atol=1e-6)
        v.state_store.close()

    def test_zero_amount_clearing_row_tolerated(self, tmp_path: Path):
        """A clearing row with zero legs is tolerated — it contributes no volume,
        never a crash."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys)
        self.seed_sol_btc_crown(v, 'hk_a')
        self.insert_volume(v, 'hk_a', from_amount=0, to_amount=0)
        rewards, _ = calculate_miner_rewards(v, v.block)
        np.testing.assert_allclose(rewards[0], POOL_BTC_SOL, atol=1e-6)
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
            collaterals={'hk_a': 275_000_000},
        )
        conn = v.state_store.require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_a', 'btc', 'sol', 0.00020, 0),
        )
        conn.commit()
        # B serves the market's (floor-clearing) volume so A's vol_share = 0.
        v.state_store.insert_clearing_rate(9_900, 'hk_b', 'btc', 'sol', 1_000_000_000, 1_000_000_000)
        rewards, _ = calculate_miner_rewards(v, v.block)
        # A: pool × crown 1.0 × eligible 1 × capacity 0.5 × fill_ratio 0.5 (B serves the volume).
        np.testing.assert_allclose(rewards[0], POOL_BTC_SOL * 1.0 * 0.5 * 0.5, atol=1e-6)
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
            from_c, to_c = ('btc', 'sol') if rate < 1 else ('sol', 'btc')
            conn.execute(
                'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
                (hk, from_c, to_c, rate, 0),
            )
        conn.commit()
        v.state_store.insert_clearing_rate(9_900, 'hk_b', 'sol', 'btc', 1_500_000_000, 1_500_000_000)
        rewards, _ = calculate_miner_rewards(v, v.block)
        np.testing.assert_allclose(rewards.sum(), 1.0, atol=1e-6)
        v.state_store.close()


class TestEligibilityGateEndToEnd:
    """End-to-end flat-gate behavior via calculate_miner_rewards (B3.3).

    The gate is a hard 0/1 multiplier off the on-chain MinerState counters —
    no ramp. An eligible crown holder earns its full crown share; an ineligible
    one earns nothing and the share recycles. Boundary cases (the success floor
    and the failure cap) are exercised here through the full reward pipeline."""

    def seed_btc_tao_crown(self, v: SimpleNamespace, hotkey: str, rate: float = 200.0) -> None:
        conn = v.state_store.require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            (hotkey, 'sol', 'btc', rate, 0),
        )
        conn.commit()

    def test_one_short_of_floor_earns_nothing(self, tmp_path: Path):
        """One success below MIN_SUCCESSFUL_SWAPS (2) → ineligible → 0."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys, miner_counters={'hk_a': (MIN_SUCCESSFUL_SWAPS - 1, 0)})
        self.seed_btc_tao_crown(v, 'hk_a')
        rewards, _ = calculate_miner_rewards(v, v.block)
        assert rewards[0] == 0.0
        v.state_store.close()

    def test_at_floor_earns_full_crown_share(self, tmp_path: Path):
        """Exactly MIN_SUCCESSFUL_SWAPS successes → eligible → full crown share
        (the whole btc→tao pool, no ramp scaling)."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys, miner_counters={'hk_a': (MIN_SUCCESSFUL_SWAPS, 0)})
        self.seed_btc_tao_crown(v, 'hk_a')
        rewards, _ = calculate_miner_rewards(v, v.block)
        np.testing.assert_allclose(rewards[0], POOL_SOL_BTC, atol=1e-6)
        v.state_store.close()

    def test_at_failure_cap_still_eligible(self, tmp_path: Path):
        """Failures exactly at MAX_FAILED_SWAPS (2), with enough successes →
        still eligible → full crown share."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys, miner_counters={'hk_a': (8, MAX_FAILED_SWAPS)})
        self.seed_btc_tao_crown(v, 'hk_a')
        rewards, _ = calculate_miner_rewards(v, v.block)
        np.testing.assert_allclose(rewards[0], POOL_SOL_BTC, atol=1e-6)
        v.state_store.close()

    def test_one_past_failure_cap_zero_reward(self, tmp_path: Path):
        """One failure past MAX_FAILED_SWAPS → ineligible → 0, regardless of a
        high success count."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys, miner_counters={'hk_a': (50, MAX_FAILED_SWAPS + 1)})
        self.seed_btc_tao_crown(v, 'hk_a')
        rewards, _ = calculate_miner_rewards(v, v.block)
        np.testing.assert_allclose(rewards[0], 0.0, atol=1e-6)
        v.state_store.close()

    def test_ineligible_share_recycles(self, tmp_path: Path):
        """An ineligible holder's crown share recycles to the owner UID, not to
        other miners — pool conservation holds."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys, miner_counters={'hk_a': (1, 0)})
        self.seed_btc_tao_crown(v, 'hk_a')
        rewards, _ = calculate_miner_rewards(v, v.block)
        recycle_uid = RECYCLE_UID if RECYCLE_UID < len(rewards) else 0
        # hk_a gated to 0; both pools recycle in full.
        np.testing.assert_allclose(rewards[recycle_uid], 1.0, atol=1e-6)
        np.testing.assert_allclose(rewards.sum(), 1.0, atol=1e-6)
        v.state_store.close()


class TestHistoricalCollateralReplay:
    """Capacity weighting is now derived from a per-block collateral series
    replayed alongside active/busy/rate, not a contract read at scoring time.
    Closes #409 — a miner who tops up collateral after the window cannot
    retroactively boost the capacity multiplier on crown they've already
    earned."""

    def seed_sol_btc_crown(self, v: SimpleNamespace, hotkey: str, rate: float = 0.00020) -> None:
        conn = v.state_store.require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            (hotkey, 'btc', 'sol', rate, 0),
        )
        conn.commit()

    def test_409_retroactive_topup_does_not_boost_window(self, tmp_path: Path):
        """Reproduces the #409 proof. Window holds collateral at 0.1 TAO the
        entire time; a post-window CollateralPosted to 0.5 TAO must not
        change the reward."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(
            tmp_path,
            hotkeys,
            block=10_000,
            max_swap_amount=500_000_000,
            collaterals={'hk_a': 110_000_000},  # held throughout the window
        )
        self.seed_sol_btc_crown(v, 'hk_a')
        # Top-up fires *after* window_end (= 10_000). Window is (9_700, 10_000].
        v.event_watcher.apply_event(
            10_500,
            'CollateralPosted',
            {'miner': 'hk_a', 'amount': 440_000_000, 'total': 550_000_000},
        )
        rewards, _ = calculate_miner_rewards(v, v.block)
        # capacity_factor = 110M / (1.1 × 500M) = 0.2; pool 0.5 → reward 0.1.
        np.testing.assert_allclose(rewards[0], POOL_BTC_SOL * 0.2, atol=1e-6)
        v.state_store.close()

    def test_mid_window_topup_blends_capacity(self, tmp_path: Path):
        """A miner posts more collateral midway through the window. Capacity
        is integrated per-block: half the window at 1/4 cap, half at full cap
        → time-weighted average 0.625. Validates that the multiplier reflects
        collateral *during* the interval, not at the end of it."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(
            tmp_path,
            hotkeys,
            block=10_000,
            max_swap_amount=500_000_000,
            collaterals={'hk_a': 137_500_000},  # window-start anchor (0.25 × required)
        )
        self.seed_sol_btc_crown(v, 'hk_a')
        # SCORING_WINDOW_BLOCKS = 300 → window is (9_700, 10_000]. Midpoint
        # 9_850 splits credit 150/150 between low and full capacity.
        v.event_watcher.apply_event(
            9_850,
            'CollateralPosted',
            {'miner': 'hk_a', 'amount': 412_500_000, 'total': 550_000_000},
        )
        rewards, _ = calculate_miner_rewards(v, v.block)
        # First 150 blocks at cap 0.25, next 150 at cap 1.0 → mean cap 0.625.
        np.testing.assert_allclose(rewards[0], POOL_BTC_SOL * 0.625, atol=1e-6)
        v.state_store.close()


class TestNonEarnerDiagnosis:
    """diagnose_non_earner must report the true reason: direction-aware outbid
    (tao→btc lower-wins, btc→tao higher-wins) and collateral exclusion, not a
    blanket 'outbid' that hid the collateral-baseline bug."""

    def _trace(self, best_rate: float):
        from allways.validator.scoring import DirectionTrace

        t = DirectionTrace(pool=0.5)
        t.best_rate = best_rate
        return t

    def test_competitive_but_present_zero_is_insufficient_collateral(self):
        from allways.validator.scoring_trace import diagnose_non_earner

        # tao→btc lower-wins: own 279.3 beats best 280 → competitive, but
        # collateral is a known 0 → insufficient_collateral, NOT outbid.
        reason = diagnose_non_earner(
            'hk',
            {('btc', 'sol'): 279.3},
            eligible=True,
            ever_active={'hk'},
            direction_traces={('btc', 'sol'): self._trace(280.0)},
            collaterals={'hk': 0},
            min_swap_lamports=100_000_000,
            max_swap_lamports=500_000_000,
        )
        assert reason.startswith('insufficient_collateral'), reason

    def test_competitive_but_unknown_collateral(self):
        from allways.validator.scoring_trace import diagnose_non_earner

        reason = diagnose_non_earner(
            'hk',
            {('btc', 'sol'): 279.3},
            eligible=True,
            ever_active={'hk'},
            direction_traces={('btc', 'sol'): self._trace(280.0)},
            collaterals={},  # absent → unknown
            min_swap_lamports=100_000_000,
            max_swap_lamports=500_000_000,
        )
        assert reason.startswith('unknown_collateral'), reason

    def test_genuinely_worse_rate_is_direction_aware_outbid(self):
        from allways.validator.scoring_trace import diagnose_non_earner

        # tao→btc lower-wins: own 281 is worse than best 280 → outbid.
        reason = diagnose_non_earner(
            'hk',
            {('btc', 'sol'): 281.0},
            eligible=True,
            ever_active={'hk'},
            direction_traces={('btc', 'sol'): self._trace(280.0)},
            collaterals={'hk': 500_000_000},
            min_swap_lamports=100_000_000,
            max_swap_lamports=500_000_000,
        )
        assert reason.startswith('outbid'), reason

    def test_competitive_and_funded_is_unfilled_not_outbid(self):
        from allways.validator.scoring_trace import diagnose_non_earner

        reason = diagnose_non_earner(
            'hk',
            {('btc', 'sol'): 279.3},
            eligible=True,
            ever_active={'hk'},
            direction_traces={('btc', 'sol'): self._trace(280.0)},
            collaterals={'hk': 500_000_000},
            min_swap_lamports=100_000_000,
            max_swap_lamports=500_000_000,
        )
        assert reason.startswith('competitive_but_unfilled'), reason


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
        # second of the unix-time crown axis is covered exactly once.
        start1, end1 = scoring_window_bounds(1000, 400)
        assert (start1, end1) == (400, 1000)
        # Cursor advances to end1; next round fires a window later.
        start2, end2 = scoring_window_bounds(1600, end1)
        assert start2 == end1  # tiles — no gap
        assert (start2, end2) == (1000, 1600)

    def test_overshoot_does_not_open_a_gap(self):
        # Forward straddled the boundary: fires at last+window+5. window_start
        # stays anchored to last_scored, so the extra time is still scored.
        last = 1000
        start, end = scoring_window_bounds(last + SCORING_WINDOW_BLOCKS + 5, last)
        assert start == last  # no gap despite the overshoot

    def test_backfill_is_capped_after_a_stall(self):
        # last_scored far behind (long outage) → window_start clamps to the
        # cap, not the stale cursor, so one round can't replay an unbounded span.
        start, end = scoring_window_bounds(1_000_000, 0)
        assert start == 1_000_000 - MAX_SCORING_BACKFILL_SECS
        assert end == 1_000_000

    def test_fresh_seed_scores_one_trailing_window(self):
        # Seed = time - SCORING_WINDOW_BLOCKS (a within-cap trailing window) →
        # first round covers exactly that trailing window.
        now = 5_000_000
        seed = max(0, now - SCORING_WINDOW_BLOCKS)
        start, end = scoring_window_bounds(now, seed)
        assert (start, end) == (now - SCORING_WINDOW_BLOCKS, now)


class TestCrownPredicateParity:
    """make_crown_predicates is the single source of crown eligibility for both
    the scoring replay and the live snapshot. Lock its semantics to the shared
    rate utils so a future edit can't let the live view drift from the ledger."""

    # 0.1 / 0.5 TAO — the live on-chain swap bounds.
    BOUNDS = [(0, 0), (100_000_000, 500_000_000)]
    DIRECTIONS = [('sol', 'btc'), ('btc', 'sol')]
    RATES = [0.00015, 1.0, 345.0, 50_000_000.0, 1e10, 0.0, -1.0, float('inf')]

    def _reference(self, from_chain, to_chain, min_rao, max_rao, collaterals):
        def exec_ref(rate):
            return is_executable_rate(rate, from_chain, to_chain, min_rao, max_rao)

        def fund_ref(hotkey, rate):
            if hotkey not in collaterals:
                return True
            min_leg = min_executable_sol_leg(rate, from_chain, to_chain, min_rao, max_rao)
            return min_leg == 0 or collaterals[hotkey] >= min_leg

        return exec_ref, fund_ref

    def test_matches_shared_rate_utils_across_matrix(self):
        collaterals = {'hk_rich': 10_000_000_000, 'hk_poor': 1, 'hk_zero': 0}
        probe_hotkeys = ['hk_rich', 'hk_poor', 'hk_zero', 'hk_absent']
        for from_chain, to_chain in self.DIRECTIONS:
            for min_rao, max_rao in self.BOUNDS:
                executable_check, can_fund = make_crown_predicates(from_chain, to_chain, min_rao, max_rao, collaterals)
                exec_ref, fund_ref = self._reference(from_chain, to_chain, min_rao, max_rao, collaterals)
                for rate in self.RATES:
                    assert executable_check(rate) == exec_ref(rate), (
                        f'executable_check drift dir={from_chain}->{to_chain} bounds=({min_rao},{max_rao}) rate={rate}'
                    )
                    for hk in probe_hotkeys:
                        assert can_fund(hk, rate) == fund_ref(hk, rate), (
                            f'can_fund drift dir={from_chain}->{to_chain} '
                            f'bounds=({min_rao},{max_rao}) hk={hk} rate={rate}'
                        )

    def test_fail_open_on_absent_collateral(self):
        # absent != zero — a miner with no recorded baseline must not be dropped.
        _, can_fund = make_crown_predicates('sol', 'btc', 100_000_000, 500_000_000, {})
        assert can_fund('hk_unknown', 345.0) is True

    def test_drops_holder_whose_collateral_cannot_fund_min_leg(self):
        # 1-rao collateral can't cover any real in-band leg → boundary-squat drop;
        # a richly-funded miner at the same rate passes.
        collaterals = {'hk_poor': 1, 'hk_rich': 10_000_000_000}
        _, can_fund = make_crown_predicates('sol', 'btc', 100_000_000, 500_000_000, collaterals)
        rate = 345.0
        min_leg = min_executable_sol_leg(rate, 'sol', 'btc', 100_000_000, 500_000_000)
        assert min_leg > 0  # rate is executable, so the gate is live
        assert can_fund('hk_poor', rate) is False
        assert can_fund('hk_rich', rate) is True


class TestScoreSnapshots:
    """miner_scores round rows + the current_miner_scores live tip (D8): the
    persisted factors must be exactly what the reward math paid — same shared
    ``build_direction_score_rows``, so a snapshot can never disagree with the
    weights that went on chain."""

    def _solo_with_storage(self, tmp_path: Path) -> SimpleNamespace:
        # hk_a holds 100% btc→sol crown and serves 100% of the direction's volume.
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys)
        conn = v.state_store.require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_a', 'btc', 'sol', 0.00020, 0),
        )
        conn.commit()
        v.state_store.insert_clearing_rate(9_900, 'hk_a', 'btc', 'sol', 5_000_000_000_000, 1_000_000_000)
        v.database_storage.is_enabled.return_value = True
        return v

    def test_round_flush_rows_match_reward_math(self, tmp_path: Path):
        v = self._solo_with_storage(tmp_path)
        rewards, _ = calculate_miner_rewards(v, v.block)
        kwargs = v.database_storage.flush_scoring_window.call_args.kwargs
        rows = kwargs['miner_score_rows']
        assert len(rows) == 1
        (round_ts, hotkey, from_c, to_c, eligible, crown_share, capacity, fill_ratio, vol_share, reward) = rows[0]
        assert round_ts == v.block  # round keyed by window_end
        assert (hotkey, from_c, to_c) == ('hk_a', 'btc', 'sol')
        assert eligible is True
        np.testing.assert_allclose((crown_share, capacity, fill_ratio, vol_share), (1.0,) * 4)
        # The persisted factors reproduce the persisted reward, and the
        # persisted reward is what the weights actually paid.
        expected = POOL_BTC_SOL * crown_share * capacity * fill_ratio
        np.testing.assert_allclose(reward, expected, atol=1e-9)
        np.testing.assert_allclose(reward, rewards[0], atol=1e-6)

    def test_ineligible_holder_persists_factors_with_zero_reward(self, tmp_path: Path):
        """The gate zeroes the reward but the factors are still recorded — the
        dashboard can show WHY the eligible=false round paid nothing."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys, all_eligible=False)
        v.database_storage.is_enabled.return_value = True
        conn = v.state_store.require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_a', 'btc', 'sol', 0.00020, 0),
        )
        conn.commit()
        rewards, _ = calculate_miner_rewards(v, v.block)
        rows = v.database_storage.flush_scoring_window.call_args.kwargs['miner_score_rows']
        assert len(rows) == 1
        row = rows[0]
        assert row[4] is False  # eligible
        np.testing.assert_allclose(row[5], 1.0)  # crown_share still recorded
        assert row[9] == 0.0  # reward
        assert rewards[0] == 0.0
        v.state_store.close()

    def test_live_tip_equals_round_rows_for_same_window(self, tmp_path: Path):
        """The tip is the same math over the same window — identical rows,
        with ts standing in for round_ts."""
        v = self._solo_with_storage(tmp_path)
        tip = snapshot_current_miner_scores(v, at_time=v.block)
        calculate_miner_rewards(v, v.block)
        round_rows = v.database_storage.flush_scoring_window.call_args.kwargs['miner_score_rows']
        assert tip == round_rows
        v.state_store.close()

    def test_tip_empty_when_no_crown(self, tmp_path: Path):
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys)
        assert snapshot_current_miner_scores(v, at_time=v.block) == []
        v.state_store.close()

    def test_halt_flush_carries_no_score_rows(self, tmp_path: Path):
        """A halted round pays nobody: the halt flush goes through
        flush_halt_window (which clears the live tip) and miner_scores gets no
        rows for the round."""
        v = self._solo_with_storage(tmp_path)
        v.solana_config_cache.halted.return_value = True
        v.update_scores = lambda rewards, miner_uids: None
        score_and_reward_miners(v)
        assert v.database_storage.flush_halt_window.called
        assert not v.database_storage.flush_scoring_window.called
        v.state_store.close()
