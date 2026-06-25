"""C5 — crown-time scoring replay tests."""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import numpy as np
import pytest

from allways.constants import (
    DIRECTION_POOLS,
    MAX_FAILED_SWAPS,
    MAX_SCORING_BACKFILL_SECS,
    MIN_SUCCESSFUL_SWAPS,
    RECYCLE_UID,
    SCORING_WINDOW_BLOCKS,
)
from allways.utils.rate import is_executable_rate, min_executable_tao_leg
from allways.validator import scoring as scoring_mod
from allways.validator.event_index import SolanaEventIndex
from allways.validator.event_watcher import ActiveEvent, CollateralEvent, ContractEventWatcher
from allways.validator.scoring import (
    build_direction_volumes,
    build_eligibility,
    calculate_miner_rewards,
    crown_holders_at_instant,
    due_for_scoring,
    is_eligible,
    make_crown_predicates,
    realized_vwap,
    replay_crown_time_window,
    score_and_reward_miners,
    scoring_window_bounds,
    snapshot_current_crown_holders,
)
from allways.validator.state_store import ValidatorStateStore

# Mirror production pool shares so these stay in sync if DIRECTION_POOLS changes.
POOL_TAO_BTC = DIRECTION_POOLS[('tao', 'btc')]
POOL_BTC_TAO = DIRECTION_POOLS[('btc', 'tao')]
MIN_COLLATERAL = 100_000_000  # 0.1 TAO

METADATA_PATH = Path(__file__).parent.parent / 'allways' / 'metadata' / 'allways_swap_manager.json'


def make_metagraph(hotkeys: list[str]) -> SimpleNamespace:
    n = SimpleNamespace(item=lambda: len(hotkeys))
    return SimpleNamespace(n=n, hotkeys=list(hotkeys))


class FakeSolanaClient:
    """Stand-in exposing ``get_all('MinerState')`` for the flat eligibility gate
    (B3.3) and ``get_all('MinerDirectionStats')`` for the realized-volume read
    (B3.5). Each entry's ``miner`` is the test hotkey string; the autouse
    ``_identity_attribution`` fixture makes pubkey→hotkey attribution identity so
    state keys by the same opaque strings the crown tables use. End-to-end
    sr25519 attribution is covered in ``tests/test_eligibility.py``."""

    def __init__(self, miner_counters: dict[str, tuple[int, int]]):
        self._counters = miner_counters
        # (hotkey, from_chain, to_chain) -> (total_from_amount, total_to_amount).
        self._dir_stats: dict[tuple[str, str, str], tuple[int, int]] = {}

    def add_direction_stats(
        self,
        miner_hotkey: str,
        from_amount: int,
        to_amount: int,
        from_chain: str = 'tao',
        to_chain: str = 'btc',
    ) -> None:
        """Seed a ``MinerDirectionStats`` row — the on-chain realized-volume
        ledger that replaces the per-validator ``swap_outcomes`` table (B3.5)."""
        self._dir_stats[(miner_hotkey, from_chain, to_chain)] = (int(from_amount), int(to_amount))

    def get_all(self, name: str):
        if name == 'MinerState':
            return [
                (f'pda_{hk}', SimpleNamespace(miner=hk, successful_swaps=s, failed_swaps=f))
                for hk, (s, f) in self._counters.items()
            ]
        if name == 'MinerDirectionStats':
            return [
                (
                    f'stats_{hk}_{fr}_{to}',
                    SimpleNamespace(
                        miner=hk,
                        from_chain=fr,
                        to_chain=to,
                        total_from_amount=fa,
                        total_to_amount=ta,
                    ),
                )
                for (hk, fr, to), (fa, ta) in self._dir_stats.items()
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
    """Insert an active-flag event into the watcher's in-memory state *and* mirror
    it to the state-store ``active_events`` table. The B3.4 crown reads active state
    through ``SolanaEventIndex`` (the DB tables), so a pre-window anchor that lived
    only in watcher memory would be invisible to scoring — write both."""
    event = ActiveEvent(hotkey=hotkey, active=active, block=block)
    watcher.active_events.append(event)
    watcher.active_events_by_hotkey.setdefault(hotkey, []).append(event)
    watcher.active_events.sort(key=lambda ev: ev.block)
    watcher.active_events_by_hotkey[hotkey].sort(key=lambda ev: ev.block)
    if active:
        watcher.active_miners.add(hotkey)
    else:
        watcher.active_miners.discard(hotkey)
    watcher.state_store.insert_active_event(block, hotkey, active)


def seed_collateral(watcher: ContractEventWatcher, hotkey: str, collateral_rao: int, block: int) -> None:
    """Insert a collateral event into the watcher's in-memory state *and* mirror it
    to the state-store ``collateral_events`` table, so the B3.4 ``SolanaEventIndex``
    crown read sees the anchor (see ``seed_active``)."""
    event = CollateralEvent(hotkey=hotkey, collateral_rao=int(collateral_rao), block=block)
    watcher.collateral_events.append(event)
    watcher.collateral_events_by_hotkey.setdefault(hotkey, []).append(event)
    watcher.collateral_events.sort(key=lambda ev: ev.block)
    watcher.collateral_events_by_hotkey[hotkey].sort(key=lambda ev: ev.block)
    watcher.state_store.insert_collateral_event(block, hotkey, int(collateral_rao))


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
    bounds_cache = MagicMock()
    bounds_cache.max_swap_amount.return_value = max_swap_amount
    bounds_cache.min_swap_amount.return_value = min_swap_amount
    contract_client = MagicMock()
    contract_client.get_miner_collateral.side_effect = lambda hk: collaterals.get(hk, 0)
    database_storage = MagicMock()
    database_storage.is_enabled.return_value = False
    if miner_counters is None:
        default = (MIN_SUCCESSFUL_SWAPS, 0) if all_eligible else (0, 0)
        miner_counters = {hk: default for hk in hotkeys}
    return SimpleNamespace(
        block=block,
        # Seed one window back so scoring_window_bounds yields the same
        # [block - SCORING_WINDOW_BLOCKS, block] window these tests assume.
        last_scored_block=max(0, block - SCORING_WINDOW_BLOCKS),
        metagraph=make_metagraph(hotkeys),
        state_store=store,
        # ``event_watcher`` is kept purely as a convenient DB writer for the
        # crown event tables (its apply_event/seed_* persist to state_store);
        # scoring reads those tables back through ``event_index`` (B3.4).
        event_watcher=watcher,
        event_index=SolanaEventIndex(store),
        bounds_cache=bounds_cache,
        contract_client=contract_client,
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


class TestRealizedVWAP:
    """Realized VWAP = total_to_amount / total_from_amount with exact-integer
    accumulation and a divide-by-zero guard (B3.5)."""

    def test_basic_ratio(self):
        assert realized_vwap(2, 4) == 0.5
        assert realized_vwap(15, 3) == 5.0

    def test_zero_denominator_guarded(self):
        # No executed from-leg volume → 0.0, never a ZeroDivisionError.
        assert realized_vwap(1_000, 0) == 0.0
        assert realized_vwap(0, 0) == 0.0

    def test_negative_denominator_guarded(self):
        # Defensive: a non-positive from-leg can't yield a meaningful rate.
        assert realized_vwap(5, -3) == 0.0

    def test_exact_integer_legs_before_final_ratio(self):
        # Legs are summed as exact integers on-chain; only the final ratio is a
        # float. A u128-scale numerator divides without precision loss in the
        # leading digits.
        to_amt = 10**30 + 7
        from_amt = 10**29
        np.testing.assert_allclose(realized_vwap(to_amt, from_amt), 10.0, rtol=1e-9)


class TestBuildDirectionVolumes:
    """build_direction_volumes attributes on-chain MinerDirectionStats to
    metagraph hotkeys, keyed by direction (B3.5)."""

    def test_maps_pubkey_to_hotkey_uid_per_direction(self):
        metagraph = make_metagraph(['hk_a', 'hk_b'])
        client = FakeSolanaClient({'hk_a': (5, 0), 'hk_b': (5, 0)})
        client.add_direction_stats('hk_a', from_amount=300, to_amount=600, from_chain='tao', to_chain='btc')
        client.add_direction_stats('hk_b', from_amount=100, to_amount=50, from_chain='btc', to_chain='tao')
        vols = build_direction_volumes(client, metagraph)
        assert vols['hk_a'][('tao', 'btc')].from_amount == 300
        assert vols['hk_a'][('tao', 'btc')].vwap == 2.0  # 600 / 300
        assert vols['hk_b'][('btc', 'tao')].from_amount == 100

    def test_off_metagraph_miner_dropped(self):
        metagraph = make_metagraph(['hk_a'])
        client = FakeSolanaClient({'hk_a': (5, 0), 'hk_ghost': (5, 0)})
        client.add_direction_stats('hk_a', from_amount=10, to_amount=10)
        client.add_direction_stats('hk_ghost', from_amount=99, to_amount=99)
        vols = build_direction_volumes(client, metagraph)
        assert set(vols) == {'hk_a'}

    def test_direction_is_lowercased(self):
        metagraph = make_metagraph(['hk_a'])
        client = FakeSolanaClient({'hk_a': (5, 0)})
        client.add_direction_stats('hk_a', from_amount=10, to_amount=10, from_chain='TAO', to_chain='BTC')
        vols = build_direction_volumes(client, metagraph)
        assert ('tao', 'btc') in vols['hk_a']


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
        make_watcher(store, active={'hk_a'})
        conn = store.require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_a', 'tao', 'btc', 0.00015, 0),
        )
        conn.commit()

        crown = replay_crown_time_window(
            store=store,
            event_index=SolanaEventIndex(store),
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
        make_watcher(store, active={'hk_a', 'hk_b'})
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
            event_index=SolanaEventIndex(store),
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
        # Seed enough collateral so hk_sane clears the per-block boundary-squat
        # gate — this test isn't exercising that gate.
        seed_collateral(watcher, 'hk_sane', 500_000_000, block=0)
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
            event_index=SolanaEventIndex(store),
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
            ('hk_squat', 'btc', 'tao', 50000.0, 0),
        )
        conn.commit()

        crown = replay_crown_time_window(
            store=store,
            event_index=SolanaEventIndex(store),
            from_chain='btc',
            to_chain='tao',
            window_start=100,
            window_end=1100,
            rewardable_hotkeys={'hk_squat'},
            min_swap_rao=100_000_000,
            max_swap_rao=500_000_000,
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
            ('hk_squat', 'btc', 'tao', 50000.0, 0),  # best rate, can't fund
            ('hk_funded', 'btc', 'tao', 326.0, 0),  # runner-up, can fund
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
            to_chain='tao',
            window_start=100,
            window_end=1100,
            rewardable_hotkeys={'hk_squat', 'hk_funded'},
            min_swap_rao=100_000_000,
            max_swap_rao=500_000_000,
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
            ('hk_squat', 'btc', 'tao', 50000.0, 0),
        )
        conn.commit()

        crown = replay_crown_time_window(
            store=store,
            event_index=SolanaEventIndex(store),
            from_chain='btc',
            to_chain='tao',
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
            ('hk_squat', 'btc', 'tao', 50000.0, 0),
        )
        conn.commit()

        crown = replay_crown_time_window(
            store=store,
            event_index=SolanaEventIndex(store),
            from_chain='btc',
            to_chain='tao',
            window_start=100,
            window_end=1100,
            rewardable_hotkeys={'hk_squat'},
            min_swap_rao=100_000_000,
            max_swap_rao=500_000_000,
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
            ('hk_a', 'btc', 'tao', 1e10, 0),
        )
        conn.commit()

        # Round N — permissive bounds, full window credited.
        permissive = replay_crown_time_window(
            store=store,
            event_index=SolanaEventIndex(store),
            from_chain='btc',
            to_chain='tao',
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
            to_chain='tao',
            window_start=1100,
            window_end=2100,
            rewardable_hotkeys={'hk_a'},
            min_swap_rao=100_000_000,
            max_swap_rao=500_000_000,
        )
        assert strict == {}

        # Re-run round N — same inputs, same result. Confirms the strict round
        # did not mutate state in a way that retroactively wipes earlier credit.
        permissive_replay = replay_crown_time_window(
            store=store,
            event_index=SolanaEventIndex(store),
            from_chain='btc',
            to_chain='tao',
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
            event_index=SolanaEventIndex(store),
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
        make_watcher(store, active={'hk_a', 'hk_b'})
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
            event_index=SolanaEventIndex(store),
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
        make_watcher(store, active={'hk_a', 'hk_b'})
        conn = store.require_connection()
        for hk in ('hk_a', 'hk_b'):
            conn.execute(
                'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
                (hk, 'tao', 'btc', 0.00020, 0),
            )
        conn.commit()

        crown = replay_crown_time_window(
            store=store,
            event_index=SolanaEventIndex(store),
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
        make_watcher(store, active={'hk_a'})
        conn = store.require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_a', 'tao', 'btc', 0.00020, 5_000),
        )
        conn.commit()

        crown = replay_crown_time_window(
            store=store,
            event_index=SolanaEventIndex(store),
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
            event_index=SolanaEventIndex(store),
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
            event_index=SolanaEventIndex(store),
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
            event_index=SolanaEventIndex(store),
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


class TestSnapshotCurrentCrownHolders:
    """The per-forward-step live-crown snapshot must stay consistent with the
    scoring ledger: it reconstructs the same 5-tuple window state and applies
    the same boundary-squat gate."""

    def _seed_rate(self, store: ValidatorStateStore, hotkey: str, rate: float) -> None:
        conn = store.require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            (hotkey, 'btc', 'tao', rate, 0),
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

        rows = snapshot_current_crown_holders(v)

        holders = [row[2] for row in rows[('btc', 'tao')]]
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
        self._seed_rate(v.state_store, 'hk_squat', 50000.0)  # best rate, can't fund
        self._seed_rate(v.state_store, 'hk_funded', 326.0)  # runner-up, can fund

        rows = snapshot_current_crown_holders(v)

        holders = [row[2] for row in rows[('btc', 'tao')]]
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
                (hk, 'btc', 'tao', rate, 0),
            )
        conn.commit()

        snapshot_holders = [row[2] for row in snapshot_current_crown_holders(v)[('btc', 'tao')]]
        ledger = replay_crown_time_window(
            store=v.state_store,
            event_index=v.event_index,
            from_chain='btc',
            to_chain='tao',
            window_start=100,
            window_end=1100,
            rewardable_hotkeys={'hk_squat', 'hk_funded'},
            min_swap_rao=100_000_000,
            max_swap_rao=500_000_000,
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

        rewards, uids = calculate_miner_rewards(v)

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
        for direction in (('tao', 'btc'), ('btc', 'tao')):
            conn.execute(
                'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
                ('hk_a', direction[0], direction[1], 0.00020, 0),
            )
        conn.commit()

        rewards, _ = calculate_miner_rewards(v)

        np.testing.assert_allclose(rewards[0], POOL_TAO_BTC + POOL_BTC_TAO, atol=1e-6)
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
            ('hk_a', 'tao', 'btc', 0.00020, 0),
        )
        conn.commit()

        rewards, _ = calculate_miner_rewards(v)

        assert rewards[0] == 0.0
        np.testing.assert_allclose(rewards[RECYCLE_UID], 1.0, atol=1e-6)
        v.state_store.close()

    def test_eligible_high_fail_miner_excluded(self, tmp_path: Path):
        """Plenty of successes but failures over the cap → ineligible → 0. A
        rolling strike count, mirrored from the on-chain MinerState counters."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(
            tmp_path, hotkeys=hotkeys, miner_counters={'hk_a': (50, MAX_FAILED_SWAPS + 1)}
        )
        conn = v.state_store.require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_a', 'tao', 'btc', 0.00020, 0),
        )
        conn.commit()

        rewards, _ = calculate_miner_rewards(v)

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
            ('hk_a', 'tao', 'btc', 0.00020, 0),
        )
        conn.commit()

        rewards, _ = calculate_miner_rewards(v)

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
        # Wipe the bootstrap active seed from the index series — hk_a has never
        # been activated on-chain. Mirrors a miner that registered but never
        # called set_active(true).
        conn = v.state_store.require_connection()
        conn.execute("DELETE FROM active_events WHERE hotkey = 'hk_a'")
        conn.commit()
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
            event_index=SolanaEventIndex(store),
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
            event_index=SolanaEventIndex(store),
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
            event_index=SolanaEventIndex(store),
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
            event_index=SolanaEventIndex(store),
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
            event_index=SolanaEventIndex(store),
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
            event_index=SolanaEventIndex(store),
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
            event_index=SolanaEventIndex(store),
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
            event_index=SolanaEventIndex(store),
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
        # hk_a holds 100% of tao→btc crown, full capacity, eligible, no volume penalty.
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
        # Pool conservation: hk_a got POOL_TAO_BTC*0.25; the rest of both buckets
        # and the unallocated pool all recycle, so recycle = 1 - that share.
        recycle_uid = RECYCLE_UID if RECYCLE_UID < len(rewards) else 0
        np.testing.assert_allclose(rewards[recycle_uid], 1.0 - POOL_TAO_BTC * 0.25, atol=1e-6)
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
        """A *known* zero collateral event with max_swap set → factor 0 → no
        reward, full recycle. Seeded as an explicit present-0 event (not an
        absent series, which now fails open — see
        test_unknown_collateral_fails_open)."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys, max_swap_amount=500_000_000)
        seed_collateral(v.event_watcher, 'hk_a', 0, block=0)  # present, known zero
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
        # Both split crown 50/50. A's capacity = 1.0, B's = 0.2.
        # A earns pool * 0.5 * 1.0; B earns pool * 0.5 * 0.2.
        np.testing.assert_allclose(rewards[0], POOL_TAO_BTC * 0.5 * 1.0, atol=1e-6)
        np.testing.assert_allclose(rewards[1], POOL_TAO_BTC * 0.5 * 0.2, atol=1e-6)
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

    def test_unknown_collateral_fails_open(self, tmp_path: Path):
        """A miner with NO collateral event in the watcher's series (unknown,
        not zero) must fail OPEN: capacity 1.0 and can_fund passes, so it earns
        the full pool. The contract auto-deactivates anyone below
        min_collateral, so an active miner always holds enough — treating a
        missing baseline as zero would silently drop honest miners from crown
        (the collateral-baseline bug). Absent != zero."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys, max_swap_amount=500_000_000)  # no collaterals dict → absent
        self.seed_tao_btc_crown(v, 'hk_a')
        rewards, _ = calculate_miner_rewards(v)
        np.testing.assert_allclose(rewards[0], POOL_TAO_BTC, atol=1e-6)
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
        for from_c, to_c in (('tao', 'btc'), ('btc', 'tao')):
            conn.execute(
                'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
                ('hk_a', from_c, to_c, 0.00020 if from_c == 'tao' else 200.0, 0),
            )
        conn.commit()
        calculate_miner_rewards(v)
        assert v.contract_client.get_miner_collateral.call_count == 0
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
        swap_id: int = 1,  # retained for call-site compatibility; ignored (cumulative on-chain ledger)
        resolved_block: int = 9_900,  # ignored — MinerDirectionStats has no per-swap block window
        completed: bool = True,
        from_chain: str = 'tao',
        to_chain: str = 'btc',
        to_amount: int | None = None,
    ) -> None:
        """Seed realized per-direction volume on the on-chain ``MinerDirectionStats``
        ledger (B3.5), replacing the ``swap_outcomes`` table. ``tao_amount`` is the
        from-leg amount the ``volume_factor`` compares. A non-completed swap never
        accrues on-chain, so it's a no-op — timed-out swaps contribute no volume."""
        if not completed:
            return
        v.solana_client.add_direction_stats(
            miner_hotkey,
            from_amount=tao_amount,
            to_amount=tao_amount if to_amount is None else to_amount,
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
        """A timed-out swap contributes no volume — only completed swaps do."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys)
        self.seed_tao_btc_crown(v, 'hk_a')
        self.insert_volume(
            v,
            'hk_a',
            tao_amount=1_000_000_000,
            swap_id=1,
            completed=False,
        )
        # A timed-out swap never accrues on-chain, so MinerDirectionStats has no row.
        assert build_direction_volumes(v.solana_client, v.metagraph) == {}
        rewards, _ = calculate_miner_rewards(v)
        # Eligible solo crown holder, zero counted volume → idle-network
        # short-circuit → factor 1.0 → full tao→btc pool.
        np.testing.assert_allclose(rewards[0], POOL_TAO_BTC, atol=1e-6)
        v.state_store.close()

    def test_volume_split_per_direction(self, tmp_path: Path):
        """Per-direction volume isolates each market. A miner with volume in both
        directions is keyed by direction in ``build_direction_volumes``, so each
        direction's ``from_amount`` is read independently off MinerDirectionStats."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys)
        self.seed_tao_btc_crown(v, 'hk_a')
        self.insert_volume(v, 'hk_a', tao_amount=300_000_000, swap_id=1, from_chain='tao', to_chain='btc')
        self.insert_volume(v, 'hk_a', tao_amount=200_000_000, swap_id=2, from_chain='btc', to_chain='tao')
        vols = build_direction_volumes(v.solana_client, v.metagraph)
        assert vols['hk_a'][('tao', 'btc')].from_amount == 300_000_000
        assert vols['hk_a'][('btc', 'tao')].from_amount == 200_000_000
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

    def test_zero_amount_stats_row_tolerated(self, tmp_path: Path):
        """A MinerDirectionStats row with zero realized volume is tolerated — it
        carries the direction but contributes no volume (vol_share stays 0)."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys)
        v.solana_client.add_direction_stats('hk_a', from_amount=0, to_amount=0, from_chain='tao', to_chain='btc')
        vols = build_direction_volumes(v.solana_client, v.metagraph)
        assert vols['hk_a'][('tao', 'btc')].from_amount == 0
        assert vols['hk_a'][('tao', 'btc')].vwap == 0.0  # zero from-leg → guarded
        v.state_store.close()


class TestRewardShapeWeights:
    """Reward = eligible × [w_a·crown + w_b·quality_volume] (B3.5). w_b is pinned
    to 0.0, so the distributed weights match the B3.3 crown-only reward; a
    positive w_b shifts weight toward realized volume (Phase-C sanity only)."""

    def _solo_crown_with_volume(self, tmp_path: Path) -> SimpleNamespace:
        # hk_a holds 100% tao→btc crown and serves 100% of the volume.
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys)
        conn = v.state_store.require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_a', 'tao', 'btc', 0.00020, 0),
        )
        conn.commit()
        v.solana_client.add_direction_stats(
            'hk_a', from_amount=500_000_000, to_amount=100_000, from_chain='tao', to_chain='btc'
        )
        return v

    def test_wb_zero_reproduces_b33_crown_only(self, tmp_path: Path):
        """Default weights (w_a=1, w_b=0): solo crown holder with full vol_share
        earns the whole pool — the volume_factor short-circuits to 1.0 and the
        quality_volume component is zeroed out."""
        v = self._solo_crown_with_volume(tmp_path)
        rewards, _ = calculate_miner_rewards(v)
        np.testing.assert_allclose(rewards[0], POOL_TAO_BTC, atol=1e-6)
        v.state_store.close()

    def test_wb_positive_shifts_weight_toward_volume(self, tmp_path: Path, monkeypatch):
        """A positive w_b adds the realized-volume component on top of the crown
        component, so a volume-serving crown holder earns strictly more than it
        does under the w_b=0 baseline."""
        v_base = self._solo_crown_with_volume(tmp_path / 'base')
        baseline, _ = calculate_miner_rewards(v_base)
        v_base.state_store.close()

        monkeypatch.setattr(scoring_mod, 'REWARD_WEIGHT_QUALITY_VOLUME', 0.5)
        v_vol = self._solo_crown_with_volume(tmp_path / 'vol')
        weighted, _ = calculate_miner_rewards(v_vol)
        v_vol.state_store.close()

        # Crown share 1.0, vol_share 1.0 → qv component = pool. Adds 0.5·pool.
        assert weighted[0] > baseline[0]
        np.testing.assert_allclose(weighted[0], baseline[0] + 0.5 * POOL_TAO_BTC, atol=1e-6)


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
        v.solana_client.add_direction_stats(
            'hk_b', from_amount=500_000_000, to_amount=500_000_000, from_chain='tao', to_chain='btc'
        )
        rewards, _ = calculate_miner_rewards(v)
        # A: pool × crown 1.0 × eligible 1 × capacity 0.5 × volume_factor 0.5
        np.testing.assert_allclose(rewards[0], POOL_TAO_BTC * 1.0 * 1.0 * 0.5 * 0.5, atol=1e-6)
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
        v.solana_client.add_direction_stats(
            'hk_b', from_amount=400_000_000, to_amount=400_000_000, from_chain='btc', to_chain='tao'
        )
        rewards, _ = calculate_miner_rewards(v)
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
            (hotkey, 'btc', 'tao', rate, 0),
        )
        conn.commit()

    def test_one_short_of_floor_earns_nothing(self, tmp_path: Path):
        """One success below MIN_SUCCESSFUL_SWAPS (2) → ineligible → 0."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys, miner_counters={'hk_a': (MIN_SUCCESSFUL_SWAPS - 1, 0)})
        self.seed_btc_tao_crown(v, 'hk_a')
        rewards, _ = calculate_miner_rewards(v)
        assert rewards[0] == 0.0
        v.state_store.close()

    def test_at_floor_earns_full_crown_share(self, tmp_path: Path):
        """Exactly MIN_SUCCESSFUL_SWAPS successes → eligible → full crown share
        (the whole btc→tao pool, no ramp scaling)."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys, miner_counters={'hk_a': (MIN_SUCCESSFUL_SWAPS, 0)})
        self.seed_btc_tao_crown(v, 'hk_a')
        rewards, _ = calculate_miner_rewards(v)
        np.testing.assert_allclose(rewards[0], POOL_BTC_TAO, atol=1e-6)
        v.state_store.close()

    def test_at_failure_cap_still_eligible(self, tmp_path: Path):
        """Failures exactly at MAX_FAILED_SWAPS (2), with enough successes →
        still eligible → full crown share."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys, miner_counters={'hk_a': (8, MAX_FAILED_SWAPS)})
        self.seed_btc_tao_crown(v, 'hk_a')
        rewards, _ = calculate_miner_rewards(v)
        np.testing.assert_allclose(rewards[0], POOL_BTC_TAO, atol=1e-6)
        v.state_store.close()

    def test_one_past_failure_cap_zero_reward(self, tmp_path: Path):
        """One failure past MAX_FAILED_SWAPS → ineligible → 0, regardless of a
        high success count."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys, miner_counters={'hk_a': (50, MAX_FAILED_SWAPS + 1)})
        self.seed_btc_tao_crown(v, 'hk_a')
        rewards, _ = calculate_miner_rewards(v)
        np.testing.assert_allclose(rewards[0], 0.0, atol=1e-6)
        v.state_store.close()

    def test_ineligible_share_recycles(self, tmp_path: Path):
        """An ineligible holder's crown share recycles to the owner UID, not to
        other miners — pool conservation holds."""
        hotkeys = pad_hotkeys_to_cover_recycle(['hk_a'])
        v = make_validator(tmp_path, hotkeys, miner_counters={'hk_a': (1, 0)})
        self.seed_btc_tao_crown(v, 'hk_a')
        rewards, _ = calculate_miner_rewards(v)
        recycle_uid = RECYCLE_UID if RECYCLE_UID < len(rewards) else 0
        # hk_a gated to 0; both pools recycle in full.
        np.testing.assert_allclose(rewards[recycle_uid], 1.0, atol=1e-6)
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


class TestHistoricalCollateralReplay:
    """Capacity weighting is now derived from a per-block collateral series
    replayed alongside active/busy/rate, not a contract read at scoring time.
    Closes #409 — a miner who tops up collateral after the window cannot
    retroactively boost the capacity multiplier on crown they've already
    earned."""

    def seed_tao_btc_crown(self, v: SimpleNamespace, hotkey: str, rate: float = 0.00020) -> None:
        conn = v.state_store.require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            (hotkey, 'tao', 'btc', rate, 0),
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
            collaterals={'hk_a': 100_000_000},  # held throughout the window
        )
        self.seed_tao_btc_crown(v, 'hk_a')
        # Top-up fires *after* window_end (= 10_000). Window is (9_700, 10_000].
        v.event_watcher.apply_event(
            10_500,
            'CollateralPosted',
            {'miner': 'hk_a', 'amount': 400_000_000, 'total': 500_000_000},
        )
        rewards, _ = calculate_miner_rewards(v)
        # capacity_factor = 100M / 500M = 0.2; pool 0.5 → reward 0.1.
        np.testing.assert_allclose(rewards[0], POOL_TAO_BTC * 0.2, atol=1e-6)
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
            collaterals={'hk_a': 125_000_000},  # window-start anchor
        )
        self.seed_tao_btc_crown(v, 'hk_a')
        # SCORING_WINDOW_BLOCKS = 300 → window is (9_700, 10_000]. Midpoint
        # 9_850 splits credit 150/150 between low and full capacity.
        v.event_watcher.apply_event(
            9_850,
            'CollateralPosted',
            {'miner': 'hk_a', 'amount': 375_000_000, 'total': 500_000_000},
        )
        rewards, _ = calculate_miner_rewards(v)
        # First 150 blocks at cap 0.25, next 150 at cap 1.0 → mean cap 0.625.
        np.testing.assert_allclose(rewards[0], POOL_TAO_BTC * 0.625, atol=1e-6)
        v.state_store.close()

    def test_get_miner_collaterals_at_returns_latest(self, tmp_path: Path):
        """Unit test the event watcher's per-block collateral reconstruction:
        the latest event at or before the queried block wins."""
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active=set())
        watcher.apply_event(100, 'CollateralPosted', {'miner': 'hk_a', 'amount': 50, 'total': 100_000_000})
        watcher.apply_event(500, 'CollateralPosted', {'miner': 'hk_a', 'amount': 50, 'total': 250_000_000})
        watcher.apply_event(800, 'CollateralWithdrawn', {'miner': 'hk_a', 'amount': 50, 'remaining': 50_000_000})
        assert watcher.get_miner_collaterals_at(50) == {}
        assert watcher.get_miner_collaterals_at(100) == {'hk_a': 100_000_000}
        assert watcher.get_miner_collaterals_at(499) == {'hk_a': 100_000_000}
        assert watcher.get_miner_collaterals_at(500) == {'hk_a': 250_000_000}
        assert watcher.get_miner_collaterals_at(799) == {'hk_a': 250_000_000}
        assert watcher.get_miner_collaterals_at(800) == {'hk_a': 50_000_000}
        store.close()

    def test_swap_completed_deducts_fee_from_collateral_series(self, tmp_path: Path):
        """``apply_collateral_penalty`` silently deducts the fee inside
        ``confirm_swap`` — the contract emits no CollateralWithdrawn for it,
        so the watcher mirrors the deduction from ``SwapCompleted.fee_amount``
        to keep the replayed series in step with on-chain collateral."""
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active={'hk_a'})
        watcher.apply_event(100, 'CollateralPosted', {'miner': 'hk_a', 'amount': 0, 'total': 500_000_000})
        watcher.apply_event(200, 'SwapInitiated', {'swap_id': 1, 'miner': 'hk_a'})
        watcher.apply_event(
            300,
            'SwapCompleted',
            {'swap_id': 1, 'miner': 'hk_a', 'tao_amount': 0, 'fee_amount': 50_000_000},
        )
        assert watcher.get_miner_collaterals_at(300) == {'hk_a': 450_000_000}
        store.close()

    def test_swap_timed_out_deducts_slash_from_collateral_series(self, tmp_path: Path):
        """Same mirror for slashes — ``SwapTimedOut.slash_amount`` reduces the
        replayed series so a post-slash crown interval gets the lower
        capacity, not the pre-slash value."""
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active={'hk_a'})
        watcher.apply_event(100, 'CollateralPosted', {'miner': 'hk_a', 'amount': 0, 'total': 500_000_000})
        watcher.apply_event(200, 'SwapInitiated', {'swap_id': 1, 'miner': 'hk_a'})
        watcher.apply_event(
            300,
            'SwapTimedOut',
            {'swap_id': 1, 'miner': 'hk_a', 'slash_amount': 200_000_000},
        )
        assert watcher.get_miner_collaterals_at(300) == {'hk_a': 300_000_000}
        store.close()

    def test_prune_keeps_latest_collateral_event_per_hotkey(self, tmp_path: Path):
        """Mirrors the active-prune anchor rule: collateral events older than
        the cutoff drop, but the most recent per-hotkey row is kept so
        post-prune reconstruction at any block ≥ cutoff still returns the
        correct value."""
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active=set())
        watcher.apply_event(100, 'CollateralPosted', {'miner': 'hk_a', 'amount': 0, 'total': 100_000_000})
        watcher.apply_event(200, 'CollateralPosted', {'miner': 'hk_a', 'amount': 0, 'total': 200_000_000})
        watcher.apply_event(5_000, 'CollateralPosted', {'miner': 'hk_a', 'amount': 0, 'total': 500_000_000})
        watcher.apply_event(50, 'CollateralPosted', {'miner': 'hk_b', 'amount': 0, 'total': 300_000_000})
        # current_block=10_000, SCORING_WINDOW_BLOCKS=300 → cutoff=9_700.
        watcher.prune_old_events(10_000)
        blocks_a = [ev.block for ev in watcher.collateral_events_by_hotkey['hk_a']]
        assert blocks_a == [5_000]
        blocks_b = [ev.block for ev in watcher.collateral_events_by_hotkey['hk_b']]
        assert blocks_b == [50]
        assert watcher.get_miner_collaterals_at(20_000) == {'hk_a': 500_000_000, 'hk_b': 300_000_000}
        store.close()

    def test_collateral_events_persist_across_hydrate(self, tmp_path: Path):
        """Warm-restart hydration: collateral events written to state.db
        round-trip through hydrate_from_db so the in-memory series and the
        ``by_hotkey`` index match what was on disk."""
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active={'hk_a'})
        watcher.apply_event(100, 'CollateralPosted', {'miner': 'hk_a', 'amount': 0, 'total': 100_000_000})
        watcher.apply_event(500, 'CollateralPosted', {'miner': 'hk_a', 'amount': 0, 'total': 200_000_000})
        # Build a second watcher pointed at the same DB and hydrate.
        store.set_event_cursor(600)
        watcher2 = ContractEventWatcher(
            substrate=MagicMock(),
            contract_address='5contract',
            metadata_path=METADATA_PATH,
            state_store=store,
        )
        watcher2.hydrate_from_db()
        assert [(ev.block, ev.collateral_rao) for ev in watcher2.collateral_events] == [
            (100, 100_000_000),
            (500, 200_000_000),
        ]
        assert watcher2.get_miner_collaterals_at(1_000) == {'hk_a': 200_000_000}
        store.close()

    def test_fee_without_baseline_does_not_fabricate_zero(self, tmp_path: Path):
        """SwapCompleted fee for a miner with NO collateral baseline must NOT
        write ``0 + (-fee)`` clipped to 0 — that pinned the miner at zero and
        dropped them from crown via the capacity / can_fund gate. With no
        baseline the delta is skipped and the miner stays *unknown* (absent),
        which the scoring gate fails open on."""
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active={'hk_a'})
        # No CollateralPosted first → unknown baseline.
        watcher.apply_event(200, 'SwapInitiated', {'swap_id': 1, 'miner': 'hk_a'})
        watcher.apply_event(
            300, 'SwapCompleted', {'swap_id': 1, 'miner': 'hk_a', 'tao_amount': 0, 'fee_amount': 50_000_000}
        )
        assert 'hk_a' not in watcher.collateral_events_by_hotkey  # no fabricated 0 row
        assert watcher.get_miner_collaterals_at(300) == {}  # absent == unknown
        assert watcher._latest_collateral('hk_a') is None
        store.close()

    def test_reconcile_collateral_from_contract(self, tmp_path: Path):
        """Reconcile resyncs active miners to on-chain truth: heals a corrupted
        present-0 (hk_a), seeds an unknown (hk_b), skips inactive miners
        (hk_c), and is idempotent when values already match."""
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active={'hk_a', 'hk_b'})  # hk_c not active
        seed_collateral(watcher, 'hk_a', 0, block=100)  # corrupted present-0
        contract = MagicMock()
        vals = {'hk_a': 474_000_000, 'hk_b': 600_000_000, 'hk_c': 999_000_000}
        contract.get_miner_collateral.side_effect = lambda hk: vals.get(hk, 0)
        updated = watcher.reconcile_collateral_from_contract(9_000, ['hk_a', 'hk_b', 'hk_c'], contract)
        assert updated == 2
        snap = watcher.get_miner_collaterals_at(9_000)
        assert snap == {'hk_a': 474_000_000, 'hk_b': 600_000_000}  # hk_c skipped (inactive)
        # Idempotent: values now match → no new events.
        assert watcher.reconcile_collateral_from_contract(9_100, ['hk_a', 'hk_b', 'hk_c'], contract) == 0
        store.close()

    def test_reconcile_skips_on_rpc_failure(self, tmp_path: Path):
        """A contract read failure leaves the prior value untouched — a
        transient RPC blip can't zero a miner's collateral."""
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        watcher = make_watcher(store, active={'hk_a'})
        seed_collateral(watcher, 'hk_a', 123_000_000, block=0)
        contract = MagicMock()
        contract.get_miner_collateral.side_effect = RuntimeError('rpc down')
        assert watcher.reconcile_collateral_from_contract(9_000, ['hk_a'], contract) == 0
        assert watcher.get_miner_collaterals_at(9_000) == {'hk_a': 123_000_000}  # unchanged
        store.close()


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
            {('tao', 'btc'): 279.3},
            eligible=True,
            ever_active={'hk'},
            direction_traces={('tao', 'btc'): self._trace(280.0)},
            collaterals={'hk': 0},
            min_swap_rao=100_000_000,
            max_swap_rao=500_000_000,
        )
        assert reason.startswith('insufficient_collateral'), reason

    def test_competitive_but_unknown_collateral(self):
        from allways.validator.scoring_trace import diagnose_non_earner

        reason = diagnose_non_earner(
            'hk',
            {('tao', 'btc'): 279.3},
            eligible=True,
            ever_active={'hk'},
            direction_traces={('tao', 'btc'): self._trace(280.0)},
            collaterals={},  # absent → unknown
            min_swap_rao=100_000_000,
            max_swap_rao=500_000_000,
        )
        assert reason.startswith('unknown_collateral'), reason

    def test_genuinely_worse_rate_is_direction_aware_outbid(self):
        from allways.validator.scoring_trace import diagnose_non_earner

        # tao→btc lower-wins: own 281 is worse than best 280 → outbid.
        reason = diagnose_non_earner(
            'hk',
            {('tao', 'btc'): 281.0},
            eligible=True,
            ever_active={'hk'},
            direction_traces={('tao', 'btc'): self._trace(280.0)},
            collaterals={'hk': 500_000_000},
            min_swap_rao=100_000_000,
            max_swap_rao=500_000_000,
        )
        assert reason.startswith('outbid'), reason

    def test_competitive_and_funded_is_unfilled_not_outbid(self):
        from allways.validator.scoring_trace import diagnose_non_earner

        reason = diagnose_non_earner(
            'hk',
            {('tao', 'btc'): 279.3},
            eligible=True,
            ever_active={'hk'},
            direction_traces={('tao', 'btc'): self._trace(280.0)},
            collaterals={'hk': 500_000_000},
            min_swap_rao=100_000_000,
            max_swap_rao=500_000_000,
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
    DIRECTIONS = [('btc', 'tao'), ('tao', 'btc')]
    RATES = [0.00015, 1.0, 345.0, 50_000_000.0, 1e10, 0.0, -1.0, float('inf')]

    def _reference(self, from_chain, to_chain, min_rao, max_rao, collaterals):
        def exec_ref(rate):
            return is_executable_rate(rate, from_chain, to_chain, min_rao, max_rao)

        def fund_ref(hotkey, rate):
            if hotkey not in collaterals:
                return True
            min_leg = min_executable_tao_leg(rate, from_chain, to_chain, min_rao, max_rao)
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
        _, can_fund = make_crown_predicates('btc', 'tao', 100_000_000, 500_000_000, {})
        assert can_fund('hk_unknown', 345.0) is True

    def test_drops_holder_whose_collateral_cannot_fund_min_leg(self):
        # 1-rao collateral can't cover any real in-band leg → boundary-squat drop;
        # a richly-funded miner at the same rate passes.
        collaterals = {'hk_poor': 1, 'hk_rich': 10_000_000_000}
        _, can_fund = make_crown_predicates('btc', 'tao', 100_000_000, 500_000_000, collaterals)
        rate = 345.0
        min_leg = min_executable_tao_leg(rate, 'btc', 'tao', 100_000_000, 500_000_000)
        assert min_leg > 0  # rate is executable, so the gate is live
        assert can_fund('hk_poor', rate) is False
        assert can_fund('hk_rich', rate) is True
