"""The scoring math: the pure helpers (pools, crown holders, capacity, eligibility, diagnosis), and the crown
replay, a full round and fill qualification over the ledger."""

from functools import partial
from types import SimpleNamespace

import pytest

from allways.classes import ActivityTransition
from allways.constants import (
    ALPHA_FAMILY,
    ALPHA_PRICE_TTL_SECS,
    DIRECTION_POOLS,
    FAMILY_PAIR_COUNTS,
    MAX_FAILED_SWAPS,
    MINER_POOL_SHARE,
    POOL_VOLUME_ALPHA,
    QUALITY_VOLUME_BETA,
    RATE_PRECISION,
    RECYCLE_UID,
    required_collateral,
    scoring_family,
)
from allways.eligibility import direction_eligible, is_eligible, purse_active
from allways.solana import pdas
from allways.utils.rate import is_executable_rate, min_executable_hub_leg
from allways.validator.scoring.fee_pool import FeePoolShare
from allways.validator.scoring.ledger import ACTIVE, ACTIVITY, RATE, Ledger
from allways.validator.scoring.rewards import (
    AlphaPrices,
    capacity_factor,
    compute_direction_pools,
    crown_can_fund,
    crown_depth_shares,
    crown_holders_at_instant,
    crown_now,
    fill_held_crown,
    make_crown_predicates,
    replay_crown_time_window,
    score_round,
)
from allways.validator.store.db import Store
from allways.validator.world.snapshot import World


def _miner_state(successful: int, failed: int, settling_until: int = 0) -> SimpleNamespace:
    return SimpleNamespace(successful_swaps=successful, failed_swaps=failed, settling_until=settling_until)


class TestIsEligibleHelper:
    """Binary gate: failures <= MAX_FAILED_SWAPS (lifetime, on-chain) AND a fill in the
    trailing activity window (``recent_fills``). No success minimum. ``recent_fills=None``
    = ledger younger than the window → strikes only."""

    def test_strikes_only_when_ledger_is_young(self):
        assert is_eligible(_miner_state(0, 0)) is True
        assert is_eligible(_miner_state(0, MAX_FAILED_SWAPS)) is True  # boundary
        assert is_eligible(_miner_state(50, MAX_FAILED_SWAPS + 1)) is False

    def test_activity_window_gates_once_ledger_is_old(self):
        assert is_eligible(_miner_state(0, 0), hotkey='hk_a', recent_fills={'sol': {'hk_a'}}) is True
        assert is_eligible(_miner_state(50, 0), hotkey='hk_a', recent_fills={}) is False
        assert is_eligible(_miner_state(50, 0), hotkey='hk_a', recent_fills={'sol': {'hk_b'}}) is False

    def test_strikes_override_activity(self):
        assert (
            is_eligible(_miner_state(50, MAX_FAILED_SWAPS + 1), hotkey='hk_a', recent_fills={'sol': {'hk_a'}}) is False
        )

    def test_global_reading_is_any_purse_lane_reading_is_own_purse(self):
        """A tao-only fill keeps the miner globally eligible but only the tao lanes live."""
        fills = {'tao': {'hk_a'}}
        ms = _miner_state(0, 0)
        assert is_eligible(ms, hotkey='hk_a', recent_fills=fills) is True
        assert purse_active('hk_a', 'tao', fills) is True
        assert purse_active('hk_a', 'sol', fills) is False
        assert direction_eligible(ms, 'sol', 'tao', 0, backing='tao', hotkey='hk_a', recent_fills=fills) is True
        assert direction_eligible(ms, 'sol', 'tao', 0, backing='sol', hotkey='hk_a', recent_fills=fills) is False
        assert direction_eligible(ms, 'btc', 'sol', 0, backing='sol', hotkey='hk_a', recent_fills=fills) is False
        # Pair-level reading: live while ANY of the pair's hubs is active.
        assert direction_eligible(ms, 'sol', 'tao', 0, hotkey='hk_a', recent_fills=fills) is True
        assert direction_eligible(ms, 'btc', 'sol', 0, hotkey='hk_a', recent_fills=fills) is False
        # Young ledger: everything reads active.
        assert purse_active('hk_a', 'sol', None) is True


class TestComputeDirectionPools:
    """Every pair belongs to one family (sol, tao, or alpha for any pair with an alpha leg);
    each family holds an equal share of the pool and an idle family's share recycles. Inside a
    family a live pair's share is (1−α)/live_pairs + α·family_volume_share, split evenly between
    its two legs, then across each leg's backing lanes (F4). Volume is the hub leg for sol/tao and
    the fill's TAO collateral for alpha. Dead pairs are present at 0.0; no volume anywhere falls
    back to DIRECTION_POOLS. Keys are lanes: (from, to, backing)."""

    FAMILY = MINER_POOL_SHARE / len(FAMILY_PAIR_COUNTS)

    @staticmethod
    def family_total(pools, family):
        return sum(v for (f, t, _b), v in pools.items() if scoring_family(f, t) == family)

    def test_no_volume_falls_back_to_equal_family_shares(self):
        pools = compute_direction_pools({})
        assert pools[('btc', 'sol', 'sol')] == DIRECTION_POOLS[('btc', 'sol')]
        # sol↔tao's two lanes split their direction's pool — budget-neutral, not grown.
        assert pools[('sol', 'tao', 'sol')] == DIRECTION_POOLS[('sol', 'tao')] / 2
        assert pools[('sol', 'tao', 'tao')] == pools[('sol', 'tao', 'sol')]
        for family in FAMILY_PAIR_COUNTS:
            assert self.family_total(pools, family) == pytest.approx(self.FAMILY)

    def test_single_live_pair_takes_its_whole_family_share(self):
        pools = compute_direction_pools({('btc', 'sol'): {'hk_a': (5, 100)}})
        assert pools[('btc', 'sol', 'sol')] == pytest.approx(self.FAMILY / 2)
        assert pools[('sol', 'btc', 'sol')] == pools[('btc', 'sol', 'sol')]  # quiet leg rides its pair
        dead = {lane: v for lane, v in pools.items() if lane[:2] not in (('btc', 'sol'), ('sol', 'btc'))}
        assert dead and all(v == 0.0 for v in dead.values())
        assert set(pools) == set(compute_direction_pools({}))  # dead lanes are listed, not dropped
        assert sum(pools.values()) == pytest.approx(self.FAMILY)  # the idle families' shares recycle

    def test_floor_is_split_among_live_pairs_only(self):
        # BTC pair: 300 SOL (to_amount is the SOL leg of btc→sol); TAO pair: 100 SOL (from_amount of
        # sol→tao). Spoke-side legs are huge to prove they never enter the weighting.
        pools = compute_direction_pools(
            {
                ('btc', 'sol'): {'hk_a': (10**15, 200), 'hk_b': (10**15, 100)},
                ('sol', 'tao'): {'hk_c': (100, 10**15)},
            }
        )
        floor_pair = (1 - POOL_VOLUME_ALPHA) / 2
        assert pools[('btc', 'sol', 'sol')] == pytest.approx(self.FAMILY * (floor_pair + POOL_VOLUME_ALPHA * 0.75) / 2)
        # Volume tilts the whole sol↔tao PAIR; each direction's pool then halves across its two lanes.
        tao_sol_direction = self.FAMILY * (floor_pair + POOL_VOLUME_ALPHA * 0.25) / 2
        assert pools[('tao', 'sol', 'sol')] == pytest.approx(tao_sol_direction / 2)
        assert pools[('tao', 'sol', 'tao')] == pytest.approx(tao_sol_direction / 2)
        assert pools[('sol', 'eth', 'sol')] == 0.0  # registered, quiet → dead
        assert sum(pools.values()) == pytest.approx(self.FAMILY)

    def test_live_families_share_equally_whatever_their_volume(self):
        pools = compute_direction_pools(
            {
                ('btc', 'sol'): {'hk_a': (1, 10**12)},  # lamports, sol family
                ('tao', 'eth'): {'hk_b': (5, 10**9)},  # rao, tao family
                ('sol', 'sn7'): {'hk_c': (1, 1, 10)},  # TAO collateral, alpha family
            }
        )
        for family in FAMILY_PAIR_COUNTS:
            assert self.family_total(pools, family) == pytest.approx(self.FAMILY)
        assert pools[('tao', 'eth', 'tao')] == pytest.approx(self.FAMILY / 2)
        assert pools[('tao', 'btc', 'tao')] == 0.0
        assert sum(pools.values()) == pytest.approx(MINER_POOL_SHARE)

    def test_alpha_pairs_cannot_move_the_sol_or_tao_pools(self):
        core = {('btc', 'sol'): {'hk_a': (1, 500)}, ('tao', 'eth'): {'hk_b': (5, 900)}}
        alpha = {(f'sn{n}', 'avax'): {'hk_c': (10**12, 1, 10**12)} for n in range(1, 129)}
        before = compute_direction_pools(core)
        after = compute_direction_pools({**core, **alpha})
        for lane, pool in before.items():
            if scoring_family(*lane[:2]) != ALPHA_FAMILY:
                assert after[lane] == pool

    def test_alpha_volume_is_the_tao_collateral(self):
        # sn7↔avax cleared 3x the TAO value of sn64↔avax, however many alpha units each moved.
        pools = compute_direction_pools(
            {('sn7', 'avax'): {'hk_a': (1, 10**15, 300)}, ('sn64', 'avax'): {'hk_b': (10**15, 1, 100)}}
        )
        sn7 = pools[('sn7', 'avax', 'tao')] + pools[('avax', 'sn7', 'tao')]
        assert sn7 == pytest.approx(self.FAMILY * ((1 - POOL_VOLUME_ALPHA) / 2 + POOL_VOLUME_ALPHA * 0.75))

    def test_lane_split_is_budget_neutral(self):
        # However volume lands on sol↔tao's two lanes, a live family still pays exactly its share.
        volume_cases = [
            {('sol', 'tao'): {'hk': (10**12, 1)}},
            {('btc', 'sol'): {'hk': (1, 10**12)}, ('sol', 'tao'): {'hk': (5 * 10**11, 1)}},
            {('tao', 'sol'): {'hk': (1, 10**10)}},
        ]
        for volumes in volume_cases:
            assert sum(compute_direction_pools(volumes).values()) == pytest.approx(self.FAMILY), volumes

    def test_leg_direction_within_pair_is_irrelevant(self):
        # 500 SOL cleared btc→sol vs 500 SOL cleared sol→btc: same pair volume,
        # identical pools — one leg can't be tilted independently of its twin.
        forward = compute_direction_pools({('btc', 'sol'): {'hk': (1, 500)}})
        reverse = compute_direction_pools({('sol', 'btc'): {'hk': (500, 1)}})
        assert forward == reverse


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


class TestCrownRateBand:
    """rate_band semantics: qualified quotes within the band of the best
    qualified rate hold the crown together, anchor first."""

    def test_near_rates_join_band_higher_wins(self):
        # b is 0.45% off the leader (inside a 0.5% band); c at 1.5% stays out.
        rates = {'a': 0.00020, 'b': 0.0001991, 'c': 0.000197}
        holders = crown_holders_at_instant(rates, {'a', 'b', 'c'}, rate_band=0.005)
        assert holders == ['a', 'b']

    def test_near_rates_join_band_lower_wins(self):
        # Lower is better: b asks 0.4% more (inside), c 1.2% more (outside).
        rates = {'a': 250.0, 'b': 251.0, 'c': 253.0}
        holders = crown_holders_at_instant(rates, {'a', 'b', 'c'}, lower_rate_wins=True, rate_band=0.005)
        assert holders == ['a', 'b']

    def test_band_anchors_on_best_qualified_rate(self):
        # A busy leader can't drag the band out of everyone else's reach: the
        # anchor is the best *qualified* rate, and the band opens around it.
        rates = {'busy': 0.00030, 'a': 0.00020, 'b': 0.0001995}
        holders = crown_holders_at_instant(rates, {'busy', 'a', 'b'}, rewardable_by_state={'a', 'b'}, rate_band=0.005)
        assert holders == ['a', 'b']


class TestCrownDepthShares:
    def test_proportional_to_collateral(self):
        shares = crown_depth_shares(['a', 'b'], {'a': 300, 'b': 100}, 0)
        assert shares == {'a': 0.75, 'b': 0.25}

    def test_depth_capped_at_full_capacity(self):
        # 2 SOL vs the 0.55 cap for a 0.5 max_swap: collateral past a
        # full-capacity fill buys no extra share (never pay-to-win).
        shares = crown_depth_shares(['whale', 'full'], {'whale': 2_000_000_000, 'full': 550_000_000}, 500_000_000)
        assert shares == {'whale': 0.5, 'full': 0.5}


class TestCapacityFactorHelper:
    """Direct unit tests for the capacity_factor pure function. Full credit requires
    backing a max_swap fill at the contract's 1.10× gate: denominator = 1.1 × max_swap.
    The ramp below full is linear (ratio ** CAPACITY_CURVE_EXPONENT, k=1), so partial
    depth earns its raw ratio."""

    def test_at_required_collateral_is_full(self):

        assert capacity_factor(550_000_000, 500_000_000) == 1.0

    def test_at_max_swap_is_under_full(self):
        """Collateral == max_swap can't actually accept a max_swap fill (needs 1.1×)."""

        assert capacity_factor(500_000_000, 500_000_000) == 500_000_000 / 550_000_000

    def test_half_required_is_half(self):
        """Linear ramp (k=1): half the required collateral earns half."""

        assert capacity_factor(275_000_000, 500_000_000) == 0.5

    def test_above_required_caps_at_one(self):

        assert capacity_factor(2_000_000_000, 500_000_000) == 1.0

    def test_zero_collateral_is_zero(self):

        assert capacity_factor(0, 500_000_000) == 0.0

    def test_zero_max_swap_is_fail_safe_one(self):
        """Bounds cache cold start / RPC failure returns 0; factor should
        default to 1.0 so the first scoring pass doesn't zero everyone."""

        assert capacity_factor(100_000_000, 0) == 1.0


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
            min_leg = min_executable_hub_leg(rate, from_chain, to_chain, min_rao, max_rao)
            return min_leg == 0 or collaterals.get(hotkey, 0) >= required_collateral(min_leg)

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

    def test_drops_holder_whose_collateral_cannot_fund_min_leg(self):
        # 1-rao collateral can't cover any real in-band leg → boundary-squat drop;
        # a richly-funded miner at the same rate passes.
        collaterals = {'hk_poor': 1, 'hk_rich': 10_000_000_000}
        _, can_fund = make_crown_predicates('sol', 'btc', 100_000_000, 500_000_000, collaterals)
        rate = 345.0
        min_leg = min_executable_hub_leg(rate, 'sol', 'btc', 100_000_000, 500_000_000)
        assert min_leg > 0  # rate is executable, so the gate is live
        assert can_fund('hk_poor', rate) is False
        assert can_fund('hk_rich', rate) is True


class TestCrownCanFund:
    """The crown funding gate mirrors routing's swap_viable / the contract's
    1.10× reserve requirement — a miner below required_collateral(min_leg) is
    unreservable at any size and must earn no crown."""

    BOUNDS = dict(from_chain='sol', to_chain='btc', min_swap_hub=100_000_000, max_swap_hub=500_000_000)
    RATE = 326.0

    def min_leg(self) -> int:
        return min_executable_hub_leg(self.RATE, **self.BOUNDS)

    def test_collateral_at_raw_leg_is_rejected(self):
        # Regression: the pre-1.1× gate passed exactly-min_leg collateral, paying
        # crown to a miner the contract would refuse to reserve at any size.
        leg = self.min_leg()
        assert leg > 0
        assert not crown_can_fund('hk', self.RATE, collaterals={'hk': leg}, **self.BOUNDS)

    def test_collateral_at_required_collateral_passes(self):
        assert crown_can_fund('hk', self.RATE, collaterals={'hk': required_collateral(self.min_leg())}, **self.BOUNDS)

    def test_unknown_collateral_fails_closed(self):
        assert not crown_can_fund('hk', self.RATE, collaterals={}, **self.BOUNDS)


# ── over the ledger ─────────────────────────────────────────────────────────

NOW = 10_000
BEST, NEXT = 0.0001, 0.0002  # btc→sol is quoted in sol per btc: lower wins
LANE = ('btc', 'sol', 'sol')


def _ledger(rates=None, active=('hk_a', 'hk_b')):
    ledger = Ledger(Store(':memory:'))
    for hk in active:
        ledger.put(0, hk, ACTIVE, 1)
    for hk, rate in (rates or {}).items():
        ledger.put(0, hk, RATE, rate, *LANE)
    return ledger


def _replay(ledger, start=100, end=1100, rewardable=('hk_a', 'hk_b'), **kw):
    return replay_crown_time_window(ledger, 'btc', 'sol', start, end, set(rewardable), backing='sol', **kw)


def test_the_best_rate_holds_the_window_until_it_is_beaten():
    ledger = _ledger({'hk_a': NEXT, 'hk_b': BEST})
    assert _replay(ledger) == {'hk_b': 1000.0}
    ledger.put(600, 'hk_a', RATE, BEST / 2, *LANE)
    assert _replay(ledger) == {'hk_b': 500.0, 'hk_a': 500.0}


def test_a_busy_miner_forfeits_the_crown_to_the_next_rate():
    ledger = _ledger({'hk_a': NEXT, 'hk_b': BEST})
    ledger.put(300, 'hk_b', ACTIVITY, int(ActivityTransition.RESERVE_START), hub='sol')
    ledger.put(700, 'hk_b', ACTIVITY, int(ActivityTransition.RESERVE_EXPIRE), hub='sol')
    assert _replay(ledger) == {'hk_b': 600.0, 'hk_a': 400.0}


def test_busy_on_the_other_hub_keeps_the_crown():
    ledger = _ledger({'hk_a': NEXT, 'hk_b': BEST})
    ledger.put(300, 'hk_b', ACTIVITY, int(ActivityTransition.RESERVE_START), hub='tao')
    assert _replay(ledger) == {'hk_b': 1000.0}


def test_a_deactivated_miner_drops_out_and_the_intervals_say_who_held_when():
    ledger = _ledger({'hk_a': NEXT, 'hk_b': BEST})
    ledger.put(400, 'hk_b', ACTIVE, 0)
    intervals = []
    assert _replay(ledger, intervals_out=intervals) == {'hk_b': 300.0, 'hk_a': 700.0}
    assert [(s, e, list(h)) for s, e, h, _ in intervals] == [(100, 400, ['hk_b']), (400, 1100, ['hk_a'])]


HOTKEYS = tuple(['hk_a', 'hk_b'] + [f'filler{i}' for i in range(RECYCLE_UID)])


def _fixed(rate):
    return int(rate * RATE_PRECISION)


def _round_world(failed_b=0):
    def miner(pk, failed):
        return SimpleNamespace(
            miner=pk,
            active=True,
            active_backings=pdas.BACKING_BIT_SOL,
            collateral=0,
            failed_swaps=failed,
            successful_swaps=0,
            settling_until=0,
        )

    def quote(pk, rate):
        return SimpleNamespace(
            miner=pk,
            from_chain='btc',
            to_chain='sol',
            collateral_chain='sol',
            rate=_fixed(rate),
            miner_from_addr='',
            miner_to_addr='',
        )

    config = SimpleNamespace(
        halted=False, min_swap_amount=0, max_swap_amount=0, tao_min_swap_amount=0, tao_max_swap_amount=0
    )
    return World(
        taken_at=NOW,
        slot=1,
        complete=True,
        config=config,
        accounts={
            'MinerState': {'a': miner('pk_a', 0), 'b': miner('pk_b', failed_b)},
            'MinerQuote': {'qa': quote('pk_a', NEXT), 'qb': quote('pk_b', BEST)},
        },
        attribution={'pk_a': 'hk_a', 'pk_b': 'hk_b'},
    )


def _score(world):
    ledger = _ledger({'hk_a': NEXT, 'hk_b': BEST})
    prices = AlphaPrices({}, lambda: None)
    return score_round(world, HOTKEYS, ledger, {}, prices, NOW - 1000, NOW, FeePoolShare(0.0, None, 1.0))


def test_a_round_pays_the_crown_holder_its_lane_pool_and_recycles_the_rest():
    result = _score(_round_world())
    pool = DIRECTION_POOLS[('btc', 'sol')]
    paid = (1 - QUALITY_VOLUME_BETA) * pool  # no qualified volume: the β slice recycles too
    assert result.rewards[1] == pytest.approx(paid)
    assert result.rewards[0] == 0
    assert float(result.rewards.sum()) == pytest.approx(1.0)
    assert [(r.hotkey, r.crown_share) for r in result.rows] == [('hk_b', 1.0)]
    assert result.intervals[LANE] == [(NOW - 1000, NOW, {'hk_b': 1.0}, BEST)]


def test_an_ineligible_miner_is_no_crown_candidate_so_the_next_rate_earns():
    result = _score(_round_world(failed_b=MAX_FAILED_SWAPS + 1))
    assert result.rewards[0] > 0 and result.rewards[1] == 0


def test_the_live_crown_is_who_holds_each_quoted_lane_right_now_and_moves_with_the_ledger():
    ledger = _ledger({'hk_a': NEXT, 'hk_b': BEST})
    prices = AlphaPrices({}, lambda: None)
    assert crown_now(_round_world(), HOTKEYS, ledger, prices, NOW) == {LANE: [(*LANE, 'hk_b', 1.0, BEST, NOW)]}
    ledger.put(NOW + 5, 'hk_b', ACTIVITY, int(ActivityTransition.RESERVE_START), hub='sol')
    assert crown_now(_round_world(), HOTKEYS, ledger, prices, NOW + 10) == {
        LANE: [(*LANE, 'hk_a', 1.0, NEXT, NOW + 10)]
    }
    halted = _round_world()
    halted.config.halted = True
    assert crown_now(halted, HOTKEYS, ledger, prices, NOW + 10) == {LANE: []}


def test_a_fill_qualifies_only_if_its_miner_held_the_crown_when_reserved():
    world = _round_world()
    ledger = _ledger({'hk_a': NEXT, 'hk_b': BEST})
    held = partial(fill_held_crown, world, set(HOTKEYS), ledger, AlphaPrices({}, lambda: None))
    assert held('hk_b', *LANE, 1000, 500)
    assert not held('hk_a', *LANE, 1000, 500)
    assert not held('stranger', *LANE, 1000, 500)


class _Alpha:
    netuid = 7
    chain_def = SimpleNamespace(decimals=9)

    def __init__(self):
        self.reads = 0

    def value_rao(self, amount):
        self.reads += 1
        return amount // 2


def test_alpha_prices_come_from_the_price_table_first_and_are_kept_for_their_ttl():
    alpha = _Alpha()
    table = {'prices': {7: SimpleNamespace(rao=250_000_000)}}
    prices = AlphaPrices({'sn7': alpha}, lambda: table['prices'])
    assert prices.unit_value('sol', 'sn7', 'tao', NOW) == 0.25
    table['prices'] = {}
    assert prices.unit_value('sol', 'sn7', 'tao', NOW + 1) == 0.25  # kept
    assert prices.unit_value('sol', 'sn7', 'tao', NOW + ALPHA_PRICE_TTL_SECS) == 0.5  # read on its own
    assert prices.unit_value('btc', 'sol', 'sol', NOW) == 1.0 and alpha.reads == 1
