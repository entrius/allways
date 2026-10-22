"""Tests for allways.utils.rate — to_amount calculation and fee deduction math — and the
seam's ``rate_quote`` candidates built on top of it."""

import time
from decimal import Decimal
from types import SimpleNamespace

import pytest
from solders.keypair import Keypair as SolKeypair

from allways.chains import get_chain_def
from allways.cli.swap_commands.swap_intake import compute_intake_amounts
from allways.constants import BTC_TO_SAT, RATE_PRECISION, TAO_TO_RAO
from allways.utils.rate import (
    apply_fee_deduction,
    calculate_to_amount,
    directional_rate,
    is_executable_rate,
    normalize_rate,
    quantize_rate_fixed,
)
from allways.validator.serving.requests import RATE_LEVELS_LIMIT, rate_quote
from allways.validator.world.binding import hotkey_ss58
from tests.validator.fakes import Ctx

# Chain decimals
TAO_DEC = 9
BTC_DEC = 8
ETH_DEC = 18
SOL_DEC = 9
USDC_DEC = 6


class TestCalculateToAmount:
    """Forward multiplies by the rate, reverse divides; decimals shift both ways (more or fewer than the hub)."""

    @pytest.mark.parametrize(
        'source, rate, is_reverse, to_dec, from_dec, expected',
        [
            # 0.01 BTC @ 345 TAO/BTC → 3.45 TAO
            (int(Decimal('0.01') * BTC_TO_SAT), '345', False, TAO_DEC, BTC_DEC, 3_450_000_000),
            # 345 TAO @ 345 → 1 BTC
            (345 * TAO_TO_RAO, '345', True, TAO_DEC, BTC_DEC, BTC_TO_SAT),
            # 1 ETH @ 2000 → 2000 TAO (18 decimals, decimal_diff = -9)
            (10**ETH_DEC, '2000', False, TAO_DEC, ETH_DEC, 2000 * TAO_TO_RAO),
            # sol ↔ arbusdc: a spoke with FEWER decimals than the hub (6 vs 9)
            (10**SOL_DEC, '150', False, USDC_DEC, SOL_DEC, 150 * 10**USDC_DEC),
            (150 * 10**USDC_DEC, '150', True, USDC_DEC, SOL_DEC, 10**SOL_DEC),
            # 1 µUSDC @ 150 USDC/SOL → 1000/150 lamports, floored: the coarsest step of the pair
            (1, '150', True, USDC_DEC, SOL_DEC, 6),
            (0, '345', False, TAO_DEC, BTC_DEC, 0),
            (1_000_000, '0', False, TAO_DEC, BTC_DEC, 0),
        ],
    )
    def test_to_amount(self, source, rate, is_reverse, to_dec, from_dec, expected):
        result = calculate_to_amount(source, rate, is_reverse=is_reverse, to_decimals=to_dec, from_decimals=from_dec)
        assert result == expected

    @pytest.mark.parametrize(
        'source, rate',
        [
            (int(Decimal('0.5') * BTC_TO_SAT), '345.123456789'),
        ],
    )
    def test_rate_string_is_applied_as_an_exact_decimal(self, source, rate):
        result = calculate_to_amount(source, rate, is_reverse=False, to_decimals=TAO_DEC, from_decimals=BTC_DEC)
        rate_fixed = int(Decimal(rate) * RATE_PRECISION)
        assert result == source * rate_fixed * 10 // RATE_PRECISION

    @pytest.mark.parametrize(
        'source, rate, first_reverse, to_dec, from_dec',
        [
            (int(Decimal('0.01') * BTC_TO_SAT), '345', False, TAO_DEC, BTC_DEC),
            (10**SOL_DEC, '150', False, USDC_DEC, SOL_DEC),
        ],
    )
    def test_round_trip_is_exact(self, source, rate, first_reverse, to_dec, from_dec):
        there = calculate_to_amount(source, rate, is_reverse=first_reverse, to_decimals=to_dec, from_decimals=from_dec)
        back = calculate_to_amount(
            there, rate, is_reverse=not first_reverse, to_decimals=to_dec, from_decimals=from_dec
        )
        assert back == source


class TestFeeDeduction:
    """Fee = tao_amount // 100 (1%). User receives tao_amount - fee."""

    FEE_DIVISOR = 100

    def test_fee_plus_user_equals_total(self):
        """apply_fee_deduction = to_amount - to_amount // divisor, so
        fee + user_receives must exactly equal the input."""
        tao_amount = 3_450_000_000
        fee = tao_amount // self.FEE_DIVISOR
        user = apply_fee_deduction(tao_amount, self.FEE_DIVISOR)
        assert fee + user == tao_amount

    def test_apply_fee_deduction_on_unaligned_amount(self):
        """Floor division floors the fee, so 1-off amounts don't over-refund."""
        # 99 // 100 = 0 → user receives 99 (all of it, no fee taken)
        assert apply_fee_deduction(99, 100) == 99
        # 100 // 100 = 1 → user receives 99
        assert apply_fee_deduction(100, 100) == 99
        # 101 // 100 = 1 → user receives 100
        assert apply_fee_deduction(101, 100) == 100
        assert apply_fee_deduction(0, 100) == 0


class TestNormalizeRate:
    """6-sig-fig canonicalization applied at every commitment ingest gate."""

    @pytest.mark.parametrize(
        'raw, expected',
        [
            (250.123456789, '250.12'),
            (0.0001234567, '0.00012346'),
            (345.000000, '345'),
            (0.0, '0'),
            (1e-6, '1e-06'),  # sub-1e-4 values switch to scientific (:g)
        ],
    )
    def test_canonical_string(self, raw, expected):
        assert normalize_rate(raw) == expected

    def test_idempotent(self):
        """Round-tripping a normalized rate through float→normalize is a no-op."""
        for raw in (345.12, 0.0001234567, 250.123456789, 1e-6):
            once = normalize_rate(raw)
            twice = normalize_rate(float(once))
            assert once == twice

    def test_round_trip_preserves_float_equality(self):
        """float(normalize_rate(x)) must equal float(normalize_rate(x)) re-parsed
        for IEEE-754 stability — scoring (.rate) and consensus hash (.rate_str)
        share a MinerPair, so any drift would split validators."""
        for raw in (345.12, 0.0001234567, 250.123, 0.5):
            s = normalize_rate(raw)
            assert float(s) == float(normalize_rate(float(s)))


def test_directional_rate_inverts_only_the_reverse_leg():
    """Canonical stored rate → directional 'to per 1 from' display."""
    assert directional_rate('sol', 'btc', '0.0021') == '0.0021'
    assert directional_rate('btc', 'sol', '0.0021') == f'{1 / 0.0021:.8g}'
    # canonical_pair(tao, btc) = (tao, btc) — TAO is a hub: btc→tao is the reverse leg
    assert directional_rate('btc', 'tao', '0.003') == f'{1 / 0.003:.8g}'
    # canonical_pair(eth, btc) = (btc, eth) — alphabetical fallback: eth→btc is the reverse leg
    assert directional_rate('eth', 'btc', '20') == f'{1 / 20:.8g}'
    assert directional_rate('btc', 'eth', '20') == '20'
    assert directional_rate('btc', 'sol', '0') == '0'
    assert directional_rate('btc', 'sol', 'n/a') == 'n/a'


class TestIsExecutableRate:
    """Crown-eligibility gate against sentinel quotes that no user can route.

    ``rate`` is the CANONICAL number the chain stores — spoke per 1 SOL — in BOTH
    directions (what every production caller feeds). SOL is the bounded asset
    (``collateral_amount``): the contract's ``min_swap_amount``/``max_swap_amount``
    constrain the SOL leg, in lamports. Bounds here: ``min_swap=0.1 SOL``, ``max_swap=0.5 SOL``.

    The crown-relevant sentinel is a LOW canonical rate on the spoke→sol direction
    (lowest wins that sort): it maps even 1 smallest-unit of spoke above ``max_swap``,
    so nothing routes. A huge canonical rate is routable in principle and simply loses
    the sort — permissive by design.
    """

    MIN = 100_000_000  # 0.1 SOL
    MAX = 500_000_000  # 0.5 SOL

    @pytest.mark.parametrize(
        'spoke, hub, rate, floor',
        [
            ('btc', 'sol', 0.0021, None),  # 0.1 SOL needs ~21_000 sats
            ('eth', 'sol', 0.05, None),  # more decimals than the hub → fractional decimal_factor
            ('hype', 'sol', 5.0, None),
            ('tao', 'sol', 2.0, None),  # both 9-decimal
            # USDC rows size their floor at 5 USDC (crown band); 0.1 SOL needs ~15 USDC at 150/SOL.
            ('arbusdc', 'sol', 150.0, 5_000_000),
            ('solusdc', 'sol', 150.0, 5_000_000),
            ('ethusdc', 'sol', 150.0, 5_000_000),
            # paxg rates run ~1e-2; below 0.02 the gold-unit floor is the binding term in min_source.
            ('paxg', 'sol', 0.017, 2_000_000_000_000_000),
            ('paxg', 'tao', 0.046, None),  # the TAO hub has its own lanes
        ],
    )
    def test_a_sane_rate_routes_both_directions_above_the_spoke_floor(self, spoke, hub, rate, floor):
        if floor is not None:
            assert get_chain_def(spoke).min_onchain_amount == floor
        assert is_executable_rate(rate, spoke, hub, self.MIN, self.MAX) is True
        assert is_executable_rate(rate, hub, spoke, self.MIN, self.MAX) is True

    @pytest.mark.parametrize(
        'from_chain, to_chain, rate',
        [
            ('btc', 'sol', 1e-12),  # 1 sat maps to 5e12 lamports — far above max_swap
            ('eth', 'sol', 1e-20),  # 1 wei maps to 1e11 lamports
            ('ethusdc', 'sol', 1e-9),  # even the 5 USDC floor overshoots max_swap
        ],
    )
    def test_an_absurdly_low_rate_routes_nothing(self, from_chain, to_chain, rate):
        assert is_executable_rate(rate, from_chain, to_chain, self.MIN, self.MAX) is False

    @pytest.mark.parametrize(
        'from_chain, to_chain, rate',
        [
            ('btc', 'sol', 1.797e308),  # float-max: its inverse is subnormal — rejected, never crashes
            ('btc', 'sol', 0.0),
            ('btc', 'sol', -1.0),
            ('btc', 'sol', float('nan')),
        ],
    )
    def test_a_non_positive_or_non_finite_rate_is_rejected(self, from_chain, to_chain, rate):
        assert is_executable_rate(rate, from_chain, to_chain, self.MIN, self.MAX) is False

    def test_huge_rate_loses_the_sort_but_is_routable(self):
        # 1e10 BTC/SOL is a terrible offer that can never win the crown; a (huge)
        # source does route an in-bounds SOL leg, so the gate stays permissive.
        assert is_executable_rate(1e10, 'btc', 'sol', self.MIN, self.MAX) is True

    def test_bounds_unset_is_permissive(self):
        """Both bounds at 0 → no on-chain limit configured → don't filter.
        Matches the contract's unset-bounds sentinel."""
        assert is_executable_rate(1e-12, 'btc', 'sol', 0, 0) is True
        assert is_executable_rate(1e-12, 'sol', 'btc', 0, 0) is True

    def test_max_unset_only_lower_bound_enforced(self):
        """If only min_swap is set, every fundable-source rate is executable."""
        assert is_executable_rate(1e-12, 'btc', 'sol', self.MIN, 0) is True
        assert is_executable_rate(1e-12, 'sol', 'btc', self.MIN, 0) is True

    def test_no_hub_pair_is_permissive(self):
        """A pair with no hub leg (btc↔eth) has no bounded asset to enforce
        against → permissive regardless of rate."""
        assert is_executable_rate(1e10, 'btc', 'eth', self.MIN, self.MAX) is True
        assert is_executable_rate(1e-8, 'eth', 'btc', self.MIN, self.MAX) is True

    DUST = get_chain_def('btc').min_onchain_amount  # smallest fundable BTC source

    def test_one_sat_boundary_rate_rejected(self):
        """At r = 10/max_swap the only in-bounds source is 1 sat — below the BTC
        dust floor, so unfundable. Rejected (the boundary of the squat regime)."""
        rate = 10 / self.MAX  # 1 sat → exactly max_swap on the SOL leg
        assert is_executable_rate(rate, 'btc', 'sol', self.MIN, self.MAX) is False
        assert is_executable_rate(rate, 'sol', 'btc', self.MIN, self.MAX) is False

    def test_dust_floor_boundary_rate_executable(self):
        """At the rate where the dust floor maps exactly to max_swap, the smallest
        fundable source is in-bounds — just executable."""
        rate = (10 * self.DUST) / self.MAX  # DUST sats → max_swap on the SOL leg
        assert is_executable_rate(rate, 'btc', 'sol', self.MIN, self.MAX) is True
        assert is_executable_rate(rate, 'sol', 'btc', self.MIN, self.MAX) is True

    def test_just_past_dust_floor_boundary_rejected(self):
        """Just below the boundary, even the dust floor overshoots max_swap →
        no fundable source routes."""
        rate = ((10 * self.DUST) / self.MAX) * 0.999
        assert is_executable_rate(rate, 'btc', 'sol', self.MIN, self.MAX) is False

    def test_declared_alpha_leg_is_bounded_by_its_tao_value(self):
        """sn7↔avax is TAO-backed: the TAO bounds apply to the sn7 leg valued at 0.01 TAO per sn7."""
        tao_bounds = (TAO_TO_RAO // 10, 10 * TAO_TO_RAO)
        priced = {'bounded_chain': 'sn7', 'unit_value': 0.01}
        assert is_executable_rate(0.15, 'avax', 'sn7', *tao_bounds, **priced) is True  # AVAX per sn7
        assert is_executable_rate(1e-30, 'avax', 'sn7', *tao_bounds, **priced) is False  # unroutable
        # sol↔sn7 is anchored on SOL but bounds its sn7 leg (rate = sn7 per SOL).
        assert is_executable_rate(15_000.0, 'sol', 'sn7', *tao_bounds, **priced) is True
        assert is_executable_rate(1e30, 'sol', 'sn7', *tao_bounds, **priced) is False


class TestQuantizeRate:
    """quantize_rate_fixed floors to RATE_SIG_FIGS (=5) sig figs, mirroring the on-chain
    quantize_rate_sig_figs (set_quote.rs). Keep these cases in lockstep with the Rust unit test."""

    P = RATE_PRECISION

    def test_zero_and_small_pass_through(self):
        assert quantize_rate_fixed(0) == 0
        assert quantize_rate_fixed(-5) == 0
        assert quantize_rate_fixed(12_345) == 12_345  # <= 5 digits, untouched

    def test_floors_never_rounds(self):
        # 1.23459 → 1.2345 (floor, not 1.2346); 123456 → 123450.
        assert quantize_rate_fixed(1_234_590_000_000_000_000) == 1_234_500_000_000_000_000
        assert quantize_rate_fixed(123_456) == 123_450

    def test_sub_perceptible_undercut_collapses_to_same_bucket(self):
        # 5.00001 and 5.00002 both floor to 5.0 → they tie & split, no free crown steal.
        assert quantize_rate_fixed(5_000_010_000_000_000_000) == 5 * self.P
        assert quantize_rate_fixed(5_000_020_000_000_000_000) == 5 * self.P
        assert quantize_rate_fixed(4_999_900_000_000_000_000) != quantize_rate_fixed(5 * self.P)


class TestRateQuoteCandidates:
    """``rate_quote`` candidates: the selector's ranking for the asked size, served whole so a
    consumer can walk a tolerance band with no second scan. Bounds 0.1–5 SOL, deep collateral."""

    MIN = 100_000_000
    MAX = 5_000_000_000
    SOL = 1_000_000_000

    def _validator(self, from_chain, to_chain, rates, unbound=()):
        """One sol-backed miner per rate, in that order: bound states served to the request, quotes read off
        the accounts; ``unbound`` indexes have no attribution (no Binding PDA)."""
        miners = [SolKeypair().pubkey() for _ in rates]
        hotkeys = [hotkey_ss58(bytes([i + 1]) * 32) if i not in unbound else None for i in range(len(miners))]
        quotes = [
            SimpleNamespace(
                miner=pk,
                from_chain=from_chain,
                to_chain=to_chain,
                rate=int(Decimal(rate) * RATE_PRECISION),
                collateral_chain='sol',
                miner_from_addr='from',
                miner_to_addr='to',
            )
            for pk, rate in zip(miners, rates)
        ]
        live_states = {
            hk: SimpleNamespace(
                miner=pk,
                active=True,
                failed_swaps=0,
                collateral=100 * self.SOL,
                active_swap_backings=0,
                busy_until=[0, 0],
            )
            for pk, hk in zip(miners, hotkeys)
            if hk
        }
        accounts = {'MinerQuote': [(q.miner, q) for q in quotes]}
        cfg = SimpleNamespace(min_swap_amount=self.MIN, max_swap_amount=self.MAX)
        reservations, pools = {}, {}  # (miner, backing) -> account, as the World holds them
        client = SimpleNamespace(
            get_all=accounts.__getitem__,
            get_config=lambda: cfg,
            get_bond_attestation=lambda miner, chain: None,
            get_reservation=lambda miner, backing='sol': reservations.get((str(miner), backing)),
            get_pool=lambda miner, backing='sol': pools.get((str(miner), backing)),
            reservations=reservations,
            pools=pools,
        )
        return Ctx(client, states=live_states), hotkeys

    def test_candidates_follow_selector_order_capped(self):
        # 6 quotes, sol→btc for 1 SOL: most BTC first, an exact tie keeps input order, 6th cut.
        rates = ['0.5', '0.6', '0.5', '0.55', '0.45', '0.4']
        validator, hotkeys = self._validator('sol', 'btc', rates)
        rq = rate_quote(validator, 'sol', 'btc', self.SOL)
        assert [c['rate_display'] for c in rq.candidates] == ['0.6', '0.55', '0.5', '0.5', '0.45']
        assert [c['miner_hotkey'] for c in rq.candidates] == [hotkeys[i] for i in (1, 3, 0, 2, 4)]
        assert len(rq.candidates) == RATE_LEVELS_LIMIT
        assert rq.candidates[0] == {
            'miner_hotkey': rq.quote.miner_hotkey,
            'rate_display': rq.quote.rate_display,
            'to_amount': rq.quote.to_amount,
            'max_from_amount': self.MAX,
            'estimated_free_at': 0,
        }
        assert rq.candidates[1]['to_amount'] == 55_000_000
        assert rq.min_from_amount == self.MIN  # hub source: min_swap as-is

    def _open_pool(self, ctx, hotkey, from_chain, to_chain, closes_in=20):
        """An open pool on ``hotkey``'s sol hub, with busy_until set as the contract sets it at open."""
        state = ctx.miner_states()[hotkey]
        now = int(time.time())
        state.busy_until[0] = now + closes_in + 150 + 600
        ctx.accounts().pools[(str(state.miner), 'sol')] = SimpleNamespace(
            from_chain=from_chain, to_chain=to_chain, opened_at=now - 5, closes_at=now + closes_in
        )
        return state

    def test_sole_busy_miner_is_still_quoted(self):
        ctx, hotkeys = self._validator('sol', 'btc', ['0.5'])
        state = self._open_pool(ctx, hotkeys[0], 'btc', 'sol')

        rq = rate_quote(ctx, 'sol', 'btc', self.SOL)

        assert rq.quote is not None
        assert rq.quote.estimated_free_at == state.busy_until[0]
        assert rq.candidates[0]['estimated_free_at'] == state.busy_until[0]

    def test_free_candidate_ranks_ahead_of_better_busy_rate(self):
        ctx, hotkeys = self._validator('sol', 'btc', ['0.6', '0.5'])
        self._open_pool(ctx, hotkeys[0], 'btc', 'sol')

        rq = rate_quote(ctx, 'sol', 'btc', self.SOL)

        assert [c['miner_hotkey'] for c in rq.candidates] == [hotkeys[1], hotkeys[0]]
        assert rq.quote.miner_hotkey == hotkeys[1] and rq.quote.estimated_free_at == 0

    def test_open_pool_on_the_asked_pair_is_joinable_now(self):
        ctx, hotkeys = self._validator('sol', 'btc', ['0.6', '0.5'])
        self._open_pool(ctx, hotkeys[0], 'sol', 'btc')

        rq = rate_quote(ctx, 'sol', 'btc', self.SOL)

        assert rq.quote.miner_hotkey == hotkeys[0] and rq.quote.estimated_free_at == 0

    def test_unfilled_seat_past_its_finalize_window_is_free(self):
        ctx, hotkeys = self._validator('sol', 'btc', ['0.6', '0.5'])
        state = ctx.miner_states()[hotkeys[0]]
        now = int(time.time())
        state.busy_until[0] = now + 500  # set at the pool open, still ahead
        ctx.accounts().reservations[(str(state.miner), 'sol')] = SimpleNamespace(reserved_until=0, finalize_by=now - 10)

        rq = rate_quote(ctx, 'sol', 'btc', self.SOL)

        assert rq.quote.miner_hotkey == hotkeys[0] and rq.quote.estimated_free_at == 0

    @pytest.mark.parametrize('bad_field', ['miner_from_addr', 'miner_to_addr'])
    def test_bad_best_address_is_absent_from_quote_levels_depth_and_candidates(self, bad_field):
        validator, hotkeys = self._validator('sol', 'btc', ['0.6', '0.5'])
        quotes = [q for _key, q in validator.client.get_all('MinerQuote')]
        for index, quote in enumerate(quotes):
            quote.miner_from_addr = f'from-{index}'
            quote.miner_to_addr = f'to-{index}'
        setattr(quotes[0], bad_field, 'bad')
        provider = SimpleNamespace(chain=SimpleNamespace(is_valid_address=lambda address: address != 'bad'))
        validator.providers = {'sol': provider, 'btc': provider}

        rq = rate_quote(validator, 'sol', 'btc', self.SOL)

        assert rq.quote.miner_hotkey == hotkeys[1]
        assert rq.levels == [{'rate_display': '0.5', 'max_from_amount': self.MAX}]
        assert rq.max_from_amount == self.MAX
        assert [candidate['rate_display'] for candidate in rq.candidates] == ['0.5']

    def test_all_bad_addresses_leave_a_truthful_empty_rate(self):
        validator, _ = self._validator('sol', 'btc', ['0.6'])
        quote = validator.client.get_all('MinerQuote')[0][1]
        quote.miner_from_addr = 'bad'
        quote.miner_to_addr = 'valid'
        provider = SimpleNamespace(chain=SimpleNamespace(is_valid_address=lambda address: address != 'bad'))
        validator.providers = {'sol': provider}

        rq = rate_quote(validator, 'sol', 'btc', self.SOL)

        assert rq.quote is None and rq.reason == 'no miner offers this direction'
        assert rq.levels == [] and rq.max_from_amount == 0 and rq.min_from_amount == 0 and rq.candidates == []

    def test_unbound_miners_backfill_before_the_cap(self):
        # 7 viable, the selector's top pick unbound: the quote is the best BOUND intake, the
        # unbound one never rides, and the next bound miner backfills the fifth slot.
        rates = ['0.5', '0.6', '0.5', '0.55', '0.45', '0.4', '0.3']
        validator, hotkeys = self._validator('sol', 'btc', rates, unbound={1})
        rq = rate_quote(validator, 'sol', 'btc', self.SOL)
        assert rq.quote.miner_hotkey == hotkeys[3] == rq.candidates[0]['miner_hotkey']
        assert rq.quote.to_amount == 55_000_000 == rq.candidates[0]['to_amount']
        assert [c['miner_hotkey'] for c in rq.candidates] == [hotkeys[i] for i in (3, 0, 2, 4, 5)]

    def test_min_from_amount_is_in_source_units_for_a_spoke_source(self):
        # btc→sol: the minimum is on the SOL (hub) leg. The floor is the smallest BTC amount whose
        # SOL leg reaches min_swap at the best level rate — exact, so its predecessor falls short.
        validator, _ = self._validator('btc', 'sol', ['0.5', '0.25'])
        rq = rate_quote(validator, 'btc', 'sol', 10_000_000)
        floor = rq.min_from_amount
        assert floor == 2_500_000  # 0.025 BTC at 0.25 BTC/SOL → exactly 0.1 SOL
        assert compute_intake_amounts('btc', 'sol', floor, '0.25').collateral_amount >= self.MIN
        assert compute_intake_amounts('btc', 'sol', floor - 1, '0.25').collateral_amount < self.MIN
