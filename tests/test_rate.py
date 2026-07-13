"""Tests for allways.utils.rate — to_amount calculation and fee deduction math."""

from decimal import Decimal

from allways.chains import get_chain
from allways.constants import BTC_TO_SAT, RATE_PRECISION, TAO_TO_RAO
from allways.utils.rate import (
    apply_fee_deduction,
    calculate_to_amount,
    is_executable_rate,
    normalize_rate,
    quantize_rate_display,
    quantize_rate_fixed,
)

# Chain decimals
TAO_DEC = 9
BTC_DEC = 8
ETH_DEC = 18


class TestBtcToTao:
    """BTC → TAO: forward direction, multiply by rate."""

    def test_standard_rate(self):
        # 0.01 BTC @ rate 345 (1 BTC = 345 TAO) → 3.45 TAO
        source = int(Decimal('0.01') * BTC_TO_SAT)  # 1_000_000 sat
        result = calculate_to_amount(source, '345', is_reverse=False, to_decimals=TAO_DEC, from_decimals=BTC_DEC)
        expected = 3_450_000_000  # 3.45 TAO in rao
        assert result == expected

    def test_one_btc(self):
        # 1 BTC @ rate 345 → 345 TAO
        source = BTC_TO_SAT  # 100_000_000 sat
        result = calculate_to_amount(source, '345', is_reverse=False, to_decimals=TAO_DEC, from_decimals=BTC_DEC)
        assert result == 345 * TAO_TO_RAO

    def test_round_rate(self):
        # 1 BTC @ rate 100 → 100 TAO
        source = BTC_TO_SAT
        result = calculate_to_amount(source, '100', is_reverse=False, to_decimals=TAO_DEC, from_decimals=BTC_DEC)
        assert result == 100 * TAO_TO_RAO

    def test_small_amount(self):
        # 1 sat @ rate 345 → 3450 rao
        result = calculate_to_amount(1, '345', is_reverse=False, to_decimals=TAO_DEC, from_decimals=BTC_DEC)
        assert result == 3450

    def test_fractional_rate(self):
        # 0.01 BTC @ rate 344.827586 → ~3.44827586 TAO
        source = int(Decimal('0.01') * BTC_TO_SAT)
        result = calculate_to_amount(source, '344.827586', is_reverse=False, to_decimals=TAO_DEC, from_decimals=BTC_DEC)
        rate_fixed = int(Decimal('344.827586') * RATE_PRECISION)
        expected = source * rate_fixed * 10 // RATE_PRECISION
        assert result == expected


class TestTaoToBtc:
    """TAO → BTC: reverse direction, divide by rate."""

    def test_standard_rate(self):
        # 345 TAO @ rate 345 (1 BTC = 345 TAO) → 1 BTC
        source = 345 * TAO_TO_RAO
        result = calculate_to_amount(source, '345', is_reverse=True, to_decimals=TAO_DEC, from_decimals=BTC_DEC)
        assert result == BTC_TO_SAT  # 100_000_000 sat = 1 BTC

    def test_small_amount(self):
        # 3.45 TAO @ rate 345 → 0.01 BTC = 1_000_000 sat
        source = 3_450_000_000  # 3.45 TAO in rao
        result = calculate_to_amount(source, '345', is_reverse=True, to_decimals=TAO_DEC, from_decimals=BTC_DEC)
        assert result == 1_000_000

    def test_round_rate(self):
        # 100 TAO @ rate 100 → 1 BTC
        source = 100 * TAO_TO_RAO
        result = calculate_to_amount(source, '100', is_reverse=True, to_decimals=TAO_DEC, from_decimals=BTC_DEC)
        assert result == BTC_TO_SAT


class TestRoundTrip:
    """Converting BTC→TAO then TAO→BTC should preserve amounts."""

    def test_btc_tao_btc_symmetry(self):
        source_sat = int(Decimal('0.01') * BTC_TO_SAT)
        tao_rao = calculate_to_amount(source_sat, '345', is_reverse=False, to_decimals=TAO_DEC, from_decimals=BTC_DEC)
        back_sat = calculate_to_amount(tao_rao, '345', is_reverse=True, to_decimals=TAO_DEC, from_decimals=BTC_DEC)
        assert back_sat == source_sat

    def test_tao_btc_tao_symmetry(self):
        source_rao = 345 * TAO_TO_RAO
        btc_sat = calculate_to_amount(source_rao, '345', is_reverse=True, to_decimals=TAO_DEC, from_decimals=BTC_DEC)
        back_rao = calculate_to_amount(btc_sat, '345', is_reverse=False, to_decimals=TAO_DEC, from_decimals=BTC_DEC)
        assert back_rao == source_rao


class TestDirectionSpecificRates:
    """Different rates for each direction produce different amounts."""

    def test_forward_vs_reverse_different_amounts(self):
        # Forward: 0.01 BTC @ 340 → 3.4 TAO
        fwd = calculate_to_amount(1_000_000, '340', is_reverse=False, to_decimals=TAO_DEC, from_decimals=BTC_DEC)
        assert fwd == 3_400_000_000  # 3.4 TAO

        # Reverse: 3.5 TAO @ 350 → 0.01 BTC
        rev = calculate_to_amount(3_500_000_000, '350', is_reverse=True, to_decimals=TAO_DEC, from_decimals=BTC_DEC)
        assert rev == 1_000_000  # 0.01 BTC

        # The rates differ, so round-tripping at different rates loses/gains value
        assert fwd != calculate_to_amount(
            1_000_000, '350', is_reverse=False, to_decimals=TAO_DEC, from_decimals=BTC_DEC
        )


class TestFutureEth:
    """ETH ↔ TAO with 18 decimal places (decimal_diff = 9 - 18 = -9)."""

    def test_eth_to_tao(self):
        # 1 ETH @ rate 2000 → 2000 TAO
        source = 10**ETH_DEC  # 1 ETH in wei
        result = calculate_to_amount(source, '2000', is_reverse=False, to_decimals=TAO_DEC, from_decimals=ETH_DEC)
        assert result == 2000 * TAO_TO_RAO

    def test_tao_to_eth(self):
        # 2000 TAO @ rate 2000 → 1 ETH
        source = 2000 * TAO_TO_RAO
        result = calculate_to_amount(source, '2000', is_reverse=True, to_decimals=TAO_DEC, from_decimals=ETH_DEC)
        assert result == 10**ETH_DEC

    def test_eth_tao_round_trip(self):
        source_wei = 10**ETH_DEC  # 1 ETH
        tao_rao = calculate_to_amount(source_wei, '2000', is_reverse=False, to_decimals=TAO_DEC, from_decimals=ETH_DEC)
        back_wei = calculate_to_amount(tao_rao, '2000', is_reverse=True, to_decimals=TAO_DEC, from_decimals=ETH_DEC)
        assert back_wei == source_wei


class TestEdgeCases:
    """Edge cases and invariants."""

    def test_zero_source(self):
        result = calculate_to_amount(0, '345', is_reverse=False, to_decimals=TAO_DEC, from_decimals=BTC_DEC)
        assert result == 0

    def test_zero_rate(self):
        result = calculate_to_amount(1_000_000, '0', is_reverse=False, to_decimals=TAO_DEC, from_decimals=BTC_DEC)
        assert result == 0

    def test_negative_rate_produces_negative_amount(self):
        """Negative rates aren't expected in practice — the contract rejects
        them at post time. calculate_to_amount doesn't defend against them;
        it just returns the signed product. Lock in the actual behavior so a
        silent change is caught, and document that the guard lives upstream.
        """
        result = calculate_to_amount(1_000_000, '-345', is_reverse=False, to_decimals=TAO_DEC, from_decimals=BTC_DEC)
        assert result == -calculate_to_amount(
            1_000_000, '345', is_reverse=False, to_decimals=TAO_DEC, from_decimals=BTC_DEC
        )
        assert result < 0

    def test_determinism_across_calls(self):
        results = set()
        for _ in range(100):
            results.add(
                calculate_to_amount(
                    1_000_000,
                    '345',
                    is_reverse=False,
                    to_decimals=TAO_DEC,
                    from_decimals=BTC_DEC,
                )
            )
        assert len(results) == 1

    def test_rate_string_not_float(self):
        # Decimal('0.1') is exact; float 0.1 is not
        source = 10 * BTC_TO_SAT  # 10 BTC
        result = calculate_to_amount(source, '345.1', is_reverse=False, to_decimals=TAO_DEC, from_decimals=BTC_DEC)
        rate_fixed = int(Decimal('345.1') * RATE_PRECISION)
        expected = source * rate_fixed * 10 // RATE_PRECISION
        assert result == expected

    def test_high_precision_rate(self):
        source = int(Decimal('0.5') * BTC_TO_SAT)
        result = calculate_to_amount(
            source,
            '345.123456789',
            is_reverse=False,
            to_decimals=TAO_DEC,
            from_decimals=BTC_DEC,
        )
        rate_fixed = int(Decimal('345.123456789') * RATE_PRECISION)
        expected = source * rate_fixed * 10 // RATE_PRECISION
        assert result == expected


class TestFeeDeduction:
    """Fee = tao_amount // 100 (1%). User receives tao_amount - fee."""

    FEE_DIVISOR = 100

    def test_standard_fee(self):
        to_amount = 3_450_000_000  # 3.45 TAO
        result = apply_fee_deduction(to_amount, self.FEE_DIVISOR)
        fee = to_amount // self.FEE_DIVISOR  # 34_500_000
        assert result == to_amount - fee

    def test_fee_is_floor_division(self):
        assert 1 // self.FEE_DIVISOR == 0

    def test_fee_at_100_rao(self):
        assert 100 // self.FEE_DIVISOR == 1

    def test_fee_at_99_rao(self):
        assert 99 // self.FEE_DIVISOR == 0

    def test_large_amount(self):
        tao_amount = 1000 * TAO_TO_RAO
        fee = tao_amount // self.FEE_DIVISOR
        assert fee == 10 * TAO_TO_RAO

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

    def test_apply_fee_deduction_zero_amount(self):
        assert apply_fee_deduction(0, 100) == 0


class TestNormalizeRate:
    """6-sig-fig canonicalization applied at every commitment ingest gate."""

    def test_integer_rate(self):
        assert normalize_rate(345) == '345'

    def test_already_within_precision(self):
        assert normalize_rate(345.12) == '345.12'
        assert normalize_rate(0.5) == '0.5'

    def test_truncates_excess_precision(self):
        assert normalize_rate(250.123456789) == '250.12'
        assert normalize_rate(0.0001234567) == '0.00012346'

    def test_strips_trailing_zeros(self):
        assert normalize_rate(345.000000) == '345'
        assert normalize_rate(0.500000) == '0.5'

    def test_zero(self):
        assert normalize_rate(0) == '0'
        assert normalize_rate(0.0) == '0'

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

    def test_small_rate_uses_scientific_notation(self):
        """Pre-existing :g behavior — sub-1e-4 values switch to scientific.
        Documented so a future change to fixed-point doesn't silently break."""
        assert normalize_rate(1e-6) == '1e-06'


class TestIsExecutableRate:
    """Crown-eligibility gate against sentinel quotes that no user can route.

    SOL is the bounded asset (``collateral_amount``): the contract's ``min_swap_amount``/``max_swap_amount``
    constrain the SOL leg, in lamports. Bounds: ``min_swap=0.1 SOL``, ``max_swap=0.5 SOL``. (BTC decimals
    8, SOL/TAO 9 → btc↔sol decimal_factor 10, same boundary arithmetic as the retired btc↔tao gate.)
    """

    MIN = 100_000_000  # 0.1 SOL
    MAX = 500_000_000  # 0.5 SOL

    def test_sane_btc_to_sol_rate_executable(self):
        assert is_executable_rate(326.0, 'btc', 'sol', self.MIN, self.MAX) is True

    def test_sane_sol_to_btc_rate_executable(self):
        assert is_executable_rate(326.0, 'sol', 'btc', self.MIN, self.MAX) is True

    def test_huge_btc_to_sol_rate_rejected(self):
        """1e10 SOL/BTC: 1 sat → 1e11 lamports = 100 SOL, far above 0.5 SOL max.
        No positive integer sat lands in [0.1, 0.5] SOL."""
        assert is_executable_rate(1e10, 'btc', 'sol', self.MIN, self.MAX) is False

    def test_float_max_btc_to_sol_rate_rejected(self):
        """The other sentinel miners post: float-max wins the rate sort but
        overflows the conversion math entirely."""
        assert is_executable_rate(1.797e308, 'btc', 'sol', self.MIN, self.MAX) is False

    def test_zero_rate_rejected(self):
        assert is_executable_rate(0.0, 'btc', 'sol', self.MIN, self.MAX) is False

    def test_negative_rate_rejected(self):
        assert is_executable_rate(-1.0, 'btc', 'sol', self.MIN, self.MAX) is False

    def test_non_finite_rate_rejected(self):
        assert is_executable_rate(float('inf'), 'btc', 'sol', self.MIN, self.MAX) is False
        assert is_executable_rate(float('nan'), 'btc', 'sol', self.MIN, self.MAX) is False

    def test_bounds_unset_is_permissive(self):
        """Both bounds at 0 → no on-chain limit configured → don't filter.
        Matches the contract's unset-bounds sentinel."""
        assert is_executable_rate(1e10, 'btc', 'sol', 0, 0) is True
        assert is_executable_rate(1e-10, 'sol', 'btc', 0, 0) is True

    def test_max_unset_only_lower_bound_enforced(self):
        """If only min_swap is set, every rate above the floor is executable."""
        assert is_executable_rate(1e10, 'btc', 'sol', self.MIN, 0) is True

    def test_sol_to_btc_lowball_rate_rejected(self):
        """sol→btc with rate=1e-8 implies 0.1 SOL buys 1e7 BTC — destination
        absurdity, not a granularity miss. Caught by the symmetric check:
        treating 1/r = 1e8 as a btc→sol rate, 1 sat already overshoots
        max_swap on the SOL leg, so the original rate is sentinel-low."""
        assert is_executable_rate(1e-8, 'sol', 'btc', self.MIN, self.MAX) is False

    def test_non_sol_pair_is_permissive(self):
        """A pair with no SOL leg (e.g. legacy btc↔tao) has no SOL bound to
        enforce → permissive regardless of rate."""
        assert is_executable_rate(1e10, 'btc', 'tao', self.MIN, self.MAX) is True
        assert is_executable_rate(1e-8, 'tao', 'btc', self.MIN, self.MAX) is True

    def test_sane_tao_sol_rates_executable(self):
        """tao↔sol: both 9-decimal, decimal_factor 1. A ~1:1 rate routes."""
        assert is_executable_rate(1.0, 'tao', 'sol', self.MIN, self.MAX) is True
        assert is_executable_rate(1.0, 'sol', 'tao', self.MIN, self.MAX) is True

    def test_huge_tao_to_sol_rate_rejected(self):
        """1e10 SOL/TAO: the smallest fundable TAO source maps far above max_swap."""
        assert is_executable_rate(1e10, 'tao', 'sol', self.MIN, self.MAX) is False

    def test_lowball_sol_to_tao_rate_rejected(self):
        """Symmetric out of SOL: 1e-10 TAO/SOL → inverse 1e10 overshoots on the SOL leg."""
        assert is_executable_rate(1e-10, 'sol', 'tao', self.MIN, self.MAX) is False

    DUST = get_chain('btc').min_onchain_amount  # smallest fundable BTC source

    def test_sub_dust_boundary_rate_rejected(self):
        """At max_swap/10, the only in-bounds source is 1 sat — below the BTC
        dust floor, so unfundable. Rejected (the crown-squat rate)."""
        rate = self.MAX / 10  # SOL leg at 1 sat == max_swap; 1 sat < dust
        assert is_executable_rate(rate, 'btc', 'sol', self.MIN, self.MAX) is False

    def test_dust_floor_boundary_rate_executable(self):
        """At the rate where the dust floor maps exactly to max_swap, the
        smallest fundable source is in-bounds — just executable."""
        rate = self.MAX / (10 * self.DUST)  # DUST sat → max_swap on the SOL leg
        assert is_executable_rate(rate, 'btc', 'sol', self.MIN, self.MAX) is True

    def test_just_past_dust_floor_boundary_rejected(self):
        """Just above the boundary, even the dust floor overshoots max_swap →
        no fundable source routes."""
        rate = (self.MAX / (10 * self.DUST)) * 1.0001
        assert is_executable_rate(rate, 'btc', 'sol', self.MIN, self.MAX) is False

    def test_sol_to_btc_sub_dust_boundary_rate_rejected(self):
        """Symmetric: r = 10/max_swap maps 1 sat (sub-dust) to max_swap on the
        inverse leg — rejected (the crown-squat rate)."""
        rate = 10 / self.MAX
        assert is_executable_rate(rate, 'sol', 'btc', self.MIN, self.MAX) is False

    def test_sol_to_btc_dust_floor_boundary_executable(self):
        """Symmetric boundary at the dust floor: the dust-clearing inverse
        source maps in-bounds — just executable."""
        rate = (10 * self.DUST) / self.MAX
        assert is_executable_rate(rate, 'sol', 'btc', self.MIN, self.MAX) is True

    def test_sol_to_btc_sentinel_unset_bounds_still_permissive(self):
        """Unset bounds disable the gate in both directions — keeps the
        legacy "no on-chain bounds yet" path permissive."""
        assert is_executable_rate(1e-8, 'sol', 'btc', 0, 0) is True

    def test_sol_to_btc_zero_max_only_min_set_is_permissive(self):
        """If only min_swap is set (max_swap=0), any rate above the floor
        symmetry still passes. Mirrors test_max_unset_only_lower_bound_enforced."""
        assert is_executable_rate(1e-8, 'sol', 'btc', self.MIN, 0) is True


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

    def test_genuine_5sf_improvement_survives(self):
        assert quantize_rate_fixed(4_999_900_000_000_000_000) != quantize_rate_fixed(5 * self.P)

    def test_display_helper_round_trips(self):
        assert quantize_rate_display(5.00001) == 5.0
        assert quantize_rate_display(1.23459) == 1.2345
        assert quantize_rate_display(0.0) == 0.0
