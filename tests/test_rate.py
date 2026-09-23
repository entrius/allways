"""Tests for allways.utils.rate — to_amount calculation and fee deduction math — and the
seam's ``rate_quote`` candidates built on top of it."""

from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

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
    quantize_rate_display,
    quantize_rate_fixed,
)
from allways.validator.binding import hotkey_ss58
from allways.validator.reserve_engine import RATE_LEVELS_LIMIT, rate_quote

# Chain decimals
TAO_DEC = 9
BTC_DEC = 8
ETH_DEC = 18
SOL_DEC = 9
USDC_DEC = 6


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


class TestSolArbusdc:
    """sol ↔ arbusdc: the first spoke with FEWER decimals than the hub (6 vs 9) whose
    canonical dest is the spoke — decimal_diff = 6 - 9 = -3, integer-exact both branches."""

    def test_sol_to_arbusdc(self):
        # 1 SOL @ 150 USDC/SOL → 150 USDC
        result = calculate_to_amount(10**SOL_DEC, '150', is_reverse=False, to_decimals=USDC_DEC, from_decimals=SOL_DEC)
        assert result == 150 * 10**USDC_DEC

    def test_arbusdc_to_sol(self):
        # 150 USDC @ 150 USDC/SOL → 1 SOL
        source = 150 * 10**USDC_DEC
        result = calculate_to_amount(source, '150', is_reverse=True, to_decimals=USDC_DEC, from_decimals=SOL_DEC)
        assert result == 10**SOL_DEC

    def test_round_trip_exact(self):
        source = 10**SOL_DEC
        usdc = calculate_to_amount(source, '150', is_reverse=False, to_decimals=USDC_DEC, from_decimals=SOL_DEC)
        back = calculate_to_amount(usdc, '150', is_reverse=True, to_decimals=USDC_DEC, from_decimals=SOL_DEC)
        assert back == source

    def test_single_microusdc_granularity(self):
        # 1 µUSDC @ 150 USDC/SOL → 1000/150 lamports, floored: the coarsest step of the pair.
        result = calculate_to_amount(1, '150', is_reverse=True, to_decimals=USDC_DEC, from_decimals=SOL_DEC)
        assert result == 6


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


class TestDirectionalRate:
    """Canonical stored rate → directional 'to per 1 from' display."""

    def test_forward_is_identity(self):
        assert directional_rate('sol', 'btc', '0.0021') == '0.0021'

    def test_reverse_is_reciprocal(self):
        assert directional_rate('btc', 'sol', '0.0021') == f'{1 / 0.0021:.8g}'

    def test_tao_hub_pair_reverse(self):
        # canonical_pair(tao, btc) = (tao, btc) — TAO is a hub: btc→tao is the reverse leg
        assert directional_rate('btc', 'tao', '0.003') == f'{1 / 0.003:.8g}'
        assert directional_rate('tao', 'btc', '0.003') == '0.003'

    def test_no_hub_pair_reverse(self):
        # canonical_pair(eth, btc) = (btc, eth) — alphabetical fallback: eth→btc is the reverse leg
        assert directional_rate('eth', 'btc', '20') == f'{1 / 20:.8g}'
        assert directional_rate('btc', 'eth', '20') == '20'

    def test_zero_and_non_numeric_pass_through(self):
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

    def test_sane_btc_rates_executable_both_directions(self):
        # ~0.0021 BTC per SOL: 0.1 SOL needs ~21_000 sats — comfortably fundable.
        assert is_executable_rate(0.0021, 'btc', 'sol', self.MIN, self.MAX) is True
        assert is_executable_rate(0.0021, 'sol', 'btc', self.MIN, self.MAX) is True

    def test_crown_squat_low_btc_rate_rejected(self):
        # 1e-12 BTC/SOL wins the btc→sol sort but maps 1 sat to 5e12 lamports —
        # far above max_swap; no positive integer source routes.
        assert is_executable_rate(1e-12, 'btc', 'sol', self.MIN, self.MAX) is False
        assert is_executable_rate(1e-12, 'sol', 'btc', self.MIN, self.MAX) is False

    def test_float_max_rate_rejected(self):
        """Sentinel miners post float-max: its inverse is subnormal and the band math
        leaves float range entirely — rejected, never crashes."""
        assert is_executable_rate(1.797e308, 'btc', 'sol', self.MIN, self.MAX) is False
        assert is_executable_rate(1.797e308, 'sol', 'btc', self.MIN, self.MAX) is False

    def test_huge_rate_loses_the_sort_but_is_routable(self):
        # 1e10 BTC/SOL is a terrible offer that can never win the crown; a (huge)
        # source does route an in-bounds SOL leg, so the gate stays permissive.
        assert is_executable_rate(1e10, 'btc', 'sol', self.MIN, self.MAX) is True

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
        assert is_executable_rate(1e-12, 'btc', 'sol', 0, 0) is True
        assert is_executable_rate(1e-12, 'sol', 'btc', 0, 0) is True

    def test_max_unset_only_lower_bound_enforced(self):
        """If only min_swap is set, every fundable-source rate is executable."""
        assert is_executable_rate(1e-12, 'btc', 'sol', self.MIN, 0) is True
        assert is_executable_rate(1e-12, 'sol', 'btc', self.MIN, 0) is True

    def test_sane_eth_rates_executable(self):
        """ETH has MORE decimals than the hub (18 vs 9) → fractional decimal_factor (1e-9).
        ~0.05 ETH per SOL: 0.1 SOL needs 5e15 wei (0.005 ETH), comfortably fundable."""
        assert is_executable_rate(0.05, 'eth', 'sol', self.MIN, self.MAX) is True
        assert is_executable_rate(0.05, 'sol', 'eth', self.MIN, self.MAX) is True

    def test_crown_squat_low_eth_rate_rejected(self):
        """1e-20 ETH/SOL: 1 wei maps to 1e11 lamports — above max_swap; unroutable."""
        assert is_executable_rate(1e-20, 'eth', 'sol', self.MIN, self.MAX) is False

    def test_sane_hype_rates_executable(self):
        # ~0.2 SOL per HYPE, canonical 'HYPE per 1 SOL' — same 18 decimals as ETH but twice
        # the on-chain floor, which is the input this gate actually reads.
        assert is_executable_rate(5.0, 'hype', 'sol', self.MIN, self.MAX) is True
        assert is_executable_rate(5.0, 'sol', 'hype', self.MIN, self.MAX) is True

    def test_absurd_hype_rate_unexecutable(self):
        assert is_executable_rate(1e-20, 'hype', 'sol', self.MIN, self.MAX) is False

    def test_sane_tao_sol_rates_executable(self):
        """tao↔sol: both 9-decimal, decimal_factor 1. ~2 TAO per SOL routes."""
        assert is_executable_rate(2.0, 'tao', 'sol', self.MIN, self.MAX) is True
        assert is_executable_rate(2.0, 'sol', 'tao', self.MIN, self.MAX) is True

    def test_crown_squat_low_tao_rate_rejected(self):
        """1e-10 TAO/SOL: 1 rao overshoots max_swap on the SOL leg — unroutable."""
        assert is_executable_rate(1e-10, 'tao', 'sol', self.MIN, self.MAX) is False
        assert is_executable_rate(1e-10, 'sol', 'tao', self.MIN, self.MAX) is False

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

    def test_arbusdc_routes_at_its_five_dollar_floor(self):
        """arbusdc's floor is now 5 USDC — a rate-sanity input to the crown gate (matching ethusdc),
        tightening it against absurd rates routable only for dust. A real rate still routes BOTH ways
        (0.1 SOL needs ~15 USDC at 150/SOL), which also keeps the F1 orientation-defect guard."""
        assert get_chain_def('arbusdc').min_onchain_amount == 5_000_000
        assert is_executable_rate(150.0, 'arbusdc', 'sol', self.MIN, self.MAX) is True
        assert is_executable_rate(150.0, 'sol', 'arbusdc', self.MIN, self.MAX) is True

    def test_solusdc_routes_at_its_five_dollar_floor(self):
        """solusdc sizes its floor like every USDC row (crown band), not off Solana's cheap fees —
        and the same-ledger pair must route both ways like any other spoke."""
        assert get_chain_def('solusdc').min_onchain_amount == 5_000_000
        assert is_executable_rate(150.0, 'solusdc', 'sol', self.MIN, self.MAX) is True
        assert is_executable_rate(150.0, 'sol', 'solusdc', self.MIN, self.MAX) is True

    def test_ethusdc_routes_at_its_five_dollar_floor(self):
        """ethusdc's floor is a rate-sanity input, not a per-swap minimum (that is the
        contract's min_swap_amount): non-binding here, since 0.1 SOL needs ~15 USDC at 150/SOL."""
        assert get_chain_def('ethusdc').min_onchain_amount == 5_000_000
        assert is_executable_rate(150.0, 'ethusdc', 'sol', self.MIN, self.MAX) is True
        assert is_executable_rate(150.0, 'sol', 'ethusdc', self.MIN, self.MAX) is True

    def test_crown_squat_low_ethusdc_rate_rejected(self):
        """1e-9 µUSDC/SOL: even the 5 USDC floor overshoots max_swap — unroutable."""
        assert is_executable_rate(1e-9, 'ethusdc', 'sol', self.MIN, self.MAX) is False

    def test_paxg_routes_at_its_gold_unit_floor(self):
        """Canonical is spoke-per-1-hub, so paxg rates run ~1e-2 (0.017 PAXG per SOL at
        $75.71/SOL, $4354/oz) — three orders off every other spoke. Below rate 0.02 the floor
        is the BINDING term in min_source, so a later edit to it silently unroutes both pairs."""
        assert get_chain_def('paxg').min_onchain_amount == 2_000_000_000_000_000
        assert is_executable_rate(0.017, 'paxg', 'sol', self.MIN, self.MAX) is True
        assert is_executable_rate(0.017, 'sol', 'paxg', self.MIN, self.MAX) is True

    def test_paxg_routes_on_the_tao_hub_too(self):
        """Multi-hub: a spoke is FOUR directions, and the TAO hub has its own bounds
        (TAO_MAX_SWAP_AMOUNT_RAO = 1 tao). At 0.046 PAXG/TAO the lanes clear with ~22x
        headroom; they stop routing below TAO ~$9, which is the number to watch."""
        assert is_executable_rate(0.046, 'paxg', 'tao', self.MIN, self.MAX) is True
        assert is_executable_rate(0.046, 'tao', 'paxg', self.MIN, self.MAX) is True

    def test_crown_squat_low_paxg_rate_rejected(self):
        """1e-9 PAXG/SOL: the gold-unit floor overshoots max_swap — unroutable."""
        assert is_executable_rate(1e-9, 'paxg', 'sol', self.MIN, self.MAX) is False

    def test_arbusdc_survives_a_real_dust_floor(self):
        """What PR-E unlocks: with the orientation fixed, a real economic floor
        (0.01 USDC = 10_000 µUSDC) no longer kills the arbusdc→sol direction —
        0.1 SOL needs ~15 USDC at 150 USDC/SOL, far above any dust floor."""
        floor = replace(get_chain_def('arbusdc'), min_onchain_amount=10_000)
        with patch.dict('allways.chains.SUPPORTED_CHAINS', {'arbusdc': floor}):
            assert is_executable_rate(150.0, 'arbusdc', 'sol', self.MIN, self.MAX) is True
            assert is_executable_rate(150.0, 'sol', 'arbusdc', self.MIN, self.MAX) is True


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


class TestRateQuoteCandidates:
    """``rate_quote`` candidates: the selector's ranking for the asked size, served whole so a
    consumer can walk a tolerance band with no second scan. Bounds 0.1–5 SOL, deep collateral."""

    MIN = 100_000_000
    MAX = 5_000_000_000
    SOL = 1_000_000_000

    def _validator(self, from_chain, to_chain, rates, unbound=()):
        """One sol-backed miner per rate, in that order; ``unbound`` indexes have no Binding PDA."""
        miners = [SolKeypair().pubkey() for _ in rates]
        hotkeys = {str(pk): bytes([i + 1]) * 32 for i, pk in enumerate(miners) if i not in unbound}
        quotes = [
            SimpleNamespace(
                miner=pk,
                from_chain=from_chain,
                to_chain=to_chain,
                rate=int(Decimal(rate) * RATE_PRECISION),
                collateral_chain='sol',
            )
            for pk, rate in zip(miners, rates)
        ]
        client = SimpleNamespace(
            get_config=lambda: SimpleNamespace(min_swap_amount=self.MIN, max_swap_amount=self.MAX),
            get_all=lambda kind: [(q.miner, q) for q in quotes],
            get_miner_state=lambda pk: SimpleNamespace(active=True, collateral=100 * self.SOL),
            get_binding=lambda pk: SimpleNamespace(hotkey=hotkeys[str(pk)]) if str(pk) in hotkeys else None,
        )
        return SimpleNamespace(solana_client=client), [
            hotkey_ss58(hotkeys[str(pk)]) if str(pk) in hotkeys else None for pk in miners
        ]

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
        }
        assert rq.candidates[1]['to_amount'] == 55_000_000
        assert rq.min_from_amount == self.MIN  # hub source: min_swap as-is

    def test_miss_carries_min_and_no_candidates(self):
        validator, _ = self._validator('sol', 'btc', ['0.5'])
        rq = rate_quote(validator, 'sol', 'btc', self.MIN - 1)
        assert rq.quote is None and 'below min swap' in rq.reason
        assert rq.candidates == []
        assert rq.min_from_amount == self.MIN and rq.max_from_amount == self.MAX

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

    def test_spoke_source_miss_below_min_still_carries_the_floor(self):
        # The level rate prices the floor even when nothing fits the asked size.
        validator, _ = self._validator('btc', 'sol', ['0.5', '0.25'])
        rq = rate_quote(validator, 'btc', 'sol', 2_500_000 - 1)
        assert rq.quote is None and 'below min swap' in rq.reason
        assert rq.min_from_amount == 2_500_000 and rq.candidates == []

    def test_spoke_source_min_is_zero_without_depth(self):
        validator, _ = self._validator('btc', 'sol', [])
        assert rate_quote(validator, 'btc', 'sol', 10_000_000).min_from_amount == 0
