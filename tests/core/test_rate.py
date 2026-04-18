"""Tests for allways.utils.rate — to_amount calculation and fee deduction math."""

from decimal import Decimal

from allways.constants import BTC_TO_SAT, RATE_PRECISION, TAO_TO_RAO
from allways.utils.rate import apply_fee_deduction, calculate_to_amount

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
