"""Unit tests for per-address-type input vsize in select_utxos (issue #459).

Tests that P2SH-P2WPKH inputs are sized at 91 vB (not 68 vB) and that
select_utxos fee estimates match measured transaction sizes.
"""

import os
from unittest.mock import patch

import pytest

ADDR_TYPE_P2WPKH = 'p2wpkh'
ADDR_TYPE_P2SH_P2WPKH = 'p2wpkh-p2sh'
ADDR_TYPE_P2PKH = 'p2pkh'
ADDR_TYPE_P2TR = 'p2tr'

TEST_WIF = 'cMahea7zqjxrtgAbB7LSGbcQUr1uX1ojuat9jZodMN87JcbXMTcA'


def make_lightweight_provider():
    from allways.chain_providers.bitcoin import BitcoinProvider

    with patch.dict(os.environ, {'BTC_MODE': 'lightweight', 'BTC_PRIVATE_KEY': TEST_WIF}, clear=False):
        return BitcoinProvider()


class TestInputVsizeConstants:
    """select_utxos must use per-type input vsizes, not a flat segwit/legacy boolean."""

    def test_p2wpkh_input_vsize_is_68(self):
        provider = make_lightweight_provider()
        assert provider._INPUT_VSIZE[ADDR_TYPE_P2WPKH] == 68

    def test_p2sh_p2wpkh_input_vsize_is_91(self):
        """P2SH-P2WPKH is 91 vB (23 vB larger than P2WPKH due to redeemScript push)."""
        provider = make_lightweight_provider()
        assert provider._INPUT_VSIZE[ADDR_TYPE_P2SH_P2WPKH] == 91

    def test_p2pkh_input_vsize_is_148(self):
        provider = make_lightweight_provider()
        assert provider._INPUT_VSIZE[ADDR_TYPE_P2PKH] == 148

    def test_p2sh_p2wpkh_larger_than_p2wpkh(self):
        """Nested segwit is strictly larger than native segwit per input."""
        provider = make_lightweight_provider()
        assert provider._INPUT_VSIZE[ADDR_TYPE_P2SH_P2WPKH] > provider._INPUT_VSIZE[ADDR_TYPE_P2WPKH]


class TestSelectUtxosFeeEstimate:
    """select_utxos must produce higher fee estimates for P2SH-P2WPKH than P2WPKH."""

    def _make_utxos(self, n: int, value_each: int = 100_000):
        return [{'txid': f'aa' * 32, 'vout': i, 'value': value_each} for i in range(n)]

    def _run_select(self, provider, utxos, amount, addr_type):
        with patch.object(provider, 'estimate_fee_rate', return_value=10):
            return provider.select_utxos(utxos, amount, addr_type)

    def test_p2sh_p2wpkh_fee_exceeds_p2wpkh_fee_for_multiple_inputs(self):
        """With 4+ inputs, P2SH-P2WPKH fee must exceed P2WPKH fee (issue #459 crossover)."""
        provider = make_lightweight_provider()
        utxos = self._make_utxos(6, value_each=50_000)
        amount = 200_000

        result_wpkh = self._run_select(provider, utxos, amount, ADDR_TYPE_P2WPKH)
        result_sh = self._run_select(provider, utxos, amount, ADDR_TYPE_P2SH_P2WPKH)

        assert result_wpkh is not None, 'P2WPKH selection should succeed'
        assert result_sh is not None, 'P2SH-P2WPKH selection should succeed'

        _, _, fee_wpkh = result_wpkh
        _, _, fee_sh = result_sh

        assert fee_sh > fee_wpkh, (
            f'P2SH-P2WPKH fee ({fee_sh}) must exceed P2WPKH fee ({fee_wpkh}) for multi-input tx'
        )

    def test_single_p2sh_p2wpkh_input_fee_is_correct(self):
        """Single P2SH-P2WPKH input: fee = (11 + 1×91 + 2×31) × fee_rate = 164 × 10 = 1640."""
        provider = make_lightweight_provider()
        utxos = self._make_utxos(1, value_each=1_000_000)
        amount = 500_000
        result = self._run_select(provider, utxos, amount, ADDR_TYPE_P2SH_P2WPKH)
        assert result is not None
        _, _, fee = result
        # est_vsize = 11 + 1*91 + 2*31 = 164; fee = 164 * 10
        assert fee == 164 * 10, f'Expected fee 1640, got {fee}'

    def test_single_p2wpkh_input_fee_is_correct(self):
        """Single P2WPKH input: fee = (11 + 1×68 + 2×31) × fee_rate = 141 × 10 = 1410."""
        provider = make_lightweight_provider()
        utxos = self._make_utxos(1, value_each=1_000_000)
        amount = 500_000
        result = self._run_select(provider, utxos, amount, ADDR_TYPE_P2WPKH)
        assert result is not None
        _, _, fee = result
        # est_vsize = 11 + 1*68 + 2*31 = 141; fee = 141 * 10
        assert fee == 141 * 10, f'Expected fee 1410, got {fee}'

    def test_old_flat_segwit_would_underestimate_p2sh_p2wpkh(self):
        """Regression: the old 68-vB estimate for is_segwit=True was always wrong for P2SH-P2WPKH."""
        provider = make_lightweight_provider()
        # Directly exercise the fixed constant vs. the old flat value
        assert provider._INPUT_VSIZE[ADDR_TYPE_P2SH_P2WPKH] != 68, (
            'P2SH-P2WPKH input vsize must not be 68 (that was the pre-fix bug)'
        )
