"""Tests for allways.chains — chain registry, confirmation math, safety blocks."""

import pytest

from allways.chains import (
    CHAIN_BTC,
    CHAIN_TAO,
    confirmations_to_subtensor_blocks,
    get_chain,
)


class TestGetChain:
    def test_btc(self):
        assert get_chain('btc') is CHAIN_BTC

    def test_tao(self):
        assert get_chain('tao') is CHAIN_TAO

    def test_unsupported_raises(self):
        with pytest.raises(KeyError):
            get_chain('eth')


class TestChainProperties:
    def test_btc_decimals(self):
        assert CHAIN_BTC.decimals == 8

    def test_tao_decimals(self):
        assert CHAIN_TAO.decimals == 9

    def test_btc_block_time(self):
        assert CHAIN_BTC.seconds_per_block == 600

    def test_tao_block_time(self):
        assert CHAIN_TAO.seconds_per_block == 12


class TestConfirmationsToSubtensorBlocks:
    def test_btc(self):
        # ceil(3 * 600 / 12) = ceil(150) = 150
        assert confirmations_to_subtensor_blocks('btc') == 150

    def test_tao(self):
        # ceil(6 * 12 / 12) = ceil(6) = 6
        assert confirmations_to_subtensor_blocks('tao') == 6
