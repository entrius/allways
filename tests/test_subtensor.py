"""Tests for SubtensorProvider basics (connection, cache)."""

from unittest.mock import MagicMock

import pytest

from allways.chain_providers.subtensor import SubtensorProvider


class TestProviderBasics:
    def test_get_chain_returns_tao(self):
        from allways.chains import CHAIN_TAO
        assert SubtensorProvider(MagicMock()).get_chain() is CHAIN_TAO

    def test_check_connection_success(self):
        subtensor = MagicMock()
        subtensor.get_current_block.return_value = 12345
        SubtensorProvider(subtensor).check_connection()
        subtensor.get_current_block.assert_called_once()

    def test_check_connection_raises_on_failure(self):
        subtensor = MagicMock()
        subtensor.get_current_block.side_effect = RuntimeError('down')
        with pytest.raises(ConnectionError, match='Cannot reach Subtensor'):
            SubtensorProvider(subtensor).check_connection()

    def test_clear_cache(self):
        provider = SubtensorProvider(MagicMock())
        provider.block_cache[1] = {'data': 'x'}
        provider.clear_cache()
        assert provider.block_cache == {}
