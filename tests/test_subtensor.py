"""Tests for SubtensorProvider SCALE decoding, address validation, and basics."""

from unittest.mock import MagicMock

from allways.chain_providers.subtensor import SubtensorProvider


class TestDecodeCompact:
    def test_mode0_zero(self):
        val, consumed = SubtensorProvider.decode_compact(bytes([0]))
        assert val == 0
        assert consumed == 1

    def test_mode0_max(self):
        val, consumed = SubtensorProvider.decode_compact(bytes([252]))
        assert val == 63
        assert consumed == 1

    def test_mode1_64(self):
        encoded = bytes([((64 << 2) | 1) & 0xFF, (64 << 2 | 1) >> 8])
        val, consumed = SubtensorProvider.decode_compact(encoded)
        assert val == 64
        assert consumed == 2

    def test_mode1_roundtrip_various(self):
        for n in [64, 100, 1000, 16383]:
            raw = (n << 2) | 1
            encoded = bytes([raw & 0xFF, (raw >> 8) & 0xFF])
            val, consumed = SubtensorProvider.decode_compact(encoded)
            assert val == n, f'Failed for n={n}'
            assert consumed == 2

    def test_mode2_16384(self):
        n = 16384
        raw = (n << 2) | 2
        encoded = raw.to_bytes(4, 'little')
        val, consumed = SubtensorProvider.decode_compact(encoded)
        assert val == n
        assert consumed == 4

    def test_mode2_large(self):
        n = 100000
        raw = (n << 2) | 2
        encoded = raw.to_bytes(4, 'little')
        val, consumed = SubtensorProvider.decode_compact(encoded)
        assert val == n
        assert consumed == 4

    def test_mode3_big_integer(self):
        n = 2**32 + 1
        n_bytes = (n.bit_length() + 7) // 8
        first_byte = ((n_bytes - 4) << 2) | 3
        encoded = bytes([first_byte]) + n.to_bytes(n_bytes, 'little')
        val, consumed = SubtensorProvider.decode_compact(encoded)
        assert val == n
        assert consumed == 1 + n_bytes

    def test_empty_bytes(self):
        val, consumed = SubtensorProvider.decode_compact(b'')
        assert val == 0
        assert consumed == 0

    def test_mode1_insufficient(self):
        val, consumed = SubtensorProvider.decode_compact(bytes([0x01]))
        assert val == 0
        assert consumed == 0


class TestIsValidAddress:
    def _provider(self):
        return SubtensorProvider(MagicMock())

    def test_valid_ss58(self):
        p = self._provider()
        addr = '5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY'
        assert p.is_valid_address(addr) is True

    def test_wrong_length(self):
        p = self._provider()
        assert p.is_valid_address('5GrwvaEF') is False

    def test_invalid_chars(self):
        p = self._provider()
        assert p.is_valid_address('0' * 48) is False

    def test_empty(self):
        p = self._provider()
        assert p.is_valid_address('') is False

    def test_none(self):
        p = self._provider()
        assert p.is_valid_address(None) is False


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
        import pytest
        subtensor = MagicMock()
        subtensor.get_current_block.side_effect = RuntimeError('down')
        with pytest.raises(ConnectionError, match='Cannot reach Subtensor'):
            SubtensorProvider(subtensor).check_connection()

    def test_clear_cache(self):
        provider = SubtensorProvider(MagicMock())
        provider.block_cache[1] = {'data': 'x'}
        provider.clear_cache()
        assert provider.block_cache == {}
