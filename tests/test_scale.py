"""Tests for SCALE encoding/decoding in contract_client and subtensor provider."""

import struct
from unittest.mock import MagicMock

from allways.chain_providers.subtensor import SubtensorProvider
from allways.contract_client import AllwaysContractClient
from allways.utils.scale import compact_encode_len, decode_string


def make_client():
    """Create a contract client for isolated encoder/decoder tests.

    Uses ``__new__`` to skip ``__init__`` because the real constructor
    connects to subtensor for the initial metadata read. These tests only
    need the encoder/decoder methods, so we instantiate bare and attach
    a mocked ``subtensor.substrate`` for the few methods that call
    ``ss58_decode`` / ``ss58_encode``.
    """
    client = AllwaysContractClient.__new__(AllwaysContractClient)
    client.subtensor = MagicMock()
    client.subtensor.substrate = MagicMock()
    return client


# =========================================================================
# _encode_value tests
# =========================================================================


class TestEncodeU32:
    def test_zero(self):
        c = make_client()
        assert c.encode_value(0, 'u32') == struct.pack('<I', 0)

    def test_42(self):
        c = make_client()
        assert c.encode_value(42, 'u32') == struct.pack('<I', 42)

    def test_max(self):
        c = make_client()
        assert c.encode_value(2**32 - 1, 'u32') == struct.pack('<I', 2**32 - 1)


class TestEncodeU64:
    def test_zero(self):
        c = make_client()
        assert c.encode_value(0, 'u64') == struct.pack('<Q', 0)

    def test_value(self):
        c = make_client()
        assert c.encode_value(123456789, 'u64') == struct.pack('<Q', 123456789)

    def test_max(self):
        c = make_client()
        assert c.encode_value(2**64 - 1, 'u64') == struct.pack('<Q', 2**64 - 1)


class TestEncodeU128:
    def test_zero(self):
        c = make_client()
        result = c.encode_value(0, 'u128')
        assert len(result) == 16
        assert result == b'\x00' * 16

    def test_small(self):
        c = make_client()
        v = 42
        result = c.encode_value(v, 'u128')
        low, high = struct.unpack_from('<QQ', result)
        assert low == 42
        assert high == 0

    def test_max(self):
        c = make_client()
        v = 2**128 - 1
        result = c.encode_value(v, 'u128')
        low, high = struct.unpack_from('<QQ', result)
        assert low == 2**64 - 1
        assert high == 2**64 - 1
        assert low + (high << 64) == v

    def test_high_bits(self):
        c = make_client()
        v = 2**64  # Just the high word
        result = c.encode_value(v, 'u128')
        low, high = struct.unpack_from('<QQ', result)
        assert low == 0
        assert high == 1


class TestEncodeBool:
    def test_true(self):
        c = make_client()
        assert c.encode_value(True, 'bool') == b'\x01'

    def test_false(self):
        c = make_client()
        assert c.encode_value(False, 'bool') == b'\x00'


class TestEncodeStr:
    def test_hello(self):
        c = make_client()
        result = c.encode_value('hello', 'str')
        # Compact length 5 = 5 << 2 = 20 = 0x14, then 'hello' bytes
        assert result == bytes([20]) + b'hello'

    def test_empty(self):
        c = make_client()
        result = c.encode_value('', 'str')
        assert result == bytes([0])


class TestEncodeVecU64:
    def test_three_items(self):
        c = make_client()
        result = c.encode_value([1, 2, 3], 'vec_u64')
        # Compact len 3 = 3 << 2 = 12, then 3 x u64 LE
        expected = bytes([12]) + struct.pack('<QQQ', 1, 2, 3)
        assert result == expected

    def test_empty(self):
        c = make_client()
        result = c.encode_value([], 'vec_u64')
        assert result == bytes([0])


# =========================================================================
# _compact_encode_len tests
# =========================================================================


class TestCompactEncodeLen:
    def test_single_byte_mode_zero(self):
        assert compact_encode_len(0) == bytes([0])

    def test_single_byte_mode_63(self):
        # 63 << 2 = 252 = 0xFC
        assert compact_encode_len(63) == bytes([252])

    def test_two_byte_mode_64(self):
        result = compact_encode_len(64)
        assert len(result) == 2
        # Decode back: (byte0 | byte1<<8) >> 2 = 64
        val = (result[0] | (result[1] << 8)) >> 2
        assert val == 64

    def test_two_byte_mode_16383(self):
        result = compact_encode_len(16383)
        assert len(result) == 2
        val = (result[0] | (result[1] << 8)) >> 2
        assert val == 16383

    def test_four_byte_mode_16384(self):
        result = compact_encode_len(16384)
        assert len(result) == 4
        val = (result[0] | (result[1] << 8) | (result[2] << 16) | (result[3] << 24)) >> 2
        assert val == 16384

    def test_four_byte_mode_large(self):
        result = compact_encode_len(100000)
        assert len(result) == 4
        val = (result[0] | (result[1] << 8) | (result[2] << 16) | (result[3] << 24)) >> 2
        assert val == 100000


# =========================================================================
# _extract_* tests
# =========================================================================


class TestExtractU32:
    def test_roundtrip(self):
        c = make_client()
        for v in [0, 42, 1000, 2**32 - 1]:
            encoded = struct.pack('<I', v)
            assert c.extract_u32(encoded) == v

    def test_insufficient_bytes(self):
        c = make_client()
        assert c.extract_u32(b'\x00\x00') is None

    def test_empty(self):
        c = make_client()
        assert c.extract_u32(b'') is None

    def test_none(self):
        c = make_client()
        assert c.extract_u32(None) is None


class TestExtractU64:
    def test_roundtrip(self):
        c = make_client()
        for v in [0, 42, 10**15, 2**64 - 1]:
            encoded = struct.pack('<Q', v)
            assert c.extract_u64(encoded) == v

    def test_insufficient_bytes(self):
        c = make_client()
        assert c.extract_u64(b'\x00' * 4) is None


class TestExtractU128:
    def test_roundtrip(self):
        c = make_client()
        for v in [0, 42, 10**18, 2**128 - 1]:
            encoded = c.encode_value(v, 'u128')
            assert c.extract_u128(encoded) == v

    def test_max_value(self):
        c = make_client()
        v = 2**128 - 1
        data = struct.pack('<QQ', v & 0xFFFFFFFFFFFFFFFF, v >> 64)
        assert c.extract_u128(data) == v

    def test_insufficient_bytes(self):
        c = make_client()
        assert c.extract_u128(b'\x00' * 8) is None


# =========================================================================
# decode_string tests
# =========================================================================


class TestDecodeString:
    def test_roundtrip_short(self):
        c = make_client()
        encoded = c.encode_value('hello', 'str')
        s, offset = decode_string(encoded, 0)
        assert s == 'hello'
        assert offset == len(encoded)

    def test_roundtrip_empty(self):
        c = make_client()
        encoded = c.encode_value('', 'str')
        s, offset = decode_string(encoded, 0)
        assert s == ''

    def test_roundtrip_medium(self):
        c = make_client()
        text = 'x' * 100  # Still in single-byte compact mode
        encoded = c.encode_value(text, 'str')
        s, offset = decode_string(encoded, 0)
        assert s == text

    def test_offset_past_end(self):
        s, offset = decode_string(b'\x00', 10)
        assert s == ''

    def test_roundtrip_two_byte_compact(self):
        c = make_client()
        # String of length 64+ triggers two-byte compact mode
        text = 'a' * 64
        encoded = c.encode_value(text, 'str')
        s, offset = decode_string(encoded, 0)
        assert s == text


# =========================================================================
# _decode_swap_data tests
# =========================================================================


class TestDecodeSwapData:
    def encode_swap_bytes(
        self,
        client,
        swap_id=1,
        from_chain='btc',
        to_chain='tao',
        from_amount=100000,
        to_amount=0,
        tao_amount=1_000_000_000,
        miner_from_address='bc1qminer',
        miner_to_address='5Cminer',
        rate='345',
        from_tx_hash='txhash',
        from_tx_block=50,
        to_tx_hash='',
        to_tx_block=0,
        status=0,
        initiated_block=100,
        timeout_block=400,
        fulfilled_block=0,
        completed_block=0,
    ):
        """Build raw SCALE bytes for a SwapData struct."""
        user_bytes = b'\x01' * 32
        miner_bytes = b'\x02' * 32

        client.subtensor.substrate.ss58_encode.side_effect = lambda hex_str: f'5{hex_str[:47]}'

        data = b''
        data += struct.pack('<Q', swap_id)
        data += user_bytes
        data += miner_bytes
        data += client.encode_value(from_chain, 'str')
        data += client.encode_value(to_chain, 'str')
        data += client.encode_value(from_amount, 'u128')
        data += client.encode_value(to_amount, 'u128')
        data += client.encode_value(tao_amount, 'u128')
        data += client.encode_value('bc1quser', 'str')
        data += client.encode_value('5Cuser', 'str')
        data += client.encode_value(miner_from_address, 'str')
        data += client.encode_value(miner_to_address, 'str')
        data += client.encode_value(rate, 'str')
        data += client.encode_value(from_tx_hash, 'str')
        data += struct.pack('<I', from_tx_block)
        data += client.encode_value(to_tx_hash, 'str')
        data += struct.pack('<I', to_tx_block)
        data += bytes([status])
        data += struct.pack('<I', initiated_block)
        data += struct.pack('<I', timeout_block)
        data += struct.pack('<I', fulfilled_block)
        data += struct.pack('<I', completed_block)
        return data

    def test_decode_valid(self):
        c = make_client()
        data = self.encode_swap_bytes(c)
        swap = c.decode_swap_data(data)
        assert swap is not None
        assert swap.id == 1
        assert swap.from_chain == 'btc'
        assert swap.to_chain == 'tao'
        assert swap.from_amount == 100000
        assert swap.tao_amount == 1_000_000_000
        assert swap.from_tx_hash == 'txhash'
        assert swap.from_tx_block == 50
        assert swap.initiated_block == 100
        assert swap.timeout_block == 400
        assert swap.status.value == 0

    def test_decode_fulfilled(self):
        c = make_client()
        data = self.encode_swap_bytes(
            c, status=1, fulfilled_block=150, to_tx_hash='dtxhash', to_tx_block=145, to_amount=990_000_000
        )
        swap = c.decode_swap_data(data)
        assert swap is not None
        assert swap.status.value == 1
        assert swap.fulfilled_block == 150
        assert swap.to_amount == 990_000_000

    def test_decode_truncated(self):
        c = make_client()
        data = self.encode_swap_bytes(c)
        swap = c.decode_swap_data(data[:10])
        assert swap is None

    def test_decode_empty(self):
        c = make_client()
        swap = c.decode_swap_data(b'')
        assert swap is None


# =========================================================================
# SubtensorProvider.decode_compact tests
# =========================================================================


class TestDecodeCompact:
    def test_mode0_zero(self):
        val, consumed = SubtensorProvider.decode_compact(bytes([0]))
        assert val == 0
        assert consumed == 1

    def test_mode0_max(self):
        # 63 in mode 0: 63 << 2 = 252
        val, consumed = SubtensorProvider.decode_compact(bytes([252]))
        assert val == 63
        assert consumed == 1

    def test_mode1_64(self):
        # Encode 64: (64 << 2) | 1 = 257 -> bytes [1, 1] (LE)
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
        # Mode 3: first byte = (num_extra_bytes - 4) << 2 | 3
        # For a number that fits in 5 bytes: n_bytes=5, first_byte = (5-4)<<2|3 = 7
        n = 2**32 + 1  # 4294967297, needs 5 bytes
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


# =========================================================================
# SubtensorProvider.is_valid_address tests
# =========================================================================


class TestIsValidAddress:
    def provider(self):
        return SubtensorProvider(MagicMock())

    def test_valid_ss58(self):
        p = self.provider()
        # Typical 48-char SS58 address
        addr = '5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY'
        assert p.is_valid_address(addr) is True

    def test_wrong_length(self):
        p = self.provider()
        assert p.is_valid_address('5GrwvaEF') is False

    def test_invalid_chars(self):
        p = self.provider()
        # Contains 0, O, I, l — invalid in base58
        assert p.is_valid_address('0' * 48) is False

    def test_invalid_checksum(self):
        p = self.provider()
        assert p.is_valid_address('5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQZ') is False

    def test_base58_shape_without_valid_ss58_payload(self):
        p = self.provider()
        assert p.is_valid_address('1' * 48) is False

    def test_empty(self):
        p = self.provider()
        assert p.is_valid_address('') is False

    def test_none(self):
        p = self.provider()
        assert p.is_valid_address(None) is False
