"""SCALE codec primitives shared by the contract client, event watcher, and axon handlers.

Only the subset of SCALE we need for the ink! swap manager contract — not a
general SCALE implementation. Streaming decoders return ``(value, new_offset)``
so compound structs can chain reads without manual offset bookkeeping.
"""

import struct
from typing import Tuple

from bittensor.utils import ss58_encode

# SS58 prefix for Bittensor (matches substrate.ss58_format on all configured networks).
SS58_PREFIX = 42

# Byte widths of fixed-size SCALE primitives.
U32_BYTES = 4
U64_BYTES = 8
U128_BYTES = 16
ACCOUNT_ID_BYTES = 32


def strip_hex_prefix(s: str) -> str:
    """Remove a leading ``0x`` from a hex string if present."""
    return s[2:] if s.startswith('0x') else s


def compact_encode_len(length: int) -> bytes:
    """SCALE compact-encode a length prefix."""
    if length < 64:
        return bytes([length << 2])
    if length < 16384:
        return bytes([((length << 2) | 1) & 0xFF, length >> 6])
    return bytes(
        [
            ((length << 2) | 2) & 0xFF,
            (length >> 6) & 0xFF,
            (length >> 14) & 0xFF,
            (length >> 22) & 0xFF,
        ]
    )


def encode_bytes(data: bytes) -> bytes:
    """SCALE-encode raw bytes as compact length prefix + bytes."""
    return compact_encode_len(len(data)) + data


def encode_str(s: str) -> bytes:
    """SCALE-encode a UTF-8 string as compact length prefix + bytes."""
    return encode_bytes(s.encode('utf-8'))


def encode_u128(value: int) -> bytes:
    """SCALE-encode a u128 as 16 little-endian bytes."""
    return value.to_bytes(U128_BYTES, 'little')


# ─── Streaming decoders ────────────────────────────────────────────────────


def decode_u32(data: bytes, offset: int) -> Tuple[int, int]:
    return struct.unpack_from('<I', data, offset)[0], offset + U32_BYTES


def decode_u64(data: bytes, offset: int) -> Tuple[int, int]:
    return struct.unpack_from('<Q', data, offset)[0], offset + U64_BYTES


def decode_u128(data: bytes, offset: int) -> Tuple[int, int]:
    lo = struct.unpack_from('<Q', data, offset)[0]
    hi = struct.unpack_from('<Q', data, offset + U64_BYTES)[0]
    return lo + (hi << 64), offset + U128_BYTES


def decode_bool(data: bytes, offset: int) -> Tuple[bool, int]:
    return data[offset] != 0, offset + 1


def decode_account_id(data: bytes, offset: int) -> Tuple[str, int]:
    raw = data[offset : offset + ACCOUNT_ID_BYTES]
    return ss58_encode(raw, SS58_PREFIX), offset + ACCOUNT_ID_BYTES


def decode_string(data: bytes, offset: int) -> Tuple[str, int]:
    """SCALE-decode a compact-length-prefixed UTF-8 string.

    Returns ``('', offset)`` on truncated or out-of-bounds input so callers
    streaming composite structs degrade cleanly instead of raising.
    """
    if offset >= len(data):
        return '', offset
    first = data[offset]
    mode = first & 0x03
    if mode == 0:
        str_len = first >> 2
        offset += 1
    elif mode == 1:
        if offset + 1 >= len(data):
            return '', offset
        str_len = (data[offset] | (data[offset + 1] << 8)) >> 2
        offset += 2
    else:
        if offset + 3 >= len(data):
            return '', offset
        str_len = (data[offset] | (data[offset + 1] << 8) | (data[offset + 2] << 16) | (data[offset + 3] << 24)) >> 2
        offset += 4
    if offset + str_len > len(data):
        return '', offset
    s = data[offset : offset + str_len].decode('utf-8', errors='replace')
    return s, offset + str_len
