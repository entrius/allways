"""SCALE codec + ink! metadata reader for the bond vault.

Only the shapes this contract's surface actually uses. Balances on the WIRE are u64: the vault
runs `CustomEnvironment::Balance = u64` because subtensor's chain Balance is rao-as-u64 (the
Spike 0 finding). Event payloads carry the same amounts widened to u128 — the contract emits
`.into()` for a stable indexer ABI — so decode side and call side deliberately differ.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

ACCOUNT_LEN = 32
HASH_LEN = 32


class VaultCodecError(Exception):
    """Malformed metadata, or a payload that doesn't match the declared shape."""


# ─── encoding ────────────────────────────────────────────────────────────────


def compact(n: int) -> bytes:
    """SCALE compact-length prefix. Covers the batch sizes this contract accepts (MAX_BATCH 256)."""
    if n < 0:
        raise VaultCodecError(f'compact length cannot be negative: {n}')
    if n < 64:
        return bytes([n << 2])
    if n < 2**14:
        return ((n << 2) | 0b01).to_bytes(2, 'little')
    if n < 2**30:
        return ((n << 2) | 0b10).to_bytes(4, 'little')
    raise VaultCodecError(f'compact length out of supported range: {n}')


def u8(n: int) -> bytes:
    return int(n).to_bytes(1, 'little')


def u16(n: int) -> bytes:
    return int(n).to_bytes(2, 'little')


def u32(n: int) -> bytes:
    return int(n).to_bytes(4, 'little')


def u64(n: int) -> bytes:
    return int(n).to_bytes(8, 'little')


def boolean(b: bool) -> bytes:
    return bytes([1 if b else 0])


def account_bytes(ss58: str) -> bytes:
    """ss58 address → 32-byte AccountId."""
    import bittensor as bt

    try:
        raw = bytes(bt.Keypair(ss58_address=ss58).public_key)
    except Exception as e:
        raise VaultCodecError(f'Not a valid ss58 address: {ss58}') from e
    if len(raw) != ACCOUNT_LEN:
        raise VaultCodecError(f'AccountId must be {ACCOUNT_LEN} bytes, got {len(raw)}')
    return raw


def account_ss58(raw: bytes) -> str:
    """32-byte AccountId → ss58 address."""
    import bittensor as bt

    try:
        return bt.Keypair(public_key='0x' + bytes(raw).hex()).ss58_address
    except Exception as e:
        raise VaultCodecError(f'Not a valid AccountId: {bytes(raw).hex()}') from e


def hash32(value) -> bytes:
    """32-byte Hash from raw bytes or hex (0x-prefixed or bare) — the Solana swap_key as swap_ref."""
    if isinstance(value, (bytes, bytearray)):
        raw = bytes(value)
    else:
        text = str(value)
        raw = bytes.fromhex(text[2:] if text.startswith('0x') else text)
    if len(raw) != HASH_LEN:
        raise VaultCodecError(f'Hash must be {HASH_LEN} bytes, got {len(raw)}')
    return raw


def fee_entries(entries: Sequence[Tuple[str, int]]) -> bytes:
    """`Vec<(AccountId, Balance)>` for vote_collect_fees_batch. Order is the caller's — it is hashed
    into the round key, so every validator must submit the identical sequence."""
    out = [compact(len(entries))]
    for hotkey, total in entries:
        out.append(account_bytes(hotkey))
        out.append(u64(total))
    return b''.join(out)


# ─── decoding ────────────────────────────────────────────────────────────────


class Reader:
    """Minimal SCALE cursor over a return/event payload."""

    def __init__(self, raw: bytes):
        self.raw = bytes(raw)
        self.pos = 0

    def take(self, n: int) -> bytes:
        if self.pos + n > len(self.raw):
            raise VaultCodecError(f'payload truncated: wanted {n} bytes at {self.pos} of {len(self.raw)}')
        out = self.raw[self.pos : self.pos + n]
        self.pos += n
        return out

    def uint(self, width: int) -> int:
        return int.from_bytes(self.take(width), 'little')

    def boolean(self) -> bool:
        return self.take(1)[0] == 1

    def account(self) -> str:
        return account_ss58(self.take(ACCOUNT_LEN))

    def hash32(self) -> bytes:
        return self.take(HASH_LEN)

    def string(self) -> str:
        length = self.compact()
        return self.take(length).decode('utf-8', errors='replace')

    def compact(self) -> int:
        first = self.take(1)[0]
        mode = first & 0b11
        if mode == 0b00:
            return first >> 2
        if mode == 0b01:
            return ((first | (self.take(1)[0] << 8)) >> 2) & 0xFFFF
        if mode == 0b10:
            rest = self.take(3)
            return (int.from_bytes(bytes([first]) + rest, 'little')) >> 2
        raise VaultCodecError('big-int compact lengths are not used by this contract')


def unwrap_lang_error(raw: bytes) -> Optional[bytes]:
    """ink! wraps every message return in `Result<T, LangError>`; a leading 0x00 is Ok.
    None when the payload is empty or signals a LangError."""
    if not raw or raw[0] != 0:
        return None
    return raw[1:]


# ─── metadata ────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class EventArg:
    label: str
    type_name: str
    indexed: bool


@dataclass(frozen=True)
class EventSpec:
    label: str
    args: Tuple[EventArg, ...]
    signature_topic: Optional[str]


# ink! metadata `displayName` → decoder. Event amounts are u128 even where the message-side
# Balance is u64, so both widths appear.
_SCALARS = {
    'u8': lambda r: r.uint(1),
    'u16': lambda r: r.uint(2),
    'u32': lambda r: r.uint(4),
    'u64': lambda r: r.uint(8),
    'u128': lambda r: r.uint(16),
    'Balance': lambda r: r.uint(8),
    'bool': lambda r: r.boolean(),
    'AccountId': lambda r: r.account(),
    'Hash': lambda r: r.hash32(),
    'String': lambda r: r.string(),
}


class VaultMetadata:
    """Selectors + event specs read off the cargo-contract metadata JSON."""

    def __init__(self, spec: Dict[str, Any]):
        messages = spec.get('spec', {}).get('messages', [])
        if not messages:
            raise VaultCodecError('vault metadata carries no messages — artifact out of date?')
        self.selectors: Dict[str, bytes] = {m['label']: bytes.fromhex(m['selector'][2:]) for m in messages}
        self.events: Dict[str, EventSpec] = {}
        for e in spec.get('spec', {}).get('events', []):
            args = tuple(
                EventArg(a['label'], _display_name(a['type']), bool(a.get('indexed'))) for a in e.get('args', [])
            )
            topic = e.get('signature_topic')
            self.events[e['label']] = EventSpec(e['label'], args, topic.lower() if topic else None)
        self._by_topic = {e.signature_topic: e for e in self.events.values() if e.signature_topic}

    @classmethod
    def from_path(cls, path: Path | str) -> 'VaultMetadata':
        try:
            return cls(json.loads(Path(path).read_text()))
        except OSError as e:
            raise VaultCodecError(
                f'Vault metadata not readable at {path} ({e}) — build the contract or set ALLWAYS_VAULT_METADATA'
            ) from e

    def selector(self, label: str) -> bytes:
        if label not in self.selectors:
            raise VaultCodecError(f'Message `{label}` missing from vault metadata — artifact out of date?')
        return self.selectors[label]

    def call(self, label: str, *args: bytes) -> bytes:
        """Selector + SCALE-encoded args — the `data` blob for a `Contracts.call`."""
        return self.selector(label) + b''.join(args)

    def spec_for_topic(self, topic: str) -> Optional[EventSpec]:
        return self._by_topic.get(str(topic).lower())

    def decode_event(self, spec: EventSpec, data: bytes) -> Dict[str, Any]:
        """ink! v5 encodes EVERY field (indexed ones too) into the event payload; topics carry the
        signature + indexed hashes on top. Unknown arg types abort the decode rather than guess."""
        reader = Reader(data)
        out: Dict[str, Any] = {}
        for arg in spec.args:
            decoder = _SCALARS.get(arg.type_name)
            if decoder is None:
                raise VaultCodecError(f'{spec.label}.{arg.label}: unsupported type {arg.type_name}')
            out[arg.label] = decoder(reader)
        return out


def _display_name(type_ref: Dict[str, Any]) -> str:
    names: List[str] = list(type_ref.get('displayName') or [])
    return names[-1] if names else ''
