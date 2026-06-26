"""Decode + ingest allways_swap_manager program events from Solana tx logs (B3).

Anchor self-CPI events are emitted as base64 `Program data:` log lines = 8-byte event discriminator +
borsh(body). Discriminators + field order copied verbatim from target/idl/allways_swap_manager.json. The
validator replays these to reconstruct per-instant miner state (collateral, busy, rate, active) for the
crown — replacing the old substrate event_watcher. Only the crown-relevant events are decoded here.
"""

from dataclasses import dataclass
from typing import Any, List, Optional, Tuple

from borsh_construct import I64, U64, U128, CStruct, String
from solders.pubkey import Pubkey

from allways.solana.layouts import Hash32, Pubkey32

# 8-byte event discriminators (IDL `events[].discriminator` = sha256("event:<Name>")[:8]).
EVENT_DISCRIMINATORS = {
    'CollateralPosted': bytes([133, 193, 58, 199, 229, 183, 154, 206]),
    'CollateralWithdrawn': bytes([51, 224, 133, 106, 74, 173, 72, 82]),
    'SwapInitiated': bytes([88, 197, 100, 28, 189, 82, 98, 2]),
    'SwapCompleted': bytes([118, 93, 218, 77, 215, 165, 112, 76]),
    'SwapTimedOut': bytes([216, 21, 45, 129, 255, 250, 107, 166]),
    'QuoteSet': bytes([216, 112, 83, 84, 181, 53, 176, 105]),
    'QuoteRemoved': bytes([52, 211, 141, 65, 95, 43, 64, 32]),
    'MinerActivated': bytes([203, 75, 131, 151, 24, 167, 159, 19]),
    'MinerDeactivated': bytes([31, 67, 233, 59, 174, 101, 245, 122]),
}

# Borsh bodies (post-discriminator), field order locked to events.rs.
EVENT_LAYOUTS = {
    'CollateralPosted': CStruct('miner' / Pubkey32, 'amount' / U64, 'total' / U64),
    'CollateralWithdrawn': CStruct('miner' / Pubkey32, 'amount' / U64, 'total' / U64),
    'SwapInitiated': CStruct(
        'swap_key' / Hash32,
        'user' / Pubkey32,
        'miner' / Pubkey32,
        'sol_amount' / U64,
        'from_amount' / U128,
        'to_amount' / U128,
        'initiated_at' / I64,
    ),
    'SwapCompleted': CStruct(
        'swap_key' / Hash32,
        'miner' / Pubkey32,
        'sol_amount' / U64,
        'fee' / U64,
        'from_chain' / String,
        'to_chain' / String,
        'from_amount' / U128,
        'to_amount' / U128,
        'rate' / U128,
    ),
    'SwapTimedOut': CStruct('swap_key' / Hash32, 'miner' / Pubkey32, 'sol_amount' / U64, 'slash' / U64),
    'QuoteSet': CStruct(
        'miner' / Pubkey32,
        'from_chain' / String,
        'to_chain' / String,
        'rate' / U128,
        'liquidity' / U128,
        'updated_at' / I64,
        'update_fee' / U64,
    ),
    'QuoteRemoved': CStruct('miner' / Pubkey32, 'from_chain' / String, 'to_chain' / String, 'remove_fee' / U64),
    'MinerActivated': CStruct('miner' / Pubkey32, 'at' / I64),
    'MinerDeactivated': CStruct('miner' / Pubkey32, 'at' / I64),
}

# Fields to convert from raw 32 bytes → solders Pubkey after parse.
EVENT_PUBKEY_FIELDS = {
    'CollateralPosted': ['miner'],
    'CollateralWithdrawn': ['miner'],
    'SwapInitiated': ['user', 'miner'],
    'SwapCompleted': ['miner'],
    'SwapTimedOut': ['miner'],
    'QuoteSet': ['miner'],
    'QuoteRemoved': ['miner'],
    'MinerActivated': ['miner'],
    'MinerDeactivated': ['miner'],
}

_BY_DISC = {disc: name for name, disc in EVENT_DISCRIMINATORS.items()}

PROGRAM_DATA_PREFIX = 'Program data: '


def decode_event(raw: bytes) -> Optional[Tuple[str, Any]]:
    """Decode one `Program data:` payload → (event_name, parsed). None for unknown/foreign discriminators
    (the program logs many event types; the crown only ingests the ones in EVENT_DISCRIMINATORS)."""
    if len(raw) < 8:
        return None
    name = _BY_DISC.get(bytes(raw[:8]))
    if name is None:
        return None
    parsed = EVENT_LAYOUTS[name].parse(raw[8:])
    for field in EVENT_PUBKEY_FIELDS.get(name, []):
        parsed[field] = Pubkey.from_bytes(bytes(parsed[field]))
    return name, parsed


@dataclass
class EventRecord:
    name: str
    fields: Any
    slot: int
    block_time: Optional[int]  # unix seconds; None only on a not-yet-stamped tip tx
    signature: str


class SolanaEventIngest:
    """Continuous, cursor-based ingest of program events. Each pass fetches every signature newer than the
    last cursor (oldest-first), decodes the crown-relevant events, and returns typed records + the advanced
    cursor. Must run every forward step so the cursor never falls behind the RPC signature-prune window."""

    def __init__(self, solana_client: Any, max_pages: int = 20, page_size: int = 1000):
        self.client = solana_client
        self.max_pages = max_pages
        self.page_size = page_size

    def _fetch_new_signatures(self, until_sig: Optional[str]) -> List[dict]:
        """Signature entries strictly newer than until_sig, returned OLDEST-first (RPC gives newest-first).
        Pages backwards via `before` until it reaches `until_sig` or runs dry."""
        collected: List[dict] = []
        before: Optional[str] = None
        for _ in range(self.max_pages):
            batch = self.client.rpc.get_signatures_for_address(
                self.client.program_id, before=before, until=until_sig, limit=self.page_size
            )
            if not batch:
                break
            collected.extend(batch)
            if len(batch) < self.page_size:
                break
            before = batch[-1]['signature']
        collected.reverse()
        return collected

    def _decode_signature(self, entry: dict) -> List[EventRecord]:
        if entry.get('err') is not None:
            return []  # a failed tx commits no events
        sig = entry['signature']
        slot = entry.get('slot')
        block_time = entry.get('blockTime')
        out: List[EventRecord] = []
        for raw in self.client.get_event_logs(sig):
            decoded = decode_event(raw)
            if decoded is not None:
                name, fields = decoded
                out.append(EventRecord(name=name, fields=fields, slot=slot, block_time=block_time, signature=sig))
        return out

    def poll(self, until_sig: Optional[str]) -> Tuple[List[EventRecord], Optional[str]]:
        """Fetch + decode all events newer than until_sig. Returns (records oldest-first, new_cursor_sig).
        The cursor is the newest signature seen this pass (unchanged if nothing new)."""
        entries = self._fetch_new_signatures(until_sig)
        records: List[EventRecord] = []
        for entry in entries:
            records.extend(self._decode_signature(entry))
        new_cursor = entries[-1]['signature'] if entries else until_sig
        return records, new_cursor
