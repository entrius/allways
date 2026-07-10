"""Decode + ingest allways_swap_manager program events from Solana tx logs (B3).

Anchor self-CPI events are emitted as base64 `Program data:` log lines = 8-byte event discriminator +
borsh(body). Discriminators + field order copied verbatim from target/idl/allways_swap_manager.json. The
validator replays these to reconstruct per-instant miner state (collateral, activity, rate, active) for the
crown — replacing the old substrate event_watcher. Only the crown-relevant events are decoded here.
"""

from dataclasses import dataclass
from typing import Any, List, Optional, Tuple

from solders.pubkey import Pubkey

# One source of truth for event discriminators + borsh layouts (all program events) lives in layouts.py,
# alongside the account layouts. The validator crown ingests only the subset it cares about (event_index.py
# dispatches by name and ignores the rest); the dashboard indexer decodes the full set from the same tables.
from allways.solana.layouts import EVENT_DISCRIMINATORS, EVENT_LAYOUTS, EVENT_PUBKEY_FIELDS

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
