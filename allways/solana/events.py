"""Decode + ingest allways_swap_manager program events from Solana tx logs (B3).

Events are emitted with Anchor `emit!` as base64 `Program data:` log lines = 8-byte event discriminator +
borsh(body). Any program can log such a line, so a line counts only when the program itself logged it
(program_event_payloads). Discriminators + field order copied verbatim from target/idl/allways_swap_manager.json.
The validator replays these to reconstruct per-instant miner state (collateral, activity, rate, active) for the
crown — replacing the old substrate event_watcher. Only the crown-relevant events are decoded here.
"""

import base64
import binascii
import logging
import re
import time
from dataclasses import dataclass
from typing import Any, List, Optional, Tuple

from construct import ConstructError
from solders.pubkey import Pubkey

# One source of truth for event discriminators + borsh layouts (all program events) lives in layouts.py,
# alongside the account layouts. The validator crown ingests only the subset it cares about (event_index.py
# dispatches by name and ignores the rest); the dashboard indexer decodes the full set from the same tables.
from allways.solana.layouts import EVENT_DISCRIMINATORS, EVENT_LAYOUTS, EVENT_PRIOR_LAYOUTS, EVENT_PUBKEY_FIELDS

_BY_DISC = {disc: name for name, disc in EVENT_DISCRIMINATORS.items()}

PROGRAM_DATA_PREFIX = 'Program data: '

# Only the runtime writes these frame lines; a program's own output is always `Program log:` / `Program data:`.
_B58 = r'[1-9A-HJ-NP-Za-km-z]{32,44}'
_INVOKE = re.compile(rf'^Program ({_B58}) invoke \[(\d+)\]$')
_EXIT = re.compile(rf'^Program ({_B58}) (?:success$|failed: )')

logger = logging.getLogger(__name__)


def program_event_payloads(logs: Optional[List[str]], program_id: Any) -> List[bytes]:
    """Decoded `Program data:` payloads that ``program_id`` itself logged, read off the invoke stack. A line
    logged by any other program (one that only lists ours as an account) is not ours and is dropped.
    Frames that don't nest as the runtime writes them void the whole tx: nothing in it is attributable."""
    program, stack, out = str(program_id), [], []
    for line in logs or []:
        invoke = _INVOKE.match(line)
        if invoke:
            if int(invoke.group(2)) != len(stack) + 1:
                return _unattributable(logs)
            stack.append(invoke.group(1))
            continue
        exit_ = _EXIT.match(line)
        if exit_:
            if not stack or stack.pop() != exit_.group(1):
                return _unattributable(logs)
            continue
        if line.startswith(PROGRAM_DATA_PREFIX) and stack and stack[-1] == program:
            try:
                out.append(base64.b64decode(line[len(PROGRAM_DATA_PREFIX) :], validate=True))
            except (binascii.Error, ValueError):
                continue
    return out


def _unattributable(logs: List[str]) -> List[bytes]:
    logger.warning('Dropping a tx whose log frames do not nest (%d lines): its events cannot be attributed', len(logs))
    return []


# Slot age at which poll() stops holding the cursor for an unstamped tx (~2 min): past this the
# RPC is never going to backfill its blockTime, and holding would wedge ingest forever.
UNSTAMPED_GIVE_UP_SLOTS = 300

# Lookback cap for a fresh database or a long outage. The longest swap lives ~5h (TTL + two 140-min
# extension budgets + base timeout), so the anti-replay ledgers and fill checks need no more than this.
HISTORY_HORIZON_SECS = 6 * 3600
# Per-pass decode budget: one getTransaction per signature, so a backlog is drained across passes
# instead of in one pass the watchdog kills (and that, since ingest runs before the swap loop, blocks votes).
MAX_DECODE_PER_PASS = 500
MAX_DECODE_SECS = 30


def decode_event(raw: bytes) -> Optional[Tuple[str, Any]]:
    """Decode one `Program data:` payload → (event_name, parsed). None for unknown/foreign discriminators
    (the program logs many event types; the crown only ingests the ones in EVENT_DISCRIMINATORS)."""
    if len(raw) < 8:
        return None
    name = _BY_DISC.get(bytes(raw[:8]))
    if name is None:
        return None
    parsed = None
    for layout in (EVENT_LAYOUTS[name], *EVENT_PRIOR_LAYOUTS.get(name, ())):
        try:
            parsed = layout.parse(raw[8:])
            break
        except ConstructError:
            continue
    if parsed is None:
        # A foreign-version body (e.g. a pre-v3 event in a genesis rescan) — drop it like a
        # foreign discriminator instead of letting the parse error kill the caller's whole pass.
        return None
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

    def __init__(
        self,
        solana_client: Any,
        max_pages: int = 20,
        page_size: int = 1000,
        horizon_secs: Optional[int] = None,
        max_decode: int = MAX_DECODE_PER_PASS,
        max_decode_secs: float = MAX_DECODE_SECS,
        clock=time.time,
    ):
        self.client = solana_client
        self.max_pages = max_pages
        self.page_size = page_size
        self.horizon_secs = HISTORY_HORIZON_SECS if horizon_secs is None else horizon_secs
        self.max_decode = max_decode
        self.max_decode_secs = max_decode_secs
        self.clock = clock
        # F3: fail-closed pagination. When a backlog exceeds max_pages*page_size in one pass, the
        # partial (newest) run is buffered and paging resumes deeper next call — so the older gap
        # toward until_sig is drained across ticks instead of silently dropped.
        self._pending: List[dict] = []
        self._resume_before: Optional[str] = None

    def _fetch_new_signatures(self, until_sig: Optional[str]) -> List[dict]:
        """Signature entries strictly newer than until_sig, returned OLDEST-first (RPC gives newest-first).
        Pages backwards via `before` until it reaches `until_sig` or runs dry. If max_pages is exhausted
        with a still-full page, the gap remains: the run is buffered and `[]` is returned so the caller
        holds its cursor and this resumes deeper next call (never dropping the gap)."""
        before = self._resume_before
        # Roll back this pass's partial appends if any page fails: the caller holds its cursor and
        # re-pages from the same resume point next tick, so a retained partial page would be re-fetched
        # and duplicated (and reordered) in a later drain. `_resume_before` is untouched mid-loop, so
        # rewinding `_pending` to its pre-pass length restores the exact pre-call buffer.
        checkpoint = len(self._pending)
        try:
            for _ in range(self.max_pages):
                batch = self.client.rpc.get_signatures_for_address(
                    self.client.program_id, before=before, until=until_sig, limit=self.page_size
                )
                if not batch:
                    return self._drain_pending()
                kept = self._within_horizon(batch)
                self._pending.extend(kept)
                if len(kept) < len(batch):
                    logger.warning(
                        'Event history cut at the %ss horizon: older program events are skipped '
                        '(fresh database or long outage)',
                        self.horizon_secs,
                    )
                    return self._drain_pending()
                before = batch[-1]['signature']
                if len(batch) < self.page_size:
                    return self._drain_pending()
        except Exception:
            del self._pending[checkpoint:]
            raise
        # Gap still open: hold here and resume paging from `before` on the next call.
        self._resume_before = before
        return []

    def _within_horizon(self, batch: List[dict]) -> List[dict]:
        """The newest-first prefix of ``batch`` inside the lookback horizon. Unstamped entries are kept."""
        cutoff = self.clock() - self.horizon_secs
        for i, entry in enumerate(batch):
            block_time = entry.get('blockTime')
            if block_time is not None and block_time < cutoff:
                return batch[:i]
        return batch

    def _drain_pending(self) -> List[dict]:
        collected = self._pending
        self._pending = []
        self._resume_before = None
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
        """Fetch + decode events newer than until_sig, at most one decode budget's worth per pass (the
        cursor stops at the last entry read, so the rest follows next pass) and never further back than
        the horizon. Returns (records oldest-first, new_cursor_sig).
        The cursor advances only through stamped entries and holds at the first not-yet-stamped
        (blockTime-less) tip tx, so its events are re-read once stamped instead of skipped forever.
        An unstamped tx older than UNSTAMPED_GIVE_UP_SLOTS has its blockTime read off the transaction
        itself before anything is written off: a listing that omits one is not the same as a chain that
        has none, and the events of a closed account cannot be recovered once the cursor is past them.
        Only an entry the transaction cannot stamp either is abandoned (holding would wedge the cursor),
        and that is logged rather than silent."""
        entries = self._fetch_new_signatures(until_sig)
        newest_slot = entries[-1]['slot'] if entries else 0
        records: List[EventRecord] = []
        new_cursor = until_sig
        started, decoded = self.clock(), 0
        for entry in entries:
            if decoded >= self.max_decode or self.clock() - started >= self.max_decode_secs:
                break  # the rest of the backlog is read next pass, from the advanced cursor
            unstamped = entry.get('blockTime') is None and entry.get('err') is None
            if unstamped and newest_slot - entry['slot'] < UNSTAMPED_GIVE_UP_SLOTS:
                break
            if unstamped:
                entry['blockTime'] = self._stamp_from_tx(entry['signature'])
                unstamped = entry['blockTime'] is None
                if unstamped:
                    logger.warning(
                        'Abandoning unstamped signature=%s slot=%s — neither the listing nor the '
                        'transaction carries a blockTime, so its events are never ingested',
                        entry['signature'],
                        entry['slot'],
                    )
            if not unstamped:
                records.extend(self._decode_signature(entry))
                decoded += entry.get('err') is None
            new_cursor = entry['signature']
        return records, new_cursor

    def _stamp_from_tx(self, signature: str) -> Optional[int]:
        """blockTime read straight off the transaction, for an entry the signature listing left unstamped;
        None when the RPC has none there either, or cannot be reached for it."""
        try:
            tx = self.client.rpc.get_transaction(signature)
        except Exception as error:  # noqa: BLE001 - an unreadable tx is just an entry we cannot stamp yet
            logger.warning('Stamp read failed signature=%s: %s', signature, error)
            return None
        return (tx or {}).get('blockTime')
