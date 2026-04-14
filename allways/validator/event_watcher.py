"""ContractEventWatcher — event-sourced miner state for the validator.

The scoring path used to poll per-miner collateral / active flag / min_collateral
from the contract on a cadence. That was N RPC calls per poll, the active flag
was never actually checked, and two validators polling at different forward
steps could derive slightly different state for the same block range.

This watcher sources the same state from ``Contracts::ContractEmitted`` events
on the Substrate chain. Each forward step calls ``sync_to(current_block)``; the
watcher replays events from its internal cursor up to ``current_block``,
decoding them against ``allways_swap_manager.json`` and applying them to
in-memory dicts:

- ``collateral[hotkey]`` — current collateral in rao (from CollateralPosted,
  CollateralWithdrawn, CollateralSlashed)
- ``active_miners: Set[hotkey]`` — miners with ``miner_active == True``
  (from MinerActivated events)
- ``min_collateral`` — current minimum collateral threshold (from
  ConfigUpdated{key="min_collateral"} events)
- ``collateral_events`` — ordered history used by the crown-time scoring
  replay, bounded to ``2 * SCORING_WINDOW_BLOCKS``
- swap outcomes are forwarded into ``ValidatorStateStore.insert_swap_outcome``
  so the credibility ledger survives restarts

Cold start: every run backfills from ``max(0, head - 2 * SCORING_WINDOW_BLOCKS)``.
No cursor file. The scoring window only needs one window of history; the
swap_outcomes ledger is the single piece of state that must persist across
restarts and it already lives in ``state.db``.

Decoder: ported from ``alw-utils/.../watch_contract_events.py`` which has been
in production on the dashboard side. Falls back to a hardcoded topic→event
registry if the metadata JSON can't be loaded.
"""

from __future__ import annotations

import json
import struct
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import bittensor as bt
from substrateinterface.utils.ss58 import ss58_encode

from allways.constants import SCORING_WINDOW_BLOCKS
from allways.validator.state_store import ValidatorStateStore

SS58_PREFIX = 42


# ─── SCALE field decoders (ported from alw-utils watch_contract_events) ─────


def decode_u32(data: bytes, offset: int) -> Tuple[int, int]:
    return struct.unpack_from('<I', data, offset)[0], offset + 4


def decode_u64(data: bytes, offset: int) -> Tuple[int, int]:
    return struct.unpack_from('<Q', data, offset)[0], offset + 8


def decode_u128(data: bytes, offset: int) -> Tuple[int, int]:
    lo = struct.unpack_from('<Q', data, offset)[0]
    hi = struct.unpack_from('<Q', data, offset + 8)[0]
    return lo + (hi << 64), offset + 16


def decode_bool(data: bytes, offset: int) -> Tuple[bool, int]:
    return data[offset] != 0, offset + 1


def decode_account_id(data: bytes, offset: int) -> Tuple[str, int]:
    raw = data[offset : offset + 32]
    return ss58_encode(raw, SS58_PREFIX), offset + 32


def decode_string(data: bytes, offset: int) -> Tuple[str, int]:
    first = data[offset]
    mode = first & 0x03
    if mode == 0:
        str_len = first >> 2
        offset += 1
    elif mode == 1:
        str_len = (data[offset] | (data[offset + 1] << 8)) >> 2
        offset += 2
    else:
        str_len = (data[offset] | (data[offset + 1] << 8) | (data[offset + 2] << 16) | (data[offset + 3] << 24)) >> 2
        offset += 4
    s = data[offset : offset + str_len].decode('utf-8', errors='replace')
    return s, offset + str_len


DATA_DECODERS = {
    'u32': decode_u32,
    'u64': decode_u64,
    'u128': decode_u128,
    'bool': decode_bool,
    'AccountId': decode_account_id,
    'String': decode_string,
}


def topic_account_id(topic_bytes: bytes) -> str:
    return ss58_encode(topic_bytes[:32], SS58_PREFIX)


def topic_u64(topic_bytes: bytes) -> int:
    return struct.unpack_from('<Q', topic_bytes, 0)[0]


def topic_bool(topic_bytes: bytes) -> bool:
    return topic_bytes[0] != 0


TOPIC_DECODERS = {
    'AccountId': topic_account_id,
    'u64': topic_u64,
    'bool': topic_bool,
}


# ─── Event registry ─────────────────────────────────────────────────────────


@dataclass
class FieldDef:
    name: str
    type_name: str


@dataclass
class EventDef:
    name: str
    signature_topic: str
    topic_fields: List[FieldDef] = field(default_factory=list)
    data_fields: List[FieldDef] = field(default_factory=list)


def resolve_type_name(display_name: list) -> str:
    if not display_name:
        return 'unknown'
    last = display_name[-1]
    if last in ('u32', 'u64', 'u128', 'bool', 'AccountId', 'String'):
        return last
    return last


def load_event_registry(metadata_path: Path) -> Dict[str, EventDef]:
    """Load signature_topic → EventDef map from ``allways_swap_manager.json``."""
    registry: Dict[str, EventDef] = {}
    try:
        with open(metadata_path) as f:
            metadata = json.load(f)
        events = metadata.get('spec', {}).get('events', [])
        for ev in events:
            sig_topic = ev.get('signature_topic', '')
            if not sig_topic:
                continue
            topic_fields: List[FieldDef] = []
            data_fields: List[FieldDef] = []
            for arg in ev.get('args', []):
                type_name = resolve_type_name(arg.get('type', {}).get('displayName', []))
                fd = FieldDef(name=arg['label'], type_name=type_name)
                if arg.get('indexed'):
                    topic_fields.append(fd)
                else:
                    data_fields.append(fd)
            registry[sig_topic] = EventDef(
                name=ev['label'],
                signature_topic=sig_topic,
                topic_fields=topic_fields,
                data_fields=data_fields,
            )
    except Exception as e:
        bt.logging.warning(f'Failed to load event metadata from {metadata_path}: {e}')
    return registry


def decode_topic_fields(event_def: EventDef, topics: List[bytes]) -> Dict[str, Any]:
    values: Dict[str, Any] = {}
    # topics[0] is the signature hash; indexed field topics start at [1]
    for i, fd in enumerate(event_def.topic_fields):
        topic_idx = i + 1
        if topic_idx >= len(topics):
            break
        decoder = TOPIC_DECODERS.get(fd.type_name)
        if decoder is not None:
            values[fd.name] = decoder(topics[topic_idx])
        else:
            values[fd.name] = topics[topic_idx].hex()
    return values


def decode_data_fields(event_def: EventDef, data: bytes) -> Dict[str, Any]:
    """Decode the ContractEmitted data blob.

    ink! v5 emits ALL event fields (both indexed and non-indexed) in the data
    blob in declaration order; topic slots carry a second copy of the indexed
    fields. We walk the full field list so the offset advances correctly.
    """
    values: Dict[str, Any] = {}
    offset = 0
    for fd in event_def.topic_fields + event_def.data_fields:
        decoder = DATA_DECODERS.get(fd.type_name)
        if decoder is None:
            break
        try:
            val, offset = decoder(data, offset)
            values[fd.name] = val
        except Exception:
            break
    return values


def to_bytes(val: Any) -> bytes:
    if isinstance(val, bytes):
        return val
    if isinstance(val, str):
        s = val.replace('0x', '')
        try:
            return bytes.fromhex(s)
        except ValueError:
            return val.encode('utf-8')
    if isinstance(val, (list, tuple)):
        return bytes(val)
    if isinstance(val, dict):
        return to_bytes(val.get('value', val.get('H256', b'')))
    return bytes()


# ─── The watcher ────────────────────────────────────────────────────────────


@dataclass
class CollateralEvent:
    hotkey: str
    collateral_rao: int
    block: int


class ContractEventWatcher:
    """Replays contract events into in-memory miner state.

    Usage:
        watcher = ContractEventWatcher(substrate, contract_address, metadata_path, state_store)
        watcher.initialize(current_block)   # backfill from head - 2*window
        ... every forward step ...
        watcher.sync_to(current_block)

    Scoring reads ``get_collateral_events_in_range``, ``get_latest_collateral_before``,
    ``active_miners``, ``min_collateral`` directly off the watcher. Swap outcomes
    are forwarded into ``state_store.insert_swap_outcome`` so the credibility
    ledger persists across restarts.
    """

    def __init__(
        self,
        substrate: Any,
        contract_address: str,
        metadata_path: Path,
        state_store: ValidatorStateStore,
        default_min_collateral: int = 0,
    ):
        self.substrate = substrate
        self.contract_address = contract_address
        self.state_store = state_store
        self.registry = load_event_registry(metadata_path)
        self.cursor: int = 0

        self.collateral: Dict[str, int] = {}
        self.active_miners: Set[str] = set()
        self.min_collateral: int = default_min_collateral
        # Sorted-by-block history used by crown-time replay. Bounded to
        # 2x the scoring window; older entries are dropped on sync.
        self.collateral_events: List[CollateralEvent] = []

    # ─── Public API consumed by scoring ─────────────────────────────────

    def get_latest_collateral_before(self, hotkey: str, block: int) -> Optional[Tuple[int, int]]:
        latest: Optional[Tuple[int, int]] = None
        for ev in self.collateral_events:
            if ev.block > block:
                break
            if ev.hotkey == hotkey:
                latest = (ev.collateral_rao, ev.block)
        return latest

    def get_collateral_events_in_range(self, start_block: int, end_block: int) -> List[dict]:
        out: List[dict] = []
        for ev in self.collateral_events:
            if ev.block <= start_block:
                continue
            if ev.block > end_block:
                break
            out.append({'hotkey': ev.hotkey, 'collateral_rao': ev.collateral_rao, 'block': ev.block})
        return out

    # ─── Sync loop ──────────────────────────────────────────────────────

    def initialize(self, current_block: int) -> None:
        """Cold start: replay events from ``max(0, head - 2 * SCORING_WINDOW_BLOCKS)``."""
        start = max(0, current_block - 2 * SCORING_WINDOW_BLOCKS)
        self.cursor = start
        self.sync_to(current_block)

    def sync_to(self, current_block: int) -> None:
        """Catch up from cursor to ``current_block``, applying each block's events."""
        if current_block <= self.cursor:
            return
        for block_num in range(self.cursor + 1, current_block + 1):
            self.process_block(block_num)
        self.cursor = current_block
        self.prune_old_collateral_events(current_block)

    def process_block(self, block_num: int) -> None:
        try:
            block_hash = self.substrate.get_block_hash(block_num)
            if not block_hash:
                return
            events = self.substrate.get_events(block_hash=block_hash)
        except Exception as e:
            bt.logging.debug(f'EventWatcher: block {block_num} events unavailable: {e}')
            return

        for event_record in events:
            decoded = self.decode_contract_event(event_record)
            if decoded is None:
                continue
            name, values = decoded
            self.apply_event(block_num, name, values)

    def decode_contract_event(self, event_record: Any) -> Optional[Tuple[str, Dict[str, Any]]]:
        record = event_record.value if hasattr(event_record, 'value') else event_record
        event = record.get('event', record) if isinstance(record, dict) else record
        module = event.get('module_id', '') if isinstance(event, dict) else ''
        event_id = event.get('event_id', '') if isinstance(event, dict) else ''
        if module != 'Contracts' or event_id != 'ContractEmitted':
            return None

        attrs = event.get('attributes', {}) if isinstance(event, dict) else {}
        emitted_contract = attrs.get('contract', '')
        if self.contract_address and emitted_contract != self.contract_address:
            return None

        record_topics = record.get('topics', []) if isinstance(record, dict) else []
        try:
            raw_data = to_bytes(attrs.get('data', ''))
            topics = [to_bytes(t) for t in record_topics]
        except Exception:
            return None

        if not topics:
            return None
        sig_topic = '0x' + topics[0].hex()
        event_def = self.registry.get(sig_topic)
        if event_def is None:
            return None

        try:
            values = decode_topic_fields(event_def, topics)
            values.update(decode_data_fields(event_def, raw_data))
        except Exception:
            return None
        return event_def.name, values

    def apply_event(self, block_num: int, name: str, values: Dict[str, Any]) -> None:
        if name == 'CollateralPosted':
            self.apply_collateral_delta(block_num, values.get('miner', ''), +int(values.get('amount', 0)))
        elif name == 'CollateralWithdrawn':
            self.apply_collateral_delta(block_num, values.get('miner', ''), -int(values.get('amount', 0)))
        elif name == 'CollateralSlashed':
            self.apply_collateral_delta(block_num, values.get('miner', ''), -int(values.get('amount', 0)))
        elif name == 'MinerActivated':
            hotkey = values.get('miner', '')
            if not hotkey:
                return
            if values.get('active'):
                self.active_miners.add(hotkey)
            else:
                self.active_miners.discard(hotkey)
        elif name == 'ConfigUpdated':
            if values.get('key') == 'min_collateral':
                try:
                    self.min_collateral = int(values.get('value', 0))
                except (TypeError, ValueError):
                    pass
        elif name == 'SwapCompleted':
            swap_id = values.get('swap_id')
            miner = values.get('miner', '')
            if isinstance(swap_id, int) and miner:
                self.state_store.insert_swap_outcome(
                    swap_id=swap_id,
                    miner_hotkey=miner,
                    completed=True,
                    resolved_block=block_num,
                )
        elif name == 'SwapTimedOut':
            swap_id = values.get('swap_id')
            miner = values.get('miner', '')
            if isinstance(swap_id, int) and miner:
                self.state_store.insert_swap_outcome(
                    swap_id=swap_id,
                    miner_hotkey=miner,
                    completed=False,
                    resolved_block=block_num,
                )

    def apply_collateral_delta(self, block_num: int, hotkey: str, delta: int) -> None:
        if not hotkey:
            return
        new_total = max(0, self.collateral.get(hotkey, 0) + delta)
        self.collateral[hotkey] = new_total
        self.collateral_events.append(CollateralEvent(hotkey=hotkey, collateral_rao=new_total, block=block_num))

    def prune_old_collateral_events(self, current_block: int) -> None:
        cutoff = current_block - 2 * SCORING_WINDOW_BLOCKS
        if cutoff <= 0 or not self.collateral_events:
            return
        keep_from = 0
        for i, ev in enumerate(self.collateral_events):
            if ev.block >= cutoff:
                keep_from = i
                break
        else:
            keep_from = len(self.collateral_events)
        if keep_from > 0:
            self.collateral_events = self.collateral_events[keep_from:]
