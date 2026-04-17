"""ContractEventWatcher — event-sourced miner state for the validator.

Each forward step calls ``sync_to(current_block)``; the watcher replays
``Contracts::ContractEmitted`` events from its cursor up to ``current_block``
and applies them to in-memory state used by the crown-time scoring replay.
Tracks three things: the current on-chain active set (for rate-gating),
per-hotkey busy deltas (reservations → swap resolution), and swap outcomes
forwarded into ``ValidatorStateStore.insert_swap_outcome`` so the
credibility ledger survives restarts. Collateral and config scalars are
trusted to the contract — see ``vote_deactivate`` for the min-raise
remediation path.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import bittensor as bt

from allways.constants import SCORING_WINDOW_BLOCKS
from allways.utils.scale import (
    decode_account_id,
    decode_bool,
    decode_string,
    decode_u32,
    decode_u64,
    decode_u128,
    strip_hex_prefix,
)
from allways.validator.state_store import ValidatorStateStore

DATA_DECODERS = {
    'u32': decode_u32,
    'u64': decode_u64,
    'u128': decode_u128,
    'bool': decode_bool,
    'AccountId': decode_account_id,
    'String': decode_string,
}


def topic_account_id(topic_bytes: bytes) -> str:
    return decode_account_id(topic_bytes, 0)[0]


def topic_u64(topic_bytes: bytes) -> int:
    return decode_u64(topic_bytes, 0)[0]


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
        try:
            return bytes.fromhex(strip_hex_prefix(val))
        except ValueError:
            return val.encode('utf-8')
    if isinstance(val, (list, tuple)):
        return bytes(val)
    if isinstance(val, dict):
        return to_bytes(val.get('value', val.get('H256', b'')))
    return bytes()


# ─── The watcher ────────────────────────────────────────────────────────────


@dataclass
class BusyEvent:
    """``delta`` is +1 on SwapInitiated and -1 on SwapCompleted/SwapTimedOut.
    A miner is busy (excluded from crown) whenever the running sum is > 0."""

    hotkey: str
    delta: int
    block: int


@dataclass
class ActiveEvent:
    """Transition of a miner's on-chain active flag. Replayed per-block so
    scoring judges active state as-of each block in the window, not as-of
    the scoring moment."""

    hotkey: str
    active: bool
    block: int


MAX_BLOCKS_PER_SYNC = 50


class ContractEventWatcher:
    """Replays contract events into in-memory miner state."""

    def __init__(
        self,
        substrate: Any,
        contract_address: str,
        metadata_path: Path,
        state_store: ValidatorStateStore,
    ):
        self.substrate = substrate
        self.contract_address = contract_address
        self.state_store = state_store
        self.registry = load_event_registry(metadata_path)
        self.cursor: int = 0

        self.active_miners: Set[str] = set()
        self.open_swap_count: Dict[str, int] = {}
        self.busy_events: List[BusyEvent] = []
        self.active_events: List[ActiveEvent] = []
        self.active_events_by_hotkey: Dict[str, List[ActiveEvent]] = {}

    # ─── Public API consumed by scoring ─────────────────────────────────

    def get_busy_events_in_range(self, start_block: int, end_block: int) -> List[dict]:
        out: List[dict] = []
        for ev in self.busy_events:
            if ev.block <= start_block:
                continue
            if ev.block > end_block:
                break
            out.append({'hotkey': ev.hotkey, 'delta': ev.delta, 'block': ev.block})
        return out

    def get_busy_miners_at(self, block: int) -> Dict[str, int]:
        """Per-hotkey open-swap count at ``block``, reconstructed by replaying
        every delta at or before ``block``."""
        counts: Dict[str, int] = {}
        for ev in self.busy_events:
            if ev.block > block:
                break
            counts[ev.hotkey] = counts.get(ev.hotkey, 0) + ev.delta
        return {hk: c for hk, c in counts.items() if c > 0}

    def get_active_events_in_range(self, start_block: int, end_block: int) -> List[dict]:
        """Active-flag transitions in ``(start_block, end_block]``, oldest first."""
        out: List[dict] = []
        for ev in self.active_events:
            if ev.block <= start_block:
                continue
            if ev.block > end_block:
                break
            out.append({'hotkey': ev.hotkey, 'active': ev.active, 'block': ev.block})
        return out

    def get_active_miners_at(self, block: int) -> Set[str]:
        """Active set at ``block``, reconstructed by replaying every active
        transition at or before ``block``. The bootstrap seeds an event at
        ``cursor`` for each hotkey the contract reports as active at cold
        start, so pre-cursor state is anchored the same way the collateral
        snapshot is."""
        latest: Dict[str, bool] = {}
        for ev in self.active_events:
            if ev.block > block:
                break
            latest[ev.hotkey] = ev.active
        return {hk for hk, is_active in latest.items() if is_active}

    # ─── Sync loop ──────────────────────────────────────────────────────

    def initialize(
        self,
        current_block: int,
        metagraph_hotkeys: Optional[List[str]] = None,
        contract_client: Any = None,
    ) -> None:
        """Cold start: snapshot contract state for every metagraph miner, then
        rewind the cursor by one scoring window so ``sync_to`` backfills it
        before the first scoring pass runs."""
        if metagraph_hotkeys and contract_client is not None:
            for hotkey in metagraph_hotkeys:
                try:
                    if contract_client.get_miner_active_flag(hotkey):
                        self.active_miners.add(hotkey)
                except Exception as e:
                    bt.logging.debug(f'EventWatcher bootstrap: active flag read failed for {hotkey[:8]}: {e}')
            # Without this seed, a miner already serving a swap at startup
            # would be treated as idle until the next terminal event.
            try:
                in_flight = contract_client.get_active_swaps() or []
                seen_hotkeys = set()
                for swap in in_flight:
                    hk = getattr(swap, 'miner_hotkey', '')
                    init_block = getattr(swap, 'initiated_block', current_block)
                    if not hk:
                        continue
                    seen_hotkeys.add(hk)
                    self.open_swap_count[hk] = self.open_swap_count.get(hk, 0) + 1
                    self.busy_events.append(BusyEvent(hotkey=hk, delta=+1, block=init_block))
                if seen_hotkeys:
                    self.busy_events.sort(key=lambda ev: ev.block)
                    bt.logging.info(f'EventWatcher bootstrap: seeded {len(seen_hotkeys)} miners as busy from contract')
            except Exception as e:
                bt.logging.debug(f'EventWatcher bootstrap: active swaps read failed: {e}')
            bt.logging.info(f'EventWatcher initialized: {len(self.active_miners)} active miners')
        self.cursor = max(0, current_block - SCORING_WINDOW_BLOCKS)
        # Anchor the historical active set at the cursor so scoring sees the
        # bootstrap state at window_start. Subsequent MinerActivated events
        # replayed during sync_to apply on top.
        for hotkey in list(self.active_miners):
            event = ActiveEvent(hotkey=hotkey, active=True, block=self.cursor)
            self.active_events.append(event)
            self.active_events_by_hotkey.setdefault(hotkey, []).append(event)

    def sync_to(self, current_block: int) -> None:
        """Catch up from cursor to ``current_block`` in MAX_BLOCKS_PER_SYNC
        chunks so a long outage doesn't freeze the forward loop."""
        if current_block <= self.cursor:
            return
        end = min(current_block, self.cursor + MAX_BLOCKS_PER_SYNC)
        for block_num in range(self.cursor + 1, end + 1):
            self.process_block(block_num)
        self.cursor = end
        self.prune_old_events(current_block)

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
        except Exception as e:
            bt.logging.warning(f'EventWatcher: failed to parse event bytes: {e}')
            return None

        if not topics:
            bt.logging.warning('EventWatcher: contract event with no topics — decoder cannot identify it')
            return None
        sig_topic = '0x' + topics[0].hex()
        event_def = self.registry.get(sig_topic)
        if event_def is None:
            # Unknown event topic — likely metadata drift. Warn once per topic
            # so a stale metadata.json doesn't silently drop every event.
            bt.logging.warning(f'EventWatcher: unknown event topic {sig_topic[:18]}... — metadata may be stale')
            return None

        try:
            values = decode_topic_fields(event_def, topics)
            values.update(decode_data_fields(event_def, raw_data))
        except Exception as e:
            bt.logging.warning(f'EventWatcher: failed to decode {event_def.name}: {e}')
            return None
        return event_def.name, values

    def apply_event(self, block_num: int, name: str, values: Dict[str, Any]) -> None:
        if name == 'MinerActivated':
            hotkey = values.get('miner', '')
            if not hotkey:
                return
            active = bool(values.get('active'))
            self.record_active_transition(block_num, hotkey, active)
        elif name == 'SwapInitiated':
            miner = values.get('miner', '')
            if miner:
                self.apply_busy_delta(block_num, miner, +1)
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
                self.apply_busy_delta(block_num, miner, -1)
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
                self.apply_busy_delta(block_num, miner, -1)

    def record_active_transition(self, block_num: int, hotkey: str, active: bool) -> None:
        """Apply an on-chain active-flag transition to both the current-state
        snapshot and the historical event log. A no-op if the flag already
        matches — duplicate MinerActivated emissions don't pollute the log."""
        if not hotkey:
            return
        currently_active = hotkey in self.active_miners
        if currently_active == active:
            return
        if active:
            self.active_miners.add(hotkey)
        else:
            self.active_miners.discard(hotkey)
        event = ActiveEvent(hotkey=hotkey, active=active, block=block_num)
        self.active_events.append(event)
        self.active_events_by_hotkey.setdefault(hotkey, []).append(event)

    def apply_busy_delta(self, block_num: int, hotkey: str, delta: int) -> None:
        """Apply a ±1 transition. Drops any -1 with no matching prior +1
        rather than letting the open-swap count go negative."""
        if delta == 0:
            return
        current = self.open_swap_count.get(hotkey, 0)
        new_count = current + delta
        if new_count < 0:
            return
        self.open_swap_count[hotkey] = new_count
        self.busy_events.append(BusyEvent(hotkey=hotkey, delta=delta, block=block_num))

    def prune_old_events(self, current_block: int) -> None:
        """Drop busy and active events older than one scoring window. Latest
        active event per hotkey is preserved as a state-reconstruction anchor;
        busy events are kept while the open-swap count is still > 0 so the
        matching -1 isn't orphaned."""
        cutoff = current_block - SCORING_WINDOW_BLOCKS
        if cutoff <= 0:
            return
        if self.busy_events:
            open_now = {hk for hk, c in self.open_swap_count.items() if c > 0}
            self.busy_events = [ev for ev in self.busy_events if ev.block >= cutoff or ev.hotkey in open_now]
        if self.active_events:
            latest_per_hotkey: Dict[str, ActiveEvent] = {}
            for ev in self.active_events:
                latest_per_hotkey[ev.hotkey] = ev
            self.active_events = [
                ev for ev in self.active_events if ev.block >= cutoff or latest_per_hotkey.get(ev.hotkey) is ev
            ]
            for hotkey, events in list(self.active_events_by_hotkey.items()):
                latest = events[-1] if events else None
                pruned = [ev for ev in events if ev.block >= cutoff or ev is latest]
                if pruned:
                    self.active_events_by_hotkey[hotkey] = pruned
                else:
                    del self.active_events_by_hotkey[hotkey]
