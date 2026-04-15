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
  replay, bounded to one ``SCORING_WINDOW_BLOCKS`` (plus one anchor row per
  hotkey so state reconstruction at window_start always has something)
- ``busy_events`` — per-hotkey open-swap count transitions, same retention
- swap outcomes are forwarded into ``ValidatorStateStore.insert_swap_outcome``
  so the credibility ledger survives restarts

Cold start: ``initialize`` snapshots current contract state for all metagraph
miners, seeds busy state from in-flight swaps, then rewinds the cursor to
``head - SCORING_WINDOW_BLOCKS``. The existing bounded ``sync_to`` catches up
over ~24 forward steps and fills the scoring window long before the first
scoring pass runs. Swap_outcomes is the one ledger that must persist across
restarts and already lives in ``state.db``.

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


@dataclass
class BusyEvent:
    """A transition in a miner's open-swap count.

    ``delta`` is +1 on SwapInitiated and -1 on SwapCompleted/SwapTimedOut. The
    running sum of deltas per hotkey is the number of in-flight swaps; a miner
    is "busy" (excluded from crown) whenever that sum is > 0.
    """

    hotkey: str
    delta: int
    block: int


MAX_BLOCKS_PER_SYNC = 50


class ContractEventWatcher:
    """Replays contract events into in-memory miner state.

    Usage:
        watcher = ContractEventWatcher(substrate, contract_address, metadata_path, state_store)
        watcher.initialize(current_block, metagraph_hotkeys, contract_client)
        ... every forward step ...
        watcher.sync_to(current_block)

    Scoring reads ``get_collateral_events_in_range``, ``get_latest_collateral_before``,
    ``active_miners``, ``min_collateral`` directly off the watcher. Swap outcomes
    are forwarded into ``state_store.insert_swap_outcome`` so the credibility
    ledger persists across restarts.

    ``initialize`` snapshots current on-chain state for every metagraph miner,
    then advances the cursor to ``current_block``. From that point forward only
    events drive state changes. This avoids the trap where a miner who posted
    collateral before the replay window would look like they had zero.

    ``sync_to`` is bounded to ``MAX_BLOCKS_PER_SYNC`` per call so a long outage
    doesn't block the forward loop — the cursor catches up over multiple
    forward steps at ~50 blocks per tick.
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
        # Per-hotkey view of collateral_events for O(log n) latest-before
        # lookups during scoring replay. Kept in sync with the flat list.
        self.collateral_events_by_hotkey: Dict[str, List[CollateralEvent]] = {}

        # Per-miner open-swap count and transition history. Count > 0 means
        # the miner is currently handling a swap and should be excluded from
        # crown-time credit — idle runners-up earn that interval instead.
        self.open_swap_count: Dict[str, int] = {}
        self.busy_events: List[BusyEvent] = []

    # ─── Public API consumed by scoring ─────────────────────────────────

    def get_latest_collateral_before(self, hotkey: str, block: int) -> Optional[Tuple[int, int]]:
        """Most recent collateral event for ``hotkey`` at or before ``block``.

        O(log n) via binary search on the per-hotkey event list. If no events
        exist for the hotkey at all (bootstrap-only miners), returns the
        static snapshot at block 0 — that's the authoritative pre-event
        state. If events exist but none fall at/before ``block``, returns
        None: the snapshot reflects state AFTER the existing events and is
        not valid for queries in the pre-event gap.
        """
        from bisect import bisect_right

        events = self.collateral_events_by_hotkey.get(hotkey)
        if not events:
            snapshot = self.collateral.get(hotkey)
            return (snapshot, 0) if snapshot is not None else None
        idx = bisect_right([e.block for e in events], block) - 1
        if idx < 0:
            return None
        ev = events[idx]
        return ev.collateral_rao, ev.block

    def get_collateral_events_in_range(self, start_block: int, end_block: int) -> List[dict]:
        out: List[dict] = []
        for ev in self.collateral_events:
            if ev.block <= start_block:
                continue
            if ev.block > end_block:
                break
            out.append({'hotkey': ev.hotkey, 'collateral_rao': ev.collateral_rao, 'block': ev.block})
        return out

    def get_busy_events_in_range(self, start_block: int, end_block: int) -> List[dict]:
        """Ordered busy transitions in ``(start_block, end_block]``."""
        out: List[dict] = []
        for ev in self.busy_events:
            if ev.block <= start_block:
                continue
            if ev.block > end_block:
                break
            out.append({'hotkey': ev.hotkey, 'delta': ev.delta, 'block': ev.block})
        return out

    def get_busy_miners_at(self, block: int) -> Dict[str, int]:
        """Reconstruct the per-hotkey open-swap count at ``block``.

        Walks every busy event at or before ``block`` and applies its delta.
        Used by the crown-time replay to seed ``currently_busy`` state at the
        window start.
        """
        counts: Dict[str, int] = {}
        for ev in self.busy_events:
            if ev.block > block:
                break
            counts[ev.hotkey] = counts.get(ev.hotkey, 0) + ev.delta
        return {hk: c for hk, c in counts.items() if c > 0}

    # ─── Sync loop ──────────────────────────────────────────────────────

    def initialize(
        self,
        current_block: int,
        metagraph_hotkeys: Optional[List[str]] = None,
        contract_client: Any = None,
    ) -> None:
        """Cold start: snapshot contract state for every metagraph miner, then
        advance the cursor so only forward events drive state changes.

        Callers should pass ``metagraph_hotkeys`` and a read-capable
        ``contract_client``. If either is missing (e.g. in unit tests) the
        watcher falls back to an empty snapshot and starts at ``current_block``
        — scoring will simply not credit any miner until events arrive.
        """
        if metagraph_hotkeys and contract_client is not None:
            for hotkey in metagraph_hotkeys:
                try:
                    collateral = contract_client.get_miner_collateral(hotkey) or 0
                except Exception as e:
                    bt.logging.debug(f'EventWatcher bootstrap: collateral read failed for {hotkey[:8]}: {e}')
                    collateral = 0
                if collateral > 0:
                    self.collateral[hotkey] = collateral
                try:
                    if contract_client.get_miner_active_flag(hotkey):
                        self.active_miners.add(hotkey)
                except Exception as e:
                    bt.logging.debug(f'EventWatcher bootstrap: active flag read failed for {hotkey[:8]}: {e}')
            try:
                raw_min = contract_client.get_min_collateral() or 0
                if raw_min > 0:
                    self.min_collateral = raw_min
            except Exception as e:
                bt.logging.debug(f'EventWatcher bootstrap: min_collateral read failed: {e}')
            # Seed busy state from any in-flight swaps. Without this a miner
            # serving a swap at watcher-startup would be treated as idle until
            # the next terminal event flipped them free.
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
            bt.logging.info(
                f'EventWatcher initialized: {len(self.collateral)} collateral entries, '
                f'{len(self.active_miners)} active miners, min_collateral={self.min_collateral}'
            )
        # Rewind the cursor so ``sync_to`` backfills the full scoring window
        # on cold start. The existing bounded-chunk sync loop catches up over
        # ~24 forward steps (50 blocks/step × 1200 blocks), finishing long
        # before the first scoring pass at step SCORING_INTERVAL_STEPS.
        self.cursor = max(0, current_block - SCORING_WINDOW_BLOCKS)

    def sync_to(self, current_block: int) -> None:
        """Catch up from cursor to ``current_block`` in bounded chunks.

        At most ``MAX_BLOCKS_PER_SYNC`` blocks are processed per call so a
        multi-minute outage doesn't freeze the forward loop on one sync. The
        cursor advances incrementally across forward steps until it catches
        up to head.
        """
        if current_block <= self.cursor:
            return
        end = min(current_block, self.cursor + MAX_BLOCKS_PER_SYNC)
        for block_num in range(self.cursor + 1, end + 1):
            self.process_block(block_num)
        self.cursor = end
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
        if name == 'CollateralPosted':
            # `total` is the authoritative post-event balance — use it as a
            # SET so we don't drift when the replay window is missing prior
            # events. Fall back to an add if `total` isn't present.
            hotkey = values.get('miner', '')
            total = values.get('total')
            if total is not None:
                self.set_collateral(block_num, hotkey, int(total))
            else:
                self.adjust_collateral(block_num, hotkey, +int(values.get('amount', 0)))
        elif name == 'CollateralWithdrawn':
            hotkey = values.get('miner', '')
            remaining = values.get('remaining')
            if remaining is not None:
                self.set_collateral(block_num, hotkey, int(remaining))
            else:
                self.adjust_collateral(block_num, hotkey, -int(values.get('amount', 0)))
        elif name == 'CollateralSlashed':
            # Slashed has no `total` / `remaining` field — it only carries the
            # slash amount. Subtract from the current snapshot (which was
            # seeded at initialize() or updated by a prior Posted/Withdrawn).
            self.adjust_collateral(block_num, values.get('miner', ''), -int(values.get('amount', 0)))
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
        elif name == 'SwapInitiated':
            # Miner becomes busy — excluded from crown-time credit until a
            # terminal event (Completed/TimedOut) frees them.
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

    def apply_busy_delta(self, block_num: int, hotkey: str, delta: int) -> None:
        """Apply a ±1 transition to ``hotkey``'s open-swap count.

        Floors the count at zero: a terminal event we observe with no matching
        SwapInitiated in history (e.g. a swap that started before the watcher
        bootstrapped) is dropped rather than letting the count go negative.
        """
        if delta == 0:
            return
        current = self.open_swap_count.get(hotkey, 0)
        new_count = current + delta
        if new_count < 0:
            return
        self.open_swap_count[hotkey] = new_count
        self.busy_events.append(BusyEvent(hotkey=hotkey, delta=delta, block=block_num))

    def set_collateral(self, block_num: int, hotkey: str, new_total: int) -> None:
        """Record an authoritative post-event collateral balance for ``hotkey``."""
        if not hotkey:
            return
        new_total = max(0, new_total)
        self.collateral[hotkey] = new_total
        event = CollateralEvent(hotkey=hotkey, collateral_rao=new_total, block=block_num)
        self.collateral_events.append(event)
        self.collateral_events_by_hotkey.setdefault(hotkey, []).append(event)

    def adjust_collateral(self, block_num: int, hotkey: str, delta: int) -> None:
        """Add a delta to the current collateral snapshot and emit an event row."""
        if not hotkey:
            return
        new_total = max(0, self.collateral.get(hotkey, 0) + delta)
        self.set_collateral(block_num, hotkey, new_total)

    def prune_old_collateral_events(self, current_block: int) -> None:
        """Drop collateral and busy events older than one scoring window.

        The single latest collateral event per hotkey is always preserved
        (even if it's older than the cutoff), so crown-time replay can still
        reconstruct window-start state for miners who haven't posted or
        withdrawn inside the window. Busy events are only pruned when the
        hotkey's current open-swap count is zero — never drop a +1 whose
        matching -1 hasn't been observed yet.
        """
        cutoff = current_block - SCORING_WINDOW_BLOCKS
        if cutoff <= 0:
            return
        if self.collateral_events:
            latest_per_hotkey = {}
            for ev in self.collateral_events:
                latest_per_hotkey[ev.hotkey] = ev  # last write wins (events are append-order)
            self.collateral_events = [
                ev for ev in self.collateral_events if ev.block >= cutoff or latest_per_hotkey.get(ev.hotkey) is ev
            ]
            for hotkey, events in list(self.collateral_events_by_hotkey.items()):
                latest = events[-1] if events else None
                pruned = [ev for ev in events if ev.block >= cutoff or ev is latest]
                if pruned:
                    self.collateral_events_by_hotkey[hotkey] = pruned
                else:
                    del self.collateral_events_by_hotkey[hotkey]
        if self.busy_events:
            open_now = {hk for hk, c in self.open_swap_count.items() if c > 0}
            self.busy_events = [ev for ev in self.busy_events if ev.block >= cutoff or ev.hotkey in open_now]
