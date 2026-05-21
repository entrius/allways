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

from allways.classes import SwapStatus
from allways.commitments import read_miner_commitment
from allways.constants import SCORING_WINDOW_BLOCKS
from allways.utils.logging import miner_label as _miner_label
from allways.utils.scale import (
    decode_account_id,
    decode_bool,
    decode_string,
    decode_u32,
    decode_u64,
    decode_u128,
    strip_hex_prefix,
)
from allways.validator.state_store import ReservationPin, ValidatorStateStore
from allways.validator.swap_tracker import SwapTracker

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
EVENT_PRUNE_INTERVAL_BLOCKS = 100  # O(events) sweep; window much wider than per-step delta.


class ContractEventWatcher:
    """Replays contract events into in-memory miner state."""

    def __init__(
        self,
        substrate: Any,
        contract_address: str,
        metadata_path: Path,
        state_store: ValidatorStateStore,
        swap_tracker: Optional[SwapTracker] = None,
        metagraph: Optional[Any] = None,
        *,
        netuid: Optional[int] = None,
        subtensor: Any = None,
    ):
        self.substrate = substrate
        self.contract_address = contract_address
        self.state_store = state_store
        # netuid + subtensor are needed to pin a miner's commitment at the
        # reservation block — ``read_miner_commitment`` calls
        # ``subtensor.determine_block_hash``, which the bare ``substrate``
        # lacks. When absent (e.g. the unit-test helper) the MinerReserved
        # handler no-ops, so the watcher still works without them.
        self.netuid = netuid
        self.subtensor = subtensor
        # Late-bindable so the validator can construct the tracker after the
        # watcher (cycle in current init order). Timeout extension finalize
        # writes are skipped until this is set.
        self.swap_tracker = swap_tracker
        # Optional — used purely for UID-resolved log labels.
        self.metagraph = metagraph
        self.registry = load_event_registry(metadata_path)
        self.cursor: int = 0
        self.last_prune_block: int = 0

        self.active_miners: Set[str] = set()
        self.open_swap_count: Dict[str, int] = {}
        self.busy_events: List[BusyEvent] = []
        self.active_events: List[ActiveEvent] = []
        self.active_events_by_hotkey: Dict[str, List[ActiveEvent]] = {}
        # Swap IDs whose +1 was seeded directly from the contract's active-swap
        # list during initialize(). Replay must skip their SwapInitiated event
        # to avoid double-counting — the busy tick is already in open_swap_count.
        # Entries are discarded on the matching terminal event. Persisted to
        # bootstrapped_swaps so a warm restart preserves the skip-list.
        self.bootstrapped_swap_ids: Set[int] = set()
        # Pruned-block counters: each sync_to resets these and emits a single
        # summary INFO line at the end if any blocks were skipped because their
        # state had been pruned by the RPC node (public finney nodes keep only
        # ~240 blocks of state, but the watcher replays a full scoring window).
        self.pruned_block_count: int = 0
        self.pruned_block_first: Optional[int] = None
        self.pruned_block_last: Optional[int] = None

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
        """Branch on persisted cursor: warm restart hydrates from state.db so
        sync_to picks up where the last process left off (no contract reads,
        no replay of pre-cursor history). A fresh DB or a cursor more than
        one scoring window behind falls back to cold bootstrap."""
        persisted_cursor = self.state_store.get_event_cursor()
        if persisted_cursor is None:
            self.cold_bootstrap(current_block, metagraph_hotkeys, contract_client)
            return
        gap = current_block - persisted_cursor
        if gap > SCORING_WINDOW_BLOCKS:
            bt.logging.warning(
                f'EventWatcher: persisted cursor {persisted_cursor} is {gap} blocks behind '
                f'current {current_block} (> SCORING_WINDOW_BLOCKS={SCORING_WINDOW_BLOCKS}). '
                'Resetting persistence and falling back to cold bootstrap.'
            )
            self.state_store.reset_event_watcher_state()
            self.cold_bootstrap(current_block, metagraph_hotkeys, contract_client)
            return
        self.hydrate_from_db()

    def cold_bootstrap(
        self,
        current_block: int,
        metagraph_hotkeys: Optional[List[str]] = None,
        contract_client: Any = None,
    ) -> None:
        """Snapshot contract state for every metagraph miner, persist the
        anchors, then rewind the cursor by one scoring window so ``sync_to``
        backfills it before the first scoring pass runs."""
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
                    swap_id = getattr(swap, 'id', None)
                    if isinstance(swap_id, int):
                        self.bootstrapped_swap_ids.add(swap_id)
                        self.state_store.add_bootstrapped_swap(swap_id)
                    seen_hotkeys.add(hk)
                    self.open_swap_count[hk] = self.open_swap_count.get(hk, 0) + 1
                    self.busy_events.append(BusyEvent(hotkey=hk, delta=+1, block=init_block))
                    self.state_store.insert_busy_event(
                        init_block, hk, +1, swap_id if isinstance(swap_id, int) else None
                    )
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
            self.state_store.insert_active_event(self.cursor, hotkey, True)
        self.state_store.set_event_cursor(self.cursor)

    def hydrate_from_db(self) -> None:
        """Rebuild every in-memory mirror from state.db. Called on warm restart
        when the persisted cursor is within one scoring window of head — the
        contract is bypassed entirely; DB is treated as source of truth."""
        self.cursor = self.state_store.get_event_cursor() or 0
        self.bootstrapped_swap_ids = self.state_store.load_bootstrapped_swaps()

        active_rows = self.state_store.load_all_active_events()
        self.active_events = [
            ActiveEvent(hotkey=r['hotkey'], active=bool(r['active']), block=r['block_num']) for r in active_rows
        ]
        self.active_events_by_hotkey = {}
        latest_active: Dict[str, bool] = {}
        for ev in self.active_events:
            self.active_events_by_hotkey.setdefault(ev.hotkey, []).append(ev)
            latest_active[ev.hotkey] = ev.active
        self.active_miners = {hk for hk, is_active in latest_active.items() if is_active}

        busy_rows = self.state_store.load_all_busy_events()
        self.busy_events = [BusyEvent(hotkey=r['hotkey'], delta=r['delta'], block=r['block_num']) for r in busy_rows]
        counts: Dict[str, int] = {}
        for ev in self.busy_events:
            counts[ev.hotkey] = counts.get(ev.hotkey, 0) + ev.delta
        self.open_swap_count = {hk: c for hk, c in counts.items() if c > 0}

        bt.logging.info(
            f'EventWatcher hydrated from DB: cursor={self.cursor}, '
            f'{len(self.active_miners)} active, {sum(self.open_swap_count.values())} open swaps'
        )

    def sync_to(self, current_block: int) -> None:
        """Catch up from cursor to ``current_block`` in MAX_BLOCKS_PER_SYNC
        chunks so a long outage doesn't freeze the forward loop."""
        if current_block <= self.cursor:
            return
        self.pruned_block_count = 0
        self.pruned_block_first = None
        self.pruned_block_last = None
        end = min(current_block, self.cursor + MAX_BLOCKS_PER_SYNC)
        for block_num in range(self.cursor + 1, end + 1):
            self.process_block(block_num)
        if current_block - self.last_prune_block >= EVENT_PRUNE_INTERVAL_BLOCKS:
            self.prune_old_events(current_block)
            self.last_prune_block = current_block
        if self.pruned_block_count > 0:
            bt.logging.info(
                f'EventWatcher: {self.pruned_block_count} pruned blocks skipped '
                f'(blocks {self.pruned_block_first}..{self.pruned_block_last}) — '
                'RPC node retains only recent state'
            )

    def process_block(self, block_num: int) -> None:
        try:
            block_hash = self.substrate.get_block_hash(block_num)
            if not block_hash:
                return
            events = self.substrate.get_events(block_hash=block_hash)
        except Exception as e:
            msg = str(e).lower()
            # Public finney nodes drop state past ~240 blocks; replaying a full
            # scoring window after restart hits this on every old block.
            # Substrate-interface raises this as "State already discarded" or
            # "state pruned" — match liberally on either word. The block is
            # permanently unavailable, so advance the cursor past it: otherwise
            # a cold start (cursor = head − 600) would loop on the first pruned
            # block forever and never reach the live region. Counters collapse
            # the noise into one summary line per sync_to.
            if ('state' in msg and 'discarded' in msg) or 'pruned' in msg:
                self.pruned_block_count += 1
                if self.pruned_block_first is None:
                    self.pruned_block_first = block_num
                self.pruned_block_last = block_num
                self.cursor = block_num
                self.state_store.set_event_cursor(block_num)
            else:
                # Transient (RPC hiccup, node lag): leave the cursor so the
                # block is retried on the next sync_to.
                bt.logging.debug(f'EventWatcher: block {block_num} events unavailable: {e}')
            return

        for event_record in events:
            decoded = self.decode_contract_event(event_record)
            if decoded is None:
                continue
            name, values = decoded
            try:
                self.apply_event(block_num, name, values)
            except Exception as e:
                # Don't propagate — sync_to wouldn't advance the cursor,
                # which would re-replay every successful apply_event in the
                # same block on the next pass and double-apply busy deltas.
                bt.logging.warning(f'EventWatcher: apply_event {name}@{block_num} failed: {e}')
        self.cursor = block_num
        self.state_store.set_event_cursor(block_num)

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
            state_changed = (hotkey in self.active_miners) != active
            self.record_active_transition(block_num, hotkey, active)
            if state_changed:
                bt.logging.info(
                    f'EventWatcher: {self._label(hotkey)} MinerActivated(active={active}) @ block {block_num}'
                )
        elif name == 'MinerReserved':
            miner = values.get('miner', '')
            reserved_until = values.get('reserved_until')
            if miner and isinstance(reserved_until, int):
                self.record_reservation_pin(block_num, miner, reserved_until)
        elif name == 'SwapInitiated':
            swap_id = values.get('swap_id')
            miner = values.get('miner', '')
            if miner:
                # The reservation has now become a swap — its commitment
                # snapshot is captured in the on-chain swap struct, so the
                # local pin is no longer needed.
                self.state_store.remove_reservation_pin(miner)
                # Skip if this swap's +1 was already seeded from the contract's
                # live active-swap list at bootstrap — otherwise a restart
                # whose replay window covers the original SwapInitiated would
                # double-count the miner as busy.
                if isinstance(swap_id, int) and swap_id in self.bootstrapped_swap_ids:
                    return
                self.apply_busy_delta(block_num, miner, +1, swap_id if isinstance(swap_id, int) else None)
                bt.logging.info(f'EventWatcher: {self._label(miner)} SwapInitiated swap=#{swap_id} @ block {block_num}')
        elif name == 'SwapCompleted':
            swap_id = values.get('swap_id')
            miner = values.get('miner', '')
            if isinstance(swap_id, int) and miner:
                tao = int(values.get('tao_amount') or 0)
                from_chain, to_chain = self._lookup_swap_direction(swap_id)
                self.state_store.insert_swap_outcome(
                    swap_id=swap_id,
                    miner_hotkey=miner,
                    completed=True,
                    resolved_block=block_num,
                    tao_amount=tao,
                    from_chain=from_chain,
                    to_chain=to_chain,
                )
                self.apply_busy_delta(block_num, miner, -1, swap_id)
                self.bootstrapped_swap_ids.discard(swap_id)
                self.state_store.remove_bootstrapped_swap(swap_id)
                if self.swap_tracker is not None:
                    self.swap_tracker.resolve(swap_id, SwapStatus.COMPLETED, block_num)
                bt.logging.info(
                    f'EventWatcher: {self._label(miner)} SwapCompleted swap=#{swap_id} tao={tao} @ block {block_num}'
                )
        elif name == 'SwapTimedOut':
            swap_id = values.get('swap_id')
            miner = values.get('miner', '')
            if isinstance(swap_id, int) and miner:
                from_chain, to_chain = self._lookup_swap_direction(swap_id)
                self.state_store.insert_swap_outcome(
                    swap_id=swap_id,
                    miner_hotkey=miner,
                    completed=False,
                    resolved_block=block_num,
                    from_chain=from_chain,
                    to_chain=to_chain,
                )
                # Defensive: a SwapInitiated this validator missed would leave
                # a stale pin behind — clear it on the terminal event too.
                self.state_store.remove_reservation_pin(miner)
                self.apply_busy_delta(block_num, miner, -1, swap_id)
                self.bootstrapped_swap_ids.discard(swap_id)
                self.state_store.remove_bootstrapped_swap(swap_id)
                if self.swap_tracker is not None:
                    self.swap_tracker.resolve(swap_id, SwapStatus.TIMED_OUT, block_num)
                bt.logging.warning(
                    f'EventWatcher: {self._label(miner)} SwapTimedOut swap=#{swap_id} @ block {block_num} (slash)'
                )
        elif name == 'ReservationExtensionFinalized':
            # Event-driven cache update for the local pending_confirms row —
            # replaces the polling refresh that the legacy vote-extend flow
            # needed (commit 1b942e8). Without this write the upstream
            # purge_expired sweep would delete a still-live entry at its
            # stale reserved_until.
            miner = values.get('miner', '')
            applied_target = values.get('applied_target')
            if miner and isinstance(applied_target, int):
                self.state_store.update_reserved_until(miner, applied_target)
                bt.logging.info(
                    f'EventWatcher: {self._label(miner)} ReservationExtensionFinalized '
                    f'applied_target={applied_target} @ block {block_num}'
                )
                # Keep the pin's TTL in step so purge_expired_reservation_pins
                # doesn't drop a still-live pin at its stale deadline.
                self.state_store.update_reservation_pin_reserved_until(miner, applied_target)
        elif name == 'TimeoutExtensionFinalized':
            swap_id = values.get('swap_id')
            applied_target = values.get('applied_target')
            if self.swap_tracker is not None and isinstance(swap_id, int) and isinstance(applied_target, int):
                self.swap_tracker.update_timeout_block(swap_id, applied_target)
                bt.logging.info(
                    f'EventWatcher: TimeoutExtensionFinalized swap=#{swap_id} '
                    f'applied_target={applied_target} @ block {block_num}'
                )

    def _label(self, hotkey: str) -> str:
        return _miner_label(self.metagraph, hotkey)

    def _lookup_swap_direction(self, swap_id: int) -> Tuple[str, str]:
        """Resolve (from_chain, to_chain) for a swap that's just terminated.

        SwapCompleted/SwapTimedOut events carry no direction. The tracker still
        holds the Swap (resolve() runs after we record the outcome), so it's
        the authoritative source. Returns ('', '') when the tracker is unset
        or doesn't know the swap — e.g. a swap that completed/timed out before
        the validator caught up. Empty direction means the outcome won't
        contribute to per-direction volume sums, which is the safe default."""
        if self.swap_tracker is None:
            return '', ''
        swap = self.swap_tracker.active.get(swap_id)
        if swap is None:
            return '', ''
        return (swap.from_chain or '').lower(), (swap.to_chain or '').lower()

    def record_reservation_pin(self, block_num: int, miner: str, reserved_until: int) -> None:
        """Pin the miner's commitment as of the reservation block ``block_num``.

        ``handle_swap_confirm`` later resolves the swap's rate + addresses from
        this pin instead of the miner's live commitment, closing the window in
        which a miner could move its rate or deposit address after the user
        reserved. The read is at the canonical block ``block_num`` so every
        validator derives a byte-identical pin.

        On any failure — a transient RPC error, a pruned block during backfill,
        or a missing commitment — no pin is written: a validator with no pin
        falls back to the live commitment in ``handle_swap_confirm``, but a
        validator must never persist a *wrong* pin.
        """
        if self.subtensor is None or self.netuid is None:
            bt.logging.debug(
                f'EventWatcher: MinerReserved for {miner[:8]} at block {block_num} — '
                'no subtensor/netuid wired, skipping reservation pin'
            )
            return
        try:
            commitment = read_miner_commitment(
                subtensor=self.subtensor,
                netuid=self.netuid,
                hotkey=miner,
                block=block_num,
            )
        except Exception as e:
            bt.logging.warning(
                f'EventWatcher: reservation pin commitment read failed for '
                f'{miner[:8]} at block {block_num}: {e} — no pin written, will fall back'
            )
            return
        if commitment is None:
            bt.logging.warning(
                f'EventWatcher: no commitment for {miner[:8]} at reservation block '
                f'{block_num} — no pin written, will fall back'
            )
            return
        self.state_store.upsert_reservation_pin(
            ReservationPin(
                miner_hotkey=miner,
                reserve_block=block_num,
                from_chain=commitment.from_chain,
                to_chain=commitment.to_chain,
                rate_str=commitment.rate_str,
                counter_rate_str=commitment.counter_rate_str,
                miner_from_address=commitment.from_address,
                miner_to_address=commitment.to_address,
                reserved_until=reserved_until,
            )
        )
        bt.logging.info(
            f'EventWatcher: pinned reservation for {miner[:8]} at block {block_num} '
            f'({commitment.from_chain}->{commitment.to_chain})'
        )

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
        self.state_store.insert_active_event(block_num, hotkey, active)

    def apply_busy_delta(self, block_num: int, hotkey: str, delta: int, swap_id: Optional[int] = None) -> None:
        """Apply a ±1 transition. Drops any -1 with no matching prior +1
        rather than letting the open-swap count go negative. ``swap_id`` is
        persisted on the row for traceability and pairing of +1/-1's."""
        if delta == 0:
            return
        current = self.open_swap_count.get(hotkey, 0)
        new_count = current + delta
        if new_count < 0:
            bt.logging.warning(
                f'EventWatcher: dropping busy delta {delta} for {hotkey[:8]} at block {block_num} '
                f'(current count={current}, would go negative — missed SwapInitiated?)'
            )
            return
        self.open_swap_count[hotkey] = new_count
        self.busy_events.append(BusyEvent(hotkey=hotkey, delta=delta, block=block_num))
        self.state_store.insert_busy_event(block_num, hotkey, delta, swap_id)

    def prune_old_events(self, current_block: int) -> None:
        """Drop busy and active events older than one scoring window. Latest
        active event per hotkey is preserved as a state-reconstruction anchor;
        busy events are kept while the open-swap count is still > 0 so the
        matching -1 isn't orphaned. Mirrors the prune onto the SQL tables so
        warm restarts see the same anchor invariants."""
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
        self.state_store.prune_active_events(cutoff)
        self.state_store.prune_busy_events(cutoff)
