"""Incremental swap lifecycle tracker. Maintains the in-memory active set
so the forward loop knows what to verify, vote on, and time out. Swap
outcomes (credibility ledger writes) are owned by ``ContractEventWatcher``."""

import asyncio
from typing import Dict, List, Set

import bittensor as bt

from allways.classes import Swap, SwapStatus
from allways.constants import EXTEND_THRESHOLD_BLOCKS
from allways.contract_client import AllwaysContractClient

ACTIVE_STATUSES = (SwapStatus.ACTIVE, SwapStatus.FULFILLED)

# Re-fetch the last N swap IDs each poll regardless of last_scanned_id so a
# silent get_swap None during discovery self-heals next poll. Mirrors the
# miner's RESCAN_WINDOW (#264). Bounded by subnet-wide swap creation rate
# per forward step, not by the validator's tracked-set size.
RESCAN_WINDOW = 16


def _swap_label(swap: Swap) -> str:
    return f'{swap.from_chain.upper()}->{swap.to_chain.upper()}'


class SwapTracker:
    """Discovery scans new swap IDs since the last poll; monitoring re-fetches
    all tracked ACTIVE/FULFILLED swaps each poll."""

    def __init__(self, client: AllwaysContractClient):
        self.client = client
        self.last_scanned_id = 0
        self.active: Dict[int, Swap] = {}
        self.voted_ids: Set[int] = set()

    def initialize(self):
        """Cold start: seed the active set from the contract's live swaps.

        A full scan (``max_gap=None``) is required: an ACTIVE or FULFILLED
        swap can sit behind any run of pruned neighbors, and a bounded gap
        heuristic would silently drop it across restart.
        """
        # Snapshot the cursor before scanning so a swap created mid-init is
        # picked up by the next poll rather than silently skipped.
        next_id = self.client.get_next_swap_id()
        for swap in self.client.get_active_swaps(max_gap=None):
            self.active[swap.id] = swap
        self.last_scanned_id = next_id - 1
        bt.logging.info(f'SwapTracker initialized: active={len(self.active)}, last_scanned_id={self.last_scanned_id}')

    def resolve(self, swap_id: int, status: SwapStatus, block: int):
        """Drop a swap from tracking after our vote reached quorum or after
        the watcher observed a SwapCompleted/SwapTimedOut event. Idempotent
        — no-op when the swap isn't tracked."""
        swap = self.active.pop(swap_id, None)
        if swap is None:
            return
        swap.status = status
        swap.completed_block = block
        self.voted_ids.discard(swap_id)
        bt.logging.info(f'Swap {swap_id}: dropped from active ({status.name} at block {block})')

    def mark_voted(self, swap_id: int):
        """Mark a swap as voted on to prevent redundant confirm/timeout extrinsics."""
        self.voted_ids.add(swap_id)

    def is_voted(self, swap_id: int) -> bool:
        return swap_id in self.voted_ids

    def update_timeout_block(self, swap_id: int, timeout_block: int) -> None:
        """Apply an externally-observed timeout bump (event-driven from
        ``TimeoutExtensionFinalized``). Skips swaps not currently tracked
        and never moves the deadline backwards — the next ``poll`` will
        pick up the contract value either way."""
        swap = self.active.get(swap_id)
        if swap is None:
            return
        if timeout_block > swap.timeout_block:
            swap.timeout_block = timeout_block

    async def poll(self):
        """Incremental refresh — called every forward step."""
        try:
            await self.poll_inner()
        except (ConnectionError, TimeoutError, asyncio.TimeoutError) as e:
            bt.logging.warning(f'SwapTracker poll transient error: {e}')
        except Exception as e:
            bt.logging.error(f'SwapTracker poll error: {e}')
            raise

    async def poll_inner(self):
        next_id = await asyncio.to_thread(self.client.get_next_swap_id)

        # --- Discovery phase: scan new swap IDs ---
        # Sequential — substrate WS isn't thread-safe (see contract_client mutex);
        # parallel fanout used to surface as silent get_swap Nones. RESCAN_WINDOW
        # re-checks recent IDs so a transient skip self-heals next poll.
        fresh: Set[int] = set()
        start_id = max(1, min(self.last_scanned_id + 1, next_id - RESCAN_WINDOW))
        for sid in range(start_id, next_id):
            try:
                swap = await asyncio.to_thread(self.client.get_swap, sid)
            except Exception as e:
                bt.logging.debug(f'SwapTracker discovery({sid}) failed, will retry: {e}')
                continue
            if swap is None:
                continue
            if swap.status in ACTIVE_STATUSES:
                if swap.id not in self.active:
                    bt.logging.info(f'Swap {swap.id} [{_swap_label(swap)}]: now {swap.status.name}, monitoring')
                self.active[swap.id] = swap
                fresh.add(swap.id)

        if next_id > 1:
            self.last_scanned_id = next_id - 1

        # --- Monitoring phase: refresh active set ---
        stale_ids = [sid for sid in self.active if sid not in fresh]
        if not stale_ids:
            self.prune_stale_voted_ids()
            return

        # Refresh: update active swaps, drop on terminal status. A None or
        # transient RPC error leaves the swap in active — resolution is owned
        # by the event watcher (SwapCompleted/SwapTimedOut → tracker.resolve).
        for sid in stale_ids:
            try:
                result = await asyncio.to_thread(self.client.get_swap, sid)
            except Exception as e:
                bt.logging.debug(f'SwapTracker refresh({sid}) failed, will retry: {e}')
                continue
            if result is None:
                continue
            if result.status in ACTIVE_STATUSES:
                prev = self.active.get(sid)
                if prev is not None and prev.status != result.status:
                    bt.logging.info(f'Swap {sid} [{_swap_label(result)}]: {prev.status.name} -> {result.status.name}')
                self.active[sid] = result
            else:
                # Terminal status from chain — drop with reason for observability.
                self.resolve(sid, result.status, result.completed_block or 0)

        self.prune_stale_voted_ids()

    def prune_stale_voted_ids(self) -> None:
        """Drop any voted state for swaps no longer being tracked. Normally
        handled inline in ``resolve``/refresh, but an exceptional path (e.g.
        active.pop raced by a fixture) can leave orphans."""
        active_ids = set(self.active.keys())
        self.voted_ids -= self.voted_ids - active_ids

    def get_fulfilled(self, current_block: int) -> List[Swap]:
        """Active FULFILLED swaps not yet past timeout (ready for verification)."""
        return [
            s
            for s in self.active.values()
            if s.status == SwapStatus.FULFILLED and (s.timeout_block == 0 or current_block <= s.timeout_block)
        ]

    def get_near_timeout_fulfilled(self, current_block: int) -> List[Swap]:
        """FULFILLED swaps within EXTEND_THRESHOLD_BLOCKS of their timeout."""
        return [
            s
            for s in self.active.values()
            if s.status == SwapStatus.FULFILLED
            and s.timeout_block > 0
            and current_block >= s.timeout_block - EXTEND_THRESHOLD_BLOCKS
        ]

    def get_timed_out(self, current_block: int) -> List[Swap]:
        """Active ACTIVE or FULFILLED swaps past their timeout_block."""
        return [
            s
            for s in self.active.values()
            if s.status in (SwapStatus.ACTIVE, SwapStatus.FULFILLED)
            and s.timeout_block > 0
            and current_block > s.timeout_block
        ]
