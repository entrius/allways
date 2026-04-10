"""Incremental swap lifecycle tracker. Eliminates O(N) full scans."""

import asyncio
from typing import Dict, List, Set

import bittensor as bt

from allways.classes import Swap, SwapStatus
from allways.contract_client import AllwaysContractClient

ACTIVE_STATUSES = (SwapStatus.ACTIVE, SwapStatus.FULFILLED)


class SwapTracker:
    """Tracks swap lifecycle incrementally. No full scans after initialization.

    Two layers:
    - Discovery: scan only NEW swap IDs since last poll
    - Monitoring: re-fetch all tracked ACTIVE/FULFILLED swaps each poll

    Resolved swaps are no longer stored on-chain, so cold start only recovers
    active swaps. The scoring window populates naturally as swaps complete.
    """

    def __init__(
        self,
        client: AllwaysContractClient,
        fulfillment_timeout_blocks: int,
        window_blocks: int,
    ):
        self.client = client
        self.last_scanned_id = 0
        self.active: Dict[int, Swap] = {}
        self.window: List[Swap] = []
        self.voted_ids: Set[int] = set()

        self.fulfillment_timeout_blocks = fulfillment_timeout_blocks
        self.window_blocks = window_blocks

    def initialize(self, current_block: int):
        """Cold start — scan backward from latest swap to populate active set."""
        next_id = self.client.get_next_swap_id()
        if next_id <= 1:
            self.last_scanned_id = 0
            bt.logging.info('SwapTracker initialized: no swaps exist')
            return

        cutoff_block = current_block - self.window_blocks - self.fulfillment_timeout_blocks
        latest_id = next_id - 1

        for swap_id in reversed(range(1, next_id)):
            swap = self.client.get_swap(swap_id)
            if swap is None:
                continue

            if swap.initiated_block < cutoff_block:
                bt.logging.debug(
                    f'SwapTracker init: stopping at swap {swap_id} '
                    f'(initiated_block={swap.initiated_block} < cutoff={cutoff_block})'
                )
                break

            if swap.status in ACTIVE_STATUSES:
                self.active[swap.id] = swap

        self.last_scanned_id = latest_id

        bt.logging.info(f'SwapTracker initialized: active={len(self.active)}, last_scanned_id={self.last_scanned_id}')

    def resolve(self, swap_id: int, status: SwapStatus, block: int):
        """Move a swap from active tracking to the scoring window with its terminal state.

        Called when the validator's vote reaches quorum (confirm or timeout).
        The contract removes swap data on resolution, so get_swap() returns None
        after this point — we must capture the terminal state here for scoring.
        """
        swap = self.active.pop(swap_id, None)
        if swap is None:
            return
        swap.status = status
        swap.completed_block = block
        self.window.append(swap)
        self.voted_ids.discard(swap_id)

    def mark_voted(self, swap_id: int):
        """Mark a swap as voted on to prevent redundant vote extrinsics."""
        self.voted_ids.add(swap_id)

    def is_voted(self, swap_id: int) -> bool:
        """Check if we've already voted on this swap."""
        return swap_id in self.voted_ids

    async def poll(self, current_block: int = 0):
        """Incremental update — called every forward step (~12s)."""
        self._current_block = current_block
        try:
            await self._poll_inner()
        except (ConnectionError, TimeoutError, asyncio.TimeoutError) as e:
            bt.logging.warning(f'SwapTracker poll transient error: {e}')
        except Exception as e:
            bt.logging.error(f'SwapTracker poll error: {e}')
            raise

    async def _poll_inner(self):
        next_id = await asyncio.to_thread(self.client.get_next_swap_id)

        # --- Discovery phase: scan new swap IDs ---
        fresh: Set[int] = set()
        new_ids = list(range(self.last_scanned_id + 1, next_id))
        if new_ids:
            swaps = await asyncio.gather(*[asyncio.to_thread(self.client.get_swap, sid) for sid in new_ids])

            for sid, swap in zip(new_ids, swaps):
                if swap is None:
                    continue
                if swap.status in ACTIVE_STATUSES:
                    self.active[swap.id] = swap
                    fresh.add(swap.id)

        if new_ids:
            bt.logging.debug(f'SwapTracker: discovered {len(fresh)} active from {len(new_ids)} new IDs')

        if next_id > 1:
            self.last_scanned_id = next_id - 1

        # --- Monitoring phase: refresh active set ---
        stale_ids = [sid for sid in self.active if sid not in fresh]
        if not stale_ids:
            return

        swaps = await asyncio.gather(*[asyncio.to_thread(self.client.get_swap, sid) for sid in stale_ids])

        resolved_ids = []
        for sid, swap in zip(stale_ids, swaps):
            if swap is None:
                # Swap removed from contract (resolved by another validator's quorum vote).
                # If resolve() already captured it, active won't have it; otherwise infer state.
                last_known = self.active.get(sid)
                if last_known is not None and sid not in self.voted_ids:
                    was_past_timeout = last_known.timeout_block > 0 and self._current_block > last_known.timeout_block
                    last_known.status = SwapStatus.TIMED_OUT if was_past_timeout else SwapStatus.COMPLETED
                    last_known.completed_block = self._current_block
                    self.window.append(last_known)
                resolved_ids.append(sid)
            elif swap.status in ACTIVE_STATUSES:
                self.active[sid] = swap
            else:
                resolved_ids.append(sid)
                self.window.append(swap)

        for sid in resolved_ids:
            self.active.pop(sid, None)
            self.voted_ids.discard(sid)

        if resolved_ids:
            bt.logging.debug(f'SwapTracker: resolved {len(resolved_ids)}, {len(self.active)} still active')

    def prune_window(self, current_block: int):
        """Remove resolved swaps older than the scoring window."""
        window_start = current_block - self.window_blocks
        before = len(self.window)
        self.window = [s for s in self.window if _resolved_block(s) >= window_start]
        pruned = before - len(self.window)
        if pruned > 0:
            bt.logging.debug(f'SwapTracker: pruned {pruned} expired swaps from window')

    def get_fulfilled(self, current_block: int) -> List[Swap]:
        """Active FULFILLED swaps not yet past timeout (ready for verification)."""
        return [
            s
            for s in self.active.values()
            if s.status == SwapStatus.FULFILLED and (s.timeout_block == 0 or current_block <= s.timeout_block)
        ]

    def get_near_timeout_fulfilled(self, current_block: int, threshold: int) -> List[Swap]:
        """FULFILLED swaps approaching timeout (within threshold blocks)."""
        return [
            s
            for s in self.active.values()
            if s.status == SwapStatus.FULFILLED and s.timeout_block > 0 and current_block >= s.timeout_block - threshold
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


def _resolved_block(swap: Swap) -> int:
    """Block when a terminal swap was resolved."""
    if swap.completed_block > 0:
        return swap.completed_block
    if swap.timeout_block > 0:
        return swap.timeout_block
    return swap.initiated_block
