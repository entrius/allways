"""Incremental swap lifecycle tracker. Eliminates O(N) full scans."""

import asyncio
from typing import Dict, List, Optional, Set

import bittensor as bt

from allways.classes import Swap, SwapStatus
from allways.contract_client import AllwaysContractClient
from allways.validator.scoring_store import ScoringWindowStore, resolved_block

ACTIVE_STATUSES = (SwapStatus.ACTIVE, SwapStatus.FULFILLED)


class SwapTracker:
    """Tracks swap lifecycle incrementally. No full scans after initialization.

    Two layers:
    - Discovery: scan only NEW swap IDs since last poll
    - Monitoring: re-fetch all tracked ACTIVE/FULFILLED swaps each poll

    Resolved swaps are no longer stored on-chain, so cold start only recovers
    active swaps from chain. When a store is configured, the scoring window and
    voted IDs are restored from disk before active-swap reconciliation.
    """

    def __init__(
        self,
        client: AllwaysContractClient,
        fulfillment_timeout_blocks: int,
        window_blocks: int,
        store: Optional[ScoringWindowStore] = None,
    ):
        self.client = client
        self.last_scanned_id = 0
        self.active: Dict[int, Swap] = {}
        self.window: List[Swap] = []
        self.voted_ids: Set[int] = set()

        self.fulfillment_timeout_blocks = fulfillment_timeout_blocks
        self.window_blocks = window_blocks
        self._store = store

    def initialize(self, current_block: int):
        """Cold start — scan backward from latest swap to populate active set.

        Also restores the scoring window and voted set from disk so that
        scoring is continuous across restarts.
        """
        if self._store:
            restored_window, restored_voted = self._store.load(self.window_blocks, current_block)
            self.window = restored_window
            self.voted_ids = restored_voted

        next_id = self.client.get_next_swap_id()
        if next_id <= 1:
            stale_voted = len(self.voted_ids)
            if stale_voted > 0:
                self.voted_ids.clear()
                bt.logging.debug(f'SwapTracker init: pruned {stale_voted} stale voted IDs (no active swaps)')
                self._persist()
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

        if self.voted_ids:
            before = len(self.voted_ids)
            self.voted_ids.intersection_update(self.active.keys())
            pruned = before - len(self.voted_ids)
            if pruned > 0:
                bt.logging.debug(f'SwapTracker init: pruned {pruned} stale voted IDs')

        self.last_scanned_id = latest_id
        self._persist()

        bt.logging.info(f'SwapTracker initialized: active={len(self.active)}, last_scanned_id={self.last_scanned_id}')

    def resolve(self, swap: Swap, status: Optional[SwapStatus] = None, current_block: Optional[int] = None) -> None:
        """Move a terminal swap from active tracking into the scoring window and persist."""
        if status is not None:
            swap.status = status

        if swap.status == SwapStatus.COMPLETED and swap.completed_block <= 0 and current_block is not None:
            swap.completed_block = current_block
        if swap.status == SwapStatus.TIMED_OUT and swap.timeout_block <= 0 and current_block is not None:
            swap.timeout_block = current_block

        if swap.status not in (SwapStatus.COMPLETED, SwapStatus.TIMED_OUT):
            return

        self.active.pop(swap.id, None)
        self.voted_ids.discard(swap.id)

        for i, existing in enumerate(self.window):
            if existing.id == swap.id:
                self.window[i] = swap
                break
        else:
            self.window.append(swap)

        self._persist()

    def mark_voted(self, swap_id: int):
        """Mark a swap as voted on to prevent redundant vote extrinsics."""
        self.voted_ids.add(swap_id)
        self._persist()

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

        resolved_without_payload = []
        resolved_with_payload = 0
        for sid, swap in zip(stale_ids, swaps):
            if swap is None:
                # Swap removed from contract (resolved by quorum): infer terminal state
                # from timeout relation and persist immediately for restart safety.
                last_known = self.active.get(sid)
                if last_known is not None:
                    was_past_timeout = last_known.timeout_block > 0 and self._current_block > last_known.timeout_block
                    inferred_status = SwapStatus.TIMED_OUT if was_past_timeout else SwapStatus.COMPLETED
                    self.resolve(last_known, status=inferred_status, current_block=self._current_block)
                    resolved_with_payload += 1
                else:
                    resolved_without_payload.append(sid)
            elif swap.status in ACTIVE_STATUSES:
                self.active[sid] = swap
            else:
                self.resolve(swap)
                resolved_with_payload += 1

        for sid in resolved_without_payload:
            self.active.pop(sid, None)
            self.voted_ids.discard(sid)

        if resolved_without_payload:
            self._persist()

        resolved_total = resolved_with_payload + len(resolved_without_payload)
        if resolved_total:
            bt.logging.debug(f'SwapTracker: resolved {resolved_total}, {len(self.active)} still active')

    def prune_window(self, current_block: int):
        """Remove resolved swaps older than the scoring window."""
        window_start = current_block - self.window_blocks
        before = len(self.window)
        self.window = [s for s in self.window if resolved_block(s) >= window_start]
        pruned = before - len(self.window)
        if pruned > 0:
            bt.logging.debug(f'SwapTracker: pruned {pruned} expired swaps from window')
            self._persist()

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

    def _persist(self) -> None:
        """Save window and voted set to disk if a store is configured."""
        if self._store:
            self._store.save(self.window, self.voted_ids)

