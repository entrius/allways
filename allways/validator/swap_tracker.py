"""Incremental swap lifecycle tracker. Eliminates O(N) full scans.

Swap outcomes (credibility ledger writes) are owned by
``ContractEventWatcher``, which replays ``SwapCompleted`` / ``SwapTimedOut``
events into ``state_store.swap_outcomes``. The tracker here just maintains
the in-memory active set so the forward loop knows what to verify, vote on,
and time out.
"""

import asyncio
from typing import Dict, List, Set

import bittensor as bt

from allways.classes import Swap, SwapStatus
from allways.contract_client import AllwaysContractClient

ACTIVE_STATUSES = (SwapStatus.ACTIVE, SwapStatus.FULFILLED)

# How many consecutive ``get_swap == None`` polls we tolerate before dropping
# a swap from the active set. Tolerates transient RPC flakes without the
# fragile timeout-block inference the V1 tracker used.
NULL_SWAP_RETRY_LIMIT = 3


class SwapTracker:
    """Tracks swap lifecycle incrementally. No full scans after initialization.

    Two layers:
    - Discovery: scan only NEW swap IDs since last poll
    - Monitoring: re-fetch all tracked ACTIVE/FULFILLED swaps each poll
    """

    def __init__(
        self,
        client: AllwaysContractClient,
        fulfillment_timeout_blocks: int,
    ):
        self.client = client
        self.last_scanned_id = 0
        self.active: Dict[int, Swap] = {}
        self.voted_ids: Set[int] = set()
        self._null_retry_count: Dict[int, int] = {}
        self.fulfillment_timeout_blocks = fulfillment_timeout_blocks

    def initialize(self, current_block: int):
        """Cold start — scan backward from latest swap to populate active set."""
        next_id = self.client.get_next_swap_id()
        if next_id <= 1:
            self.last_scanned_id = 0
            bt.logging.info('SwapTracker initialized: no swaps exist')
            return

        cutoff_block = current_block - self.fulfillment_timeout_blocks
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
        """Drop a swap from active tracking after this validator's vote reached quorum.

        Outcome persistence is the event watcher's job — we only manage the
        in-memory active set here.
        """
        swap = self.active.pop(swap_id, None)
        if swap is None:
            return
        swap.status = status
        swap.completed_block = block
        self.voted_ids.discard(swap_id)
        self._null_retry_count.pop(swap_id, None)

    def mark_voted(self, swap_id: int):
        """Mark a swap as voted on to prevent redundant vote extrinsics."""
        self.voted_ids.add(swap_id)

    def is_voted(self, swap_id: int) -> bool:
        """Check if we've already voted on this swap."""
        return swap_id in self.voted_ids

    async def poll(self, current_block: int = 0):
        """Incremental update — called every forward step (~12s)."""
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

        resolved_ids: List[int] = []
        for sid, swap in zip(stale_ids, swaps):
            if swap is None:
                # Contract returned None. Either the swap resolved and was
                # removed from contract storage, or an RPC flake. Retry a few
                # times before dropping — event watcher will write the outcome
                # when it replays the SwapCompleted/SwapTimedOut events.
                retries = self._null_retry_count.get(sid, 0) + 1
                if retries >= NULL_SWAP_RETRY_LIMIT:
                    resolved_ids.append(sid)
                else:
                    self._null_retry_count[sid] = retries
            elif swap.status in ACTIVE_STATUSES:
                self.active[sid] = swap
                self._null_retry_count.pop(sid, None)
            else:
                resolved_ids.append(sid)

        for sid in resolved_ids:
            self.active.pop(sid, None)
            self.voted_ids.discard(sid)
            self._null_retry_count.pop(sid, None)

        if resolved_ids:
            bt.logging.debug(f'SwapTracker: resolved {len(resolved_ids)}, {len(self.active)} still active')

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
