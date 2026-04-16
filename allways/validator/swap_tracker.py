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

# Consecutive None polls tolerated before treating a swap as resolved. Smooths
# RPC flakes without the fragile timeout-block inference the V1 tracker used.
NULL_SWAP_RETRY_LIMIT = 3


class SwapTracker:
    """Discovery scans new swap IDs since the last poll; monitoring re-fetches
    all tracked ACTIVE/FULFILLED swaps each poll."""

    def __init__(
        self,
        client: AllwaysContractClient,
        fulfillment_timeout_blocks: int,
    ):
        self.client = client
        self.last_scanned_id = 0
        self.active: Dict[int, Swap] = {}
        self.voted_ids: Set[int] = set()
        # swap_id → timeout_block at vote time. ``is_extend_timeout_voted``
        # auto-clears the entry once the contract has bumped the swap past
        # the voted value so the next extension round can vote again.
        self.extend_timeout_voted_at: Dict[int, int] = {}
        self.null_retry_count: Dict[int, int] = {}
        self.fulfillment_timeout_blocks = fulfillment_timeout_blocks

    def initialize(self, current_block: int):
        """Cold start: scan backward from latest swap to seed active set."""
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
        """Drop a swap from tracking after our vote reached quorum."""
        swap = self.active.pop(swap_id, None)
        if swap is None:
            return
        swap.status = status
        swap.completed_block = block
        self.voted_ids.discard(swap_id)
        self.extend_timeout_voted_at.pop(swap_id, None)
        self.null_retry_count.pop(swap_id, None)

    def mark_voted(self, swap_id: int):
        """Mark a swap as voted on to prevent redundant confirm/timeout extrinsics."""
        self.voted_ids.add(swap_id)

    def is_voted(self, swap_id: int) -> bool:
        return swap_id in self.voted_ids

    def mark_extend_timeout_voted(self, swap_id: int) -> None:
        swap = self.active.get(swap_id)
        if swap is not None:
            self.extend_timeout_voted_at[swap_id] = swap.timeout_block

    def is_extend_timeout_voted(self, swap_id: int) -> bool:
        voted_at = self.extend_timeout_voted_at.get(swap_id)
        if voted_at is None:
            return False
        swap = self.active.get(swap_id)
        if swap is not None and swap.timeout_block > voted_at:
            # contract extended the swap → vote opens again for the next round
            self.extend_timeout_voted_at.pop(swap_id, None)
            return False
        return True

    async def poll(self, current_block: int = 0):
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
        fresh: Set[int] = set()
        new_ids = list(range(self.last_scanned_id + 1, next_id))
        if new_ids:
            # return_exceptions=True keeps one flaky get_swap from killing the step.
            swaps = await asyncio.gather(
                *[asyncio.to_thread(self.client.get_swap, sid) for sid in new_ids],
                return_exceptions=True,
            )
            for sid, result in zip(new_ids, swaps):
                if isinstance(result, Exception):
                    bt.logging.debug(f'SwapTracker: get_swap({sid}) failed during discovery: {result}')
                    continue
                swap = result
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
            self.prune_stale_voted_ids()
            return

        swaps = await asyncio.gather(
            *[asyncio.to_thread(self.client.get_swap, sid) for sid in stale_ids],
            return_exceptions=True,
        )

        # Null and transient errors share one retry policy — a missing swap
        # is either an RPC flake or a freshly-resolved entry the event
        # watcher will record. Retry a few times, then drop.
        resolved_ids: List[int] = []
        for sid, result in zip(stale_ids, swaps):
            if isinstance(result, Exception):
                bt.logging.debug(f'SwapTracker: get_swap({sid}) failed during refresh: {result}')
                result = None

            if result is None:
                if self.bump_null_retry(sid):
                    resolved_ids.append(sid)
            elif result.status in ACTIVE_STATUSES:
                self.active[sid] = result
                self.null_retry_count.pop(sid, None)
            else:
                resolved_ids.append(sid)

        for sid in resolved_ids:
            self.active.pop(sid, None)
            self.voted_ids.discard(sid)
            self.null_retry_count.pop(sid, None)

        if resolved_ids:
            bt.logging.debug(f'SwapTracker: resolved {len(resolved_ids)}, {len(self.active)} still active')

        self.prune_stale_voted_ids()

    def bump_null_retry(self, swap_id: int) -> bool:
        """Returns True when the retry limit is hit and the caller should
        treat the swap as resolved."""
        retries = self.null_retry_count.get(swap_id, 0) + 1
        if retries >= NULL_SWAP_RETRY_LIMIT:
            return True
        self.null_retry_count[swap_id] = retries
        return False

    def prune_stale_voted_ids(self) -> None:
        """Drop any voted state for swaps no longer being tracked. Normally
        handled inline in ``resolve``/refresh, but an exceptional path (e.g.
        active.pop raced by a fixture) can leave orphans."""
        active_ids = set(self.active.keys())
        self.voted_ids -= self.voted_ids - active_ids
        for sid in list(self.extend_timeout_voted_at.keys()):
            if sid not in active_ids:
                del self.extend_timeout_voted_at[sid]

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
