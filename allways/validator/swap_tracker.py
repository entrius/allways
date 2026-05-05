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

# Cold-start backward scan halts after this many consecutive None lookups.
# Resolved swaps are pruned from contract storage (`swaps.remove`), so a long
# run of Nones means we've passed the live-swap region. Block-age cutoffs
# silently dropped long-stuck ACTIVE swaps; this does not. Tuned low for
# current volume — bump if the gap between contiguous active swap IDs grows.
MAX_INIT_GAP = 20

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
        self.null_retry_count: Dict[int, int] = {}

    def initialize(self):
        """Cold start: scan backward from latest swap to seed active set.

        Halts on consecutive Nones (resolved/pruned region) rather than on
        block age — an ACTIVE swap orphaned by a prior validator outage can be
        arbitrarily old, and a block-based cutoff would let it fall through.
        """
        next_id = self.client.get_next_swap_id()
        if next_id <= 1:
            self.last_scanned_id = 0
            bt.logging.info('SwapTracker initialized: no swaps exist')
            return

        consecutive_none = 0
        for swap_id in range(next_id - 1, 0, -1):
            swap = self.client.get_swap(swap_id)
            if swap is None:
                consecutive_none += 1
                if consecutive_none >= MAX_INIT_GAP:
                    bt.logging.debug(
                        f'SwapTracker init: stopping at swap {swap_id} '
                        f'after {consecutive_none} consecutive resolved/pruned swaps'
                    )
                    break
                continue
            consecutive_none = 0
            if swap.status in ACTIVE_STATUSES:
                self.active[swap.id] = swap

        self.last_scanned_id = next_id - 1

        bt.logging.info(f'SwapTracker initialized: active={len(self.active)}, last_scanned_id={self.last_scanned_id}')

    def resolve(self, swap_id: int, status: SwapStatus, block: int):
        """Drop a swap from tracking after our vote reached quorum."""
        swap = self.active.pop(swap_id, None)
        if swap is None:
            return
        swap.status = status
        swap.completed_block = block
        self.voted_ids.discard(swap_id)
        self.null_retry_count.pop(swap_id, None)

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
                self.null_retry_count.pop(swap.id, None)

        if next_id > 1:
            self.last_scanned_id = next_id - 1

        # --- Monitoring phase: refresh active set ---
        stale_ids = [sid for sid in self.active if sid not in fresh]
        if not stale_ids:
            self.prune_stale_voted_ids()
            return

        # Null and transient errors share one retry policy — a missing swap
        # is either an RPC flake or a freshly-resolved entry the event
        # watcher will record. Retry a few times, then drop.
        resolved_ids: List[int] = []
        for sid in stale_ids:
            try:
                result = await asyncio.to_thread(self.client.get_swap, sid)
            except Exception as e:
                # Transient RPC error — leave the swap in active and don't
                # advance null-retry. Retried next poll.
                bt.logging.debug(f'SwapTracker refresh({sid}) failed, will retry: {e}')
                continue

            if result is None:
                if self.bump_null_retry(sid):
                    resolved_ids.append(sid)
            elif result.status in ACTIVE_STATUSES:
                prev = self.active.get(sid)
                if prev is not None and prev.status != result.status:
                    bt.logging.info(f'Swap {sid} [{_swap_label(result)}]: {prev.status.name} -> {result.status.name}')
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
