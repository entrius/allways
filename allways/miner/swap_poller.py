"""Polls the smart contract for new swaps assigned to this miner using incremental scanning."""

from typing import Dict, List, Set, Tuple

import bittensor as bt

from allways.classes import Swap, SwapStatus
from allways.contract_client import AllwaysContractClient

RESCAN_WINDOW = 16
ACTIVE_STATUSES = (SwapStatus.ACTIVE, SwapStatus.FULFILLED)
MAX_REFRESH_MISSES = 3


class SwapPoller:
    """Incrementally polls the contract for swaps assigned to this miner.

    Uses a cursor to avoid O(N) full scans. Only fetches new swap IDs
    since last poll, then refreshes the active set. Whether a swap has
    already been handled is tracked by ``SwapFulfiller``'s persistent
    send cache — this poller reports raw contract state only.
    """

    def __init__(self, contract_client: AllwaysContractClient, miner_hotkey: str):
        self.client = contract_client
        self.miner_hotkey = miner_hotkey
        self.last_scanned_id = 0
        self.active: Dict[int, Swap] = {}
        self.active_miss_counts: Dict[int, int] = {}
        self.last_poll_ok: bool = True

    def poll(self) -> Tuple[List[Swap], List[Swap]]:
        """Incremental poll. Returns (active, fulfilled) for this miner."""
        try:
            result = self.poll_inner()
            self.last_poll_ok = True
            return result
        except Exception as e:
            bt.logging.error(f'SwapPoller poll error: {type(e).__name__}: {e}')
            self.last_poll_ok = False
            return [], []

    def poll_inner(self) -> Tuple[List[Swap], List[Swap]]:
        # 1. Discover new swaps since last scan
        fresh: Set[int] = set()
        next_id = self.client.get_next_swap_id()
        start = max(1, min(self.last_scanned_id + 1, next_id - RESCAN_WINDOW))
        for swap_id in range(start, next_id):
            try:
                swap = self.client.get_swap(swap_id)
            except Exception as e:
                bt.logging.debug(f'SwapPoller discovery({swap_id}) failed, will retry: {e}')
                continue
            if swap and swap.miner_hotkey == self.miner_hotkey:
                if swap.status in ACTIVE_STATUSES:
                    if swap.id not in self.active:
                        bt.logging.info(
                            f'Discovered swap {swap.id}: {swap.from_chain} -> {swap.to_chain}, '
                            f'tao_amount={swap.tao_amount}, status={swap.status.name}'
                        )
                    self.active[swap.id] = swap
                    self.active_miss_counts.pop(swap.id, None)
                    fresh.add(swap.id)
        if next_id > 1:
            self.last_scanned_id = next_id - 1

        # 2. Refresh active set — skip freshly discovered swaps, remove resolved
        resolved: list[tuple[int, str]] = []
        for swap_id in list(self.active):
            if swap_id in fresh:
                continue
            try:
                swap = self.client.get_swap(swap_id)
            except Exception as e:
                bt.logging.debug(f'SwapPoller refresh({swap_id}) failed, will retry: {e}')
                continue
            if swap is None:
                misses = self.active_miss_counts.get(swap_id, 0) + 1
                self.active_miss_counts[swap_id] = misses
                if misses >= MAX_REFRESH_MISSES:
                    resolved.append((swap_id, f'GONE_AFTER_{misses}_MISSES'))
                continue
            if swap.status not in ACTIVE_STATUSES:
                terminal = swap.status.name
                resolved.append((swap_id, terminal))
            else:
                self.active[swap_id] = swap
                self.active_miss_counts.pop(swap_id, None)
        for sid, terminal in resolved:
            self.active.pop(sid, None)
            self.active_miss_counts.pop(sid, None)
            bt.logging.info(f'Swap {sid}: dropped from active (status={terminal})')

        # 3. Return categorized by contract status
        active_swaps = [s for s in self.active.values() if s.status == SwapStatus.ACTIVE]
        fulfilled = [s for s in self.active.values() if s.status == SwapStatus.FULFILLED]
        return active_swaps, fulfilled
