"""Block-TTL cache for admin-set contract bounds.

min_collateral, min_swap_amount, and max_swap_amount only change via admin
tx, so re-reading them on every axon request burns RPC budget the chain
may not have (testnet caps at 45/min). Halt is in the same shape (admin
toggle), just shorter TTL because the dashboard wants quick freshness.
"""

import threading
from typing import Any, Callable, Optional

import bittensor as bt

from allways.contract_client import AllwaysContractClient


class BoundsCache:
    TTL_BLOCKS = 300
    # Halt changes via admin tx like the bounds, but a 60min lag here would
    # mean validators keep voting for an hour after a halt (contract just
    # rejects them) and the dashboard live-crown clears an hour after the
    # unhalt. 5 blocks (~60s) keeps the worst-case lag tolerable.
    HALT_TTL_BLOCKS = 5

    def __init__(
        self,
        contract: AllwaysContractClient,
        get_block: Callable[[], int],
        lock: Optional[Any] = None,
    ):
        self._contract = contract
        self._get_block = get_block
        self._cache: dict[str, tuple[int, Any]] = {}
        # get_block and the reads share the caller's subtensor websocket; hold
        # the connection's lock so a shared thread can't race us into recv.
        self._lock = lock or threading.Lock()

    def _cached(self, key: str, read: Callable[[], int]) -> int:
        with self._lock:
            now = self._get_block()
            entry = self._cache.get(key)
            if entry is not None and now - entry[0] < self.TTL_BLOCKS:
                return entry[1]
            value = read()
            self._cache[key] = (now, value)
            return value

    def min_collateral(self) -> int:
        return self._cached('min_collateral', self._contract.get_min_collateral)

    def min_swap_amount(self) -> int:
        return self._cached('min_swap_amount', self._contract.get_min_swap_amount)

    def max_swap_amount(self) -> int:
        return self._cached('max_swap_amount', self._contract.get_max_swap_amount)

    def halted(self) -> bool:
        """Cached get_halted with separate (shorter) TTL.

        On RPC failure, returns the last cached value if any (so transient
        flakes don't churn the writer) or False (no prior cache → assume
        not halted, matching scoring.contract_is_halted's fail-open
        behavior so a flaky RPC can't zero every miner's reward)."""
        with self._lock:
            now = self._get_block()
            entry = self._cache.get('halted')
            if entry is not None and now - entry[0] < self.HALT_TTL_BLOCKS:
                return bool(entry[1])
            try:
                value = bool(self._contract.get_halted())
                self._cache['halted'] = (now, value)
                return value
            except Exception as e:
                bt.logging.warning(f'halt RPC check failed: {e}')
                return bool(entry[1]) if entry is not None else False
