"""Block-TTL cache for admin-set contract bounds.

min_collateral, min_swap_amount, and max_swap_amount only change via admin
tx, so re-reading them on every axon request burns RPC budget the chain
may not have (testnet caps at 45/min).
"""

from typing import Callable

from allways.contract_client import AllwaysContractClient


class BoundsCache:
    TTL_BLOCKS = 300

    def __init__(self, contract: AllwaysContractClient, get_block: Callable[[], int]):
        self._contract = contract
        self._get_block = get_block
        self._cache: dict[str, tuple[int, int]] = {}

    def _cached(self, key: str, read: Callable[[], int]) -> int:
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
