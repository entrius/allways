"""Block-TTL cache for admin-set contract bounds.

min_collateral, min_swap_amount, and max_swap_amount only change via admin
tx, so re-reading them on every axon request burns RPC budget the chain
may not have (testnet caps at 45/min). Halt is in the same shape (admin
toggle), just shorter TTL because the dashboard wants quick freshness.
"""

import threading
import time
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


class SolanaConfigCache:
    """Time-TTL cache over the on-chain Solana ``Config`` account — the
    validator's scoring-path source for swap bounds + halt after B3.6 dropped
    the substrate reads. One ``Config`` read carries every field, so the whole
    struct is cached; halt re-reads on a shorter TTL for dashboard freshness.
    """

    TTL_SECS = 300
    HALT_TTL_SECS = 60

    def __init__(self, solana_client: Any, clock: Optional[Callable[[], float]] = None):
        self._client = solana_client
        self._clock = clock or time.time
        self._lock = threading.Lock()
        self._cfg: Any = None
        self._cfg_at: float = 0.0
        self._halt: Optional[bool] = None
        self._halt_at: float = 0.0

    def _fresh_config(self) -> Any:
        now = self._clock()
        if self._cfg is None or now - self._cfg_at >= self.TTL_SECS:
            self._cfg = self._client.get_config()
            self._cfg_at = now
        return self._cfg

    def min_swap_amount(self) -> int:
        return int(self._fresh_config().min_swap_amount)

    def max_swap_amount(self) -> int:
        return int(self._fresh_config().max_swap_amount)

    def fulfillment_timeout_secs(self) -> int:
        return int(self._fresh_config().fulfillment_timeout_secs)

    def halted(self) -> bool:
        """Shorter-TTL halt read. Fails open to last-known (or False) so a
        flaky RPC can't zero every miner's reward — matches
        ``scoring.contract_is_halted``."""
        with self._lock:
            now = self._clock()
            if self._halt is not None and now - self._halt_at < self.HALT_TTL_SECS:
                return self._halt
            try:
                value = bool(self._client.get_config().halted)
                self._halt = value
                self._halt_at = now
                return value
            except Exception as e:
                bt.logging.warning(f'halt Config read failed: {e}')
                return self._halt if self._halt is not None else False
