"""Time-TTL cache for admin-set contract bounds, sourced off the Solana ``Config``.

min_swap_amount, max_swap_amount, fulfillment_timeout, and halt only change via
admin tx, so re-reading them on every scoring round burns RPC budget. One
``Config`` read carries every field; halt re-reads on a shorter TTL for
dashboard freshness.
"""

import threading
import time
from typing import Any, Callable, Optional

import bittensor as bt


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

    def reservation_ttl_secs(self) -> int:
        return int(self._fresh_config().reservation_ttl_secs)

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
