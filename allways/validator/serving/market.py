"""What /rate quotes off: the World's quotes, attestations, reservations and pools for the bound, on-metagraph miners, and alpha
prices memoed for a few seconds (the one read a quote can cost)."""

import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from allways.assets.alpha import Alpha

PRICE_TTL_SECS = 12


class Market:
    """The reader swap intake quotes off: only bound, on-metagraph miners (``states``, by hotkey) are listed."""

    def __init__(self, states: Dict[str, Any], accounts: Any):
        self.attribution = {str(ms.miner): hotkey for hotkey, ms in states.items()}
        self._states = {str(ms.miner): ms for ms in states.values()}
        self._accounts = accounts

    def get_all(self, name: str) -> List[Tuple[str, object]]:
        if name != 'MinerQuote':
            raise KeyError(name)
        return self._accounts.get_all('MinerQuote')

    def get_miner_state(self, miner):
        return self._states.get(str(miner))

    def get_bond_attestation(self, miner, chain: str = 'tao'):
        return self._accounts.get_bond_attestation(miner, chain)

    def get_reservation(self, miner, backing: str = 'sol'):
        return self._accounts.get_reservation(miner, backing)

    def get_pool(self, miner, backing: str = 'sol'):
        return self._accounts.get_pool(miner, backing)


class AlphaPriceMemo:
    # Rao per whole alpha, per netuid. Single flight: the thread that finds it cold reads, the rest wait.

    def __init__(self, clock: Callable[[], float] = time.time):
        self._clock = clock
        self._lock = threading.Lock()
        self._prices: Dict[int, Tuple[float, int]] = {}

    def rao_per_whole(self, alpha: Alpha) -> int:
        with self._lock:
            now = self._clock()
            read_at, rao = self._prices.get(alpha.netuid, (None, 0))
            if read_at is None or now - read_at >= PRICE_TTL_SECS:
                rao = alpha.value_rao(10**alpha.chain_def.decimals)  # a failed read memoizes nothing
                self._prices[alpha.netuid] = (now, rao)
            return rao


class MemoPricedAlpha:
    # value_rao at head comes off the memo; a dated read, and everything else, pass through to the alpha.

    def __init__(self, alpha: Alpha, memo: AlphaPriceMemo):
        self._alpha = alpha
        self._memo = memo

    def __getattr__(self, name):
        return getattr(self._alpha, name)

    def value_rao(self, amount: int, block: Optional[int] = None) -> int:
        if block is not None:
            return self._alpha.value_rao(amount, block=block)
        return int(amount) * self._memo.rao_per_whole(self._alpha) // 10**self._alpha.chain_def.decimals


def memo_priced(providers: Dict[str, object], memo: AlphaPriceMemo) -> Dict[str, object]:
    # Wraps each alpha for the memo; the shared provider objects themselves are untouched.
    return {chain: MemoPricedAlpha(p, memo) if isinstance(p, Alpha) else p for chain, p in providers.items()}
