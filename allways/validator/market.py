"""What ``/rate`` reads instead of the chain: the market as the forward step last saw it, plus a short
alpha-price memo. A quote is then math on held data; reserve, confirm and finalize keep reading live."""

import threading
import time
from typing import Callable, Dict, List, Optional, Tuple

from allways.assets.alpha import Alpha
from allways.validator.binding import build_attribution

HELD_ACCOUNTS = ('MinerQuote', 'MinerState', 'BondAttestation')
MARKET_MAX_AGE_SECS = 60  # older means the forward step stalled: quote off a live read instead
PRICE_TTL_SECS = 12


class MarketSnapshot:
    """One read of every quote, miner state and bond attestation, with the verified pubkey→hotkey
    attribution. Immutable; answers the reader calls ``swap_intake`` makes on the Solana client."""

    def __init__(self, taken_at: float, accounts: Dict[str, List[Tuple[str, object]]], attribution: Dict[str, str]):
        self.taken_at = taken_at
        self.attribution = attribution
        self._accounts = accounts
        self._states = {str(ms.miner): ms for _pda, ms in accounts['MinerState']}
        self._attestations = {(str(ba.miner), str(ba.chain).lower()): ba for _pda, ba in accounts['BondAttestation']}

    @classmethod
    def read(cls, client) -> 'MarketSnapshot':
        return cls(time.time(), {name: client.get_all(name) for name in HELD_ACCOUNTS}, build_attribution(client))

    def stale(self, now: float) -> bool:
        return now - self.taken_at > MARKET_MAX_AGE_SECS

    def get_all(self, name: str) -> List[Tuple[str, object]]:
        return self._accounts[name]

    def get_miner_state(self, miner):
        return self._states.get(str(miner))

    def get_bond_attestation(self, miner, chain: str = 'tao'):
        return self._attestations.get((str(miner), chain))


class AlphaPriceMemo:
    """Rao per whole alpha, per netuid, held ``PRICE_TTL_SECS``. One read at a time: the thread that finds
    the memo cold reads while the rest wait for its answer."""

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
    """``alpha`` with ``value_rao`` at head served off the memo; a dated read, and everything else, pass through."""

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
    """``providers`` with every alpha wrapped for the memo; the shared objects themselves are untouched."""
    return {chain: MemoPricedAlpha(p, memo) if isinstance(p, Alpha) else p for chain, p in providers.items()}
