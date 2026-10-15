"""What ``/rate`` reads instead of the chain: the live miner states the forward step already computes, plus
memos of quotes, bond attestations and alpha prices read only when a quote asks. Reserve, confirm and finalize
read live."""

import threading
import time
from typing import Callable, Dict, List, Optional, Tuple

from allways.assets.alpha import Alpha

MARKET_TTL_SECS = 60  # how long /rate trusts a read: forward's held states and its own quote memo alike
PRICE_TTL_SECS = 12


class HeldStates:
    """Forward's ``live_states`` (bound, on-metagraph ``{hotkey: MinerState}``), time-stamped and re-keyed by
    miner pubkey for a quote. Its hotkeys ARE the verified attribution (``live_miner_states`` binds them)."""

    def __init__(self, taken_at: float, live_states: Dict[str, object]):
        self.taken_at = taken_at
        self.attribution = {str(ms.miner): hotkey for hotkey, ms in live_states.items()}
        self._by_miner = {str(ms.miner): ms for ms in live_states.values()}

    def stale(self, now: float) -> bool:
        return now - self.taken_at > MARKET_TTL_SECS

    def get_miner_state(self, miner):
        return self._by_miner.get(str(miner))


class QuoteMemo:
    """MinerQuote and BondAttestation accounts, read when a quote asks and held ``MARKET_TTL_SECS``. One read
    at a time: the thread that finds the memo cold reads while the rest wait for its answer."""

    def __init__(self, clock: Callable[[], float] = time.time):
        self._clock = clock
        self._lock = threading.Lock()
        self._read_at: Optional[float] = None
        self._quotes: List[Tuple[str, object]] = []
        self._attestations: Dict[Tuple[str, str], object] = {}

    def accounts(self, client) -> Tuple[List[Tuple[str, object]], Dict[Tuple[str, str], object]]:
        with self._lock:
            now = self._clock()
            if self._read_at is None or now - self._read_at >= MARKET_TTL_SECS:
                quotes = client.get_all('MinerQuote')  # a failed read memoizes nothing
                attestations = client.get_all('BondAttestation')
                self._quotes = quotes
                self._attestations = {(str(ba.miner), str(ba.chain).lower()): ba for _pda, ba in attestations}
                self._read_at = now
            return self._quotes, self._attestations


class Market:
    """The reader ``swap_intake`` quotes off: held states with memoed quotes and attestations."""

    def __init__(
        self, states: HeldStates, quotes: List[Tuple[str, object]], attestations: Dict[Tuple[str, str], object]
    ):
        self.attribution = states.attribution
        self._states = states
        self._quotes = quotes
        self._attestations = attestations

    def get_all(self, name: str) -> List[Tuple[str, object]]:
        if name != 'MinerQuote':
            raise KeyError(name)
        return self._quotes

    def get_miner_state(self, miner):
        return self._states.get_miner_state(miner)

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
