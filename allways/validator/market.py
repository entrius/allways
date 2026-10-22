# What /rate reads instead of the chain: forward's live miner states plus memos of quotes, bond attestations,
# reservations, pools and alpha prices that are read only when a quote asks. Reserve, confirm and finalize read live.

import threading
import time
from typing import Callable, Dict, List, Optional, Tuple

from allways.assets.alpha import Alpha
from allways.solana.pdas import reservation_pda

MARKET_TTL_SECS = 60  # how long /rate trusts a read: forward's held states and its own quote memo alike
PRICE_TTL_SECS = 12


class QuoteMemo:
    # One read at a time: the thread that finds the memo cold reads while the rest wait for its answer.

    def __init__(self, clock: Callable[[], float] = time.time):
        self._clock = clock
        self._lock = threading.Lock()
        self._read_at: Optional[float] = None
        self._quotes: List[Tuple[str, object]] = []
        self._attestations: Dict[Tuple[str, str], object] = {}
        self._holds: Tuple[Dict[Tuple[str, str], object], Dict[Tuple[str, str], object]] = ({}, {})

    def accounts(self, client) -> tuple:
        """(quotes, attestations, reservations, pools); the last three keyed by (miner, backing)."""
        with self._lock:
            now = self._clock()
            if self._read_at is None or now - self._read_at >= MARKET_TTL_SECS:
                quotes = client.get_all('MinerQuote')  # a failed read memoizes nothing
                attestations = client.get_all('BondAttestation')
                reservations = client.get_all('Reservation')
                pools = client.get_all('Pool')
                self._quotes = quotes
                self._attestations = {(str(ba.miner), str(ba.chain).lower()): ba for _pda, ba in attestations}
                self._holds = _holds_by_hub(client, reservations, pools)
                self._read_at = now
            return self._quotes, self._attestations, *self._holds


def _holds_by_hub(client, reservations, pools) -> tuple:
    # A Reservation names no miner; it sits at the PDA of the pool's (miner, backing), opened with it.
    by_pda = {str(pda): r for pda, r in reservations}
    resv, pool_by_hub = {}, {}
    for _pda, pool in pools:
        hub = (str(pool.miner), str(pool.collateral_chain))
        pool_by_hub[hub] = pool
        r = by_pda.get(str(reservation_pda(pool.miner, hub[1], client.program_id)))
        if r is not None:
            resv[hub] = r
    return resv, pool_by_hub


class Market:
    # The reader swap_intake quotes off. ``live_states`` is forward's bound, on-metagraph {hotkey: MinerState}:
    # its hotkeys are the verified attribution, so no Binding read is needed.

    def __init__(
        self,
        live_states: Dict[str, object],
        quotes: List[Tuple[str, object]],
        attestations: Dict[Tuple[str, str], object],
        reservations: Optional[Dict[Tuple[str, str], object]] = None,
        pools: Optional[Dict[Tuple[str, str], object]] = None,
    ):
        self.attribution = {str(ms.miner): hotkey for hotkey, ms in live_states.items()}
        self._states = {str(ms.miner): ms for ms in live_states.values()}
        self._quotes = quotes
        self._attestations = attestations
        self._reservations = reservations or {}
        self._pools = pools or {}

    def get_all(self, name: str) -> List[Tuple[str, object]]:
        if name != 'MinerQuote':
            raise KeyError(name)
        return self._quotes

    def get_miner_state(self, miner):
        return self._states.get(str(miner))

    def get_bond_attestation(self, miner, chain: str = 'tao'):
        return self._attestations.get((str(miner), chain))

    def get_reservation(self, miner, backing: str = 'sol'):
        return self._reservations.get((str(miner), backing))

    def get_pool(self, miner, backing: str = 'sol'):
        return self._pools.get((str(miner), backing))


class AlphaPriceMemo:
    # Rao per whole alpha, per netuid. Single-flight like QuoteMemo.

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
