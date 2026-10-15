"""What ``/rate`` reads instead of the chain: the market as the forward step last saw it. A quote is then
math on held data; reserve, confirm and finalize keep reading live."""

import time
from typing import Dict, List, Tuple

from allways.validator.binding import build_attribution

HELD_ACCOUNTS = ('MinerQuote', 'MinerState', 'BondAttestation')
MARKET_MAX_AGE_SECS = 60  # older means the forward step stalled: quote off a live read instead


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
