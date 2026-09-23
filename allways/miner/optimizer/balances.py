"""Balance reads for the quote optimizer that never report a failed read as 0.

The miner's own chain providers answer a failed balance read with 0, and to the optimizer 0 means "send more or pull the
quote". Each reader here holds the last good reading through a failed read for ``HOLD_SECS``, and reports unknown
(None) rather than 0 once nothing fresh is left.

TAO: the miner's Subtensor websocket is not safe to share across threads — an optimizer read that lands while the swap
loop resyncs the metagraph fails with "cannot call recv while another thread is already running recv", and the miner's
TAO provider reports that failure as a 0 balance (seen on mainnet as a wallet flapping between 0.8339 and 0.0000 TAO).
Its reader keeps its own connection and reconnects after a failure.

BTC: the miner's BTC provider returns 0 when every Esplora endpoint fails. Its reader asks the same endpoints through
the provider and counts the same coins — confirmed and mempool, since a payout may spend unconfirmed change.
"""

import time
from typing import Callable, Dict, Optional, Tuple

import bittensor as bt

HOLD_SECS = 300
ESPLORA_TIMEOUT_SECS = 15


class HeldBalanceReader:
    """``get_balance(address)`` in the chain's smallest unit, shaped like a chain provider so the optimizer can use it in
    place of one. Subclasses implement ``read``, raising on any failure."""

    chain = ''

    def __init__(self, clock: Callable[[], float] = time.time):
        self.clock = clock
        self.last: Dict[str, Tuple[int, float]] = {}

    def read(self, address: str) -> int:
        raise NotImplementedError

    def on_failure(self) -> None:
        """Reset whatever the failed read may have broken."""

    def get_balance(self, address: str) -> Optional[int]:
        now = self.clock()
        try:
            balance = int(self.read(address))
        except Exception as e:
            self.on_failure()
            held = self.last.get(address)
            if held is not None and now - held[1] <= HOLD_SECS:
                bt.logging.debug(
                    f'optimizer: {self.chain} balance read failed ({e}); holding the reading from {int(now - held[1])}s ago'
                )
                return held[0]
            bt.logging.warning(f'optimizer: {self.chain} balance read failed ({e}); balance unknown')
            return None
        self.last[address] = (balance, now)
        return balance


class TaoBalanceReader(HeldBalanceReader):
    """TAO in rao, on a Subtensor connection of its own that is reopened after a failed read."""

    chain = 'TAO'

    def __init__(self, connect: Callable[[], object], clock: Callable[[], float] = time.time):
        super().__init__(clock)
        self.connect = connect
        self.subtensor = None

    def read(self, address: str) -> int:
        if self.subtensor is None:
            self.subtensor = self.connect()
        return int(self.subtensor.get_balance(address))

    def on_failure(self) -> None:
        self.subtensor = None  # reconnect on the next read


class BtcBalanceReader(HeldBalanceReader):
    """BTC in satoshis from the miner's BTC provider's Esplora endpoints (``btc_api_get`` tries each in turn)."""

    chain = 'BTC'

    def __init__(self, provider, clock: Callable[[], float] = time.time):
        super().__init__(clock)
        self.provider = provider

    def read(self, address: str) -> int:
        resp = self.provider.btc_api_get(f'/address/{address}', timeout=ESPLORA_TIMEOUT_SECS)
        resp.raise_for_status()
        data = resp.json()
        chain, mempool = data['chain_stats'], data['mempool_stats']
        return (int(chain['funded_txo_sum']) - int(chain['spent_txo_sum'])) + (
            int(mempool['funded_txo_sum']) - int(mempool['spent_txo_sum'])
        )
