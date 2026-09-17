"""TAO balance reads for the quote optimizer, on a Subtensor connection of its own.

The miner's Subtensor websocket is not safe to share across threads: an optimizer read that lands while the swap loop
resyncs the metagraph fails with "cannot call recv while another thread is already running recv", and the miner's TAO
provider reports that failure as a 0 balance (seen on mainnet as a wallet flapping between 0.8339 and 0.0000 TAO).
This reader keeps its own connection, reconnects after a failure, holds the last good reading through a failed read
for ``HOLD_SECS``, and reports unknown (None) rather than 0 once nothing fresh is left.
"""

import time
from typing import Callable, Dict, Optional, Tuple

import bittensor as bt

HOLD_SECS = 300


class TaoBalanceReader:
    """``get_balance(address)`` in rao, shaped like a chain provider so the optimizer can use it in place of one."""

    def __init__(self, connect: Callable[[], object], clock: Callable[[], float] = time.time):
        self.connect = connect
        self.clock = clock
        self.subtensor = None
        self.last: Dict[str, Tuple[int, float]] = {}

    def get_balance(self, address: str) -> Optional[int]:
        now = self.clock()
        try:
            if self.subtensor is None:
                self.subtensor = self.connect()
            balance = int(self.subtensor.get_balance(address))
        except Exception as e:
            self.subtensor = None  # reconnect on the next read
            held = self.last.get(address)
            if held is not None and now - held[1] <= HOLD_SECS:
                bt.logging.debug(
                    f'optimizer: TAO balance read failed ({e}); holding the reading from {int(now - held[1])}s ago'
                )
                return held[0]
            bt.logging.warning(f'optimizer: TAO balance read failed ({e}); balance unknown')
            return None
        self.last[address] = (balance, now)
        return balance
