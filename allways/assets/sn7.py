from typing import Optional

import bittensor as bt

from allways.assets.alpha import Alpha
from allways.chains import CHAIN_SN7


class Sn7(Alpha):
    def __init__(self, subtensor: bt.Subtensor, wallet: Optional[bt.Wallet] = None):
        super().__init__(CHAIN_SN7, subtensor, wallet)
