from abc import ABC, abstractmethod
from typing import Optional


class Chain(ABC):
    """A network you can talk to: reach it, read blocks/txs, know its id.

    Assets live on chains (`Asset.chain`). Single-asset networks fuse the two — the
    asset class also implements Chain and `.chain` returns itself — and split the day
    the chain hosts its second asset. Multi-asset families (EVM) bind a shared,
    config-driven Chain instance per network instead of a class per chain.
    """

    @abstractmethod
    def get_current_block_height(self) -> Optional[int]:
        """Chain tip block height. None on transient backend failure."""
        ...
