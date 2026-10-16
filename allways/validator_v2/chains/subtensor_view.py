"""The validator's read of Bittensor: the current block and the subnet metagraph, on a slow cadence.

The only reader of the subnet's state on subtensor; weights, scoring and the stake-weight vote read the
published ``Head`` instead of each calling subtensor. Its connection is its own (the TAO watcher and the
weight setter each hold theirs), so a slow metagraph sync can't stall a leg check."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Callable, Optional

from allways.validator_v2.runtime.bus import Latest
from allways.validator_v2.runtime.component import Component

METAGRAPH_SYNC_SECS = 300.0


@dataclass(frozen=True)
class Head:
    block: int
    block_read_at: float
    metagraph: Any  # bt.Metagraph: hotkeys, alpha_stake, …
    metagraph_read_at: float

    def block_at(self, now: float, seconds_per_block: float = 12.0) -> int:
        """The block now, projected from the last read (a read is at most one tick old)."""
        return self.block + int((now - self.block_read_at) // seconds_per_block)


class SubtensorView(Component):
    name = 'subtensor'
    interval_secs = 12.0
    stall_secs = 600.0

    def __init__(self, subtensor_factory: Callable[[], Any], netuid: int, out: Latest[Head], clock=time.time) -> None:
        super().__init__(clock)
        self._factory = subtensor_factory
        self.netuid = netuid
        self.out = out
        self._subtensor: Any = None
        self._metagraph: Optional[Any] = None
        self._metagraph_at = 0.0

    def setup(self) -> None:
        self._subtensor = self._factory()

    def tick(self, now: float) -> None:
        block = int(self._subtensor.get_current_block())
        if self._metagraph is None or now - self._metagraph_at >= METAGRAPH_SYNC_SECS:
            self._metagraph = self._subtensor.metagraph(self.netuid)
            self._metagraph_at = now
        self.out.set(Head(block, now, self._metagraph, self._metagraph_at))
