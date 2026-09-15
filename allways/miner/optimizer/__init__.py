"""Quote optimizer: an opt-in strategy that keeps a miner's SOL↔TAO quotes priced (README, "Quote Optimizer").

``attach_optimizer(miner)`` is the only entry point the base miner uses; the rest of this package is the strategy,
free to be copied or replaced by a miner's own."""

from allways.miner.optimizer.attach import attach_optimizer

__all__ = ['attach_optimizer']
