"""Logging surface the lifted alpha modules expect.

These loggers propagate to the root logger that bittensor configures, so alpha lines land in the
neuron's log file and Docker stream without a handler of our own — and stay visible to pytest's
caplog, which captures at the root.
"""

import logging


class _SwapAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return f'[swap {self.extra["swap_id"]}] {msg}', kwargs


def get_logger(process: str) -> logging.Logger:
    logger = logging.getLogger(f'alpha.{process}')
    # bittensor's import lowers the level of loggers that already exist; set ours explicitly so an
    # alpha logger created after that import still emits INFO.
    logger.setLevel(logging.INFO)
    return logger


def swap_log(logger: logging.Logger, swap_id: str) -> logging.LoggerAdapter:
    return _SwapAdapter(logger, {'swap_id': swap_id})
