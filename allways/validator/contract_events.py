"""Canonical contract event labels consumed by the validator."""

from __future__ import annotations

from enum import Enum


class ContractEventName(str, Enum):
    MINER_ACTIVATED = 'MinerActivated'
    SWAP_INITIATED = 'SwapInitiated'
    SWAP_COMPLETED = 'SwapCompleted'
    SWAP_TIMED_OUT = 'SwapTimedOut'
