"""Shared fee calculation for scoring and recycle."""

from typing import List

from allways.classes import Swap, SwapStatus
from allways.constants import DEFAULT_FEE_DIVISOR


def swap_fee_rao(swap: Swap, fee_divisor: int = DEFAULT_FEE_DIVISOR) -> int:
    """Calculate a single swap's fee in rao.

    tao_amount is always in rao regardless of swap direction,
    matching the contract's fee calculation: tao_amount // fee_divisor.
    """
    return swap.tao_amount // fee_divisor


def windowed_fees_rao(window: List[Swap], fee_divisor: int = DEFAULT_FEE_DIVISOR) -> int:
    """Sum all completed swap fees in rao across a scoring window."""
    return sum(swap_fee_rao(swap, fee_divisor) for swap in window if swap.status == SwapStatus.COMPLETED)
