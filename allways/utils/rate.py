"""Shared rate calculation — single source of truth for dest_amount math."""

from decimal import Decimal
from typing import Tuple

from allways.chains import CHAIN_TAO, get_chain
from allways.constants import RATE_PRECISION


def calculate_dest_amount(
    source_amount: int,
    rate: str,
    source_is_tao: bool,
    tao_decimals: int,
    asset_decimals: int,
) -> int:
    """Calculate dest_amount from source_amount and committed rate using fixed-point arithmetic.

    Rate is TAO per 1 non-TAO asset in display units (e.g. 345 means 1 BTC = 345 TAO).
    Uses Decimal for rate conversion to avoid IEEE 754 float rounding artifacts.
    The rate parameter should be the raw string from the miner's commitment.

    Used by miner (fulfillment), validator (verification), and CLI (display).
    All three MUST use this function to guarantee identical results.

    Args:
        source_amount: Amount in smallest units (sat, rao, wei, etc.)
        rate: TAO per 1 non-TAO asset as a string (e.g. '345')
        source_is_tao: True when source chain is TAO
        tao_decimals: Decimal places for TAO (9)
        asset_decimals: Decimal places for non-TAO asset (8 for BTC, 18 for ETH)
    """
    rate_fixed = int(Decimal(rate) * RATE_PRECISION)
    if rate_fixed == 0:
        return 0

    decimal_diff = tao_decimals - asset_decimals

    if source_is_tao:
        # TAO → non-TAO: divide by rate, adjust for decimals
        if decimal_diff >= 0:
            return source_amount * RATE_PRECISION // (rate_fixed * 10**decimal_diff)
        else:
            return source_amount * RATE_PRECISION * 10 ** (-decimal_diff) // rate_fixed
    else:
        # non-TAO → TAO: multiply by rate, adjust for decimals
        if decimal_diff >= 0:
            return source_amount * rate_fixed * 10**decimal_diff // RATE_PRECISION
        else:
            return source_amount * rate_fixed // (RATE_PRECISION * 10 ** (-decimal_diff))


def expected_swap_amounts(swap, fee_divisor: int) -> Tuple[int, int]:
    """Compute expected dest_amount and fee-adjusted user_receives from a swap's on-chain fields.

    Single source of truth used by both miner (fulfillment) and validator (verification).
    Returns (raw_dest_amount, user_receives) or (0, 0) if the rate is invalid.
    """
    source_is_tao = swap.source_chain == 'tao'
    non_tao_chain = swap.dest_chain if source_is_tao else swap.source_chain
    asset_decimals = get_chain(non_tao_chain).decimals

    dest_amount = calculate_dest_amount(
        swap.source_amount,
        swap.rate,
        source_is_tao,
        CHAIN_TAO.decimals,
        asset_decimals,
    )
    if dest_amount == 0:
        return 0, 0

    user_receives = apply_fee_deduction(dest_amount, fee_divisor)
    return dest_amount, user_receives


def apply_fee_deduction(dest_amount: int, fee_divisor: int) -> int:
    """Deduct fee from dest_amount. Returns the amount the user receives.

    fee = dest_amount // fee_divisor (integer floor division, deterministic).
    user_receives = dest_amount - fee.

    Used by miner (to send reduced amount) and validator (to verify reduced amount).
    Both MUST use this function to guarantee identical results.
    """
    return dest_amount - dest_amount // fee_divisor
