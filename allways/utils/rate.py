"""Shared rate calculation — single source of truth for to_amount math."""

from decimal import Decimal
from typing import Tuple

from allways.chains import canonical_pair, get_chain
from allways.classes import Swap
from allways.constants import RATE_PRECISION


def calculate_to_amount(
    from_amount: int,
    rate: str,
    is_reverse: bool,
    to_decimals: int,
    from_decimals: int,
) -> int:
    """Calculate to_amount from from_amount and committed rate using fixed-point arithmetic.

    Rate is 'canonical_dest per 1 canonical_source' in display units (e.g. 345 means 1 BTC = 345 TAO).
    Uses Decimal for rate conversion to avoid IEEE 754 float rounding artifacts.
    The rate parameter should be the raw string from the miner's commitment.

    Used by miner (fulfillment), validator (verification), and CLI (display).
    All three MUST use this function to guarantee identical results.

    Args:
        from_amount: Amount in smallest units (sat, rao, wei, etc.)
        rate: Canonical dest per 1 canonical source as a string (e.g. '345')
        is_reverse: True when swap direction is opposite of canonical order
        to_decimals: Decimal places for canonical dest chain (e.g. 9 for TAO)
        from_decimals: Decimal places for canonical source chain (e.g. 8 for BTC)
    """
    rate_fixed = int(Decimal(rate) * RATE_PRECISION)
    if rate_fixed == 0:
        return 0

    decimal_diff = to_decimals - from_decimals

    if is_reverse:
        # Reverse direction: divide by rate, adjust for decimals
        if decimal_diff >= 0:
            return from_amount * RATE_PRECISION // (rate_fixed * 10**decimal_diff)
        else:
            return from_amount * RATE_PRECISION * 10 ** (-decimal_diff) // rate_fixed
    else:
        # Forward direction: multiply by rate, adjust for decimals
        if decimal_diff >= 0:
            return from_amount * rate_fixed * 10**decimal_diff // RATE_PRECISION
        else:
            return from_amount * rate_fixed // (RATE_PRECISION * 10 ** (-decimal_diff))


def expected_swap_amounts(swap: Swap, fee_divisor: int) -> Tuple[int, int]:
    """Compute expected to_amount and fee-adjusted user_receives from a swap's on-chain fields.

    Single source of truth used by both miner (fulfillment) and validator (verification).
    Returns (raw_dest_amount, user_receives) or (0, 0) if the rate is invalid.
    """
    canon_from, canon_to = canonical_pair(swap.from_chain, swap.to_chain)
    is_reverse = swap.from_chain != canon_from

    to_amount = calculate_to_amount(
        swap.from_amount,
        swap.rate,
        is_reverse,
        get_chain(canon_to).decimals,
        get_chain(canon_from).decimals,
    )
    if to_amount == 0:
        return 0, 0

    user_receives = apply_fee_deduction(to_amount, fee_divisor)
    return to_amount, user_receives


def apply_fee_deduction(to_amount: int, fee_divisor: int) -> int:
    """Deduct fee from to_amount. Returns the amount the user receives.

    fee = to_amount // fee_divisor (integer floor division, deterministic).
    user_receives = to_amount - fee.

    Used by miner (to send reduced amount) and validator (to verify reduced amount).
    Both MUST use this function to guarantee identical results.
    """
    return to_amount - to_amount // fee_divisor
