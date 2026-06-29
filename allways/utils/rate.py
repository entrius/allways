"""Shared rate calculation — single source of truth for to_amount math."""

import math
from decimal import Decimal
from typing import Tuple

from allways.chains import canonical_pair, get_chain
from allways.classes import Swap
from allways.constants import NUMERAIRE_CHAIN, RATE_PRECISION, RATE_SIG_FIGS


def normalize_rate(rate: float) -> str:
    """Canonical RATE_SIG_FIGS-precision string for any committed rate."""
    return f'{rate:.{RATE_SIG_FIGS}g}'


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


def quote_within_slippage(quoted: int, recomputed: int, slippage_bps: int) -> bool:
    """True if `recomputed` is no more than `slippage_bps` below `quoted`.

    One-sided (DEX 'minimum received'): a favorable move — recomputed >= quoted —
    always passes. Pure integer math for determinism across validators. When
    slippage_bps >= 10_000 the threshold is non-positive, so it always passes.
    """
    if quoted <= 0 or recomputed <= 0:
        return False
    if recomputed >= quoted:
        return True
    return recomputed * 10_000 >= quoted * (10_000 - slippage_bps)


def is_executable_rate(
    rate: float,
    from_chain: str,
    to_chain: str,
    min_swap_lamports: int,
    max_swap_lamports: int,
) -> bool:
    """True iff the rate is fundably routable in its declared direction.

    Crown-eligibility gate against rates that no user can route. The on-chain swap bounds
    (``min_swap_amount``/``max_swap_amount``) constrain the **SOL leg** (``sol_amount``), so SOL is the
    bounded asset and ``min_swap_lamports``/``max_swap_lamports`` are SOL lamports. Routable means a source >= the
    source chain's ``min_onchain_amount`` maps a SOL leg into ``[min, max]``.

    * X→SOL: high-side rates — even the smallest fundable source maps above ``max``, so no fundable
      source produces an in-bounds SOL leg.
    * SOL→X: the SOL leg IS the source, so it trivially fits any bounds, but absurd rates imply absurd
      destinations. Caught by the symmetric check on ``1/rate``: if the inverse direction has no fundable
      source, the original rate is at an extreme of the executable spectrum.

    A bound at ``0`` is the contract's "unset" sentinel and disables that side; both at 0 → permissive.
    Non-SOL pairs (no SOL leg) have no bound to enforce → permissive.
    """
    if not math.isfinite(rate) or rate <= 0:
        return False
    if min_swap_lamports <= 0 and max_swap_lamports <= 0:
        return True

    def _has_integer_routable_source(forward_rate: float, src_chain: str) -> bool:
        # For a "src → sol" direction at ``forward_rate`` (sol per src), is there an src amount that is
        # fundable on-chain (>= the chain's min_onchain_amount) whose SOL leg lands in bounds?
        src = get_chain(src_chain)
        decimal_factor = 10 ** (get_chain(NUMERAIRE_CHAIN).decimals - src.decimals)
        denom = forward_rate * decimal_factor
        if not math.isfinite(denom) or denom <= 0:
            # rate × decimal_factor overflowed → smallest positive integer source already maps above max.
            return False
        # Floor at the source chain's dust/existential minimum: a rate whose only in-bounds source is
        # below it (e.g. 1 sat) is unfundable, so unexecutable.
        min_source = max(src.min_onchain_amount, math.ceil(max(1, min_swap_lamports) / denom))
        if max_swap_lamports <= 0:
            return True
        max_source = math.floor(max_swap_lamports / denom)
        return min_source <= max_source

    if to_chain == NUMERAIRE_CHAIN:
        # Forward into SOL: sol_lamports = source_units × rate × 10**(sol_dec - src_dec).
        return _has_integer_routable_source(rate, from_chain)

    if from_chain == NUMERAIRE_CHAIN and to_chain != NUMERAIRE_CHAIN:
        # Reverse out of SOL: the SOL leg is the source itself, so any positive lamport in [min, max] is
        # trivially in bounds — but absurd rates imply destinations so large no rational user would route,
        # and the miner can post them just to win the lowest-rate-wins crown. Treat ``1/rate`` as a
        # ``to_chain → sol`` rate and apply the same integer-routability check by symmetry.
        inverse = 1.0 / rate
        if not math.isfinite(inverse) or inverse <= 0:
            return False
        return _has_integer_routable_source(inverse, to_chain)

    # Non-SOL pairs have no SOL-leg bound to enforce.
    return True


def min_executable_sol_leg(
    rate: float,
    from_chain: str,
    to_chain: str,
    min_swap_lamports: int,
    max_swap_lamports: int,
) -> int:
    """Smallest SOL leg (lamports) the rate produces among in-band fundable swaps.

    Shares band math with is_executable_rate; SOL is the bounded asset (``sol_amount``). Returns 0 when
    no in-band fundable swap exists (rate unexecutable) — caller treats as "no constraint".
    """
    if not is_executable_rate(rate, from_chain, to_chain, min_swap_lamports, max_swap_lamports):
        return 0
    if from_chain == NUMERAIRE_CHAIN:
        return max(get_chain(NUMERAIRE_CHAIN).min_onchain_amount, max(0, min_swap_lamports))
    if to_chain == NUMERAIRE_CHAIN:
        src = get_chain(from_chain)
        decimal_factor = 10 ** (get_chain(NUMERAIRE_CHAIN).decimals - src.decimals)
        denom = rate * decimal_factor
        if not math.isfinite(denom) or denom <= 0:
            return 0
        min_source = max(src.min_onchain_amount, math.ceil(max(1, min_swap_lamports) / denom))
        return int(min_source * denom)
    return 0
