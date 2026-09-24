"""Shared rate calculation — single source of truth for to_amount math."""

import math
from decimal import Decimal
from typing import TYPE_CHECKING, Optional, Tuple

from allways.chains import canonical_pair, get_chain_def
from allways.constants import RATE_PRECISION, RATE_SIG_FIGS, hub_leg

if TYPE_CHECKING:
    from allways.solana.client import SolanaSwap


def normalize_rate(rate: float) -> str:
    """Canonical RATE_SIG_FIGS-precision string for any committed rate."""
    return f'{rate:.{RATE_SIG_FIGS}g}'


def quantize_rate_fixed(rate_fixed: int) -> int:
    """Floor a fixed-point rate (display × RATE_PRECISION) to RATE_SIG_FIGS significant figures.

    Integer-exact mirror of the contract's ``quantize_rate_sig_figs`` (set_quote.rs): both floor the
    same way, so the CLI previews and posts exactly what the chain will store and the validator's crown
    ingest agrees byte-for-byte. Floor, not round — a rate can never gain a tick by rounding up.
    """
    if rate_fixed <= 0:
        return 0
    digits = len(str(rate_fixed))
    if digits <= RATE_SIG_FIGS:
        return rate_fixed
    pow10 = 10 ** (digits - RATE_SIG_FIGS)
    return rate_fixed // pow10 * pow10


def quantize_rate_display(rate: float) -> float:
    """Display-domain convenience: the RATE_SIG_FIGS-floored rate as a float, for CLI previews."""
    return quantize_rate_fixed(int(rate * RATE_PRECISION)) / RATE_PRECISION


def canonical_rate(from_chain: str, to_chain: str, directional: float) -> float:
    """Directional 'to per 1 from' of the (from → to) leg → canonical 'dest per 1 canonical source'.

    Inverse of ``directional_rate`` — the input boundary where human-typed rates become the one
    number the chain stores. 0 (direction not offered) passes through."""
    if directional == 0 or from_chain == canonical_pair(from_chain, to_chain)[0]:
        return directional
    return 1 / directional


def directional_rate(from_chain: str, to_chain: str, rate_display: str) -> str:
    """Directional 'to per 1 from' rate for display. Stored quotes are canonical 'dest per 1 canonical
    source', which reads backwards for a reverse direction (BTC→SOL stored 0.0021 really means ~476
    SOL per BTC). Return the reciprocal there so `amount × shown-rate ≈ you receive` always reconciles."""
    try:
        r = float(rate_display)
    except (TypeError, ValueError):
        return rate_display
    if r > 0 and from_chain != canonical_pair(from_chain, to_chain)[0]:
        r = 1.0 / r
    return f'{r:.8g}'


def calculate_to_amount(
    from_amount: int,
    rate,
    is_reverse: bool,
    to_decimals: int,
    from_decimals: int,
) -> int:
    """Calculate to_amount from from_amount and committed rate using fixed-point arithmetic.

    ``rate`` accepts either the on-chain u128 fixed-point int (rate × RATE_PRECISION) OR a display string
    ('0.0021', '345') — both resolve to the same rate_fixed, so on-chain (int) and CLI/commitment (str)
    callers agree byte-for-byte. Used by miner (fulfillment), validator (verification), and CLI (display).

    Args:
        from_amount: Amount in smallest units (sat, rao, wei, etc.)
        rate: u128 fixed-point int, or a 'canonical dest per 1 canonical source' display string
        is_reverse: True when swap direction is opposite of canonical order
        to_decimals: Decimal places for canonical dest chain (e.g. 9 for TAO)
        from_decimals: Decimal places for canonical source chain (e.g. 8 for BTC)
    """
    rate_fixed = int(rate) if isinstance(rate, int) else int(Decimal(rate) * RATE_PRECISION)
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


def max_from_for_to_cap(
    to_cap: int,
    rate,
    is_reverse: bool,
    to_decimals: int,
    from_decimals: int,
) -> int:
    """Largest from_amount whose ``calculate_to_amount`` stays <= ``to_cap`` — its exact floor-division
    inverse, branch for branch, so the pair can never disagree. 0 when no positive amount fits."""
    rate_fixed = int(rate) if isinstance(rate, int) else int(Decimal(rate) * RATE_PRECISION)
    if rate_fixed == 0 or to_cap <= 0:
        return 0
    decimal_diff = to_decimals - from_decimals
    if is_reverse:
        if decimal_diff >= 0:
            num, den = RATE_PRECISION, rate_fixed * 10**decimal_diff
        else:
            num, den = RATE_PRECISION * 10 ** (-decimal_diff), rate_fixed
    else:
        if decimal_diff >= 0:
            num, den = rate_fixed * 10**decimal_diff, RATE_PRECISION
        else:
            num, den = rate_fixed, RATE_PRECISION * 10 ** (-decimal_diff)
    # to = from × num // den  ⇒  largest from with from × num < (to_cap + 1) × den
    return max(0, ((to_cap + 1) * den - 1) // num)


def expected_swap_amounts(swap: 'SolanaSwap', fee_divisor: int) -> Tuple[int, int]:
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
        get_chain_def(canon_to).decimals,
        get_chain_def(canon_from).decimals,
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


def bound_units_per_other_unit(rate: float, from_chain: str, to_chain: str, bounded: str, unit_value: float) -> float:
    """Bound units per smallest unit of the pair's other leg. ``rate`` is canonical (other per 1 anchor);
    ``unit_value`` values one smallest unit of ``bounded`` in the bounds' unit (1.0 unless a declared leg)."""
    other = to_chain if bounded == from_chain else from_chain
    bounded_per_other = 1.0 / rate if bounded == hub_leg(from_chain, to_chain) else rate
    return bounded_per_other * 10 ** (get_chain_def(bounded).decimals - get_chain_def(other).decimals) * unit_value


def is_executable_rate(
    rate: float,
    from_chain: str,
    to_chain: str,
    min_swap_hub: int,
    max_swap_hub: int,
    bounded_chain: Optional[str] = None,
    unit_value: float = 1.0,
) -> bool:
    """True iff the rate is fundably routable in its declared direction.

    Crown-eligibility gate against rates that no user can route. ``rate`` is the CANONICAL
    number every caller stores and feeds — spoke per 1 hub — in BOTH directions. The on-chain
    swap bounds constrain the pair's **hub leg** (``collateral_amount``: SOL lamports under the
    SOL bounds for a SOL-hub pair, rao under the TAO bounds for a TAO-hub pair — callers pass
    the hub leg's own bounds, see ``hub_bounds``). Routable means a spoke source >= the spoke's
    ``min_onchain_amount`` maps a hub leg into ``[min, max]``.

    Both directions reduce to the same spoke-side question. X→hub: an absurdly LOW canonical
    rate (the crown-squat — lowest wins that sort) makes even 1 smallest-unit of spoke
    overshoot ``max``, so nothing routes. hub→X: the hub leg is the source and trivially fits,
    but the symmetric spoke-side check keeps the executable spectrum bounded.

    ``bounded_chain`` names the leg the bounds constrain (default: the hub leg); a declared alpha leg
    passes its price as ``unit_value`` so the bounds stay in the backing's unit.

    A bound at ``0`` is the contract's "unset" sentinel and disables that side; both at 0 →
    permissive. Pairs with no hub leg have no bound to enforce → permissive.
    """
    if not math.isfinite(rate) or rate <= 0:
        return False
    if min_swap_hub <= 0 and max_swap_hub <= 0:
        return True
    bounded = bounded_chain or hub_leg(from_chain, to_chain)
    if bounded is None:
        # No hub leg → no bounded asset to enforce against.
        return True
    # Is there an amount of the other leg, fundable on-chain (>= its min_onchain_amount), whose
    # bounded leg lands in bounds? bound_units = other_units × denom.
    other = get_chain_def(to_chain if bounded == from_chain else from_chain)
    denom = bound_units_per_other_unit(rate, from_chain, to_chain, bounded, unit_value)
    if not math.isfinite(denom) or denom <= 0:
        return False
    # Floor at the other chain's dust/existential minimum: a rate whose only in-bounds amount is
    # below it (e.g. 1 sat) is unfundable, so unexecutable.
    lo = max(1, min_swap_hub) / denom
    if not math.isfinite(lo):
        # The rate is beyond float routing math (e.g. float-max canonical) — sentinel.
        return False
    min_other = max(other.min_onchain_amount, math.ceil(lo))
    if max_swap_hub <= 0:
        return True
    return min_other <= math.floor(max_swap_hub / denom)


def min_executable_hub_leg(
    rate: float,
    from_chain: str,
    to_chain: str,
    min_swap_hub: int,
    max_swap_hub: int,
    bounded_chain: Optional[str] = None,
    unit_value: float = 1.0,
) -> int:
    """Smallest bounded leg (in the bounds' unit) the rate produces among in-band fundable swaps.

    Shares band math with is_executable_rate; the bounded leg is the collateral leg
    (``collateral_amount``). Returns 0 when no in-band fundable swap exists (rate unexecutable)
    — caller treats as "no constraint".
    """
    if not is_executable_rate(rate, from_chain, to_chain, min_swap_hub, max_swap_hub, bounded_chain, unit_value):
        return 0
    bounded = bounded_chain or hub_leg(from_chain, to_chain)
    if from_chain == bounded:
        return max(math.ceil(get_chain_def(bounded).min_onchain_amount * unit_value), max(0, min_swap_hub))
    if to_chain == bounded:
        src = get_chain_def(from_chain)
        denom = bound_units_per_other_unit(rate, from_chain, to_chain, bounded, unit_value)
        if not math.isfinite(denom) or denom <= 0:
            return 0
        min_source = max(src.min_onchain_amount, math.ceil(max(1, min_swap_hub) / denom))
        return int(min_source * denom)
    return 0
