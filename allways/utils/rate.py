"""Shared rate calculation — single source of truth for to_amount math."""

import math
from decimal import Decimal
from typing import TYPE_CHECKING, Tuple

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


def is_executable_rate(
    rate: float,
    from_chain: str,
    to_chain: str,
    min_swap_hub: int,
    max_swap_hub: int,
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

    A bound at ``0`` is the contract's "unset" sentinel and disables that side; both at 0 →
    permissive. Pairs with no hub leg have no bound to enforce → permissive.
    """
    if not math.isfinite(rate) or rate <= 0:
        return False
    if min_swap_hub <= 0 and max_swap_hub <= 0:
        return True
    hub = hub_leg(from_chain, to_chain)
    if hub is None:
        # No hub leg → no bounded asset to enforce against.
        return True

    def _has_integer_routable_source(hub_per_source: float, src_chain: str) -> bool:
        # For a "src → hub" leg: is there a src amount that is fundable on-chain (>= the
        # chain's min_onchain_amount) whose hub leg lands in bounds?
        # hub_units = source_units × hub_per_source × 10**(hub_dec - src_dec).
        src = get_chain_def(src_chain)
        decimal_factor = 10 ** (get_chain_def(hub).decimals - src.decimals)
        denom = hub_per_source * decimal_factor
        if not math.isfinite(denom) or denom <= 0:
            return False
        # Floor at the source chain's dust/existential minimum: a rate whose only in-bounds source is
        # below it (e.g. 1 sat) is unfundable, so unexecutable.
        lo = max(1, min_swap_hub) / denom
        if not math.isfinite(lo):
            # The rate is beyond float routing math (e.g. float-max canonical) — sentinel.
            return False
        min_source = max(src.min_onchain_amount, math.ceil(lo))
        if max_swap_hub <= 0:
            return True
        max_source = math.floor(max_swap_hub / denom)
        return min_source <= max_source

    # Callers feed canonical spoke-per-hub; the source-side check wants hub-per-spoke — invert.
    # (The X→SOL branch used to pass the canonical rate through uninverted: the F1 orientation
    # defect, which made any real spoke dust floor unroutable at rate² error.)
    if to_chain == hub:
        return _has_integer_routable_source(1.0 / rate, from_chain)
    return _has_integer_routable_source(1.0 / rate, to_chain)


def min_executable_hub_leg(
    rate: float,
    from_chain: str,
    to_chain: str,
    min_swap_hub: int,
    max_swap_hub: int,
) -> int:
    """Smallest hub leg (hub smallest-units) the rate produces among in-band fundable swaps.

    Shares band math with is_executable_rate; the pair's hub leg is the bounded asset
    (``collateral_amount``). Returns 0 when no in-band fundable swap exists (rate unexecutable)
    — caller treats as "no constraint".
    """
    if not is_executable_rate(rate, from_chain, to_chain, min_swap_hub, max_swap_hub):
        return 0
    hub = hub_leg(from_chain, to_chain)
    if from_chain == hub:
        return max(get_chain_def(hub).min_onchain_amount, max(0, min_swap_hub))
    if to_chain == hub:
        src = get_chain_def(from_chain)
        decimal_factor = 10 ** (get_chain_def(hub).decimals - src.decimals)
        # Same orientation as the gate: canonical spoke-per-hub in, hub-per-spoke for the math.
        denom = (1.0 / rate) * decimal_factor
        if not math.isfinite(denom) or denom <= 0:
            return 0
        min_source = max(src.min_onchain_amount, math.ceil(max(1, min_swap_hub) / denom))
        return int(min_source * denom)
    return 0
