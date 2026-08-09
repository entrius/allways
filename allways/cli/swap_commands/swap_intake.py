"""Taker swap-intake — miner selection + on-chain amount derivation. No click, no owned RPC config.

Mirrors the contract: ``collateral_amount`` is the SOL leg (the bounded, collateral-backed notional). Uses the
shared ``calculate_to_amount`` so the CLI's pinned amounts agree with the miner + validator byte-for-byte.
Launch pairs always have a SOL leg (sol↔btc / sol↔tao); a pair without one is rejected here.
The one network-touching helper (``candidate_miners``) takes the Solana client as a parameter, so the
CLI taker path and the validator reserve engine build the same candidate set from the same reads.
"""

from dataclasses import dataclass
from typing import List, Optional, Tuple

from allways.chains import canonical_pair, get_chain_def
from allways.constants import COLLATERAL_REQUIREMENT_BPS, NUMERAIRE_CHAIN, RATE_PRECISION, required_collateral
from allways.utils.rate import (
    calculate_to_amount,
    is_executable_rate,
    max_from_for_to_cap,
    normalize_rate,
    quantize_rate_fixed,
)


@dataclass
class IntakeAmounts:
    collateral_amount: int  # the SOL leg, lamports (the bounded/collateralized notional)
    from_amount: int  # source leg, smallest units
    to_amount: int  # dest leg, smallest units


@dataclass
class MinerCandidate:
    miner: object  # solders Pubkey
    rate_display: str  # canonical 'dest per 1 SOL' rate, display units
    collateral: int  # miner collateral, lamports
    # The backing the winning QUOTE declared — one market per pair, mixed by rate (D2). A dual-purse
    # miner can appear twice on one direction; only this tells the two offers apart, and it is what a
    # bid must name to land on the right one.
    backing: str = NUMERAIRE_CHAIN


def to_smallest_units(amount: float, chain: str) -> int:
    """Display amount (e.g. 0.1 BTC) → smallest units (sat/lamport/rao)."""
    return int(round(amount * 10 ** get_chain_def(chain).decimals))


def rate_display_from_fixed(rate_fixed: int) -> str:
    """On-chain u128 fixed-point rate → canonical display string, floored to RATE_SIG_FIGS first.

    Mirrors the contract's set_quote floor and the crown ingest, so display/reserve/scoring agree
    even for quotes grandfathered from before the on-chain floor (rounding could disagree there)."""
    return normalize_rate(quantize_rate_fixed(rate_fixed) / RATE_PRECISION)


def candidate_miners(client, from_chain: str, to_chain: str) -> List[MinerCandidate]:
    """Active miners with a posted quote for this exact direction, collateral attached.
    Shared by the CLI taker path and the validator reserve engine so "who is
    quotable" can never diverge between what a taker sees and what reserves.

    Inactive miners are excluded: the contract rejects a reserve against them
    (reserve_on_behalf / finalize_reservation), so a taker must never see one as
    a candidate. One MinerState read per quoted miner gives both the active gate
    and the tracked collateral in a single fetch."""
    out: List[MinerCandidate] = []
    # One state read per distinct miner: a dual-purse miner can appear twice on a direction now, and
    # its `active` flag and collateral are the same for both offers.
    states: dict = {}
    for _pk, q in client.get_all('MinerQuote'):
        if q.from_chain != from_chain or q.to_chain != to_chain:
            continue
        key = str(q.miner)
        if key not in states:
            states[key] = client.get_miner_state(q.miner)
        ms = states[key]
        if ms is None or not ms.active:
            continue
        out.append(
            MinerCandidate(
                miner=q.miner,
                rate_display=rate_display_from_fixed(q.rate),
                collateral=int(ms.collateral),
                backing=getattr(q, 'collateral_chain', NUMERAIRE_CHAIN) or NUMERAIRE_CHAIN,
            )
        )
    return out


def compute_intake_amounts(from_chain: str, to_chain: str, from_amount: int, rate_display: str) -> IntakeAmounts:
    """Derive (collateral_amount, from_amount, to_amount) for a swap of ``from_amount`` (source smallest-units).

    ``rate_display`` is the miner's canonical 'dest per 1 SOL' rate. Requires one leg to be SOL.
    """
    if NUMERAIRE_CHAIN not in (from_chain, to_chain):
        raise ValueError(
            f'{from_chain}->{to_chain}: a {NUMERAIRE_CHAIN} leg is required (every launch pair is hub<->spoke)'
        )
    canon_from, canon_to = canonical_pair(from_chain, to_chain)
    is_reverse = from_chain != canon_from
    to_amount = calculate_to_amount(
        from_amount, rate_display, is_reverse, get_chain_def(canon_to).decimals, get_chain_def(canon_from).decimals
    )
    collateral_amount = from_amount if from_chain == NUMERAIRE_CHAIN else to_amount
    return IntakeAmounts(collateral_amount=collateral_amount, from_amount=from_amount, to_amount=to_amount)


# swap_gate verdicts — the contract's open_or_request guards as data, formatted by the caller.
GATE_BELOW_MIN = 'below_min'
GATE_ABOVE_MAX = 'above_max'
GATE_LOW_COLLATERAL = 'low_collateral'


def swap_gate(collateral_amount: int, collateral: int, min_swap: int, max_swap: int) -> str:
    """Pre-flight the contract's open_or_request guards (bounds + collateral). '' when viable.

    Bounds are SOL lamports (0 = unset sentinel → that side not enforced)."""
    if min_swap > 0 and collateral_amount < min_swap:
        return GATE_BELOW_MIN
    if max_swap > 0 and collateral_amount > max_swap:
        return GATE_ABOVE_MAX
    if collateral < required_collateral(collateral_amount):
        return GATE_LOW_COLLATERAL
    return ''


def swap_viable(collateral_amount: int, collateral: int, min_swap: int, max_swap: int) -> Tuple[bool, str]:
    """``swap_gate`` with SOL-denominated messages — the miner/CLI-facing phrasing."""
    gate = swap_gate(collateral_amount, collateral, min_swap, max_swap)
    if gate == GATE_BELOW_MIN:
        return False, f'below min swap ({min_swap / 1e9:.4f} SOL)'
    if gate == GATE_ABOVE_MAX:
        return False, f'above max swap ({max_swap / 1e9:.4f} SOL)'
    if gate == GATE_LOW_COLLATERAL:
        return False, f'miner collateral too low (needs {required_collateral(collateral_amount) / 1e9:.4f} SOL)'
    return True, ''


def viable_intakes(
    candidates: List[MinerCandidate],
    from_chain: str,
    to_chain: str,
    from_amount: int,
    min_swap: int,
    max_swap: int,
) -> List[Tuple[MinerCandidate, IntakeAmounts]]:
    """Every candidate passing the executable-rate + viability gates, with derived amounts.
    Stable input order. The single gating path shared by auto-select and --miner."""
    out: List[Tuple[MinerCandidate, IntakeAmounts]] = []
    for c in candidates:
        try:
            rate = float(c.rate_display)
        except (TypeError, ValueError):
            continue
        if not is_executable_rate(rate, from_chain, to_chain, min_swap, max_swap):
            continue
        amts = compute_intake_amounts(from_chain, to_chain, from_amount, c.rate_display)
        if amts.to_amount <= 0:
            continue
        if not swap_gate(amts.collateral_amount, c.collateral, min_swap, max_swap):
            out.append((c, amts))
    return out


def max_intake_from_amount(
    candidate: MinerCandidate,
    from_chain: str,
    to_chain: str,
    min_swap: int,
    max_swap: int,
) -> int:
    """Largest source amount (smallest units) this candidate can execute right now — the depth behind
    its rate. The same gates as ``viable_intakes``, solved for size instead of checked at one: the
    collateral requirement inverted exactly, clamped by ``max_swap``, 0 when even ``min_swap`` doesn't fit."""
    try:
        rate = float(candidate.rate_display)
    except (TypeError, ValueError):
        return 0
    if not is_executable_rate(rate, from_chain, to_chain, min_swap, max_swap):
        return 0
    # Largest SOL leg with required_collateral(leg) <= collateral — exact inverse of the 1.1× floor.
    cap = ((candidate.collateral + 1) * 10_000 - 1) // COLLATERAL_REQUIREMENT_BPS
    if max_swap > 0:
        cap = min(cap, max_swap)
    if cap <= 0 or cap < min_swap:
        return 0
    if from_chain == NUMERAIRE_CHAIN:
        return cap
    canon_from, canon_to = canonical_pair(from_chain, to_chain)
    return max_from_for_to_cap(
        cap, candidate.rate_display, True, get_chain_def(canon_to).decimals, get_chain_def(canon_from).decimals
    )


def _bound_in_source(bound_lamports: int, from_chain: str, rate_display: str) -> str:
    """A SOL-leg bound phrased in the taker's source asset at this candidate's canonical rate.

    Bounds are contract facts in SOL lamports; takers think in what they're sending. Display
    conversion only (float, marked ≈) — never fed back into any gate."""
    sol = f'{bound_lamports / 1e9:.4f} {NUMERAIRE_CHAIN.upper()}'
    if from_chain == NUMERAIRE_CHAIN:
        return sol
    src_amount = bound_lamports / 1e9 * float(rate_display)
    return f'≈{src_amount:.6g} {from_chain.upper()} ({sol} leg)'


def unviable_reason(
    candidates: List[MinerCandidate],
    from_chain: str,
    to_chain: str,
    from_amount: int,
    min_swap: int,
    max_swap: int,
) -> str:
    """Why nothing was quotable — the gates of ``viable_intakes``, spelled out for the taker.

    Only meaningful when ``select_best_miner`` returned None for the same inputs."""
    if not candidates:
        return 'no miner offers this direction'
    reasons: List[str] = []
    for c in candidates:
        try:
            rate = float(c.rate_display)
        except (TypeError, ValueError):
            continue
        if not is_executable_rate(rate, from_chain, to_chain, min_swap, max_swap):
            reasons.append('rate not executable')
            continue
        amts = compute_intake_amounts(from_chain, to_chain, from_amount, c.rate_display)
        if amts.to_amount <= 0:
            reasons.append('amount too small')
            continue
        gate = swap_gate(amts.collateral_amount, c.collateral, min_swap, max_swap)
        if gate == GATE_BELOW_MIN:
            reasons.append(f'below min swap ({_bound_in_source(min_swap, from_chain, c.rate_display)})')
        elif gate == GATE_ABOVE_MAX:
            reasons.append(f'above max swap ({_bound_in_source(max_swap, from_chain, c.rate_display)})')
        elif gate == GATE_LOW_COLLATERAL:
            needed = required_collateral(amts.collateral_amount)
            reasons.append(f'miner collateral too low (needs {needed / 1e9:.4f} SOL)')
    return '; '.join(dict.fromkeys(reasons)) or 'no executable quote'


def select_best_miner(
    candidates: List[MinerCandidate],
    from_chain: str,
    to_chain: str,
    from_amount: int,
    min_swap: int,
    max_swap: int,
) -> Optional[Tuple[MinerCandidate, IntakeAmounts]]:
    """Among executable + viable miners, pick the one giving the user the most dest (``to_amount``).

    None if no miner qualifies. Ties broken by first-seen (stable input order)."""
    viable = viable_intakes(candidates, from_chain, to_chain, from_amount, min_swap, max_swap)
    return max(viable, key=lambda p: p[1].to_amount, default=None)
