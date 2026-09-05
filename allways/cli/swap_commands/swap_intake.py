"""Taker swap-intake — miner selection + on-chain amount derivation. No click, no owned RPC config.

Mirrors the contract: ``collateral_amount`` is the leg denominated in the quote's BACKING (the
bounded, collateral-backed notional) — ``backing.rs::collateral_leg_amount``, so a "sol"-backed
quote is sized against its SOL leg and a "tao"-backed one against its TAO leg, in rao. Uses the
shared ``calculate_to_amount`` so the CLI's pinned amounts agree with the miner + validator
byte-for-byte. Every launch pair has a hub leg (sol↔spoke / tao↔spoke); a spoke↔spoke pair is
rejected here. The one network-touching helper (``candidate_miners``) takes the Solana client as a
parameter, so the CLI taker path and the validator reserve engine build the same candidate set from
the same reads.
"""

from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

from allways.chains import canonical_pair, get_chain_def
from allways.constants import (
    COLLATERAL_REQUIREMENT_BPS,
    NUMERAIRE_CHAIN,
    RATE_PRECISION,
    hub_leg,
    required_collateral,
)
from allways.solana.layouts import hub_reserved_collateral
from allways.solana.pdas import BACKING_BITS
from allways.utils.rate import (
    calculate_to_amount,
    is_executable_rate,
    max_from_for_to_cap,
    normalize_rate,
    quantize_rate_fixed,
)

# Per-backing bounds, keyed by backing chain id. Each pair is in that backing asset's OWN smallest
# unit — never converted through the rate, which would smuggle a price oracle into a guard.
BoundsByBacking = Dict[str, Tuple[int, int]]


@dataclass
class IntakeAmounts:
    collateral_amount: int  # the backing's leg, in the backing's smallest units (the bounded notional)
    from_amount: int  # source leg, smallest units
    to_amount: int  # dest leg, smallest units


@dataclass
class MinerCandidate:
    miner: object  # solders Pubkey
    rate_display: str  # canonical 'dest per 1 hub' rate, display units
    # The purse answering for this offer, in the BACKING's own smallest unit: local lamport
    # collateral for "sol", the attested effective bond (rao) for an off-chain backing.
    collateral: int
    # The backing the winning QUOTE declared — one market per pair, mixed by rate (D2). A dual-purse
    # miner can appear twice on one direction; only this tells the two offers apart, and it is what a
    # bid must name to land on the right one.
    backing: str = NUMERAIRE_CHAIN


def to_smallest_units(amount: float, chain: str) -> int:
    """Display amount (e.g. 0.1 BTC) → smallest units (sat/lamport/wei). Decimal, not float: an
    18-decimal chain needs more digits than a float carries, so 1.1 pins 1100000000000000128 wei —
    over what the taker sends, and the leg never verifies. Truncating keeps the pin at or below."""
    return int(Decimal(str(amount)) * 10 ** get_chain_def(chain).decimals)


def rate_display_from_fixed(rate_fixed: int) -> str:
    """On-chain u128 fixed-point rate → canonical display string, floored to RATE_SIG_FIGS first.

    Mirrors the contract's set_quote floor and the crown ingest, so display/reserve/scoring agree
    even for quotes grandfathered from before the on-chain floor (rounding could disagree there)."""
    return normalize_rate(quantize_rate_fixed(rate_fixed) / RATE_PRECISION)


def candidate_miners(client, from_chain: str, to_chain: str) -> List[MinerCandidate]:
    """Active miners with a posted quote for this exact direction, purse attached.
    Shared by the CLI taker path and the validator reserve engine so "who is
    quotable" can never diverge between what a taker sees and what reserves.

    Inactive miners are excluded: the contract rejects a reserve against them
    (reserve_on_behalf / finalize_reservation), so a taker must never see one as
    a candidate. One MinerState read per quoted miner gives both the active gate
    and the tracked collateral in a single fetch; an off-chain-backed offer needs
    one more read for the purse the contract will actually check."""
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
        backing = getattr(q, 'collateral_chain', NUMERAIRE_CHAIN) or NUMERAIRE_CHAIN
        purse = free_purse(client, q.miner, ms, backing)
        if purse is None:
            continue  # no locked bond behind the offer — the contract would refuse the reserve
        out.append(
            MinerCandidate(
                miner=q.miner,
                rate_display=rate_display_from_fixed(q.rate),
                collateral=purse,
                backing=backing,
            )
        )
    return out


def backing_purse(client, miner, miner_state, backing: str) -> Optional[int]:
    """The purse behind an offer, in the backing's own smallest unit — the mirror of
    ``backing.rs::backing_purse``. "sol" reads the local vault ledger; anything else reads the
    quorum-written attestation, which must exist AND be locked (an unlocked bond backs nothing).
    None when there is no usable purse."""
    if backing == NUMERAIRE_CHAIN:
        return int(miner_state.collateral)
    reader = getattr(client, 'get_bond_attestation', None)
    if reader is None:
        return None
    attestation = reader(miner, backing)
    if attestation is None or not attestation.locked:
        return None
    return int(attestation.effective_balance)


def free_purse(client, miner, miner_state, backing: str) -> Optional[int]:
    """What a NEW swap can draw on: ``backing_purse`` net of collateral already obligated to in-flight
    swaps on that hub. ``finalize_reservation`` gates on the same net figure, so a candidate sized on
    the gross purse passes every pre-check here and is refused on-chain at the draw."""
    bit = BACKING_BITS.get(backing)
    purse = backing_purse(client, miner, miner_state, backing) if bit else None
    if purse is None:
        return None  # an unknown backing is one offer skipped, never a KeyError across every quote
    return max(purse - hub_reserved_collateral(miner_state, bit), 0)


def floors_from_config(cfg) -> Dict[str, int]:
    """Per-backing ACTIVATION floors off the on-chain Config — mirrors ``backing.rs::activation_floor``.
    Each is in its own backing asset's smallest unit (lamports for "sol", rao for "tao"); a floor is
    never converted through a rate, which would smuggle a price oracle into a guard."""
    return {
        NUMERAIRE_CHAIN: int(getattr(cfg, 'min_collateral', 0) or 0),
        'tao': int(getattr(cfg, 'tao_min_collateral', 0) or 0),
    }


def bounds_from_config(cfg) -> BoundsByBacking:
    """Per-backing swap bounds off the on-chain Config — mirrors ``backing.rs::swap_bounds``."""
    return {
        NUMERAIRE_CHAIN: (
            int(getattr(cfg, 'min_swap_amount', 0) or 0),
            int(getattr(cfg, 'max_swap_amount', 0) or 0),
        ),
        'tao': (
            int(getattr(cfg, 'tao_min_swap_amount', 0) or 0),
            int(getattr(cfg, 'tao_max_swap_amount', 0) or 0),
        ),
    }


def hub_bounds(bounds: BoundsByBacking, from_chain: str, to_chain: str) -> Tuple[int, int]:
    """The pair's HUB-leg swap bounds, in the hub's own smallest unit — what the rate-executability
    gates (``is_executable_rate`` / selection scalars) anchor on. (0, 0) = unset/permissive for a
    pair with no hub leg. Distinct from the per-BACKING size gate: sol↔tao is SOL-anchored here even
    when a tao-backed quote's size is gated on the TAO bounds."""
    hub = hub_leg(from_chain, to_chain)
    return bounds.get(hub, (0, 0)) if hub else (0, 0)


def _bounds_for(
    backing: str, bounds_by_backing: Optional[BoundsByBacking], min_swap: int, max_swap: int
) -> Tuple[int, int]:
    """The bounds a candidate is gated on. Without a per-backing map the scalars apply to every
    candidate — the SOL-only world, unchanged."""
    if bounds_by_backing is None:
        return min_swap, max_swap
    return bounds_by_backing.get(backing, (min_swap, max_swap))


def collateral_leg_amount(backing: str, from_chain: str, from_amount: int, to_chain: str, to_amount: int) -> int:
    """The leg denominated in ``backing`` — the amount its collateral is sized against. Mirrors
    ``backing.rs::collateral_leg_amount``: validity is "backing ∈ legs", nothing about the pair."""
    if backing == from_chain:
        return from_amount
    if backing == to_chain:
        return to_amount
    raise ValueError(f'{from_chain}->{to_chain}: no leg is denominated in the "{backing}" backing')


def compute_intake_amounts(
    from_chain: str,
    to_chain: str,
    from_amount: int,
    rate_display: str,
    backing: str = NUMERAIRE_CHAIN,
) -> IntakeAmounts:
    """Derive (collateral_amount, from_amount, to_amount) for a swap of ``from_amount`` (source smallest-units).

    ``rate_display`` is the miner's canonical 'dest per 1 hub' rate. Requires one leg to be a hub.
    ``collateral_amount`` is the ``backing``'s leg, in that asset's own units — the figure
    ``finalize_reservation`` bounds and collateralizes.
    """
    if hub_leg(from_chain, to_chain) is None:
        raise ValueError(f'{from_chain}->{to_chain}: a hub leg (sol or tao) is required (every pair is hub<->spoke)')
    canon_from, canon_to = canonical_pair(from_chain, to_chain)
    is_reverse = from_chain != canon_from
    to_amount = calculate_to_amount(
        from_amount, rate_display, is_reverse, get_chain_def(canon_to).decimals, get_chain_def(canon_from).decimals
    )
    collateral_amount = collateral_leg_amount(backing, from_chain, from_amount, to_chain, to_amount)
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


def swap_viable(
    collateral_amount: int,
    collateral: int,
    min_swap: int,
    max_swap: int,
    backing: str = NUMERAIRE_CHAIN,
) -> Tuple[bool, str]:
    """``swap_gate`` with messages denominated in the backing asset — the miner/CLI-facing phrasing.
    Every figure here is already in that asset's units; nothing is converted through the rate."""
    gate = swap_gate(collateral_amount, collateral, min_swap, max_swap)
    if not gate:
        return True, ''
    scale = 10 ** get_chain_def(backing).decimals
    unit = backing.upper()
    if gate == GATE_BELOW_MIN:
        return False, f'below min swap ({min_swap / scale:.4f} {unit})'
    if gate == GATE_ABOVE_MAX:
        return False, f'above max swap ({max_swap / scale:.4f} {unit})'
    return False, f'miner collateral too low (needs {required_collateral(collateral_amount) / scale:.4f} {unit})'


def viable_intakes(
    candidates: List[MinerCandidate],
    from_chain: str,
    to_chain: str,
    from_amount: int,
    min_swap: int,
    max_swap: int,
    bounds_by_backing: Optional[BoundsByBacking] = None,
) -> List[Tuple[MinerCandidate, IntakeAmounts]]:
    """Every candidate passing the executable-rate + viability gates, with derived amounts.
    Stable input order. The single gating path shared by auto-select and --miner.

    ``min_swap``/``max_swap`` are the pair's HUB-leg bounds (``hub_bounds``): ``is_executable_rate``
    is the crown/squat heuristic about a rate nobody can route, defined on the hub leg. The purse +
    size gate below is the per-backing one."""
    out: List[Tuple[MinerCandidate, IntakeAmounts]] = []
    for c in candidates:
        try:
            rate = float(c.rate_display)
        except (TypeError, ValueError):
            continue
        if not is_executable_rate(rate, from_chain, to_chain, min_swap, max_swap):
            continue
        try:
            amts = compute_intake_amounts(from_chain, to_chain, from_amount, c.rate_display, c.backing)
        except ValueError:
            continue  # backing not in this pair's legs — the contract would refuse it too
        if amts.to_amount <= 0:
            continue
        lo, hi = _bounds_for(c.backing, bounds_by_backing, min_swap, max_swap)
        if not swap_gate(amts.collateral_amount, c.collateral, lo, hi):
            out.append((c, amts))
    return out


def max_intake_from_amount(
    candidate: MinerCandidate,
    from_chain: str,
    to_chain: str,
    min_swap: int,
    max_swap: int,
    bounds_by_backing: Optional[BoundsByBacking] = None,
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
    lo, hi = _bounds_for(candidate.backing, bounds_by_backing, min_swap, max_swap)
    # Largest backing leg with required_collateral(leg) <= purse — exact inverse of the 1.1× floor.
    cap = ((candidate.collateral + 1) * 10_000 - 1) // COLLATERAL_REQUIREMENT_BPS
    if hi > 0:
        cap = min(cap, hi)
    if cap <= 0 or cap < lo:
        return 0
    if candidate.backing == from_chain:
        return cap  # the bounded leg IS the source
    if candidate.backing != to_chain:
        return 0
    canon_from, canon_to = canonical_pair(from_chain, to_chain)
    return max_from_for_to_cap(
        cap,
        candidate.rate_display,
        from_chain != canon_from,
        get_chain_def(canon_to).decimals,
        get_chain_def(canon_from).decimals,
    )


def _bound_phrase(bound: int, backing: str, from_chain: str, rate_display: str) -> str:
    """A bound phrased for the taker. Bounds are contract facts in the BACKING asset; takers think
    in what they're sending, so when the backing is the dest-side hub leg (canonical source — the
    rate is 'dest per 1 hub'), add the source-side figure it can convert exactly. Display only
    (float, marked ≈) — never fed back into any gate."""
    native = f'{bound / 10 ** get_chain_def(backing).decimals:.4f} {backing.upper()}'
    if backing == from_chain or backing != canonical_pair(from_chain, backing)[0]:
        return native
    src_amount = bound / 10 ** get_chain_def(backing).decimals * float(rate_display)
    return f'≈{src_amount:.6g} {from_chain.upper()} ({native} leg)'


def unviable_reason(
    candidates: List[MinerCandidate],
    from_chain: str,
    to_chain: str,
    from_amount: int,
    min_swap: int,
    max_swap: int,
    bounds_by_backing: Optional[BoundsByBacking] = None,
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
        try:
            amts = compute_intake_amounts(from_chain, to_chain, from_amount, c.rate_display, c.backing)
        except ValueError as e:
            reasons.append(str(e))
            continue
        if amts.to_amount <= 0:
            reasons.append('amount too small')
            continue
        lo, hi = _bounds_for(c.backing, bounds_by_backing, min_swap, max_swap)
        gate = swap_gate(amts.collateral_amount, c.collateral, lo, hi)
        if gate == GATE_BELOW_MIN:
            reasons.append(f'below min swap ({_bound_phrase(lo, c.backing, from_chain, c.rate_display)})')
        elif gate == GATE_ABOVE_MAX:
            reasons.append(f'above max swap ({_bound_phrase(hi, c.backing, from_chain, c.rate_display)})')
        elif gate == GATE_LOW_COLLATERAL:
            reasons.append(swap_viable(amts.collateral_amount, c.collateral, lo, hi, c.backing)[1])
    return '; '.join(dict.fromkeys(reasons)) or 'no executable quote'


def select_best_miner(
    candidates: List[MinerCandidate],
    from_chain: str,
    to_chain: str,
    from_amount: int,
    min_swap: int,
    max_swap: int,
    bounds_by_backing: Optional[BoundsByBacking] = None,
) -> Optional[Tuple[MinerCandidate, IntakeAmounts]]:
    """Among executable + viable miners, pick the one giving the user the most dest (``to_amount``).

    Backing is NOT a selection input — one market per pair, mixed by rate (D2). It breaks an exact
    tie only, toward "sol": at identical value the instant-SOL-refund guarantee is strictly better
    for the taker than a TAO reimbursement that lands shortly after the timeout. Remaining ties fall
    back to first-seen (stable input order)."""
    viable = viable_intakes(candidates, from_chain, to_chain, from_amount, min_swap, max_swap, bounds_by_backing)
    return max(viable, key=lambda p: (p[1].to_amount, p[0].backing == NUMERAIRE_CHAIN), default=None)
