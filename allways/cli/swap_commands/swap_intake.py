"""Taker swap-intake — miner selection + on-chain amount derivation. No click, no owned RPC config.

Mirrors the contract: ``collateral_amount`` is the SOL leg (the bounded, collateral-backed notional). Uses the
shared ``calculate_to_amount`` so the CLI's pinned amounts agree with the miner + validator byte-for-byte.
Launch pairs always have a SOL leg (sol↔btc / sol↔tao); a pair without one is rejected here.
The one network-touching helper (``candidate_miners``) takes the Solana client as a parameter, so the
CLI taker path and the validator reserve engine build the same candidate set from the same reads.
"""

from dataclasses import dataclass
from typing import List, Optional, Tuple

from allways.chains import canonical_pair, get_chain
from allways.constants import NUMERAIRE_CHAIN, RATE_PRECISION, required_collateral
from allways.utils.rate import calculate_to_amount, is_executable_rate, normalize_rate


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


def to_smallest_units(amount: float, chain: str) -> int:
    """Display amount (e.g. 0.1 BTC) → smallest units (sat/lamport/rao)."""
    return int(round(amount * 10 ** get_chain(chain).decimals))


def rate_display_from_fixed(rate_fixed: int) -> str:
    """On-chain u128 fixed-point rate → canonical display string (matches normalize_rate)."""
    return normalize_rate(rate_fixed / RATE_PRECISION)


def candidate_miners(client, from_chain: str, to_chain: str) -> List[MinerCandidate]:
    """All miners with a posted quote for this exact direction, collateral attached.
    Shared by the CLI taker path and the validator reserve engine so "who is
    quotable" can never diverge between what a taker sees and what reserves."""
    out: List[MinerCandidate] = []
    for _pk, q in client.get_all('MinerQuote'):
        if q.from_chain != from_chain or q.to_chain != to_chain:
            continue
        collateral = client.get_collateral_lamports(q.miner) or 0
        out.append(MinerCandidate(miner=q.miner, rate_display=rate_display_from_fixed(q.rate), collateral=collateral))
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
        from_amount, rate_display, is_reverse, get_chain(canon_to).decimals, get_chain(canon_from).decimals
    )
    collateral_amount = from_amount if from_chain == NUMERAIRE_CHAIN else to_amount
    return IntakeAmounts(collateral_amount=collateral_amount, from_amount=from_amount, to_amount=to_amount)


def swap_viable(collateral_amount: int, collateral: int, min_swap: int, max_swap: int) -> Tuple[bool, str]:
    """Pre-flight the contract's open_or_request guards (bounds + collateral). Reason empty on success.

    Bounds are SOL lamports (0 = unset sentinel → that side not enforced)."""
    if min_swap > 0 and collateral_amount < min_swap:
        return False, f'below min swap ({min_swap / 1e9:.4f} SOL)'
    if max_swap > 0 and collateral_amount > max_swap:
        return False, f'above max swap ({max_swap / 1e9:.4f} SOL)'
    needed = required_collateral(collateral_amount)
    if collateral < needed:
        return False, f'miner collateral too low (needs {needed / 1e9:.4f} SOL)'
    return True, ''


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
    best: Optional[Tuple[MinerCandidate, IntakeAmounts]] = None
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
        ok, _ = swap_viable(amts.collateral_amount, c.collateral, min_swap, max_swap)
        if not ok:
            continue
        if best is None or amts.to_amount > best[1].to_amount:
            best = (c, amts)
    return best
