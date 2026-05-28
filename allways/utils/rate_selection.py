"""Pure best-rate miner selection — used by both CLI and swap-api.

Filters miners that quote the requested direction (bilateral matching via
``find_matching_miners``), drops anyone ineligible (inactive, already busy,
no collateral), and ranks by direction-aware rate. The CLI and HTTP paths
must agree on the winner; this is the one place that decision lives.
"""

from dataclasses import dataclass
from typing import List

from allways.chains import canonical_pair
from allways.classes import MinerPair
from allways.contract_client import AllwaysContractClient, ContractError


@dataclass
class EligibleMiner:
    pair: MinerPair
    collateral_rao: int


def rank_pairs_by_rate(pairs: List[MinerPair], from_chain: str, to_chain: str) -> List[MinerPair]:
    """Best-rate-first ordering for the requested swap direction.

    Rates are stored canonically (dest-per-source in alphabetical order). When
    the requested direction is the reverse, higher rate is worse for the user,
    so the sort reverses. Mirrors the CLI's inline sort in ``swap_now``.
    """
    canon_from, _ = canonical_pair(from_chain, to_chain)
    canon_is_reverse = from_chain != canon_from
    return sorted(pairs, key=lambda p: p.rate, reverse=not canon_is_reverse)


def filter_eligible(
    client: AllwaysContractClient,
    pairs: List[MinerPair],
) -> List[EligibleMiner]:
    """Drop miners that the contract would reject — active flag off, busy, or no collateral."""
    eligible: List[EligibleMiner] = []
    for pair in pairs:
        try:
            if not client.get_miner_active_flag(pair.hotkey):
                continue
            if client.get_miner_has_active_swap(pair.hotkey):
                continue
            collateral = client.get_miner_collateral(pair.hotkey)
        except ContractError:
            continue
        if collateral <= 0:
            continue
        eligible.append(EligibleMiner(pair=pair, collateral_rao=collateral))
    return eligible
