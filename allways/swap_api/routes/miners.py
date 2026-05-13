"""Live miner reads for the swap form. Always hits chain, never DB."""

from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query
from starlette.concurrency import run_in_threadpool

from allways.chains import canonical_pair, get_chain
from allways.cli.swap_commands.helpers import find_matching_miners
from allways.commitments import read_miner_commitments
from allways.contract_client import ContractError
from allways.swap_api.deps import AppState, get_state
from allways.swap_api.models import BestMinerResponse, MinerSummary
from allways.utils.rate import calculate_to_amount
from allways.utils.rate_selection import filter_eligible, rank_pairs_by_rate

router = APIRouter()


@router.get('/miners', response_model=List[MinerSummary])
async def list_miners(
    from_: str = Query(..., alias='from'),
    to: str = Query(...),
    state: AppState = Depends(get_state),
) -> List[MinerSummary]:
    """Live miner list for a direction. Excludes commitments that don't quote this pair."""
    from_ = from_.lower()
    to = to.lower()
    pairs = await run_in_threadpool(read_miner_commitments, state.subtensor, state.netuid)
    matching = find_matching_miners(pairs, from_, to)

    summaries: List[MinerSummary] = []
    for p in matching:
        try:
            is_active = await run_in_threadpool(state.contract_client.get_miner_active_flag, p.hotkey)
            has_swap = await run_in_threadpool(state.contract_client.get_miner_has_active_swap, p.hotkey)
            collateral = await run_in_threadpool(state.contract_client.get_miner_collateral, p.hotkey)
        except ContractError:
            continue
        summaries.append(
            MinerSummary(
                hotkey=p.hotkey,
                rate=p.rate_str,
                collateralRao=collateral,
                isActive=is_active,
                hasActiveSwap=has_swap,
            )
        )
    return summaries


@router.get('/miners/best', response_model=BestMinerResponse)
async def best_miner(
    from_: str = Query(..., alias='from'),
    to: str = Query(...),
    amount: int = Query(..., gt=0),
    state: AppState = Depends(get_state),
) -> BestMinerResponse:
    """Cheapest quote for ``from_chain → to_chain`` at ``amount`` (smallest unit)."""
    from_ = from_.lower()
    to = to.lower()
    if from_ == to:
        raise HTTPException(status_code=400, detail='from and to chains must differ')

    pairs = await run_in_threadpool(read_miner_commitments, state.subtensor, state.netuid)
    matching = find_matching_miners(pairs, from_, to)
    if not matching:
        raise HTTPException(status_code=404, detail=f'no miners quote {from_} → {to}')

    ranked = rank_pairs_by_rate(matching, from_, to)
    eligible = await run_in_threadpool(filter_eligible, state.contract_client, ranked)
    if not eligible:
        raise HTTPException(status_code=404, detail='no eligible miner — all busy or uncollateralized')

    best = eligible[0]
    canon_from, canon_to = canonical_pair(from_, to)
    is_reverse = from_ != canon_from
    expected_out = calculate_to_amount(
        amount,
        best.pair.rate_str,
        is_reverse,
        get_chain(canon_to).decimals,
        get_chain(canon_from).decimals,
    )

    try:
        fresh_block = await run_in_threadpool(state.subtensor.get_current_block)
    except Exception:
        fresh_block = 0

    return BestMinerResponse(
        minerHotkey=best.pair.hotkey,
        rate=best.pair.rate_str,
        expectedOut=expected_out,
        reservationCapacity=best.collateral_rao,
        sourceAddress=best.pair.from_address,
        freshAsOf=fresh_block,
    )
