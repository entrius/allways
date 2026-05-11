"""POST /reserve and POST /confirm — the only mutating endpoints."""

import asyncio
import io
import time
from dataclasses import dataclass
from typing import Awaitable, Callable, List, Optional, TypeVar

import bittensor as bt
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse
from rich.console import Console
from starlette.concurrency import run_in_threadpool

from allways.cli.dendrite_lite import broadcast_synapse_async, discover_validators
from allways.cli.validator_rejections import RejectionInfo, render_and_aggregate
from allways.commitments import get_commitment, parse_commitment_data
from allways.contract_client import ContractError
from allways.swap_api.deps import AppState, get_state
from allways.swap_api.models import (
    ConfirmRequest,
    ConfirmResponse,
    RateChangedError,
    ReserveRequest,
    ReserveResponse,
)
from allways.synapses import SwapConfirmSynapse, SwapReserveSynapse

router = APIRouter()

T = TypeVar('T')


@dataclass
class _LiveQuote:
    rate_str: str
    miner_from_address: str


def _aggregate(responses: list) -> RejectionInfo:
    """Compute the same RejectionInfo the CLI sees, but with no console output."""
    return render_and_aggregate(Console(file=io.StringIO(), record=False, no_color=True), responses, label='V')


async def _read_live_quote(
    state: AppState,
    miner_hotkey: str,
    from_chain: str,
    to_chain: str,
) -> Optional[_LiveQuote]:
    """Re-read this miner's commitment once and project it onto the requested direction."""
    raw = await run_in_threadpool(get_commitment, state.subtensor, state.netuid, miner_hotkey)
    if not raw:
        return None
    pair = parse_commitment_data(raw, uid=0, hotkey=miner_hotkey)
    if pair is None:
        return None
    from_chain = from_chain.lower()
    to_chain = to_chain.lower()
    if {pair.from_chain, pair.to_chain} != {from_chain, to_chain}:
        return None
    _, rate_str = pair.get_rate_for_direction(from_chain)
    miner_from = pair.from_address if pair.from_chain == from_chain else pair.to_address
    return _LiveQuote(rate_str=rate_str, miner_from_address=miner_from)


async def _await_quorum(
    state: AppState,
    probe: Callable[[], Awaitable[T]],
    is_done: Callable[[T], bool],
) -> Optional[T]:
    """Generic poll-with-deadline. Returns the first probe result that satisfies is_done, or None."""
    deadline = time.monotonic() + state.quorum_timeout_s
    while time.monotonic() < deadline:
        try:
            value = await probe()
            if is_done(value):
                return value
        except ContractError:
            pass
        await asyncio.sleep(state.quorum_poll_interval_s)
    return None


def _discover(state: AppState) -> List[bt.AxonInfo]:
    return discover_validators(state.subtensor, state.netuid, contract_client=state.contract_client)


@router.post('/reserve')
async def reserve(req: ReserveRequest, state: AppState = Depends(get_state)):
    quote = await _read_live_quote(state, req.minerHotkey, req.fromChain, req.toChain)
    if quote is None:
        raise HTTPException(status_code=404, detail='miner does not quote this pair')
    if quote.rate_str != req.expectedRate:
        # Zero-tolerance — see spec §4 rate-drift policy.
        return JSONResponse(
            status_code=409,
            content=RateChangedError(expected=req.expectedRate, actual=quote.rate_str).model_dump(),
        )

    axons = await run_in_threadpool(_discover, state)
    if not axons:
        raise HTTPException(status_code=503, detail='no validators reachable on metagraph')

    synapse = SwapReserveSynapse(
        miner_hotkey=req.minerHotkey,
        tao_amount=req.taoAmount,
        from_amount=req.fromAmount,
        to_amount=req.toAmount,
        from_address=req.fromAddress,
        from_address_proof=req.fromAddressProof,
        block_anchor=req.blockAnchor,
        from_chain=req.fromChain,
        to_chain=req.toChain,
    )

    responses = await broadcast_synapse_async(state.ephemeral_wallet, axons, synapse, timeout=state.quorum_timeout_s)
    info = _aggregate(responses)
    if info.accepted == 0:
        raise HTTPException(status_code=502, detail=info.headline or 'validators rejected reservation')

    async def probe_reserved_until() -> int:
        return await run_in_threadpool(state.contract_client.get_miner_reserved_until, req.minerHotkey)

    reserved_until = await _await_quorum(
        state,
        probe_reserved_until,
        is_done=lambda block: block > req.blockAnchor,
    )
    if reserved_until is None:
        raise HTTPException(status_code=504, detail='quorum did not land on-chain within timeout')

    try:
        reservation = await run_in_threadpool(state.contract_client.get_reservation, req.minerHotkey)
    except ContractError as e:
        raise HTTPException(status_code=502, detail=f'reservation read failed: {e}') from e
    if reservation is None:
        # Reserved_until advanced but the row vanished — almost certainly someone
        # else's reservation already replaced ours. Fail loudly so the UI re-quotes.
        raise HTTPException(status_code=504, detail='reservation lost mid-flow — re-quote and retry')

    return ReserveResponse(
        requestHash=reservation.hash,
        reservedUntilBlock=reserved_until,
        minerSourceAddress=quote.miner_from_address,
        minerHotkey=req.minerHotkey,
    )


@router.post('/confirm', response_model=ConfirmResponse)
async def confirm(req: ConfirmRequest, state: AppState = Depends(get_state)) -> ConfirmResponse:
    axons = await run_in_threadpool(_discover, state)
    if not axons:
        raise HTTPException(status_code=503, detail='no validators reachable on metagraph')

    miner_hotkey = req.minerHotkey

    synapse = SwapConfirmSynapse(
        reservation_id=miner_hotkey,
        from_tx_hash=req.fromTxHash,
        from_tx_proof=req.fromTxProof,
        from_address=req.fromAddress,
        from_tx_block=req.fromTxBlock,
        to_address=req.toAddress,
        from_chain=req.fromChain,
        to_chain=req.toChain,
    )

    responses = await broadcast_synapse_async(state.ephemeral_wallet, axons, synapse, timeout=state.quorum_timeout_s)
    info = _aggregate(responses)
    if info.accepted == 0:
        return ConfirmResponse(accepted=False, rejection=info.headline or 'validators rejected confirmation')

    async def probe_swap_id() -> Optional[int]:
        if not await run_in_threadpool(state.contract_client.get_miner_has_active_swap, miner_hotkey):
            return None
        active = await run_in_threadpool(state.contract_client.get_miner_active_swaps, miner_hotkey)
        # Contract guarantees at most one active swap per miner.
        return active[0].id if active else None

    swap_id = await _await_quorum(state, probe_swap_id, is_done=lambda sid: sid is not None)
    if swap_id is None:
        return ConfirmResponse(accepted=True, rejection='swap not yet on-chain — validators still confirming')
    return ConfirmResponse(accepted=True, swapId=swap_id)
