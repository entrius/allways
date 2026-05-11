from fastapi import APIRouter, Depends
from starlette.concurrency import run_in_threadpool

from allways.swap_api.deps import AppState, get_state
from allways.swap_api.models import HealthResponse

router = APIRouter()


@router.get('/healthz', response_model=HealthResponse)
async def healthz(state: AppState = Depends(get_state)) -> HealthResponse:
    try:
        block = await run_in_threadpool(state.subtensor.get_current_block)
    except Exception:
        block = None
    return HealthResponse(ok=True, chainBlock=block, contractAddress=state.contract_address)
