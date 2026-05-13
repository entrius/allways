from fastapi import APIRouter

from allways.chains import SUPPORTED_CHAINS
from allways.swap_api.models import ChainInfo, ChainsResponse

router = APIRouter()


@router.get('/chains', response_model=ChainsResponse)
async def list_chains() -> ChainsResponse:
    chain_ids = list(SUPPORTED_CHAINS.keys())
    return ChainsResponse(
        chains=[
            ChainInfo(id=c.id, name=c.name, decimals=c.decimals, native_unit=c.native_unit)
            for c in SUPPORTED_CHAINS.values()
        ],
        pairs=[(src, dst) for src in chain_ids for dst in chain_ids if src != dst],
    )
