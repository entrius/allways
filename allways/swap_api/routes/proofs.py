from fastapi import APIRouter

from allways.swap_api.models import ProofMessage
from allways.utils.proofs import reserve_proof_message, swap_proof_message

router = APIRouter()


@router.get('/proofs/reserve', response_model=ProofMessage)
async def reserve_proof(address: str, block: int) -> ProofMessage:
    return ProofMessage(message=reserve_proof_message(address, block))


@router.get('/proofs/confirm', response_model=ProofMessage)
async def confirm_proof(txHash: str) -> ProofMessage:
    return ProofMessage(message=swap_proof_message(txHash))
