"""Synapse definitions for multi-validator consensus communication.

Three synapse types for actor↔validator communication:
- MinerActivateSynapse: miner requests activation via dendrite
- SwapReserveSynapse: user requests miner reservation via dendrite-lite
- SwapConfirmSynapse: user confirms source tx sent via dendrite-lite
"""

from typing import Optional

import bittensor as bt


class MinerActivateSynapse(bt.Synapse):
    """Miner broadcasts activation request to all validators.

    Validators verify the miner's on-chain commitment exists and collateral
    meets minimum, then vote_activate on the contract.
    """

    # Request fields (miner fills)
    hotkey: str
    signature: str
    message: str

    # Response fields (validator fills)
    accepted: Optional[bool] = None
    rejection_reason: Optional[str] = None


class SwapReserveSynapse(bt.Synapse):
    """User broadcasts swap reservation request to all validators via dendrite-lite.

    Validators verify the source address proof, check miner eligibility,
    compute the request hash, and vote_reserve on the contract.
    """

    # Request fields (user fills)
    miner_hotkey: str
    tao_amount: int
    source_amount: int
    dest_amount: int
    source_address: str
    source_address_proof: str
    block_anchor: int
    source_chain: str = ''  # User's source chain (for bilateral pair support)
    dest_chain: str = ''  # User's dest chain

    # Response fields (validator fills)
    accepted: Optional[bool] = None
    rejection_reason: Optional[str] = None


class SwapConfirmSynapse(bt.Synapse):
    """User broadcasts swap confirmation after sending source funds.

    Validators verify the source transaction on-chain, check that
    the source address matches the reservation, and vote_initiate
    on the contract.
    """

    # Request fields (user fills)
    reservation_id: str
    source_tx_hash: str
    source_tx_proof: str
    source_address: str
    dest_address: str = ''
    source_chain: str = ''  # User's source chain (for bilateral pair support)
    dest_chain: str = ''  # User's dest chain

    # Response fields (validator fills)
    accepted: Optional[bool] = None
    rejection_reason: Optional[str] = None
