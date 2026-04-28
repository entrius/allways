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
    from_amount: int
    to_amount: int
    from_address: str
    from_address_proof: str
    block_anchor: int
    from_chain: str = ''  # User's source chain (for bilateral pair support)
    to_chain: str = ''  # User's dest chain

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
    from_tx_hash: str
    from_tx_proof: str
    from_address: str
    # Block the source tx was included in. When > 0, the validator uses
    # it as a ±3 block hint so verification is O(1) instead of scanning
    # the last 150 blocks — needed when the user post-tx's late or the
    # underlying node has pruned state past the default scan window.
    from_tx_block: int = 0
    to_address: str = ''
    from_chain: str = ''  # User's source chain (for bilateral pair support)
    to_chain: str = ''  # User's dest chain

    # Response fields (validator fills)
    accepted: Optional[bool] = None
    rejection_reason: Optional[str] = None
