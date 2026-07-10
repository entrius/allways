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
    """User (or a routing offering) asks a validator to enter a miner's reservation pool on their behalf.

    The validator computes the dest amount from the pinned/live rate and submits open_or_request as the
    router. All amounts are SOL-numeraire (``from_amount`` is source smallest-units); the taker identity is
    the user's Solana ``user_pubkey``, pinned into the reservation.
    """

    # Request fields (caller fills)
    miner_hotkey: str
    from_chain: str
    to_chain: str
    user_pubkey: str  # Solana taker/payout identity (base58)
    user_from_addr: str  # source-chain address the user sends from
    user_to_addr: str  # dest-chain address the user receives at
    from_amount: int  # source leg, smallest units

    # Response fields (validator fills)
    accepted: Optional[bool] = None
    rejection_reason: Optional[str] = None
    pool_closes_at: Optional[int] = None  # unix secs the on-chain window closes (caller times its follow-up)


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
