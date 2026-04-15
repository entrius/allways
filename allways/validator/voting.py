"""Thin wrappers around the 3 vote extrinsics a validator sends on swaps.

All three functions return ``bool`` — True on success, False on any error
(contract rejection, RPC failure, etc.). The caller logs the outcome and
uses the bool to decide whether to advance local state.

Keep these as module-level functions, not a class — they're pure
transformations of (client, wallet, swap_id) → bool with no retained state.
"""

import bittensor as bt

from allways.contract_client import AllwaysContractClient


def confirm_swap(client: AllwaysContractClient, wallet: bt.Wallet, swap_id: int) -> bool:
    """Vote to confirm a FULFILLED swap as COMPLETED."""
    bt.logging.info(f'Confirming swap {swap_id}')
    try:
        client.confirm_swap(wallet=wallet, swap_id=swap_id)
        return True
    except Exception as e:
        bt.logging.error(f'Confirm swap failed for swap {swap_id}: {e}')
        return False


def timeout_swap(client: AllwaysContractClient, wallet: bt.Wallet, swap_id: int) -> bool:
    """Vote to time out a swap past its deadline."""
    bt.logging.info(f'Timing out swap {swap_id}')
    try:
        client.timeout_swap(wallet=wallet, swap_id=swap_id)
        return True
    except Exception as e:
        bt.logging.error(f'Timeout swap failed for swap {swap_id}: {e}')
        return False


def extend_swap_timeout(client: AllwaysContractClient, wallet: bt.Wallet, swap_id: int) -> bool:
    """Vote to extend a FULFILLED swap's deadline when the dest tx needs
    more confirmations before timeout fires.

    Unlike confirm/timeout, this function lets exceptions propagate so
    callers can distinguish expected rejections (``AlreadyVoted``,
    ``ContractReverted``) from real errors. Returning True here means the
    extrinsic was accepted, not that the swap is actually extended.
    """
    client.vote_extend_timeout(wallet=wallet, swap_id=swap_id)
    return True
