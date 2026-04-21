"""Thin wrappers around the swap-vote extrinsics. confirm_swap and
timeout_swap return False on any error; extend_swap_timeout propagates so
the caller can distinguish ``AlreadyVoted`` from real failures."""

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
    """Vote to extend a FULFILLED swap's deadline. Lets exceptions propagate
    so the caller can distinguish ``AlreadyVoted`` from real errors."""
    client.vote_extend_timeout(wallet=wallet, swap_id=swap_id)
    return True
