"""Thin wrappers around the swap-vote extrinsics. Both return False on any
error; the caller decides whether to retry or escalate."""

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
