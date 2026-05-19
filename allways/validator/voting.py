"""Thin wrappers around the swap-vote extrinsics. Both return False on any
error; the caller decides whether to retry or escalate."""

from typing import Optional

import bittensor as bt

from allways.contract_client import AllwaysContractClient


def confirm_swap(
    client: AllwaysContractClient,
    wallet: bt.Wallet,
    swap_id: int,
    label: Optional[str] = None,
) -> bool:
    """Vote to confirm a FULFILLED swap as COMPLETED. Caller logs the outcome."""
    tag = label or f'Swap #{swap_id}'
    try:
        client.confirm_swap(wallet=wallet, swap_id=swap_id)
        return True
    except Exception as e:
        bt.logging.error(f'{tag}: confirm_swap vote failed: {e}')
        return False


def timeout_swap(
    client: AllwaysContractClient,
    wallet: bt.Wallet,
    swap_id: int,
    label: Optional[str] = None,
) -> bool:
    """Vote to time out a swap past its deadline. Caller logs the outcome."""
    tag = label or f'Swap #{swap_id}'
    try:
        client.timeout_swap(wallet=wallet, swap_id=swap_id)
        return True
    except Exception as e:
        bt.logging.error(f'{tag}: timeout_swap vote failed: {e}')
        return False
