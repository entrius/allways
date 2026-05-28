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


def vote_deactivate(
    client: AllwaysContractClient,
    wallet: bt.Wallet,
    miner_hotkey: str,
    label: Optional[str] = None,
) -> bool:
    """Vote to deactivate an active miner. Caller logs the outcome."""
    tag = label or f'Miner {miner_hotkey[:8]}'
    try:
        client.vote_deactivate(wallet=wallet, miner_hotkey=miner_hotkey)
        return True
    except Exception as e:
        bt.logging.error(f'{tag}: vote_deactivate failed: {e}')
        return False
