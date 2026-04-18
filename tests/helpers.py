"""Shared test factories and constants used across multiple test modules."""

from pathlib import Path

from allways.classes import Swap, SwapStatus

METADATA_PATH = Path(__file__).parent.parent / 'allways' / 'metadata' / 'allways_swap_manager.json'


def make_swap(
    swap_id: int = 1,
    miner_hotkey: str = 'miner',
    timeout_block: int = 500,
    rate: str = '345',
    miner_from: str = 'bc1q-miner',
) -> Swap:
    """Swap factory covering both validator and miner test contexts."""
    return Swap(
        id=swap_id,
        user_hotkey='user',
        miner_hotkey=miner_hotkey,
        from_chain='btc',
        to_chain='tao',
        from_amount=1_000_000,
        to_amount=345_000_000,
        tao_amount=345_000_000,
        user_from_address='bc1q-user',
        user_to_address='5user',
        miner_from_address=miner_from,
        rate=rate,
        status=SwapStatus.ACTIVE,
        initiated_block=100,
        timeout_block=timeout_block,
    )
