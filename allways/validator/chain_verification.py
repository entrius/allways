"""Verifies both sides of a swap using chain providers and on-chain swap data."""

import asyncio
from typing import Dict, Optional

import bittensor as bt

from allways.chain_providers.base import ChainProvider
from allways.classes import Swap
from allways.utils.rate import expected_swap_amounts


class SwapVerifier:
    """Verifies swap transactions on both source and destination chains.

    Rate and miner source address are stored on the swap struct at initiation,
    so verification is self-contained — no commitment lookup needed.
    """

    def __init__(
        self,
        chain_providers: Dict[str, ChainProvider],
        subtensor: bt.Subtensor,
        netuid: int,
        metagraph: Optional['bt.Metagraph'] = None,
        fee_divisor: int = 100,
    ):
        self.providers = chain_providers
        self.subtensor = subtensor
        self.netuid = netuid
        self.metagraph = metagraph
        self.fee_divisor = fee_divisor
        self._last_logged_confs: Dict[str, int] = {}  # swap_id:chain -> confs

    def _verify_tx(
        self, swap: Swap, chain: str, tx_hash: str, expected_recipient: str, expected_amount: int, block_hint: int = 0
    ) -> bool:
        """Verify a confirmed transaction on a specific chain."""
        provider = self.providers.get(chain)
        if not provider:
            bt.logging.warning(f'Swap {swap.id}: no provider for chain {chain}')
            return False

        if not tx_hash:
            bt.logging.debug(f'Swap {swap.id}: empty tx_hash for {chain}, skipping verification')
            return False

        try:
            tx_info = provider.verify_transaction(
                tx_hash=tx_hash,
                expected_recipient=expected_recipient,
                expected_amount=expected_amount,
                block_hint=block_hint,
            )
            if tx_info is None:
                bt.logging.debug(
                    f'Swap {swap.id}: verify_transaction returned None on {chain} '
                    f'(tx={tx_hash[:16]}... block_hint={block_hint})'
                )
            elif not tx_info.confirmed:
                log_key = f'{swap.id}:{chain}'
                prev_confs = self._last_logged_confs.get(log_key)
                if prev_confs != tx_info.confirmations:
                    self._last_logged_confs[log_key] = tx_info.confirmations
                    bt.logging.debug(
                        f'Swap {swap.id}: tx found but not confirmed on {chain} '
                        f'(confs={tx_info.confirmations} tx={tx_hash[:16]}... '
                        f'addr={expected_recipient[:16]}... expected={expected_amount})'
                    )
            return tx_info is not None and tx_info.confirmed
        except Exception as e:
            bt.logging.error(f'Swap {swap.id}: verification error on {chain}: {e}')
            return False

    async def is_swap_complete(self, swap: Swap) -> bool:
        """Verify rate, dest_amount, user send, and miner fulfillment.

        Rate and miner source address are read directly from the swap struct
        (snapshotted at initiation), so this works regardless of miner registration.
        """
        if not swap.rate or not swap.miner_source_address:
            bt.logging.warning(f'Swap {swap.id}: missing rate or miner_source_address on swap struct')
            return False

        _, expected_user_receives = expected_swap_amounts(swap, self.fee_divisor)
        if expected_user_receives == 0:
            bt.logging.warning(f'Swap {swap.id}: rate produces 0 dest_amount after fees')
            return False

        if int(swap.dest_amount) != expected_user_receives:
            bt.logging.warning(
                f'Swap {swap.id}: dest_amount mismatch — expected {expected_user_receives}, '
                f'contract has {swap.dest_amount}'
            )
            return False

        # Verify sequentially — parallel threads cause WebSocket contention
        # with the API server thread sharing the same substrate connection
        source_ok = await asyncio.to_thread(
            self._verify_tx,
            swap,
            swap.source_chain,
            swap.source_tx_hash,
            swap.miner_source_address,
            swap.source_amount,
            swap.source_tx_block,
        )
        dest_ok = await asyncio.to_thread(
            self._verify_tx,
            swap,
            swap.dest_chain,
            swap.dest_tx_hash,
            swap.user_dest_address,
            expected_user_receives,
            swap.dest_tx_block,
        )

        return source_ok and dest_ok
