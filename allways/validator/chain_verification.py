"""Verifies both sides of a swap using chain providers and on-chain swap data."""

import asyncio
from typing import Dict, Optional

import bittensor as bt

from allways.chain_providers.base import ChainProvider, ProviderUnreachableError, TransactionInfo
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
        self.last_logged_confs: Dict[str, int] = {}  # swap_id:chain -> confs

    def verify_tx(
        self,
        swap: Swap,
        chain: str,
        tx_hash: str,
        expected_recipient: str,
        expected_amount: int,
        block_hint: int = 0,
        expected_sender: str = '',
    ) -> bool:
        """Verify a confirmed transaction on a specific chain.

        Defers tx lookup, amount, and sender checks to the provider's
        ``verify_transaction`` so the defense lives in one place shared with
        the miner and axon flows. Keeps the rate-limited confirmations debug
        log here because it's specific to the validator polling loop.
        """
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
                expected_sender=expected_sender or None,
            )
            if tx_info is None:
                bt.logging.debug(
                    f'Swap {swap.id}: verify_transaction returned None on {chain} '
                    f'(tx={tx_hash[:16]}... block_hint={block_hint})'
                )
                return False
            if not tx_info.confirmed:
                self.log_confs_progress(swap.id, chain, tx_hash, tx_info, expected_recipient, expected_amount)
                return False
            return True
        except ProviderUnreachableError:
            raise
        except Exception as e:
            bt.logging.error(f'Swap {swap.id}: verification error on {chain}: {e}')
            return False

    def log_confs_progress(
        self,
        swap_id: int,
        chain: str,
        tx_hash: str,
        tx_info: TransactionInfo,
        expected_recipient: str,
        expected_amount: int,
    ) -> None:
        """Rate-limited debug log for confirmations progress on unconfirmed txs."""
        log_key = f'{swap_id}:{chain}'
        if self.last_logged_confs.get(log_key) == tx_info.confirmations:
            return
        self.last_logged_confs[log_key] = tx_info.confirmations
        bt.logging.debug(
            f'Swap {swap_id}: tx found but not confirmed on {chain} '
            f'(confs={tx_info.confirmations} tx={tx_hash[:16]}... '
            f'addr={expected_recipient[:16]}... expected={expected_amount})'
        )

    async def verify_miner_fulfillment(self, swap: Swap) -> bool:
        """Verify rate, to_amount, user send, and miner fulfillment.

        Rate and miner source address are read directly from the swap struct
        (snapshotted at initiation), so this works regardless of miner registration.
        """
        if not swap.rate or not swap.miner_from_address:
            bt.logging.warning(f'Swap {swap.id}: missing rate or miner_from_address on swap struct')
            return False

        _, expected_user_receives = expected_swap_amounts(swap, self.fee_divisor)
        if expected_user_receives == 0:
            bt.logging.warning(f'Swap {swap.id}: rate produces 0 to_amount after fees')
            return False

        if int(swap.to_amount) != expected_user_receives:
            bt.logging.warning(
                f'Swap {swap.id}: to_amount mismatch — expected {expected_user_receives}, contract has {swap.to_amount}'
            )
            return False

        # Verify sequentially — parallel threads cause WebSocket contention
        # with the API server thread sharing the same substrate connection
        source_ok = await asyncio.to_thread(
            self.verify_tx,
            swap,
            swap.from_chain,
            swap.from_tx_hash,
            swap.miner_from_address,
            swap.from_amount,
            swap.from_tx_block,
        )
        dest_ok = await asyncio.to_thread(
            self.verify_tx,
            swap,
            swap.to_chain,
            swap.to_tx_hash,
            swap.user_to_address,
            expected_user_receives,
            swap.to_tx_block,
            swap.miner_to_address,
        )

        return source_ok and dest_ok
