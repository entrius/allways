"""Verifies both sides of a swap using chain providers and on-chain swap data."""

import asyncio
from typing import Any, Dict, Optional, Set

import bittensor as bt

from allways.chain_providers.base import ChainProvider, ProviderUnreachableError, TransactionInfo
from allways.classes import Swap
from allways.utils.logging import log_on_change
from allways.utils.logging import swap_label as _swap_label
from allways.utils.rate import expected_swap_amounts


class SwapVerifier:
    """Verifies swap transactions on both source and destination chains.

    Rate and miner source address are stored on the swap struct at initiation,
    so verification is self-contained — no commitment lookup needed.

    Dest-tx replay defense: snapshots the dest chain's tip on first sighting
    of a swap and rejects later dest txs whose block predates the snapshot —
    a validator-side stand-in for a contract-level ``used_to_tx`` mirror.
    """

    def __init__(
        self,
        chain_providers: Dict[str, ChainProvider],
        fee_divisor: int = 100,
        metagraph: Optional[Any] = None,
        state_store: Optional[Any] = None,
    ):
        self.providers = chain_providers
        self.fee_divisor = fee_divisor
        self.metagraph = metagraph
        self.state_store = state_store
        self.last_logged_confs: Dict[str, int] = {}  # swap_id:chain -> confs
        self.source_verified_ids: Set[int] = set()  # source tx is final once confirmed
        # Hydrate from sqlite so a validator restart mid-swap keeps the
        # original (early) snapshot — a fresh snapshot taken after the honest
        # dest tx already landed would reject the payout as a replay.
        if state_store is not None:
            self.dest_tip_at_init: Dict[int, int] = state_store.load_dest_tip_snapshots()
        else:
            self.dest_tip_at_init = {}  # swap_id -> dest tip at first sighting (non-TAO only)

    def observe_initiation(self, swap: Swap, current_block: int = 0) -> None:
        """Snapshot the dest chain's tip on first sighting of a non-TAO swap.
        Idempotent; fails open with a one-time warning on RPC error.

        ``current_block`` is the substrate block at the moment of snapshot;
        recorded on disk only, for debugging late-snapshot incidents.
        """
        if swap.to_chain == 'tao' or swap.id in self.dest_tip_at_init:
            return
        provider = self.providers.get(swap.to_chain)
        if provider is None:
            return
        # Broad except (vs verify_tx's re-raise of ProviderUnreachableError):
        # this runs inside a forward-loop iteration and must not break it.
        try:
            tip = provider.get_current_block_height()
        except Exception:
            tip = None
        if tip and tip > 0:
            # If fulfillment landed before a late snapshot (RPC retry), cap the
            # lower bound so an honest payout is not rejected as a replay.
            if swap.to_tx_block and swap.to_tx_block > 0:
                tip = min(tip, swap.to_tx_block)
            self.dest_tip_at_init[swap.id] = tip
            if self.state_store is not None:
                # Same fail-open discipline as the RPC call above: a sqlite
                # write failure must not break the forward loop. Persistence
                # is best-effort; the in-memory snapshot remains the source
                # of truth for this validator run.
                try:
                    self.state_store.upsert_dest_tip_snapshot(
                        swap_id=swap.id,
                        dest_chain=swap.to_chain,
                        tip=tip,
                        recorded_at=current_block,
                    )
                except Exception as e:
                    bt.logging.warning(f'{self._label(swap)}: failed to persist dest-tip snapshot: {e}')
        else:
            log_on_change(
                f'snapshot_unavailable:{swap.id}',
                True,
                f'{self._label(swap)}: dest-tip snapshot failed on {swap.to_chain} — replay defense off until retry',
            )

    def prune_to_active(self, active_ids: Set[int]) -> None:
        """Drop per-swap state for swaps no longer being tracked."""
        self.dest_tip_at_init = {sid: v for sid, v in self.dest_tip_at_init.items() if sid in active_ids}
        self.source_verified_ids &= active_ids
        if self.state_store is not None:
            try:
                self.state_store.prune_dest_tip_snapshots(active_ids)
            except Exception as e:
                bt.logging.warning(f'failed to prune persisted dest-tip snapshots: {e}')

    def _label(self, swap: Swap) -> str:
        return _swap_label(swap, self.metagraph)

    def verify_tx(
        self,
        swap: Swap,
        chain: str,
        tx_hash: str,
        expected_recipient: str,
        expected_amount: int,
        block_hint: int = 0,
        expected_sender: str = '',
    ) -> Optional[TransactionInfo]:
        """Verify a confirmed transaction on a specific chain.

        Defers tx lookup, amount, and sender checks to the provider's
        ``verify_transaction`` so the defense lives in one place shared with
        the miner and axon flows. Keeps the rate-limited confirmations debug
        log here because it's specific to the validator polling loop.
        """
        provider = self.providers.get(chain)
        if not provider:
            bt.logging.warning(f'{self._label(swap)}: no provider for chain {chain}')
            return None

        if not tx_hash:
            bt.logging.debug(f'{self._label(swap)}: empty tx_hash for {chain}, skipping verification')
            return None

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
                    f'{self._label(swap)}: verify_transaction returned None on {chain} '
                    f'(tx={tx_hash[:16]}... block_hint={block_hint})'
                )
                return None
            if not tx_info.confirmed:
                self.log_confs_progress(swap, chain, tx_hash, tx_info, expected_recipient, expected_amount)
                return None
            return tx_info
        except ProviderUnreachableError:
            raise
        except Exception as e:
            bt.logging.error(f'{self._label(swap)}: verification error on {chain}: {e}')
            return None

    def is_dest_tx_fresh(self, swap: Swap, dest_info: TransactionInfo) -> bool:
        """Reject a dest tx mined before the swap was initiated (replay defense)."""
        if dest_info.block_number is None:
            return True
        lower = swap.initiated_block if swap.to_chain == 'tao' else self.dest_tip_at_init.get(swap.id)
        if lower is None:
            return True  # fail-open; observe_initiation already logged
        if dest_info.block_number < lower:
            # TAO lower bound is the swap's initiated_block; non-TAO is the
            # dest-chain tip snapshot taken at first sighting. The label
            # matters when debugging late-snapshot incidents — the bound
            # crossed is not always the initiation block.
            bound_label = 'initiated_block' if swap.to_chain == 'tao' else 'dest_tip_at_init'
            bt.logging.warning(
                f'{self._label(swap)}: dest tx at block {dest_info.block_number} < '
                f'{bound_label} {lower} — rejecting as replay (tx={swap.to_tx_hash[:16]}...)'
            )
            return False
        return True

    def log_confs_progress(
        self,
        swap: Swap,
        chain: str,
        tx_hash: str,
        tx_info: TransactionInfo,
        expected_recipient: str,
        expected_amount: int,
    ) -> None:
        """Rate-limited debug log for confirmations progress on unconfirmed txs."""
        log_key = f'{swap.id}:{chain}'
        if self.last_logged_confs.get(log_key) == tx_info.confirmations:
            return
        self.last_logged_confs[log_key] = tx_info.confirmations
        bt.logging.debug(
            f'{self._label(swap)}: tx found but not confirmed on {chain} '
            f'(confs={tx_info.confirmations} tx={tx_hash[:16]}... '
            f'addr={expected_recipient[:16]}... expected={expected_amount})'
        )

    async def verify_miner_fulfillment(self, swap: Swap) -> bool:
        """Verify the user funded the swap and the miner delivered the rate-derived amount.

        Rate and miner source address are read directly from the swap struct
        (snapshotted at initiation), so settlement keys off the rate pinned at
        reservation rather than a live commitment the miner could move.
        """
        if not swap.rate or not swap.miner_from_address:
            bt.logging.warning(f'{self._label(swap)}: missing rate or miner_from_address on swap struct')
            return False

        _, expected_user_receives = expected_swap_amounts(swap, self.fee_divisor)
        if expected_user_receives == 0:
            bt.logging.warning(f'{self._label(swap)}: rate produces 0 to_amount after fees')
            return False

        # Sequential — parallel threads contend on the shared substrate WS.
        if swap.id in self.source_verified_ids:
            source_ok = True
        else:
            source_info = await asyncio.to_thread(
                self.verify_tx,
                swap,
                swap.from_chain,
                swap.from_tx_hash,
                swap.miner_from_address,
                swap.from_amount,
                swap.from_tx_block,
            )
            source_ok = source_info is not None
            if source_ok:
                self.source_verified_ids.add(swap.id)

        dest_info = await asyncio.to_thread(
            self.verify_tx,
            swap,
            swap.to_chain,
            swap.to_tx_hash,
            swap.user_to_address,
            expected_user_receives,
            swap.to_tx_block,
            swap.miner_to_address,
        )
        dest_ok = dest_info is not None and self.is_dest_tx_fresh(swap, dest_info)

        if source_ok != dest_ok:
            log_on_change(
                f'partial:{swap.id}',
                (source_ok, dest_ok),
                f'{self._label(swap)}: partial verification (source={source_ok}, dest={dest_ok}) — '
                f'{"dest" if source_ok else "source"} side blocking confirm',
            )

        return source_ok and dest_ok
