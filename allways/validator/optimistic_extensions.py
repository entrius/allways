"""Optimistic extension watcher.

Replaces the ``vote_extend_reservation`` / ``vote_extend_timeout`` consensus
flows with single-validator propose / challenge / finalize. See
OPTIMISTIC_EXTENSION_REDESIGN.md (local planning doc) for full background.

This module provides only the per-decision primitives — picking the right
moments to invoke them lives in the validator forward loop (slice #8).
Each method takes the inputs it needs as parameters so the class is fully
mockable in unit tests without spinning up a chain.
"""

from typing import Optional

import bittensor as bt

from allways.chains import compute_extension_target
from allways.classes import PendingExtension
from allways.constants import EXTENSION_BUCKET_BLOCKS
from allways.contract_client import (
    AllwaysContractClient,
    ContractError,
    is_contract_rejection,
)


class OptimisticExtensionWatcher:
    """Decision logic for the optimistic extension flow.

    Holds no per-miner state — every method takes the inputs it needs and
    queries the contract for current pending state. This keeps the class
    stateless across calls and trivially testable; the forward loop is
    responsible for *when* to invoke each method (e.g. only when a pending
    confirm is near its reservation deadline).
    """

    def __init__(self, contract_client: AllwaysContractClient, wallet: 'bt.Wallet'):
        self.contract_client = contract_client
        self.wallet = wallet

    # ─── Reservation side ────────────────────────────────────────────────

    def maybe_propose_reservation(
        self,
        miner_hotkey: str,
        from_chain_id: str,
        from_tx_hash: bytes,
        current_block: int,
        reserved_until: int,
        observed_confirmations: int,
    ) -> bool:
        """Submit a propose_extend_reservation if no proposal is pending.

        ``reserved_until`` is the on-chain deadline as last seen by the caller
        — we re-check it inside compute_extension_target via the chain registry.
        Returns True if a propose tx was submitted (i.e. no pending entry
        existed and we put one there), False otherwise (already pending,
        target wouldn't be a forward step, contract rejected, RPC error).
        """
        existing = self._safe_get_pending_reservation(miner_hotkey)
        if existing is not None:
            return False

        target_block = compute_extension_target(
            from_chain_id, observed_confirmations, current_block
        )
        if target_block <= reserved_until:
            # Bucketed target landed at or before the existing deadline — the
            # extension is unnecessary, don't waste a tx.
            return False

        return self._try_call(
            'propose_extend_reservation',
            lambda: self.contract_client.propose_extend_reservation(
                wallet=self.wallet,
                miner_hotkey=miner_hotkey,
                from_tx_hash=from_tx_hash,
                target_block=target_block,
            ),
        )

    def maybe_challenge_reservation(
        self,
        miner_hotkey: str,
        from_chain_id: str,
        observed_confirmations: int,
        current_block: int,
    ) -> bool:
        """Challenge the pending reservation extension if its target is too far.

        Tolerance is one bucket — proposals within EXTENSION_BUCKET_BLOCKS of
        the locally-computed target are accepted as benign rounding drift.
        Skips proposals submitted by this validator's own wallet.
        """
        pending = self._safe_get_pending_reservation(miner_hotkey)
        if pending is None:
            return False
        if self._is_own_proposal(pending):
            return False

        expected = compute_extension_target(
            from_chain_id, observed_confirmations, current_block
        )
        if pending.target_block <= expected + EXTENSION_BUCKET_BLOCKS:
            return False

        return self._try_call(
            'challenge_extend_reservation',
            lambda: self.contract_client.challenge_extend_reservation(
                wallet=self.wallet, miner_hotkey=miner_hotkey,
            ),
        )

    def maybe_finalize_reservation(
        self,
        miner_hotkey: str,
        current_block: int,
        challenge_window_blocks: int,
    ) -> bool:
        """Finalize the pending reservation extension if its window has elapsed.

        Returns True if a finalize tx was submitted. The contract's own check
        is authoritative — we just gate on the local view to avoid known-doomed
        txs. ``challenge_window_blocks`` is passed in (rather than imported)
        so tests can vary it cheaply.
        """
        pending = self._safe_get_pending_reservation(miner_hotkey)
        if pending is None:
            return False
        if current_block < pending.proposed_at + challenge_window_blocks:
            return False

        return self._try_call(
            'finalize_extend_reservation',
            lambda: self.contract_client.finalize_extend_reservation(
                wallet=self.wallet, miner_hotkey=miner_hotkey,
            ),
        )

    # ─── Timeout side ────────────────────────────────────────────────────

    def maybe_propose_timeout(
        self,
        swap_id: int,
        dest_chain_id: str,
        current_block: int,
        timeout_block: int,
        observed_confirmations: int,
    ) -> bool:
        existing = self._safe_get_pending_timeout(swap_id)
        if existing is not None:
            return False

        target_block = compute_extension_target(
            dest_chain_id, observed_confirmations, current_block
        )
        if target_block <= timeout_block:
            return False

        return self._try_call(
            'propose_extend_timeout',
            lambda: self.contract_client.propose_extend_timeout(
                wallet=self.wallet, swap_id=swap_id, target_block=target_block,
            ),
        )

    def maybe_challenge_timeout(
        self,
        swap_id: int,
        dest_chain_id: str,
        observed_confirmations: int,
        current_block: int,
    ) -> bool:
        pending = self._safe_get_pending_timeout(swap_id)
        if pending is None:
            return False
        if self._is_own_proposal(pending):
            return False

        expected = compute_extension_target(
            dest_chain_id, observed_confirmations, current_block
        )
        if pending.target_block <= expected + EXTENSION_BUCKET_BLOCKS:
            return False

        return self._try_call(
            'challenge_extend_timeout',
            lambda: self.contract_client.challenge_extend_timeout(
                wallet=self.wallet, swap_id=swap_id,
            ),
        )

    def maybe_finalize_timeout(
        self,
        swap_id: int,
        current_block: int,
        challenge_window_blocks: int,
    ) -> bool:
        pending = self._safe_get_pending_timeout(swap_id)
        if pending is None:
            return False
        if current_block < pending.proposed_at + challenge_window_blocks:
            return False

        return self._try_call(
            'finalize_extend_timeout',
            lambda: self.contract_client.finalize_extend_timeout(
                wallet=self.wallet, swap_id=swap_id,
            ),
        )

    # ─── Internals ───────────────────────────────────────────────────────

    def _safe_get_pending_reservation(self, miner_hotkey: str) -> Optional[PendingExtension]:
        try:
            return self.contract_client.get_pending_reservation_extension(miner_hotkey)
        except Exception as e:
            bt.logging.debug(f'OptimisticExt: get_pending_reservation({miner_hotkey[:8]}) failed: {e}')
            return None

    def _safe_get_pending_timeout(self, swap_id: int) -> Optional[PendingExtension]:
        try:
            return self.contract_client.get_pending_timeout_extension(swap_id)
        except Exception as e:
            bt.logging.debug(f'OptimisticExt: get_pending_timeout({swap_id}) failed: {e}')
            return None

    def _is_own_proposal(self, pending: PendingExtension) -> bool:
        try:
            return pending.submitter == self.wallet.hotkey.ss58_address
        except Exception:
            return False

    def _try_call(self, label: str, fn) -> bool:
        """Run a contract write, swallow expected rejections, log unknowns.

        Expected rejections (ProposalAlreadyPending, ChallengeWindowOpen, etc.)
        mean another validator beat us to it or our local view is stale —
        normal in a multi-validator race, not worth alarming on.
        """
        try:
            fn()
            return True
        except ContractError as e:
            if is_contract_rejection(e):
                bt.logging.debug(f'OptimisticExt: {label} rejected by contract: {e}')
            else:
                bt.logging.warning(f'OptimisticExt: {label} contract error: {e}')
            return False
        except Exception as e:
            bt.logging.warning(f'OptimisticExt: {label} failed: {e}')
            return False
