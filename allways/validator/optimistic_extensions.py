"""Optimistic extension watcher.

Single-validator propose / challenge / finalize for reservation and timeout
extensions, with tiered evidence and a per-entity cap.

This module provides only the per-decision primitives — picking the right
moments to invoke them lives in the validator forward loop. Each method
takes the inputs it needs as parameters so the class is fully mockable in
unit tests without spinning up a chain.
"""

from typing import Optional

import bittensor as bt

from allways.chains import compute_extension_target, get_chain
from allways.classes import PendingExtension
from allways.constants import (
    CHALLENGE_WINDOW_BLOCKS,
    EXTENSION_BUCKET_BLOCKS,
    MAX_EXTENSIONS_PER_RESERVATION,
    MAX_EXTENSIONS_PER_SWAP,
    VALIDATOR_FORWARD_STEP_BLOCKS_ESTIMATE,
)
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
        extension_count: int,
    ) -> bool:
        """Submit a propose_extend_reservation if no proposal is pending,
        tiered on ``extension_count``.

        - Tier 0 (first extension): caller is responsible for ensuring the tx
          is visible (``tx_info != None``); target sized for one chain block.
        - Tier 1 (second extension): requires ``observed_confirmations >= 1``;
          target = chain-aware full-confirmation window.
        - Tier 2+: refused locally to avoid a doomed tx (contract rejects too).

        Returns True if a propose tx was submitted, False otherwise.
        """
        if extension_count >= MAX_EXTENSIONS_PER_RESERVATION:
            return False

        existing = self._safe_get_pending_reservation(miner_hotkey)
        if existing is not None:
            return False

        if extension_count == 0:
            remaining = 1
        else:
            if observed_confirmations < 1:
                return False
            chain = get_chain(from_chain_id)
            remaining = max(0, chain.min_confirmations - observed_confirmations)
        target_block = compute_extension_target(from_chain_id, remaining, current_block)

        # Defensive floor: target must outlast the challenge window plus one
        # forward-step worth of jitter so a single missed step still leaves
        # finalize-eligible time before the new deadline lapses. Round up to
        # an EXTENSION_BUCKET_BLOCKS boundary so validators converge.
        min_safe_target = current_block + CHALLENGE_WINDOW_BLOCKS + VALIDATOR_FORWARD_STEP_BLOCKS_ESTIMATE
        if target_block < min_safe_target:
            target_block = (
                min_safe_target - current_block + EXTENSION_BUCKET_BLOCKS - 1
            ) // EXTENSION_BUCKET_BLOCKS * EXTENSION_BUCKET_BLOCKS + current_block

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

        chain = get_chain(from_chain_id)
        remaining = max(0, chain.min_confirmations - observed_confirmations)
        expected = compute_extension_target(from_chain_id, remaining, current_block)
        if pending.target_block <= expected + EXTENSION_BUCKET_BLOCKS:
            return False

        return self._try_call(
            'challenge_extend_reservation',
            lambda: self.contract_client.challenge_extend_reservation(
                wallet=self.wallet,
                miner_hotkey=miner_hotkey,
            ),
        )

    def maybe_finalize_reservation(
        self,
        miner_hotkey: str,
        current_block: int,
        challenge_window_blocks: int,
    ) -> Optional[int]:
        """Finalize the pending reservation extension if its window has elapsed.

        Returns the applied ``target_block`` on success, ``None`` otherwise.
        Callers use the returned target to refresh local caches (e.g.
        state_store.update_reserved_until) without waiting for the next event
        sync. The contract's own check is authoritative — we just gate on the
        local view to avoid known-doomed txs. ``challenge_window_blocks`` is
        passed in (rather than imported) so tests can vary it cheaply.
        """
        pending = self._safe_get_pending_reservation(miner_hotkey)
        if pending is None:
            return None
        if current_block < pending.proposed_at + challenge_window_blocks:
            return None

        success = self._try_call(
            'finalize_extend_reservation',
            lambda: self.contract_client.finalize_extend_reservation(
                wallet=self.wallet,
                miner_hotkey=miner_hotkey,
            ),
        )
        return pending.target_block if success else None

    # ─── Timeout side ────────────────────────────────────────────────────

    def maybe_propose_timeout(
        self,
        swap_id: int,
        dest_chain_id: str,
        current_block: int,
        timeout_block: int,
        observed_confirmations: int,
        extension_count: int,
    ) -> bool:
        """Tiered timeout-extension propose. See ``maybe_propose_reservation``."""
        if extension_count >= MAX_EXTENSIONS_PER_SWAP:
            return False

        existing = self._safe_get_pending_timeout(swap_id)
        if existing is not None:
            return False

        if extension_count == 0:
            remaining = 1
        else:
            if observed_confirmations < 1:
                return False
            chain = get_chain(dest_chain_id)
            remaining = max(0, chain.min_confirmations - observed_confirmations)
        target_block = compute_extension_target(dest_chain_id, remaining, current_block)

        if target_block <= timeout_block:
            return False

        return self._try_call(
            'propose_extend_timeout',
            lambda: self.contract_client.propose_extend_timeout(
                wallet=self.wallet,
                swap_id=swap_id,
                target_block=target_block,
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

        chain = get_chain(dest_chain_id)
        remaining = max(0, chain.min_confirmations - observed_confirmations)
        expected = compute_extension_target(dest_chain_id, remaining, current_block)
        if pending.target_block <= expected + EXTENSION_BUCKET_BLOCKS:
            return False

        return self._try_call(
            'challenge_extend_timeout',
            lambda: self.contract_client.challenge_extend_timeout(
                wallet=self.wallet,
                swap_id=swap_id,
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
                wallet=self.wallet,
                swap_id=swap_id,
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
