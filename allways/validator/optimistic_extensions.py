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

from allways.chains import compute_extension_target
from allways.classes import PendingExtension
from allways.constants import (
    CHALLENGE_WINDOW_BLOCKS,
    EXTENSION_BUCKET_BLOCKS,
    MAX_EXTENSIONS_PER_RESERVATION,
    MAX_EXTENSIONS_PER_SWAP,
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

    # ─── Pending-extension fetch helpers ─────────────────────────────────
    #
    # Callers (forward.py) fetch once per row and pass the result into the
    # propose/challenge/finalize methods. Used to be three separate fetches
    # per row from inside each method — that turned into N pending rows × 3
    # RPCs per forward step, which dominated step latency on busy validators.

    def fetch_pending_reservation(self, miner_hotkey: str) -> Optional[PendingExtension]:
        return self._safe_get_pending_reservation(miner_hotkey)

    def fetch_pending_timeout(self, swap_id: int) -> Optional[PendingExtension]:
        return self._safe_get_pending_timeout(swap_id)

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
        pending: Optional[PendingExtension],
    ) -> bool:
        """Submit a propose_extend_reservation if no proposal is pending,
        tiered on ``extension_count``.

        ``pending`` is the contract's current pending entry for this miner
        (``None`` if no proposal is in-flight). Caller fetches once via
        ``fetch_pending_reservation`` and passes the same value into all
        three propose/challenge/finalize methods to avoid redundant RPCs.

        Both tiers fire on tx visibility alone with identical runway. Tier-1
        previously gated on ``observed_confirmations >= 1``; that raced with
        the challenge-window guard near the deadline, so it was dropped in
        favour of tier-1 acting as a tail-of-distribution safety net.
        Tier 2+ is refused locally to avoid a doomed tx (contract rejects too).

        Returns True if a propose tx was submitted, False otherwise.
        """
        if extension_count >= MAX_EXTENSIONS_PER_RESERVATION:
            return False

        if pending is not None:
            return False

        # Refuse a doomed propose: if the challenge window can't close before
        # the existing deadline, finalize will only become eligible after
        # expiry. Anchoring the target on ``reserved_until`` sizes the *new*
        # deadline safely past the old one, but it can't rescue a propose
        # whose challenge window outlives the original reservation.
        if current_block + CHALLENGE_WINDOW_BLOCKS >= reserved_until:
            return False

        remaining = 4  # ~48 min runway, sized for BTC block-time variance
        target_block = compute_extension_target(from_chain_id, remaining, current_block)

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
        reserved_until: int,
        pending: Optional[PendingExtension],
    ) -> bool:
        """Challenge the pending reservation extension if its target is too far.

        Mirrors the current-block-anchored math used by ``maybe_propose_reservation``
        so challenger and proposer compute the same expected target. Without
        this the two sides could drift by up to EXTEND_THRESHOLD_BLOCKS,
        which the EXTENSION_BUCKET_BLOCKS tolerance currently absorbs but
        shouldn't have to.

        Tolerance is one bucket — proposals within EXTENSION_BUCKET_BLOCKS of
        the locally-computed target are accepted as benign rounding drift.
        Skips proposals submitted by this validator's own wallet.
        """
        if pending is None:
            return False
        if self._is_own_proposal(pending):
            return False

        remaining = 4  # mirror propose
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
        pending: Optional[PendingExtension],
    ) -> Optional[int]:
        """Finalize the pending reservation extension if its window has elapsed.

        Returns the applied ``target_block`` on success, ``None`` otherwise.
        Callers use the returned target to refresh local caches (e.g.
        state_store.update_reserved_until) without waiting for the next event
        sync. ``pending`` is the caller-supplied snapshot — see
        ``maybe_propose_reservation``. The contract's own check is
        authoritative; the local pending read just lets us avoid a
        known-doomed tx. ``challenge_window_blocks`` is passed in (rather
        than imported) so tests can vary it cheaply.
        """
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
        pending: Optional[PendingExtension],
    ) -> bool:
        """Timeout-extension propose. See ``maybe_propose_reservation`` for
        the ``pending`` contract and tier semantics."""
        if extension_count >= MAX_EXTENSIONS_PER_SWAP:
            return False

        if pending is not None:
            return False

        if current_block + CHALLENGE_WINDOW_BLOCKS >= timeout_block:
            return False

        remaining = 4  # ~48 min runway, sized for BTC block-time variance
        target_block = compute_extension_target(dest_chain_id, remaining, current_block)

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
        timeout_block: int,
        pending: Optional[PendingExtension],
    ) -> bool:
        """Mirror of ``maybe_challenge_reservation``: current-block-anchored
        expected target so challenger and proposer stay aligned."""
        if pending is None:
            return False
        if self._is_own_proposal(pending):
            return False

        remaining = 4  # mirror propose
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
        pending: Optional[PendingExtension],
    ) -> Optional[int]:
        """Same shape as ``maybe_finalize_reservation``: returns the applied
        ``target_block`` so the caller can refresh ``swap_tracker`` in-line
        before the same step's ``enforce_swap_timeouts`` reads from it.
        ``pending`` is the caller-supplied snapshot — see
        ``maybe_propose_reservation``."""
        if pending is None:
            return None
        if current_block < pending.proposed_at + challenge_window_blocks:
            return None

        success = self._try_call(
            'finalize_extend_timeout',
            lambda: self.contract_client.finalize_extend_timeout(
                wallet=self.wallet,
                swap_id=swap_id,
            ),
        )
        return pending.target_block if success else None

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
