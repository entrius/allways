"""Validator forward pass — orchestrator called every step by the base neuron."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Set

import bittensor as bt

from allways.chain_providers.base import ProviderUnreachableError
from allways.classes import SwapStatus
from allways.commitments import read_miner_commitments
from allways.constants import (
    CHALLENGE_WINDOW_BLOCKS,
    EXTEND_THRESHOLD_BLOCKS,
    PENDING_CONFIRM_NULL_RETRY_LIMIT,
    SCORING_WINDOW_BLOCKS,
)
from allways.contract_client import ContractError, is_contract_rejection
from allways.utils.logging import log_on_change
from allways.utils.scale import strip_hex_prefix
from allways.validator import voting
from allways.validator.axon_handlers import (
    keccak256,
    scale_encode_initiate_hash_input,
)
from allways.validator.chain_verification import SwapVerifier
from allways.validator.scoring import score_and_reward_miners
from allways.validator.state_store import PendingConfirm
from allways.validator.swap_tracker import SwapTracker

if TYPE_CHECKING:
    from neurons.validator import Validator


async def forward(self: Validator) -> None:
    """One validator forward step. Phase order matters — each phase may depend
    on state mutated by the previous one."""
    tracker: SwapTracker = self.swap_tracker
    verifier: SwapVerifier = self.swap_verifier

    clear_provider_caches(self)

    # Sync events before purge so any ReservationExtensionFinalized in the
    # last block writes the new reserved_until back to state_store before the
    # purge sweep runs — otherwise a row whose contract deadline just bumped
    # would still be deleted at its stale (original) reserved_until.
    try:
        self.event_watcher.sync_to(self.block)
    except Exception as e:
        bt.logging.warning(f'Event watcher sync failed: {e}')

    self.state_store.purge_expired_pending_confirms()

    initialize_pending_user_reservations(self)

    poll_commitments(self)

    # Pull newly-initiated and resolved swaps off the contract.
    await tracker.poll()

    # Verify FULFILLED swaps end-to-end and vote confirm_swap. The returned
    # set is swap IDs where the provider was unreachable this cycle, so the
    # timeout phase knows to skip them (transient outage shouldn't slash).
    uncertain_swaps = await confirm_miner_fulfillments(self, tracker, verifier, self.block)

    extend_fulfilled_near_timeout(self)
    enforce_swap_timeouts(self, tracker, uncertain_swaps)

    if self.step % SCORING_WINDOW_BLOCKS == 0:
        score_and_reward_miners(self)


def clear_provider_caches(self: Validator) -> None:
    for provider in self.chain_providers.values():
        if hasattr(provider, 'clear_cache'):
            provider.clear_cache()


def initialize_pending_user_reservations(self: Validator) -> None:
    """Check queued unconfirmed txs and vote_initiate when confirmations are met."""
    from bittensor import Keypair

    items = self.state_store.get_all()
    # Drop per-entry receipts whose pending_confirm has been removed
    # (vote_initiate landed, tx not found, expired, etc.).
    live_keys = {(item.miner_hotkey, item.from_tx_hash) for item in items}
    for stale_key in [k for k in self.pending_confirm_null_polls if k not in live_keys]:
        del self.pending_confirm_null_polls[stale_key]

    if not items:
        return

    current_block = self.block

    for item in items:
        swap_label = f'{item.from_chain.upper()}->{item.to_chain.upper()}'
        try:
            uid = self.metagraph.hotkeys.index(item.miner_hotkey)
        except ValueError:
            uid = '?'
        miner_short = f'UID {uid} ({item.miner_hotkey[:8]})'
        chain_def = self.chain_providers.get(item.from_chain)
        min_confs = chain_def.get_chain().min_confirmations if chain_def else '?'

        try:
            if self.contract_client.get_miner_has_active_swap(item.miner_hotkey):
                self.state_store.remove(item.miner_hotkey)
                bt.logging.info(f'PendingConfirm [{swap_label} {miner_short}]: already has active swap, dropping')
                continue
        except Exception as e:
            bt.logging.warning(f'PendingConfirm [{swap_label} {miner_short}]: active swap check failed: {e}')

        provider = self.chain_providers.get(item.from_chain)
        if provider is None:
            self.state_store.remove(item.miner_hotkey)
            bt.logging.warning(
                f'PendingConfirm [{swap_label} {miner_short}]: no provider for {item.from_chain}, dropping'
            )
            continue

        try:
            tx_info = provider.verify_transaction(
                tx_hash=item.from_tx_hash,
                expected_recipient=item.miner_from_address,
                expected_amount=item.from_amount,
                block_hint=item.from_tx_block,
                expected_sender=item.from_address,
            )
        except ProviderUnreachableError as e:
            bt.logging.warning(f'PendingConfirm [{swap_label} {miner_short}]: provider unreachable, will retry: {e}')
            try_extend_reservation(self, item, current_block, swap_label, miner_short, tx_info=None)
            continue
        except Exception as e:
            bt.logging.error(f'PendingConfirm [{swap_label} {miner_short}]: verify_transaction error: {e}')
            continue

        if tx_info is None:
            null_key = (item.miner_hotkey, item.from_tx_hash)
            attempts = self.pending_confirm_null_polls.get(null_key, 0) + 1
            if attempts < PENDING_CONFIRM_NULL_RETRY_LIMIT:
                self.pending_confirm_null_polls[null_key] = attempts
                bt.logging.info(
                    f'PendingConfirm [{swap_label} {miner_short}]: tx {item.from_tx_hash[:16]}... '
                    f'not found (attempt {attempts}/{PENDING_CONFIRM_NULL_RETRY_LIMIT}), retrying'
                )
                try_extend_reservation(self, item, current_block, swap_label, miner_short, tx_info=None)
                continue
            self.state_store.remove(item.miner_hotkey)
            bt.logging.warning(
                f'PendingConfirm [{swap_label} {miner_short}]: tx {item.from_tx_hash[:16]}... '
                f'not found after {PENDING_CONFIRM_NULL_RETRY_LIMIT} attempts, dropping'
            )
            continue

        self.pending_confirm_null_polls.pop((item.miner_hotkey, item.from_tx_hash), None)

        log_on_change(
            f'confs:{item.miner_hotkey}',
            tx_info.confirmations,
            f'PendingConfirm [{swap_label} {miner_short}]: '
            f'{tx_info.confirmations}/{min_confs} confirmations, tx={item.from_tx_hash[:16]}...',
        )

        if not tx_info.confirmed:
            try_extend_reservation(self, item, current_block, swap_label, miner_short, tx_info=tx_info)
            continue

        # Only drop the queued entry once the vote is accepted (or the contract
        # rejects it as already-initiated). Transient RPC failures leave the
        # entry queued so the next forward step retries.
        try:
            miner_bytes = bytes.fromhex(Keypair(ss58_address=item.miner_hotkey).public_key.hex())
            hash_input = scale_encode_initiate_hash_input(
                miner_bytes,
                item.from_tx_hash,
                item.from_chain,
                item.to_chain,
                item.miner_from_address,
                item.miner_to_address,
                item.rate_str,
                item.tao_amount,
                item.from_amount,
                item.to_amount,
            )
            request_hash = keccak256(hash_input)

            user_tao_address = item.to_address if item.to_chain == 'tao' else item.from_address
            self.contract_client.vote_initiate(
                wallet=self.wallet,
                request_hash=request_hash,
                user_hotkey=user_tao_address,
                miner_hotkey=item.miner_hotkey,
                from_chain=item.from_chain,
                to_chain=item.to_chain,
                from_amount=item.from_amount,
                tao_amount=item.tao_amount,
                user_from_address=item.from_address,
                user_to_address=item.to_address,
                from_tx_hash=item.from_tx_hash,
                from_tx_block=tx_info.block_number or 0,
                to_amount=item.to_amount,
                miner_from_address=item.miner_from_address,
                miner_to_address=item.miner_to_address,
                rate=item.rate_str,
            )
            self.state_store.remove(item.miner_hotkey)
            bt.logging.success(
                f'PendingConfirm [{swap_label} {miner_short}]: '
                f'confirmed! voted initiate (tao={item.tao_amount / 1e9:.4f})'
            )
        except ContractError as e:
            if is_contract_rejection(e):
                self.state_store.remove(item.miner_hotkey)
                bt.logging.info(
                    f'PendingConfirm [{swap_label} {miner_short}]: contract rejected (likely already initiated)'
                )
            else:
                bt.logging.error(f'PendingConfirm [{swap_label} {miner_short}]: vote_initiate failed: {e}')
        except Exception as e:
            bt.logging.error(f'PendingConfirm [{swap_label} {miner_short}]: unexpected error: {e}')


def try_extend_reservation(
    self: Validator,
    item: PendingConfirm,
    current_block: int,
    swap_label: str,
    miner_short: str,
    tx_info,
) -> None:
    """Drive the tiered optimistic-extension decisions for one pending confirm.

    Finalize is unconditional — another validator may have proposed while we
    couldn't see the tx, and we should still help close its window. Propose
    and challenge require *visibility* (``tx_info != None``); the watcher
    itself enforces the per-tier evidence rule (tier 0: visibility OK; tier 1:
    confirmations >= 1; tier 2+: refused). See OPTIMISTIC_EXTENSION_REDESIGN.md
    §13.
    """
    try:
        reserved_until = self.contract_client.get_miner_reserved_until(item.miner_hotkey)
    except Exception as e:
        bt.logging.debug(f'PendingConfirm [{swap_label} {miner_short}]: reserved_until read failed: {e}')
        reserved_until = item.reserved_until

    self.optimistic_extensions.maybe_finalize_reservation(
        miner_hotkey=item.miner_hotkey,
        current_block=current_block,
        challenge_window_blocks=CHALLENGE_WINDOW_BLOCKS,
    )

    if tx_info is None:
        return
    if reserved_until >= current_block + EXTEND_THRESHOLD_BLOCKS:
        return

    try:
        extension_count = self.contract_client.get_reservation_extension_count(item.miner_hotkey)
    except Exception as e:
        bt.logging.debug(f'PendingConfirm [{swap_label} {miner_short}]: extension_count read failed: {e}')
        return

    self.optimistic_extensions.maybe_challenge_reservation(
        miner_hotkey=item.miner_hotkey,
        from_chain_id=item.from_chain,
        observed_confirmations=tx_info.confirmations,
        current_block=current_block,
    )

    try:
        from_tx_hash_bytes = bytes.fromhex(strip_hex_prefix(item.from_tx_hash))
    except ValueError:
        bt.logging.debug(f'PendingConfirm [{swap_label} {miner_short}]: malformed from_tx_hash, skipping propose')
        return

    proposed = self.optimistic_extensions.maybe_propose_reservation(
        miner_hotkey=item.miner_hotkey,
        from_chain_id=item.from_chain,
        from_tx_hash=from_tx_hash_bytes,
        current_block=current_block,
        reserved_until=reserved_until,
        observed_confirmations=tx_info.confirmations,
        extension_count=extension_count,
    )
    if proposed:
        bt.logging.info(
            f'PendingConfirm [{swap_label} {miner_short}]: '
            f'proposed reservation extension (tier {extension_count}, '
            f'{reserved_until - current_block} blocks remaining, {tx_info.confirmations} confs)'
        )


def poll_commitments(self: Validator) -> None:
    """Read every miner commitment via one query_map RPC and persist diffs.

    Cost is one round-trip regardless of miner count, so per-block sampling
    gives the crown-time series ~1-block accuracy. Event retention pruning
    runs in the scoring round, not here.
    """
    refresh_miner_rates(self)
    purge_deregistered_hotkeys(self)


def refresh_miner_rates(self: Validator) -> None:
    try:
        pairs = read_miner_commitments(self.subtensor, self.config.netuid)
    except Exception as e:
        bt.logging.warning(f'Commitment poll failed: {e}')
        return

    current_hotkeys = set(self.metagraph.hotkeys)

    for pair in pairs:
        if pair.hotkey not in current_hotkeys:
            continue
        for from_c, to_c, r in (
            (pair.from_chain, pair.to_chain, pair.rate),
            (pair.to_chain, pair.from_chain, pair.counter_rate),
        ):
            if r <= 0:
                continue  # miner opted out of this direction
            key = (pair.hotkey, from_c, to_c)
            if self.last_known_rates.get(key) == r:
                continue
            self.state_store.insert_rate_event(
                hotkey=pair.hotkey,
                from_chain=from_c,
                to_chain=to_c,
                rate=r,
                block=self.block,
            )
            self.last_known_rates[key] = r


def purge_deregistered_hotkeys(self: Validator) -> None:
    current_hotkeys = set(self.metagraph.hotkeys)
    stale = {hk for (hk, _, _) in self.last_known_rates.keys()} - current_hotkeys
    if not stale:
        return
    for hk in stale:
        self.state_store.delete_hotkey(hk)
    self.last_known_rates = {k: v for k, v in self.last_known_rates.items() if k[0] not in stale}


async def confirm_miner_fulfillments(
    self: Validator,
    tracker: SwapTracker,
    verifier: SwapVerifier,
    current_block: int,
) -> Set[int]:
    """Verify FULFILLED swaps and vote confirm. Returns swap IDs whose
    provider was unreachable so the caller can skip them on timeout enforce."""
    uncertain: Set[int] = set()
    fulfilled = [s for s in tracker.get_fulfilled(current_block) if not tracker.is_voted(s.id)]
    if not fulfilled:
        return uncertain

    results = await asyncio.gather(
        *[verifier.verify_miner_fulfillment(swap) for swap in fulfilled],
        return_exceptions=True,
    )
    for swap, result in zip(fulfilled, results):
        if isinstance(result, ProviderUnreachableError):
            bt.logging.warning(f'Swap {swap.id}: provider unreachable, deferring verification')
            uncertain.add(swap.id)
            continue
        if isinstance(result, Exception):
            bt.logging.error(f'Swap {swap.id}: verification error: {result}')
            continue
        if result:
            if voting.confirm_swap(self.contract_client, self.wallet, swap.id):
                tracker.resolve(swap.id, SwapStatus.COMPLETED, current_block)
                bt.logging.success(f'Swap {swap.id}: verified complete, confirmed')
            # On vote failure, voting.confirm_swap already logs the error;
            # the entry stays in tracker and retries next step.
    return uncertain


def extend_fulfilled_near_timeout(self: Validator) -> None:
    """Drive tiered optimistic timeout extensions for FULFILLED swaps near
    deadline. Same dispatch shape as ``try_extend_reservation``: finalize
    always; propose/challenge fire on visibility, with the watcher enforcing
    per-tier evidence rules (§13).
    """
    tracker: SwapTracker = self.swap_tracker
    current_block = self.block

    for swap in tracker.get_near_timeout_fulfilled(current_block):
        swap_label = f'{swap.from_chain.upper()}->{swap.to_chain.upper()}'
        ctx = f'Swap #{swap.id} [{swap_label}]'

        self.optimistic_extensions.maybe_finalize_timeout(
            swap_id=swap.id,
            current_block=current_block,
            challenge_window_blocks=CHALLENGE_WINDOW_BLOCKS,
        )

        provider = self.chain_providers.get(swap.to_chain)
        if not provider or not swap.to_tx_hash:
            continue

        try:
            tx_info = provider.verify_transaction(
                tx_hash=swap.to_tx_hash,
                expected_recipient=swap.user_to_address,
                expected_amount=swap.to_amount,
                block_hint=swap.to_tx_block,
            )
        except Exception as e:
            bt.logging.debug(f'{ctx}: extend check verify_transaction error: {e}')
            continue

        if tx_info is None:
            continue  # dest tx invisible — neither tier qualifies

        try:
            extension_count = self.contract_client.get_swap_extension_count(swap.id)
        except Exception as e:
            bt.logging.debug(f'{ctx}: extension_count read failed: {e}')
            continue

        chain_def = provider.get_chain()
        log_on_change(
            f'dest_confs:{swap.id}',
            tx_info.confirmations,
            f'{ctx}: {tx_info.confirmations}/{chain_def.min_confirmations} dest confirmations, '
            f'{swap.timeout_block - current_block} blocks until timeout',
        )

        self.optimistic_extensions.maybe_challenge_timeout(
            swap_id=swap.id,
            dest_chain_id=swap.to_chain,
            observed_confirmations=tx_info.confirmations,
            current_block=current_block,
        )
        proposed = self.optimistic_extensions.maybe_propose_timeout(
            swap_id=swap.id,
            dest_chain_id=swap.to_chain,
            current_block=current_block,
            timeout_block=swap.timeout_block,
            observed_confirmations=tx_info.confirmations,
            extension_count=extension_count,
        )
        if proposed:
            bt.logging.info(
                f'{ctx}: proposed timeout extension (tier {extension_count}, '
                f'{tx_info.confirmations}/{chain_def.min_confirmations} dest confirmations)'
            )


def enforce_swap_timeouts(self: Validator, tracker: SwapTracker, uncertain_swaps: Set[int]) -> None:
    """Timeout expired swaps, skipping uncertain_swaps where the provider was unreachable this cycle."""
    for swap in tracker.get_timed_out(self.block):
        if tracker.is_voted(swap.id):
            continue
        if swap.id in uncertain_swaps:
            bt.logging.warning(f'Swap {swap.id}: deferring timeout, provider was unreachable')
            continue

        if voting.timeout_swap(self.contract_client, self.wallet, swap.id):
            tracker.resolve(swap.id, SwapStatus.TIMED_OUT, self.block)
            bt.logging.warning(f'Swap {swap.id}: timed out')
        # On vote failure, voting.timeout_swap already logs the error.
