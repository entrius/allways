"""Validator forward pass — orchestrator called every step by the base neuron."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Set

import bittensor as bt

from allways.chain_providers.base import ProviderUnreachableError
from allways.classes import SwapStatus
from allways.commitments import read_miner_commitments
from allways.constants import (
    EXTEND_THRESHOLD_BLOCKS,
    PENDING_CONFIRM_NULL_RETRY_LIMIT,
    SCORING_WINDOW_BLOCKS,
)
from allways.contract_client import ContractError, is_contract_rejection
from allways.utils.logging import log_on_change
from allways.validator import voting
from allways.validator.axon_handlers import (
    keccak256,
    miner_public_key_bytes,
    scale_encode_extend_hash_input,
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
    bt.logging.info(f'Forward step {self.step}')

    tracker: SwapTracker = self.swap_tracker
    verifier: SwapVerifier = self.swap_verifier

    clear_provider_caches(self)
    self.state_store.purge_expired_pending_confirms()

    initialize_pending_user_reservations(self)

    poll_commitments(self)

    try:
        self.event_watcher.sync_to(self.block)
    except Exception as e:
        bt.logging.warning(f'Event watcher sync failed: {e}')

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
    items = self.state_store.get_all()
    # Drop per-entry receipts whose pending_confirm has been removed
    # (vote_initiate landed, tx not found, expired, etc.).
    live_keys = {(item.miner_hotkey, item.from_tx_hash) for item in items}
    for stale_key in [k for k in self.extend_reservation_voted_at if k not in live_keys]:
        del self.extend_reservation_voted_at[stale_key]
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
            try_extend_reservation(self, item, current_block, swap_label, miner_short)
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
                try_extend_reservation(self, item, current_block, swap_label, miner_short)
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
            try_extend_reservation(self, item, current_block, swap_label, miner_short)
            continue

        # Only drop the queued entry once the vote is accepted (or the contract
        # rejects it as already-initiated). Transient RPC failures leave the
        # entry queued so the next forward step retries.
        try:
            miner_bytes = miner_public_key_bytes(item.miner_hotkey)
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
) -> None:
    """Vote to extend reservation if nearing expiry, protecting users during provider outages."""
    try:
        reserved_until = self.contract_client.get_miner_reserved_until(item.miner_hotkey)
        if reserved_until >= current_block + EXTEND_THRESHOLD_BLOCKS:
            return

        vote_key = (item.miner_hotkey, item.from_tx_hash)
        voted_at = self.extend_reservation_voted_at.get(vote_key)
        if voted_at is not None and reserved_until <= voted_at:
            return  # already voted under this reservation; contract hasn't extended yet

        miner_bytes = miner_public_key_bytes(item.miner_hotkey)
        extend_hash = keccak256(scale_encode_extend_hash_input(miner_bytes, item.from_tx_hash))
        self.contract_client.vote_extend_reservation(
            wallet=self.wallet,
            request_hash=extend_hash,
            miner_hotkey=item.miner_hotkey,
            from_tx_hash=item.from_tx_hash,
        )
        self.extend_reservation_voted_at[vote_key] = reserved_until
        bt.logging.info(
            f'PendingConfirm [{swap_label} {miner_short}]: '
            f'voted to extend reservation ({reserved_until - current_block} blocks remaining)'
        )
    except ContractError as e:
        if 'AlreadyVoted' in str(e):
            self.extend_reservation_voted_at[(item.miner_hotkey, item.from_tx_hash)] = (
                self.contract_client.get_miner_reserved_until(item.miner_hotkey)
            )
        else:
            bt.logging.debug(f'PendingConfirm [{swap_label} {miner_short}]: extend vote: {e}')
    except Exception as e:
        bt.logging.debug(f'PendingConfirm [{swap_label} {miner_short}]: extend check failed: {e}')


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
    return uncertain


def extend_fulfilled_near_timeout(self: Validator) -> None:
    """Vote to extend timeout for FULFILLED swaps whose dest tx is visible
    on-chain but not yet at min confirmations."""
    tracker: SwapTracker = self.swap_tracker
    current_block = self.block

    for swap in tracker.get_near_timeout_fulfilled(current_block):
        if tracker.is_extend_timeout_voted(swap.id):
            continue

        swap_label = f'{swap.from_chain.upper()}->{swap.to_chain.upper()}'
        ctx = f'Swap #{swap.id} [{swap_label}]'

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
            continue  # dest tx not found — let it time out

        chain_def = provider.get_chain()
        log_on_change(
            f'dest_confs:{swap.id}',
            tx_info.confirmations,
            f'{ctx}: {tx_info.confirmations}/{chain_def.min_confirmations} dest confirmations, '
            f'{swap.timeout_block - current_block} blocks until timeout',
        )

        try:
            voting.extend_swap_timeout(self.contract_client, self.wallet, swap.id)
            tracker.mark_extend_timeout_voted(swap.id)
            bt.logging.info(
                f'{ctx}: voted to extend timeout '
                f'({tx_info.confirmations}/{chain_def.min_confirmations} dest confirmations)'
            )
        except ContractError as e:
            if 'AlreadyVoted' in str(e):
                tracker.mark_extend_timeout_voted(swap.id)
            elif not is_contract_rejection(e):
                bt.logging.debug(f'{ctx}: extend timeout vote: {e}')
        except Exception as e:
            bt.logging.debug(f'{ctx}: extend timeout failed: {e}')


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
