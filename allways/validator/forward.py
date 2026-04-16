"""Validator forward pass — the orchestrator called every step by the base neuron.

The forward loop does all the per-step validator work (rate sampling, event
sync, pending-confirm drain, fulfillment verification, timeout enforcement).
Scoring lives in its own module (``allways.validator.scoring``) and is invoked
only on the periodic scoring interval.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Set

import bittensor as bt

from allways.chain_providers.base import ProviderUnreachableError
from allways.classes import SwapStatus
from allways.commitments import read_miner_commitments
from allways.constants import EXTEND_THRESHOLD_BLOCKS, SCORING_WINDOW_BLOCKS
from allways.contract_client import ContractError, is_contract_rejection
from allways.utils.logging import log_on_change
from allways.validator import voting
from allways.validator.axon_handlers import (
    keccak256,
    scale_encode_extend_hash_input,
    scale_encode_initiate_hash_input,
)
from allways.validator.chain_verification import SwapVerifier
from allways.validator.scoring import run_scoring_pass
from allways.validator.state_store import PendingConfirm
from allways.validator.swap_tracker import SwapTracker

if TYPE_CHECKING:
    from neurons.validator import Validator


async def forward(self: Validator) -> None:
    """One validator forward step.

    Every step is the same flow, organized into numbered phases below. Each
    phase updates validator state the next phase may depend on, so ordering
    matters — don't reshuffle without re-reading the dependencies.
    """
    bt.logging.info(f'Forward step {self.step}')

    tracker: SwapTracker = self.swap_tracker
    verifier: SwapVerifier = self.swap_verifier

    # 1. House-keeping — clear per-step chain-provider caches and drop any
    #    pending confirms whose reservation has already expired.
    clear_provider_caches(self)
    self.state_store.purge_expired_pending()

    # 2. Pending confirms → vote_initiate — drain the axon-fed queue of
    #    user swaps whose source tx has reached enough confirmations.
    initialize_pending_user_reservations(self)

    # 3. Rate sampling — read every miner commitment in a single query_map
    #    RPC and persist direction-level diffs into rate_events (the input
    #    to crown-time scoring).
    poll_commitments(self)

    # 4. Event sync — replay Contracts::ContractEmitted events for collateral,
    #    active flag, min_collateral, swap outcomes, and busy intervals.
    try:
        self.event_watcher.sync_to(self.block)
    except Exception as e:
        bt.logging.warning(f'Event watcher sync failed: {e}')

    # 5. Swap tracker refresh — pull newly-initiated and resolved swaps.
    await tracker.poll(self.block)

    # 6. Fulfillment confirm — verify FULFILLED swaps end-to-end and vote
    #    confirm_swap. Returns the swaps where the provider was unreachable
    #    this cycle so the timeout phase knows to skip them (transient
    #    outage shouldn't trigger a slash).
    uncertain_swaps = await confirm_miner_fulfillments(self, tracker, verifier, self.block)

    # 7. Timeout extend — for FULFILLED swaps nearing deadline with a dest tx
    #    visible on-chain, vote to extend the timeout so the tx has time to
    #    confirm.
    extend_fulfilled_near_timeout(self)

    # 8. Timeout enforce — slash FULFILLED/ACTIVE swaps past their deadline,
    #    skipping the uncertain set from step 6.
    enforce_swap_timeouts(self, tracker, uncertain_swaps)

    # 9. Scoring (periodic) — once per scoring window. Replays the crown-time
    #    window and commits miner weights.
    if self.step % SCORING_WINDOW_BLOCKS == 0:
        run_scoring_pass(self)


# ─── Step 1: House-keeping ──────────────────────────────────────────────


def clear_provider_caches(self: Validator) -> None:
    """Clear per-poll caches on chain providers."""
    for provider in self.chain_providers.values():
        if hasattr(provider, 'clear_cache'):
            provider.clear_cache()


# ─── Step 2: Pending confirms → vote_initiate ───────────────────────────


def initialize_pending_user_reservations(self: Validator) -> None:
    """Check queued unconfirmed txs and vote_initiate when confirmations are met."""
    from substrateinterface import Keypair

    items = self.state_store.get_all()
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

        # Skip if swap already initiated (another validator reached quorum)
        try:
            if self.contract_client.get_miner_has_active_swap(item.miner_hotkey):
                self.state_store.remove(item.miner_hotkey)
                bt.logging.info(f'PendingConfirm [{swap_label} {miner_short}]: already has active swap, dropping')
                continue
        except Exception as e:
            bt.logging.warning(f'PendingConfirm [{swap_label} {miner_short}]: active swap check failed: {e}')

        # Re-verify tx with main-loop chain provider
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
            )
        except ProviderUnreachableError as e:
            bt.logging.warning(f'PendingConfirm [{swap_label} {miner_short}]: provider unreachable, will retry: {e}')
            try_extend_reservation(self, item, current_block, swap_label, miner_short)
            continue
        except Exception as e:
            bt.logging.error(f'PendingConfirm [{swap_label} {miner_short}]: verify_transaction error: {e}')
            continue

        if tx_info is None:
            self.state_store.remove(item.miner_hotkey)
            bt.logging.warning(
                f'PendingConfirm [{swap_label} {miner_short}]: tx {item.from_tx_hash[:16]}... not found, dropping'
            )
            continue

        log_on_change(
            f'confs:{item.miner_hotkey}',
            tx_info.confirmations,
            f'PendingConfirm [{swap_label} {miner_short}]: '
            f'{tx_info.confirmations}/{min_confs} confirmations, tx={item.from_tx_hash[:16]}...',
        )

        try_extend_reservation(self, item, current_block, swap_label, miner_short)

        if not tx_info.confirmed:
            continue

        # Confirmed — compute hash and vote. Only drop the queued entry once the
        # vote is accepted (or the contract tells us someone else already
        # initiated it). On transient RPC/network failure we leave the entry in
        # place so the next forward step retries instead of silently losing it.
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
                # Contract rejected — in practice this means another validator
                # already reached initiate quorum, so the entry is no longer
                # actionable. Drop it.
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
    from substrateinterface import Keypair

    try:
        reserved_until = self.contract_client.get_miner_reserved_until(item.miner_hotkey)
        blocks_left = reserved_until - current_block
        if reserved_until < current_block + EXTEND_THRESHOLD_BLOCKS:
            miner_bytes = bytes.fromhex(Keypair(ss58_address=item.miner_hotkey).public_key.hex())
            extend_hash = keccak256(scale_encode_extend_hash_input(miner_bytes, item.from_tx_hash))
            self.contract_client.vote_extend_reservation(
                wallet=self.wallet,
                request_hash=extend_hash,
                miner_hotkey=item.miner_hotkey,
                from_tx_hash=item.from_tx_hash,
            )
            bt.logging.info(
                f'PendingConfirm [{swap_label} {miner_short}]: '
                f'voted to extend reservation ({blocks_left} blocks remaining)'
            )
    except ContractError as e:
        if 'AlreadyVoted' not in str(e):
            bt.logging.debug(f'PendingConfirm [{swap_label} {miner_short}]: extend vote: {e}')
    except Exception as e:
        bt.logging.debug(f'PendingConfirm [{swap_label} {miner_short}]: extend check failed: {e}')


# ─── Step 3: Commitment polling ─────────────────────────────────────────


def poll_commitments(self: Validator) -> None:
    """Rate-side validator tick.

    Fires every forward step because ``read_miner_commitments`` is a single
    ``query_map`` RPC — the cost is one round-trip regardless of miner count,
    so per-block sampling is cheap and gives the crown-time series its
    tightest possible accuracy (~1 block granularity).

    Two steps:

    1. ``refresh_miner_rates`` — pull the current commitment snapshot and
       persist any direction-level diffs vs the in-memory cache.
    2. ``purge_deregistered_hotkeys`` — drop any hotkeys that have left the
       metagraph since the last poll.

    Event retention pruning lives in ``run_scoring_pass`` — it's bounded-growth
    hygiene, not correctness, so the once-per-scoring-round cadence is enough.
    """
    refresh_miner_rates(self)
    purge_deregistered_hotkeys(self)


def refresh_miner_rates(self: Validator) -> None:
    """Pull all miner commitments and persist direction-level rate diffs.

    Rate events matching the cached ``last_known_rates`` value are skipped.
    Accepted events update the cache; deduped inserts also update the cache
    so we don't retry the same blocked write on every subsequent poll.
    """
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
    """Drop rates/collateral/outcomes for hotkeys that left the metagraph."""
    current_hotkeys = set(self.metagraph.hotkeys)
    stale = {hk for (hk, _, _) in self.last_known_rates.keys()} - current_hotkeys
    if not stale:
        return
    for hk in stale:
        self.state_store.delete_hotkey(hk)
    self.last_known_rates = {k: v for k, v in self.last_known_rates.items() if k[0] not in stale}


# ─── Steps 6-8: Fulfillment confirm, extend, timeout enforce ────────────


async def confirm_miner_fulfillments(
    self: Validator,
    tracker: SwapTracker,
    verifier: SwapVerifier,
    current_block: int,
) -> Set[int]:
    """Verify FULFILLED swaps; returns IDs where provider was unreachable so enforce_swap_timeouts skips them."""
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
    """Extend timeout for FULFILLED swaps where dest tx exists but isn't confirmed yet.

    Mirrors reservation extension logic: when a swap is nearing timeout but the
    miner has sent the dest funds (tx visible on-chain), vote to extend the
    timeout so the transaction has time to confirm.
    """
    tracker: SwapTracker = self.swap_tracker
    current_block = self.block

    for swap in tracker.get_near_timeout_fulfilled(current_block, EXTEND_THRESHOLD_BLOCKS):
        swap_label = f'{swap.from_chain.upper()}->{swap.to_chain.upper()}'
        ctx = f'Swap #{swap.id} [{swap_label}]'

        # Check if dest tx exists on-chain (even if unconfirmed)
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
            continue  # dest tx not found — don't extend, let it time out

        blocks_left = swap.timeout_block - current_block
        chain_def = provider.get_chain()
        log_on_change(
            f'dest_confs:{swap.id}',
            tx_info.confirmations,
            f'{ctx}: {tx_info.confirmations}/{chain_def.min_confirmations} dest confirmations, '
            f'{blocks_left} blocks until timeout',
        )

        # Dest tx exists (confirmed or not) — vote to extend timeout
        try:
            if voting.extend_swap_timeout(self.contract_client, self.wallet, swap.id):
                bt.logging.info(
                    f'{ctx}: voted to extend timeout '
                    f'({tx_info.confirmations}/{chain_def.min_confirmations} dest confirmations)'
                )
        except ContractError as e:
            if 'AlreadyVoted' not in str(e) and not is_contract_rejection(e):
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
