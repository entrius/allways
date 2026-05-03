"""Validator forward pass — orchestrator called every step by the base neuron."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Set, Tuple, Type, Union

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

    # Sync events first so ReservationExtensionFinalized writes from the
    # previous block reach state_store before the per-row finalize/init loop
    # reads them.
    try:
        self.event_watcher.sync_to(self.block)
        bt.logging.info('forward: events synced')
    except Exception as e:
        bt.logging.warning(f'Event watcher sync failed: {e}')

    # Init runs *before* purge so maybe_finalize_reservation gets a chance to
    # fire on rows whose original reserved_until has just lapsed. A successful
    # finalize bumps state_store.reserved_until in-line (see
    # try_extend_reservation), so the subsequent purge sees the fresh deadline
    # and leaves the row alone. Without this ordering, any propose whose
    # finalize-eligible step lands at-or-after the original reserved_until
    # would be silently lost to the purge before init ever sees the row.
    initialize_pending_user_reservations(self)

    self.state_store.purge_expired_pending_confirms()

    poll_commitments(self)

    # Pull newly-initiated and resolved swaps off the contract.
    await tracker.poll()
    bt.logging.info('forward: tracker polled')

    # Verify FULFILLED swaps end-to-end and vote confirm_swap. The returned
    # set is swap IDs where the provider was unreachable this cycle, so the
    # timeout phase knows to skip them (transient outage shouldn't slash).
    uncertain_swaps = await confirm_miner_fulfillments(self, tracker, verifier, self.block)
    bt.logging.info('forward: fulfillments verified')

    extend_fulfilled_near_timeout(self)
    enforce_swap_timeouts(self, tracker, uncertain_swaps)

    if self.step % SCORING_WINDOW_BLOCKS == 0:
        score_and_reward_miners(self)
        bt.logging.info('forward: scoring done')


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
    # One {hotkey: uid} pass replaces an O(N) list scan per row when sizing
    # up the per-row log prefix.
    hotkey_to_uid = {hk: uid for uid, hk in enumerate(self.metagraph.hotkeys)}

    for item in items:
        swap_label = f'{item.from_chain.upper()}->{item.to_chain.upper()}'
        uid = hotkey_to_uid.get(item.miner_hotkey, '?')
        miner_short = f'UID {uid} ({item.miner_hotkey[:8]})'
        provider = self.chain_providers.get(item.from_chain)
        min_confs = provider.get_chain().min_confirmations if provider else '?'

        # In-memory fast path; only verify with contract before dropping (watcher could be stale).
        if self.event_watcher.open_swap_count.get(item.miner_hotkey, 0) > 0:
            try:
                if self.contract_client.get_miner_has_active_swap(item.miner_hotkey):
                    self.state_store.remove(item.miner_hotkey)
                    bt.logging.info(f'PendingConfirm [{swap_label} {miner_short}]: already has active swap, dropping')
                    continue
            except Exception as e:
                bt.logging.warning(f'PendingConfirm [{swap_label} {miner_short}]: active swap check failed: {e}')

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
    confirmations >= 1; tier 2+: refused).
    """
    # ``current_block`` from the caller was captured at step start; verifying
    # each pending confirm before this one can burn many blocks. Refresh so
    # the EXTEND_THRESHOLD_BLOCKS gate matches the height the propose tx will
    # actually land at, not where the step began.
    try:
        current_block = self.subtensor.get_current_block()
    except Exception as e:
        bt.logging.debug(f'PendingConfirm [{swap_label} {miner_short}]: refresh current_block failed: {e}')

    try:
        reserved_until = self.contract_client.get_miner_reserved_until(item.miner_hotkey)
    except Exception as e:
        bt.logging.debug(f'PendingConfirm [{swap_label} {miner_short}]: reserved_until read failed: {e}')
        reserved_until = item.reserved_until

    # One pending-extension fetch shared across finalize/challenge/propose;
    # used to be three separate RPCs per row per step.
    pending = self.optimistic_extensions.fetch_pending_reservation(item.miner_hotkey)

    finalized_target = self.optimistic_extensions.maybe_finalize_reservation(
        miner_hotkey=item.miner_hotkey,
        current_block=current_block,
        challenge_window_blocks=CHALLENGE_WINDOW_BLOCKS,
        pending=pending,
    )
    if finalized_target is not None:
        # Same-step write so the upstream purge sweep sees the bumped deadline.
        # The matching ReservationExtensionFinalized event won't be picked up
        # until the next forward step's event_watcher.sync_to.
        self.state_store.update_reserved_until(item.miner_hotkey, finalized_target)
        reserved_until = finalized_target
        # The just-finalized proposal is gone from contract storage; refresh
        # so downstream challenge/propose see the post-finalize state instead
        # of a stale "still pending" snapshot.
        pending = None

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
        reserved_until=reserved_until,
        pending=pending,
    )

    try:
        from_tx_hash_bytes = bytes.fromhex(strip_hex_prefix(item.from_tx_hash))
    except ValueError:
        bt.logging.debug(f'PendingConfirm [{swap_label} {miner_short}]: malformed from_tx_hash, skipping propose')
        return
    if len(from_tx_hash_bytes) != 32:
        # The contract's `Hash` parameter is fixed at 32 bytes and the SCALE
        # encoder silently pads/truncates anything else, which would emit an
        # event topic that doesn't match the user's actual tx_hash. Bail.
        bt.logging.debug(
            f'PendingConfirm [{swap_label} {miner_short}]: from_tx_hash is '
            f'{len(from_tx_hash_bytes)}B, expected 32; skipping propose'
        )
        return

    proposed = self.optimistic_extensions.maybe_propose_reservation(
        miner_hotkey=item.miner_hotkey,
        from_chain_id=item.from_chain,
        from_tx_hash=from_tx_hash_bytes,
        current_block=current_block,
        reserved_until=reserved_until,
        observed_confirmations=tx_info.confirmations,
        extension_count=extension_count,
        pending=pending,
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


RECOVERABLE_VERIFY_EXCEPTIONS: Tuple[Type[BaseException], ...] = (
    ProviderUnreachableError,
    asyncio.TimeoutError,
)


async def _verify_swallowing_only_recoverable(
    verifier: SwapVerifier,
    swap,
) -> Union[bool, ProviderUnreachableError, asyncio.TimeoutError]:
    """Run ``verifier.verify_miner_fulfillment`` and convert recoverable
    transport exceptions into return values. Programming errors
    (``AttributeError``, ``KeyError``, ``TypeError``, …) and any other
    unexpected exception types intentionally propagate so the forward
    step fails loud instead of being silently logged as a generic
    "verification error" forever (#178).

    The previous shape — ``asyncio.gather(..., return_exceptions=True)``
    paired with ``isinstance(result, Exception)`` — caught literally
    every exception type and reduced them all to a logged warning,
    which masked real bugs as flaky network conditions.
    """
    try:
        return await verifier.verify_miner_fulfillment(swap)
    except RECOVERABLE_VERIFY_EXCEPTIONS as exc:
        return exc


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

    # Note: no `return_exceptions=True` — only the explicitly-recoverable
    # exception types are caught (in the wrapper above) and returned as
    # values; anything else propagates so it's not silently masked (#178).
    results = await asyncio.gather(
        *[_verify_swallowing_only_recoverable(verifier, swap) for swap in fulfilled],
    )
    for swap, result in zip(fulfilled, results):
        if isinstance(result, ProviderUnreachableError):
            bt.logging.warning(f'Swap {swap.id}: provider unreachable, deferring verification')
            uncertain.add(swap.id)
            continue
        if isinstance(result, asyncio.TimeoutError):
            bt.logging.warning(f'Swap {swap.id}: verification timed out, deferring')
            uncertain.add(swap.id)
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
    per-tier evidence rules.
    """
    tracker: SwapTracker = self.swap_tracker
    current_block = self.block

    for swap in tracker.get_near_timeout_fulfilled(current_block):
        swap_label = f'{swap.from_chain.upper()}->{swap.to_chain.upper()}'
        ctx = f'Swap #{swap.id} [{swap_label}]'

        # One pending-extension fetch shared across finalize/challenge/propose.
        pending = self.optimistic_extensions.fetch_pending_timeout(swap.id)

        finalized_target = self.optimistic_extensions.maybe_finalize_timeout(
            swap_id=swap.id,
            current_block=current_block,
            challenge_window_blocks=CHALLENGE_WINDOW_BLOCKS,
            pending=pending,
        )
        if finalized_target is not None:
            # Same-step write so enforce_swap_timeouts (which runs immediately
            # after this loop) reads the bumped deadline rather than the
            # pre-finalize value the next event sync would otherwise carry in.
            tracker.update_timeout_block(swap.id, finalized_target)
            # Just-finalized proposal is gone from contract storage.
            pending = None

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
            timeout_block=swap.timeout_block,
            pending=pending,
        )
        proposed = self.optimistic_extensions.maybe_propose_timeout(
            swap_id=swap.id,
            dest_chain_id=swap.to_chain,
            current_block=current_block,
            timeout_block=swap.timeout_block,
            observed_confirmations=tx_info.confirmations,
            extension_count=extension_count,
            pending=pending,
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
