"""Validator forward pass - scoring entry point."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Dict, List, Optional, Set, Tuple

import bittensor as bt
import numpy as np

from allways.chain_providers.base import ProviderUnreachableError
from allways.classes import SwapStatus
from allways.commitments import read_miner_commitments
from allways.constants import (
    COMMITMENT_POLL_INTERVAL_BLOCKS,
    DIRECTION_POOLS,
    EVENT_RETENTION_BLOCKS,
    EXTEND_THRESHOLD_BLOCKS,
    RECYCLE_UID,
    SCORING_INTERVAL_STEPS,
    SCORING_WINDOW_BLOCKS,
    SUCCESS_EXPONENT,
)
from allways.contract_client import ContractError
from allways.utils.logging import log_on_change
from allways.validator.axon_handlers import (
    keccak256,
    scale_encode_extend_hash_input,
    scale_encode_initiate_hash_input,
)
from allways.validator.chain_verification import SwapVerifier
from allways.validator.state_store import ValidatorStateStore
from allways.validator.swap_tracker import SwapTracker
from allways.validator.voting import SwapVoter

if TYPE_CHECKING:
    from neurons.validator import Validator


async def forward(self: Validator) -> None:
    """Main validator forward pass.

    Called by BaseValidatorNeuron.concurrent_forward() each step.

    Flow:
    1. Process pending confirmations (queued by axon handler, awaiting tx confirmations)
    2. Commitment poll (rates)
    3. Event watcher sync (collateral, active flag, min_collateral, swap outcomes)
    4. Poll tracker for new/updated swaps (incremental)
    5. For FULFILLED swaps, verify both sides -> confirm_swap
    6. For FULFILLED swaps near timeout with unconfirmed dest tx -> extend timeout
    7. For ACTIVE/FULFILLED past timeout -> timeout_swap (single trigger)
    8. Every SCORING_INTERVAL_STEPS, score from in-memory window
    """
    bt.logging.info(f'Forward step {self.step}')

    tracker: SwapTracker = self.swap_tracker
    verifier: SwapVerifier = self.swap_verifier
    voter: SwapVoter = self.swap_voter

    clear_provider_caches(self)
    initialize_pending_user_reservations(self)
    poll_commitments(self)
    try:
        self.event_watcher.sync_to(self.block)
    except Exception as e:
        bt.logging.warning(f'Event watcher sync failed: {e}')
    await tracker.poll(self.block)
    uncertain = await confirm_miner_fulfillments(tracker, verifier, voter, self.block)
    extend_fulfilled_near_timeout(self)
    enforce_swap_timeouts(self, tracker, voter, uncertain)

    if self.step % SCORING_INTERVAL_STEPS == 0:
        run_scoring_pass(self)


def clear_provider_caches(self: Validator) -> None:
    """Clear per-poll caches on chain providers."""
    for provider in self.chain_providers.values():
        if hasattr(provider, 'clear_cache'):
            provider.clear_cache()


def poll_commitments(self: Validator) -> None:
    """Rate-side validator tick.

    Three independent steps run at ``COMMITMENT_POLL_INTERVAL_BLOCKS`` cadence:

    1. ``prune_aged_rate_events`` — trim history older than the retention
       window so the SQLite tables stay bounded.
    2. ``refresh_miner_rates`` — read all miner commitments from the local
       subtensor and persist direction-level diffs.
    3. ``purge_deregistered_hotkeys`` — drop any hotkeys that have left the
       metagraph since the last poll, both from the store and the in-memory
       cache.

    Kept as a thin orchestrator so each concern can be tested and reasoned
    about independently.
    """
    if self.block - self.last_commitment_poll_block < COMMITMENT_POLL_INTERVAL_BLOCKS:
        return
    self.last_commitment_poll_block = self.block

    prune_aged_rate_events(self)
    refresh_miner_rates(self)
    purge_deregistered_hotkeys(self)


def prune_aged_rate_events(self: Validator) -> None:
    """Delete rate/collateral events older than ``EVENT_RETENTION_BLOCKS``.

    Retention is deliberately 2× the scoring window so ``get_latest_*_before``
    calls at the window start can always find prior state to reconstruct from.
    """
    cutoff = self.block - EVENT_RETENTION_BLOCKS
    if cutoff > 0:
        self.state_store.prune_events_older_than(cutoff)


def refresh_miner_rates(self: Validator) -> None:
    """Pull all miner commitments and persist direction-level rate diffs.

    Rate events that match the cached ``_last_known_rates`` value are skipped
    entirely. Rate events accepted by the store update the cache; throttled or
    deduped inserts still update the cache so we don't repeatedly retry the
    same blocked write on every subsequent poll.
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


def try_extend_reservation(self: Validator, item, current_block: int, swap_label: str, miner_short: str) -> None:
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

        if tx_info.sender and tx_info.sender != item.from_address:
            self.state_store.remove(item.miner_hotkey)
            bt.logging.warning(
                f'PendingConfirm [{swap_label} {miner_short}]: sender mismatch '
                f'(expected {item.from_address}, got {tx_info.sender}), dropping'
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
            if 'ContractReverted' in str(e):
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


async def confirm_miner_fulfillments(
    tracker: SwapTracker,
    verifier: SwapVerifier,
    voter: SwapVoter,
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
            if voter.confirm_swap(swap.id):
                tracker.resolve(swap.id, SwapStatus.COMPLETED, current_block)
                bt.logging.success(f'Swap {swap.id}: verified complete, confirmed')
    return uncertain


def extend_fulfilled_near_timeout(self: Validator) -> None:
    """Extend timeout for FULFILLED swaps where dest tx exists but isn't confirmed yet.

    Mirrors reservation extension logic: when a swap is nearing timeout but the
    miner has sent the dest funds (tx visible on-chain), vote to extend the timeout
    so the transaction has time to confirm.
    """
    tracker: SwapTracker = self.swap_tracker
    voter: SwapVoter = self.swap_voter
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
            if voter.extend_timeout(swap.id):
                bt.logging.info(
                    f'{ctx}: voted to extend timeout '
                    f'({tx_info.confirmations}/{chain_def.min_confirmations} dest confirmations)'
                )
        except ContractError as e:
            if 'AlreadyVoted' not in str(e) and 'ContractReverted' not in str(e):
                bt.logging.debug(f'{ctx}: extend timeout vote: {e}')
        except Exception as e:
            bt.logging.debug(f'{ctx}: extend timeout failed: {e}')


def enforce_swap_timeouts(self: Validator, tracker: SwapTracker, voter: SwapVoter, uncertain_swaps: Set[int]) -> None:
    """Timeout expired swaps, skipping uncertain_swaps where the provider was unreachable this cycle."""
    for swap in tracker.get_timed_out(self.block):
        if tracker.is_voted(swap.id):
            continue
        if swap.id in uncertain_swaps:
            bt.logging.warning(f'Swap {swap.id}: deferring timeout, provider was unreachable')
            continue

        if voter.timeout_swap(swap.id):
            tracker.resolve(swap.id, SwapStatus.TIMED_OUT, self.block)
            bt.logging.warning(f'Swap {swap.id}: timed out')


def run_scoring_pass(self: Validator) -> None:
    """Run a V1 scoring pass and commit weights."""
    try:
        rewards, miner_uids = calculate_miner_rewards(self)
        if len(miner_uids) > 0 and len(rewards) > 0:
            self.update_scores(rewards, miner_uids)
    except Exception as e:
        bt.logging.error(f'Scoring failed: {e}')


def calculate_miner_rewards(self: Validator) -> Tuple[np.ndarray, Set[int]]:
    """Crown-time based reward computation.

    For each direction in ``DIRECTION_POOLS``:
      1. Replay rate events (from state_store) and collateral events (from
         event_watcher) chronologically over the window
      2. At each block boundary, determine crown holders (tied best-rate miners
         that are in the metagraph AND active on-chain AND have
         collateral >= the event-watcher's cached ``min_collateral``)
      3. Accumulate crown_blocks per hotkey, splitting evenly on ties
      4. ``rewards[uid] += pool * (crown_blocks[hk] / total) * success_rate ** SUCCESS_EXPONENT``

    Anything not distributed to miners recycles to ``RECYCLE_UID``.
    """
    n_uids = self.metagraph.n.item()
    if n_uids == 0:
        return np.array([], dtype=np.float32), set()

    window_end = self.block
    window_start = max(0, window_end - SCORING_WINDOW_BLOCKS)

    # Miners must be both in the metagraph (registered) AND active on the
    # contract (miner_active == true). Active is sourced from MinerActivated
    # events replayed by the watcher.
    in_metagraph: Set[str] = set(self.metagraph.hotkeys)
    eligible_hotkeys: Set[str] = in_metagraph & self.event_watcher.active_miners
    hotkey_to_uid: Dict[str, int] = {self.metagraph.hotkeys[uid]: uid for uid in range(n_uids)}

    rewards = np.zeros(n_uids, dtype=np.float32)
    success_stats = self.state_store.get_all_time_success_rates()
    min_collateral = int(self.event_watcher.min_collateral or 0)

    for (from_chain, to_chain), pool in DIRECTION_POOLS.items():
        crown_blocks = replay_crown_time_window(
            store=self.state_store,
            event_watcher=self.event_watcher,
            from_chain=from_chain,
            to_chain=to_chain,
            window_start=window_start,
            window_end=window_end,
            eligible_hotkeys=eligible_hotkeys,
            min_collateral=min_collateral,
        )
        total = sum(crown_blocks.values())
        if total == 0:
            continue  # empty bucket — pool recycles via the remainder below

        for hotkey, blocks in crown_blocks.items():
            uid = hotkey_to_uid.get(hotkey)
            if uid is None:
                continue  # dereg'd mid-window; credit forfeited
            share = blocks / total
            sr = success_rate(success_stats.get(hotkey))
            rewards[uid] += pool * share * (sr**SUCCESS_EXPONENT)

    recycle_uid = RECYCLE_UID if RECYCLE_UID < n_uids else 0
    distributed = float(rewards.sum())
    rewards[recycle_uid] += max(0.0, 1.0 - distributed)

    bt.logging.info(
        f'V1 scoring: window=[{window_start}, {window_end}], '
        f'distributed={distributed:.6f}, recycled={max(0.0, 1.0 - distributed):.6f}'
    )

    return rewards, set(range(n_uids))


def success_rate(stats: Optional[Tuple[int, int]]) -> float:
    """All-time success rate. Zero-outcome miners default to 1.0 (optimistic)."""
    if stats is None:
        return 1.0
    completed, timed_out = stats
    total = completed + timed_out
    if total == 0:
        return 1.0
    return completed / total


def replay_crown_time_window(
    store: ValidatorStateStore,
    event_watcher,
    from_chain: str,
    to_chain: str,
    window_start: int,
    window_end: int,
    eligible_hotkeys: Set[str],
    min_collateral: int,
) -> Dict[str, float]:
    """Walk the merged rate + collateral event stream, accumulate crown blocks.

    Rates come from ``store`` (populated by commitment polling). Collateral
    history comes from ``event_watcher`` (populated by contract event replay).
    Returns ``{hotkey: crown_blocks_float}``. Ties split credit evenly across
    the tied interval.
    """
    # 1. Reconstruct state at window_start for every eligible hotkey.
    current_rates: Dict[str, float] = {}
    current_collateral: Dict[str, int] = {}

    for hotkey in eligible_hotkeys:
        latest_rate = store.get_latest_rate_before(hotkey, from_chain, to_chain, window_start)
        if latest_rate is not None:
            current_rates[hotkey] = latest_rate[0]
        latest_col = event_watcher.get_latest_collateral_before(hotkey, window_start)
        if latest_col is not None:
            current_collateral[hotkey] = latest_col[0]
        else:
            # No event before window_start — fall back to the watcher's
            # current value so a miner whose only collateral event predates
            # the retention window still gets credited accurately.
            snapshot = event_watcher.collateral.get(hotkey)
            if snapshot is not None:
                current_collateral[hotkey] = snapshot

    # 2. Merge rate and collateral events within the window, oldest first.
    #    Collateral events sort BEFORE rate events at the same block so a
    #    simultaneous "collateral drops + best rate" transition resolves to
    #    the post-drop state before rate attribution.
    rate_events = store.get_rate_events_in_range(from_chain, to_chain, window_start, window_end)
    col_events = event_watcher.get_collateral_events_in_range(window_start, window_end)

    merged: List[Tuple[int, int, str, str, float]] = []
    for e in rate_events:
        merged.append((e['block'], 1, 'rate', e['hotkey'], float(e['rate'])))
    for e in col_events:
        merged.append((e['block'], 0, 'collateral', e['hotkey'], float(e['collateral_rao'])))
    merged.sort(key=lambda x: (x[0], x[1]))

    # 3. Walk intervals, crediting current holders.
    crown_blocks: Dict[str, float] = {}
    prev_block = window_start

    def attribute(interval_start: int, interval_end: int) -> None:
        duration = interval_end - interval_start
        if duration <= 0:
            return
        holders = crown_holders_at_instant(current_rates, current_collateral, min_collateral, eligible_hotkeys)
        if not holders:
            return
        split = duration / len(holders)
        for hk in holders:
            crown_blocks[hk] = crown_blocks.get(hk, 0.0) + split

    for block, _order, kind, hotkey, value in merged:
        attribute(prev_block, block)
        if kind == 'rate':
            current_rates[hotkey] = value
        else:
            current_collateral[hotkey] = int(value)
        prev_block = block

    attribute(prev_block, window_end)
    return crown_blocks


def crown_holders_at_instant(
    rates: Dict[str, float],
    collaterals: Dict[str, int],
    min_collateral: int,
    eligible: Set[str],
) -> List[str]:
    """Hotkeys tied for best rate, with collateral >= min and eligible."""
    candidates = {
        hk: r for hk, r in rates.items() if hk in eligible and collaterals.get(hk, 0) >= min_collateral and r > 0
    }
    if not candidates:
        return []
    best = max(candidates.values())
    return [hk for hk, r in candidates.items() if r == best]
