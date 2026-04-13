"""Validator forward pass - scoring entry point."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from statistics import mean
from typing import TYPE_CHECKING, Dict, Set, Tuple

import bittensor as bt
import numpy as np

from allways.chain_providers.base import ProviderUnreachableError
from allways.classes import MinerScoringStats, SwapStatus
from allways.constants import (
    EXTEND_THRESHOLD_BLOCKS,
    SCORING_INTERVAL_STEPS,
    SCORING_SUCCESS_EXPONENT,
    SCORING_WINDOW_BLOCKS,
)
from allways.contract_client import ContractError
from allways.utils.logging import log_on_change
from allways.validator.axon_handlers import (
    _keccak256,
    _scale_encode_extend_hash_input,
    _scale_encode_initiate_hash_input,
)
from allways.validator.chain_verification import SwapVerifier
from allways.validator.recycle import apply_recycle
from allways.validator.swap_tracker import SwapTracker
from allways.validator.utils.fees import swap_fee_rao
from allways.validator.voting import SwapVoter

if TYPE_CHECKING:
    from neurons.validator import Validator


async def forward(self: Validator) -> None:
    """Main validator forward pass.

    Called by BaseValidatorNeuron.concurrent_forward() each step.

    Flow:
    1. Process pending confirmations (queued by axon handler, awaiting tx confirmations)
    2. Poll tracker for new/updated swaps (incremental)
    3. For FULFILLED swaps, verify both sides -> confirm_swap
    4. For FULFILLED swaps near timeout with unconfirmed dest tx -> extend timeout
    5. For ACTIVE/FULFILLED past timeout -> timeout_swap (single trigger)
    6. Every SCORING_INTERVAL_STEPS, score from in-memory window
    """
    bt.logging.info(f'Forward step {self.step}')

    tracker: SwapTracker = self.swap_tracker
    verifier: SwapVerifier = self.swap_verifier
    voter: SwapVoter = self.swap_voter

    _clear_provider_caches(self)
    _process_pending_confirms(self)
    await tracker.poll(self.block)
    uncertain = await _verify_fulfilled(tracker, verifier, voter, self.block)
    _extend_near_timeout_fulfilled(self)
    _timeout_expired(self, tracker, voter, uncertain)

    if self.step % SCORING_INTERVAL_STEPS == 0:
        _score_miners(self, tracker)


def _clear_provider_caches(self: Validator) -> None:
    """Clear per-poll caches on chain providers."""
    for provider in self.chain_providers.values():
        if hasattr(provider, 'clear_cache'):
            provider.clear_cache()


def _try_extend_reservation(self: Validator, item, current_block: int, swap_label: str, miner_short: str) -> None:
    """Vote to extend reservation if nearing expiry, protecting users during provider outages."""
    from substrateinterface import Keypair

    try:
        reserved_until = self.contract_client.get_miner_reserved_until(item.miner_hotkey)
        blocks_left = reserved_until - current_block
        if reserved_until < current_block + EXTEND_THRESHOLD_BLOCKS:
            miner_bytes = bytes.fromhex(Keypair(ss58_address=item.miner_hotkey).public_key.hex())
            extend_hash = _keccak256(_scale_encode_extend_hash_input(miner_bytes, item.source_tx_hash))
            self.contract_client.vote_extend_reservation(
                wallet=self.wallet,
                request_hash=extend_hash,
                miner_hotkey=item.miner_hotkey,
                source_tx_hash=item.source_tx_hash,
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


def _process_pending_confirms(self: Validator) -> None:
    """Check queued unconfirmed txs and vote_initiate when confirmations are met."""
    from substrateinterface import Keypair

    items = self.pending_confirms.get_all()
    if not items:
        return

    current_block = self.block

    for item in items:
        swap_label = f'{item.source_chain.upper()}->{item.dest_chain.upper()}'
        try:
            uid = self.metagraph.hotkeys.index(item.miner_hotkey)
        except ValueError:
            uid = '?'
        miner_short = f'UID {uid} ({item.miner_hotkey[:8]})'
        chain_def = self.chain_providers.get(item.source_chain)
        min_confs = chain_def.get_chain().min_confirmations if chain_def else '?'

        # Skip if swap already initiated (another validator reached quorum)
        try:
            if self.contract_client.get_miner_has_active_swap(item.miner_hotkey):
                self.pending_confirms.remove(item.miner_hotkey)
                bt.logging.info(f'PendingConfirm [{swap_label} {miner_short}]: already has active swap, dropping')
                continue
        except Exception as e:
            bt.logging.warning(f'PendingConfirm [{swap_label} {miner_short}]: active swap check failed: {e}')

        # Re-verify tx with main-loop chain provider
        provider = self.chain_providers.get(item.source_chain)
        if provider is None:
            self.pending_confirms.remove(item.miner_hotkey)
            bt.logging.warning(
                f'PendingConfirm [{swap_label} {miner_short}]: no provider for {item.source_chain}, dropping'
            )
            continue

        try:
            tx_info = provider.verify_transaction(
                tx_hash=item.source_tx_hash,
                expected_recipient=item.miner_deposit_address,
                expected_amount=item.source_amount,
            )
        except ProviderUnreachableError as e:
            bt.logging.warning(f'PendingConfirm [{swap_label} {miner_short}]: provider unreachable, will retry: {e}')
            _try_extend_reservation(self, item, current_block, swap_label, miner_short)
            continue
        except Exception as e:
            bt.logging.error(f'PendingConfirm [{swap_label} {miner_short}]: verify_transaction error: {e}')
            continue

        if tx_info is None:
            self.pending_confirms.remove(item.miner_hotkey)
            bt.logging.warning(
                f'PendingConfirm [{swap_label} {miner_short}]: tx {item.source_tx_hash[:16]}... not found, dropping'
            )
            continue

        log_on_change(
            f'confs:{item.miner_hotkey}',
            tx_info.confirmations,
            f'PendingConfirm [{swap_label} {miner_short}]: '
            f'{tx_info.confirmations}/{min_confs} confirmations, tx={item.source_tx_hash[:16]}...',
        )

        _try_extend_reservation(self, item, current_block, swap_label, miner_short)

        if not tx_info.confirmed:
            continue

        # Confirmed — compute hash and vote
        self.pending_confirms.remove(item.miner_hotkey)
        try:
            miner_bytes = bytes.fromhex(Keypair(ss58_address=item.miner_hotkey).public_key.hex())
            hash_input = _scale_encode_initiate_hash_input(
                miner_bytes,
                item.source_tx_hash,
                item.source_chain,
                item.dest_chain,
                item.miner_deposit_address,
                item.miner_dest_address,
                item.rate_str,
                item.tao_amount,
                item.source_amount,
                item.dest_amount,
            )
            request_hash = _keccak256(hash_input)

            user_tao_address = item.dest_address if item.dest_chain == 'tao' else item.source_address
            self.contract_client.vote_initiate(
                wallet=self.wallet,
                request_hash=request_hash,
                user_hotkey=user_tao_address,
                miner_hotkey=item.miner_hotkey,
                source_chain=item.source_chain,
                dest_chain=item.dest_chain,
                source_amount=item.source_amount,
                tao_amount=item.tao_amount,
                user_source_address=item.source_address,
                user_dest_address=item.dest_address,
                source_tx_hash=item.source_tx_hash,
                source_tx_block=tx_info.block_number or 0,
                dest_amount=item.dest_amount,
                miner_source_address=item.miner_deposit_address,
                miner_dest_address=item.miner_dest_address,
                rate=item.rate_str,
            )
            bt.logging.success(
                f'PendingConfirm [{swap_label} {miner_short}]: '
                f'confirmed! voted initiate (tao={item.tao_amount / 1e9:.4f})'
            )
        except ContractError as e:
            if 'ContractReverted' in str(e):
                bt.logging.info(
                    f'PendingConfirm [{swap_label} {miner_short}]: contract rejected (likely already initiated)'
                )
            else:
                bt.logging.error(f'PendingConfirm [{swap_label} {miner_short}]: vote_initiate failed: {e}')
        except Exception as e:
            bt.logging.error(f'PendingConfirm [{swap_label} {miner_short}]: unexpected error: {e}')


async def _verify_fulfilled(
    tracker: SwapTracker,
    verifier: SwapVerifier,
    voter: SwapVoter,
    current_block: int,
) -> Set[int]:
    """Verify FULFILLED swaps; returns IDs where provider was unreachable so _timeout_expired skips them."""
    uncertain: Set[int] = set()
    fulfilled = [s for s in tracker.get_fulfilled(current_block) if not tracker.is_voted(s.id)]
    if not fulfilled:
        return uncertain

    results = await asyncio.gather(
        *[verifier.is_swap_complete(swap) for swap in fulfilled],
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
                tracker.mark_voted(swap.id)
                _resolve_after_vote(tracker, swap, SwapStatus.COMPLETED, current_block)
                bt.logging.success(f'Swap {swap.id}: verified complete, confirmed')
    return uncertain


def _resolve_after_vote(tracker: SwapTracker, swap, terminal_status: SwapStatus, current_block: int) -> None:
    """Persist terminal outcomes promptly after a successful vote.

    If the contract still returns the swap, prefer that payload. If the swap is
    no longer queryable (resolved entries are pruned on-chain), persist a local
    terminal snapshot to preserve scoring continuity across restarts.
    """
    latest = None
    try:
        latest = tracker.client.get_swap(swap.id)
    except Exception as e:
        bt.logging.debug(f'Swap {swap.id}: post-vote refresh failed, deferring resolve persistence: {e}')
        return

    if latest is not None:
        if latest.status in (SwapStatus.COMPLETED, SwapStatus.TIMED_OUT):
            tracker.resolve(latest)
        return

    tracker.resolve(replace(swap), status=terminal_status, current_block=current_block)


def _extend_near_timeout_fulfilled(self: Validator) -> None:
    """Extend timeout for FULFILLED swaps where dest tx exists but isn't confirmed yet.

    Mirrors reservation extension logic: when a swap is nearing timeout but the
    miner has sent the dest funds (tx visible on-chain), vote to extend the timeout
    so the transaction has time to confirm.
    """
    tracker: SwapTracker = self.swap_tracker
    voter: SwapVoter = self.swap_voter
    current_block = self.block

    for swap in tracker.get_near_timeout_fulfilled(current_block, EXTEND_THRESHOLD_BLOCKS):
        swap_label = f'{swap.source_chain.upper()}->{swap.dest_chain.upper()}'
        ctx = f'Swap #{swap.id} [{swap_label}]'

        # Check if dest tx exists on-chain (even if unconfirmed)
        provider = self.chain_providers.get(swap.dest_chain)
        if not provider or not swap.dest_tx_hash:
            continue

        try:
            tx_info = provider.verify_transaction(
                tx_hash=swap.dest_tx_hash,
                expected_recipient=swap.user_dest_address,
                expected_amount=swap.dest_amount,
                block_hint=swap.dest_tx_block,
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


def _timeout_expired(self: Validator, tracker: SwapTracker, voter: SwapVoter, uncertain_swaps: Set[int]) -> None:
    """Timeout expired swaps, skipping uncertain_swaps where the provider was unreachable this cycle."""
    for swap in tracker.get_timed_out(self.block):
        if tracker.is_voted(swap.id):
            continue
        if swap.id in uncertain_swaps:
            bt.logging.warning(f'Swap {swap.id}: deferring timeout, provider was unreachable')
            continue

        if voter.timeout_swap(swap.id):
            tracker.mark_voted(swap.id)
            _resolve_after_vote(tracker, swap, SwapStatus.TIMED_OUT, self.block)
            bt.logging.warning(f'Swap {swap.id}: timed out')


def _score_miners(self: Validator, tracker: SwapTracker) -> None:
    """Score miners from the in-memory window and update weights."""
    try:
        tracker.prune_window(self.block)
        rewards, miner_uids = calculate_miner_rewards(self, tracker)
        rewards, miner_uids = apply_recycle(self, rewards, miner_uids, tracker)
        if len(miner_uids) > 0 and len(rewards) > 0:
            self.update_scores(rewards, miner_uids)
    except Exception as e:
        bt.logging.error(f'Scoring failed: {e}')


def calculate_miner_rewards(
    self: Validator,
    tracker: SwapTracker,
) -> Tuple[np.ndarray, Set[int]]:
    """Calculate rewards from the tracker's in-memory scoring window.

    score = success_rate^8 * volume_weight * speed_score
    """
    hotkey_to_uid: Dict[str, int] = {}
    for uid in range(self.metagraph.n.item()):
        hotkey_to_uid[self.metagraph.hotkeys[uid]] = uid

    stats: Dict[int, MinerScoringStats] = {}

    for swap in tracker.window:
        uid = hotkey_to_uid.get(swap.miner_hotkey)
        if uid is None:
            continue

        if uid not in stats:
            stats[uid] = MinerScoringStats(uid=uid)

        if swap.status == SwapStatus.COMPLETED:
            stats[uid].windowed_fees += swap_fee_rao(swap, self.fee_divisor)
            stats[uid].completed += 1
            if swap.fulfilled_block > 0:
                chain = swap.dest_chain
                if chain not in stats[uid].fulfillment_times_by_chain:
                    stats[uid].fulfillment_times_by_chain[chain] = []
                stats[uid].fulfillment_times_by_chain[chain].append(swap.fulfilled_block - swap.initiated_block)

        elif swap.status == SwapStatus.TIMED_OUT:
            stats[uid].timeouts += 1

    if not stats:
        return np.array([]), set()

    active_stats = {uid: s for uid, s in stats.items() if s.completed + s.timeouts > 0}
    if not active_stats:
        return np.array([]), set()

    total_fees = sum(s.windowed_fees for s in active_stats.values())
    if total_fees == 0:
        total_fees = 1

    # Per-chain fastest averages: compare miners only against others
    # fulfilling on the same dest chain (BTC vs BTC, TAO vs TAO, etc.)
    all_chains: set = set()
    avg_speeds_by_chain: Dict[str, Dict[int, float]] = {}
    for uid, s in active_stats.items():
        for chain, times in s.fulfillment_times_by_chain.items():
            all_chains.add(chain)
            if chain not in avg_speeds_by_chain:
                avg_speeds_by_chain[chain] = {}
            avg_speeds_by_chain[chain][uid] = mean(times)

    fastest_by_chain: Dict[str, float] = {chain: min(speeds.values()) for chain, speeds in avg_speeds_by_chain.items()}

    uids = sorted(active_stats.keys())
    rewards = np.zeros(len(uids), dtype=np.float32)

    for i, uid in enumerate(uids):
        s = active_stats[uid]
        total = s.completed + s.timeouts
        success_rate = (s.completed / total) ** SCORING_SUCCESS_EXPONENT
        volume_weight = s.windowed_fees / total_fees
        speed_score = _chain_weighted_speed(uid, s, fastest_by_chain, avg_speeds_by_chain)
        rewards[i] = success_rate * volume_weight * speed_score

    bt.logging.info(
        f'Windowed scoring: {len(uids)} miners, window={SCORING_WINDOW_BLOCKS} blocks, '
        f'total_fees={total_fees}, fastest_by_chain={fastest_by_chain}'
    )

    return rewards, set(uids)


def _chain_weighted_speed(
    uid: int,
    s: MinerScoringStats,
    fastest_by_chain: Dict[str, float],
    avg_speeds_by_chain: Dict[str, Dict[int, float]],
) -> float:
    """Compute speed score weighted across dest chains.

    Each chain's speed score = fastest_avg / miner_avg (within that chain).
    Final score is a weighted average by swap count per chain.
    """
    total_swaps = 0
    weighted_sum = 0.0

    for chain, times in s.fulfillment_times_by_chain.items():
        fastest = fastest_by_chain.get(chain, 0)
        miner_avg = avg_speeds_by_chain.get(chain, {}).get(uid, 0)
        if fastest > 0 and miner_avg > 0:
            chain_score = fastest / miner_avg
            count = len(times)
            weighted_sum += chain_score * count
            total_swaps += count

    return weighted_sum / total_swaps if total_swaps > 0 else 0.0
