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
    COLLATERAL_POLL_INTERVAL_BLOCKS,
    COMMITMENT_POLL_INTERVAL_BLOCKS,
    DIRECTION_POOLS,
    EXTEND_THRESHOLD_BLOCKS,
    MIN_COLLATERAL_REFRESH_INTERVAL_BLOCKS,
    RECYCLE_UID,
    SCORING_INTERVAL_STEPS,
    SCORING_WINDOW_BLOCKS,
    SUCCESS_EXPONENT,
)
from allways.contract_client import ContractError
from allways.utils.logging import log_on_change
from allways.validator.axon_handlers import (
    _keccak256,
    _scale_encode_extend_hash_input,
    _scale_encode_initiate_hash_input,
)
from allways.validator.chain_verification import SwapVerifier
from allways.validator.rate_state import RateStateStore
from allways.validator.swap_tracker import SwapTracker
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
    _poll_commitments(self)
    _refresh_min_collateral(self)
    _poll_collaterals(self)
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


def _poll_commitments(self: Validator) -> None:
    """Read all miner commitments from the local subtensor and persist diffs.

    Runs every ``COMMITMENT_POLL_INTERVAL_BLOCKS``. For each miner pair in the
    metagraph, emits a ``rate_event`` per direction whose rate changed since the
    cache snapshot. The ``RateStateStore`` enforces the per-hotkey throttle.
    Also purges deregistered hotkeys from the store and local cache.
    """
    if self.block - self._last_commitment_poll_block < COMMITMENT_POLL_INTERVAL_BLOCKS:
        return
    self._last_commitment_poll_block = self.block

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
            (pair.source_chain, pair.dest_chain, pair.rate),
            (pair.dest_chain, pair.source_chain, pair.counter_rate),
        ):
            if r <= 0:
                continue  # miner opted out of this direction
            key = (pair.hotkey, from_c, to_c)
            if self._last_known_rates.get(key) == r:
                continue
            inserted = self.rate_state_store.insert_rate_event(
                hotkey=pair.hotkey,
                from_chain=from_c,
                to_chain=to_c,
                rate=r,
                block=self.block,
            )
            if inserted:
                self._last_known_rates[key] = r

    stale = {hk for (hk, _, _) in self._last_known_rates.keys()} - current_hotkeys
    for hk in stale:
        self.rate_state_store.delete_hotkey(hk)
    if stale:
        self._last_known_rates = {k: v for k, v in self._last_known_rates.items() if k[0] not in stale}


def _poll_collaterals(self: Validator) -> None:
    """Query each tracked miner's collateral and persist diffs.

    Runs every ``COLLATERAL_POLL_INTERVAL_BLOCKS``. Only miners with a cached
    rate (i.e. in ``_last_known_rates``) are polled — those are the ones that
    can hold a crown. The contract stores collateral as a single per-miner
    balance, so each row has no direction.
    """
    if self.block - self._last_collateral_poll_block < COLLATERAL_POLL_INTERVAL_BLOCKS:
        return
    self._last_collateral_poll_block = self.block

    tracked_hotkeys = {key[0] for key in self._last_known_rates.keys()}
    current_hotkeys = set(self.metagraph.hotkeys)

    for hotkey in tracked_hotkeys:
        if hotkey not in current_hotkeys:
            continue
        try:
            collateral = self.contract_client.get_miner_collateral(hotkey)
        except Exception as e:
            bt.logging.debug(f'Collateral read failed for {hotkey[:8]}: {e}')
            continue
        if self._last_known_collaterals.get(hotkey) == collateral:
            continue
        inserted = self.rate_state_store.insert_collateral_event(
            hotkey=hotkey,
            collateral_rao=collateral,
            block=self.block,
        )
        if inserted:
            self._last_known_collaterals[hotkey] = collateral

    stale = set(self._last_known_collaterals.keys()) - current_hotkeys
    for hk in stale:
        self._last_known_collaterals.pop(hk, None)


def _refresh_min_collateral(self: Validator) -> None:
    """Refresh the cached ``min_collateral`` from the contract every ~4h.

    The value is read once at validator init and then refreshed on this
    cadence. The cached value feeds crown eligibility during scoring replay.
    """
    if self.block - self._last_min_collateral_refresh_block < MIN_COLLATERAL_REFRESH_INTERVAL_BLOCKS:
        return
    try:
        value = self.contract_client.get_min_collateral()
    except Exception as e:
        bt.logging.warning(f'min_collateral refresh failed: {e}')
        return
    if value is None:
        return
    if value != self._min_collateral_rao:
        bt.logging.info(f'min_collateral changed: {self._min_collateral_rao} -> {value}')
        self._min_collateral_rao = value
    self._last_min_collateral_refresh_block = self.block


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
                tracker.resolve(swap.id, SwapStatus.COMPLETED, current_block)
                bt.logging.success(f'Swap {swap.id}: verified complete, confirmed')
    return uncertain


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
            tracker.resolve(swap.id, SwapStatus.TIMED_OUT, self.block)
            bt.logging.warning(f'Swap {swap.id}: timed out')


def _score_miners(self: Validator, tracker: SwapTracker) -> None:
    """Run a V1 scoring pass and commit weights."""
    try:
        tracker.prune_window(self.block)
        rewards, miner_uids = calculate_miner_rewards(self, tracker)
        if len(miner_uids) > 0 and len(rewards) > 0:
            self.update_scores(rewards, miner_uids)
    except Exception as e:
        bt.logging.error(f'Scoring failed: {e}')


def calculate_miner_rewards(
    self: Validator,
    _tracker: SwapTracker,
) -> Tuple[np.ndarray, Set[int]]:
    """Crown-time based reward computation.

    For each direction in ``DIRECTION_POOLS``:
      1. Replay rate_events and collateral_events chronologically over the window
      2. At each block boundary, determine crown holders (tied best-rate miners
         with collateral >= the cached ``_min_collateral_rao`` and still in the
         metagraph)
      3. Accumulate crown_blocks per hotkey, splitting evenly on ties
      4. ``rewards[uid] += pool * (crown_blocks[hk] / total) * success_rate ** SUCCESS_EXPONENT``

    Anything not distributed to miners recycles to ``RECYCLE_UID``.
    """
    n_uids = self.metagraph.n.item()
    if n_uids == 0:
        return np.array([], dtype=np.float32), set()

    window_end = self.block
    window_start = max(0, window_end - SCORING_WINDOW_BLOCKS)

    active_hotkeys: Set[str] = set(self.metagraph.hotkeys)
    hotkey_to_uid: Dict[str, int] = {self.metagraph.hotkeys[uid]: uid for uid in range(n_uids)}

    rewards = np.zeros(n_uids, dtype=np.float32)
    success_stats = self.rate_state_store.get_all_time_success_rates()
    min_collateral = int(getattr(self, '_min_collateral_rao', 0) or 0)

    for (from_chain, to_chain), pool in DIRECTION_POOLS.items():
        crown_blocks = _replay_crown_time(
            store=self.rate_state_store,
            from_chain=from_chain,
            to_chain=to_chain,
            window_start=window_start,
            window_end=window_end,
            active_hotkeys=active_hotkeys,
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
            sr = _success_rate(success_stats.get(hotkey))
            rewards[uid] += pool * share * (sr**SUCCESS_EXPONENT)

    recycle_uid = RECYCLE_UID if RECYCLE_UID < n_uids else 0
    distributed = float(rewards.sum())
    rewards[recycle_uid] += max(0.0, 1.0 - distributed)

    bt.logging.info(
        f'V1 scoring: window=[{window_start}, {window_end}], '
        f'distributed={distributed:.6f}, recycled={max(0.0, 1.0 - distributed):.6f}'
    )

    return rewards, set(range(n_uids))


def _success_rate(stats: Optional[Tuple[int, int]]) -> float:
    """All-time success rate. Zero-outcome miners default to 1.0 (optimistic)."""
    if stats is None:
        return 1.0
    completed, timed_out = stats
    total = completed + timed_out
    if total == 0:
        return 1.0
    return completed / total


def _replay_crown_time(
    store: RateStateStore,
    from_chain: str,
    to_chain: str,
    window_start: int,
    window_end: int,
    active_hotkeys: Set[str],
    min_collateral: int,
) -> Dict[str, float]:
    """Walk the merged rate + collateral event stream, accumulate crown blocks.

    Returns ``{hotkey: crown_blocks_float}``. Ties split credit evenly across
    the tied interval.
    """
    # 1. Reconstruct state at window_start for every currently-active hotkey.
    current_rates: Dict[str, float] = {}
    current_collateral: Dict[str, int] = {}

    for hotkey in active_hotkeys:
        latest_rate = store.get_latest_rate_before(hotkey, from_chain, to_chain, window_start)
        if latest_rate is not None:
            current_rates[hotkey] = latest_rate[0]
        latest_col = store.get_latest_collateral_before(hotkey, window_start)
        if latest_col is not None:
            current_collateral[hotkey] = latest_col[0]

    # 2. Merge rate and collateral events within the window, oldest first.
    #    Collateral events sort BEFORE rate events at the same block so a
    #    simultaneous "collateral drops + best rate" transition resolves to
    #    the post-drop state before rate attribution.
    rate_events = store.get_rate_events_in_range(from_chain, to_chain, window_start, window_end)
    col_events = store.get_collateral_events_in_range(window_start, window_end)

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
        holders = _crown_holders(current_rates, current_collateral, min_collateral, active_hotkeys)
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


def _crown_holders(
    rates: Dict[str, float],
    collaterals: Dict[str, int],
    min_collateral: int,
    active: Set[str],
) -> List[str]:
    """Hotkeys tied for best rate, with collateral >= min and in the metagraph."""
    eligible = {hk: r for hk, r in rates.items() if hk in active and collaterals.get(hk, 0) >= min_collateral and r > 0}
    if not eligible:
        return []
    best = max(eligible.values())
    return [hk for hk, r in eligible.items() if r == best]
