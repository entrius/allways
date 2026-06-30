"""Validator forward pass — orchestrator called every step by the base neuron."""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING

import bittensor as bt

from allways.utils.logging import log_crown_winners
from allways.validator.binding import build_attribution
from allways.validator.scoring import (
    due_for_scoring,
    score_and_reward_miners,
    snapshot_current_crown_holders,
)

if TYPE_CHECKING:
    from neurons.validator import Validator


async def forward(self: Validator) -> None:
    """One validator forward step — Solana-sourced end to end.

    ``SolanaSwapLoop.run_once`` drives the swap lifecycle (discover live Solana
    swaps off the contract, verify both legs with replay-freshness gates, cast
    the on-chain consensus vote). Then the program's crown-relevant events are
    ingested into ``SolanaEventIndex`` and, on the block-gated cadence, replayed
    to score + reward miners and snapshot the live crown. Scoring CADENCE stays
    subtensor-block-gated (heartbeat + set_weights are TAO); only the crown
    replay WINDOW is unix-time.
    """
    self.check_block_progress(self.reconnect_and_propagate)

    clear_provider_caches(self)

    # Solana `timeout_at`/`created_at` are unix seconds, not substrate blocks.
    # run_once casts on-chain votes (network I/O), so run it off the event loop.
    now = int(time.time())
    # Permissionless crank: turn closed reservation pools into winner Reservations before processing swaps.
    resolved = await asyncio.to_thread(self.solana_swap_loop.resolve_pools_once, now)
    if resolved:
        bt.logging.info(f'forward step #{self.step}: resolved {len(resolved)} reservation pool(s)')
    decisions = await asyncio.to_thread(self.solana_swap_loop.run_once, now)
    bt.logging.info(
        f'forward step #{self.step} @ block {self.block}: solana swap loop processed {len(decisions)} live swap(s)'
    )

    # Fold new program events into the crown index before scoring reads it.
    ingest_solana_events(self)

    if due_for_scoring(self.block, self.last_scored_block, self.initial_scoring_done):
        score_and_reward_miners(self)
        self.initial_scoring_done = True
        bt.logging.info('forward: scoring done')

    # Live current-crown snapshot — computed every step (sub-ms in-memory) so the
    # per-step crown log line works for validators that haven't opted into DB
    # writes; the write is gated by STORE_DB_RESULTS and wrapped so a DB outage
    # never propagates into the forward loop. Reads "now" on the unix-time axis,
    # matching the scoring window. No halt check here (that RPC is the expensive
    # one); halt-aware clearing happens once per round in `_flush_halt_window`.
    crown_snapshot = snapshot_current_crown_holders(self)
    log_crown_winners(self.metagraph, self.block, crown_snapshot)
    if self.database_storage.is_enabled():
        try:
            self.database_storage.upsert_current_crown_snapshot(crown_snapshot)
        except Exception as e:
            bt.logging.warning(f'current_crown_holders snapshot failed: {e}')


def clear_provider_caches(self: Validator) -> None:
    for provider in self.chain_providers.values():
        if hasattr(provider, 'clear_cache'):
            provider.clear_cache()


def ingest_solana_events(self: Validator) -> None:
    """Poll program events newer than the stored cursor and fold them into the
    crown ``SolanaEventIndex`` (active/activity/collateral/rate tables), attributing
    each event's miner Solana pubkey → bound hotkey via the sr25519 binding. The
    cursor advances only after a successful poll, so a transient RPC failure
    re-reads the same window next step instead of skipping events."""
    cursor = self.state_store.get_solana_event_cursor()
    try:
        records, new_cursor = self.event_ingest.poll(cursor)
    except Exception as e:
        bt.logging.warning(f'forward: solana event poll failed: {e}')
        return
    if records:
        attribution = build_attribution(self.solana_client)
        written = self.event_index.ingest(records, attribution)
        bt.logging.info(f'forward: ingested {written}/{len(records)} solana event(s)')
    if new_cursor is not None and new_cursor != cursor:
        self.state_store.set_solana_event_cursor(new_cursor)
