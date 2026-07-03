"""Axon handlers for multi-validator consensus synapses.

Each synapse type has three handler functions:
- blacklist_fn: fast auth check (reject unauthorized callers)
- priority_fn: ordering for concurrent requests
- forward_fn: validate request + vote on contract

These are attached to the validator's axon via functools.partial
to inject the validator context.
"""

from typing import TYPE_CHECKING, Tuple

import bittensor as bt

from allways.synapses import MinerActivateSynapse, SwapConfirmSynapse, SwapReserveSynapse
from allways.utils.logging import miner_label as _miner_label
from allways.validator.reserve_engine import (
    confirm_deposit,
    reserve_on_behalf,
    resolve_miner_pubkey,
)

if TYPE_CHECKING:
    from neurons.validator import Validator


def reject_synapse(synapse: bt.Synapse, reason: str, context: str = '') -> None:
    """Mark a synapse as rejected with a reason and log it."""
    synapse.accepted = False
    synapse.rejection_reason = reason
    if context:
        bt.logging.info(f'{context}: {reason}')


def miner_label(validator: 'Validator', miner_hotkey: str) -> str:
    """Return ``UID N / hotkey[:8]`` — leads every miner log line with the UID."""
    return _miner_label(getattr(validator, 'metagraph', None), miner_hotkey)


# =============================================================================
# MinerActivateSynapse handlers
# =============================================================================


async def blacklist_miner_activate(
    validator: 'Validator',
    synapse: MinerActivateSynapse,
) -> Tuple[bool, str]:
    """Reject synapses from unregistered hotkeys."""
    if synapse.dendrite is None or synapse.dendrite.hotkey is None:
        return True, 'Missing dendrite or hotkey'
    if synapse.dendrite.hotkey not in validator.metagraph.hotkeys:
        bt.logging.info(f'Blacklisted unregistered hotkey: {synapse.dendrite.hotkey}')
        return True, 'Unregistered hotkey'
    return False, 'Hotkey recognized'


async def priority_miner_activate(
    validator: 'Validator',
    synapse: MinerActivateSynapse,
) -> float:
    """Priority by stake — higher stake processed first."""
    try:
        uid = validator.metagraph.hotkeys.index(synapse.dendrite.hotkey)
        return float(validator.metagraph.S[uid])
    except (ValueError, IndexError):
        return 0.0


async def handle_miner_activate(
    validator: 'Validator',
    synapse: MinerActivateSynapse,
) -> MinerActivateSynapse:
    """Process miner activation: confirm on-chain eligibility + vote_activate on the Solana contract.

    The miner flags down a validator; the validator resolves its bound Solana pubkey (A5 HotkeyBinding),
    checks MinerState (not already active, collateral >= Config.min_collateral — the contract re-checks
    both at quorum), and submits vote_activate. The old commitment/quote gate is a Phase-8 concern; the
    contract's vote_activate requires no quote, so it's dropped here."""
    miner_hotkey = synapse.dendrite.hotkey
    client = validator.solana_client
    label = miner_label(validator, miner_hotkey)
    ctx = f'[{label}] MinerActivate'
    bt.logging.info(f'{ctx}: REQUEST received (full hotkey={miner_hotkey})')

    try:
        # Fresh registration check (cached metagraph can be stale) — substrate read, hold axon_lock.
        with validator.axon_lock:
            registered = validator.axon_subtensor.is_hotkey_registered(
                netuid=validator.config.netuid,
                hotkey_ss58=miner_hotkey,
            )
        if not registered:
            reject_synapse(synapse, 'Hotkey not registered on subnet', ctx)
            return synapse

        miner_pk = resolve_miner_pubkey(validator, miner_hotkey)
        if miner_pk is None:
            reject_synapse(synapse, 'Hotkey not bound to a Solana miner (call bind_hotkey first)', ctx)
            return synapse

        miner_state = client.get_miner_state(miner_pk)
        if miner_state is None:
            reject_synapse(synapse, 'Miner has no on-chain state yet (post collateral first)', ctx)
            return synapse
        if miner_state.active:
            reject_synapse(synapse, 'Miner is already active', ctx)
            return synapse

        min_collateral = client.get_config().min_collateral
        if miner_state.collateral < min_collateral:
            reject_synapse(synapse, f'Insufficient collateral: {miner_state.collateral} < {min_collateral}', ctx)
            return synapse

        client.vote_activate(miner_pk)
        synapse.accepted = True
        bt.logging.info(f'{ctx}: ACTIVATED — vote_activate submitted')

    except Exception as e:
        bt.logging.error(f'{ctx} failed: {e}')
        reject_synapse(synapse, str(e))

    return synapse


# =============================================================================
# SwapReserveSynapse handlers
# =============================================================================


async def blacklist_swap_reserve(
    validator: 'Validator',
    synapse: SwapReserveSynapse,
) -> Tuple[bool, str]:
    """Pass-through — custom field checks happen in forward handler.

    Bittensor's axon middleware constructs the synapse from HTTP headers (default values)
    before calling blacklist. Custom fields (from_address, proof, etc.) are only available
    in the JSON body, which is parsed later for the forward handler.
    """
    return False, 'Passed'


async def priority_swap_reserve(
    validator: 'Validator',
    synapse: SwapReserveSynapse,
) -> float:
    """Flat priority for user requests."""
    return 1.0


async def handle_swap_reserve(
    validator: 'Validator',
    synapse: SwapReserveSynapse,
) -> SwapReserveSynapse:
    """Enter a miner's reservation pool on the caller's behalf (validator = router → high win odds).

    Thin transport wrapper over the shared `reserve_on_behalf` kernel op (same op the HTTP seam calls) —
    it validates eligibility + rate-consistency and submits open_or_request (open or free upsert)."""
    miner = synapse.miner_hotkey
    label = miner_label(validator, miner)
    direction = f'{(synapse.from_chain or "?").upper()}->{(synapse.to_chain or "?").upper()}'
    ctx = f'[{label}] SwapReserve {direction}'
    try:
        result = reserve_on_behalf(
            validator,
            miner,
            synapse.from_chain,
            synapse.to_chain,
            synapse.user_pubkey,
            synapse.user_from_addr,
            synapse.user_to_addr,
            synapse.from_amount,
        )
        if not result.ok:
            reject_synapse(synapse, result.reason, ctx)
            return synapse
        synapse.accepted = True
        synapse.pool_closes_at = result.pool_closes_at
        bt.logging.info(f'{ctx}: entered pool on-behalf (closes_at={result.pool_closes_at}, tx={result.sig[:16]}…)')
    except Exception as e:
        bt.logging.error(f'{ctx} failed: {e}')
        reject_synapse(synapse, str(e))
    return synapse


# =============================================================================
# SwapConfirmSynapse handlers
# =============================================================================


async def blacklist_swap_confirm(
    validator: 'Validator',
    synapse: SwapConfirmSynapse,
) -> Tuple[bool, str]:
    """Pass-through — custom field checks happen in forward handler.

    See blacklist_swap_reserve docstring for rationale.
    """
    return False, 'Passed'


async def priority_swap_confirm(
    validator: 'Validator',
    synapse: SwapConfirmSynapse,
) -> float:
    """Flat priority for user requests."""
    return 1.0


async def handle_swap_confirm(
    validator: 'Validator',
    synapse: SwapConfirmSynapse,
) -> SwapConfirmSynapse:
    """Claim relay: verify the user's source deposit against the pinned Reservation, relay submit_swap_claim
    (→ PendingAttestation). Thin wrapper over the shared `confirm_deposit` kernel op (same op the HTTP seam
    calls). reservation_id is the miner hotkey (reservations are keyed by miner)."""
    miner_hotkey = synapse.reservation_id
    label = miner_label(validator, miner_hotkey)
    ctx = f'[{label}] SwapConfirm'
    bt.logging.info(f'{ctx}: REQUEST (from_tx={synapse.from_tx_hash} block_hint={synapse.from_tx_block})')
    try:
        result = confirm_deposit(validator, miner_hotkey, synapse.from_tx_hash, synapse.from_tx_block)
        if not result.ok:
            reject_synapse(synapse, result.reason, ctx)
            return synapse
        synapse.accepted = True
        bt.logging.info(f'{ctx}: CLAIM relayed (swap_key={result.swap_key[:16]}…, tx={synapse.from_tx_hash[:16]}…)')
    except Exception as e:
        bt.logging.error(f'{ctx} failed: {e}')
        reject_synapse(synapse, str(e))
    return synapse
