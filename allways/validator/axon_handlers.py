"""Axon handlers for multi-validator consensus synapses.

Each synapse type has three handler functions:
- blacklist_fn: fast auth check (reject unauthorized callers)
- priority_fn: ordering for concurrent requests
- forward_fn: validate request + vote on contract

These are attached to the validator's axon via functools.partial
to inject the validator context.
"""

import time
from typing import TYPE_CHECKING, Tuple

import bittensor as bt
from bittensor import Keypair

from allways.chain_providers.base import ProviderUnreachableError
from allways.solana.client import swap_key_from_tx_hash
from allways.synapses import MinerActivateSynapse, SwapConfirmSynapse, SwapReserveSynapse
from allways.utils.logging import miner_label as _miner_label
from allways.validator.binding import verify_binding
from allways.validator.solana_swap_loop import is_tx_fresh

if TYPE_CHECKING:
    from neurons.validator import Validator

EMPTY_SWAP_KEY = b'\x00' * 32


def resolve_miner_pubkey(validator: 'Validator', miner_hotkey: str):
    """Map a Bittensor hotkey (ss58) → the miner's bound Solana pubkey via the HotkeyBinding PDA (A5).

    Returns None if unbound or if the sr25519 sig fails to verify. The contract stores the sig unverified
    (too costly on-chain), so we verify here — same as scoring's `build_attribution` — else an attacker could
    squat a victim's set-once marker with a garbage sig."""
    hotkey_bytes = bytes.fromhex(Keypair(ss58_address=miner_hotkey).public_key.hex())
    hk_binding = validator.solana_client.get_hotkey_binding(hotkey_bytes)
    if hk_binding is None:
        return None
    binding = validator.solana_client.get_binding(hk_binding.miner)
    if binding is None or bytes(binding.hotkey) != hotkey_bytes:
        return None
    if not verify_binding(hk_binding.miner, binding.hotkey, binding.hotkey_sig):
        bt.logging.warning(f'binding for {miner_hotkey}: invalid sr25519 sig, refusing to resolve')
        return None
    return hk_binding.miner


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
    """Reservation intake moved on-chain to Solana — STUB until Phase 9.

    The substrate ``vote_reserve`` path is decommissioned. On Solana a user reserves a
    miner through the contract's reservation pool (``open_or_request`` → permissionless
    stake-weighted ``resolve_pool`` draw), not a validator axon vote, so this handler
    rejects until that taker intake is wired.
    """
    # TODO(phase9): wire the Solana reservation-pool intake here, or retire this synapse.
    miner = synapse.miner_hotkey
    label = miner_label(validator, miner)
    direction = f'{(synapse.from_chain or "?").upper()}->{(synapse.to_chain or "?").upper()}'
    ctx = f'[{label}] SwapReserve {direction}'
    reject_synapse(
        synapse,
        'Reservations have moved to the Solana reservation pool (lands in Phase 9); '
        'the substrate reserve path is retired.',
        ctx,
    )
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
    """Claim relay: the user flags down a validator with their source-tx hash; the validator verifies the
    deposit against the pinned on-chain Reservation and relays submit_swap_claim, creating the Swap in
    PendingAttestation. Validators then attest (vote_initiate) via the swap loop.

    All swap terms (amounts, addresses, payout) are pinned in the immutable Reservation — submit_swap_claim
    copies them on-chain, so the relay only needs the source-tx hash + block. We still verify the deposit
    here (recipient/amount/sender against the reservation, plus replay-freshness) to avoid relaying a junk
    claim that would only need close_stale_claim later. If the tx isn't visible/confirmed yet, reject so
    the user resends once it confirms (no pending-confirm queue)."""
    miner_hotkey = synapse.reservation_id  # reservation_id is the miner hotkey (reservation keyed by miner)
    client = validator.solana_client
    label = miner_label(validator, miner_hotkey)
    ctx = f'[{label}] SwapConfirm'
    bt.logging.info(
        f'{ctx}: REQUEST received (user claims source tx sent) — '
        f'from_tx={synapse.from_tx_hash} from_tx_block_hint={synapse.from_tx_block}'
    )

    try:
        if not synapse.from_tx_hash:
            reject_synapse(synapse, 'Missing source tx hash', ctx)
            return synapse

        miner_pk = resolve_miner_pubkey(validator, miner_hotkey)
        if miner_pk is None:
            reject_synapse(synapse, 'Hotkey not bound to a Solana miner', ctx)
            return synapse

        reservation = client.get_reservation(miner_pk)
        if reservation is None:
            reject_synapse(synapse, 'No reservation for this miner', ctx)
            return synapse
        now = int(time.time())
        if reservation.reserved_until == 0 or reservation.reserved_until < now:
            reject_synapse(synapse, 'Reservation is not active', ctx)
            return synapse
        if bytes(reservation.claimed_swap_key) != EMPTY_SWAP_KEY:
            reject_synapse(synapse, 'Reservation already has a claimed swap', ctx)
            return synapse

        provider = validator.axon_chain_providers.get(reservation.from_chain)
        if provider is None:
            reject_synapse(synapse, f'Unsupported source chain: {reservation.from_chain}', ctx)
            return synapse

        # Verify the user's deposit against the pinned reservation terms. expected_sender =
        # reservation.from_addr defends user-snipes-miner (claiming a third-party tx of the right amount).
        try:
            tx_info = provider.verify_transaction(
                tx_hash=synapse.from_tx_hash,
                expected_recipient=reservation.miner_from_addr,
                expected_amount=int(reservation.from_amount),
                block_hint=synapse.from_tx_block,
                expected_sender=reservation.from_addr,
            )
        except ProviderUnreachableError:
            reject_synapse(synapse, 'Source-chain provider unreachable; resend shortly', ctx)
            return synapse

        if tx_info is None or not tx_info.confirmed:
            reject_synapse(
                synapse,
                'Source tx not yet visible/confirmed — resend once it has enough confirmations',
                ctx,
            )
            return synapse

        # Source freshness: the deposit must be mined after the reservation was created (replay defense).
        grace = getattr(provider.get_chain(), 'replay_grace_secs', 0)
        if not is_tx_fresh(tx_info, int(reservation.created_at), grace):
            reject_synapse(synapse, 'Source tx fails freshness — stale/replayed deposit', ctx)
            return synapse

        swap_key = swap_key_from_tx_hash(synapse.from_tx_hash)
        client.submit_swap_claim(miner_pk, swap_key, synapse.from_tx_hash, tx_info.block_number or 0)
        synapse.accepted = True
        bt.logging.info(
            f'{ctx}: CLAIM relayed (swap_key={swap_key.hex()[:16]}..., tx={synapse.from_tx_hash[:16]}...); '
            f'validators will attest'
        )

    except Exception as e:
        bt.logging.error(f'{ctx} failed: {e}')
        reject_synapse(synapse, str(e))

    return synapse
