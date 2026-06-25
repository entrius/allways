"""Axon handlers for multi-validator consensus synapses.

Each synapse type has three handler functions:
- blacklist_fn: fast auth check (reject unauthorized callers)
- priority_fn: ordering for concurrent requests
- forward_fn: validate request + vote on contract

These are attached to the validator's axon via functools.partial
to inject the validator context.
"""

import time
from typing import TYPE_CHECKING, Optional, Tuple

import bittensor as bt
from bittensor import Keypair
from Crypto.Hash import keccak

from allways.chain_providers.base import ProviderUnreachableError
from allways.chains import canonical_pair, get_chain
from allways.classes import MinerPair
from allways.commitments import read_miner_commitment
from allways.constants import RESERVATION_COOLDOWN_BLOCKS, RESERVE_SLIPPAGE_MAX_BPS
from allways.contract_client import AllwaysContractClient, ContractError
from allways.solana.client import swap_key_from_tx_hash
from allways.synapses import MinerActivateSynapse, SwapConfirmSynapse, SwapReserveSynapse
from allways.utils.logging import miner_label as _miner_label
from allways.utils.proofs import reserve_proof_message, swap_proof_message
from allways.utils.rate import (
    calculate_to_amount,
    derive_tao_leg,
    is_executable_rate,
    quote_within_slippage,
)
from allways.utils.scale import encode_bytes, encode_str, encode_u128
from allways.validator.solana_swap_loop import is_tx_fresh
from allways.validator.state_store import PendingConfirm, ReservationPin

if TYPE_CHECKING:
    from neurons.validator import Validator

EMPTY_SWAP_KEY = b'\x00' * 32


def keccak256(data: bytes) -> bytes:
    """Compute Keccak-256 hash (matches ink::env::hash::Keccak256)."""
    return keccak.new(data=data, digest_bits=256).digest()


def resolve_miner_pubkey(validator: 'Validator', miner_hotkey: str):
    """Map a Bittensor hotkey (ss58) → the miner's bound Solana pubkey via the HotkeyBinding PDA (A5).

    Returns None if the miner hasn't bound a hotkey yet. PDAs (reservation / miner_state) are keyed by the
    Solana pubkey, so every Solana-side action from an axon handler goes through this binding."""
    hotkey_bytes = bytes.fromhex(Keypair(ss58_address=miner_hotkey).public_key.hex())
    binding = validator.solana_client.get_hotkey_binding(hotkey_bytes)
    return binding.miner if binding is not None else None


def scale_encode_reserve_hash_input(
    miner_bytes: bytes,
    from_addr_bytes: bytes,
    from_chain: str,
    to_chain: str,
    tao_amount: int,
    from_amount: int,
    to_amount: int,
) -> bytes:
    """SCALE-encode the reserve hash input tuple: (AccountId, String, String, String, u128, u128, u128).

    Matches ink::env::hash_encoded::<Keccak256, _>(
        &(miner, user_from_address, from_chain, to_chain, tao_amount, from_amount, to_amount)
    ).
    """
    return (
        miner_bytes
        + encode_bytes(from_addr_bytes)
        + encode_str(from_chain)
        + encode_str(to_chain)
        + encode_u128(tao_amount)
        + encode_u128(from_amount)
        + encode_u128(to_amount)
    )


def scale_encode_initiate_hash_input(
    miner_bytes: bytes,
    from_tx_hash: str,
    from_chain: str,
    to_chain: str,
    miner_from_address: str,
    miner_to_address: str,
    rate: str,
    tao_amount: int,
    from_amount: int,
    to_amount: int,
) -> bytes:
    """SCALE-encode the initiate hash input tuple.

    Matches ink::env::hash_encoded::<Keccak256, _>(
        &(miner, from_tx_hash, from_chain, to_chain,
          miner_from_address, miner_to_address, rate,
          tao_amount, from_amount, to_amount)
    ).

    Including the chains, miner addresses, and rate in the hash forces validator
    consensus on the full swap shape — the quorum-reaching vote cannot substitute
    any of these fields without invalidating the hash.
    """
    return (
        miner_bytes
        + encode_str(from_tx_hash)
        + encode_str(from_chain)
        + encode_str(to_chain)
        + encode_str(miner_from_address)
        + encode_str(miner_to_address)
        + encode_str(rate)
        + encode_u128(tao_amount)
        + encode_u128(from_amount)
        + encode_u128(to_amount)
    )


def resolve_swap_direction(
    commitment: MinerPair,
    synapse_from_chain: str,
    synapse_to_chain: str,
) -> Optional[Tuple[str, str, str, str, float, str]]:
    """Resolve deposit/fulfillment addresses and rate from commitment and requested direction.

    Returns (from_chain, to_chain, deposit_addr, fulfillment_addr, rate, rate_str) or None.
    """
    from_chain = synapse_from_chain or commitment.from_chain
    to_chain = synapse_to_chain or commitment.to_chain
    is_canonical = from_chain == commitment.from_chain
    deposit_addr = commitment.from_address if is_canonical else commitment.to_address
    fulfillment_addr = commitment.to_address if is_canonical else commitment.from_address
    rate, rate_str = commitment.get_rate_for_direction(from_chain)
    if rate <= 0:
        return None
    return from_chain, to_chain, deposit_addr, fulfillment_addr, rate, rate_str


def recompute_reserve_amounts(
    commitment: MinerPair,
    from_chain: str,
    to_chain: str,
    from_amount: int,
) -> int:
    """Recompute to_amount from the miner's commitment rate, mirroring the CLI
    quote path so a correctly-quoted reservation matches the reserve-time rate."""
    canon_from, canon_to = canonical_pair(from_chain, to_chain)
    is_reverse = from_chain != canon_from
    _, rate_str = commitment.get_rate_for_direction(from_chain)
    return calculate_to_amount(
        from_amount,
        rate_str,
        is_reverse,
        get_chain(canon_to).decimals,
        get_chain(canon_from).decimals,
    )


def load_swap_commitment(validator: 'Validator', miner_hotkey: str) -> Optional[MinerPair]:
    """Read miner commitment and validate chains differ. Returns commitment or None.

    Passes the validator's cached metagraph so read_miner_commitment skips the
    full subnet metagraph download — that sync takes 30s+ on testnet finney and
    was the real source of axon handler timeouts.
    """
    commitment = read_miner_commitment(
        subtensor=validator.axon_subtensor,
        netuid=validator.config.netuid,
        hotkey=miner_hotkey,
        metagraph=validator.metagraph,
    )
    if commitment is None or commitment.from_chain == commitment.to_chain:
        return None
    return commitment


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
    """Validate swap reservation request and vote on contract."""
    contract: AllwaysContractClient = validator.axon_contract_client
    miner = synapse.miner_hotkey
    label = miner_label(validator, miner)
    direction = f'{(synapse.from_chain or "?").upper()}->{(synapse.to_chain or "?").upper()}'
    ctx = f'[{label}] SwapReserve {direction}'
    bt.logging.info(
        f'{ctx}: REQUEST received — tao={synapse.tao_amount} '
        f'from_amount={synapse.from_amount} to_amount={synapse.to_amount} '
        f'slippage_bps={synapse.slippage_bps} '
        f'user_from_address={synapse.from_address} block_anchor={synapse.block_anchor}'
    )

    try:
        # Halt blocks reservations contract-side; fast-reject here so a halt
        # can't flood doomed vote_reserve extrinsics that starve confirm/timeout
        # votes. halted() fails open, so an RPC blip falls through to the contract.
        if validator.bounds_cache.halted():
            reject_synapse(synapse, 'System is halted — reservations paused', ctx)
            return synapse

        # Cheap, local checks BEFORE axon_lock — invalid signatures, missing fields,
        # and bad direction are rejected without serializing on the substrate websocket.
        if not synapse.from_address or not synapse.from_address_proof:
            reject_synapse(synapse, 'Missing source address or proof', ctx)
            return synapse
        if not synapse.from_chain or not synapse.to_chain:
            reject_synapse(synapse, 'Missing from_chain or to_chain', ctx)
            return synapse
        if synapse.from_chain == synapse.to_chain:
            reject_synapse(synapse, 'Source and destination chains must be different', ctx)
            return synapse

        provider = validator.axon_chain_providers.get(synapse.from_chain)
        if provider is None:
            reject_synapse(synapse, f'Unsupported chain: {synapse.from_chain}', ctx)
            return synapse
        proof_message = reserve_proof_message(synapse.from_address, synapse.block_anchor)
        if not provider.verify_from_proof(synapse.from_address, proof_message, synapse.from_address_proof):
            reject_synapse(synapse, 'Invalid source address proof', ctx)
            return synapse

        # Pure-local crypto — compute the request hash outside the lock as a cheap pre-check.
        from_addr_bytes = synapse.from_address.encode('utf-8')
        miner_bytes = bytes.fromhex(Keypair(ss58_address=miner).public_key.hex())
        request_hash = keccak256(
            scale_encode_reserve_hash_input(
                miner_bytes,
                from_addr_bytes,
                synapse.from_chain,
                synapse.to_chain,
                synapse.tao_amount,
                synapse.from_amount,
                synapse.to_amount,
            )
        )

        # Substrate early-reject checks (commitment / slippage / already-reserved /
        # cooldown) run BEFORE the source-balance lookup. The balance call is the
        # only external dependency on this path — for a BTC source it is an uncached
        # Esplora HTTP request — so doing it last means spam destined for any of these
        # cheap rejections never reaches it, capping per-request amplification.
        with validator.axon_lock:
            commitment = load_swap_commitment(validator, miner)
            if commitment is None:
                reject_synapse(synapse, 'No valid commitment', ctx)
                return synapse

            # The requested direction must match one of the commitment's chains
            # and the miner must quote a non-zero rate for it. This blocks a DoS
            # where a user could lock a miner for the reservation TTL on a
            # direction that would only fail at confirm time.
            if synapse.from_chain not in (commitment.from_chain, commitment.to_chain):
                reject_synapse(synapse, 'Miner does not support this swap direction', ctx)
                return synapse
            reserve_rate, reserve_rate_str = commitment.get_rate_for_direction(synapse.from_chain)
            if reserve_rate <= 0:
                reject_synapse(synapse, 'Miner does not support this swap direction', ctx)
                return synapse
            min_swap = validator.bounds_cache.min_swap_amount()
            max_swap = validator.bounds_cache.max_swap_amount()
            if not is_executable_rate(reserve_rate, synapse.from_chain, synapse.to_chain, min_swap, max_swap):
                reject_synapse(synapse, 'Miner rate is not executable under current swap bounds', ctx)
                return synapse
            bt.logging.info(
                f'{ctx}: commitment ok — miner_rate={reserve_rate_str or reserve_rate} '
                f'miner_from={commitment.from_address} miner_to={commitment.to_address}'
            )

            # A user address equal to one of the miner's committed addresses makes
            # a swap leg a self-transfer (A->A) that delivers nothing. Reject early
            # so a self-flow operator can't even hold the reservation.
            if synapse.from_address in (commitment.from_address, commitment.to_address):
                reject_synapse(
                    synapse, 'Source address matches the miner commitment — self-transfers are not valid swaps', ctx
                )
                return synapse

            # Gate the user's quote against the rate read at reserve time, and
            # reject a tao_amount that doesn't match the submitted from/to legs.
            expected_to_amount = recompute_reserve_amounts(
                commitment,
                synapse.from_chain,
                synapse.to_chain,
                synapse.from_amount,
            )
            expected_tao_amount = derive_tao_leg(
                synapse.from_chain, synapse.from_amount, synapse.to_chain, synapse.to_amount
            )
            if synapse.tao_amount != expected_tao_amount:
                reject_synapse(
                    synapse,
                    'tao_amount is inconsistent with from_amount/to_amount — re-quote and retry',
                    ctx,
                )
                return synapse
            slippage_bps = max(0, min(synapse.slippage_bps, RESERVE_SLIPPAGE_MAX_BPS))
            if not quote_within_slippage(synapse.to_amount, expected_to_amount, slippage_bps):
                reject_synapse(
                    synapse,
                    'Quoted amount is below your slippage band — the miner rate moved, re-quote and retry',
                    ctx,
                )
                return synapse

            collateral, active, has_swap, reserved_until, _ = contract.get_miner_snapshot(miner)
            if not active:
                reject_synapse(synapse, 'Miner not active', ctx)
                return synapse
            if has_swap:
                reject_synapse(synapse, 'Miner has an active swap', ctx)
                return synapse
            # Read the current block via axon_subtensor — validator.block goes
            # through self.subtensor, which the forward loop already uses;
            # concurrent reads collide on the same websocket.
            cur_block = validator.axon_subtensor.get_current_block()
            if reserved_until >= cur_block:
                reject_synapse(synapse, 'Miner already reserved', ctx)
                return synapse
            if synapse.tao_amount > collateral:
                reject_synapse(synapse, 'Insufficient miner collateral', ctx)
                return synapse

            min_collateral = validator.bounds_cache.min_collateral()
            if min_collateral > 0 and collateral < min_collateral:
                reject_synapse(synapse, 'Miner collateral below minimum', ctx)
                return synapse

            if min_swap > 0 and synapse.tao_amount < min_swap:
                reject_synapse(synapse, f'Swap amount below minimum ({synapse.tao_amount} < {min_swap} rao)', ctx)
                return synapse
            if max_swap > 0 and synapse.tao_amount > max_swap:
                reject_synapse(synapse, f'Swap amount above maximum ({synapse.tao_amount} > {max_swap} rao)', ctx)
                return synapse

            strike_count, last_expired = contract.get_cooldown(synapse.from_address)
            if strike_count > 0 and last_expired > 0:
                cooldown = RESERVATION_COOLDOWN_BLOCKS * (2 ** (strike_count - 1))
                if cur_block < last_expired + cooldown:
                    remaining = (last_expired + cooldown) - cur_block
                    reject_synapse(
                        synapse,
                        f'Address on cooldown: ~{remaining} blocks remaining '
                        f'(strike {strike_count}, {cooldown}-block window)',
                        ctx,
                    )
                    return synapse

        # Source balance is the most expensive gate, so it runs last — only after a
        # request has cleared every cheap rejection. A TAO source reads balance over
        # the shared substrate websocket, so it must serialise under axon_lock; a BTC
        # source is HTTP and stays lock-free to avoid stalling the forward loop behind
        # a slow Esplora call.
        if provider.uses_substrate:
            with validator.axon_lock:
                balance = provider.get_balance(synapse.from_address)
        else:
            balance = provider.get_balance(synapse.from_address)
        if balance < synapse.from_amount:
            reject_synapse(synapse, 'Insufficient source balance', ctx)
            return synapse

        # Submit the reserve vote. The contract is the atomic gate; the handler
        # checks above are best-effort early-rejects. Moving the balance lookup
        # ahead of the vote opens a small window in which a concurrent request could
        # reserve this miner first — that race costs at most one doomed vote_reserve,
        # which the contract rejects, so the early-reject guarantee is unchanged.
        with validator.axon_lock:
            bt.logging.info(
                f'{ctx}: preflight ok — collateral={collateral} reserved_until={reserved_until} '
                f'cur_block={cur_block} → submitting vote_reserve'
            )
            contract.vote_reserve(
                wallet=validator.wallet,
                request_hash=request_hash,
                miner_hotkey=miner,
                user_from_address=synapse.from_address,
                from_chain=synapse.from_chain,
                to_chain=synapse.to_chain,
                tao_amount=synapse.tao_amount,
                from_amount=synapse.from_amount,
                to_amount=synapse.to_amount,
            )
            synapse.accepted = True
            bt.logging.info(
                f'{ctx}: RESERVED — vote_reserve submitted '
                f'(tao={synapse.tao_amount}, rate={reserve_rate_str or reserve_rate})'
            )

            # Pin now so a fast SwapConfirm finds it; on failure the watcher backfills, never rejects the reserve.
            try:
                validator.state_store.upsert_reservation_pin(
                    ReservationPin(
                        miner_hotkey=miner,
                        reserve_block=cur_block,
                        from_chain=commitment.from_chain,
                        to_chain=commitment.to_chain,
                        rate_str=commitment.rate_str,
                        counter_rate_str=commitment.counter_rate_str,
                        miner_from_address=commitment.from_address,
                        miner_to_address=commitment.to_address,
                        reserved_until=contract.get_miner_reserved_until(miner),
                    )
                )
            except Exception as e:
                bt.logging.warning(f'{ctx}: synchronous pin write failed: {e} — watcher will backfill')

    except ContractError as e:
        bt.logging.error(f'{ctx} failed: {e}')
        reject_synapse(synapse, str(e))
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
