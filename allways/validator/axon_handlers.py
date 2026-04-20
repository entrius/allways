"""Axon handlers for multi-validator consensus synapses.

Each synapse type has three handler functions:
- blacklist_fn: fast auth check (reject unauthorized callers)
- priority_fn: ordering for concurrent requests
- forward_fn: validate request + vote on contract

These are attached to the validator's axon via functools.partial
to inject the validator context.
"""

from typing import TYPE_CHECKING, Optional, Tuple

import bittensor as bt
from Crypto.Hash import keccak
from substrateinterface import Keypair

from allways.classes import MinerPair
from allways.commitments import read_miner_commitment
from allways.constants import RESERVATION_COOLDOWN_BLOCKS
from allways.contract_client import AllwaysContractClient, ContractError, is_contract_rejection
from allways.synapses import MinerActivateSynapse, SwapConfirmSynapse, SwapReserveSynapse
from allways.utils.misc import is_reserved
from allways.utils.proofs import reserve_proof_message, swap_proof_message
from allways.utils.scale import encode_bytes, encode_str, encode_u128
from allways.validator.state_store import PendingConfirm

if TYPE_CHECKING:
    from neurons.validator import Validator


def keccak256(data: bytes) -> bytes:
    """Compute Keccak-256 hash (matches ink::env::hash::Keccak256)."""
    return keccak.new(data=data, digest_bits=256).digest()


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


def scale_encode_extend_hash_input(miner_bytes: bytes, from_tx_hash: str) -> bytes:
    """SCALE-encode the extend hash input tuple: (AccountId, &str).

    Matches ink::env::hash_encoded::<Keccak256, _>(&(miner, from_tx_hash)).
    """
    return miner_bytes + encode_str(from_tx_hash)


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


def load_swap_commitment(validator: 'Validator', miner_hotkey: str) -> Optional[MinerPair]:
    """Read miner commitment and validate chains differ. Returns commitment or None."""
    commitment = read_miner_commitment(
        subtensor=validator.axon_subtensor,
        netuid=validator.config.netuid,
        hotkey=miner_hotkey,
    )
    if commitment is None or commitment.from_chain == commitment.to_chain:
        return None
    return commitment


def reject_synapse(synapse: bt.Synapse, reason: str, context: str = '') -> None:
    """Mark a synapse as rejected with a reason and debug log."""
    synapse.accepted = False
    synapse.rejection_reason = reason
    if context:
        bt.logging.debug(f'{context}: {reason}')


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
        bt.logging.debug(f'Blacklisted unregistered hotkey: {synapse.dendrite.hotkey}')
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
    """Process miner activation: verify commitment + vote on contract."""
    miner_hotkey = synapse.dendrite.hotkey
    contract: AllwaysContractClient = validator.axon_contract_client
    bt.logging.info(f'MinerActivate request from {miner_hotkey}')

    ctx = f'MinerActivate({miner_hotkey})'

    try:
        with validator.axon_lock:
            # Fresh registration check (cached metagraph can be stale)
            if not validator.axon_subtensor.is_hotkey_registered(
                netuid=validator.config.netuid,
                hotkey_ss58=miner_hotkey,
            ):
                reject_synapse(synapse, 'Hotkey not registered on subnet', ctx)
                return synapse

            commitment = read_miner_commitment(
                subtensor=validator.axon_subtensor,
                netuid=validator.config.netuid,
                hotkey=miner_hotkey,
            )
            if commitment is None:
                reject_synapse(synapse, 'No commitment found', ctx)
                return synapse

            if contract.get_miner_active_flag(miner_hotkey):
                reject_synapse(synapse, 'Miner is already active', ctx)
                return synapse

            collateral = contract.get_miner_collateral(miner_hotkey)
            min_collateral = contract.get_min_collateral()
            if collateral < min_collateral:
                reject_synapse(synapse, f'Insufficient collateral: {collateral} < {min_collateral}', ctx)
                return synapse

            contract.vote_activate(wallet=validator.wallet, miner_hotkey=miner_hotkey)
            synapse.accepted = True
            bt.logging.info(f'Voted to activate miner {miner_hotkey}')

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
    bt.logging.info(f'SwapReserve request: miner={synapse.miner_hotkey}, tao={synapse.tao_amount}')

    miner = synapse.miner_hotkey
    ctx = f'SwapReserve({miner})'

    try:
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
        proof_message = f'allways-reserve:{synapse.from_address}:{synapse.block_anchor}'
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

        # Everything below touches substrate (commitment read, contract reads, vote).
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
            reserve_rate, _ = commitment.get_rate_for_direction(synapse.from_chain)
            if reserve_rate <= 0:
                reject_synapse(synapse, 'Miner does not support this swap direction', ctx)
                return synapse

            balance = provider.get_balance(synapse.from_address)
            if balance < synapse.from_amount:
                reject_synapse(synapse, 'Insufficient source balance', ctx)
                return synapse

            if not contract.get_miner_active_flag(miner):
                reject_synapse(synapse, 'Miner not active', ctx)
                return synapse

            if contract.get_miner_has_active_swap(miner):
                reject_synapse(synapse, 'Miner has an active swap', ctx)
                return synapse

            reserved_until = contract.get_miner_reserved_until(miner)
            if is_reserved(reserved_until, validator.block):
                reject_synapse(synapse, 'Miner already reserved', ctx)
                return synapse

            collateral = contract.get_miner_collateral(miner)
            if synapse.tao_amount > collateral:
                reject_synapse(synapse, 'Insufficient miner collateral', ctx)
                return synapse

            min_collateral = contract.get_min_collateral()
            if min_collateral > 0 and collateral < min_collateral:
                reject_synapse(synapse, 'Miner collateral below minimum', ctx)
                return synapse

            min_swap = contract.get_min_swap_amount()
            max_swap = contract.get_max_swap_amount()
            if min_swap > 0 and synapse.tao_amount < min_swap:
                reject_synapse(synapse, f'Swap amount below minimum ({synapse.tao_amount} < {min_swap} rao)', ctx)
                return synapse
            if max_swap > 0 and synapse.tao_amount > max_swap:
                reject_synapse(synapse, f'Swap amount above maximum ({synapse.tao_amount} > {max_swap} rao)', ctx)
                return synapse

            strike_count, last_expired = contract.get_cooldown(synapse.from_address)
            if strike_count > 0 and last_expired > 0:
                cooldown = RESERVATION_COOLDOWN_BLOCKS * (2 ** (strike_count - 1))
                if validator.block < last_expired + cooldown:
                    reject_synapse(synapse, f'Address on cooldown ({cooldown} blocks remaining)', ctx)
                    return synapse

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
            bt.logging.info(f'Voted to reserve miner {miner}')

    except ContractError as e:
        bt.logging.error(f'{ctx} failed: {e}')
        reason = 'Contract rejected the reservation' if is_contract_rejection(e) else str(e)
        reject_synapse(synapse, reason)
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
    """Verify source transaction and vote to initiate swap."""
    contract: AllwaysContractClient = validator.axon_contract_client
    bt.logging.info(f'SwapConfirm request: miner={synapse.reservation_id}, tx={synapse.from_tx_hash}')

    miner = synapse.reservation_id  # reservation_id is miner_hotkey (reservation keyed by miner)
    ctx = f'SwapConfirm({miner})'

    try:
        if not synapse.from_address or not synapse.from_tx_proof:
            reject_synapse(synapse, 'Missing source address or proof', ctx)
            return synapse
        if not synapse.to_address:
            reject_synapse(synapse, 'Missing destination address', ctx)
            return synapse

        with validator.axon_lock:
            reserved_until = contract.get_miner_reserved_until(miner)
            if not is_reserved(reserved_until, validator.block):
                reject_synapse(synapse, 'No active reservation for this miner', ctx)
                return synapse

            res_data = contract.get_reservation_data(miner)
            if res_data is None:
                reject_synapse(synapse, 'Reservation data not found', ctx)
                return synapse

            res_tao_amount, res_source_amount, res_dest_amount = res_data

            commitment = load_swap_commitment(validator, miner)
            if commitment is None:
                reject_synapse(synapse, 'No valid commitment', ctx)
                return synapse

            direction = resolve_swap_direction(commitment, synapse.from_chain, synapse.to_chain)
            if direction is None:
                reject_synapse(synapse, 'Miner does not support this swap direction', ctx)
                return synapse
            (
                swap_from_chain,
                swap_to_chain,
                miner_from_address,
                miner_fulfillment_address,
                _,
                selected_rate_str,
            ) = direction

            provider = validator.axon_chain_providers.get(swap_from_chain)
            if provider is None:
                reject_synapse(synapse, f'Unsupported chain: {swap_from_chain}', ctx)
                return synapse

            # Prove the caller controls from_address by verifying a signature over
            # the tx hash. Without this, anyone observing a user's on-chain source
            # tx could submit a confirm with their own to_address and redirect the
            # miner's fulfillment — the on-chain sender check alone doesn't bind
            # the confirm caller to the source address.
            proof_message = f'allways-swap:{synapse.from_tx_hash}'
            if not provider.verify_from_proof(synapse.from_address, proof_message, synapse.from_tx_proof):
                reject_synapse(synapse, 'Invalid source tx proof', ctx)
                return synapse

            # Validate destination address format — prevents a user from locking a
            # miner's reservation with an unfulfillable to_address that only fails
            # once the miner attempts to send (or silently accepts garbage on-chain).
            to_provider = validator.axon_chain_providers.get(swap_to_chain)
            if to_provider is None:
                reject_synapse(synapse, f'Unsupported destination chain: {swap_to_chain}', ctx)
                return synapse
            if not to_provider.is_valid_address(synapse.to_address):
                reject_synapse(synapse, 'Invalid destination address format', ctx)
                return synapse

            # Defend against user-snipes-miner by passing expected_sender: a user
            # could otherwise reserve a miner and claim any third-party tx of the
            # right amount to the miner's address. The base provider wraps this
            # check; the specific rejection reason is logged there at warning level.
            tx_info = provider.verify_transaction(
                tx_hash=synapse.from_tx_hash,
                expected_recipient=miner_from_address,
                expected_amount=res_source_amount,
                expected_sender=synapse.from_address,
            )
            if tx_info is None:
                reject_synapse(synapse, 'Source transaction not found, amount or sender mismatch', ctx)
                return synapse

            if not tx_info.confirmed:
                chain_def = provider.get_chain()
                pending = PendingConfirm(
                    miner_hotkey=miner,
                    from_tx_hash=synapse.from_tx_hash,
                    from_chain=swap_from_chain,
                    to_chain=swap_to_chain,
                    from_address=synapse.from_address,
                    to_address=synapse.to_address,
                    tao_amount=res_tao_amount,
                    from_amount=res_source_amount,
                    to_amount=res_dest_amount,
                    miner_from_address=miner_from_address,
                    miner_to_address=miner_fulfillment_address,
                    rate_str=selected_rate_str,
                    reserved_until=reserved_until,
                )
                validator.state_store.enqueue(pending)
                synapse.accepted = True
                synapse.rejection_reason = (
                    f'Queued — {tx_info.confirmations}/{chain_def.min_confirmations} confirmations. '
                    f'Validator will auto-initiate when confirmed.'
                )
                bt.logging.info(f'{ctx} queued: {tx_info.confirmations}/{chain_def.min_confirmations} confirmations')
                return synapse

            miner_bytes = bytes.fromhex(Keypair(ss58_address=miner).public_key.hex())
            request_hash = keccak256(
                scale_encode_initiate_hash_input(
                    miner_bytes,
                    synapse.from_tx_hash,
                    swap_from_chain,
                    swap_to_chain,
                    miner_from_address,
                    miner_fulfillment_address,
                    selected_rate_str,
                    res_tao_amount,
                    res_source_amount,
                    res_dest_amount,
                )
            )

            # user_hotkey must be SS58 (TAO address): to_address for BTC→TAO, from_address for TAO→BTC
            user_tao_address = synapse.to_address if swap_to_chain == 'tao' else synapse.from_address
            contract.vote_initiate(
                wallet=validator.wallet,
                request_hash=request_hash,
                user_hotkey=user_tao_address,
                miner_hotkey=miner,
                from_chain=swap_from_chain,
                to_chain=swap_to_chain,
                from_amount=res_source_amount,
                tao_amount=res_tao_amount,
                user_from_address=synapse.from_address,
                user_to_address=synapse.to_address,
                from_tx_hash=synapse.from_tx_hash,
                from_tx_block=tx_info.block_number or 0,
                to_amount=res_dest_amount,
                miner_from_address=miner_from_address,
                miner_to_address=miner_fulfillment_address,
                rate=selected_rate_str,
            )
            synapse.accepted = True
            bt.logging.info(f'Voted to initiate swap for miner {miner}')

    except ContractError as e:
        bt.logging.error(f'{ctx} failed: {e}')
        reason = 'Contract rejected the swap initiation' if is_contract_rejection(e) else str(e)
        reject_synapse(synapse, reason)
    except Exception as e:
        bt.logging.error(f'{ctx} failed: {e}')
        reject_synapse(synapse, str(e))

    return synapse
