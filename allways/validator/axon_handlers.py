"""Axon handlers for multi-validator consensus synapses.

Each synapse type has three handler functions:
- blacklist_fn: fast auth check (reject unauthorized callers)
- priority_fn: ordering for concurrent requests
- forward_fn: validate request + vote on contract

These are attached to the validator's axon via functools.partial
to inject the validator context.
"""

from typing import Tuple

import bittensor as bt
from Crypto.Hash import keccak
from substrateinterface import Keypair

from allways.commitments import read_miner_commitment
from allways.constants import RESERVATION_COOLDOWN_BLOCKS
from allways.contract_client import AllwaysContractClient, ContractError, compact_encode_len
from allways.synapses import MinerActivateSynapse, SwapConfirmSynapse, SwapReserveSynapse
from allways.validator.pending_confirms import PendingConfirm


def _keccak256(data: bytes) -> bytes:
    """Compute Keccak-256 hash (matches ink::env::hash::Keccak256)."""
    return keccak.new(data=data, digest_bits=256).digest()


def _scale_encode_reserve_hash_input(
    miner_bytes: bytes,
    source_addr_bytes: bytes,
    tao_amount: int,
    source_amount: int,
    dest_amount: int,
) -> bytes:
    """SCALE-encode the reserve hash input tuple: (AccountId, Vec<u8>, u128, u128, u128).

    Matches ink::env::hash_encoded::<Keccak256, _>(
        &(miner, user_source_address, tao_amount, source_amount, dest_amount)
    ).
    """
    return (
        miner_bytes  # AccountId: 32 bytes raw
        + compact_encode_len(len(source_addr_bytes))
        + source_addr_bytes  # Vec<u8>: compact length + data
        + tao_amount.to_bytes(16, 'little')  # u128: 16 bytes LE
        + source_amount.to_bytes(16, 'little')  # u128: 16 bytes LE
        + dest_amount.to_bytes(16, 'little')  # u128: 16 bytes LE
    )


def _scale_encode_extend_hash_input(
    miner_bytes: bytes,
    source_tx_hash: str,
) -> bytes:
    """SCALE-encode the extend hash input tuple: (AccountId, &str).

    Matches ink::env::hash_encoded::<Keccak256, _>(&(miner, source_tx_hash)).
    """
    tx_bytes = source_tx_hash.encode('utf-8')
    return (
        miner_bytes  # AccountId: 32 bytes raw
        + compact_encode_len(len(tx_bytes))
        + tx_bytes  # &str (SCALE: compact length + bytes)
    )


def _scale_encode_initiate_hash_input(
    miner_bytes: bytes,
    source_tx_hash: str,
    tao_amount: int,
    source_amount: int,
    dest_amount: int,
) -> bytes:
    """SCALE-encode the initiate hash input tuple: (AccountId, &str, u128, u128, u128).

    Matches ink::env::hash_encoded::<Keccak256, _>(&(miner, source_tx_hash, tao_amount, source_amount, dest_amount)).
    """
    tx_bytes = source_tx_hash.encode('utf-8')
    return (
        miner_bytes  # AccountId: 32 bytes raw
        + compact_encode_len(len(tx_bytes))
        + tx_bytes  # &str (SCALE: compact length + bytes)
        + tao_amount.to_bytes(16, 'little')  # u128: 16 bytes LE
        + source_amount.to_bytes(16, 'little')  # u128: 16 bytes LE
        + dest_amount.to_bytes(16, 'little')  # u128: 16 bytes LE
    )


def _reject(synapse, reason: str, context: str = '') -> None:
    """Mark a synapse as rejected with a reason and debug log."""
    synapse.accepted = False
    synapse.rejection_reason = reason
    if context:
        bt.logging.debug(f'{context}: {reason}')


# =============================================================================
# MinerActivateSynapse handlers
# =============================================================================


async def blacklist_miner_activate(
    validator,
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
    validator,
    synapse: MinerActivateSynapse,
) -> float:
    """Priority by stake — higher stake processed first."""
    try:
        uid = validator.metagraph.hotkeys.index(synapse.dendrite.hotkey)
        return float(validator.metagraph.S[uid])
    except (ValueError, IndexError):
        return 0.0


async def handle_miner_activate(
    validator,
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
                _reject(synapse, 'Hotkey not registered on subnet', ctx)
                return synapse

            commitment = read_miner_commitment(
                subtensor=validator.axon_subtensor,
                netuid=validator.config.netuid,
                hotkey=miner_hotkey,
            )
            if commitment is None:
                _reject(synapse, 'No commitment found', ctx)
                return synapse

            if contract.get_miner_active_flag(miner_hotkey):
                _reject(synapse, 'Miner is already active', ctx)
                return synapse

            collateral = contract.get_miner_collateral(miner_hotkey)
            min_collateral = contract.get_min_collateral()
            if collateral < min_collateral:
                _reject(synapse, f'Insufficient collateral: {collateral} < {min_collateral}', ctx)
                return synapse

            contract.vote_activate(wallet=validator.wallet, miner_hotkey=miner_hotkey)
            synapse.accepted = True
            bt.logging.info(f'Voted to activate miner {miner_hotkey}')

    except Exception as e:
        bt.logging.error(f'{ctx} failed: {e}')
        _reject(synapse, str(e))

    return synapse


# =============================================================================
# SwapReserveSynapse handlers
# =============================================================================


async def blacklist_swap_reserve(
    validator,
    synapse: SwapReserveSynapse,
) -> Tuple[bool, str]:
    """Pass-through — custom field checks happen in forward handler.

    Bittensor's axon middleware constructs the synapse from HTTP headers (default values)
    before calling blacklist. Custom fields (source_address, proof, etc.) are only available
    in the JSON body, which is parsed later for the forward handler.
    """
    return False, 'Passed'


async def priority_swap_reserve(
    validator,
    synapse: SwapReserveSynapse,
) -> float:
    """Flat priority for user requests."""
    return 1.0


async def handle_swap_reserve(
    validator,
    synapse: SwapReserveSynapse,
) -> SwapReserveSynapse:
    """Validate swap reservation request and vote on contract."""
    contract: AllwaysContractClient = validator.axon_contract_client
    bt.logging.info(f'SwapReserve request: miner={synapse.miner_hotkey}, tao={synapse.tao_amount}')

    miner = synapse.miner_hotkey
    ctx = f'SwapReserve({miner})'

    try:
        if not synapse.source_address or not synapse.source_address_proof:
            _reject(synapse, 'Missing source address or proof', ctx)
            return synapse

        with validator.axon_lock:
            commitment = read_miner_commitment(
                subtensor=validator.axon_subtensor,
                netuid=validator.config.netuid,
                hotkey=miner,
            )
            if commitment is None:
                _reject(synapse, 'Miner has no commitment', ctx)
                return synapse

            if commitment.source_chain == commitment.dest_chain:
                _reject(synapse, 'Source and destination chains must be different', ctx)
                return synapse

            swap_source_chain = synapse.source_chain or commitment.source_chain

            provider = validator.axon_chain_providers.get(swap_source_chain)
            if provider is None:
                _reject(synapse, f'Unsupported chain: {swap_source_chain}', ctx)
                return synapse

            proof_message = f'allways-reserve:{synapse.source_address}:{synapse.block_anchor}'
            if not provider.verify_source_proof(synapse.source_address, proof_message, synapse.source_address_proof):
                _reject(synapse, 'Invalid source address proof', ctx)
                return synapse

            balance = provider.get_balance(synapse.source_address)
            if balance < synapse.source_amount:
                _reject(synapse, 'Insufficient source balance', ctx)
                return synapse

            if not contract.get_miner_active_flag(miner):
                _reject(synapse, 'Miner not active', ctx)
                return synapse

            if contract.get_miner_has_active_swap(miner):
                _reject(synapse, 'Miner has an active swap', ctx)
                return synapse

            reserved_until = contract.get_miner_reserved_until(miner)
            if reserved_until >= validator.block:
                _reject(synapse, 'Miner already reserved', ctx)
                return synapse

            collateral = contract.get_miner_collateral(miner)
            if synapse.tao_amount > collateral:
                _reject(synapse, 'Insufficient miner collateral', ctx)
                return synapse

            min_collateral = contract.get_min_collateral()
            if min_collateral > 0 and collateral < min_collateral:
                _reject(synapse, 'Miner collateral below minimum', ctx)
                return synapse

            min_swap = contract.get_min_swap_amount()
            max_swap = contract.get_max_swap_amount()
            if min_swap > 0 and synapse.tao_amount < min_swap:
                _reject(synapse, f'Swap amount below minimum ({synapse.tao_amount} < {min_swap} rao)', ctx)
                return synapse
            if max_swap > 0 and synapse.tao_amount > max_swap:
                _reject(synapse, f'Swap amount above maximum ({synapse.tao_amount} > {max_swap} rao)', ctx)
                return synapse

            source_addr_bytes = synapse.source_address.encode('utf-8')
            strike_count, last_expired = contract.get_cooldown(source_addr_bytes)
            if strike_count > 0 and last_expired > 0:
                cooldown = RESERVATION_COOLDOWN_BLOCKS * (2 ** (strike_count - 1))
                if validator.block < last_expired + cooldown:
                    _reject(synapse, f'Address on cooldown ({cooldown} blocks remaining)', ctx)
                    return synapse

            miner_bytes = bytes.fromhex(Keypair(ss58_address=miner).public_key.hex())
            request_hash = _keccak256(
                _scale_encode_reserve_hash_input(
                    miner_bytes,
                    source_addr_bytes,
                    synapse.tao_amount,
                    synapse.source_amount,
                    synapse.dest_amount,
                )
            )

            contract.vote_reserve(
                wallet=validator.wallet,
                request_hash=request_hash,
                miner_hotkey=miner,
                user_source_address=source_addr_bytes,
                tao_amount=synapse.tao_amount,
                source_amount=synapse.source_amount,
                dest_amount=synapse.dest_amount,
            )
            synapse.accepted = True
            bt.logging.info(f'Voted to reserve miner {miner}')

    except ContractError as e:
        bt.logging.error(f'{ctx} failed: {e}')
        reason = 'Contract rejected the reservation' if 'ContractReverted' in str(e) else str(e)
        _reject(synapse, reason)
    except Exception as e:
        bt.logging.error(f'{ctx} failed: {e}')
        _reject(synapse, str(e))

    return synapse


# =============================================================================
# SwapConfirmSynapse handlers
# =============================================================================


async def blacklist_swap_confirm(
    validator,
    synapse: SwapConfirmSynapse,
) -> Tuple[bool, str]:
    """Pass-through — custom field checks happen in forward handler.

    See blacklist_swap_reserve docstring for rationale.
    """
    return False, 'Passed'


async def priority_swap_confirm(
    validator,
    synapse: SwapConfirmSynapse,
) -> float:
    """Flat priority for user requests."""
    return 1.0


async def handle_swap_confirm(
    validator,
    synapse: SwapConfirmSynapse,
) -> SwapConfirmSynapse:
    """Verify source transaction and vote to initiate swap."""
    contract: AllwaysContractClient = validator.axon_contract_client
    bt.logging.info(f'SwapConfirm request: miner={synapse.reservation_id}, tx={synapse.source_tx_hash}')

    miner = synapse.reservation_id  # reservation_id is miner_hotkey (reservation keyed by miner)
    ctx = f'SwapConfirm({miner})'

    try:
        if not synapse.source_address or not synapse.source_tx_proof:
            _reject(synapse, 'Missing source address or proof', ctx)
            return synapse

        with validator.axon_lock:
            reserved_until = contract.get_miner_reserved_until(miner)
            if reserved_until < validator.block:
                _reject(synapse, 'No active reservation for this miner', ctx)
                return synapse

            res_data = contract.get_reservation_data(miner)
            if res_data is None:
                _reject(synapse, 'Reservation data not found', ctx)
                return synapse

            res_tao_amount, res_source_amount, res_dest_amount = res_data[1], res_data[2], res_data[3]

            commitment = read_miner_commitment(
                subtensor=validator.axon_subtensor,
                netuid=validator.config.netuid,
                hotkey=miner,
            )
            if commitment is None:
                _reject(synapse, 'Miner commitment not found', ctx)
                return synapse

            if commitment.source_chain == commitment.dest_chain:
                _reject(synapse, 'Source and destination chains must be different', ctx)
                return synapse

            swap_source_chain = synapse.source_chain or commitment.source_chain
            swap_dest_chain = synapse.dest_chain or commitment.dest_chain

            miner_deposit_address = (
                commitment.source_address if swap_source_chain == commitment.source_chain else commitment.dest_address
            )

            provider = validator.axon_chain_providers.get(swap_source_chain)
            if provider is None:
                _reject(synapse, f'Unsupported chain: {swap_source_chain}', ctx)
                return synapse

            tx_info = provider.verify_transaction(
                tx_hash=synapse.source_tx_hash,
                expected_recipient=miner_deposit_address,
                expected_amount=res_source_amount,
            )
            if tx_info is None:
                _reject(synapse, 'Source transaction not found or amount mismatch', ctx)
                return synapse

            if not tx_info.confirmed:
                chain_def = provider.get_chain()
                pending = PendingConfirm(
                    miner_hotkey=miner,
                    source_tx_hash=synapse.source_tx_hash,
                    source_chain=swap_source_chain,
                    dest_chain=swap_dest_chain,
                    source_address=synapse.source_address,
                    dest_address=synapse.dest_address,
                    tao_amount=res_tao_amount,
                    source_amount=res_source_amount,
                    dest_amount=res_dest_amount,
                    miner_deposit_address=miner_deposit_address,
                    rate_str=commitment.rate_str,
                    reserved_until=reserved_until,
                )
                if validator.pending_confirms.enqueue(pending):
                    synapse.accepted = True
                    synapse.rejection_reason = (
                        f'Queued — {tx_info.confirmations}/{chain_def.min_confirmations} confirmations. '
                        f'Validator will auto-initiate when confirmed.'
                    )
                    bt.logging.info(
                        f'{ctx} queued: {tx_info.confirmations}/{chain_def.min_confirmations} confirmations'
                    )
                else:
                    _reject(synapse, 'Validator confirmation queue is full — retry later', ctx)
                return synapse

            miner_bytes = bytes.fromhex(Keypair(ss58_address=miner).public_key.hex())
            request_hash = _keccak256(
                _scale_encode_initiate_hash_input(
                    miner_bytes,
                    synapse.source_tx_hash,
                    res_tao_amount,
                    res_source_amount,
                    res_dest_amount,
                )
            )

            # user_hotkey must be SS58 (TAO address): dest_address for BTC→TAO, source_address for TAO→BTC
            user_tao_address = synapse.dest_address if swap_dest_chain == 'tao' else synapse.source_address
            contract.vote_initiate(
                wallet=validator.wallet,
                request_hash=request_hash,
                user_hotkey=user_tao_address,
                miner_hotkey=miner,
                source_chain=swap_source_chain,
                dest_chain=swap_dest_chain,
                source_amount=res_source_amount,
                tao_amount=res_tao_amount,
                user_source_address=synapse.source_address,
                user_dest_address=synapse.dest_address,
                source_tx_hash=synapse.source_tx_hash,
                source_tx_block=tx_info.block_number or 0,
                dest_amount=res_dest_amount,
                miner_source_address=miner_deposit_address,
                rate=commitment.rate_str,
            )
            synapse.accepted = True
            bt.logging.info(f'Voted to initiate swap for miner {miner}')

    except ContractError as e:
        bt.logging.error(f'{ctx} failed: {e}')
        reason = 'Contract rejected the swap initiation' if 'ContractReverted' in str(e) else str(e)
        _reject(synapse, reason)
    except Exception as e:
        bt.logging.error(f'{ctx} failed: {e}')
        _reject(synapse, str(e))

    return synapse
