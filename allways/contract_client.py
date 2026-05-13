"""Client for interacting with the Allways Swap Manager smart contract.

This module bypasses substrate-interface's ``ContractInstance`` layer and
talks to the contract via raw ``state_call`` / signed extrinsic RPCs. The
ContractInstance path hits SCALE-decode bugs against the subtensor runtime
we target; the raw path works and is proven in gittensor's production
clients.

Layout
------

1. **Selector registry** (``CONTRACT_SELECTORS``): maps each contract
   message name to its 4-byte ink! selector. Selectors are generated at
   contract build time and pinned here — keep in sync with
   ``allways/metadata/allways_swap_manager.json``. Adding a new contract
   message means:
     a. Adding the selector bytes to ``CONTRACT_SELECTORS``
     b. Adding the parameter signature to ``METHOD_SIGNATURES``
     c. Adding a wrapper method on ``AllwaysContractClient``

2. **Parameter encoder** (``encode_value``): minimal SCALE encoder for the
   primitive types we use (u32, u64, u128, AccountId, String, bytes,
   bool, vec_u64). Not a general SCALE implementation — only supports
   what the ink! methods here need.

3. **Reader helpers** (``read_u32``, ``read_u64``, ``read_u128``,
   ``read_bool``, ``read_account_id``, ``raw_contract_read``): call the
   contract via ``state_call`` then decode the ContractExecResult envelope
   and ink! Result discriminant. All raise ``ContractError`` on failure
   (including the explicit contract-reject path via ``decode_contract_error``).

4. **Writer** (``exec_contract_raw``): signs and submits an extrinsic,
   waits for inclusion, and raises ``ContractError`` on any failure. All
   message wrappers (e.g. ``vote_initiate``, ``mark_fulfilled``) route
   through here.

5. **Error flow**: every contract failure ends up as ``ContractError``.
   Callers that specifically need to distinguish "contract explicitly
   rejected this call" from "something else went wrong" should use
   ``is_contract_rejection(e)`` — that's the only discrimination we
   maintain a single source of truth for.
"""

import os
import struct
import threading
from typing import Any, Callable, List, Optional, Tuple, TypeVar

import bittensor as bt
from async_substrate_interface.errors import ExtrinsicNotFound
from bittensor import Keypair
from websockets.exceptions import ConnectionClosed

from allways.classes import PendingExtension, Reservation, Swap, SwapStatus
from allways.constants import CONTRACT_ADDRESS, MIN_BALANCE_FOR_TX_RAO
from allways.utils.scale import (
    ACCOUNT_ID_BYTES,
    U16_BYTES,
    U32_BYTES,
    U64_BYTES,
    U128_BYTES,
    compact_encode_len,
    decode_account_id,
    decode_string,
    decode_u16,
    decode_u32,
    decode_u64,
    decode_u128,
    encode_bytes,
    encode_str,
    encode_u128,
    strip_hex_prefix,
)

T = TypeVar('T')

# =========================================================================
# Contract selectors (from metadata — deterministic per contract build)
# =========================================================================

CONTRACT_SELECTORS = {
    'post_collateral': bytes.fromhex('31b3f423'),
    'withdraw_collateral': bytes.fromhex('e098e62d'),
    'vote_reserve': bytes.fromhex('ff3b86a0'),
    'cancel_reservation': bytes.fromhex('c4cb59cb'),
    'vote_initiate': bytes.fromhex('90c444d8'),
    'mark_fulfilled': bytes.fromhex('2dbeeb8d'),
    'confirm_swap': bytes.fromhex('d0065335'),
    'timeout_swap': bytes.fromhex('5325f39c'),
    'claim_slash': bytes.fromhex('cf3c3dd9'),
    'deactivate': bytes.fromhex('339db2a5'),
    'vote_activate': bytes.fromhex('00088a2d'),
    'vote_deactivate': bytes.fromhex('dac13f65'),
    'transfer_ownership': bytes.fromhex('107e33ea'),
    'add_validator': bytes.fromhex('82f48fa6'),
    'remove_validator': bytes.fromhex('62135acd'),
    'set_fulfillment_timeout': bytes.fromhex('e9cb777b'),
    'set_min_collateral': bytes.fromhex('b3f48b5e'),
    'set_max_collateral': bytes.fromhex('b7fae7fd'),
    'set_consensus_threshold': bytes.fromhex('c0d8ec47'),
    'set_min_swap_amount': bytes.fromhex('800e1573'),
    'set_max_swap_amount': bytes.fromhex('3e868f32'),
    'set_reservation_ttl': bytes.fromhex('3143d9e3'),
    'recycle_fees': bytes.fromhex('97756ea1'),
    'enable_chain_ext': bytes.fromhex('aef4a766'),
    'get_swap': bytes.fromhex('a35f1bbf'),
    'get_collateral': bytes.fromhex('f48343ad'),
    'get_miner_active': bytes.fromhex('25652be8'),
    'get_miner_has_active_swap': bytes.fromhex('1d07dec1'),
    'get_miner_snapshot': bytes.fromhex('ffd9e2e6'),
    'is_validator': bytes.fromhex('f844fc5f'),
    'get_next_swap_id': bytes.fromhex('d80244d2'),
    'get_fulfillment_timeout': bytes.fromhex('e820174a'),
    'get_min_collateral': bytes.fromhex('233a7832'),
    'get_max_collateral': bytes.fromhex('54945717'),
    'get_accumulated_fees': bytes.fromhex('bf3b5d4e'),
    'get_total_recycled_fees': bytes.fromhex('9910e939'),
    'get_owner': bytes.fromhex('07fcd0b1'),
    'get_recycle_address': bytes.fromhex('3847e06c'),
    'get_staking_hotkey': bytes.fromhex('47e11891'),
    'get_netuid': bytes.fromhex('75b98cec'),
    'get_chain_ext_enabled': bytes.fromhex('06d94687'),
    'get_pending_slash': bytes.fromhex('48c78c4a'),
    'get_min_swap_amount': bytes.fromhex('fca7daa4'),
    'get_max_swap_amount': bytes.fromhex('97826e04'),
    'get_miner_reserved_until': bytes.fromhex('d5ed7150'),
    'get_reservation_ttl': bytes.fromhex('f7e24a31'),
    'get_miner_deactivation_block': bytes.fromhex('361acc31'),
    'get_consensus_threshold': bytes.fromhex('2c283460'),
    'get_validator_count': bytes.fromhex('a30ab5c4'),
    'get_validators': bytes.fromhex('a28acf8e'),
    'get_reservation_data': bytes.fromhex('79fe2717'),
    'get_reservation': bytes.fromhex('3690f521'),
    'get_pending_reserve_vote_count': bytes.fromhex('3781315a'),
    'get_cooldown': bytes.fromhex('19a837c6'),
    'propose_extend_reservation': bytes.fromhex('9c9a8e8e'),
    'challenge_extend_reservation': bytes.fromhex('40b77e21'),
    'finalize_extend_reservation': bytes.fromhex('baf47953'),
    'get_pending_reservation_extension': bytes.fromhex('d79424b8'),
    'get_reservation_extension_count': bytes.fromhex('c5f9a918'),
    'propose_extend_timeout': bytes.fromhex('94c87a1d'),
    'challenge_extend_timeout': bytes.fromhex('682cf8eb'),
    'finalize_extend_timeout': bytes.fromhex('b23b4d80'),
    'get_pending_timeout_extension': bytes.fromhex('6bd06828'),
    'get_swap_extension_count': bytes.fromhex('c2a875b1'),
    'set_halted': bytes.fromhex('8fe1c210'),
    'get_halted': bytes.fromhex('ec540804'),
}

# Arg types: method -> [(arg_name, type_tag)]
CONTRACT_ARG_TYPES = {
    'post_collateral': [],
    'withdraw_collateral': [('amount', 'u128')],
    'vote_reserve': [
        ('request_hash', 'hash'),
        ('miner', 'AccountId'),
        ('user_from_address', 'str'),
        ('from_chain', 'str'),
        ('to_chain', 'str'),
        ('tao_amount', 'u128'),
        ('from_amount', 'u128'),
        ('to_amount', 'u128'),
    ],
    'cancel_reservation': [('miner', 'AccountId')],
    'vote_initiate': [
        ('request_hash', 'hash'),
        ('user', 'AccountId'),
        ('miner', 'AccountId'),
        ('from_chain', 'str'),
        ('to_chain', 'str'),
        ('from_amount', 'u128'),
        ('tao_amount', 'u128'),
        ('user_from_address', 'str'),
        ('user_to_address', 'str'),
        ('from_tx_hash', 'str'),
        ('from_tx_block', 'u32'),
        ('to_amount', 'u128'),
        ('miner_from_address', 'str'),
        ('miner_to_address', 'str'),
        ('rate', 'str'),
    ],
    'vote_activate': [('miner', 'AccountId')],
    'vote_deactivate': [('miner', 'AccountId')],
    'mark_fulfilled': [('swap_id', 'u64'), ('to_tx_hash', 'str'), ('to_tx_block', 'u32'), ('to_amount', 'u128')],
    'confirm_swap': [('swap_id', 'u64')],
    'timeout_swap': [('swap_id', 'u64')],
    'claim_slash': [('swap_id', 'u64')],
    'deactivate': [('miner', 'AccountId')],
    'transfer_ownership': [('new_owner', 'AccountId')],
    'add_validator': [('validator', 'AccountId')],
    'remove_validator': [('validator', 'AccountId')],
    'set_fulfillment_timeout': [('blocks', 'u32')],
    'set_min_collateral': [('amount', 'u128')],
    'set_max_collateral': [('amount', 'u128')],
    'set_consensus_threshold': [('percent', 'u8')],
    'set_min_swap_amount': [('amount', 'u128')],
    'set_max_swap_amount': [('amount', 'u128')],
    'set_reservation_ttl': [('blocks', 'u32')],
    'recycle_fees': [],
    'enable_chain_ext': [],
    'get_swap': [('swap_id', 'u64')],
    'get_collateral': [('hotkey', 'AccountId')],
    'get_miner_active': [('hotkey', 'AccountId')],
    'get_miner_has_active_swap': [('hotkey', 'AccountId')],
    'get_miner_snapshot': [('miner', 'AccountId')],
    'is_validator': [('account', 'AccountId')],
    'get_next_swap_id': [],
    'get_fulfillment_timeout': [],
    'get_min_collateral': [],
    'get_max_collateral': [],
    'get_miner_deactivation_block': [('miner', 'AccountId')],
    'get_consensus_threshold': [],
    'get_validator_count': [],
    'get_validators': [],
    'get_reservation_data': [('miner', 'AccountId')],
    'get_reservation': [('miner', 'AccountId')],
    'get_pending_reserve_vote_count': [('miner', 'AccountId')],
    'propose_extend_reservation': [
        ('miner', 'AccountId'),
        ('from_tx_hash', 'hash'),
        ('target_block', 'u32'),
    ],
    'challenge_extend_reservation': [('miner', 'AccountId')],
    'finalize_extend_reservation': [('miner', 'AccountId')],
    'get_pending_reservation_extension': [('miner', 'AccountId')],
    'get_reservation_extension_count': [('miner', 'AccountId')],
    'propose_extend_timeout': [
        ('swap_id', 'u64'),
        ('target_block', 'u32'),
    ],
    'challenge_extend_timeout': [('swap_id', 'u64')],
    'finalize_extend_timeout': [('swap_id', 'u64')],
    'get_pending_timeout_extension': [('swap_id', 'u64')],
    'get_swap_extension_count': [('swap_id', 'u64')],
    'get_cooldown': [('from_address', 'str')],
    'set_halted': [('halted', 'bool')],
    'get_halted': [],
    'get_accumulated_fees': [],
    'get_total_recycled_fees': [],
    'get_owner': [],
    'get_pending_slash': [('swap_id', 'u64')],
    'get_min_swap_amount': [],
    'get_max_swap_amount': [],
    'get_miner_reserved_until': [('miner', 'AccountId')],
    'get_reservation_ttl': [],
}

DEFAULT_GAS_LIMIT = {'ref_time': 10_000_000_000, 'proof_size': 500_000}

_EXTRINSIC_NOT_FOUND = (ExtrinsicNotFound,)


# ContractExecResult byte layout offsets (after gas prefix)
_GAS_PREFIX_BYTES = 16  # Skip gas consumed/required
_RESULT_OK_OFFSET = 10  # Byte indicating Ok(0x00) vs Err in Result
_FLAGS_OFFSET = 11  # 4-byte flags field
_DATA_COMPACT_OFFSET = 15  # Start of compact-encoded data length

# =========================================================================
# Error types
# =========================================================================


# Ink! contract error variants — order must match smart-contracts/ink/errors.rs enum
CONTRACT_ERROR_VARIANTS = {
    0: ('NotOwner', 'Caller is not the contract owner'),
    1: ('InsufficientCollateral', 'Insufficient collateral to cover swap volume'),
    2: ('SwapNotFound', 'Swap ID not found'),
    3: ('AlreadyVoted', 'Validator has already voted on this swap'),
    4: ('InvalidStatus', 'Swap is not in the expected status for this operation'),
    5: ('MinerNotActive', 'Miner is not active'),
    6: ('MinerStillActive', 'Miner is still active (must deactivate before withdrawing)'),
    7: ('TransferFailed', 'Transfer failed'),
    8: ('NotTimedOut', 'Swap has not timed out yet'),
    9: ('NotAssignedMiner', 'Caller is not the assigned miner for this swap'),
    10: ('NotValidator', 'Caller is not a registered validator'),
    11: ('DuplicateSourceTx', 'Source transaction hash already used in another swap'),
    12: ('InvalidAmount', 'Swap amounts must be greater than zero'),
    13: ('NoPendingSlash', 'No pending slash to claim'),
    14: ('InputEmpty', 'Required input is empty'),
    15: ('InputTooLong', 'Input string exceeds maximum allowed length'),
    16: ('MinerHasActiveSwap', 'Miner already has an active swap'),
    17: ('WithdrawalCooldown', 'Withdrawal cooldown not met'),
    18: ('AmountBelowMinimum', 'Swap amount below minimum'),
    19: ('AmountAboveMaximum', 'Swap amount above maximum'),
    20: ('MinerReserved', 'Miner is already reserved by another user'),
    21: ('NoReservation', 'No active reservation for this miner'),
    22: ('ExceedsMaxCollateral', 'Collateral exceeds maximum allowed'),
    23: ('HashMismatch', 'Request hash does not match computed hash'),
    24: ('PendingConflict', 'A pending vote exists for a different request'),
    25: ('SameChain', 'Source and destination chains must be different'),
    26: ('SystemHalted', 'System is halted — no new activity allowed'),
    27: ('ProposalAlreadyPending', 'An optimistic extension proposal already exists for this entity'),
    28: ('ChallengeWindowOpen', 'Cannot finalize: challenge window has not yet elapsed'),
    29: ('ChallengeWindowClosed', 'Cannot challenge: challenge window has already elapsed'),
    30: ('NoProposal', 'No pending optimistic extension exists for this entity'),
    31: ('ExtensionTooLong', 'Proposed extension target exceeds MAX_EXTENSION_BLOCKS'),
    32: ('TargetNotForward', 'Proposed target must be strictly greater than the current deadline'),
    33: ('InvalidTarget', 'Proposed target is invalid (e.g. not strictly in the future)'),
    34: ('MaxExtensionsExceeded', 'Cumulative extension cap reached for this reservation/swap'),
}


class ContractError(Exception):
    """Raised when a contract call fails.

    A failure can be one of: contract not initialized, RPC failure, unknown
    method selector, insufficient balance, or the contract explicitly
    rejecting the call (a.k.a. "ContractReverted"). Callers that need to
    distinguish "contract deliberately rejected" from "something else went
    wrong" should use ``is_contract_rejection`` — it's the only branch we
    reliably want to differentiate.
    """


def is_contract_rejection(e: BaseException) -> bool:
    """Return True if ``e`` represents a contract-side rejection.

    Matches both our own ContractError messages that include ``contract
    rejected`` (explicit revert), and substrate's ``ContractReverted`` string
    which bubbles up from signed extrinsics. One place to keep this check in
    sync so callers don't re-implement the string match.
    """
    msg = str(e)
    return 'contract rejected' in msg or 'ContractReverted' in msg


# =========================================================================
# Client
# =========================================================================


def get_contract_address() -> Optional[str]:
    return os.environ.get('CONTRACT_ADDRESS') or CONTRACT_ADDRESS


# Errors that signal a wedged substrate WebSocket — caller can swap the
# connection and retry once. Anything else propagates as-is.
RECONNECT_EXCEPTIONS: tuple[type[BaseException], ...] = (
    ConnectionClosed,
    ConnectionError,
    TimeoutError,
)


class AllwaysContractClient:
    """Client for the Allways Swap Manager contract using raw RPC calls."""

    def __init__(
        self,
        contract_address: Optional[str] = None,
        subtensor: Optional[bt.Subtensor] = None,
        reconnect_subtensor: Optional[Callable[[], None]] = None,
    ):
        self.contract_address = contract_address or get_contract_address() or ''
        self.subtensor = subtensor
        # Owner-supplied callback that rebuilds substrate state and updates
        # ``self.subtensor`` to a fresh ``bt.Subtensor``. Invoked at most once
        # per RPC call when a connection-class error fires; on success the
        # call is retried, on failure the original error is re-raised.
        self.reconnect_subtensor = reconnect_subtensor
        self.readonly_keypair = Keypair.create_from_uri('//Alice')
        self.initialized = False
        # substrate-interface's WebSocketProvider isn't thread-safe; serialize
        # access so concurrent threads can't both land in recv at the same time.
        self._substrate_lock = threading.Lock()

        if not self.contract_address:
            bt.logging.warning('Allways contract address not set')

    def substrate_call(self, fn: Callable[[Any], T]) -> T:
        """Run ``fn(substrate)``; on a connection-class error, invoke the
        reconnect callback once and retry. Caller's callback is responsible
        for replacing ``self.subtensor`` with a fresh handle.

        Retry is safe for reads and for writes that the contract guards
        against duplicates (e.g. vote_*, mark_fulfilled — already-voted /
        status checks reject the second submission). Do NOT use this path
        for value-bearing or otherwise non-idempotent extrinsics: if the
        WS dies after the node accepted the first extrinsic but before the
        receipt returned, the retry composes a fresh nonce and submits a
        second extrinsic that can also land.
        """
        with self._substrate_lock:
            try:
                return fn(self.subtensor.substrate)
            except RECONNECT_EXCEPTIONS as e:
                if self.reconnect_subtensor is None:
                    raise
                bt.logging.warning(f'Substrate WS error ({e!s}); reconnecting and retrying once')
                try:
                    self.reconnect_subtensor()
                except Exception as reconnect_err:
                    bt.logging.error(f'Substrate reconnect callback failed: {reconnect_err}')
                    raise e from reconnect_err
                return fn(self.subtensor.substrate)

    def ensure_initialized(self):
        if not self.contract_address:
            raise ContractError('contract address not set')
        if not self.subtensor:
            raise ContractError('subtensor not available')
        if not self.initialized:
            bt.logging.info(f'Contract client ready for {self.contract_address}')
            self.initialized = True

    # =========================================================================
    # Raw RPC layer
    # =========================================================================

    def raw_contract_read(
        self,
        method: str,
        args: Optional[dict] = None,
        caller: Optional[Keypair] = None,
    ) -> Optional[bytes]:
        """Read from contract via raw state_call RPC.

        Returns the SCALE-encoded return payload after stripping the
        ContractExecResult envelope and ink! Result discriminant.
        Returns None on any error or contract revert.
        """
        try:
            selector = CONTRACT_SELECTORS.get(method)
            if not selector:
                return None

            encoded_args = self.encode_args(method, args or {})
            input_data = selector + encoded_args

            kp = caller or self.readonly_keypair
            substrate = self.subtensor.substrate

            origin = bytes.fromhex(substrate.ss58_decode(kp.ss58_address))
            dest = bytes.fromhex(substrate.ss58_decode(self.contract_address))
            value = b'\x00' * 8  # Subtensor Balance is u64
            gas_limit = b'\x00'  # None for dry-run
            storage_limit = b'\x00'  # None for dry-run

            prefix = origin + dest + value + gas_limit + storage_limit
            call_params = prefix + compact_encode_len(len(input_data)) + input_data
            result = self.substrate_call(
                lambda s: s.rpc_request('state_call', ['ContractsApi_call', '0x' + call_params.hex()])
            )

            if not result.get('result'):
                return None

            raw = bytes.fromhex(strip_hex_prefix(result['result']))
            if len(raw) < 32:
                return None

            # ContractExecResult layout after gas prefix:
            # StorageDeposit(9) + debug_message(1) + Result(1) + flags(4) + data(compact+bytes)
            r = raw[_GAS_PREFIX_BYTES:]
            if len(r) < _DATA_COMPACT_OFFSET or r[_RESULT_OK_OFFSET] != 0x00:
                return None

            flags, _ = decode_u32(r, _FLAGS_OFFSET)
            is_revert = bool(flags & 1)

            data_compact = r[_DATA_COMPACT_OFFSET]
            data_mode = data_compact & 0x03
            if data_mode == 0:
                data_len = data_compact >> 2
                data_start = 16
            elif data_mode == 1:
                if len(r) < 17:
                    return None
                data_len = (r[15] | (r[16] << 8)) >> 2
                data_start = 17
            else:
                return None

            if len(r) < data_start + data_len or data_len < 1:
                if is_revert:
                    raise ContractError(f'{method}: contract rejected (no details)')
                return None

            # REVERT flag means the contract returned Err — decode the error variant.
            # Data layout: [LangError discriminant] [Result discriminant] [Error variant byte]
            if is_revert:
                raise self.decode_contract_error(method, r, data_start, data_len)

            # First byte is ink! LangError discriminant (0x00 = Ok)
            if r[data_start] != 0x00:
                return None

            return r[data_start + 1 : data_start + data_len]

        except ContractError:
            raise
        except RECONNECT_EXCEPTIONS:
            # Don't mask transient WS failures as "no result" — callers need
            # to distinguish "RPC blip, retry me" from "contract returned None".
            raise
        except Exception as e:
            bt.logging.debug(f'Raw contract read {method} failed: {e}')
            return None

    @staticmethod
    def decode_contract_error(method: str, r: bytes, data_start: int, data_len: int) -> ContractError:
        """Decode an ink! contract error variant from the REVERT payload.

        Data layout: [LangError discriminant (1)] [Result Err discriminant (1)] [Error variant (1)]
        """
        payload = r[data_start : data_start + data_len]
        # payload[0] = LangError (0x00 = Ok), payload[1] = Result (0x01 = Err), payload[2] = variant
        if len(payload) >= 3 and payload[0] == 0x00 and payload[1] == 0x01:
            variant_idx = payload[2]
            variant = CONTRACT_ERROR_VARIANTS.get(variant_idx)
            if variant:
                name, description = variant
                return ContractError(f'{method}: {name} — {description}')
            return ContractError(f'{method}: unknown error variant ({variant_idx})')
        return ContractError(f'{method}: contract rejected')

    def exec_contract_raw(
        self,
        method: str,
        args: Optional[dict] = None,
        keypair: Optional[Keypair] = None,
        value: int = 0,
        gas_limit: dict = None,
        wait_for_inclusion: bool = True,
    ) -> str:
        """Execute a contract method via raw extrinsic submission. Returns tx hash.

        ``wait_for_inclusion=False`` returns immediately after submission and
        skips the receipt-success check, so contract reverts can't be detected
        in-band. Use only for idempotent calls whose dedup is contract-side
        (e.g. propose/challenge extension flows that the contract rejects via
        ProposalAlreadyPending / ChallengeWindow* if our local snapshot is
        stale). The forward step gets back a step-time worth of latency that
        a synchronous inclusion wait would otherwise burn.
        """
        gas_limit = gas_limit or DEFAULT_GAS_LIMIT
        selector = CONTRACT_SELECTORS.get(method)
        if not selector:
            raise ContractError(f'{method}: unknown method')

        encoded_args = self.encode_args(method, args or {})
        call_data = selector + encoded_args

        signer_address = keypair.ss58_address
        try:
            account_info = self.substrate_call(lambda s: s.query('System', 'Account', [signer_address]))
            account_data = account_info.value if hasattr(account_info, 'value') else account_info
            free_balance = account_data.get('data', {}).get('free', 0)
            if free_balance < MIN_BALANCE_FOR_TX_RAO:
                bt.logging.warning(
                    f'{method}: low free balance on {signer_address} ({free_balance} rao); '
                    f'submitting anyway — chain will reject if truly insufficient'
                )
        except Exception as e:
            bt.logging.debug(f'{method}: pre-flight balance probe failed, proceeding: {e}')

        def submit_extrinsic(s):
            call = s.compose_call(
                call_module='Contracts',
                call_function='call',
                call_params={
                    'dest': {'Id': self.contract_address},
                    'value': value,
                    'gas_limit': gas_limit,
                    'storage_deposit_limit': None,
                    'data': '0x' + call_data.hex(),
                },
            )
            extrinsic = s.create_signed_extrinsic(call=call, keypair=keypair)
            return s.submit_extrinsic(extrinsic, wait_for_inclusion=wait_for_inclusion, wait_for_finalization=False)

        try:
            receipt = self.substrate_call(submit_extrinsic)
        except Exception as e:
            raise ContractError(f'{method}: exec failed: {e}') from e

        if not wait_for_inclusion:
            # No block to inspect; trust the submission and let the next event
            # sync surface the actual outcome. Caller relies on contract-side
            # idempotency to absorb stale-view duplicates.
            return receipt.extrinsic_hash

        try:
            if receipt.is_success:
                return receipt.extrinsic_hash
            else:
                raise ContractError(f'{method}: {receipt.error_message}')
        except _EXTRINSIC_NOT_FOUND:
            return receipt.extrinsic_hash

    # =========================================================================
    # SCALE encoding / decoding helpers
    # =========================================================================

    def encode_args(self, method: str, args: dict) -> bytes:
        arg_types = CONTRACT_ARG_TYPES.get(method, [])
        encoded = b''
        for arg_name, type_tag in arg_types:
            if arg_name not in args:
                raise ValueError(f'Missing argument: {arg_name}')
            v = args[arg_name]
            encoded += self.encode_value(v, type_tag)
        return encoded

    def encode_value(self, value, type_tag: str) -> bytes:
        if type_tag == 'u8':
            return struct.pack('B', int(value))
        elif type_tag == 'hash':
            if isinstance(value, str):
                return bytes.fromhex(strip_hex_prefix(value))
            return bytes(value)[:ACCOUNT_ID_BYTES].ljust(ACCOUNT_ID_BYTES, b'\x00')
        elif type_tag == 'bytes':
            data = value if isinstance(value, (bytes, bytearray)) else value.encode('utf-8')
            return encode_bytes(data)
        elif type_tag == 'u32':
            return struct.pack('<I', int(value))
        elif type_tag == 'u64':
            return struct.pack('<Q', int(value))
        elif type_tag == 'u128':
            return encode_u128(int(value))
        elif type_tag == 'bool':
            return b'\x01' if value else b'\x00'
        elif type_tag == 'AccountId':
            if isinstance(value, str):
                return bytes.fromhex(self.subtensor.substrate.ss58_decode(value))
            return bytes(value)
        elif type_tag == 'str':
            return encode_str(value) if isinstance(value, str) else encode_bytes(value)
        elif type_tag == 'vec_u64':
            items = list(value)
            encoded = compact_encode_len(len(items))
            for item in items:
                encoded += struct.pack('<Q', int(item))
            return encoded
        raise ValueError(f'Unsupported type: {type_tag}')

    def extract_u16(self, data: bytes) -> Optional[int]:
        if not data or len(data) < U16_BYTES:
            return None
        return decode_u16(data, 0)[0]

    def extract_u32(self, data: bytes) -> Optional[int]:
        if not data or len(data) < U32_BYTES:
            return None
        return decode_u32(data, 0)[0]

    def extract_u64(self, data: bytes) -> Optional[int]:
        if not data or len(data) < U64_BYTES:
            return None
        return decode_u64(data, 0)[0]

    def extract_u128(self, data: bytes) -> Optional[int]:
        if not data or len(data) < U128_BYTES:
            return None
        return decode_u128(data, 0)[0]

    def extract_bool(self, data: bytes) -> Optional[bool]:
        if not data:
            return None
        return data[0] != 0

    def extract_account_id(self, data: bytes) -> Optional[str]:
        if not data or len(data) < ACCOUNT_ID_BYTES:
            return None
        return decode_account_id(data, 0)[0]

    def decode_swap_data(self, data: bytes, offset: int = 0) -> Optional[Swap]:
        """Decode a SwapData struct from raw SCALE bytes."""
        try:
            o = offset
            swap_id, o = decode_u64(data, o)
            user, o = decode_account_id(data, o)
            miner, o = decode_account_id(data, o)
            from_chain, o = decode_string(data, o)
            to_chain, o = decode_string(data, o)
            from_amount, o = decode_u128(data, o)
            to_amount, o = decode_u128(data, o)
            tao_amount, o = decode_u128(data, o)
            user_from_address, o = decode_string(data, o)
            user_to_address, o = decode_string(data, o)
            miner_from_address, o = decode_string(data, o)
            miner_to_address, o = decode_string(data, o)
            rate, o = decode_string(data, o)
            from_tx_hash, o = decode_string(data, o)
            from_tx_block, o = decode_u32(data, o)
            to_tx_hash, o = decode_string(data, o)
            to_tx_block, o = decode_u32(data, o)
            status_byte = data[o]
            o += 1
            status = SwapStatus(status_byte) if status_byte <= 3 else SwapStatus.ACTIVE
            initiated_block, o = decode_u32(data, o)
            timeout_block, o = decode_u32(data, o)
            fulfilled_block, o = decode_u32(data, o)
            completed_block, o = decode_u32(data, o)

            return Swap(
                id=swap_id,
                user_hotkey=user,
                miner_hotkey=miner,
                from_chain=from_chain,
                to_chain=to_chain,
                from_amount=from_amount,
                to_amount=to_amount,
                tao_amount=tao_amount,
                user_from_address=user_from_address,
                user_to_address=user_to_address,
                miner_from_address=miner_from_address,
                miner_to_address=miner_to_address,
                rate=rate,
                from_tx_hash=from_tx_hash,
                from_tx_block=from_tx_block,
                to_tx_hash=to_tx_hash,
                to_tx_block=to_tx_block,
                status=status,
                initiated_block=initiated_block,
                timeout_block=timeout_block,
                fulfilled_block=fulfilled_block,
                completed_block=completed_block,
            )
        except Exception as e:
            bt.logging.debug(f'Failed to decode SwapData: {e}')
            return None

    # =========================================================================
    # Read helpers (typed wrappers over _raw_contract_read)
    # =========================================================================

    def _read_typed(self, method: str, extractor, default, args: dict = None):
        self.ensure_initialized()
        data = self.raw_contract_read(method, args)
        if data is None:
            raise ContractError(f'{method}: no response')
        v = extractor(data)
        return v if v is not None else default

    def read_u16(self, method: str, args: dict = None) -> int:
        return self._read_typed(method, self.extract_u16, 0, args)

    def read_u32(self, method: str, args: dict = None) -> int:
        return self._read_typed(method, self.extract_u32, 0, args)

    def read_u64(self, method: str, args: dict = None) -> int:
        return self._read_typed(method, self.extract_u64, 0, args)

    def read_u128(self, method: str, args: dict = None) -> int:
        return self._read_typed(method, self.extract_u128, 0, args)

    def read_bool(self, method: str, args: dict = None) -> bool:
        return self._read_typed(method, self.extract_bool, False, args)

    def read_account_id(self, method: str, args: dict = None) -> str:
        return self._read_typed(method, self.extract_account_id, '', args)

    def read_option_swap(self, method: str, args: dict = None, caller=None) -> Optional[Swap]:
        """Read a method that returns Option<SwapData>."""
        self.ensure_initialized()
        data = self.raw_contract_read(method, args, caller=caller)
        if data is None or len(data) < 1:
            return None
        # Option discriminant: 0x00 = None, 0x01 = Some
        if data[0] == 0x00:
            return None
        if data[0] == 0x01:
            return self.decode_swap_data(data, offset=1)
        return None

    def decode_reservation(self, data: bytes, offset: int = 0) -> Optional[Reservation]:
        try:
            o = offset
            hash_bytes = data[o : o + 32]
            o += 32
            from_addr, o = decode_string(data, o)
            from_chain, o = decode_string(data, o)
            to_chain, o = decode_string(data, o)
            tao_amount, o = decode_u128(data, o)
            from_amount, o = decode_u128(data, o)
            to_amount, o = decode_u128(data, o)
            reserved_until, o = decode_u32(data, o)
            return Reservation(
                hash='0x' + hash_bytes.hex(),
                from_addr=from_addr,
                from_chain=from_chain,
                to_chain=to_chain,
                tao_amount=tao_amount,
                from_amount=from_amount,
                to_amount=to_amount,
                reserved_until=reserved_until,
            )
        except Exception as e:
            bt.logging.debug(f'Failed to decode Reservation: {e}')
            return None

    # =========================================================================
    # Query Functions (Read-only)
    # =========================================================================

    def get_swap(self, swap_id: int) -> Optional[Swap]:
        """Get an active/fulfilled swap by ID. Returns None if not found or already resolved."""
        return self.read_option_swap('get_swap', {'swap_id': swap_id})

    def get_active_swaps(self, max_gap: int = 50) -> List[Swap]:
        """Scan backward from latest swap ID, returning all ACTIVE/FULFILLED swaps.

        Stops after max_gap consecutive None results (pruned/resolved gaps).
        """
        self.ensure_initialized()
        next_id = self.get_next_swap_id()
        if next_id <= 1:
            return []

        active_statuses = (SwapStatus.ACTIVE, SwapStatus.FULFILLED)
        swaps = []
        consecutive_none = 0
        for swap_id in range(next_id - 1, 0, -1):
            swap = self.get_swap(swap_id)
            if swap is None:
                consecutive_none += 1
                if consecutive_none >= max_gap:
                    break
            else:
                consecutive_none = 0
                if swap.status in active_statuses:
                    swaps.append(swap)

        swaps.reverse()
        return swaps

    def get_miner_active_swaps(self, hotkey: str, max_gap: int = 50) -> List[Swap]:
        return [s for s in self.get_active_swaps(max_gap) if s.miner_hotkey == hotkey]

    def get_miner_collateral(self, hotkey: str) -> int:
        return self.read_u128('get_collateral', {'hotkey': hotkey})

    def get_fulfillment_timeout(self) -> int:
        return self.read_u32('get_fulfillment_timeout')

    def get_miner_active_flag(self, hotkey: str) -> bool:
        return self.read_bool('get_miner_active', {'hotkey': hotkey})

    def get_miner_has_active_swap(self, hotkey: str) -> bool:
        return self.read_bool('get_miner_has_active_swap', {'hotkey': hotkey})

    def get_miner_snapshot(self, hotkey: str) -> Tuple[int, bool, bool, int, int]:
        """Composite miner read: (collateral, active, has_active_swap,
        reserved_until, deactivation_block). One RPC round-trip.
        """
        self.ensure_initialized()
        data = self.raw_contract_read('get_miner_snapshot', {'miner': hotkey})
        if data is None or len(data) < 26:
            return (0, False, False, 0, 0)
        collateral_lo = struct.unpack_from('<Q', data, 0)[0]
        collateral_hi = struct.unpack_from('<Q', data, 8)[0]
        collateral = collateral_lo + (collateral_hi << 64)
        active = data[16] != 0
        has_active_swap = data[17] != 0
        reserved_until = struct.unpack_from('<I', data, 18)[0]
        deactivation_block = struct.unpack_from('<I', data, 22)[0]
        return (collateral, active, has_active_swap, reserved_until, deactivation_block)

    def get_next_swap_id(self) -> int:
        return self.read_u64('get_next_swap_id')

    def get_pending_slash(self, swap_id: int) -> int:
        return self.read_u128('get_pending_slash', {'swap_id': swap_id})

    def get_min_collateral(self) -> int:
        return self.read_u128('get_min_collateral')

    def get_max_collateral(self) -> int:
        return self.read_u128('get_max_collateral')

    def get_miner_deactivation_block(self, hotkey: str) -> int:
        return self.read_u32('get_miner_deactivation_block', {'miner': hotkey})

    def get_consensus_threshold(self) -> int:
        self.ensure_initialized()
        data = self.raw_contract_read('get_consensus_threshold')
        if data is None or len(data) < 1:
            return 0
        return data[0]

    def get_validator_count(self) -> int:
        return self.read_u32('get_validator_count')

    def get_pending_reserve_vote_count(self, miner_hotkey: str) -> int:
        return self.read_u32('get_pending_reserve_vote_count', {'miner': miner_hotkey})

    def get_cooldown(self, from_address: str) -> Tuple[int, int]:
        """Returns (strike_count, last_expired_block) for a source address."""
        self.ensure_initialized()
        data = self.raw_contract_read('get_cooldown', {'from_address': from_address})
        if data is None or len(data) < 5:
            return (0, 0)
        strike_count = data[0]
        last_expired, _ = decode_u32(data, 1)
        return (strike_count, last_expired)

    def get_accumulated_fees(self) -> int:
        return self.read_u128('get_accumulated_fees')

    def get_total_recycled_fees(self) -> int:
        return self.read_u128('get_total_recycled_fees')

    def get_min_swap_amount(self) -> int:
        return self.read_u128('get_min_swap_amount')

    def get_max_swap_amount(self) -> int:
        return self.read_u128('get_max_swap_amount')

    def get_owner(self) -> str:
        return self.read_account_id('get_owner')

    def get_halted(self) -> bool:
        return self.read_bool('get_halted')

    def get_recycle_address(self) -> str:
        return self.read_account_id('get_recycle_address')

    def get_staking_hotkey(self) -> str:
        return self.read_account_id('get_staking_hotkey')

    def get_netuid(self) -> int:
        return self.read_u16('get_netuid')

    def get_chain_ext_enabled(self) -> bool:
        return self.read_bool('get_chain_ext_enabled')

    def is_validator(self, account: str) -> bool:
        return self.read_bool('is_validator', {'account': account})

    def get_validators(self) -> List[str]:
        """Return all whitelisted validator SS58 addresses.

        Payload is a SCALE Vec<AccountId>: compact-encoded length followed
        by N * 32 bytes. Returns [] on any read/decode failure so callers
        can treat it like an empty set without special-casing.
        """
        self.ensure_initialized()
        data = self.raw_contract_read('get_validators')
        if not data:
            return []
        try:
            first = data[0]
            mode = first & 0x03
            if mode == 0:
                count = first >> 2
                offset = 1
            elif mode == 1:
                if len(data) < 2:
                    return []
                count = (first | (data[1] << 8)) >> 2
                offset = 2
            else:
                if len(data) < 4:
                    return []
                count = (first | (data[1] << 8) | (data[2] << 16) | (data[3] << 24)) >> 2
                offset = 4
            validators: List[str] = []
            for _ in range(count):
                if offset + ACCOUNT_ID_BYTES > len(data):
                    break
                addr, offset = decode_account_id(data, offset)
                validators.append(addr)
            return validators
        except Exception as e:
            bt.logging.debug(f'get_validators decode failed: {e}')
            return []

    def get_miner_reserved_until(self, miner_hotkey: str) -> int:
        return self.read_u32('get_miner_reserved_until', {'miner': miner_hotkey})

    def get_reservation_ttl(self) -> int:
        return self.read_u32('get_reservation_ttl')

    def get_reservation_data(self, miner_hotkey: str) -> Optional[Tuple[int, int, int]]:
        """Get reservation amounts for a miner.

        Returns (tao_amount, from_amount, to_amount) or None. Callers that
        also need reserved_until should use ``get_miner_reserved_until``.
        """
        self.ensure_initialized()
        data = self.raw_contract_read('get_reservation_data', {'miner': miner_hotkey})
        if data is None or len(data) < 1:
            return None
        # Option discriminant: 0x00 = None, 0x01 = Some
        if data[0] == 0x00:
            return None
        if data[0] != 0x01:
            return None
        o = 1
        tao_amount, o = decode_u128(data, o)
        from_amount, o = decode_u128(data, o)
        to_amount, _ = decode_u128(data, o)
        return (tao_amount, from_amount, to_amount)

    def get_reservation(self, miner_hotkey: str) -> Optional[Reservation]:
        """Full reservation record (hash, from_addr, amounts, reserved_until)."""
        self.ensure_initialized()
        data = self.raw_contract_read('get_reservation', {'miner': miner_hotkey})
        if data is None or len(data) < 1 or data[0] != 0x01:
            return None
        return self.decode_reservation(data, offset=1)

    # =========================================================================
    # Transaction Functions (Write)
    # =========================================================================

    def _exec_logged(
        self,
        method: str,
        wallet: bt.Wallet,
        log_msg: str,
        args: Optional[dict] = None,
        value: int = 0,
        wait_for_inclusion: bool = True,
    ) -> str:
        self.ensure_initialized()
        tx_hash = self.exec_contract_raw(
            method,
            args=args,
            keypair=wallet.hotkey,
            value=value,
            wait_for_inclusion=wait_for_inclusion,
        )
        bt.logging.info(f'{log_msg}: {tx_hash}')
        return tx_hash

    def post_collateral(self, wallet: bt.Wallet, amount_rao: int) -> str:
        """Post collateral to the contract. Amount is sent as value with the extrinsic."""
        return self._exec_logged('post_collateral', wallet, 'Collateral posted', value=amount_rao)

    def withdraw_collateral(self, wallet: bt.Wallet, amount_rao: int) -> str:
        return self._exec_logged('withdraw_collateral', wallet, 'Collateral withdrawn', {'amount': amount_rao})

    def vote_reserve(
        self,
        wallet: bt.Wallet,
        request_hash: bytes,
        miner_hotkey: str,
        user_from_address: str,
        from_chain: str,
        to_chain: str,
        tao_amount: int,
        from_amount: int,
        to_amount: int,
    ) -> str:
        return self._exec_logged(
            'vote_reserve',
            wallet,
            f'Vote reserve for miner {miner_hotkey}',
            {
                'request_hash': request_hash,
                'miner': miner_hotkey,
                'user_from_address': user_from_address,
                'from_chain': from_chain,
                'to_chain': to_chain,
                'tao_amount': tao_amount,
                'from_amount': from_amount,
                'to_amount': to_amount,
            },
        )

    # ─── Optimistic extensions ────────────────────────────────────────────
    # Single-validator propose/challenge/finalize with tiered evidence + cap.

    def propose_extend_reservation(
        self,
        wallet: bt.Wallet,
        miner_hotkey: str,
        from_tx_hash: bytes,
        target_block: int,
    ) -> str:
        return self._exec_logged(
            'propose_extend_reservation',
            wallet,
            f'Propose extend reservation miner={miner_hotkey} target={target_block}',
            {'miner': miner_hotkey, 'from_tx_hash': from_tx_hash, 'target_block': target_block},
            wait_for_inclusion=False,
        )

    def challenge_extend_reservation(self, wallet: bt.Wallet, miner_hotkey: str) -> str:
        return self._exec_logged(
            'challenge_extend_reservation',
            wallet,
            f'Challenge extend reservation miner={miner_hotkey}',
            {'miner': miner_hotkey},
            wait_for_inclusion=False,
        )

    def finalize_extend_reservation(self, wallet: bt.Wallet, miner_hotkey: str) -> str:
        return self._exec_logged(
            'finalize_extend_reservation',
            wallet,
            f'Finalize extend reservation miner={miner_hotkey}',
            {'miner': miner_hotkey},
        )

    def propose_extend_timeout(
        self,
        wallet: bt.Wallet,
        swap_id: int,
        target_block: int,
    ) -> str:
        return self._exec_logged(
            'propose_extend_timeout',
            wallet,
            f'Propose extend timeout swap={swap_id} target={target_block}',
            {'swap_id': swap_id, 'target_block': target_block},
            wait_for_inclusion=False,
        )

    def challenge_extend_timeout(self, wallet: bt.Wallet, swap_id: int) -> str:
        return self._exec_logged(
            'challenge_extend_timeout',
            wallet,
            f'Challenge extend timeout swap={swap_id}',
            {'swap_id': swap_id},
            wait_for_inclusion=False,
        )

    def finalize_extend_timeout(self, wallet: bt.Wallet, swap_id: int) -> str:
        return self._exec_logged(
            'finalize_extend_timeout',
            wallet,
            f'Finalize extend timeout swap={swap_id}',
            {'swap_id': swap_id},
        )

    def _decode_pending_extension(self, data: bytes) -> Optional[PendingExtension]:
        """Decode an Option<PendingExtension> SCALE payload."""
        if not data:
            return None
        # Option discriminant: 0x00 = None, 0x01 = Some
        if data[0] == 0x00:
            return None
        if data[0] != 0x01:
            return None
        o = 1
        submitter, o = decode_account_id(data, o)
        target_block, o = decode_u32(data, o)
        proposed_at, _ = decode_u32(data, o)
        return PendingExtension(submitter=submitter, target_block=target_block, proposed_at=proposed_at)

    def get_pending_reservation_extension(self, miner_hotkey: str) -> Optional[PendingExtension]:
        self.ensure_initialized()
        data = self.raw_contract_read('get_pending_reservation_extension', {'miner': miner_hotkey})
        if data is None:
            return None
        return self._decode_pending_extension(data)

    def get_pending_timeout_extension(self, swap_id: int) -> Optional[PendingExtension]:
        self.ensure_initialized()
        data = self.raw_contract_read('get_pending_timeout_extension', {'swap_id': swap_id})
        if data is None:
            return None
        return self._decode_pending_extension(data)

    def get_reservation_extension_count(self, miner_hotkey: str) -> int:
        """How many extensions have been finalized on this miner's current
        reservation. Drives tier selection. Returns 0 on read failure or for
        miners with no extensions yet — both are equivalent for tier-1
        eligibility."""
        self.ensure_initialized()
        data = self.raw_contract_read('get_reservation_extension_count', {'miner': miner_hotkey})
        if not data:
            return 0
        return int(data[0])

    def get_swap_extension_count(self, swap_id: int) -> int:
        """Counterpart for the timeout side."""
        self.ensure_initialized()
        data = self.raw_contract_read('get_swap_extension_count', {'swap_id': swap_id})
        if not data:
            return 0
        return int(data[0])

    def cancel_reservation(self, wallet: bt.Wallet, miner_hotkey: str) -> str:
        return self._exec_logged(
            'cancel_reservation',
            wallet,
            f'Reservation cancelled for {miner_hotkey}',
            {'miner': miner_hotkey},
        )

    def vote_initiate(
        self,
        wallet: bt.Wallet,
        request_hash: bytes,
        user_hotkey: str,
        miner_hotkey: str,
        from_chain: str,
        to_chain: str,
        from_amount: int,
        tao_amount: int,
        user_from_address: str,
        user_to_address: str,
        from_tx_hash: str,
        from_tx_block: int = 0,
        to_amount: int = 0,
        miner_from_address: str = '',
        miner_to_address: str = '',
        rate: str = '',
    ) -> str:
        """Vote to initiate a swap. On quorum, swap is created on contract."""
        return self._exec_logged(
            'vote_initiate',
            wallet,
            f'Vote initiate for miner {miner_hotkey}',
            {
                'request_hash': request_hash,
                'user': user_hotkey,
                'miner': miner_hotkey,
                'from_chain': from_chain,
                'to_chain': to_chain,
                'from_amount': from_amount,
                'tao_amount': tao_amount,
                'user_from_address': user_from_address,
                'user_to_address': user_to_address,
                'from_tx_hash': from_tx_hash,
                'from_tx_block': from_tx_block,
                'to_amount': to_amount,
                'miner_from_address': miner_from_address,
                'miner_to_address': miner_to_address,
                'rate': rate,
            },
        )

    def vote_activate(self, wallet: bt.Wallet, miner_hotkey: str) -> str:
        """Vote to activate a miner. On quorum, miner becomes active."""
        return self._exec_logged(
            'vote_activate',
            wallet,
            f'Vote activate for miner {miner_hotkey}',
            {'miner': miner_hotkey},
        )

    def vote_deactivate(self, wallet: bt.Wallet, miner_hotkey: str) -> str:
        """Vote to deactivate a miner. Validator-quorum only — the contract
        trusts the quorum and applies no collateral/status gate beyond the
        miner currently being active."""
        return self._exec_logged(
            'vote_deactivate',
            wallet,
            f'Vote deactivate for miner {miner_hotkey}',
            {'miner': miner_hotkey},
        )

    def mark_fulfilled(
        self,
        wallet: bt.Wallet,
        swap_id: int,
        to_tx_hash: str,
        to_amount: int,
        to_tx_block: int = 0,
    ) -> str:
        return self._exec_logged(
            'mark_fulfilled',
            wallet,
            f'Swap {swap_id} marked fulfilled',
            {
                'swap_id': swap_id,
                'to_tx_hash': to_tx_hash,
                'to_tx_block': to_tx_block,
                'to_amount': to_amount,
            },
        )

    def confirm_swap(self, wallet: bt.Wallet, swap_id: int) -> str:
        return self._exec_logged('confirm_swap', wallet, f'Swap {swap_id} confirmed', {'swap_id': swap_id})

    def timeout_swap(self, wallet: bt.Wallet, swap_id: int) -> str:
        return self._exec_logged('timeout_swap', wallet, f'Swap {swap_id} timed out', {'swap_id': swap_id})

    def deactivate_miner(self, wallet: bt.Wallet, miner: str) -> str:
        """Deactivate a miner directly on contract (permissionless)."""
        return self._exec_logged('deactivate', wallet, f'Miner {miner} deactivated', {'miner': miner})

    def claim_slash(self, wallet: bt.Wallet, swap_id: int) -> str:
        return self._exec_logged('claim_slash', wallet, f'Slash claimed for swap {swap_id}', {'swap_id': swap_id})

    # =========================================================================
    # Admin Transaction Functions (Write — owner-only)
    # =========================================================================

    def set_fulfillment_timeout(self, wallet: bt.Wallet, blocks: int) -> str:
        return self._exec_logged(
            'set_fulfillment_timeout', wallet, f'Fulfillment timeout set to {blocks}', {'blocks': blocks}
        )

    def set_min_collateral_amount(self, wallet: bt.Wallet, amount_rao: int) -> str:
        return self._exec_logged(
            'set_min_collateral', wallet, f'Min collateral set to {amount_rao}', {'amount': amount_rao}
        )

    def set_max_collateral_amount(self, wallet: bt.Wallet, amount_rao: int) -> str:
        return self._exec_logged(
            'set_max_collateral', wallet, f'Max collateral set to {amount_rao}', {'amount': amount_rao}
        )

    def set_consensus_threshold(self, wallet: bt.Wallet, percent: int) -> str:
        return self._exec_logged(
            'set_consensus_threshold', wallet, f'Consensus threshold set to {percent}%', {'percent': percent}
        )

    def set_min_swap_amount(self, wallet: bt.Wallet, amount_rao: int) -> str:
        return self._exec_logged(
            'set_min_swap_amount', wallet, f'Min swap amount set to {amount_rao}', {'amount': amount_rao}
        )

    def set_reservation_ttl(self, wallet: bt.Wallet, blocks: int) -> str:
        return self._exec_logged('set_reservation_ttl', wallet, f'Reservation TTL set to {blocks}', {'blocks': blocks})

    def set_halted(self, wallet: bt.Wallet, halted: bool) -> str:
        return self._exec_logged('set_halted', wallet, f'System halted set to {halted}', {'halted': halted})

    def set_max_swap_amount(self, wallet: bt.Wallet, amount_rao: int) -> str:
        return self._exec_logged(
            'set_max_swap_amount', wallet, f'Max swap amount set to {amount_rao}', {'amount': amount_rao}
        )

    def add_validator(self, wallet: bt.Wallet, validator: str) -> str:
        return self._exec_logged('add_validator', wallet, f'Validator added {validator}', {'validator': validator})

    def remove_validator(self, wallet: bt.Wallet, validator: str) -> str:
        return self._exec_logged('remove_validator', wallet, f'Validator removed {validator}', {'validator': validator})

    def recycle_fees(self, wallet: bt.Wallet) -> str:
        return self._exec_logged('recycle_fees', wallet, 'Fees recycled')

    def enable_chain_ext(self, wallet: bt.Wallet) -> str:
        self.ensure_initialized()
        tx_hash = self.exec_contract_raw('enable_chain_ext', keypair=wallet.hotkey)
        bt.logging.info(f'Chain extension latched: {tx_hash}')
        return tx_hash

    def transfer_ownership(self, wallet: bt.Wallet, new_owner: str) -> str:
        return self._exec_logged(
            'transfer_ownership', wallet, f'Ownership transferred to {new_owner}', {'new_owner': new_owner}
        )
