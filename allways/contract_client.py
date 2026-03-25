"""Client for interacting with the Allways Swap Manager smart contract.

Uses raw RPC calls to bypass substrate-interface's ContractInstance, which
has SCALE decoding issues with the devnet subtensor runtime. This approach
is proven reliable in gittensor's production contract clients.
"""

import os
import struct
from enum import Enum
from typing import List, Optional, Tuple

import bittensor as bt
from substrateinterface import Keypair
from substrateinterface.exceptions import ExtrinsicNotFound

try:
    from async_substrate_interface.errors import ExtrinsicNotFound as AsyncExtrinsicNotFound
except ImportError:
    AsyncExtrinsicNotFound = ExtrinsicNotFound

from allways.classes import Swap, SwapStatus
from allways.constants import CONTRACT_ADDRESS, MIN_BALANCE_FOR_TX_RAO

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
    'transfer_ownership': bytes.fromhex('107e33ea'),
    'add_validator': bytes.fromhex('82f48fa6'),
    'remove_validator': bytes.fromhex('62135acd'),
    'set_fulfillment_timeout': bytes.fromhex('e9cb777b'),
    'set_min_collateral': bytes.fromhex('b3f48b5e'),
    'set_max_collateral': bytes.fromhex('b7fae7fd'),
    'set_consensus_threshold': bytes.fromhex('c0d8ec47'),
    'set_min_swap_amount': bytes.fromhex('800e1573'),
    'set_max_swap_amount': bytes.fromhex('3e868f32'),
    'set_recycle_address': bytes.fromhex('50dfe685'),
    'set_reservation_ttl': bytes.fromhex('3143d9e3'),
    'set_fee_divisor': bytes.fromhex('8832de41'),
    'recycle_fees': bytes.fromhex('97756ea1'),
    'get_swap': bytes.fromhex('a35f1bbf'),
    'get_collateral': bytes.fromhex('f48343ad'),
    'get_miner_active': bytes.fromhex('25652be8'),
    'get_miner_has_active_swap': bytes.fromhex('1d07dec1'),
    'get_miner_last_resolved_block': bytes.fromhex('a4b68d1f'),
    'is_validator': bytes.fromhex('f844fc5f'),
    'get_next_swap_id': bytes.fromhex('d80244d2'),
    'get_fulfillment_timeout': bytes.fromhex('e820174a'),
    'get_min_collateral': bytes.fromhex('233a7832'),
    'get_max_collateral': bytes.fromhex('54945717'),
    'get_required_votes_count': bytes.fromhex('fe07130d'),
    'get_accumulated_fees': bytes.fromhex('bf3b5d4e'),
    'get_total_recycled_fees': bytes.fromhex('9910e939'),
    'get_owner': bytes.fromhex('07fcd0b1'),
    'get_recycle_address': bytes.fromhex('3847e06c'),
    'get_pending_slash': bytes.fromhex('48c78c4a'),
    'get_min_swap_amount': bytes.fromhex('fca7daa4'),
    'get_max_swap_amount': bytes.fromhex('97826e04'),
    'get_miner_reserved_until': bytes.fromhex('d5ed7150'),
    'get_reservation_ttl': bytes.fromhex('f7e24a31'),
    'get_fee_divisor': bytes.fromhex('41afd8bc'),
    'get_miner_deactivation_block': bytes.fromhex('361acc31'),
    'get_consensus_threshold': bytes.fromhex('2c283460'),
    'get_validator_count': bytes.fromhex('a30ab5c4'),
    'get_activation_vote_count': bytes.fromhex('154595d0'),
    'get_reservation_data': bytes.fromhex('79fe2717'),
    'get_pending_reserve_vote_count': bytes.fromhex('3781315a'),
    'get_cooldown': bytes.fromhex('19a837c6'),
    'vote_extend_reservation': bytes.fromhex('f668d950'),
    'get_extend_vote_count': bytes.fromhex('24fa0aae'),
    'vote_extend_timeout': bytes.fromhex('0fb2d2e5'),
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
        ('user_source_address', 'bytes'),
        ('tao_amount', 'u128'),
        ('source_amount', 'u128'),
        ('dest_amount', 'u128'),
    ],
    'cancel_reservation': [('miner', 'AccountId')],
    'vote_initiate': [
        ('request_hash', 'hash'),
        ('user', 'AccountId'),
        ('miner', 'AccountId'),
        ('source_chain', 'str'),
        ('dest_chain', 'str'),
        ('source_amount', 'u128'),
        ('tao_amount', 'u128'),
        ('user_source_address', 'str'),
        ('user_dest_address', 'str'),
        ('source_tx_hash', 'str'),
        ('source_tx_block', 'u32'),
        ('dest_amount', 'u128'),
        ('miner_source_address', 'str'),
        ('rate', 'str'),
    ],
    'vote_activate': [('miner', 'AccountId')],
    'mark_fulfilled': [('swap_id', 'u64'), ('dest_tx_hash', 'str'), ('dest_tx_block', 'u32'), ('dest_amount', 'u128')],
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
    'set_recycle_address': [('address', 'AccountId')],
    'set_reservation_ttl': [('blocks', 'u32')],
    'set_fee_divisor': [('divisor', 'u128')],
    'recycle_fees': [],
    'get_swap': [('swap_id', 'u64')],
    'get_collateral': [('hotkey', 'AccountId')],
    'get_miner_active': [('hotkey', 'AccountId')],
    'get_miner_has_active_swap': [('hotkey', 'AccountId')],
    'get_miner_last_resolved_block': [('miner', 'AccountId')],
    'is_validator': [('account', 'AccountId')],
    'get_next_swap_id': [],
    'get_fulfillment_timeout': [],
    'get_min_collateral': [],
    'get_max_collateral': [],
    'get_required_votes_count': [],
    'get_miner_deactivation_block': [('miner', 'AccountId')],
    'get_consensus_threshold': [],
    'get_validator_count': [],
    'get_activation_vote_count': [('miner', 'AccountId')],
    'get_reservation_data': [('miner', 'AccountId')],
    'get_pending_reserve_vote_count': [('miner', 'AccountId')],
    'vote_extend_reservation': [
        ('request_hash', 'hash'),
        ('miner', 'AccountId'),
        ('source_tx_hash', 'str'),
    ],
    'get_extend_vote_count': [('miner', 'AccountId')],
    'vote_extend_timeout': [('swap_id', 'u64')],
    'get_cooldown': [('source_address', 'bytes')],
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
    'get_fee_divisor': [],
}

DEFAULT_GAS_LIMIT = {'ref_time': 10_000_000_000, 'proof_size': 500_000}

# Exception types for receipt checking (computed once at import time)
_EXTRINSIC_NOT_FOUND = tuple(t for t in [ExtrinsicNotFound, AsyncExtrinsicNotFound] if t is not None)


def compact_encode_len(length: int) -> bytes:
    """SCALE compact-encode a length prefix. Shared by contract client and axon handlers."""
    if length < 64:
        return bytes([length << 2])
    elif length < 16384:
        return bytes([((length << 2) | 1) & 0xFF, length >> 6])
    else:
        return bytes(
            [
                ((length << 2) | 2) & 0xFF,
                (length >> 6) & 0xFF,
                (length >> 14) & 0xFF,
                (length >> 22) & 0xFF,
            ]
        )


# ContractExecResult byte layout offsets (after gas prefix)
_GAS_PREFIX_BYTES = 16  # Skip gas consumed/required
_RESULT_OK_OFFSET = 10  # Byte indicating Ok(0x00) vs Err in Result
_FLAGS_OFFSET = 11  # 4-byte flags field
_DATA_COMPACT_OFFSET = 15  # Start of compact-encoded data length

# =========================================================================
# Error types
# =========================================================================


class ContractErrorKind(Enum):
    NOT_INITIALIZED = 'not_initialized'
    RPC_FAILURE = 'rpc_failure'
    CALL_FAILED = 'call_failed'
    INSUFFICIENT_BALANCE = 'insufficient_balance'
    CONTRACT_REJECTED = 'contract_rejected'


# Ink! contract error variants — order must match smart-contracts/ink/errors.rs enum
CONTRACT_ERROR_VARIANTS = {
    0: ('NotOwner', 'Caller is not the contract owner'),
    1: ('InsufficientCollateral', 'Insufficient collateral to cover swap volume'),
    2: ('SwapNotFound', 'Swap ID not found'),
    3: ('AlreadyVoted', 'Validator has already voted on this swap'),
    4: ('InvalidStatus', 'Swap is not in the expected status for this operation'),
    5: ('ZeroAmount', 'Amount must be greater than zero'),
    6: ('MinerNotActive', 'Miner is not active'),
    7: ('MinerStillActive', 'Miner is still active (must deactivate before withdrawing)'),
    8: ('TransferFailed', 'Transfer failed'),
    9: ('NotTimedOut', 'Swap has not timed out yet'),
    10: ('NotAssignedMiner', 'Caller is not the assigned miner for this swap'),
    11: ('NotValidator', 'Caller is not a registered validator'),
    12: ('DuplicateSourceTx', 'Source transaction hash already used in another swap'),
    13: ('InvalidAmount', 'Swap amounts must be greater than zero'),
    14: ('NoPendingSlash', 'No pending slash to claim'),
    15: ('InputEmpty', 'Required input is empty'),
    16: ('InputTooLong', 'Input string exceeds maximum allowed length'),
    17: ('MinerHasActiveSwap', 'Miner already has an active swap'),
    18: ('WithdrawalCooldown', 'Withdrawal cooldown not met'),
    19: ('AmountBelowMinimum', 'Swap amount below minimum'),
    20: ('AmountAboveMaximum', 'Swap amount above maximum'),
    21: ('MinerReserved', 'Miner is already reserved by another user'),
    22: ('NoReservation', 'No active reservation for this miner'),
    23: ('ExceedsMaxCollateral', 'Collateral exceeds maximum allowed'),
    24: ('HashMismatch', 'Request hash does not match computed hash'),
    25: ('PendingConflict', 'A pending vote exists for a different request'),
    26: ('SameChain', 'Source and destination chains must be different'),
}


class ContractError(Exception):
    def __init__(self, kind: ContractErrorKind, message: str):
        self.kind = kind
        super().__init__(f'{kind.value}: {message}')


# =========================================================================
# Client
# =========================================================================


def get_contract_address() -> Optional[str]:
    return os.environ.get('CONTRACT_ADDRESS') or CONTRACT_ADDRESS


class AllwaysContractClient:
    """Client for the Allways Swap Manager contract using raw RPC calls."""

    def __init__(
        self,
        contract_address: Optional[str] = None,
        subtensor: Optional[bt.Subtensor] = None,
    ):
        self.contract_address = contract_address or get_contract_address() or ''
        self.subtensor = subtensor
        self._readonly_keypair = Keypair.create_from_uri('//Alice')
        self._initialized = False

        if not self.contract_address:
            bt.logging.warning('Allways contract address not set')

    def _ensure_initialized(self):
        if not self.contract_address:
            raise ContractError(ContractErrorKind.NOT_INITIALIZED, 'contract address not set')
        if not self.subtensor:
            raise ContractError(ContractErrorKind.NOT_INITIALIZED, 'subtensor not available')
        if not self._initialized:
            bt.logging.info(f'Contract client ready for {self.contract_address}')
            self._initialized = True

    # =========================================================================
    # Raw RPC layer
    # =========================================================================

    def _raw_contract_read(
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

            encoded_args = self._encode_args(method, args or {})
            input_data = selector + encoded_args

            kp = caller or self._readonly_keypair
            substrate = self.subtensor.substrate

            origin = bytes.fromhex(substrate.ss58_decode(kp.ss58_address))
            dest = bytes.fromhex(substrate.ss58_decode(self.contract_address))
            value = b'\x00' * 8  # Subtensor Balance is u64
            gas_limit = b'\x00'  # None for dry-run
            storage_limit = b'\x00'  # None for dry-run

            prefix = origin + dest + value + gas_limit + storage_limit
            call_params = prefix + compact_encode_len(len(input_data)) + input_data
            result = substrate.rpc_request('state_call', ['ContractsApi_call', '0x' + call_params.hex()])

            if not result.get('result'):
                return None

            raw = bytes.fromhex(result['result'].replace('0x', ''))
            if len(raw) < 32:
                return None

            # ContractExecResult layout after gas prefix:
            # StorageDeposit(9) + debug_message(1) + Result(1) + flags(4) + data(compact+bytes)
            r = raw[_GAS_PREFIX_BYTES:]
            if len(r) < _DATA_COMPACT_OFFSET or r[_RESULT_OK_OFFSET] != 0x00:
                return None

            flags = struct.unpack_from('<I', r, _FLAGS_OFFSET)[0]
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
                    raise ContractError(
                        ContractErrorKind.CONTRACT_REJECTED, f'{method}: contract rejected (no details)'
                    )
                return None

            # REVERT flag means the contract returned Err — decode the error variant.
            # Data layout: [LangError discriminant] [Result discriminant] [Error variant byte]
            if is_revert:
                raise self._decode_contract_error(method, r, data_start, data_len)

            # First byte is ink! LangError discriminant (0x00 = Ok)
            if r[data_start] != 0x00:
                return None

            return r[data_start + 1 : data_start + data_len]

        except ContractError:
            raise
        except Exception as e:
            bt.logging.debug(f'Raw contract read {method} failed: {e}')
            return None

    @staticmethod
    def _decode_contract_error(method: str, r: bytes, data_start: int, data_len: int) -> ContractError:
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
                return ContractError(ContractErrorKind.CONTRACT_REJECTED, f'{method}: {name} — {description}')
            return ContractError(
                ContractErrorKind.CONTRACT_REJECTED, f'{method}: unknown error variant ({variant_idx})'
            )
        return ContractError(ContractErrorKind.CONTRACT_REJECTED, f'{method}: contract rejected')

    def _exec_contract_raw(
        self,
        method: str,
        args: Optional[dict] = None,
        keypair: Optional[Keypair] = None,
        value: int = 0,
        gas_limit: dict = None,
    ) -> str:
        """Execute a contract method via raw extrinsic submission. Returns tx hash."""
        gas_limit = gas_limit or DEFAULT_GAS_LIMIT
        selector = CONTRACT_SELECTORS.get(method)
        if not selector:
            raise ContractError(ContractErrorKind.CALL_FAILED, f'{method}: unknown method')

        encoded_args = self._encode_args(method, args or {})
        call_data = selector + encoded_args

        substrate = self.subtensor.substrate

        signer_address = keypair.ss58_address
        try:
            account_info = substrate.query('System', 'Account', [signer_address])
        except Exception as e:
            raise ContractError(ContractErrorKind.RPC_FAILURE, f'{method}: balance query failed: {e}') from e

        account_data = account_info.value if hasattr(account_info, 'value') else account_info
        free_balance = account_data.get('data', {}).get('free', 0)
        if free_balance < MIN_BALANCE_FOR_TX_RAO:
            raise ContractError(ContractErrorKind.INSUFFICIENT_BALANCE, f'{method}: free={free_balance}')

        try:
            call = substrate.compose_call(
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
            extrinsic = substrate.create_signed_extrinsic(call=call, keypair=keypair)
            receipt = substrate.submit_extrinsic(extrinsic, wait_for_inclusion=True, wait_for_finalization=False)
        except Exception as e:
            raise ContractError(ContractErrorKind.RPC_FAILURE, f'{method}: exec failed: {e}') from e

        try:
            if receipt.is_success:
                return receipt.extrinsic_hash
            else:
                raise ContractError(ContractErrorKind.CALL_FAILED, f'{method}: {receipt.error_message}')
        except _EXTRINSIC_NOT_FOUND:
            return receipt.extrinsic_hash

    # =========================================================================
    # SCALE encoding / decoding helpers
    # =========================================================================

    def _encode_args(self, method: str, args: dict) -> bytes:
        arg_types = CONTRACT_ARG_TYPES.get(method, [])
        encoded = b''
        for arg_name, type_tag in arg_types:
            if arg_name not in args:
                raise ValueError(f'Missing argument: {arg_name}')
            v = args[arg_name]
            encoded += self._encode_value(v, type_tag)
        return encoded

    def _encode_value(self, value, type_tag: str) -> bytes:
        if type_tag == 'u8':
            return struct.pack('B', int(value))
        elif type_tag == 'hash':
            if isinstance(value, str):
                return bytes.fromhex(value.replace('0x', ''))
            return bytes(value)[:32].ljust(32, b'\x00')
        elif type_tag == 'bytes':
            data = value if isinstance(value, (bytes, bytearray)) else value.encode('utf-8')
            return self._compact_encode_len(len(data)) + data
        elif type_tag == 'u32':
            return struct.pack('<I', int(value))
        elif type_tag == 'u64':
            return struct.pack('<Q', int(value))
        elif type_tag == 'u128':
            v = int(value)
            return struct.pack('<QQ', v & 0xFFFFFFFFFFFFFFFF, v >> 64)
        elif type_tag == 'bool':
            return b'\x01' if value else b'\x00'
        elif type_tag == 'AccountId':
            if isinstance(value, str):
                return bytes.fromhex(self.subtensor.substrate.ss58_decode(value))
            return bytes(value)
        elif type_tag == 'str':
            data = value.encode('utf-8') if isinstance(value, str) else value
            return self._compact_encode_len(len(data)) + data
        elif type_tag == 'vec_u64':
            items = list(value)
            encoded = self._compact_encode_len(len(items))
            for item in items:
                encoded += struct.pack('<Q', int(item))
            return encoded
        raise ValueError(f'Unsupported type: {type_tag}')

    _compact_encode_len = staticmethod(compact_encode_len)

    def _extract_u32(self, data: bytes) -> Optional[int]:
        if not data or len(data) < 4:
            return None
        return struct.unpack_from('<I', data, 0)[0]

    def _extract_u64(self, data: bytes) -> Optional[int]:
        if not data or len(data) < 8:
            return None
        return struct.unpack_from('<Q', data, 0)[0]

    def _extract_u128(self, data: bytes) -> Optional[int]:
        if not data or len(data) < 16:
            return None
        low = struct.unpack_from('<Q', data, 0)[0]
        high = struct.unpack_from('<Q', data, 8)[0]
        return low + (high << 64)

    def _extract_bool(self, data: bytes) -> Optional[bool]:
        if not data:
            return None
        return data[0] != 0

    def _extract_account_id(self, data: bytes) -> Optional[str]:
        if not data or len(data) < 32:
            return None
        return self.subtensor.substrate.ss58_encode(data[:32].hex())

    def _decode_string(self, data: bytes, offset: int) -> Tuple[str, int]:
        """Decode a SCALE compact-prefixed string. Returns (string, new_offset)."""
        if offset >= len(data):
            return '', offset
        first = data[offset]
        mode = first & 0x03
        if mode == 0:
            str_len = first >> 2
            offset += 1
        elif mode == 1:
            if offset + 1 >= len(data):
                return '', offset
            str_len = (data[offset] | (data[offset + 1] << 8)) >> 2
            offset += 2
        else:
            if offset + 3 >= len(data):
                return '', offset
            str_len = (
                data[offset] | (data[offset + 1] << 8) | (data[offset + 2] << 16) | (data[offset + 3] << 24)
            ) >> 2
            offset += 4
        if offset + str_len > len(data):
            return '', offset
        s = data[offset : offset + str_len].decode('utf-8', errors='replace')
        return s, offset + str_len

    def _decode_swap_data(self, data: bytes, offset: int = 0) -> Optional[Swap]:
        """Decode a SwapData struct from raw SCALE bytes."""
        try:
            o = offset

            swap_id = struct.unpack_from('<Q', data, o)[0]
            o += 8
            user = self.subtensor.substrate.ss58_encode(data[o : o + 32].hex())
            o += 32
            miner = self.subtensor.substrate.ss58_encode(data[o : o + 32].hex())
            o += 32
            source_chain, o = self._decode_string(data, o)
            dest_chain, o = self._decode_string(data, o)
            source_amount_lo = struct.unpack_from('<Q', data, o)[0]
            o += 8
            source_amount_hi = struct.unpack_from('<Q', data, o)[0]
            o += 8
            source_amount = source_amount_lo + (source_amount_hi << 64)
            dest_amount_lo = struct.unpack_from('<Q', data, o)[0]
            o += 8
            dest_amount_hi = struct.unpack_from('<Q', data, o)[0]
            o += 8
            dest_amount = dest_amount_lo + (dest_amount_hi << 64)
            tao_amount_lo = struct.unpack_from('<Q', data, o)[0]
            o += 8
            tao_amount_hi = struct.unpack_from('<Q', data, o)[0]
            o += 8
            tao_amount = tao_amount_lo + (tao_amount_hi << 64)
            user_source_address, o = self._decode_string(data, o)
            user_dest_address, o = self._decode_string(data, o)
            miner_source_address, o = self._decode_string(data, o)
            rate, o = self._decode_string(data, o)
            source_tx_hash, o = self._decode_string(data, o)
            source_tx_block = struct.unpack_from('<I', data, o)[0]
            o += 4
            dest_tx_hash, o = self._decode_string(data, o)
            dest_tx_block = struct.unpack_from('<I', data, o)[0]
            o += 4
            status_byte = data[o]
            o += 1
            status = SwapStatus(status_byte) if status_byte <= 3 else SwapStatus.ACTIVE
            initiated_block = struct.unpack_from('<I', data, o)[0]
            o += 4
            timeout_block = struct.unpack_from('<I', data, o)[0]
            o += 4
            fulfilled_block = struct.unpack_from('<I', data, o)[0]
            o += 4
            completed_block = struct.unpack_from('<I', data, o)[0]
            o += 4

            return Swap(
                id=swap_id,
                user_hotkey=user,
                miner_hotkey=miner,
                source_chain=source_chain,
                dest_chain=dest_chain,
                source_amount=source_amount,
                dest_amount=dest_amount,
                tao_amount=tao_amount,
                user_source_address=user_source_address,
                user_dest_address=user_dest_address,
                miner_source_address=miner_source_address,
                rate=rate,
                source_tx_hash=source_tx_hash,
                source_tx_block=source_tx_block,
                dest_tx_hash=dest_tx_hash,
                dest_tx_block=dest_tx_block,
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

    def _read_u32(self, method: str, args: dict = None) -> int:
        self._ensure_initialized()
        data = self._raw_contract_read(method, args)
        if data is None:
            raise ContractError(ContractErrorKind.RPC_FAILURE, f'{method}: no response')
        v = self._extract_u32(data)
        return v if v is not None else 0

    def _read_u64(self, method: str, args: dict = None) -> int:
        self._ensure_initialized()
        data = self._raw_contract_read(method, args)
        if data is None:
            raise ContractError(ContractErrorKind.RPC_FAILURE, f'{method}: no response')
        v = self._extract_u64(data)
        return v if v is not None else 0

    def _read_u128(self, method: str, args: dict = None) -> int:
        self._ensure_initialized()
        data = self._raw_contract_read(method, args)
        if data is None:
            raise ContractError(ContractErrorKind.RPC_FAILURE, f'{method}: no response')
        v = self._extract_u128(data)
        return v if v is not None else 0

    def _read_bool(self, method: str, args: dict = None) -> bool:
        self._ensure_initialized()
        data = self._raw_contract_read(method, args)
        if data is None:
            raise ContractError(ContractErrorKind.RPC_FAILURE, f'{method}: no response')
        v = self._extract_bool(data)
        return v if v is not None else False

    def _read_account_id(self, method: str, args: dict = None) -> str:
        self._ensure_initialized()
        data = self._raw_contract_read(method, args)
        if data is None:
            raise ContractError(ContractErrorKind.RPC_FAILURE, f'{method}: no response')
        v = self._extract_account_id(data)
        return v if v is not None else ''

    def _read_option_swap(self, method: str, args: dict = None, caller=None) -> Optional[Swap]:
        """Read a method that returns Option<SwapData>."""
        self._ensure_initialized()
        data = self._raw_contract_read(method, args, caller=caller)
        if data is None or len(data) < 1:
            return None
        # Option discriminant: 0x00 = None, 0x01 = Some
        if data[0] == 0x00:
            return None
        if data[0] == 0x01:
            return self._decode_swap_data(data, offset=1)
        return None

    def _read_result_option_swap(self, method: str, args: dict = None, caller=None) -> Optional[Swap]:
        """Read a method that returns Result<Option<SwapData>, ContractError>."""
        self._ensure_initialized()
        data = self._raw_contract_read(method, args, caller=caller)
        if data is None or len(data) < 1:
            return None
        # Result discriminant: 0x00 = Ok, 0x01 = Err
        if data[0] != 0x00:
            if len(data) >= 2:
                variant = CONTRACT_ERROR_VARIANTS.get(data[1])
                if variant:
                    bt.logging.debug(f'{method}: contract error — {variant[0]}')
            return None
        if len(data) < 2:
            return None
        # Option discriminant
        if data[1] == 0x00:
            return None
        if data[1] == 0x01:
            return self._decode_swap_data(data, offset=2)
        return None

    def _read_result_u128(self, method: str, args: dict = None, caller=None) -> int:
        """Read a method that returns Result<u128, ContractError>."""
        self._ensure_initialized()
        data = self._raw_contract_read(method, args, caller=caller)
        if data is None or len(data) < 1:
            raise ContractError(ContractErrorKind.RPC_FAILURE, f'{method}: no response')
        if data[0] != 0x00:
            if len(data) >= 2:
                variant = CONTRACT_ERROR_VARIANTS.get(data[1])
                if variant:
                    name, description = variant
                    raise ContractError(ContractErrorKind.CONTRACT_REJECTED, f'{method}: {name} — {description}')
            raise ContractError(ContractErrorKind.CONTRACT_REJECTED, f'{method}: contract rejected')
        v = self._extract_u128(data[1:])
        return v if v is not None else 0

    # =========================================================================
    # Query Functions (Read-only)
    # =========================================================================

    def get_swap(self, swap_id: int) -> Optional[Swap]:
        """Get an active/fulfilled swap by ID. Returns None if not found or already resolved."""
        return self._read_option_swap('get_swap', {'swap_id': swap_id})

    def get_active_swaps(self, max_gap: int = 50) -> List[Swap]:
        """Scan backward from latest swap ID, returning all ACTIVE/FULFILLED swaps.

        Stops after max_gap consecutive None results (pruned/resolved gaps).
        """
        self._ensure_initialized()
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
        return self._read_u128('get_collateral', {'hotkey': hotkey})

    def get_fulfillment_timeout(self) -> int:
        return self._read_u32('get_fulfillment_timeout')

    def get_miner_active_flag(self, hotkey: str) -> bool:
        return self._read_bool('get_miner_active', {'hotkey': hotkey})

    def get_miner_has_active_swap(self, hotkey: str) -> bool:
        return self._read_bool('get_miner_has_active_swap', {'hotkey': hotkey})

    def get_miner_last_resolved_block(self, hotkey: str) -> int:
        return self._read_u32('get_miner_last_resolved_block', {'miner': hotkey})

    def get_next_swap_id(self) -> int:
        return self._read_u64('get_next_swap_id')

    def get_pending_slash(self, swap_id: int) -> int:
        return self._read_u128('get_pending_slash', {'swap_id': swap_id})

    def get_min_collateral(self) -> int:
        return self._read_u128('get_min_collateral')

    def get_max_collateral(self) -> int:
        return self._read_u128('get_max_collateral')

    def get_required_votes_count(self) -> int:
        return self._read_u32('get_required_votes_count')

    def get_miner_deactivation_block(self, hotkey: str) -> int:
        return self._read_u32('get_miner_deactivation_block', {'miner': hotkey})

    def get_consensus_threshold(self) -> int:
        self._ensure_initialized()
        data = self._raw_contract_read('get_consensus_threshold')
        if data is None or len(data) < 1:
            return 0
        return data[0]

    def get_validator_count(self) -> int:
        return self._read_u32('get_validator_count')

    def get_activation_vote_count(self, hotkey: str) -> int:
        return self._read_u32('get_activation_vote_count', {'miner': hotkey})

    def get_pending_reserve_vote_count(self, miner_hotkey: str) -> int:
        return self._read_u32('get_pending_reserve_vote_count', {'miner': miner_hotkey})

    def get_extend_vote_count(self, miner_hotkey: str) -> int:
        return self._read_u32('get_extend_vote_count', {'miner': miner_hotkey})

    def get_cooldown(self, source_address: bytes) -> Tuple[int, int]:
        """Returns (strike_count, last_expired_block) for a source address."""
        self._ensure_initialized()
        data = self._raw_contract_read('get_cooldown', {'source_address': source_address})
        if data is None or len(data) < 5:
            return (0, 0)
        strike_count = data[0]
        last_expired = struct.unpack_from('<I', data, 1)[0]
        return (strike_count, last_expired)

    def get_accumulated_fees(self) -> int:
        return self._read_u128('get_accumulated_fees')

    def get_total_recycled_fees(self) -> int:
        return self._read_u128('get_total_recycled_fees')

    def get_min_swap_amount(self) -> int:
        return self._read_u128('get_min_swap_amount')

    def get_max_swap_amount(self) -> int:
        return self._read_u128('get_max_swap_amount')

    def get_owner(self) -> str:
        return self._read_account_id('get_owner')

    def get_halted(self) -> bool:
        return self._read_bool('get_halted')

    def get_recycle_address(self) -> str:
        return self._read_account_id('get_recycle_address')

    def is_validator(self, account: str) -> bool:
        return self._read_bool('is_validator', {'account': account})

    def get_miner_reserved_until(self, miner_hotkey: str) -> int:
        return self._read_u32('get_miner_reserved_until', {'miner': miner_hotkey})

    def get_reservation_ttl(self) -> int:
        return self._read_u32('get_reservation_ttl')

    def get_fee_divisor(self) -> int:
        return self._read_u128('get_fee_divisor')

    def get_reservation_data(self, miner_hotkey: str) -> Optional[Tuple[bytes, int, int, int, int]]:
        """Get reservation data for a miner.

        Returns (source_addr, tao_amount, source_amount, dest_amount, reserved_until) or None.
        """
        self._ensure_initialized()
        data = self._raw_contract_read('get_reservation_data', {'miner': miner_hotkey})
        if data is None or len(data) < 1:
            return None
        # Option discriminant: 0x00 = None, 0x01 = Some
        if data[0] == 0x00:
            return None
        if data[0] != 0x01:
            return None
        o = 1
        # Vec<u8> source_addr: compact length + bytes
        first = data[o]
        mode = first & 0x03
        if mode == 0:
            addr_len = first >> 2
            o += 1
        elif mode == 1:
            addr_len = (data[o] | (data[o + 1] << 8)) >> 2
            o += 2
        else:
            return None
        source_addr = data[o : o + addr_len]
        o += addr_len
        # 3 x u128 + 1 x u32
        tao_lo = struct.unpack_from('<Q', data, o)[0]
        tao_hi = struct.unpack_from('<Q', data, o + 8)[0]
        tao_amount = tao_lo + (tao_hi << 64)
        o += 16
        src_lo = struct.unpack_from('<Q', data, o)[0]
        src_hi = struct.unpack_from('<Q', data, o + 8)[0]
        source_amount = src_lo + (src_hi << 64)
        o += 16
        dst_lo = struct.unpack_from('<Q', data, o)[0]
        dst_hi = struct.unpack_from('<Q', data, o + 8)[0]
        dest_amount = dst_lo + (dst_hi << 64)
        o += 16
        reserved_until = struct.unpack_from('<I', data, o)[0]
        return (source_addr, tao_amount, source_amount, dest_amount, reserved_until)

    # =========================================================================
    # Transaction Functions (Write)
    # =========================================================================

    def post_collateral(self, wallet: bt.Wallet, amount_rao: int) -> str:
        """Post collateral to the contract. Amount is sent as value with the extrinsic."""
        self._ensure_initialized()
        tx_hash = self._exec_contract_raw('post_collateral', keypair=wallet.hotkey, value=amount_rao)
        bt.logging.info(f'Collateral posted: {tx_hash}')
        return tx_hash

    def withdraw_collateral(self, wallet: bt.Wallet, amount_rao: int) -> str:
        self._ensure_initialized()
        tx_hash = self._exec_contract_raw('withdraw_collateral', args={'amount': amount_rao}, keypair=wallet.hotkey)
        bt.logging.info(f'Collateral withdrawn: {tx_hash}')
        return tx_hash

    def vote_reserve(
        self,
        wallet: bt.Wallet,
        request_hash: bytes,
        miner_hotkey: str,
        user_source_address: bytes,
        tao_amount: int,
        source_amount: int,
        dest_amount: int,
    ) -> str:
        self._ensure_initialized()
        tx_hash = self._exec_contract_raw(
            'vote_reserve',
            args={
                'request_hash': request_hash,
                'miner': miner_hotkey,
                'user_source_address': user_source_address,
                'tao_amount': tao_amount,
                'source_amount': source_amount,
                'dest_amount': dest_amount,
            },
            keypair=wallet.hotkey,
        )
        bt.logging.info(f'Vote reserve for miner {miner_hotkey}: {tx_hash}')
        return tx_hash

    def vote_extend_reservation(
        self,
        wallet: bt.Wallet,
        request_hash: bytes,
        miner_hotkey: str,
        source_tx_hash: str,
    ) -> str:
        self._ensure_initialized()
        tx_hash = self._exec_contract_raw(
            'vote_extend_reservation',
            args={
                'request_hash': request_hash,
                'miner': miner_hotkey,
                'source_tx_hash': source_tx_hash,
            },
            keypair=wallet.hotkey,
        )
        bt.logging.info(f'Vote extend reservation for miner {miner_hotkey}: {tx_hash}')
        return tx_hash

    def vote_extend_timeout(self, wallet: bt.Wallet, swap_id: int) -> str:
        self._ensure_initialized()
        tx_hash = self._exec_contract_raw(
            'vote_extend_timeout',
            args={'swap_id': swap_id},
            keypair=wallet.hotkey,
        )
        bt.logging.info(f'Vote extend timeout for swap {swap_id}: {tx_hash}')
        return tx_hash

    def cancel_reservation(self, wallet: bt.Wallet, miner_hotkey: str) -> str:
        self._ensure_initialized()
        tx_hash = self._exec_contract_raw('cancel_reservation', args={'miner': miner_hotkey}, keypair=wallet.hotkey)
        bt.logging.info(f'Reservation cancelled for {miner_hotkey}: {tx_hash}')
        return tx_hash

    def vote_initiate(
        self,
        wallet: bt.Wallet,
        request_hash: bytes,
        user_hotkey: str,
        miner_hotkey: str,
        source_chain: str,
        dest_chain: str,
        source_amount: int,
        tao_amount: int,
        user_source_address: str,
        user_dest_address: str,
        source_tx_hash: str,
        source_tx_block: int = 0,
        dest_amount: int = 0,
        miner_source_address: str = '',
        rate: str = '',
    ) -> str:
        """Vote to initiate a swap. On quorum, swap is created on contract."""
        self._ensure_initialized()
        tx_hash = self._exec_contract_raw(
            'vote_initiate',
            args={
                'request_hash': request_hash,
                'user': user_hotkey,
                'miner': miner_hotkey,
                'source_chain': source_chain,
                'dest_chain': dest_chain,
                'source_amount': source_amount,
                'tao_amount': tao_amount,
                'user_source_address': user_source_address,
                'user_dest_address': user_dest_address,
                'source_tx_hash': source_tx_hash,
                'source_tx_block': source_tx_block,
                'dest_amount': dest_amount,
                'miner_source_address': miner_source_address,
                'rate': rate,
            },
            keypair=wallet.hotkey,
        )
        bt.logging.info(f'Vote initiate for miner {miner_hotkey}: {tx_hash}')
        return tx_hash

    def vote_activate(self, wallet: bt.Wallet, miner_hotkey: str) -> str:
        """Vote to activate a miner. On quorum, miner becomes active."""
        self._ensure_initialized()
        tx_hash = self._exec_contract_raw('vote_activate', args={'miner': miner_hotkey}, keypair=wallet.hotkey)
        bt.logging.info(f'Vote activate for miner {miner_hotkey}: {tx_hash}')
        return tx_hash

    def mark_fulfilled(
        self,
        wallet: bt.Wallet,
        swap_id: int,
        dest_tx_hash: str,
        dest_amount: int,
        dest_tx_block: int = 0,
    ) -> str:
        self._ensure_initialized()
        tx_hash = self._exec_contract_raw(
            'mark_fulfilled',
            args={
                'swap_id': swap_id,
                'dest_tx_hash': dest_tx_hash,
                'dest_tx_block': dest_tx_block,
                'dest_amount': dest_amount,
            },
            keypair=wallet.hotkey,
        )
        bt.logging.info(f'Swap {swap_id} marked fulfilled: {tx_hash}')
        return tx_hash

    def confirm_swap(self, wallet: bt.Wallet, swap_id: int) -> str:
        self._ensure_initialized()
        tx_hash = self._exec_contract_raw('confirm_swap', args={'swap_id': swap_id}, keypair=wallet.hotkey)
        bt.logging.info(f'Swap {swap_id} confirmed: {tx_hash}')
        return tx_hash

    def timeout_swap(self, wallet: bt.Wallet, swap_id: int) -> str:
        self._ensure_initialized()
        tx_hash = self._exec_contract_raw('timeout_swap', args={'swap_id': swap_id}, keypair=wallet.hotkey)
        bt.logging.info(f'Swap {swap_id} timed out: {tx_hash}')
        return tx_hash

    def deactivate_miner(self, wallet: bt.Wallet, miner: str) -> str:
        """Deactivate a miner directly on contract (permissionless)."""
        self._ensure_initialized()
        tx_hash = self._exec_contract_raw('deactivate', args={'miner': miner}, keypair=wallet.hotkey)
        bt.logging.info(f'Miner {miner} deactivated: {tx_hash}')
        return tx_hash

    def claim_slash(self, wallet: bt.Wallet, swap_id: int) -> str:
        self._ensure_initialized()
        tx_hash = self._exec_contract_raw('claim_slash', args={'swap_id': swap_id}, keypair=wallet.hotkey)
        bt.logging.info(f'Slash claimed for swap {swap_id}: {tx_hash}')
        return tx_hash

    # =========================================================================
    # Admin Transaction Functions (Write — owner-only)
    # =========================================================================

    def set_fulfillment_timeout(self, wallet: bt.Wallet, blocks: int) -> str:
        self._ensure_initialized()
        tx_hash = self._exec_contract_raw('set_fulfillment_timeout', args={'blocks': blocks}, keypair=wallet.hotkey)
        bt.logging.info(f'Fulfillment timeout set to {blocks}: {tx_hash}')
        return tx_hash

    def set_min_collateral_amount(self, wallet: bt.Wallet, amount_rao: int) -> str:
        self._ensure_initialized()
        tx_hash = self._exec_contract_raw('set_min_collateral', args={'amount': amount_rao}, keypair=wallet.hotkey)
        bt.logging.info(f'Min collateral set to {amount_rao}: {tx_hash}')
        return tx_hash

    def set_max_collateral_amount(self, wallet: bt.Wallet, amount_rao: int) -> str:
        self._ensure_initialized()
        tx_hash = self._exec_contract_raw('set_max_collateral', args={'amount': amount_rao}, keypair=wallet.hotkey)
        bt.logging.info(f'Max collateral set to {amount_rao}: {tx_hash}')
        return tx_hash

    def set_consensus_threshold(self, wallet: bt.Wallet, percent: int) -> str:
        self._ensure_initialized()
        tx_hash = self._exec_contract_raw('set_consensus_threshold', args={'percent': percent}, keypair=wallet.hotkey)
        bt.logging.info(f'Consensus threshold set to {percent}%: {tx_hash}')
        return tx_hash

    def set_min_swap_amount(self, wallet: bt.Wallet, amount_rao: int) -> str:
        self._ensure_initialized()
        tx_hash = self._exec_contract_raw('set_min_swap_amount', args={'amount': amount_rao}, keypair=wallet.hotkey)
        bt.logging.info(f'Min swap amount set to {amount_rao}: {tx_hash}')
        return tx_hash

    def set_recycle_address(self, wallet: bt.Wallet, address: str) -> str:
        self._ensure_initialized()
        tx_hash = self._exec_contract_raw('set_recycle_address', args={'address': address}, keypair=wallet.hotkey)
        bt.logging.info(f'Recycle address set to {address}: {tx_hash}')
        return tx_hash

    def set_reservation_ttl(self, wallet: bt.Wallet, blocks: int) -> str:
        self._ensure_initialized()
        tx_hash = self._exec_contract_raw('set_reservation_ttl', args={'blocks': blocks}, keypair=wallet.hotkey)
        bt.logging.info(f'Reservation TTL set to {blocks}: {tx_hash}')
        return tx_hash

    def set_fee_divisor(self, wallet: bt.Wallet, divisor: int) -> str:
        self._ensure_initialized()
        tx_hash = self._exec_contract_raw('set_fee_divisor', args={'divisor': divisor}, keypair=wallet.hotkey)
        bt.logging.info(f'Fee divisor set to {divisor}: {tx_hash}')
        return tx_hash

    def set_halted(self, wallet: bt.Wallet, halted: bool) -> str:
        self._ensure_initialized()
        tx_hash = self._exec_contract_raw('set_halted', args={'halted': halted}, keypair=wallet.hotkey)
        bt.logging.info(f'System halted set to {halted}: {tx_hash}')
        return tx_hash

    def set_max_swap_amount(self, wallet: bt.Wallet, amount_rao: int) -> str:
        self._ensure_initialized()
        tx_hash = self._exec_contract_raw('set_max_swap_amount', args={'amount': amount_rao}, keypair=wallet.hotkey)
        bt.logging.info(f'Max swap amount set to {amount_rao}: {tx_hash}')
        return tx_hash

    def add_validator(self, wallet: bt.Wallet, validator: str) -> str:
        self._ensure_initialized()
        tx_hash = self._exec_contract_raw('add_validator', args={'validator': validator}, keypair=wallet.hotkey)
        bt.logging.info(f'Validator added {validator}: {tx_hash}')
        return tx_hash

    def remove_validator(self, wallet: bt.Wallet, validator: str) -> str:
        self._ensure_initialized()
        tx_hash = self._exec_contract_raw('remove_validator', args={'validator': validator}, keypair=wallet.hotkey)
        bt.logging.info(f'Validator removed {validator}: {tx_hash}')
        return tx_hash

    def recycle_fees(self, wallet: bt.Wallet) -> str:
        self._ensure_initialized()
        tx_hash = self._exec_contract_raw('recycle_fees', keypair=wallet.hotkey)
        bt.logging.info(f'Fees recycled: {tx_hash}')
        return tx_hash

    def transfer_ownership(self, wallet: bt.Wallet, new_owner: str) -> str:
        self._ensure_initialized()
        tx_hash = self._exec_contract_raw('transfer_ownership', args={'new_owner': new_owner}, keypair=wallet.hotkey)
        bt.logging.info(f'Ownership transferred to {new_owner}: {tx_hash}')
        return tx_hash
