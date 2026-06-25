"""Borsh account/nested-type layouts + discriminators for allways_swap_manager.

Hand-written to mirror smart-contracts/solana/.../state.rs EXACTLY (field order matters — a mismatch
silently mis-decodes). `rate` is u128 fixed-point (#495), not a String. Discriminators copied verbatim
from target/idl/allways_swap_manager.json (no sha256). Account data = 8-byte discriminator + borsh(fields);
these CStructs cover the post-discriminator body only.

Fixed byte arrays ([u8;32]/[u8;64], incl. pubkeys) decode to Python `bytes` via construct.Bytes; the
client converts pubkey fields to solders Pubkey when mapping to dataclasses.
"""

from borsh_construct import I64, U8, U32, U64, U128, Bool, CStruct, Enum, String, Vec
from construct import Bytes as _Raw

Pubkey32 = _Raw(32)
Hash32 = _Raw(32)
Sig64 = _Raw(64)

# 8-byte account discriminators (IDL `accounts[].discriminator`).
DISCRIMINATORS = {
    'Binding': bytes([148, 194, 179, 54, 81, 210, 85, 178]),
    'CollateralVault': bytes([19, 189, 95, 155, 100, 9, 159, 145]),
    'Config': bytes([155, 12, 170, 224, 30, 250, 204, 130]),
    'HotkeyBinding': bytes([225, 14, 96, 246, 60, 210, 97, 210]),
    'MinerDirectionStats': bytes([109, 39, 104, 238, 244, 223, 118, 88]),
    'MinerQuote': bytes([205, 197, 49, 135, 73, 161, 154, 230]),
    'MinerState': bytes([171, 93, 72, 78, 139, 153, 97, 8]),
    'Pool': bytes([241, 154, 109, 4, 17, 177, 109, 188]),
    'Reservation': bytes([188, 235, 0, 111, 208, 253, 247, 212]),
    'Swap': bytes([53, 206, 146, 152, 44, 97, 120, 177]),
    'Treasury': bytes([238, 239, 123, 238, 89, 1, 168, 253]),
    'VoteRound': bytes([155, 210, 10, 166, 226, 108, 60, 182]),
}

# --- nested types ---
ValidatorInfo = CStruct('key' / Pubkey32, 'weight' / U64)

Request = CStruct(
    'router' / Pubkey32,
    'user' / Pubkey32,
    'user_from_addr' / String,
    'user_to_addr' / String,
    'sol_amount' / U64,
    'from_amount' / U128,
    'to_amount' / U128,
)

SwapStatus = Enum('Active', 'Fulfilled', 'PendingAttestation', enum_name='SwapStatus')

# --- accounts (field order locked to state.rs / IDL) ---
Binding = CStruct(
    'miner' / Pubkey32,
    'hotkey' / Hash32,
    'hotkey_sig' / Sig64,
    'bound_at' / I64,
    'bump' / U8,
)

HotkeyBinding = CStruct('miner' / Pubkey32, 'bump' / U8)

CollateralVault = CStruct('bump' / U8)

Treasury = CStruct('total' / U64, 'bump' / U8)

MinerState = CStruct(
    'miner' / Pubkey32,
    'collateral' / U64,
    'active' / Bool,
    'has_active_swap' / Bool,
    'busy_until' / I64,
    'deactivation_at' / I64,
    'successful_swaps' / U32,
    'failed_swaps' / U32,
    'bump' / U8,
)

MinerDirectionStats = CStruct(
    'miner' / Pubkey32,
    'from_chain' / String,
    'to_chain' / String,
    'completed' / U32,
    'total_from_amount' / U128,
    'total_to_amount' / U128,
    'bump' / U8,
)

MinerQuote = CStruct(
    'miner' / Pubkey32,
    'from_chain' / String,
    'to_chain' / String,
    'miner_from_addr' / String,
    'miner_to_addr' / String,
    'rate' / U128,
    'liquidity' / U128,
    'updated_at' / I64,
    'bump' / U8,
)

Reservation = CStruct(
    'from_addr' / String,
    'user' / Pubkey32,
    'user_to_addr' / String,
    'from_chain' / String,
    'to_chain' / String,
    'sol_amount' / U64,
    'from_amount' / U128,
    'to_amount' / U128,
    'miner_from_addr' / String,
    'miner_to_addr' / String,
    'rate' / U128,
    'created_at' / I64,
    'reserved_until' / I64,
    'max_extend_at' / I64,
    'claimed_swap_key' / Hash32,
    'bump' / U8,
)

Swap = CStruct(
    'user' / Pubkey32,
    'miner' / Pubkey32,
    'from_chain' / String,
    'to_chain' / String,
    'user_from_addr' / String,
    'user_to_addr' / String,
    'miner_from_addr' / String,
    'miner_to_addr' / String,
    'rate' / U128,
    'sol_amount' / U64,
    'from_amount' / U128,
    'to_amount' / U128,
    'from_tx_hash' / String,
    'from_tx_block' / U32,
    'to_tx_hash' / String,
    'to_tx_block' / U32,
    'status' / SwapStatus,
    'initiated_at' / I64,
    'timeout_at' / I64,
    'max_extend_at' / I64,
    'fulfilled_at' / I64,
    'bump' / U8,
)

Pool = CStruct(
    'miner' / Pubkey32,
    'from_chain' / String,
    'to_chain' / String,
    'miner_from_addr' / String,
    'miner_to_addr' / String,
    'rate' / U128,
    'opened_at' / I64,
    'closes_at' / I64,
    'seed_slot' / U64,
    'requests' / Vec(Request),
    'bump' / U8,
)

VoteRound = CStruct(
    'bound_hash' / Hash32,
    'voters' / Vec(Pubkey32),
    'created_at' / I64,
    'bump' / U8,
)

Config = CStruct(
    'admin' / Pubkey32,
    'version' / U32,
    'min_collateral' / U64,
    'max_collateral' / U64,
    'fulfillment_timeout_secs' / I64,
    'min_swap_amount' / U64,
    'max_swap_amount' / U64,
    'reservation_ttl_secs' / I64,
    'consensus_threshold_percent' / U8,
    'validators' / Vec(ValidatorInfo),
    'last_weights_update' / I64,
    'halted' / Bool,
    'reservation_fee_lamports' / U64,
    'pool_window_secs' / I64,
    'weights_update_min_interval_secs' / I64,
    'max_total_extension_secs' / I64,
    'bump' / U8,
)

# Top-level pubkey fields per account (decoded bytes -> solders Pubkey by the client). Hash/id byte
# fields (hotkey, hotkey_sig, claimed_swap_key, bound_hash) intentionally stay raw bytes.
ACCOUNT_PUBKEY_FIELDS = {
    'Binding': ['miner'],
    'HotkeyBinding': ['miner'],
    'Config': ['admin'],
    'MinerState': ['miner'],
    'MinerDirectionStats': ['miner'],
    'MinerQuote': ['miner'],
    'Reservation': ['user'],
    'Swap': ['user', 'miner'],
    'Pool': ['miner'],
    'CollateralVault': [],
    'Treasury': [],
    'VoteRound': [],
}

# --- instruction encoding (8-byte discriminator from IDL + borsh arg body) ---
IX_DISCRIMINATORS = {
    'initialize': bytes([175, 175, 109, 31, 13, 152, 155, 237]),
    'bind_hotkey': bytes([160, 181, 124, 204, 23, 209, 192, 61]),
    'set_quote': bytes([59, 95, 185, 175, 67, 228, 200, 29]),
    'post_collateral': bytes([124, 252, 97, 53, 118, 194, 88, 112]),
    'withdraw_collateral': bytes([115, 135, 168, 106, 139, 214, 138, 150]),
}
IX_INITIALIZE_ARGS = CStruct(
    'min_collateral' / U64,
    'max_collateral' / U64,
    'fulfillment_timeout_secs' / I64,
    'consensus_threshold_percent' / U8,
    'min_swap_amount' / U64,
    'max_swap_amount' / U64,
    'reservation_ttl_secs' / I64,
)
IX_SET_QUOTE_ARGS = CStruct(
    'from_chain' / String,
    'to_chain' / String,
    'miner_from_addr' / String,
    'miner_to_addr' / String,
    'rate' / U128,
    'liquidity' / U128,
)
IX_AMOUNT_ARGS = CStruct('amount' / U64)

# name -> CStruct for the generic reader.
ACCOUNT_LAYOUTS = {
    'Binding': Binding,
    'CollateralVault': CollateralVault,
    'Config': Config,
    'HotkeyBinding': HotkeyBinding,
    'MinerDirectionStats': MinerDirectionStats,
    'MinerQuote': MinerQuote,
    'MinerState': MinerState,
    'Pool': Pool,
    'Reservation': Reservation,
    'Swap': Swap,
    'Treasury': Treasury,
    'VoteRound': VoteRound,
}
