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

# A bid carries only the router (two-phase: the seat winner names the fill at finalize).
Request = CStruct(
    'router' / Pubkey32,
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

# Two-phase: `router` pinned at draw (unfilled, reserved_until==0); the rest written at finalize.
Reservation = CStruct(
    'router' / Pubkey32,
    'from_addr' / String,
    'user' / Pubkey32,
    'user_to_addr' / String,
    'from_chain' / String,
    'to_chain' / String,
    'collateral_amount' / U64,
    'from_amount' / U128,
    'to_amount' / U128,
    'miner_from_addr' / String,
    'miner_to_addr' / String,
    'rate' / U128,
    'created_at' / I64,
    'reserved_until' / I64,
    'finalize_by' / I64,
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
    'collateral_amount' / U64,
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
    'finalize_window_secs' / I64,
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
    'Reservation': ['router', 'user'],
    'Swap': ['user', 'miner'],
    'Pool': ['miner'],
    'CollateralVault': [],
    'Treasury': [],
    'VoteRound': [],
}

# --- events (8-byte discriminator from IDL `events[].discriminator` + borsh(fields)) ---
# Emitted via Anchor self-CPI and surfaced in tx logs as base64 `Program data:` lines. Field order/types
# mirror events.rs / the IDL exactly. swap_key + hotkey stay raw bytes (see EVENT_PUBKEY_FIELDS).
EVENT_DISCRIMINATORS = {
    'CollateralPosted': bytes([133, 193, 58, 199, 229, 183, 154, 206]),
    'CollateralWithdrawn': bytes([51, 224, 133, 106, 74, 173, 72, 82]),
    'FulfillmentGraceApplied': bytes([201, 98, 85, 62, 191, 162, 4, 22]),
    'HaltSet': bytes([72, 72, 136, 23, 166, 26, 205, 223]),
    'HotkeyBound': bytes([168, 26, 136, 137, 160, 137, 120, 133]),
    'MinerActivated': bytes([203, 75, 131, 151, 24, 167, 159, 19]),
    'MinerDeactivated': bytes([31, 67, 233, 59, 174, 101, 245, 122]),
    'PoolDrawArmed': bytes([56, 138, 178, 84, 109, 162, 248, 202]),
    'PoolOpened': bytes([44, 53, 197, 215, 31, 61, 56, 170]),
    'PoolResolved': bytes([37, 148, 82, 156, 128, 131, 201, 171]),
    'QuoteRemoved': bytes([52, 211, 141, 65, 95, 43, 64, 32]),
    'QuoteSet': bytes([216, 112, 83, 84, 181, 53, 176, 105]),
    'ReservationExtended': bytes([242, 117, 112, 204, 238, 175, 247, 227]),
    'ReservationFilled': bytes([198, 252, 253, 103, 211, 7, 123, 183]),
    'ReservationRequested': bytes([246, 75, 57, 78, 231, 75, 222, 115]),
    'StaleClaimClosed': bytes([97, 73, 19, 101, 231, 36, 61, 186]),
    'UnfilledReservationClosed': bytes([36, 72, 242, 60, 28, 44, 38, 55]),
    'SwapClaimed': bytes([2, 124, 144, 84, 160, 92, 158, 1]),
    'SwapCompleted': bytes([118, 93, 218, 77, 215, 165, 112, 76]),
    'SwapFulfilled': bytes([62, 201, 236, 62, 234, 76, 17, 39]),
    'SwapInitiated': bytes([88, 197, 100, 28, 189, 82, 98, 2]),
    'SwapTimedOut': bytes([216, 21, 45, 129, 255, 250, 107, 166]),
    'SwapTimeoutExtended': bytes([125, 63, 87, 100, 186, 75, 227, 121]),
    'TreasuryWithdrawn': bytes([143, 181, 157, 169, 87, 155, 170, 46]),
    'ValidatorWeightsUpdated': bytes([38, 6, 182, 182, 124, 27, 131, 38]),
}

EVENT_LAYOUTS = {
    'CollateralPosted': CStruct('miner' / Pubkey32, 'amount' / U64, 'total' / U64),
    'CollateralWithdrawn': CStruct('miner' / Pubkey32, 'amount' / U64, 'total' / U64),
    'FulfillmentGraceApplied': CStruct('swap_key' / Hash32, 'miner' / Pubkey32, 'timeout_at' / I64),
    'HaltSet': CStruct('halted' / Bool),
    'HotkeyBound': CStruct('miner' / Pubkey32, 'hotkey' / Hash32, 'bound_at' / I64),
    'MinerActivated': CStruct('miner' / Pubkey32, 'at' / I64),
    'MinerDeactivated': CStruct('miner' / Pubkey32, 'at' / I64),
    'PoolOpened': CStruct(
        'miner' / Pubkey32,
        'opener' / Pubkey32,
        'from_chain' / String,
        'to_chain' / String,
        'closes_at' / I64,
        'seed_slot' / U64,
    ),
    'PoolDrawArmed': CStruct('miner' / Pubkey32, 'seed_slot' / U64),
    'PoolResolved': CStruct('miner' / Pubkey32, 'winner' / Pubkey32, 'requests' / U8),
    'QuoteRemoved': CStruct('miner' / Pubkey32, 'from_chain' / String, 'to_chain' / String, 'remove_fee' / U64),
    'QuoteSet': CStruct(
        'miner' / Pubkey32,
        'from_chain' / String,
        'to_chain' / String,
        'rate' / U128,
        'liquidity' / U128,
        'updated_at' / I64,
        'update_fee' / U64,
    ),
    'ReservationExtended': CStruct('miner' / Pubkey32, 'validator' / Pubkey32, 'reserved_until' / I64),
    'ReservationFilled': CStruct(
        'miner' / Pubkey32,
        'router' / Pubkey32,
        'user' / Pubkey32,
        'from_chain' / String,
        'to_chain' / String,
        'collateral_amount' / U64,
        'from_amount' / U128,
        'to_amount' / U128,
        'reserved_until' / I64,
    ),
    'ReservationRequested': CStruct('miner' / Pubkey32, 'router' / Pubkey32, 'requests' / U8),
    'StaleClaimClosed': CStruct('swap_key' / Hash32, 'miner' / Pubkey32),
    'UnfilledReservationClosed': CStruct('miner' / Pubkey32, 'router' / Pubkey32),
    'SwapClaimed': CStruct(
        'swap_key' / Hash32,
        'miner' / Pubkey32,
        'user' / Pubkey32,
        'from_tx_hash' / String,
        'from_tx_block' / U32,
    ),
    'SwapCompleted': CStruct(
        'swap_key' / Hash32,
        'miner' / Pubkey32,
        'collateral_amount' / U64,
        'fee' / U64,
        'from_chain' / String,
        'to_chain' / String,
        'from_amount' / U128,
        'to_amount' / U128,
        'rate' / U128,
    ),
    'SwapFulfilled': CStruct('swap_key' / Hash32, 'miner' / Pubkey32, 'to_tx_hash' / String, 'to_amount' / U128),
    'SwapInitiated': CStruct(
        'swap_key' / Hash32,
        'user' / Pubkey32,
        'miner' / Pubkey32,
        'collateral_amount' / U64,
        'from_amount' / U128,
        'to_amount' / U128,
        'initiated_at' / I64,
    ),
    'SwapTimedOut': CStruct('swap_key' / Hash32, 'miner' / Pubkey32, 'collateral_amount' / U64, 'slash' / U64),
    'SwapTimeoutExtended': CStruct('swap_key' / Hash32, 'miner' / Pubkey32, 'validator' / Pubkey32, 'timeout_at' / I64),
    'TreasuryWithdrawn': CStruct('recipient' / Pubkey32, 'amount' / U64, 'total' / U64),
    'ValidatorWeightsUpdated': CStruct('count' / U8, 'updated_at' / I64),
}

# Pubkey fields per event (decoded bytes -> solders Pubkey by the client). swap_key/hotkey stay raw bytes.
EVENT_PUBKEY_FIELDS = {
    'CollateralPosted': ['miner'],
    'CollateralWithdrawn': ['miner'],
    'FulfillmentGraceApplied': ['miner'],
    'HaltSet': [],
    'HotkeyBound': ['miner'],
    'MinerActivated': ['miner'],
    'MinerDeactivated': ['miner'],
    'PoolDrawArmed': ['miner'],
    'PoolOpened': ['miner', 'opener'],
    'PoolResolved': ['miner', 'winner'],
    'QuoteRemoved': ['miner'],
    'QuoteSet': ['miner'],
    'ReservationExtended': ['miner', 'validator'],
    'ReservationFilled': ['miner', 'router', 'user'],
    'ReservationRequested': ['miner', 'router'],
    'StaleClaimClosed': ['miner'],
    'UnfilledReservationClosed': ['miner', 'router'],
    'SwapClaimed': ['miner', 'user'],
    'SwapCompleted': ['miner'],
    'SwapFulfilled': ['miner'],
    'SwapInitiated': ['user', 'miner'],
    'SwapTimedOut': ['miner'],
    'SwapTimeoutExtended': ['miner', 'validator'],
    'TreasuryWithdrawn': ['recipient'],
    'ValidatorWeightsUpdated': [],
}

# --- instruction encoding (8-byte discriminator from IDL + borsh arg body) ---
IX_DISCRIMINATORS = {
    'initialize': bytes([175, 175, 109, 31, 13, 152, 155, 237]),
    'bind_hotkey': bytes([160, 181, 124, 204, 23, 209, 192, 61]),
    'set_quote': bytes([59, 95, 185, 175, 67, 228, 200, 29]),
    'post_collateral': bytes([124, 252, 97, 53, 118, 194, 88, 112]),
    'withdraw_collateral': bytes([115, 135, 168, 106, 139, 214, 138, 150]),
    # B2 — swap lifecycle (validator votes + miner mark + the claim relay).
    'submit_swap_claim': bytes([15, 176, 220, 236, 85, 115, 110, 135]),
    'vote_initiate': bytes([210, 23, 157, 114, 35, 129, 164, 4]),
    'confirm_swap': bytes([183, 168, 179, 117, 86, 243, 166, 195]),
    'timeout_swap': bytes([18, 157, 212, 120, 145, 200, 239, 63]),
    'close_stale_claim': bytes([185, 69, 27, 37, 187, 78, 157, 188]),  # reap an orphaned PendingAttestation claim
    'vote_activate': bytes([24, 233, 47, 230, 116, 115, 109, 41]),
    'mark_fulfilled': bytes([40, 188, 159, 127, 20, 151, 228, 191]),
    'extend_timeout': bytes([246, 84, 96, 134, 76, 55, 57, 33]),
    'extend_reservation': bytes([97, 77, 20, 170, 71, 8, 163, 187]),
    'add_validator': bytes([250, 113, 53, 54, 141, 117, 215, 185]),  # admin (test/bootstrap helper)
    # B4 — miner self-deactivate + quote retract.
    'deactivate': bytes([44, 112, 33, 172, 113, 28, 142, 13]),
    'remove_quote': bytes([168, 104, 162, 147, 237, 163, 196, 74]),
    # B4 — admin runtime config setters (Context<AdminConfig>: admin signer + config mut).
    'remove_validator': bytes([25, 96, 211, 155, 161, 14, 168, 188]),
    'set_consensus_threshold': bytes([183, 186, 216, 249, 16, 231, 243, 244]),
    'set_fulfillment_timeout': bytes([73, 250, 230, 231, 192, 14, 146, 215]),
    'set_halted': bytes([153, 114, 136, 116, 7, 134, 47, 12]),
    'set_max_collateral': bytes([18, 180, 73, 244, 52, 74, 176, 110]),
    'set_max_swap_amount': bytes([7, 177, 215, 146, 45, 7, 220, 199]),
    'set_min_collateral': bytes([128, 193, 225, 225, 249, 133, 38, 70]),
    'set_min_swap_amount': bytes([189, 59, 139, 62, 167, 12, 58, 88]),
    'set_reservation_ttl': bytes([108, 189, 90, 167, 113, 34, 82, 140]),
    'withdraw_treasury': bytes([40, 63, 122, 158, 144, 216, 83, 96]),
    # D6 — remaining admin levers (single scalar each, reusing IX_AMOUNT_ARGS u64 / IX_I64_ARGS i64).
    'set_reservation_fee': bytes([58, 32, 204, 143, 35, 61, 70, 115]),
    'set_pool_window': bytes([250, 90, 55, 0, 118, 48, 94, 204]),
    'set_weights_update_min_interval': bytes([185, 134, 117, 75, 73, 184, 80, 123]),
    'set_max_total_extension': bytes([185, 183, 148, 252, 204, 128, 7, 24]),
    # Phase 9 — swap intake (reservation-lottery pool). Two-phase: bid → draw → finalize.
    'open_or_request': bytes([174, 133, 208, 178, 0, 117, 73, 12]),
    'resolve_pool': bytes([191, 164, 190, 142, 178, 198, 162, 249]),  # no args (empty body)
    'finalize_reservation': bytes([237, 55, 120, 249, 88, 130, 214, 133]),
    'close_unfilled_reservation': bytes([162, 160, 156, 98, 241, 195, 23, 88]),  # no args (empty body)
    'set_finalize_window': bytes([84, 242, 160, 48, 107, 111, 170, 241]),  # IX_I64_ARGS
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

# B2 swap-lifecycle args. `swap_key` is a borsh `[u8; 32]` (fixed array → raw 32 bytes, no len prefix).
IX_SWAP_KEY_ARGS = CStruct('swap_key' / Hash32)  # vote_initiate, timeout_swap, close_stale_claim
IX_SUBMIT_CLAIM_ARGS = CStruct('swap_key' / Hash32, 'from_tx_hash' / String, 'from_tx_block' / U32)
IX_CONFIRM_SWAP_ARGS = CStruct('swap_key' / Hash32, 'from_chain' / String, 'to_chain' / String)
IX_MARK_FULFILLED_ARGS = CStruct('swap_key' / Hash32, 'to_tx_hash' / String, 'to_tx_block' / U32)
IX_EXTEND_TIMEOUT_ARGS = CStruct('swap_key' / Hash32, 'target_at' / I64)
IX_EXTEND_RESERVATION_ARGS = CStruct('target_at' / I64)
IX_ADD_VALIDATOR_ARGS = CStruct('validator' / Pubkey32, 'weight' / U64)

# B4 — quote retract + admin-setter args. (`deactivate` takes no args → empty body.)
IX_REMOVE_QUOTE_ARGS = CStruct('from_chain' / String, 'to_chain' / String)
# Phase 9 — two-phase reservation. A bid is just the pair (resolve_pool / close_unfilled_reservation
# take no args). The seat winner names the fill in finalize_reservation. Order = handler param order.
IX_OPEN_OR_REQUEST_ARGS = CStruct(
    'from_chain' / String,
    'to_chain' / String,
)
IX_FINALIZE_RESERVATION_ARGS = CStruct(
    'user' / Pubkey32,
    'user_from_addr' / String,
    'user_to_addr' / String,
    'collateral_amount' / U64,
    'from_amount' / U128,
    'to_amount' / U128,
)
IX_PUBKEY_ARGS = CStruct('value' / Pubkey32)  # remove_validator
IX_U8_ARGS = CStruct('value' / U8)  # set_consensus_threshold
IX_I64_ARGS = CStruct('value' / I64)  # set_fulfillment_timeout
IX_BOOL_ARGS = CStruct('value' / Bool)  # set_halted
# set_min/max_collateral, set_min/max_swap_amount, withdraw_treasury reuse IX_AMOUNT_ARGS (single u64).

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
