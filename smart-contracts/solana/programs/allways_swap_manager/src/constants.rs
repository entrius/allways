use anchor_lang::prelude::*;

/// PDA seed for the singleton config account (`seeds = [CONFIG_SEED]`).
#[constant]
pub const CONFIG_SEED: &[u8] = b"config";

/// PDA seed for the singleton native-SOL collateral vault (`seeds = [VAULT_SEED]`).
#[constant]
pub const VAULT_SEED: &[u8] = b"vault";

/// PDA seed prefix for per-miner state (`seeds = [MINER_SEED, miner_pubkey]`).
#[constant]
pub const MINER_SEED: &[u8] = b"miner";

/// PDA seed prefix for a consensus vote round
/// (`seeds = [VOTE_SEED, &[request_type], target_pubkey]`).
#[constant]
pub const VOTE_SEED: &[u8] = b"vote";

/// PDA seed prefix for a confirmed reservation (`seeds = [RESV_SEED, miner_pubkey]`).
#[constant]
pub const RESV_SEED: &[u8] = b"resv";

/// PDA seed prefix for a swap (`seeds = [SWAP_SEED, swap_key]`, swap_key = keccak(from_tx_hash)).
#[constant]
pub const SWAP_SEED: &[u8] = b"swap";

/// PDA seed prefix for the permanent source-tx replay marker (`seeds = [TX_SEED, swap_key]`).
#[constant]
pub const TX_SEED: &[u8] = b"tx";

/// PDA seed prefix for a miner's standing per-pair quote
/// (`seeds = [QUOTE_SEED, miner_pubkey, from_chain, to_chain]`). Phase 8.
#[constant]
pub const QUOTE_SEED: &[u8] = b"quote";

/// PDA seed prefix for a per-miner reservation-lottery pool (`seeds = [POOL_SEED, miner]`). Phase 9.
#[constant]
pub const POOL_SEED: &[u8] = b"pool";

/// On-chain schema/version, surfaced for upgrade tracking. Bumped as phases land.
/// v2: Phase 8 (on-chain miner quotes + per-validator weights).
/// v3: Phase 9 (reservation lottery + flat reservation fee).
/// v4: Phase 10 (consensus-governed validator weights).
/// v5: emergency halt switch.
/// v6: runtime config setters (fee/window/interval promoted to Config).
pub const CONFIG_VERSION: u32 = 6;

/// Max validators in the whitelist (bounds the Config `validators` Vec and a round's voters).
pub const MAX_VALIDATORS: usize = 16;

/// A vote round older than this (seconds) is treated as stale and reset before recording a new vote.
pub const VOTE_ROUND_TTL_SECS: i64 = 1800;

/// Request types (keys into a vote round). Mirror the ink! contract's request enum.
/// (REQ_RESERVE removed in Phase 9 — reservations are now lottery-based, not consensus-voted.)
pub const REQ_ACTIVATE: u8 = 0;
pub const REQ_INITIATE: u8 = 2;
pub const REQ_DEACTIVATE: u8 = 5;
pub const REQ_CONFIRM: u8 = 6;
pub const REQ_TIMEOUT: u8 = 7;
/// Phase 10: global (non-per-target) round for the validator-weight vector.
pub const REQ_SET_WEIGHTS: u8 = 8;

/// Protocol fee divisor — 1% (immutable), `fee = sol_amount / FEE_DIVISOR`.
pub const FEE_DIVISOR: u64 = 100;

/// Initial flat anti-spam fee (lamports) per reservation request, seeded into Config at init and
/// runtime-tunable via `set_reservation_fee`. Default 0.001 SOL.
pub const RESERVATION_FEE_LAMPORTS: u64 = 1_000_000;

/// Initial reservation-lottery pooling window (seconds), seeded into Config at init and tunable via
/// `set_pool_window`. Must stay well below the reservation TTL (separate windows).
pub const POOL_WINDOW_SECS: i64 = 3;

/// Solana slot time (ms), used to pin the draw's future seed slot from the window duration.
pub const SLOT_MS: u64 = 400;

/// Initial minimum seconds between consensus weight updates — anti-thrash floor — seeded into Config
/// at init and tunable via `set_weights_update_min_interval`.
pub const WEIGHTS_UPDATE_MIN_INTERVAL_SECS: i64 = 3600;

/// Bounded max lengths for stored strings (see SOLANA_MIGRATION_RESEARCH.md §14).
pub const MAX_ADDR_LEN: usize = 80;
pub const MAX_CHAIN_LEN: usize = 16;
pub const MAX_RATE_LEN: usize = 32;
pub const MAX_TX_LEN: usize = 128;
