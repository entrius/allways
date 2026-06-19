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
pub const CONFIG_VERSION: u32 = 5;

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

// NOTE: deploy-time **economic** levers (FEE_DIVISOR, RESERVATION_FEE_LAMPORTS, POOL_WINDOW_SECS,
// WEIGHTS_UPDATE_MIN_INTERVAL_SECS, plus the collateral & quote-fee knobs) now live in `tunables.rs`
// — one home for everything you might retune at deploy. This file keeps only **structural** constants
// (seeds, request-type bytes, max lengths, chain facts).

/// Solana slot time (ms) — a chain property (not an economic lever), used with
/// `tunables::POOL_WINDOW_SECS` to pin the draw's future seed slot from the window duration.
pub const SLOT_MS: u64 = 400;

/// Bounded max lengths for stored strings (see SOLANA_MIGRATION_RESEARCH.md §14).
pub const MAX_ADDR_LEN: usize = 80;
pub const MAX_CHAIN_LEN: usize = 16;
pub const MAX_RATE_LEN: usize = 32;
pub const MAX_TX_LEN: usize = 128;
