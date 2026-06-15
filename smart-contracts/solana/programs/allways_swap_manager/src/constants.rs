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

/// On-chain schema/version, surfaced for upgrade tracking. Bumped as phases land.
/// v2: Phase 8 (on-chain miner quotes + per-validator weights).
pub const CONFIG_VERSION: u32 = 2;

/// Max validators in the whitelist (bounds the Config `validators` Vec and a round's voters).
pub const MAX_VALIDATORS: usize = 16;

/// A vote round older than this (seconds) is treated as stale and reset before recording a new vote.
pub const VOTE_ROUND_TTL_SECS: i64 = 1800;

/// Request types (keys into a vote round). Mirror the ink! contract's request enum.
pub const REQ_ACTIVATE: u8 = 0;
pub const REQ_RESERVE: u8 = 1;
pub const REQ_INITIATE: u8 = 2;
pub const REQ_DEACTIVATE: u8 = 5;
pub const REQ_CONFIRM: u8 = 6;
pub const REQ_TIMEOUT: u8 = 7;

/// Protocol fee divisor — 1% (immutable), `fee = sol_amount / FEE_DIVISOR`.
pub const FEE_DIVISOR: u64 = 100;

/// Bounded max lengths for stored strings (see SOLANA_MIGRATION_RESEARCH.md §14).
pub const MAX_ADDR_LEN: usize = 80;
pub const MAX_CHAIN_LEN: usize = 16;
pub const MAX_RATE_LEN: usize = 32;
pub const MAX_TX_LEN: usize = 128;
