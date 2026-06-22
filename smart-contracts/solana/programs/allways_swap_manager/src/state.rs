use anchor_lang::prelude::*;

use crate::constants::{MAX_ADDR_LEN, MAX_CHAIN_LEN, MAX_RATE_LEN, MAX_TX_LEN, MAX_VALIDATORS};

/// A whitelisted validator and its draw weight. `weight` (default 1, admin-set) is the
/// stake-weight seam consumed ONLY by the reservation-lottery draw; consensus stays count-based.
#[derive(AnchorSerialize, AnchorDeserialize, Clone, InitSpace)]
pub struct ValidatorInfo {
    pub key: Pubkey,
    pub weight: u64,
}

/// Singleton config PDA (`seeds = [CONFIG_SEED]`). All amounts in lamports, durations in seconds.
#[account]
#[derive(InitSpace)]
pub struct Config {
    /// Admin authority (treasury withdrawals + config setters).
    pub admin: Pubkey,
    /// On-chain schema version, for upgrade tracking.
    pub version: u32,
    /// Minimum collateral a miner must hold to be activatable (lamports).
    pub min_collateral: u64,
    /// Maximum collateral a miner may post (lamports). 0 = no cap.
    pub max_collateral: u64,
    /// Swap fulfillment timeout (seconds); withdrawal cooldown = 2x this.
    pub fulfillment_timeout_secs: i64,
    /// Swap-size bounds on the collateral-backed (SOL) amount, in lamports. 0 = unbounded.
    pub min_swap_amount: u64,
    pub max_swap_amount: u64,
    /// How long a reservation holds a miner exclusive, in seconds.
    pub reservation_ttl_secs: i64,
    /// Quorum threshold, percent of the whitelisted validator set (e.g. 66).
    pub consensus_threshold_percent: u8,
    /// Whitelisted validator set (consensus participants) + draw weights, capped at MAX_VALIDATORS.
    #[max_len(MAX_VALIDATORS)]
    pub validators: Vec<ValidatorInfo>,
    /// Unix timestamp of the last consensus weight update (0 = never). Gates the update cadence floor.
    pub last_weights_update: i64,
    /// Emergency halt: when true, new deposits / activations / reservation pools are rejected.
    pub halted: bool,
    /// Flat anti-spam fee per reservation request, lamports (runtime-tunable; 0 disables).
    pub reservation_fee_lamports: u64,
    /// Reservation-lottery pooling window, seconds (runtime-tunable).
    pub pool_window_secs: i64,
    /// Minimum seconds between consensus weight updates (runtime-tunable anti-thrash floor).
    pub weights_update_min_interval_secs: i64,
    /// Stored PDA bump.
    pub bump: u8,
}

/// Singleton native-SOL collateral vault PDA (`seeds = [VAULT_SEED]`), program-owned.
///
/// Invariant: vault.lamports == rent_exempt_minimum + total_collateral (+ treasury + pending, later).
#[account]
#[derive(InitSpace)]
pub struct Vault {
    /// Σ of all miners' collateral credited to the vault (lamports), excludes the rent reserve.
    pub total_collateral: u64,
    /// Accrued protocol fees held in the vault (lamports), awaiting admin withdrawal (Phase 6).
    pub treasury_total: u64,
    /// Stored PDA bump.
    pub bump: u8,
}

/// Per-miner state PDA (`seeds = [MINER_SEED, miner]`).
#[account]
#[derive(InitSpace)]
pub struct MinerState {
    /// The miner (hotkey-equivalent) this state belongs to.
    pub miner: Pubkey,
    /// Collateral credited to this miner (lamports). Backed 1:1 by lamports in the Vault.
    pub collateral: u64,
    /// Whether the miner is active (set via consensus).
    pub active: bool,
    /// Whether the miner currently has an in-flight swap.
    pub has_active_swap: bool,
    /// Unix ts the miner is busy until (open pool, held reservation, or in-flight swap). Self-clearing
    /// (`now >= busy_until` = free); the non-bypassable busy lock for deactivate/withdraw_collateral.
    pub busy_until: i64,
    /// Unix timestamp of last deactivation (0 = never). Gates the withdrawal cooldown.
    pub deactivation_at: i64,
    /// Stored PDA bump.
    pub bump: u8,
}

/// A consensus vote round PDA (`seeds = [VOTE_SEED, &[request_type], target]`).
///
/// `bound_hash` binds every voter to identical request params (keccak of the canonical request),
/// preventing bait-and-switch on requests whose params aren't fully in the seeds (reserve/initiate).
#[account]
#[derive(InitSpace)]
pub struct VoteRound {
    /// keccak-256 of the canonical request params; set by the first voter, checked by the rest.
    pub bound_hash: [u8; 32],
    /// Validators who have voted this round (deduplicated), capped at MAX_VALIDATORS.
    #[max_len(MAX_VALIDATORS)]
    pub voters: Vec<Pubkey>,
    /// Unix timestamp the round opened (0 = empty/available). Used for TTL reset.
    pub created_at: i64,
    /// Stored PDA bump.
    pub bump: u8,
}

/// Confirmed reservation for a miner (`seeds = [RESV_SEED, miner]`).
///
/// Created by `resolve_pool` (lottery draw); consumed by `vote_initiate` or left to expire.
/// `reserved_until`: 0 = empty, >= now = active, 0 < it < now = expired (overwritable).
/// `from_addr` is kept so initiate can verify the initiating user matches the reserver.
#[account]
#[derive(InitSpace)]
pub struct Reservation {
    /// keccak-256 binding (miner, from_addr, chains, amounts) — same preimage validators voted on.
    pub bound_hash: [u8; 32],
    /// User's source-chain address (the reserver).
    #[max_len(MAX_ADDR_LEN)]
    pub from_addr: String,
    #[max_len(MAX_CHAIN_LEN)]
    pub from_chain: String,
    #[max_len(MAX_CHAIN_LEN)]
    pub to_chain: String,
    /// Collateral-backed swap size (SOL lamports). Bounded by Config min/max_swap_amount.
    pub sol_amount: u64,
    /// Off-chain leg amounts in their own assets (u128 to cover wei-scale).
    pub from_amount: u128,
    pub to_amount: u128,
    /// Pinned miner quote — hash-bound at reserve time. `vote_initiate` MUST honor these (not the
    /// miner's live commitment): closes the rate-swing / deposit-address-theft total-loss bug.
    #[max_len(MAX_ADDR_LEN)]
    pub miner_from_addr: String,
    #[max_len(MAX_ADDR_LEN)]
    pub miner_to_addr: String,
    #[max_len(MAX_RATE_LEN)]
    pub rate: String,
    /// Expiry, unix seconds (0 = empty).
    pub reserved_until: i64,
    /// Stored PDA bump.
    pub bump: u8,
}

/// Swap lifecycle status. Terminal states (Completed/TimedOut) are not stored — the Swap PDA is
/// closed on confirm/timeout — so only the two live states exist here.
#[derive(AnchorSerialize, AnchorDeserialize, Clone, Copy, PartialEq, Eq, InitSpace)]
pub enum SwapStatus {
    Active,
    Fulfilled,
}

/// An in-flight swap (`seeds = [SWAP_SEED, swap_key]`, swap_key = keccak(from_tx_hash)).
/// Created by `vote_initiate` on quorum; closed by `confirm_swap` / `timeout_swap`. Chains/amounts/
/// miner-quote copied from the immutable Reservation; user-side fields from the hash-bound initiate vote.
#[account]
#[derive(InitSpace)]
pub struct Swap {
    pub user: Pubkey,
    pub miner: Pubkey,
    #[max_len(MAX_CHAIN_LEN)]
    pub from_chain: String,
    #[max_len(MAX_CHAIN_LEN)]
    pub to_chain: String,
    #[max_len(MAX_ADDR_LEN)]
    pub user_from_addr: String,
    #[max_len(MAX_ADDR_LEN)]
    pub user_to_addr: String,
    #[max_len(MAX_ADDR_LEN)]
    pub miner_from_addr: String,
    #[max_len(MAX_ADDR_LEN)]
    pub miner_to_addr: String,
    #[max_len(MAX_RATE_LEN)]
    pub rate: String,
    /// Collateral-backed swap size (SOL lamports) — fee/slash basis.
    pub sol_amount: u64,
    pub from_amount: u128,
    pub to_amount: u128,
    #[max_len(MAX_TX_LEN)]
    pub from_tx_hash: String,
    pub from_tx_block: u32,
    #[max_len(MAX_TX_LEN)]
    pub to_tx_hash: String,
    pub to_tx_block: u32,
    pub status: SwapStatus,
    pub initiated_at: i64,
    pub timeout_at: i64,
    pub fulfilled_at: i64,
    pub bump: u8,
}

/// Permanent source-tx replay marker (`seeds = [TX_SEED, swap_key]`). Set `used` on initiate quorum
/// and never closed, so it outlives the Swap: a from_tx_hash can initiate at most one swap, ever.
#[account]
#[derive(InitSpace)]
pub struct TxMarker {
    pub used: bool,
    pub bump: u8,
}

/// A miner's standing on-chain quote for one pair-direction
/// (`seeds = [QUOTE_SEED, miner, from_chain, to_chain]`).
///
/// Replaces the off-chain Bittensor commitment string: one PDA per direction (the `(from_chain,
/// to_chain)` ordering encodes direction, so no `counter_rate`). Permissionless to write
/// (`set_quote`, overwrites in place); pools pin whatever's current, so staleness is the miner's
/// problem. Closed + rent-refunded via `remove_quote`.
#[account]
#[derive(InitSpace)]
pub struct MinerQuote {
    /// The miner (signer) that owns this quote.
    pub miner: Pubkey,
    #[max_len(MAX_CHAIN_LEN)]
    pub from_chain: String,
    #[max_len(MAX_CHAIN_LEN)]
    pub to_chain: String,
    /// Where the miner receives the source asset (on `from_chain`).
    #[max_len(MAX_ADDR_LEN)]
    pub miner_from_addr: String,
    /// Where the miner sends the destination asset (on `to_chain`).
    #[max_len(MAX_ADDR_LEN)]
    pub miner_to_addr: String,
    /// Offered rate, dest per 1 source, for THIS direction (string for exact sig-fig precision).
    #[max_len(MAX_RATE_LEN)]
    pub rate: String,
    /// Advertised depth in the asset's own units (u128 to cover wei-scale).
    pub liquidity: u128,
    /// Unix timestamp of the last write (staleness signal for off-chain consumers).
    pub updated_at: i64,
    /// Stored PDA bump.
    pub bump: u8,
}

/// One validator's entry into a reservation lottery `Pool`. Carries only the taker-side intent;
/// the miner quote is the pool's pinned snapshot, not per-request.
#[derive(AnchorSerialize, AnchorDeserialize, Clone, InitSpace)]
pub struct Request {
    /// The validator that routed this request (also the lottery weight key + dedup key).
    pub validator: Pubkey,
    /// The taker.
    pub user: Pubkey,
    #[max_len(MAX_ADDR_LEN)]
    pub user_from_addr: String,
    #[max_len(MAX_ADDR_LEN)]
    pub user_to_addr: String,
    /// Collateral-backed swap size (SOL lamports). Bounded by Config min/max_swap_amount.
    pub sol_amount: u64,
    pub from_amount: u128,
    pub to_amount: u128,
}

/// A reservation-lottery contest for one idle miner (`seeds = [POOL_SEED, miner]`).
///
/// Opened by the first validator to route a request (pinning the miner's quote for the chosen pair);
/// later in-window requests must match that pair. `resolve_pool` runs a stake-weighted draw after
/// `closes_at` and creates the winner's `Reservation`. Keyed per-miner; the account is reused across
/// contests (`opened_at == 0` = available), reset rather than closed by `resolve_pool`.
#[account]
#[derive(InitSpace)]
pub struct Pool {
    pub miner: Pubkey,
    /// Pinned pair + miner-quote snapshot, copied from the `MinerQuote` PDA at open.
    #[max_len(MAX_CHAIN_LEN)]
    pub from_chain: String,
    #[max_len(MAX_CHAIN_LEN)]
    pub to_chain: String,
    #[max_len(MAX_ADDR_LEN)]
    pub miner_from_addr: String,
    #[max_len(MAX_ADDR_LEN)]
    pub miner_to_addr: String,
    #[max_len(MAX_RATE_LEN)]
    pub rate: String,
    /// Unix seconds the pool opened (0 = available/empty slot).
    pub opened_at: i64,
    /// Unix seconds the request window closes; `resolve_pool` is callable after this.
    pub closes_at: i64,
    /// Future slot whose SlotHash seeds the draw (pinned at open).
    pub seed_slot: u64,
    /// Requests this contest (deduped by validator), capped at MAX_VALIDATORS.
    #[max_len(MAX_VALIDATORS)]
    pub requests: Vec<Request>,
    /// Stored PDA bump.
    pub bump: u8,
}
