use anchor_lang::prelude::*;

/// Collateral events carry the resulting total, not a delta — so a consumer can set an absolute
/// baseline from one event instead of accumulating from an unknown start. Fee/slash events follow
/// the same post-total rule.
#[event]
pub struct CollateralPosted {
    pub miner: Pubkey,
    /// Amount added this call (lamports).
    pub amount: u64,
    /// Miner's resulting collateral total after this call (lamports).
    pub total: u64,
}

#[event]
pub struct CollateralWithdrawn {
    pub miner: Pubkey,
    /// Amount removed this call (lamports).
    pub amount: u64,
    /// Miner's resulting collateral total after this call (lamports).
    pub total: u64,
}

// --- Phase 4: swap lifecycle (keyed by swap_key = keccak(from_tx_hash)) ---

/// A reservation holder recorded their source-tx hash on-chain (PendingAttestation). No miner
/// obligation yet — validators attest it via `vote_initiate`.
#[event]
pub struct SwapClaimed {
    pub swap_key: [u8; 32],
    pub miner: Pubkey,
    pub user: Pubkey,
    pub from_tx_hash: String,
    pub from_tx_block: u32,
}

/// A stale (never-attested, reservation-expired) PendingAttestation claim was reaped.
#[event]
pub struct StaleClaimClosed {
    pub swap_key: [u8; 32],
    pub miner: Pubkey,
}

#[event]
pub struct SwapInitiated {
    pub swap_key: [u8; 32],
    pub user: Pubkey,
    pub miner: Pubkey,
    pub sol_amount: u64,
    pub from_amount: u128,
    pub to_amount: u128,
    pub initiated_at: i64,
}

#[event]
pub struct SwapFulfilled {
    pub swap_key: [u8; 32],
    pub miner: Pubkey,
    pub to_tx_hash: String,
    /// Emitted so indexers don't re-read the contract for the delivered amount (v2 cleanup).
    pub to_amount: u128,
}

#[event]
pub struct SwapCompleted {
    pub swap_key: [u8; 32],
    pub miner: Pubkey,
    pub sol_amount: u64,
    /// Protocol fee taken from collateral into the treasury (lamports).
    pub fee: u64,
    /// Direction + realized leg amounts + executed rate, for off-chain per-swap history (so indexers
    /// don't re-read the now-closed Swap). Feeds the realized volume/VWAP track record (A2).
    pub from_chain: String,
    pub to_chain: String,
    pub from_amount: u128,
    pub to_amount: u128,
    /// Fixed-point executed rate (display_rate × RATE_PRECISION); matches the on-chain u128 (#495).
    pub rate: u128,
}

#[event]
pub struct SwapTimedOut {
    pub swap_key: [u8; 32],
    pub miner: Pubkey,
    pub sol_amount: u64,
    /// Collateral slashed and refunded to the user (lamports).
    pub slash: u64,
}

/// A validator slid a reservation/swap deadline forward (single-validator, no quorum). Carries the
/// new deadline (post-value) so consumers set an absolute, not a delta.
#[event]
pub struct ReservationExtended {
    pub miner: Pubkey,
    pub validator: Pubkey,
    pub reserved_until: i64,
}

#[event]
pub struct SwapTimeoutExtended {
    pub swap_key: [u8; 32],
    pub miner: Pubkey,
    pub validator: Pubkey,
    pub timeout_at: i64,
}

// --- Phase 6: treasury ---

#[event]
pub struct TreasuryWithdrawn {
    pub recipient: Pubkey,
    /// Amount withdrawn this call (lamports).
    pub amount: u64,
    /// Treasury balance remaining after this call (lamports) — post-total per convention.
    pub total: u64,
}

// --- Phase 8: miner quotes (one per (miner, from_chain, to_chain)) ---

#[event]
pub struct QuoteSet {
    pub miner: Pubkey,
    pub from_chain: String,
    pub to_chain: String,
    /// Fixed-point rate = display_rate × RATE_PRECISION (1e18).
    pub rate: u128,
    pub liquidity: u128,
    pub updated_at: i64,
    /// Anti-flashing churn fee paid into the treasury this call (lamports); 0 on first creation
    /// and once a quote has stood past the decay window.
    pub update_fee: u64,
}

#[event]
pub struct QuoteRemoved {
    pub miner: Pubkey,
    pub from_chain: String,
    pub to_chain: String,
    /// Anti-flashing churn fee paid into the treasury on removal (lamports); 0 once the quote has
    /// stood past the decay window.
    pub remove_fee: u64,
}

/// A miner (re)bound its Bittensor hotkey to its Solana pubkey (A5). The sr25519 signature lives on the
/// `Binding` PDA; the validator verifies it off-chain.
#[event]
pub struct HotkeyBound {
    pub miner: Pubkey,
    pub hotkey: [u8; 32],
    pub bound_at: i64,
}

/// Miner active-state transitions. Emitted so validators can replay the per-instant `active` history for
/// the crown capacity integral from deterministic logs alone (the `MinerState.active` flag carries no
/// history). `MinerActivated` fires on `vote_activate` quorum; `MinerDeactivated` on `vote_deactivate`
/// quorum or self-`deactivate`.
#[event]
pub struct MinerActivated {
    pub miner: Pubkey,
    pub at: i64,
}

#[event]
pub struct MinerDeactivated {
    pub miner: Pubkey,
    pub at: i64,
}

// --- Phase 9: reservation lottery (pool keyed per miner) ---

#[event]
pub struct PoolOpened {
    pub miner: Pubkey,
    pub opener: Pubkey,
    pub from_chain: String,
    pub to_chain: String,
    pub closes_at: i64,
    pub seed_slot: u64,
}

#[event]
pub struct ReservationRequested {
    pub miner: Pubkey,
    pub router: Pubkey,
    pub user: Pubkey,
    /// Number of requests in the pool after this one.
    pub requests: u8,
}

#[event]
pub struct PoolResolved {
    pub miner: Pubkey,
    /// The winning router (a validator or a plain user).
    pub winner: Pubkey,
    pub user: Pubkey,
    /// How many requests contended.
    pub requests: u8,
}

// --- Phase 10: consensus-governed validator weights ---

#[event]
pub struct ValidatorWeightsUpdated {
    /// Number of validators whose weights were set (the full set; read the vector from Config).
    pub count: u8,
    pub updated_at: i64,
}

#[event]
pub struct HaltSet {
    pub halted: bool,
}
