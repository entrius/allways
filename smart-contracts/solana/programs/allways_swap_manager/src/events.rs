use anchor_lang::prelude::*;

/// Collateral-affecting events carry the **resulting total**, not a blind delta — so any consumer
/// can set an absolute baseline from a single event instead of accumulating from an unknown start
/// (v2 cleanup #5; the missing-baseline footgun behind the v1.0.9 mass-recycle outage). Phase 4's
/// fee/slash events MUST follow the same post-total rule.
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
}

#[event]
pub struct SwapTimedOut {
    pub swap_key: [u8; 32],
    pub miner: Pubkey,
    pub sol_amount: u64,
    /// Collateral slashed and refunded to the user (lamports).
    pub slash: u64,
}

// --- Phase 6: treasury ---

#[event]
pub struct TreasuryWithdrawn {
    pub recipient: Pubkey,
    /// Amount withdrawn this call (lamports).
    pub amount: u64,
    /// Treasury balance remaining after this call (lamports) — post-total per the §5 convention.
    pub total: u64,
}

// --- Phase 8: miner quotes (one per (miner, from_chain, to_chain)) ---

#[event]
pub struct QuoteSet {
    pub miner: Pubkey,
    pub from_chain: String,
    pub to_chain: String,
    pub rate: String,
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
    pub validator: Pubkey,
    pub user: Pubkey,
    /// Number of requests in the pool after this one.
    pub requests: u8,
}

#[event]
pub struct PoolResolved {
    pub miner: Pubkey,
    /// The validator whose request won the draw.
    pub winner: Pubkey,
    pub user: Pubkey,
    /// How many requests contended.
    pub requests: u8,
}

#[event]
pub struct PoolCancelled {
    pub miner: Pubkey,
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
