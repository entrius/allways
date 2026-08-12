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
    pub collateral_amount: u64,
    pub from_amount: u128,
    pub to_amount: u128,
    pub initiated_at: i64,
    /// Backing hub (v3.1: appended last — one live swap PER hub, so per-miner events are ambiguous
    /// without it; prefix decoders keep working).
    pub collateral_chain: String,
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
    pub collateral_amount: u64,
    /// Absolute protocol fee for this swap, in the BACKING asset's smallest unit. For "sol" it is what
    /// actually moved into the treasury (clamped to available collateral); for a backing that settles
    /// elsewhere nothing moved here and this is what that chain's fee ledger owes.
    pub fee: u64,
    /// Direction + realized leg amounts + executed rate, for off-chain per-swap history (so indexers
    /// don't re-read the now-closed Swap). Feeds the realized volume/VWAP track record (A2).
    pub from_chain: String,
    pub to_chain: String,
    pub from_amount: u128,
    pub to_amount: u128,
    /// Fixed-point executed rate (display_rate × RATE_PRECISION); matches the on-chain u128 (#495).
    pub rate: u128,
    /// Backing chain this swap declared. `fee` above is the absolute protocol fee for the swap; the
    /// pair (backing, fee) is the whole input the fee relay needs to credit the right ledger.
    pub collateral_chain: String,
}

/// The timeout verdict. Self-contained by design — the slash relay must never reconstruct state, so
/// every figure here is absolute and the backing says which chain applies it.
#[event]
pub struct SwapTimedOut {
    pub swap_key: [u8; 32],
    pub miner: Pubkey,
    pub collateral_amount: u64,
    /// Collateral slashed locally and refunded to the user (lamports). 0 when the backing settles
    /// off-chain — `penalty`/`reimbursement` are then the figures the backing chain owes.
    pub slash: u64,
    /// Backing chain this swap declared ("sol" = settled atomically above).
    pub collateral_chain: String,
    /// Penalty owed, in the backing asset's smallest unit: 1.10× the swap size, pre-clamp (the purse
    /// holding the bond does the clamping). Absolute, never a delta.
    pub penalty: u64,
    /// Share of `penalty` owed to the wronged user; the surplus (if any) is protocol revenue.
    pub reimbursement: u64,
    /// Who the backing chain owes `reimbursement`: the user's own address on `collateral_chain`, so
    /// the seizure can be relayed from this event alone. Empty when the backing settles locally —
    /// that refund already moved, to `Swap::user`.
    pub payee: String,
}

/// A validator slid a reservation/swap deadline forward (single-validator, no quorum). Carries the
/// new deadline (post-value) so consumers set an absolute, not a delta.
#[event]
pub struct ReservationExtended {
    pub miner: Pubkey,
    pub validator: Pubkey,
    pub reserved_until: i64,
    /// Backing hub whose reservation slid (v3.1, appended last).
    pub collateral_chain: String,
}

#[event]
pub struct SwapTimeoutExtended {
    pub swap_key: [u8; 32],
    pub miner: Pubkey,
    pub validator: Pubkey,
    pub timeout_at: i64,
}

/// `mark_fulfilled` slid `timeout_at` forward to cover the destination chain's confirmation window
/// (so a miner who paid just before the deadline can't be slashed while the tx confirms). Post-value
/// like every deadline event. Separate from `SwapTimeoutExtended` — no validator is involved.
#[event]
pub struct FulfillmentGraceApplied {
    pub swap_key: [u8; 32],
    pub miner: Pubkey,
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
    /// The quote's declared backing — part of its identity, not a detail: the same miner may stand two
    /// quotes on one direction at different rates, and only this tells them apart in the log.
    pub collateral_chain: String,
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
    /// Which of the direction's quotes was retracted (see `QuoteSet::collateral_chain`).
    pub collateral_chain: String,
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
/// quorum, self-`deactivate`, or `apply_penalty`'s auto-deactivation (fee/slash dropping collateral
/// below the minimum — previously silent, which desynced event-driven scorers from chain state).
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

/// One backing's activation bit flipped (W2). Emitted on EVERY `vote_activate`/`vote_deactivate`
/// quorum, whereas MinerActivated/MinerDeactivated fire only when the OR view itself changes — so an
/// event-driven scorer replaying `active` from those two stays exactly as correct as before.
#[event]
pub struct MinerBackingChanged {
    pub miner: Pubkey,
    pub backing: String,
    /// The bit's new state; `active_backings` is the whole mask after this change.
    pub enabled: bool,
    pub active_backings: u8,
    pub at: i64,
}

/// A bond attestation was written by quorum. Absolute figures per the post-total convention — the
/// reconciler diffs these against the vault without re-reading Solana state.
#[event]
pub struct BondAttested {
    pub miner: Pubkey,
    pub chain: String,
    pub effective_balance: u64,
    pub locked: bool,
    pub epoch: u64,
    pub attested_at: i64,
}

/// The global attestation heartbeat advanced (the dead-man fuse's liveness signal).
#[event]
pub struct AttestHeartbeat {
    pub at: i64,
}

// --- Phase 9: reservation lottery (pool keyed per miner) ---

#[event]
pub struct PoolOpened {
    pub miner: Pubkey,
    pub opener: Pubkey,
    pub from_chain: String,
    pub to_chain: String,
    /// The pinned quote's backing — the contest is for one offer, and which purse is on the hook is
    /// part of that offer. `resolve_pool` copies it straight into the Reservation.
    pub collateral_chain: String,
    pub closes_at: i64,
    pub seed_slot: u64,
}

#[event]
pub struct ReservationRequested {
    pub miner: Pubkey,
    pub router: Pubkey,
    /// Number of bids in the pool after this one.
    pub requests: u8,
}

/// The seat winner filled its reservation: taker + amounts named, `reserved_until` set. Carries the
/// fill data the indexer needs for `active_reservations` (formerly on `ReservationRequested`).
#[event]
pub struct ReservationFilled {
    pub miner: Pubkey,
    pub router: Pubkey,
    pub user: Pubkey,
    pub from_chain: String,
    pub to_chain: String,
    pub collateral_amount: u64,
    pub from_amount: u128,
    pub to_amount: u128,
    pub reserved_until: i64,
    /// Backing hub this fill draws on (v3.1, appended last).
    pub collateral_chain: String,
}

/// An unfilled reservation was reaped after its finalize deadline (miner freed, fee already sunk).
#[event]
pub struct UnfilledReservationClosed {
    pub miner: Pubkey,
    pub router: Pubkey,
    /// Backing hub whose slot was reaped (v3.1, appended last).
    pub collateral_chain: String,
}

/// The closed pool's draw seed slot has been pinned to a not-yet-produced slot. Re-emitted if that
/// slot rolls out of SlotHashes before any crank resolves (a stall long enough to lose ~512 slots).
#[event]
pub struct PoolDrawArmed {
    pub miner: Pubkey,
    pub seed_slot: u64,
    /// Backing hub whose contest armed (v3.1, appended last).
    pub collateral_chain: String,
}

#[event]
pub struct PoolResolved {
    pub miner: Pubkey,
    /// The winning router (a validator or a plain user) — the seat winner that may finalize.
    pub winner: Pubkey,
    /// How many bids contended.
    pub requests: u8,
    /// Backing hub whose contest resolved (v3.1, appended last).
    pub collateral_chain: String,
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
