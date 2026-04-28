use ink::primitives::AccountId;

/// Event emitted when a miner posts collateral
#[ink::event]
pub struct CollateralPosted {
    #[ink(topic)]
    pub miner: AccountId,
    pub amount: u128,
    pub total: u128,
}

/// Event emitted when a miner withdraws collateral
#[ink::event]
pub struct CollateralWithdrawn {
    #[ink(topic)]
    pub miner: AccountId,
    pub amount: u128,
    pub remaining: u128,
}

/// Event emitted when a validator initiates a swap (source tx pre-verified)
#[ink::event]
pub struct SwapInitiated {
    #[ink(topic)]
    pub swap_id: u64,
    #[ink(topic)]
    pub user: AccountId,
    #[ink(topic)]
    pub miner: AccountId,
    pub from_amount: u128,
    pub initiated_block: u32,
}

/// Event emitted when a miner marks a swap as fulfilled
#[ink::event]
pub struct SwapFulfilled {
    #[ink(topic)]
    pub swap_id: u64,
    #[ink(topic)]
    pub miner: AccountId,
    pub to_tx_hash: ink::prelude::string::String,
}

/// Event emitted when validators confirm swap completion
#[ink::event]
pub struct SwapCompleted {
    #[ink(topic)]
    pub swap_id: u64,
    #[ink(topic)]
    pub miner: AccountId,
    pub tao_amount: u128,
    pub fee_amount: u128,
}

/// Event emitted when validators confirm swap timeout
#[ink::event]
pub struct SwapTimedOut {
    #[ink(topic)]
    pub swap_id: u64,
    #[ink(topic)]
    pub miner: AccountId,
    pub tao_amount: u128,
    pub slash_amount: u128,
}

/// Event emitted when collateral is slashed
#[ink::event]
pub struct CollateralSlashed {
    #[ink(topic)]
    pub miner: AccountId,
    pub amount: u128,
    pub recipient: AccountId,
}

/// Event emitted when a user claims their pending slash payout
#[ink::event]
pub struct SlashClaimed {
    #[ink(topic)]
    pub swap_id: u64,
    #[ink(topic)]
    pub user: AccountId,
    pub amount: u128,
}

/// Event emitted when a slash transfer fails and is stored for later claim
#[ink::event]
pub struct SlashPending {
    #[ink(topic)]
    pub swap_id: u64,
    #[ink(topic)]
    pub user: AccountId,
    pub amount: u128,
}

/// Event emitted when a validator casts a vote
#[ink::event]
pub struct VoteCast {
    #[ink(topic)]
    pub swap_id: u64,
    #[ink(topic)]
    pub validator: AccountId,
    pub vote_type: super::types::VoteType,
    pub vote_count: u32,
}

/// Event emitted when a validator is added or removed
#[ink::event]
pub struct ValidatorUpdated {
    #[ink(topic)]
    pub validator: AccountId,
    pub registered: bool,
}

/// Event emitted when contract configuration changes
#[ink::event]
pub struct ConfigUpdated {
    pub key: ink::prelude::string::String,
    pub value: u128,
}

/// Event emitted when accumulated fees are recycled (transferred to recycle address)
#[ink::event]
pub struct FeesRecycled {
    pub tao_amount: u128,
}

/// Event emitted when ownership is transferred
#[ink::event]
pub struct OwnershipTransferred {
    #[ink(topic)]
    pub previous_owner: AccountId,
    #[ink(topic)]
    pub new_owner: AccountId,
}

/// Event emitted when a miner is activated or deactivated
#[ink::event]
pub struct MinerActivated {
    #[ink(topic)]
    pub miner: AccountId,
    pub active: bool,
}

/// Event emitted when a miner is reserved for a swap
#[ink::event]
pub struct MinerReserved {
    #[ink(topic)]
    pub miner: AccountId,
    pub reserved_until: u32,
}

/// Event emitted when a miner reservation is extended (BTC confirmation wait)
#[ink::event]
pub struct ReservationExtended {
    #[ink(topic)]
    pub miner: AccountId,
    pub reserved_until: u32,
}

/// Event emitted when a swap timeout is extended (dest tx confirmation wait)
#[ink::event]
pub struct SwapTimeoutExtended {
    #[ink(topic)]
    pub swap_id: u64,
    pub new_timeout_block: u32,
}

/// Event emitted when a miner reservation is cancelled
#[ink::event]
pub struct ReservationCancelled {
    #[ink(topic)]
    pub miner: AccountId,
}

// ─── Optimistic extensions ─────────────────────────────────────────────────
// Six events split by side (reservation vs timeout) so downstream indexers get
// per-entity schemas without polymorphic keys. See OPTIMISTIC_EXTENSION_REDESIGN.md §4.3.

/// Reservation extension proposed by a validator (single-validator, optimistic).
#[ink::event]
pub struct ReservationExtensionProposed {
    #[ink(topic)]
    pub miner: AccountId,
    /// Source-tx hash this proposal correlates to. Lets indexers tie the
    /// extension activity back to the swap attempt without a join.
    pub from_tx_hash: ink::primitives::Hash,
    pub target_block: u32,
    #[ink(topic)]
    pub by: AccountId,
}

/// Reservation extension challenged within the challenge window. The pending
/// entry is deleted; any validator may re-propose immediately.
#[ink::event]
pub struct ReservationExtensionChallenged {
    #[ink(topic)]
    pub miner: AccountId,
    /// What target was being claimed; useful for "how off?" analytics.
    pub voided_target: u32,
    #[ink(topic)]
    pub by: AccountId,
}

/// Reservation extension finalized — `reserved_until` is now `applied_target`.
#[ink::event]
pub struct ReservationExtensionFinalized {
    #[ink(topic)]
    pub miner: AccountId,
    pub applied_target: u32,
    #[ink(topic)]
    pub by: AccountId,
}

/// Fulfillment-timeout extension proposed by a validator.
#[ink::event]
pub struct TimeoutExtensionProposed {
    #[ink(topic)]
    pub swap_id: u64,
    pub target_block: u32,
    #[ink(topic)]
    pub by: AccountId,
}

/// Fulfillment-timeout extension challenged within the challenge window.
#[ink::event]
pub struct TimeoutExtensionChallenged {
    #[ink(topic)]
    pub swap_id: u64,
    pub voided_target: u32,
    #[ink(topic)]
    pub by: AccountId,
}

/// Fulfillment-timeout extension finalized — `timeout_block` is now
/// `applied_target`.
#[ink::event]
pub struct TimeoutExtensionFinalized {
    #[ink(topic)]
    pub swap_id: u64,
    pub applied_target: u32,
    #[ink(topic)]
    pub by: AccountId,
}
