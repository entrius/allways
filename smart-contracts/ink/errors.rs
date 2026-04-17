use scale::{Decode, Encode};

/// Errors that can occur during contract execution
#[derive(Debug, PartialEq, Eq, Encode, Decode)]
#[cfg_attr(feature = "std", derive(scale_info::TypeInfo))]
pub enum Error {
    /// Caller is not the contract owner
    NotOwner,
    /// Insufficient collateral to cover swap volume
    InsufficientCollateral,
    /// Swap ID not found
    SwapNotFound,
    /// Validator has already voted on this swap
    AlreadyVoted,
    /// Swap is not in the expected status for this operation
    InvalidStatus,
    /// Amount must be greater than zero
    ZeroAmount,
    /// Miner is not active (not activated via set_active)
    MinerNotActive,
    /// Miner is still active (must deactivate before withdrawing)
    MinerStillActive,
    /// Transfer failed
    TransferFailed,
    /// Swap has not timed out yet
    NotTimedOut,
    /// Caller is not the assigned miner for this swap
    NotAssignedMiner,
    /// Caller is not a registered validator
    NotValidator,
    /// Source transaction hash already used in another swap
    DuplicateSourceTx,
    /// Swap amounts must be greater than zero
    InvalidAmount,
    /// No pending slash to claim
    NoPendingSlash,
    /// Required input is empty
    InputEmpty,
    /// Input string exceeds maximum allowed length
    InputTooLong,
    /// Miner already has an active swap
    MinerHasActiveSwap,
    /// Withdrawal cooldown not met (must wait 2 * fulfillment_timeout after deactivation)
    WithdrawalCooldown,
    /// Swap amount below minimum
    AmountBelowMinimum,
    /// Swap amount above maximum
    AmountAboveMaximum,
    /// Miner is already reserved by another user
    MinerReserved,
    /// No active reservation exists for this miner (expired or never created)
    NoReservation,
    /// Collateral exceeds maximum allowed
    ExceedsMaxCollateral,
    /// Provided request hash does not match computed hash from data
    HashMismatch,
    /// A pending vote exists for a different request (hash conflict)
    PendingConflict,
    /// Source and destination chains must be different
    SameChain,
    /// System is halted — no new activity allowed
    SystemHalted,
    /// Miner collateral meets or exceeds floor; vote_deactivate not applicable
    SufficientCollateral,
    /// Miner has an active swap; self-deactivation blocked until swap resolves
    HasActiveSwap,
    /// Miner is currently reserved; self-deactivation blocked until reservation expires
    CurrentlyReserved,
}
