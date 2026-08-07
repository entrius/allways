use scale::{Decode, Encode};

/// Errors for the bond vault.
// allow: the SCALE Encode derive's variant-index cast trips clippy's
// cast_possible_truncation on data-carrying variants (ChainExtension(u32)).
#[allow(clippy::cast_possible_truncation)]
#[derive(Debug, PartialEq, Eq, Encode, Decode)]
#[cfg_attr(feature = "std", derive(scale_info::TypeInfo))]
pub enum Error {
    /// Caller is not the contract owner
    NotOwner,
    /// Caller is not a registered validator
    NotValidator,
    /// System is halted — no new bonds or locks
    SystemHalted,
    /// Validator has already voted on this round
    AlreadyVoted,
    /// A pending round exists with a different hash for this key
    PendingConflict,
    /// Amount must be greater than zero
    InvalidAmount,
    /// Bond below the minimum required for this operation
    InsufficientCollateral,
    /// Bond would exceed the maximum allowed
    ExceedsMaxCollateral,
    /// Bond is locked — deactivate on Solana and wait for vote_unlock
    BondLocked,
    /// Bond is already in the requested lock state
    LockStateUnchanged,
    /// Vote's epoch does not match the miner's current lock_epoch
    EpochMismatch,
    /// swap_ref already slashed (permanent replay marker)
    AlreadySlashed,
    /// Reimbursement exceeds the penalty
    InvalidReimbursement,
    /// No pending slash payout to claim
    NoPendingSlash,
    /// Native transfer failed
    TransferFailed,
    /// Chain extension returned a nonzero status — carries subtensor's
    /// `Output` error code verbatim for off-chain diagnosis
    ChainExtension(u32),
    /// Fee-settle batch is empty or exceeds MAX_BATCH entries
    InvalidBatch,
    /// Operation invalid in the current state
    InvalidStatus,
}
