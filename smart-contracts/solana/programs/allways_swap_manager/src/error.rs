use anchor_lang::prelude::*;

#[error_code]
pub enum ErrorCode {
    // --- Phase 1: collateral ---
    #[msg("Amount must be greater than zero")]
    InvalidAmount,
    #[msg("Deposit would exceed the configured max collateral")]
    ExceedsMaxCollateral,
    #[msg("Insufficient collateral for this withdrawal")]
    InsufficientCollateral,
    #[msg("Miner is active; deactivate before withdrawing")]
    MinerActive,
    #[msg("Miner has an in-flight swap; cannot proceed")]
    MinerHasActiveSwap,
    #[msg("Withdrawal cooldown after deactivation has not elapsed")]
    WithdrawCooldownActive,
    #[msg("Arithmetic overflow")]
    Overflow,

    // --- Phase 2: consensus ---
    #[msg("Signer is not a whitelisted validator")]
    NotValidator,
    #[msg("Validator is already whitelisted")]
    ValidatorExists,
    #[msg("Validator set is full")]
    ValidatorSetFull,
    #[msg("Validator not found in the set")]
    ValidatorNotFound,
    #[msg("Consensus threshold must be 1..=100")]
    InvalidThreshold,
    #[msg("Validator has already voted in this round")]
    AlreadyVoted,
    #[msg("Vote round parameters do not match the open round")]
    VoteHashMismatch,
    #[msg("Miner is already active")]
    MinerAlreadyActive,
    #[msg("Miner is not active")]
    MinerNotActive,
    #[msg("Only the miner may call this")]
    NotMiner,
    #[msg("No validators configured")]
    NoValidators,
    #[msg("System is halted")]
    SystemHalted,

    // --- Phase 3: reservations ---
    #[msg("Swap amount is below the configured minimum")]
    AmountBelowMin,
    #[msg("Swap amount is above the configured maximum")]
    AmountAboveMax,
    #[msg("Config bounds are contradictory (min must not exceed max)")]
    InvalidBounds,
    #[msg("Miner already has an active reservation")]
    MinerReserved,
    #[msg("No active reservation for this miner")]
    NoReservation,
    #[msg("String exceeds its maximum stored length")]
    StringTooLong,

    // --- Phase 4: swap lifecycle ---
    #[msg("swap_key does not match keccak(from_tx_hash)")]
    SwapKeyMismatch,
    #[msg("Source transaction has already been used")]
    DuplicateSourceTx,
    #[msg("user_from_address does not match the reservation")]
    UserMismatch,
    #[msg("Swap is not in the required status for this action")]
    InvalidStatus,
    #[msg("Swap has not reached its timeout yet")]
    NotTimedOut,
    #[msg("from_chain/to_chain args do not match the swap")]
    ChainMismatch,
    #[msg("Extension target must be later than the current deadline")]
    ExtensionNotLater,
    #[msg("Extension target exceeds the deadline's absolute ceiling")]
    ExtensionExceedsCeiling,

    // --- Phase 6: treasury ---
    #[msg("Withdrawal exceeds the accrued treasury balance")]
    InsufficientTreasury,

    // --- Phase 8: miner quotes ---
    #[msg("from_chain and to_chain must differ")]
    SameChain,
    #[msg("A required string field is empty")]
    EmptyField,

    // --- Phase 9: reservation lottery ---
    #[msg("Miner has an open reservation pool for a different pair; wait for it to resolve")]
    MinerBusyDifferentPair,
    #[msg("Miner has an open reservation pool; cannot proceed")]
    MinerBusy,
    #[msg("Pool window has closed; resolve it before requesting again")]
    PoolClosed,
    #[msg("Pool window has not closed yet")]
    PoolNotClosed,
    #[msg("Validator already has a request in this pool")]
    AlreadyRequested,
    #[msg("Pool has no requests to resolve")]
    NoRequests,

    // --- Phase 10: consensus validator weights ---
    #[msg("Weights vector length must match the validator set")]
    InvalidWeightsVector,
    #[msg("Minimum interval between weight updates has not elapsed")]
    WeightsUpdateTooSoon,
}
