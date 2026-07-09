//! Shared Config-field validators, called by BOTH `initialize` and the admin setters so the two
//! write paths to a given field can't enforce divergent rules (review #8). One rule, one home.

use anchor_lang::prelude::*;

use crate::constants::{MAX_TOTAL_EXTENSION_SECS_MAX, MAX_TOTAL_EXTENSION_SECS_MIN};
use crate::error::ErrorCode;

/// Quorum threshold as a percent of the validator set.
pub fn consensus_threshold(percent: u8) -> Result<()> {
    require!((1..=100).contains(&percent), ErrorCode::InvalidThreshold);
    Ok(())
}

/// Swap fulfillment timeout floor (seconds) — long enough to be meaningful on any chain.
pub fn fulfillment_timeout(secs: i64) -> Result<()> {
    require!(secs >= 60, ErrorCode::InvalidAmount);
    Ok(())
}

/// Reservation hold must be a positive duration.
pub fn reservation_ttl(secs: i64) -> Result<()> {
    require!(secs > 0, ErrorCode::InvalidAmount);
    Ok(())
}

/// Lottery pooling window must be a positive duration.
pub fn pool_window(secs: i64) -> Result<()> {
    require!(secs > 0, ErrorCode::InvalidAmount);
    Ok(())
}

/// Anti-thrash floor between weight updates (0 disables).
pub fn weights_update_min_interval(secs: i64) -> Result<()> {
    require!(secs >= 0, ErrorCode::InvalidAmount);
    Ok(())
}

/// Min swap size: 0 = unbounded, else a sane dust floor.
pub fn min_swap_amount(amount: u64) -> Result<()> {
    require!(amount == 0 || amount >= 1000, ErrorCode::InvalidAmount);
    Ok(())
}

/// Swap-size bounds must not be contradictory (0 on either side = unbounded).
pub fn swap_bounds(min: u64, max: u64) -> Result<()> {
    require!(min == 0 || max == 0 || min <= max, ErrorCode::InvalidBounds);
    Ok(())
}

/// Collateral bounds must not be contradictory (max 0 = no cap).
pub fn collateral_bounds(min: u64, max: u64) -> Result<()> {
    require!(max == 0 || min <= max, ErrorCode::InvalidBounds);
    Ok(())
}

/// Total deadline-extension budget, clamped to [30 min, 140 min]: the floor keeps it above one BTC
/// block; the ceiling caps how long a miner can be held, since it's the only such bound.
pub fn max_total_extension(secs: i64) -> Result<()> {
    require!(
        (MAX_TOTAL_EXTENSION_SECS_MIN..=MAX_TOTAL_EXTENSION_SECS_MAX).contains(&secs),
        ErrorCode::InvalidAmount
    );
    Ok(())
}
