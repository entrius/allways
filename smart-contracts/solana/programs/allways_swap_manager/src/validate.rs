//! Shared Config-field validators, called by BOTH `initialize` and the admin setters so the two
//! write paths to a given field can't enforce divergent rules (review #8). One rule, one home.

use anchor_lang::prelude::*;

use crate::constants::{
    FINALIZE_WINDOW_SECS_MAX, FINALIZE_WINDOW_SECS_MIN, MAX_TOTAL_EXTENSION_SECS_MAX,
    MAX_TOTAL_EXTENSION_SECS_MIN, RESERVATION_FEE_LAMPORTS_MIN,
};
use crate::error::ErrorCode;

/// Chain ids must be lowercase at intake. The program's chain-identity sites disagree on ASCII
/// casing (the grace table matches case-insensitively, the numeraire collateral bind and the
/// quote/pool PDA seeds are byte-exact), so "SOL" aliases "sol" in some sites while acting as a
/// different chain in the money-critical ones — silently sizing collateral against the wrong leg.
/// This gate at the two intake doors kills the ASCII-casing alias class; any other unknown string
/// (typos, homoglyphs) is simply a distinct id that matches no real quote and dies unfunded.
pub fn chain_ids_lowercase(from_chain: &str, to_chain: &str) -> Result<()> {
    let lower = |s: &str| !s.bytes().any(|b| b.is_ascii_uppercase());
    require!(lower(from_chain) && lower(to_chain), ErrorCode::ChainNotLowercase);
    Ok(())
}

/// Quorum threshold as a percent of the validator set. Floored at a majority: anything below 51 lets
/// a single validator (or a sub-majority clique) pass votes alone once the set has >1 member.
pub fn consensus_threshold(percent: u8) -> Result<()> {
    require!((51..=100).contains(&percent), ErrorCode::InvalidThreshold);
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

/// Post-draw finalize window, clamped to [15s, 300s]: long enough for the winner's internal auction +
/// tx landing, short enough that parking a miner unfilled isn't cheap.
pub fn finalize_window(secs: i64) -> Result<()> {
    require!(
        (FINALIZE_WINDOW_SECS_MIN..=FINALIZE_WINDOW_SECS_MAX).contains(&secs),
        ErrorCode::InvalidAmount
    );
    Ok(())
}

/// Anti-thrash floor between weight updates (0 disables).
pub fn weights_update_min_interval(secs: i64) -> Result<()> {
    require!(secs >= 0, ErrorCode::InvalidAmount);
    Ok(())
}

/// Min swap size: always at least a dust floor. No 0-means-unbounded escape — a zero minimum lets
/// dust-sized swaps whose 1% fee truncates to nothing through, and it's an anti-grief brake the
/// admin key must not be able to release.
pub fn min_swap_amount(amount: u64) -> Result<()> {
    require!(amount >= 1000, ErrorCode::InvalidAmount);
    Ok(())
}

/// Reservation fee: floored, never zero — it's the only cost on opening a pool, which busies a miner
/// for the whole window + finalize + TTL.
pub fn reservation_fee(lamports: u64) -> Result<()> {
    require!(lamports >= RESERVATION_FEE_LAMPORTS_MIN, ErrorCode::InvalidAmount);
    Ok(())
}

/// Swap-size bounds must not be contradictory (max 0 = unbounded above; min has a hard floor).
pub fn swap_bounds(min: u64, max: u64) -> Result<()> {
    require!(max == 0 || min <= max, ErrorCode::InvalidBounds);
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
