use anchor_lang::prelude::*;

use crate::constants::{BACKING_BIT_SOL, BACKING_CHAIN_SOL};
use crate::error::ErrorCode;
use crate::events::{MinerBackingChanged, MinerDeactivated};
use crate::state::MinerState;

/// The share of `amount` a purse of `available` can actually cover. Split out of `apply_penalty` so a
/// backing settled off-chain can size the same penalty without touching local collateral — there the
/// purse isn't readable here, so the emitted figure is unclamped and the bond vault does the clamping.
pub fn penalty_against(available: u64, amount: u64) -> u64 {
    core::cmp::min(amount, available)
}

/// Deduct up to `amount` from the miner's collateral (clamped to available) and auto-deactivate the
/// miner if the remainder falls below `min_collateral`. Returns the actual amount deducted.
///
/// Lamports are NOT moved here — the caller moves `actual` out of the miner's per-miner collateral
/// vault (to the treasury on a confirm fee, or to the user on a slash), keeping that vault's
/// invariant (`lamports == rent + collateral`).
pub fn apply_penalty(
    miner_state: &mut Account<MinerState>,
    min_collateral: u64,
    amount: u64,
    now: i64,
) -> Result<u64> {
    let current = miner_state.collateral;
    let actual = penalty_against(current, amount);
    if actual == 0 {
        return Ok(0);
    }
    miner_state.collateral = current.checked_sub(actual).ok_or(ErrorCode::Overflow)?;

    // Only the SOL purse went deficient, so only its bit drops (D2: a deficient purse disables its own
    // quotes, not the miner). A miner still bonded on another hub keeps trading there.
    if miner_state.collateral < min_collateral && miner_state.active_backings & BACKING_BIT_SOL != 0 {
        let still_active = miner_state.set_backing(BACKING_BIT_SOL, false);
        emit!(MinerBackingChanged {
            miner: miner_state.miner,
            backing: BACKING_CHAIN_SOL.to_string(),
            enabled: false,
            active_backings: miner_state.active_backings,
            at: now,
        });
        if !still_active {
            miner_state.deactivation_at = now;
            // Without this emit the scorer — which rebuilds the active set purely from
            // MinerActivated/MinerDeactivated events — keeps paying crown to a miner the chain
            // already considers inactive, until some later vote event happens to fire.
            emit!(MinerDeactivated { miner: miner_state.miner, at: now });
        }
    }
    Ok(actual)
}
