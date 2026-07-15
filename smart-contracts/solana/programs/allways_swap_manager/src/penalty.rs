use anchor_lang::prelude::*;

use crate::error::ErrorCode;
use crate::events::MinerDeactivated;
use crate::state::MinerState;

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
    let actual = core::cmp::min(amount, current);
    if actual == 0 {
        return Ok(0);
    }
    miner_state.collateral = current.checked_sub(actual).ok_or(ErrorCode::Overflow)?;

    if miner_state.collateral < min_collateral && miner_state.active {
        miner_state.active = false;
        miner_state.deactivation_at = now;
        // Without this emit the scorer — which rebuilds the active set purely from
        // MinerActivated/MinerDeactivated events — keeps paying crown to a miner the chain
        // already considers inactive, until some later vote event happens to fire.
        emit!(MinerDeactivated { miner: miner_state.miner, at: now });
    }
    Ok(actual)
}
