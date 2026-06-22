use anchor_lang::prelude::*;

use crate::error::ErrorCode;
use crate::state::{MinerState, Vault};

/// Deduct up to `amount` from the miner's collateral (clamped to available), shrink the vault's
/// collateral total accordingly, and auto-deactivate the miner if the remainder falls below
/// `min_collateral`. Returns the actual amount deducted. Mirrors ink! `apply_collateral_penalty`.
///
/// Lamports are NOT moved here — the caller decides where the deducted value goes (a fee moved to the
/// treasury PDA on confirm, vs. a payout to the user on a slash), preserving the collateral-vault
/// invariant (`lamports == rent + total_collateral`).
pub fn apply_penalty(
    miner_state: &mut Account<MinerState>,
    vault: &mut Account<Vault>,
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
    vault.total_collateral = vault
        .total_collateral
        .checked_sub(actual)
        .ok_or(ErrorCode::Overflow)?;

    if miner_state.collateral < min_collateral && miner_state.active {
        miner_state.active = false;
        miner_state.deactivation_at = now;
    }
    Ok(actual)
}
