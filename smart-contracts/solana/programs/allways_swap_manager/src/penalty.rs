use anchor_lang::prelude::*;

use crate::constants::{BACKING_BIT_SOL, BACKING_BIT_TAO, BACKING_CHAIN_SOL, BACKING_CHAIN_TAO};
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
    let purse = miner_state.collateral;
    if drop_backing_under_floor(miner_state, BACKING_BIT_SOL, BACKING_CHAIN_SOL, purse, min_collateral, now) {
        // The SOL bit just dropped, so the local-collateral cooldown starts here — same rule as
        // self-`deactivate`. Gating it on `!still_active` instead would let a slashed dual miner idle
        // the cooldown out on its TAO purse and withdraw the moment it drops the SOL one.
        miner_state.deactivation_at = now;
    }
    Ok(actual)
}

/// Clear one hub's backing bit when its purse sits under that hub's activation floor — the single
/// auto-deactivation rule every purse shares. The SOL purse reaches it through `apply_penalty` (the
/// local fee/slash path); a vaulted purse (TAO) reaches it through `vote_set_attestation`, the only
/// place its balance lands on Solana. Without the latter a slashed-under-floor TAO bond stayed active
/// forever: unreservable by takers (`open_or_request` checks the floor) yet still a crown candidate.
/// Returns whether the bit was dropped. Emits `MinerBackingChanged`, and `MinerDeactivated` when no
/// purse is left — the scorer rebuilds its active set purely from these events.
pub fn drop_backing_under_floor(
    miner_state: &mut MinerState,
    bit: u8,
    chain: &str,
    purse: u64,
    floor: u64,
    now: i64,
) -> bool {
    if purse >= floor || miner_state.active_backings & bit == 0 {
        return false;
    }
    let still_active = miner_state.set_backing(bit, false);
    emit!(MinerBackingChanged {
        miner: miner_state.miner,
        backing: chain.to_string(),
        enabled: false,
        active_backings: miner_state.active_backings,
        at: now,
    });
    if !still_active {
        // Without this emit the scorer — which rebuilds the active set purely from
        // MinerActivated/MinerDeactivated events — keeps paying crown to a miner the chain
        // already considers inactive, until some later vote event happens to fire.
        emit!(MinerDeactivated { miner: miner_state.miner, at: now });
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constants::MAX_BACKING_SLOTS;

    fn miner(active_backings: u8) -> MinerState {
        MinerState {
            miner: Pubkey::default(),
            collateral: 0,
            active: active_backings != 0,
            active_backings,
            has_active_swap: false,
            active_swap_backings: 0,
            busy_until: [0; MAX_BACKING_SLOTS],
            settling_until: [0; MAX_BACKING_SLOTS],
            reserved_collateral: [0; MAX_BACKING_SLOTS],
            deactivation_at: 0,
            successful_swaps: 0,
            failed_swaps: 0,
            bump: 255,
        }
    }

    #[test]
    fn an_under_floor_tao_purse_drops_only_the_tao_bit() {
        let mut ms = miner(BACKING_BIT_SOL | BACKING_BIT_TAO);
        assert!(drop_backing_under_floor(&mut ms, BACKING_BIT_TAO, BACKING_CHAIN_TAO, 890_000_000, 1_000_000_000, 7));
        assert_eq!(ms.active_backings, BACKING_BIT_SOL);
        assert!(ms.active, "the SOL purse keeps the miner active");
        assert_eq!(ms.deactivation_at, 0, "the local cooldown is a SOL-purse rule; the caller sets it");
    }

    #[test]
    fn the_last_purse_under_floor_deactivates_the_miner() {
        let mut ms = miner(BACKING_BIT_TAO);
        assert!(drop_backing_under_floor(&mut ms, BACKING_BIT_TAO, BACKING_CHAIN_TAO, 0, 1, 7));
        assert_eq!(ms.active_backings, 0);
        assert!(!ms.active);
    }

    #[test]
    fn at_or_above_floor_is_a_no_op() {
        let mut ms = miner(BACKING_BIT_TAO);
        assert!(!drop_backing_under_floor(&mut ms, BACKING_BIT_TAO, BACKING_CHAIN_TAO, 1_000_000_000, 1_000_000_000, 7));
        assert_eq!(ms.active_backings, BACKING_BIT_TAO);
    }

    #[test]
    fn an_already_inactive_bit_is_a_no_op() {
        let mut ms = miner(BACKING_BIT_SOL);
        assert!(!drop_backing_under_floor(&mut ms, BACKING_BIT_TAO, BACKING_CHAIN_TAO, 0, 1, 7));
        assert_eq!(ms.active_backings, BACKING_BIT_SOL);
    }

    #[test]
    fn a_zero_floor_never_drops() {
        // Floor unset (0) mirrors the contract's "no floor" sentinel: nothing is under it.
        let mut ms = miner(BACKING_BIT_TAO);
        assert!(!drop_backing_under_floor(&mut ms, BACKING_BIT_TAO, BACKING_CHAIN_TAO, 0, 0, 7));
    }
}
