use anchor_lang::prelude::*;

use crate::backing;
use crate::constants::{BACKING_BIT_SOL, MINER_SEED};
use crate::error::ErrorCode;
use crate::events::{MinerBackingChanged, MinerDeactivated};
use crate::state::MinerState;

/// Miner self-deactivation (no consensus), either of ONE purse or of all of them. Guards read entirely
/// from `MinerState` (the mandatory account) so they can't be skipped: caller is the miner, no in-flight
/// swap, and past `busy_until` — all global, because one-swap-at-a-time spans purses.
#[derive(Accounts)]
pub struct Deactivate<'info> {
    pub miner: Signer<'info>,

    #[account(
        mut,
        seeds = [MINER_SEED, miner.key().as_ref()],
        bump = miner_state.bump,
        has_one = miner,
    )]
    pub miner_state: Account<'info, MinerState>,
}

/// `backing = Some(chain)` drops that purse only (the miner keeps trading on the rest); `None` is the
/// full exit and drops every purse at once.
pub fn handler(ctx: Context<Deactivate>, backing: Option<String>) -> Result<()> {
    let now = Clock::get()?.unix_timestamp;
    let miner_key = ctx.accounts.miner.key();

    let dropped = match &backing {
        Some(chain) => backing::backing_bit(chain)?,
        None => ctx.accounts.miner_state.active_backings,
    };

    let ms = &mut ctx.accounts.miner_state;
    require!(ms.active, ErrorCode::MinerNotActive);
    require!(ms.active_backings & dropped != 0, ErrorCode::MinerNotActive);
    require!(!ms.has_active_swap, ErrorCode::MinerHasActiveSwap);
    require!(now >= ms.busy_until, ErrorCode::MinerBusy);

    // One MinerBackingChanged per purse actually going dark, whether this is a partial exit or the
    // full one — a scorer replaying the mask must see the same events either way.
    let mut still_active = ms.active;
    for (bit, chain) in crate::constants::BACKINGS {
        if dropped & bit == 0 || ms.active_backings & bit == 0 {
            continue;
        }
        still_active = ms.set_backing(bit, false);
        emit!(MinerBackingChanged {
            miner: miner_key,
            backing: chain.to_string(),
            enabled: false,
            active_backings: ms.active_backings,
            at: now,
        });
    }

    // The cooldown guards LOCAL collateral, so it starts when the SOL purse stops serving and not
    // before: a TAO-side exit is gated on the vault's own unlock path, and starting the clock there
    // would let a miner idle out the SOL cooldown without ever having dropped the SOL bit.
    if dropped & BACKING_BIT_SOL != 0 {
        ms.deactivation_at = now;
    }
    if !still_active {
        emit!(MinerDeactivated { miner: miner_key, at: now });
    }
    msg!("miner self-deactivated: {} (mask {})", miner_key, ms.active_backings);
    Ok(())
}
