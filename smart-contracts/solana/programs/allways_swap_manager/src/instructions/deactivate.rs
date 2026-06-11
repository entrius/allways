use anchor_lang::prelude::*;

use crate::constants::{MINER_SEED, RESV_SEED};
use crate::error::ErrorCode;
use crate::state::{MinerState, Reservation};

/// Miner self-deactivation (no consensus). Guards: caller is the miner, no in-flight swap,
/// no active reservation.
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

    /// Optional: pass the miner's reservation if one exists, so the active-reservation guard runs.
    #[account(seeds = [RESV_SEED, miner.key().as_ref()], bump)]
    pub reservation: Option<Account<'info, Reservation>>,
}

pub fn handler(ctx: Context<Deactivate>) -> Result<()> {
    require!(ctx.accounts.miner_state.active, ErrorCode::MinerNotActive);
    require!(
        !ctx.accounts.miner_state.has_active_swap,
        ErrorCode::MinerHasActiveSwap
    );

    let now = Clock::get()?.unix_timestamp;

    if let Some(resv) = &ctx.accounts.reservation {
        let active_reservation = resv.reserved_until != 0 && resv.reserved_until >= now;
        require!(!active_reservation, ErrorCode::MinerReserved);
    }

    ctx.accounts.miner_state.active = false;
    ctx.accounts.miner_state.deactivation_at = now;
    msg!("miner self-deactivated: {}", ctx.accounts.miner.key());
    Ok(())
}
