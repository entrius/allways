use anchor_lang::prelude::*;

use crate::constants::MINER_SEED;
use crate::error::ErrorCode;
use crate::state::MinerState;

/// Miner self-deactivation (no consensus). Guards read entirely from `MinerState` (the mandatory
/// account) so they can't be skipped: caller is the miner, no in-flight swap, and past `busy_until`
/// (open pool / held reservation).
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

pub fn handler(ctx: Context<Deactivate>) -> Result<()> {
    let now = Clock::get()?.unix_timestamp;
    let ms = &mut ctx.accounts.miner_state;

    require!(ms.active, ErrorCode::MinerNotActive);
    require!(!ms.has_active_swap, ErrorCode::MinerHasActiveSwap);
    require!(now >= ms.busy_until, ErrorCode::MinerBusy);

    ms.active = false;
    ms.deactivation_at = now;
    msg!("miner self-deactivated: {}", ctx.accounts.miner.key());
    Ok(())
}
