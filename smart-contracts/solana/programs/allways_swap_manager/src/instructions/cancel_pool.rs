use anchor_lang::prelude::*;

use crate::constants::{CONFIG_SEED, MINER_SEED, POOL_SEED};
use crate::error::ErrorCode;
use crate::events::PoolCancelled;
use crate::state::{Config, MinerState, Pool};

/// Admin resets a stuck/abandoned open pool and frees the miner's busy lock (review #4). Fees already
/// collected stay in the treasury (non-refundable); the pool is reset, not closed (rent parked for
/// reuse). An open pool implies no reservation has been drawn yet, so clearing `busy_until` is safe.
#[derive(Accounts)]
pub struct CancelPool<'info> {
    pub admin: Signer<'info>,

    #[account(seeds = [CONFIG_SEED], bump = config.bump, has_one = admin)]
    pub config: Account<'info, Config>,

    /// CHECK: identified by address only; used in PDA seeds + the miner_state constraint.
    pub miner: UncheckedAccount<'info>,

    #[account(
        mut,
        seeds = [MINER_SEED, miner.key().as_ref()],
        bump = miner_state.bump,
        constraint = miner_state.miner == miner.key(),
    )]
    pub miner_state: Account<'info, MinerState>,

    #[account(mut, seeds = [POOL_SEED, miner.key().as_ref()], bump = pool.bump)]
    pub pool: Account<'info, Pool>,
}

pub fn handler(ctx: Context<CancelPool>) -> Result<()> {
    // Only an open pool is cancellable; once resolve_pool runs it resets opened_at and owns busy_until.
    require!(ctx.accounts.pool.opened_at != 0, ErrorCode::NoRequests);

    let pool = &mut ctx.accounts.pool;
    pool.opened_at = 0;
    pool.requests.clear();

    // The open set busy_until to window + reservation TTL; clear it so the miner is freed now.
    ctx.accounts.miner_state.busy_until = 0;

    emit!(PoolCancelled { miner: ctx.accounts.miner.key() });
    Ok(())
}
