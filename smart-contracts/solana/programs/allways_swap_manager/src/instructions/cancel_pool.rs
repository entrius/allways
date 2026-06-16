use anchor_lang::prelude::*;

use crate::constants::{CONFIG_SEED, POOL_SEED};
use crate::events::PoolCancelled;
use crate::state::{Config, Pool};

/// Admin resets a stuck/abandoned pool (e.g. the seed slot rolled off before anyone resolved, or the
/// miner deactivated mid-window), freeing the miner for a new contest. Fees already collected stay in
/// the treasury (non-refundable); the pool account is reset, not closed (rent parked for reuse).
#[derive(Accounts)]
pub struct CancelPool<'info> {
    pub admin: Signer<'info>,

    #[account(seeds = [CONFIG_SEED], bump = config.bump, has_one = admin)]
    pub config: Account<'info, Config>,

    /// CHECK: identified by address only; used in PDA seeds.
    pub miner: UncheckedAccount<'info>,

    #[account(mut, seeds = [POOL_SEED, miner.key().as_ref()], bump = pool.bump)]
    pub pool: Account<'info, Pool>,
}

pub fn handler(ctx: Context<CancelPool>) -> Result<()> {
    let pool = &mut ctx.accounts.pool;
    pool.opened_at = 0;
    pool.requests.clear();

    emit!(PoolCancelled {
        miner: ctx.accounts.miner.key(),
    });
    Ok(())
}
