use anchor_lang::prelude::*;

use crate::consensus::ensure_validator;
use crate::constants::{CONFIG_SEED, MINER_SEED, SWAP_SEED};
use crate::error::ErrorCode;
use crate::events::SwapTimeoutExtended;
use crate::state::{Config, MinerState, Swap, SwapStatus};

/// Fulfillment-side mirror of `extend_reservation`: a single validator slides a swap's `timeout_at`
/// forward while it waits on slow destination-chain confirmation. Same guards (monotonic + frozen
/// ceiling), ignores `halted`. Allowed in Active or Fulfilled — the same gate as the timeout it defers.
#[derive(Accounts)]
#[instruction(swap_key: [u8; 32])]
pub struct ExtendTimeout<'info> {
    pub validator: Signer<'info>,

    #[account(seeds = [CONFIG_SEED], bump = config.bump)]
    pub config: Account<'info, Config>,

    /// CHECK: bound via the miner_state seeds + the swap `has_one`.
    pub miner: UncheckedAccount<'info>,

    #[account(
        mut,
        seeds = [MINER_SEED, miner.key().as_ref()],
        bump = miner_state.bump,
        constraint = miner_state.miner == miner.key(),
    )]
    pub miner_state: Account<'info, MinerState>,

    #[account(
        mut,
        seeds = [SWAP_SEED, swap_key.as_ref()],
        bump = swap.bump,
        has_one = miner,
    )]
    pub swap: Account<'info, Swap>,
}

pub fn handler(ctx: Context<ExtendTimeout>, swap_key: [u8; 32], target_at: i64) -> Result<()> {
    let validator = ctx.accounts.validator.key();
    ensure_validator(&ctx.accounts.config, &validator)?;

    let swap = &mut ctx.accounts.swap;
    require!(
        swap.status == SwapStatus::Active || swap.status == SwapStatus::Fulfilled,
        ErrorCode::InvalidStatus
    );
    require!(target_at > swap.timeout_at, ErrorCode::ExtensionNotLater);
    require!(target_at <= swap.max_extend_at, ErrorCode::ExtensionExceedsCeiling);

    swap.timeout_at = target_at;
    ctx.accounts.miner_state.busy_until = target_at;

    emit!(SwapTimeoutExtended {
        swap_key,
        miner: ctx.accounts.miner.key(),
        validator,
        timeout_at: target_at,
    });
    Ok(())
}
