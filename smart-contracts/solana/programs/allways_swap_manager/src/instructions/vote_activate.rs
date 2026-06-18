use anchor_lang::prelude::*;

use crate::consensus::{record_vote, request_hash, reset_round};
use crate::constants::{CONFIG_SEED, MINER_SEED, REQ_ACTIVATE, VOTE_SEED};
use crate::error::ErrorCode;
use crate::state::{Config, MinerState, VoteRound};

/// A validator votes to activate a miner. On quorum the miner becomes active.
/// Guards: miner not already active, collateral ≥ min_collateral.
#[derive(Accounts)]
pub struct VoteActivate<'info> {
    #[account(mut)]
    pub validator: Signer<'info>,

    #[account(seeds = [CONFIG_SEED], bump = config.bump)]
    pub config: Account<'info, Config>,

    /// CHECK: identified by address only; bound via PDA seeds + the miner_state constraint.
    pub miner: UncheckedAccount<'info>,

    #[account(
        mut,
        seeds = [MINER_SEED, miner.key().as_ref()],
        bump = miner_state.bump,
        constraint = miner_state.miner == miner.key(),
    )]
    pub miner_state: Account<'info, MinerState>,

    #[account(
        init_if_needed,
        payer = validator,
        space = 8 + VoteRound::INIT_SPACE,
        seeds = [VOTE_SEED, &[REQ_ACTIVATE], miner.key().as_ref()],
        bump,
    )]
    pub vote_round: Account<'info, VoteRound>,

    pub system_program: Program<'info, System>,
}

pub fn handler(ctx: Context<VoteActivate>) -> Result<()> {
    require!(!ctx.accounts.config.halted, ErrorCode::SystemHalted);
    require!(!ctx.accounts.miner_state.active, ErrorCode::MinerAlreadyActive);
    require!(
        ctx.accounts.miner_state.collateral >= ctx.accounts.config.min_collateral,
        ErrorCode::InsufficientCollateral
    );

    let now = Clock::get()?.unix_timestamp;
    let miner_key = ctx.accounts.miner.key();
    let bound = request_hash(REQ_ACTIVATE, &miner_key);
    let validator = ctx.accounts.validator.key();
    let bump = ctx.bumps.vote_round;

    let quorum = record_vote(
        &mut ctx.accounts.vote_round,
        &ctx.accounts.config,
        validator,
        bound,
        bump,
        now,
    )?;

    if quorum {
        ctx.accounts.miner_state.active = true;
        ctx.accounts.miner_state.deactivation_at = 0;
        reset_round(&mut ctx.accounts.vote_round);
        msg!("miner activated via consensus: {}", miner_key);
    }
    Ok(())
}
