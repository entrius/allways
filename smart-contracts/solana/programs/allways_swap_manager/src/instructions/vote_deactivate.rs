use anchor_lang::prelude::*;

use crate::consensus::{record_vote, request_hash, reset_round};
use crate::constants::{CONFIG_SEED, MINER_SEED, REQ_DEACTIVATE, VOTE_SEED};
use crate::error::ErrorCode;
use crate::events::MinerDeactivated;
use crate::state::{Config, MinerState, VoteRound};

/// A validator votes to force-deactivate a miner. On quorum the miner is deactivated and
/// the post-deactivation cooldown clock starts. Guard: miner currently active.
#[derive(Accounts)]
pub struct VoteDeactivate<'info> {
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
        seeds = [VOTE_SEED, &[REQ_DEACTIVATE], miner.key().as_ref()],
        bump,
    )]
    pub vote_round: Account<'info, VoteRound>,

    pub system_program: Program<'info, System>,
}

pub fn handler(ctx: Context<VoteDeactivate>) -> Result<()> {
    let now = Clock::get()?.unix_timestamp;
    require!(ctx.accounts.miner_state.active, ErrorCode::MinerNotActive);
    // Only an idle miner can be deactivated — never one mid-commitment (open pool / held reservation /
    // in-flight swap). Mirrors self-`deactivate` and keeps the "busy ⟹ active" invariant that
    // open_or_request + resolve_pool rely on (review #3 / user req).
    require!(!ctx.accounts.miner_state.has_active_swap, ErrorCode::MinerHasActiveSwap);
    require!(now >= ctx.accounts.miner_state.busy_until, ErrorCode::MinerBusy);

    let miner_key = ctx.accounts.miner.key();
    let bound = request_hash(REQ_DEACTIVATE, &miner_key);
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
        ctx.accounts.miner_state.active = false;
        ctx.accounts.miner_state.deactivation_at = now;
        reset_round(&mut ctx.accounts.vote_round);
        emit!(MinerDeactivated { miner: miner_key, at: now });
        msg!("miner deactivated via consensus: {}", miner_key);
    }
    Ok(())
}
