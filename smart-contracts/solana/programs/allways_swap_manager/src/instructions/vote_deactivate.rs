use anchor_lang::prelude::*;

use crate::backing;
use crate::consensus::{backing_request_hash, record_vote, reset_round};
use crate::constants::{BACKING_BIT_SOL, CONFIG_SEED, MINER_SEED, REQ_DEACTIVATE, VOTE_SEED};
use crate::error::ErrorCode;
use crate::events::{MinerBackingChanged, MinerDeactivated};
use crate::state::{Config, MinerState, VoteRound};

/// A validator votes to force-deactivate ONE of a miner's backings (the #616 floor sweep, per purse).
/// On quorum that bit is cleared; the miner keeps trading on any backing still lit. The busy guards stay
/// GLOBAL — one swap at a time spans purses, so a miner mid-commitment is off limits on every backing.
#[derive(Accounts)]
#[instruction(backing: String)]
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

pub fn handler(ctx: Context<VoteDeactivate>, backing: String) -> Result<()> {
    let now = Clock::get()?.unix_timestamp;
    let bit = backing::backing_bit(&backing)?;
    require!(
        ctx.accounts.miner_state.active_backings & bit != 0,
        ErrorCode::MinerNotActive
    );
    // Only an idle miner can be deactivated — never one mid-commitment (open pool / held reservation /
    // in-flight swap). Mirrors self-`deactivate` and keeps the "busy ⟹ active" invariant that
    // open_or_request + resolve_pool rely on (review #3 / user req).
    require!(!ctx.accounts.miner_state.has_active_swap, ErrorCode::MinerHasActiveSwap);
    require!(now >= ctx.accounts.miner_state.busy_until, ErrorCode::MinerBusy);

    let miner_key = ctx.accounts.miner.key();
    let bound = backing_request_hash(REQ_DEACTIVATE, &miner_key, &backing);
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
        let active = ctx.accounts.miner_state.set_backing(bit, false);
        reset_round(&mut ctx.accounts.vote_round);
        emit!(MinerBackingChanged {
            miner: miner_key,
            backing,
            enabled: false,
            active_backings: ctx.accounts.miner_state.active_backings,
            at: now,
        });
        // The cooldown guards LOCAL collateral, so it tracks the SOL bit dropping — same rule as
        // self-`deactivate`. MinerDeactivated stays on the OR view: a miner still trading on another
        // backing has not left, and the event must not claim it has.
        if bit & BACKING_BIT_SOL != 0 {
            ctx.accounts.miner_state.deactivation_at = now;
        }
        if !active {
            emit!(MinerDeactivated { miner: miner_key, at: now });
        }
        msg!("miner deactivated via consensus: {}", miner_key);
    }
    Ok(())
}
