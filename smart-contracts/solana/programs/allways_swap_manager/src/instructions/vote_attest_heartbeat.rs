use anchor_lang::prelude::*;

use crate::consensus::{record_vote, request_hash, reset_round};
use crate::constants::{CONFIG_SEED, REQ_ATTEST_HEARTBEAT, VOTE_SEED};
use crate::events::AttestHeartbeat;
use crate::state::{Config, VoteRound};

/// Validators prove the attestation relay is alive. One global round on a lazy cadence (12–24 h), not
/// one per miner: liveness is a global question, and a per-miner age would fuse off quiet miners whose
/// attestation is old but perfectly correct.
///
/// NOT halt-gated, for the same reason as `vote_set_attestation` — the fuse must be able to reopen
/// during a halt, or every in-flight TAO-backed swap is stranded behind it.
#[derive(Accounts)]
pub struct VoteAttestHeartbeat<'info> {
    #[account(mut)]
    pub validator: Signer<'info>,

    #[account(mut, seeds = [CONFIG_SEED], bump = config.bump)]
    pub config: Account<'info, Config>,

    #[account(
        init_if_needed,
        payer = validator,
        space = 8 + VoteRound::INIT_SPACE,
        seeds = [VOTE_SEED, &[REQ_ATTEST_HEARTBEAT], config.key().as_ref()],
        bump,
    )]
    pub vote_round: Account<'info, VoteRound>,

    pub system_program: Program<'info, System>,
}

pub fn handler(ctx: Context<VoteAttestHeartbeat>) -> Result<()> {
    let now = Clock::get()?.unix_timestamp;
    // The whole payload is the request type: the timestamp comes from the chain at quorum, so there is
    // nothing a voter could disagree about.
    let config_key = ctx.accounts.config.key();
    let bound = request_hash(REQ_ATTEST_HEARTBEAT, &config_key);
    let validator = ctx.accounts.validator.key();
    let round_bump = ctx.bumps.vote_round;

    let quorum = record_vote(
        &mut ctx.accounts.vote_round,
        &ctx.accounts.config,
        validator,
        bound,
        round_bump,
        now,
    )?;

    if quorum {
        ctx.accounts.config.last_attest_heartbeat = now;
        reset_round(&mut ctx.accounts.vote_round);
        emit!(AttestHeartbeat { at: now });
        msg!("attestation heartbeat: {}", now);
    }
    Ok(())
}
