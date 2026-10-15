use anchor_lang::prelude::*;

use crate::backing;
use crate::consensus::{backing_request_hash, record_vote, reset_round};
use crate::constants::{CONFIG_SEED, REQ_ATTEST_HEARTBEAT, VOTE_SEED};
use crate::events::AttestHeartbeat;
use crate::state::{Config, VoteRound};

/// Validators prove ONE hub's relay is alive: one round per hub on a lazy cadence, never per miner, so
/// a stalled Base relay can't fuse TAO entry (or the reverse). NOT halt-gated, like `vote_set_attestation`:
/// the fuse must reopen during a halt or every in-flight remotely-backed swap is stranded.
#[derive(Accounts)]
#[instruction(hub: String)]
pub struct VoteAttestHeartbeat<'info> {
    #[account(mut)]
    pub validator: Signer<'info>,

    #[account(mut, seeds = [CONFIG_SEED], bump = config.bump)]
    pub config: Box<Account<'info, Config>>,

    #[account(
        init_if_needed,
        payer = validator,
        space = 8 + VoteRound::INIT_SPACE,
        seeds = [VOTE_SEED, &[REQ_ATTEST_HEARTBEAT], config.key().as_ref(), hub.as_bytes()],
        bump,
    )]
    pub vote_round: Account<'info, VoteRound>,

    pub system_program: Program<'info, System>,
}

pub fn handler(ctx: Context<VoteAttestHeartbeat>, hub: String) -> Result<()> {
    // Refuse "sol" (no relay to prove alive) and unknown hubs before any round opens.
    backing::heartbeat(&ctx.accounts.config, &hub)?;

    let now = Clock::get()?.unix_timestamp;
    // The payload is (type, hub): the timestamp comes from the chain at quorum, so there is nothing
    // else a voter could disagree about. The hub is in the hash as well as the seeds, belt and braces.
    let config_key = ctx.accounts.config.key();
    let bound = backing_request_hash(REQ_ATTEST_HEARTBEAT, &config_key, &hub);
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
        *backing::heartbeat_mut(&mut ctx.accounts.config, &hub)? = now;
        reset_round(&mut ctx.accounts.vote_round);
        emit!(AttestHeartbeat { at: now, hub: hub.clone() });
        msg!("attestation heartbeat [{}]: {}", hub, now);
    }
    Ok(())
}
