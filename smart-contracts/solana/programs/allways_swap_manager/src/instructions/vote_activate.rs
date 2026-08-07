use anchor_lang::prelude::*;

use crate::backing;
use crate::consensus::{backing_request_hash, record_vote, reset_round};
use crate::constants::{ATTEST_SEED, CONFIG_SEED, MINER_SEED, REQ_ACTIVATE, VOTE_SEED};
use crate::error::ErrorCode;
use crate::events::{MinerActivated, MinerBackingChanged};
use crate::state::{BondAttestation, Config, MinerState, VoteRound};

/// A validator votes to activate ONE of a miner's backings. On quorum that backing's bit is set and the
/// legacy `active` bool becomes the OR of the bits. Guards fork on the backing: "sol" checks the local
/// collateral floor; an off-chain backing checks its attestation (locked, above its own floor) and the
/// heartbeat fuse. The backing is hash-bound, so an activate("sol") vote can never co-count into a
/// quorum that activates "tao".
#[derive(Accounts)]
#[instruction(backing: String)]
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

    /// The round is keyed per miner (not per backing) — one activation proposal at a time, with the
    /// backing carried in the bound hash.
    #[account(
        init_if_needed,
        payer = validator,
        space = 8 + VoteRound::INIT_SPACE,
        seeds = [VOTE_SEED, &[REQ_ACTIVATE], miner.key().as_ref()],
        bump,
    )]
    pub vote_round: Account<'info, VoteRound>,

    /// Required for an off-chain backing, omitted for "sol" (which reads the local vault ledger).
    #[account(
        seeds = [ATTEST_SEED, miner.key().as_ref(), backing.as_bytes()],
        bump,
    )]
    pub attestation: Option<Account<'info, BondAttestation>>,

    pub system_program: Program<'info, System>,
}

pub fn handler(ctx: Context<VoteActivate>, backing: String) -> Result<()> {
    require!(!ctx.accounts.config.halted, ErrorCode::SystemHalted);
    let bit = backing::backing_bit(&backing)?;
    require!(
        ctx.accounts.miner_state.active_backings & bit == 0,
        ErrorCode::MinerAlreadyActive
    );

    let now = Clock::get()?.unix_timestamp;
    let cfg = &ctx.accounts.config;
    let attestation = ctx.accounts.attestation.as_deref();

    // Same purse read every collateral guard uses, against this backing's own floor in its own units.
    if !backing::settles_locally(&backing) {
        backing::require_fresh_heartbeat(cfg, now)?;
    }
    let purse = backing::backing_purse(&backing, &ctx.accounts.miner_state, attestation)?;
    let floor = backing::activation_floor(cfg, &backing)?;
    require!(purse >= floor, ErrorCode::InsufficientCollateral);

    let miner_key = ctx.accounts.miner.key();
    let bound = backing_request_hash(REQ_ACTIVATE, &miner_key, &backing);
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
        let was_active = ctx.accounts.miner_state.active;
        let active = ctx.accounts.miner_state.set_backing(bit, true);
        ctx.accounts.miner_state.deactivation_at = 0;
        reset_round(&mut ctx.accounts.vote_round);
        emit!(MinerBackingChanged {
            miner: miner_key,
            backing,
            enabled: true,
            active_backings: ctx.accounts.miner_state.active_backings,
            at: now,
        });
        // Only when the OR view itself flips, so scorers replaying `active` from these two events see
        // exactly the history they saw pre-W2.
        if !was_active && active {
            emit!(MinerActivated { miner: miner_key, at: now });
        }
        msg!("miner activated via consensus: {}", miner_key);
    }
    Ok(())
}
