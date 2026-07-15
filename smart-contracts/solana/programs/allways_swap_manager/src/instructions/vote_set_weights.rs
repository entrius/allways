use anchor_lang::prelude::*;

use crate::consensus::{record_vote, reset_round, weights_hash};
use crate::constants::{CONFIG_SEED, REQ_SET_WEIGHTS, VOTE_SEED};
use crate::error::ErrorCode;
use crate::events::ValidatorWeightsUpdated;
use crate::state::{Config, VoteRound};

/// A validator votes to set the full validator-weight vector. Validators submit a vector index-aligned
/// to `Config.validators`; on quorum the weights are saved. Everyone votes the same snapshot
/// (hash-bound on keys+weights), so divergent vectors never co-count.
///
/// The round PDA is keyed by `round_key` (= the snapshot hash, checked in the handler), NOT a global
/// singleton: competing proposals get separate rounds that coexist, so one junk vote can no longer
/// park a wrong hash in the shared round and freeze weight updates for the whole round TTL.
#[derive(Accounts)]
#[instruction(weights: Vec<u64>, round_key: [u8; 32])]
pub struct VoteSetWeights<'info> {
    #[account(mut)]
    pub validator: Signer<'info>,

    #[account(mut, seeds = [CONFIG_SEED], bump = config.bump)]
    pub config: Account<'info, Config>,

    #[account(
        init_if_needed,
        payer = validator,
        space = 8 + VoteRound::INIT_SPACE,
        seeds = [VOTE_SEED, &[REQ_SET_WEIGHTS], round_key.as_ref()],
        bump,
    )]
    pub vote_round: Account<'info, VoteRound>,

    pub system_program: Program<'info, System>,
}

pub fn handler(ctx: Context<VoteSetWeights>, weights: Vec<u64>, round_key: [u8; 32]) -> Result<()> {
    let now = Clock::get()?.unix_timestamp;

    // Cadence floor (anti-thrash): can't re-set faster than the min interval; first update is always allowed.
    let last = ctx.accounts.config.last_weights_update;
    require!(
        last == 0
            || now.saturating_sub(last) >= ctx.accounts.config.weights_update_min_interval_secs,
        ErrorCode::WeightsUpdateTooSoon
    );

    require!(
        weights.len() == ctx.accounts.config.validators.len(),
        ErrorCode::InvalidWeightsVector
    );

    // The PDA seed must BE the snapshot hash — otherwise a voter could route an arbitrary weights
    // vector into another proposal's round and taint its bound_hash.
    let bound = weights_hash(&ctx.accounts.config.validators, &weights);
    require!(round_key == bound, ErrorCode::WeightsRoundKeyMismatch);
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
        let config = &mut ctx.accounts.config;
        for (v, w) in config.validators.iter_mut().zip(weights.iter()) {
            v.weight = *w;
        }
        config.last_weights_update = now;
        let count = config.validators.len() as u8;
        reset_round(&mut ctx.accounts.vote_round);
        emit!(ValidatorWeightsUpdated {
            count,
            updated_at: now,
        });
        msg!("validator weights updated via consensus ({} validators)", count);
    }
    Ok(())
}
