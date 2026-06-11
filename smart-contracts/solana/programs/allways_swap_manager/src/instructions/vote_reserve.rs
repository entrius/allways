use anchor_lang::prelude::*;

use crate::consensus::{record_vote, reserve_hash, reset_round};
use crate::constants::{
    CONFIG_SEED, MAX_ADDR_LEN, MAX_CHAIN_LEN, MAX_RATE_LEN, MINER_SEED, REQ_RESERVE, RESV_SEED,
    VOTE_SEED,
};
use crate::error::ErrorCode;
use crate::state::{Config, MinerState, Reservation, VoteRound};

/// A validator votes to reserve a miner for a specific pending swap (chains + amounts + user source
/// address). On quorum the Reservation is written with a TTL. The bound-hash binds all swap terms so
/// validators can't vote on differing amounts/addresses in the same round.
#[derive(Accounts)]
pub struct VoteReserve<'info> {
    #[account(mut)]
    pub validator: Signer<'info>,

    #[account(seeds = [CONFIG_SEED], bump = config.bump)]
    pub config: Account<'info, Config>,

    /// CHECK: identified by address only; bound via PDA seeds + the miner_state constraint.
    pub miner: UncheckedAccount<'info>,

    #[account(
        seeds = [MINER_SEED, miner.key().as_ref()],
        bump = miner_state.bump,
        constraint = miner_state.miner == miner.key(),
    )]
    pub miner_state: Account<'info, MinerState>,

    #[account(
        init_if_needed,
        payer = validator,
        space = 8 + VoteRound::INIT_SPACE,
        seeds = [VOTE_SEED, &[REQ_RESERVE], miner.key().as_ref()],
        bump,
    )]
    pub vote_round: Account<'info, VoteRound>,

    #[account(
        init_if_needed,
        payer = validator,
        space = 8 + Reservation::INIT_SPACE,
        seeds = [RESV_SEED, miner.key().as_ref()],
        bump,
    )]
    pub reservation: Account<'info, Reservation>,

    pub system_program: Program<'info, System>,
}

#[allow(clippy::too_many_arguments)]
pub fn handler(
    ctx: Context<VoteReserve>,
    from_addr: String,
    from_chain: String,
    to_chain: String,
    sol_amount: u64,
    from_amount: u128,
    to_amount: u128,
    miner_from_addr: String,
    miner_to_addr: String,
    rate: String,
) -> Result<()> {
    require!(from_addr.len() <= MAX_ADDR_LEN, ErrorCode::StringTooLong);
    require!(from_chain.len() <= MAX_CHAIN_LEN, ErrorCode::StringTooLong);
    require!(to_chain.len() <= MAX_CHAIN_LEN, ErrorCode::StringTooLong);
    require!(miner_from_addr.len() <= MAX_ADDR_LEN, ErrorCode::StringTooLong);
    require!(miner_to_addr.len() <= MAX_ADDR_LEN, ErrorCode::StringTooLong);
    require!(rate.len() <= MAX_RATE_LEN, ErrorCode::StringTooLong);

    let cfg = &ctx.accounts.config;
    require!(
        cfg.min_swap_amount == 0 || sol_amount >= cfg.min_swap_amount,
        ErrorCode::AmountBelowMin
    );
    require!(
        cfg.max_swap_amount == 0 || sol_amount <= cfg.max_swap_amount,
        ErrorCode::AmountAboveMax
    );

    require!(ctx.accounts.miner_state.active, ErrorCode::MinerNotActive);
    require!(
        !ctx.accounts.miner_state.has_active_swap,
        ErrorCode::MinerHasActiveSwap
    );
    require!(
        ctx.accounts.miner_state.collateral >= cfg.min_collateral,
        ErrorCode::InsufficientCollateral
    );

    let now = Clock::get()?.unix_timestamp;

    // Reject only an *active* reservation; empty (0) or expired slots may be overwritten.
    let r = &ctx.accounts.reservation;
    let active_reservation = r.reserved_until != 0 && r.reserved_until >= now;
    require!(!active_reservation, ErrorCode::MinerReserved);

    let miner_key = ctx.accounts.miner.key();
    let bound = reserve_hash(
        &miner_key,
        &from_addr,
        &from_chain,
        &to_chain,
        sol_amount,
        from_amount,
        to_amount,
        &miner_from_addr,
        &miner_to_addr,
        &rate,
    );
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
        let ttl = ctx.accounts.config.reservation_ttl_secs;
        let reservation_bump = ctx.bumps.reservation;
        let r = &mut ctx.accounts.reservation;
        r.bound_hash = bound;
        r.from_addr = from_addr;
        r.from_chain = from_chain;
        r.to_chain = to_chain;
        r.sol_amount = sol_amount;
        r.from_amount = from_amount;
        r.to_amount = to_amount;
        r.miner_from_addr = miner_from_addr;
        r.miner_to_addr = miner_to_addr;
        r.rate = rate;
        r.reserved_until = now.saturating_add(ttl);
        r.bump = reservation_bump;
        reset_round(&mut ctx.accounts.vote_round);
        msg!("miner reserved: {} until {}", miner_key, r.reserved_until);
    }
    Ok(())
}
