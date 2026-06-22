use anchor_lang::prelude::*;

use crate::consensus::{record_vote, reset_round, swap_request_hash};
use crate::constants::{
    COLLATERAL_SEED, CONFIG_SEED, FEE_DIVISOR, MINER_SEED, REQ_CONFIRM, SWAP_SEED, TREASURY_SEED,
    VOTE_SEED,
};
use crate::error::ErrorCode;
use crate::events::SwapCompleted;
use crate::penalty::apply_penalty;
use crate::state::{CollateralVault, Config, MinerState, Swap, SwapStatus, Treasury, VoteRound};

/// Validators confirm a fulfilled swap. On quorum the 1% fee is moved from the miner's collateral
/// vault into the Treasury PDA, the miner is freed, and the Swap account is closed (rent reclaimed).
#[derive(Accounts)]
#[instruction(swap_key: [u8; 32])]
pub struct ConfirmSwap<'info> {
    #[account(mut)]
    pub validator: Signer<'info>,

    #[account(seeds = [CONFIG_SEED], bump = config.bump)]
    pub config: Account<'info, Config>,

    /// CHECK: bound via miner_state seeds + the swap `has_one`.
    pub miner: UncheckedAccount<'info>,

    #[account(
        mut,
        seeds = [MINER_SEED, miner.key().as_ref()],
        bump = miner_state.bump,
        constraint = miner_state.miner == miner.key(),
    )]
    pub miner_state: Account<'info, MinerState>,

    /// The miner's own collateral vault — the 1% fee is debited from here.
    #[account(mut, seeds = [COLLATERAL_SEED, miner.key().as_ref()], bump = collateral_vault.bump)]
    pub collateral_vault: Account<'info, CollateralVault>,

    /// Subnet treasury — the 1% fee moves here out of the miner's collateral vault.
    #[account(mut, seeds = [TREASURY_SEED], bump = treasury.bump)]
    pub treasury: Account<'info, Treasury>,

    #[account(
        mut,
        seeds = [SWAP_SEED, swap_key.as_ref()],
        bump = swap.bump,
        has_one = miner,
    )]
    pub swap: Account<'info, Swap>,

    #[account(
        init_if_needed,
        payer = validator,
        space = 8 + VoteRound::INIT_SPACE,
        seeds = [VOTE_SEED, &[REQ_CONFIRM], swap_key.as_ref()],
        bump,
    )]
    pub vote_round: Account<'info, VoteRound>,

    pub system_program: Program<'info, System>,
}

pub fn handler(ctx: Context<ConfirmSwap>, swap_key: [u8; 32]) -> Result<()> {
    require!(
        ctx.accounts.swap.status == SwapStatus::Fulfilled,
        ErrorCode::InvalidStatus
    );

    let now = Clock::get()?.unix_timestamp;
    let bound = swap_request_hash(REQ_CONFIRM, &swap_key);
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
        let sol_amount = ctx.accounts.swap.sol_amount;
        let miner = ctx.accounts.swap.miner;
        let fee = sol_amount / FEE_DIVISOR;
        let min_collateral = ctx.accounts.config.min_collateral;

        let actual_fee = apply_penalty(&mut ctx.accounts.miner_state, min_collateral, fee, now)?;
        // Move the fee out of the miner's collateral vault into the subnet treasury (both are
        // program-owned PDAs → direct lamport math). apply_penalty already shrank the ledger.
        if actual_fee > 0 {
            ctx.accounts.collateral_vault.to_account_info().sub_lamports(actual_fee)?;
            ctx.accounts.treasury.to_account_info().add_lamports(actual_fee)?;
            ctx.accounts.treasury.total = ctx
                .accounts
                .treasury
                .total
                .checked_add(actual_fee)
                .ok_or(ErrorCode::Overflow)?;
        }
        ctx.accounts.miner_state.has_active_swap = false;
        ctx.accounts.miner_state.busy_until = 0;

        reset_round(&mut ctx.accounts.vote_round);
        ctx.accounts.swap.close(ctx.accounts.validator.to_account_info())?;

        emit!(SwapCompleted {
            swap_key,
            miner,
            sol_amount,
            fee: actual_fee,
        });
    }
    Ok(())
}
