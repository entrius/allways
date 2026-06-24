use anchor_lang::prelude::*;

use crate::consensus::{record_vote, reset_round, swap_request_hash};
use crate::constants::{
    COLLATERAL_SEED, CONFIG_SEED, FEE_DIVISOR, MINER_SEED, REQ_CONFIRM, STATS_SEED, SWAP_SEED,
    TREASURY_SEED, VOTE_SEED,
};
use crate::error::ErrorCode;
use crate::events::SwapCompleted;
use crate::penalty::apply_penalty;
use crate::state::{
    CollateralVault, Config, MinerDirectionStats, MinerState, Swap, SwapStatus, Treasury, VoteRound,
};

/// Validators confirm a fulfilled swap. On quorum the 1% fee is moved from the miner's collateral
/// vault into the Treasury PDA, the miner is freed, and the Swap account is closed (rent reclaimed).
#[derive(Accounts)]
#[instruction(swap_key: [u8; 32], from_chain: String, to_chain: String)]
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
        constraint = swap.from_chain == from_chain @ ErrorCode::ChainMismatch,
        constraint = swap.to_chain == to_chain @ ErrorCode::ChainMismatch,
    )]
    pub swap: Box<Account<'info, Swap>>,

    /// Realized per-direction stats, keyed by the (constrained-to-swap) chain args. Created on the first
    /// completed swap in this direction (validator-funded), reused + accrued thereafter.
    /// Boxed (with `swap`) to keep these String-heavy accounts off the BPF stack.
    #[account(
        init_if_needed,
        payer = validator,
        space = 8 + MinerDirectionStats::INIT_SPACE,
        seeds = [STATS_SEED, miner.key().as_ref(), from_chain.as_bytes(), to_chain.as_bytes()],
        bump,
    )]
    pub direction_stats: Box<Account<'info, MinerDirectionStats>>,

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

// `_from_chain`/`_to_chain` feed only the account seeds + the swap constraint (resolved before this
// body); the handler reads the chains from the swap itself.
pub fn handler(
    ctx: Context<ConfirmSwap>,
    swap_key: [u8; 32],
    _from_chain: String,
    _to_chain: String,
) -> Result<()> {
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
        // Read realized direction data off the swap before it's closed below.
        let from_amount = ctx.accounts.swap.from_amount;
        let to_amount = ctx.accounts.swap.to_amount;
        let from_chain = ctx.accounts.swap.from_chain.clone();
        let to_chain = ctx.accounts.swap.to_chain.clone();
        let rate = ctx.accounts.swap.rate.clone();
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
        ctx.accounts.miner_state.successful_swaps =
            ctx.accounts.miner_state.successful_swaps.saturating_add(1);

        // Accrue the miner's realized per-direction track record (count saturates; volume totals use
        // checked_add so a saturated sum can never silently corrupt the VWAP the validator reads).
        let stats_bump = ctx.bumps.direction_stats;
        let stats = &mut ctx.accounts.direction_stats;
        if stats.miner == Pubkey::default() {
            stats.miner = miner;
            stats.from_chain = from_chain.clone();
            stats.to_chain = to_chain.clone();
            stats.bump = stats_bump;
        }
        stats.completed = stats.completed.saturating_add(1);
        stats.total_sol_amount = stats
            .total_sol_amount
            .checked_add(sol_amount as u128)
            .ok_or(ErrorCode::Overflow)?;
        stats.total_from_amount = stats
            .total_from_amount
            .checked_add(from_amount)
            .ok_or(ErrorCode::Overflow)?;
        stats.total_to_amount = stats
            .total_to_amount
            .checked_add(to_amount)
            .ok_or(ErrorCode::Overflow)?;

        reset_round(&mut ctx.accounts.vote_round);
        ctx.accounts.swap.close(ctx.accounts.validator.to_account_info())?;

        emit!(SwapCompleted {
            swap_key,
            miner,
            sol_amount,
            fee: actual_fee,
            from_chain,
            to_chain,
            from_amount,
            to_amount,
            rate,
        });
    }
    Ok(())
}
