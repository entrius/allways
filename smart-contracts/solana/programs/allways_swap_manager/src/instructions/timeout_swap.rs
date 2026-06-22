use anchor_lang::prelude::*;

use crate::consensus::{record_vote, reset_round, swap_request_hash};
use crate::constants::{COLLATERAL_SEED, CONFIG_SEED, MINER_SEED, REQ_TIMEOUT, SWAP_SEED, VOTE_SEED};
use crate::error::ErrorCode;
use crate::events::SwapTimedOut;
use crate::penalty::apply_penalty;
use crate::state::{CollateralVault, Config, MinerState, Swap, SwapStatus, VoteRound};

/// Validators time out a swap whose deadline passed. On quorum the miner's collateral is slashed and
/// refunded to the user (collateral vault -> user), the miner is freed, and the Swap is closed.
#[derive(Accounts)]
#[instruction(swap_key: [u8; 32])]
pub struct TimeoutSwap<'info> {
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

    /// The miner's own collateral vault — the slash is debited from here.
    #[account(mut, seeds = [COLLATERAL_SEED, miner.key().as_ref()], bump = collateral_vault.bump)]
    pub collateral_vault: Account<'info, CollateralVault>,

    /// CHECK: receives the slashed refund; must equal `swap.user`.
    #[account(mut)]
    pub user: UncheckedAccount<'info>,

    #[account(
        mut,
        seeds = [SWAP_SEED, swap_key.as_ref()],
        bump = swap.bump,
        has_one = miner,
        constraint = swap.user == user.key() @ ErrorCode::UserMismatch,
    )]
    pub swap: Account<'info, Swap>,

    #[account(
        init_if_needed,
        payer = validator,
        space = 8 + VoteRound::INIT_SPACE,
        seeds = [VOTE_SEED, &[REQ_TIMEOUT], swap_key.as_ref()],
        bump,
    )]
    pub vote_round: Account<'info, VoteRound>,

    pub system_program: Program<'info, System>,
}

pub fn handler(ctx: Context<TimeoutSwap>, swap_key: [u8; 32]) -> Result<()> {
    let now = Clock::get()?.unix_timestamp;
    {
        let swap = &ctx.accounts.swap;
        require!(
            swap.status == SwapStatus::Active || swap.status == SwapStatus::Fulfilled,
            ErrorCode::InvalidStatus
        );
        require!(now >= swap.timeout_at, ErrorCode::NotTimedOut);
    }

    let bound = swap_request_hash(REQ_TIMEOUT, &swap_key);
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
        let min_collateral = ctx.accounts.config.min_collateral;

        // v2 #4: a failed swap is penalized at the over-collateralization multiplier (1.10×), and
        // the entire slash is refunded to the wronged user (made more than whole). The 1.1× initiate
        // guard + one-swap-at-a-time invariant guarantee the miner can cover it; apply_penalty still
        // clamps to available collateral as a safety net.
        let penalty = crate::constants::required_collateral(sol_amount);

        let slash = apply_penalty(&mut ctx.accounts.miner_state, min_collateral, penalty, now)?;

        // Refund the slashed collateral to the user (miner's collateral vault → user, native lamports).
        if slash > 0 {
            ctx.accounts.collateral_vault.to_account_info().sub_lamports(slash)?;
            ctx.accounts.user.to_account_info().add_lamports(slash)?;
        }
        ctx.accounts.miner_state.has_active_swap = false;
        ctx.accounts.miner_state.busy_until = 0;

        reset_round(&mut ctx.accounts.vote_round);
        ctx.accounts.swap.close(ctx.accounts.validator.to_account_info())?;

        emit!(SwapTimedOut {
            swap_key,
            miner,
            sol_amount,
            slash,
        });
    }
    Ok(())
}
