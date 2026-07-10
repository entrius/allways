use anchor_lang::prelude::*;
use anchor_lang::system_program::{transfer, Transfer};

use crate::constants::{COLLATERAL_SEED, CONFIG_SEED, MINER_SEED};
use crate::error::ErrorCode;
use crate::events::CollateralPosted;
use crate::state::{CollateralVault, Config, MinerState};

/// Miner deposits SOL collateral into their OWN per-miner collateral vault (system CPI). The
/// `MinerState.collateral` ledger increases; the lamports land in `[COLLATERAL_SEED, miner]`. First
/// deposit lazily creates both the `MinerState` and the collateral-vault PDAs (miner pays rent).
#[derive(Accounts)]
pub struct PostCollateral<'info> {
    #[account(mut)]
    pub miner: Signer<'info>,

    #[account(seeds = [CONFIG_SEED], bump = config.bump)]
    pub config: Account<'info, Config>,

    #[account(
        init_if_needed,
        payer = miner,
        space = 8 + MinerState::INIT_SPACE,
        seeds = [MINER_SEED, miner.key().as_ref()],
        bump,
    )]
    pub miner_state: Account<'info, MinerState>,

    /// The miner's own collateral vault — holds only this miner's collateral lamports.
    #[account(
        init_if_needed,
        payer = miner,
        space = 8 + CollateralVault::INIT_SPACE,
        seeds = [COLLATERAL_SEED, miner.key().as_ref()],
        bump,
    )]
    pub collateral_vault: Account<'info, CollateralVault>,

    pub system_program: Program<'info, System>,
}

pub fn handler(ctx: Context<PostCollateral>, amount: u64) -> Result<()> {
    require!(!ctx.accounts.config.halted, ErrorCode::SystemHalted);
    require!(amount > 0, ErrorCode::InvalidAmount);

    let miner_key = ctx.accounts.miner.key();
    let max = ctx.accounts.config.max_collateral;
    let current = ctx.accounts.miner_state.collateral;
    let new_collateral = current.checked_add(amount).ok_or(ErrorCode::Overflow)?;
    require!(
        max == 0 || new_collateral <= max,
        ErrorCode::ExceedsMaxCollateral
    );

    // Move lamports miner -> the miner's own collateral vault via the system program.
    transfer(
        CpiContext::new(
            ctx.accounts.system_program.key(),
            Transfer {
                from: ctx.accounts.miner.to_account_info(),
                to: ctx.accounts.collateral_vault.to_account_info(),
            },
        ),
        amount,
    )?;

    // Update ledgers (identity fields set on first deposit — init_if_needed zeroes them).
    let bump = ctx.bumps.miner_state;
    let ms = &mut ctx.accounts.miner_state;
    if ms.miner == Pubkey::default() {
        ms.miner = miner_key;
        ms.bump = bump;
    }
    ms.collateral = new_collateral;
    ctx.accounts.collateral_vault.bump = ctx.bumps.collateral_vault;

    emit!(CollateralPosted {
        miner: miner_key,
        amount,
        total: new_collateral,
    });
    Ok(())
}
