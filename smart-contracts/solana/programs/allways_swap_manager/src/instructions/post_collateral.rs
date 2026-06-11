use anchor_lang::prelude::*;
use anchor_lang::system_program::{transfer, Transfer};

use crate::constants::{CONFIG_SEED, MINER_SEED, VAULT_SEED};
use crate::error::ErrorCode;
use crate::events::CollateralPosted;
use crate::state::{Config, MinerState, Vault};

/// Miner deposits SOL collateral. Lamports move miner → vault (system CPI); the miner's
/// `MinerState` ledger and the vault's `total_collateral` both increase. First deposit
/// lazily creates the `MinerState` PDA (miner pays its rent).
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

    #[account(mut, seeds = [VAULT_SEED], bump = vault.bump)]
    pub vault: Account<'info, Vault>,

    pub system_program: Program<'info, System>,
}

pub fn handler(ctx: Context<PostCollateral>, amount: u64) -> Result<()> {
    require!(amount > 0, ErrorCode::InvalidAmount);

    let miner_key = ctx.accounts.miner.key();
    let max = ctx.accounts.config.max_collateral;
    let current = ctx.accounts.miner_state.collateral;
    let new_collateral = current.checked_add(amount).ok_or(ErrorCode::Overflow)?;
    require!(
        max == 0 || new_collateral <= max,
        ErrorCode::ExceedsMaxCollateral
    );

    // Move lamports miner -> vault via the system program.
    transfer(
        CpiContext::new(
            ctx.accounts.system_program.key(),
            Transfer {
                from: ctx.accounts.miner.to_account_info(),
                to: ctx.accounts.vault.to_account_info(),
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

    let vault = &mut ctx.accounts.vault;
    vault.total_collateral = vault
        .total_collateral
        .checked_add(amount)
        .ok_or(ErrorCode::Overflow)?;

    emit!(CollateralPosted {
        miner: miner_key,
        amount,
        total: new_collateral,
    });
    Ok(())
}
