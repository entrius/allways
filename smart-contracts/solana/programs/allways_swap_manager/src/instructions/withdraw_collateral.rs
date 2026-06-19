use anchor_lang::prelude::*;

use crate::constants::{CONFIG_SEED, MINER_SEED, VAULT_SEED};
use crate::error::ErrorCode;
use crate::events::CollateralWithdrawn;
use crate::state::{Config, MinerState, Vault};

/// Miner withdraws SOL collateral back to their wallet. Guards mirror the ink! contract:
/// miner must be inactive, have no in-flight swap, and (if deactivated) be past the
/// post-deactivation cooldown (2× fulfillment timeout). Lamports move vault → miner via
/// direct lamport math (the program owns the vault).
#[derive(Accounts)]
pub struct WithdrawCollateral<'info> {
    #[account(mut)]
    pub miner: Signer<'info>,

    #[account(seeds = [CONFIG_SEED], bump = config.bump)]
    pub config: Account<'info, Config>,

    #[account(
        mut,
        seeds = [MINER_SEED, miner.key().as_ref()],
        bump = miner_state.bump,
        has_one = miner,
    )]
    pub miner_state: Account<'info, MinerState>,

    #[account(mut, seeds = [VAULT_SEED], bump = vault.bump)]
    pub vault: Account<'info, Vault>,
}

pub fn handler(ctx: Context<WithdrawCollateral>, amount: u64) -> Result<()> {
    require!(amount > 0, ErrorCode::InvalidAmount);

    let collateral = ctx.accounts.miner_state.collateral;
    let active = ctx.accounts.miner_state.active;
    let has_active_swap = ctx.accounts.miner_state.has_active_swap;
    let busy_until = ctx.accounts.miner_state.busy_until;
    let deactivation_at = ctx.accounts.miner_state.deactivation_at;

    require!(!active, ErrorCode::MinerActive);
    require!(!has_active_swap, ErrorCode::MinerHasActiveSwap);
    require!(amount <= collateral, ErrorCode::InsufficientCollateral);

    let now = Clock::get()?.unix_timestamp;

    // Cannot withdraw while busy (open pool / held reservation) — read from state, non-bypassable.
    require!(now >= busy_until, ErrorCode::MinerBusy);

    // Post-deactivation cooldown: wait 2× fulfillment timeout before pulling collateral.
    if deactivation_at != 0 {
        let cooldown_end = deactivation_at
            .checked_add(ctx.accounts.config.fulfillment_timeout_secs.saturating_mul(2))
            .ok_or(ErrorCode::Overflow)?;
        require!(now >= cooldown_end, ErrorCode::WithdrawCooldownActive);
    }

    // Move lamports vault -> miner (program-owned vault → direct lamport math).
    ctx.accounts.vault.to_account_info().sub_lamports(amount)?;
    ctx.accounts.miner.to_account_info().add_lamports(amount)?;

    // Update ledgers.
    ctx.accounts.miner_state.collateral = collateral
        .checked_sub(amount)
        .ok_or(ErrorCode::Overflow)?;
    let vault = &mut ctx.accounts.vault;
    vault.total_collateral = vault
        .total_collateral
        .checked_sub(amount)
        .ok_or(ErrorCode::Overflow)?;

    emit!(CollateralWithdrawn {
        miner: ctx.accounts.miner.key(),
        amount,
        total: ctx.accounts.miner_state.collateral,
    });
    Ok(())
}
