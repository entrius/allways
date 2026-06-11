use anchor_lang::prelude::*;

use crate::constants::{CONFIG_SEED, VAULT_SEED};
use crate::error::ErrorCode;
use crate::events::TreasuryWithdrawn;
use crate::state::{Config, Vault};

/// Admin withdraws accrued protocol fees from the vault's treasury to a recipient. Native-lamport
/// move (vault → recipient) + decrement of `treasury_total`, so the vault invariant
/// (`lamports == rent + total_collateral + treasury_total`) is preserved. Cannot touch collateral:
/// the guard is `amount <= treasury_total`, and collateral lamports are tracked separately.
#[derive(Accounts)]
pub struct WithdrawTreasury<'info> {
    pub admin: Signer<'info>,

    #[account(seeds = [CONFIG_SEED], bump = config.bump, has_one = admin)]
    pub config: Account<'info, Config>,

    #[account(mut, seeds = [VAULT_SEED], bump = vault.bump)]
    pub vault: Account<'info, Vault>,

    /// CHECK: receives the withdrawn fees; admin chooses the destination.
    #[account(mut)]
    pub recipient: UncheckedAccount<'info>,
}

pub fn handler(ctx: Context<WithdrawTreasury>, amount: u64) -> Result<()> {
    require!(amount > 0, ErrorCode::InvalidAmount);
    require!(
        amount <= ctx.accounts.vault.treasury_total,
        ErrorCode::InsufficientTreasury
    );

    ctx.accounts.vault.to_account_info().sub_lamports(amount)?;
    ctx.accounts.recipient.to_account_info().add_lamports(amount)?;

    let vault = &mut ctx.accounts.vault;
    vault.treasury_total = vault
        .treasury_total
        .checked_sub(amount)
        .ok_or(ErrorCode::Overflow)?;

    emit!(TreasuryWithdrawn {
        recipient: ctx.accounts.recipient.key(),
        amount,
        total: vault.treasury_total,
    });
    Ok(())
}
