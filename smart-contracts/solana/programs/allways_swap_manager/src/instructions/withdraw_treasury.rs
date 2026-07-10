use anchor_lang::prelude::*;

use crate::constants::{CONFIG_SEED, TREASURY_SEED};
use crate::error::ErrorCode;
use crate::events::TreasuryWithdrawn;
use crate::state::{Config, Treasury};

/// Admin withdraws accrued subnet revenue from the treasury to the admin wallet. Native-lamport
/// move (treasury → admin) + decrement of `total`, preserving the treasury invariant
/// (`lamports == rent + total`). Structurally cannot touch collateral — that lives in a separate PDA.
#[derive(Accounts)]
pub struct WithdrawTreasury<'info> {
    pub admin: Signer<'info>,

    #[account(seeds = [CONFIG_SEED], bump = config.bump, has_one = admin)]
    pub config: Account<'info, Config>,

    #[account(mut, seeds = [TREASURY_SEED], bump = treasury.bump)]
    pub treasury: Account<'info, Treasury>,

    /// CHECK: pinned to the admin so a bad CLI argument or compromised tooling cannot route
    /// fees to an arbitrary address; onward distribution is a second, admin-signed hop.
    #[account(mut, constraint = recipient.key() == config.admin @ ErrorCode::TreasuryRecipientNotAdmin)]
    pub recipient: UncheckedAccount<'info>,
}

pub fn handler(ctx: Context<WithdrawTreasury>, amount: u64) -> Result<()> {
    require!(amount > 0, ErrorCode::InvalidAmount);
    require!(
        amount <= ctx.accounts.treasury.total,
        ErrorCode::InsufficientTreasury
    );

    ctx.accounts.treasury.to_account_info().sub_lamports(amount)?;
    ctx.accounts.recipient.to_account_info().add_lamports(amount)?;

    let treasury = &mut ctx.accounts.treasury;
    treasury.total = treasury
        .total
        .checked_sub(amount)
        .ok_or(ErrorCode::Overflow)?;

    emit!(TreasuryWithdrawn {
        recipient: ctx.accounts.recipient.key(),
        amount,
        total: treasury.total,
    });
    Ok(())
}
