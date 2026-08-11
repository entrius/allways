use anchor_lang::prelude::*;

use crate::constants::{BACKING_BIT_SOL, COLLATERAL_SEED, CONFIG_SEED, MINER_SEED};
use crate::error::ErrorCode;
use crate::events::CollateralWithdrawn;
use crate::state::{CollateralVault, Config, MinerState};

/// Miner withdraws SOL collateral from their own collateral vault back to their wallet. Guards are
/// SOL-hub-scoped (v3.1): the SOL purse must be dark, its hub free (no in-flight SOL swap, past its
/// busy slot), and (if deactivated) past the cooldown (2x fulfillment timeout). A live TAO purse no
/// longer blocks this — SOL collateral backs nothing there. Lamports move vault -> miner.
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

    #[account(mut, seeds = [COLLATERAL_SEED, miner.key().as_ref()], bump = collateral_vault.bump)]
    pub collateral_vault: Account<'info, CollateralVault>,
}

pub fn handler(ctx: Context<WithdrawCollateral>, amount: u64) -> Result<()> {
    require!(amount > 0, ErrorCode::InvalidAmount);

    let collateral = ctx.accounts.miner_state.collateral;
    let sol_active = ctx.accounts.miner_state.active_backings & BACKING_BIT_SOL != 0;
    let sol_swap = ctx.accounts.miner_state.swap_on(BACKING_BIT_SOL);
    let busy_until = ctx.accounts.miner_state.busy_slot(BACKING_BIT_SOL);
    let deactivation_at = ctx.accounts.miner_state.deactivation_at;

    require!(!sol_active, ErrorCode::MinerActive);
    require!(!sol_swap, ErrorCode::MinerHasActiveSwap);
    require!(amount <= collateral, ErrorCode::InsufficientCollateral);

    let now = Clock::get()?.unix_timestamp;

    // Cannot withdraw while the SOL hub is busy (open pool / held reservation) — non-bypassable.
    require!(now >= busy_until, ErrorCode::MinerBusy);

    // Post-deactivation cooldown: wait 2× fulfillment timeout before pulling collateral.
    if deactivation_at != 0 {
        let cooldown_end = deactivation_at
            .checked_add(ctx.accounts.config.fulfillment_timeout_secs.saturating_mul(2))
            .ok_or(ErrorCode::Overflow)?;
        require!(now >= cooldown_end, ErrorCode::WithdrawCooldownActive);
    }

    // Move lamports the miner's collateral vault -> miner (program-owned → direct lamport math).
    ctx.accounts.collateral_vault.to_account_info().sub_lamports(amount)?;
    ctx.accounts.miner.to_account_info().add_lamports(amount)?;

    // Update ledger.
    ctx.accounts.miner_state.collateral = collateral
        .checked_sub(amount)
        .ok_or(ErrorCode::Overflow)?;

    emit!(CollateralWithdrawn {
        miner: ctx.accounts.miner.key(),
        amount,
        total: ctx.accounts.miner_state.collateral,
    });
    Ok(())
}
