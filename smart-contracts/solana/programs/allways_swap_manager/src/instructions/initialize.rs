use anchor_lang::prelude::*;

use crate::constants::{
    CONFIG_SEED, CONFIG_VERSION, POOL_WINDOW_SECS, RESERVATION_FEE_LAMPORTS, TREASURY_SEED,
    VAULT_SEED, WEIGHTS_UPDATE_MIN_INTERVAL_SECS,
};
use crate::state::{Config, Treasury, Vault};

/// Create the singleton Config + native-SOL Vault PDAs. Records admin, collateral bounds,
/// fulfillment timeout (seconds), and the consensus threshold. Validator set starts empty
/// (populated via `add_validator`).
#[derive(Accounts)]
pub struct Initialize<'info> {
    #[account(mut)]
    pub admin: Signer<'info>,

    #[account(
        init,
        payer = admin,
        space = 8 + Config::INIT_SPACE,
        seeds = [CONFIG_SEED],
        bump,
    )]
    pub config: Account<'info, Config>,

    #[account(
        init,
        payer = admin,
        space = 8 + Vault::INIT_SPACE,
        seeds = [VAULT_SEED],
        bump,
    )]
    pub vault: Account<'info, Vault>,

    #[account(
        init,
        payer = admin,
        space = 8 + Treasury::INIT_SPACE,
        seeds = [TREASURY_SEED],
        bump,
    )]
    pub treasury: Account<'info, Treasury>,

    pub system_program: Program<'info, System>,
}

#[allow(clippy::too_many_arguments)]
pub fn handler(
    ctx: Context<Initialize>,
    min_collateral: u64,
    max_collateral: u64,
    fulfillment_timeout_secs: i64,
    consensus_threshold_percent: u8,
    min_swap_amount: u64,
    max_swap_amount: u64,
    reservation_ttl_secs: i64,
) -> Result<()> {
    // Same validators the admin setters use, so init can't seed a value a setter would later reject.
    crate::validate::consensus_threshold(consensus_threshold_percent)?;
    crate::validate::fulfillment_timeout(fulfillment_timeout_secs)?;
    crate::validate::reservation_ttl(reservation_ttl_secs)?;
    crate::validate::min_swap_amount(min_swap_amount)?;
    crate::validate::swap_bounds(min_swap_amount, max_swap_amount)?;
    crate::validate::collateral_bounds(min_collateral, max_collateral)?;

    let config = &mut ctx.accounts.config;
    config.admin = ctx.accounts.admin.key();
    config.version = CONFIG_VERSION;
    config.min_collateral = min_collateral;
    config.max_collateral = max_collateral;
    config.fulfillment_timeout_secs = fulfillment_timeout_secs;
    config.min_swap_amount = min_swap_amount;
    config.max_swap_amount = max_swap_amount;
    config.reservation_ttl_secs = reservation_ttl_secs;
    config.consensus_threshold_percent = consensus_threshold_percent;
    config.validators = Vec::new();
    config.last_weights_update = 0;
    config.halted = false;
    config.reservation_fee_lamports = RESERVATION_FEE_LAMPORTS;
    config.pool_window_secs = POOL_WINDOW_SECS;
    config.weights_update_min_interval_secs = WEIGHTS_UPDATE_MIN_INTERVAL_SECS;
    config.bump = ctx.bumps.config;

    let vault = &mut ctx.accounts.vault;
    vault.total_collateral = 0;
    vault.bump = ctx.bumps.vault;

    let treasury = &mut ctx.accounts.treasury;
    treasury.total = 0;
    treasury.bump = ctx.bumps.treasury;

    msg!(
        "initialized: admin={}, threshold={}%, min_collateral={}, max_collateral={}, timeout_secs={}",
        config.admin,
        config.consensus_threshold_percent,
        config.min_collateral,
        config.max_collateral,
        config.fulfillment_timeout_secs
    );
    Ok(())
}
