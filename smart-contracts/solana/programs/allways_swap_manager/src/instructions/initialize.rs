use anchor_lang::prelude::*;

use crate::constants::{CONFIG_SEED, CONFIG_VERSION, VAULT_SEED};
use crate::error::ErrorCode;
use crate::state::{Config, Vault};

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
    require!(
        (1..=100).contains(&consensus_threshold_percent),
        ErrorCode::InvalidThreshold
    );

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
    config.bump = ctx.bumps.config;

    let vault = &mut ctx.accounts.vault;
    vault.total_collateral = 0;
    vault.treasury_total = 0;
    vault.bump = ctx.bumps.vault;

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
