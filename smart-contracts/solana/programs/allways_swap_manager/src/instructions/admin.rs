use anchor_lang::prelude::*;

use crate::constants::{CONFIG_SEED, MAX_VALIDATORS};
use crate::error::ErrorCode;
use crate::state::Config;

/// Admin-only config mutations (validator set + consensus threshold).
#[derive(Accounts)]
pub struct AdminConfig<'info> {
    pub admin: Signer<'info>,

    #[account(mut, seeds = [CONFIG_SEED], bump = config.bump, has_one = admin)]
    pub config: Account<'info, Config>,
}

pub fn add_validator(ctx: Context<AdminConfig>, validator: Pubkey) -> Result<()> {
    let config = &mut ctx.accounts.config;
    require!(!config.validators.contains(&validator), ErrorCode::ValidatorExists);
    require!(config.validators.len() < MAX_VALIDATORS, ErrorCode::ValidatorSetFull);
    config.validators.push(validator);
    msg!("validator added: {}", validator);
    Ok(())
}

pub fn remove_validator(ctx: Context<AdminConfig>, validator: Pubkey) -> Result<()> {
    let config = &mut ctx.accounts.config;
    let before = config.validators.len();
    config.validators.retain(|v| v != &validator);
    require!(config.validators.len() < before, ErrorCode::ValidatorNotFound);
    msg!("validator removed: {}", validator);
    Ok(())
}

pub fn set_consensus_threshold(ctx: Context<AdminConfig>, percent: u8) -> Result<()> {
    require!((1..=100).contains(&percent), ErrorCode::InvalidThreshold);
    ctx.accounts.config.consensus_threshold_percent = percent;
    msg!("consensus threshold = {}%", percent);
    Ok(())
}
