use anchor_lang::prelude::*;

use crate::constants::{CONFIG_SEED, MAX_VALIDATORS};
use crate::error::ErrorCode;
use crate::state::{Config, ValidatorInfo};

/// Admin-only config mutations (validator set + consensus threshold).
#[derive(Accounts)]
pub struct AdminConfig<'info> {
    pub admin: Signer<'info>,

    #[account(mut, seeds = [CONFIG_SEED], bump = config.bump, has_one = admin)]
    pub config: Account<'info, Config>,
}

pub fn add_validator(ctx: Context<AdminConfig>, validator: Pubkey, weight: u64) -> Result<()> {
    let config = &mut ctx.accounts.config;
    require!(
        !config.validators.iter().any(|v| v.key == validator),
        ErrorCode::ValidatorExists
    );
    require!(config.validators.len() < MAX_VALIDATORS, ErrorCode::ValidatorSetFull);
    config.validators.push(ValidatorInfo { key: validator, weight });
    msg!("validator added: {} (weight {})", validator, weight);
    Ok(())
}

pub fn remove_validator(ctx: Context<AdminConfig>, validator: Pubkey) -> Result<()> {
    let config = &mut ctx.accounts.config;
    let before = config.validators.len();
    config.validators.retain(|v| v.key != validator);
    require!(config.validators.len() < before, ErrorCode::ValidatorNotFound);
    msg!("validator removed: {}", validator);
    Ok(())
}

/// Set a validator's draw weight (the seam a future stake oracle writes through). Consensus is
/// count-based, so this only affects the Phase 9 lottery draw.
pub fn set_validator_weight(ctx: Context<AdminConfig>, validator: Pubkey, weight: u64) -> Result<()> {
    let config = &mut ctx.accounts.config;
    let entry = config
        .validators
        .iter_mut()
        .find(|v| v.key == validator)
        .ok_or(ErrorCode::ValidatorNotFound)?;
    entry.weight = weight;
    msg!("validator weight: {} = {}", validator, weight);
    Ok(())
}

pub fn set_consensus_threshold(ctx: Context<AdminConfig>, percent: u8) -> Result<()> {
    require!((1..=100).contains(&percent), ErrorCode::InvalidThreshold);
    ctx.accounts.config.consensus_threshold_percent = percent;
    msg!("consensus threshold = {}%", percent);
    Ok(())
}
