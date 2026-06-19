use anchor_lang::prelude::*;

use crate::constants::{CONFIG_SEED, MAX_VALIDATORS};
use crate::error::ErrorCode;
use crate::events::HaltSet;
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

pub fn set_halted(ctx: Context<AdminConfig>, halted: bool) -> Result<()> {
    ctx.accounts.config.halted = halted;
    emit!(HaltSet { halted });
    msg!("halted = {}", halted);
    Ok(())
}

// --- Runtime config setters ( port of ink! owner setters; 0 = "unset" where applicable) ---

pub fn set_min_collateral(ctx: Context<AdminConfig>, amount: u64) -> Result<()> {
    ctx.accounts.config.min_collateral = amount;
    msg!("min_collateral = {}", amount);
    Ok(())
}

pub fn set_max_collateral(ctx: Context<AdminConfig>, amount: u64) -> Result<()> {
    ctx.accounts.config.max_collateral = amount;
    msg!("max_collateral = {}", amount);
    Ok(())
}

pub fn set_fulfillment_timeout(ctx: Context<AdminConfig>, secs: i64) -> Result<()> {
    require!(secs >= 60, ErrorCode::InvalidAmount);
    ctx.accounts.config.fulfillment_timeout_secs = secs;
    msg!("fulfillment_timeout_secs = {}", secs);
    Ok(())
}

pub fn set_min_swap_amount(ctx: Context<AdminConfig>, amount: u64) -> Result<()> {
    require!(amount == 0 || amount >= 1000, ErrorCode::InvalidAmount);
    ctx.accounts.config.min_swap_amount = amount;
    msg!("min_swap_amount = {}", amount);
    Ok(())
}

pub fn set_max_swap_amount(ctx: Context<AdminConfig>, amount: u64) -> Result<()> {
    ctx.accounts.config.max_swap_amount = amount;
    msg!("max_swap_amount = {}", amount);
    Ok(())
}

pub fn set_reservation_ttl(ctx: Context<AdminConfig>, secs: i64) -> Result<()> {
    require!(secs > 0, ErrorCode::InvalidAmount);
    ctx.accounts.config.reservation_ttl_secs = secs;
    msg!("reservation_ttl_secs = {}", secs);
    Ok(())
}

pub fn set_reservation_fee(ctx: Context<AdminConfig>, lamports: u64) -> Result<()> {
    ctx.accounts.config.reservation_fee_lamports = lamports;
    msg!("reservation_fee_lamports = {}", lamports);
    Ok(())
}

pub fn set_pool_window(ctx: Context<AdminConfig>, secs: i64) -> Result<()> {
    require!(secs > 0, ErrorCode::InvalidAmount);
    ctx.accounts.config.pool_window_secs = secs;
    msg!("pool_window_secs = {}", secs);
    Ok(())
}

pub fn set_weights_update_min_interval(ctx: Context<AdminConfig>, secs: i64) -> Result<()> {
    require!(secs >= 0, ErrorCode::InvalidAmount);
    ctx.accounts.config.weights_update_min_interval_secs = secs;
    msg!("weights_update_min_interval_secs = {}", secs);
    Ok(())
}
