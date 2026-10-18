use anchor_lang::prelude::*;

use crate::constants::{CONFIG_SEED, MAX_VALIDATORS};
use crate::error::ErrorCode;
use crate::events::{ConfigChanged, HaltSet};
use crate::state::{Config, ValidatorInfo};

/// Admin-only config mutations (validator set + consensus threshold).
#[derive(Accounts)]
pub struct AdminConfig<'info> {
    pub admin: Signer<'info>,

    #[account(mut, seeds = [CONFIG_SEED], bump = config.bump, has_one = admin)]
    pub config: Box<Account<'info, Config>>,
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
    emit!(ConfigChanged { setting: "validators".to_string() });
    Ok(())
}

pub fn remove_validator(ctx: Context<AdminConfig>, validator: Pubkey) -> Result<()> {
    let config = &mut ctx.accounts.config;
    let before = config.validators.len();
    config.validators.retain(|v| v.key != validator);
    require!(config.validators.len() < before, ErrorCode::ValidatorNotFound);
    msg!("validator removed: {}", validator);
    emit!(ConfigChanged { setting: "validators".to_string() });
    Ok(())
}

pub fn set_consensus_threshold(ctx: Context<AdminConfig>, percent: u8) -> Result<()> {
    crate::validate::consensus_threshold(percent)?;
    ctx.accounts.config.consensus_threshold_percent = percent;
    msg!("consensus threshold = {}%", percent);
    emit!(ConfigChanged { setting: "consensus_threshold".to_string() });
    Ok(())
}

pub fn set_halted(ctx: Context<AdminConfig>, halted: bool) -> Result<()> {
    ctx.accounts.config.halted = halted;
    emit!(HaltSet { halted });
    msg!("halted = {}", halted);
    emit!(ConfigChanged { setting: "halted".to_string() });
    Ok(())
}

// --- Runtime config setters (port of ink! owner setters). Bounds are validated via the shared
// `crate::validate` helpers — the SAME rules `initialize` applies, so neither path can set a value
// the other would reject (review #8). 0 means "unbounded/disabled" where a validator allows it.

pub fn set_min_collateral(ctx: Context<AdminConfig>, amount: u64) -> Result<()> {
    crate::validate::collateral_bounds(amount, ctx.accounts.config.max_collateral)?;
    ctx.accounts.config.min_collateral = amount;
    msg!("min_collateral = {}", amount);
    emit!(ConfigChanged { setting: "min_collateral".to_string() });
    Ok(())
}

pub fn set_max_collateral(ctx: Context<AdminConfig>, amount: u64) -> Result<()> {
    crate::validate::collateral_bounds(ctx.accounts.config.min_collateral, amount)?;
    ctx.accounts.config.max_collateral = amount;
    msg!("max_collateral = {}", amount);
    emit!(ConfigChanged { setting: "max_collateral".to_string() });
    Ok(())
}

pub fn set_fulfillment_timeout(ctx: Context<AdminConfig>, secs: i64) -> Result<()> {
    crate::validate::fulfillment_timeout(secs)?;
    ctx.accounts.config.fulfillment_timeout_secs = secs;
    msg!("fulfillment_timeout_secs = {}", secs);
    emit!(ConfigChanged { setting: "fulfillment_timeout".to_string() });
    Ok(())
}

pub fn set_min_swap_amount(ctx: Context<AdminConfig>, amount: u64) -> Result<()> {
    crate::validate::min_swap_amount(amount)?;
    crate::validate::swap_bounds(amount, ctx.accounts.config.max_swap_amount)?;
    ctx.accounts.config.min_swap_amount = amount;
    msg!("min_swap_amount = {}", amount);
    emit!(ConfigChanged { setting: "min_swap_amount".to_string() });
    Ok(())
}

pub fn set_max_swap_amount(ctx: Context<AdminConfig>, amount: u64) -> Result<()> {
    crate::validate::swap_bounds(ctx.accounts.config.min_swap_amount, amount)?;
    ctx.accounts.config.max_swap_amount = amount;
    msg!("max_swap_amount = {}", amount);
    emit!(ConfigChanged { setting: "max_swap_amount".to_string() });
    Ok(())
}

// The TAO-backed pair of the two above, in rao. Same validators — the bounds rules are per-asset in
// units only, never in policy — so neither backing can be tuned to a shape the other would reject.

pub fn set_tao_min_swap_amount(ctx: Context<AdminConfig>, amount: u64) -> Result<()> {
    crate::validate::min_swap_amount(amount)?;
    crate::validate::swap_bounds(amount, ctx.accounts.config.tao_max_swap_amount)?;
    ctx.accounts.config.tao_min_swap_amount = amount;
    msg!("tao_min_swap_amount = {}", amount);
    emit!(ConfigChanged { setting: "tao_min_swap_amount".to_string() });
    Ok(())
}

pub fn set_tao_max_swap_amount(ctx: Context<AdminConfig>, amount: u64) -> Result<()> {
    crate::validate::swap_bounds(ctx.accounts.config.tao_min_swap_amount, amount)?;
    ctx.accounts.config.tao_max_swap_amount = amount;
    msg!("tao_max_swap_amount = {}", amount);
    emit!(ConfigChanged { setting: "tao_max_swap_amount".to_string() });
    Ok(())
}

/// Attested bond floor for activating the TAO backing, in rao — the twin of `set_min_collateral`.
pub fn set_tao_min_collateral(ctx: Context<AdminConfig>, amount: u64) -> Result<()> {
    crate::validate::remote_min_collateral(amount)?;
    ctx.accounts.config.tao_min_collateral = amount;
    msg!("tao_min_collateral = {}", amount);
    emit!(ConfigChanged { setting: "tao_min_collateral".to_string() });
    Ok(())
}

// The Base USDC hub's twins of the TAO setters above, in µUSDC — same validators, same rules.

pub fn set_usdc_min_swap_amount(ctx: Context<AdminConfig>, amount: u64) -> Result<()> {
    crate::validate::min_swap_amount(amount)?;
    crate::validate::swap_bounds(amount, ctx.accounts.config.usdc_max_swap_amount)?;
    ctx.accounts.config.usdc_min_swap_amount = amount;
    msg!("usdc_min_swap_amount = {}", amount);
    emit!(ConfigChanged { setting: "usdc_min_swap_amount".to_string() });
    Ok(())
}

pub fn set_usdc_max_swap_amount(ctx: Context<AdminConfig>, amount: u64) -> Result<()> {
    crate::validate::swap_bounds(ctx.accounts.config.usdc_min_swap_amount, amount)?;
    ctx.accounts.config.usdc_max_swap_amount = amount;
    msg!("usdc_max_swap_amount = {}", amount);
    emit!(ConfigChanged { setting: "usdc_max_swap_amount".to_string() });
    Ok(())
}

pub fn set_usdc_min_collateral(ctx: Context<AdminConfig>, amount: u64) -> Result<()> {
    crate::validate::remote_min_collateral(amount)?;
    ctx.accounts.config.usdc_min_collateral = amount;
    msg!("usdc_min_collateral = {}", amount);
    emit!(ConfigChanged { setting: "usdc_min_collateral".to_string() });
    Ok(())
}

/// Dead-man fuse: how stale ONE hub's heartbeat may get before that hub's entry is refused. Sized
/// off the heartbeat cadence, not the swap lifecycle.
pub fn set_attest_max_age(ctx: Context<AdminConfig>, hub: String, secs: i64) -> Result<()> {
    crate::validate::attest_max_age(secs)?;
    *crate::backing::max_age_mut(&mut ctx.accounts.config, &hub)? = secs;
    msg!("attest_max_age_secs[{}] = {}", hub, secs);
    emit!(ConfigChanged { setting: "attest_max_age".to_string() });
    Ok(())
}

/// Retire ONE hub's bond-vault epoch namespace. Call once per vault replacement, BEFORE relayers
/// point at the new vault. Increment-only — a decrement would let a stale round from a retired vault
/// satisfy the monotonic guard again.
pub fn bump_vault_generation(ctx: Context<AdminConfig>, hub: String) -> Result<()> {
    let generation = crate::backing::generation_mut(&mut ctx.accounts.config, &hub)?;
    *generation = generation.checked_add(1).ok_or(ErrorCode::Overflow)?;
    msg!("vault_generation[{}] = {}", hub, generation);
    emit!(ConfigChanged { setting: "vault_generation".to_string() });
    Ok(())
}

pub fn set_settlement_grace(ctx: Context<AdminConfig>, secs: i64) -> Result<()> {
    crate::validate::settlement_grace(secs)?;
    ctx.accounts.config.settlement_grace_secs = secs;
    msg!("settlement_grace_secs = {}", secs);
    emit!(ConfigChanged { setting: "settlement_grace".to_string() });
    Ok(())
}

pub fn set_reservation_ttl(ctx: Context<AdminConfig>, secs: i64) -> Result<()> {
    crate::validate::reservation_ttl(secs)?;
    ctx.accounts.config.reservation_ttl_secs = secs;
    msg!("reservation_ttl_secs = {}", secs);
    emit!(ConfigChanged { setting: "reservation_ttl".to_string() });
    Ok(())
}

pub fn set_reservation_fee(ctx: Context<AdminConfig>, lamports: u64) -> Result<()> {
    crate::validate::reservation_fee(lamports)?;
    ctx.accounts.config.reservation_fee_lamports = lamports;
    msg!("reservation_fee_lamports = {}", lamports);
    emit!(ConfigChanged { setting: "reservation_fee".to_string() });
    Ok(())
}

pub fn set_pool_window(ctx: Context<AdminConfig>, secs: i64) -> Result<()> {
    crate::validate::pool_window(secs)?;
    ctx.accounts.config.pool_window_secs = secs;
    msg!("pool_window_secs = {}", secs);
    emit!(ConfigChanged { setting: "pool_window".to_string() });
    Ok(())
}

pub fn set_finalize_window(ctx: Context<AdminConfig>, secs: i64) -> Result<()> {
    crate::validate::finalize_window(secs)?;
    ctx.accounts.config.finalize_window_secs = secs;
    msg!("finalize_window_secs = {}", secs);
    emit!(ConfigChanged { setting: "finalize_window".to_string() });
    Ok(())
}

pub fn set_weights_update_min_interval(ctx: Context<AdminConfig>, secs: i64) -> Result<()> {
    crate::validate::weights_update_min_interval(secs)?;
    ctx.accounts.config.weights_update_min_interval_secs = secs;
    msg!("weights_update_min_interval_secs = {}", secs);
    emit!(ConfigChanged { setting: "weights_update_min_interval".to_string() });
    Ok(())
}

pub fn set_max_total_extension(ctx: Context<AdminConfig>, secs: i64) -> Result<()> {
    crate::validate::max_total_extension(secs)?;
    ctx.accounts.config.max_total_extension_secs = secs;
    msg!("max_total_extension_secs = {}", secs);
    emit!(ConfigChanged { setting: "max_total_extension".to_string() });
    Ok(())
}
