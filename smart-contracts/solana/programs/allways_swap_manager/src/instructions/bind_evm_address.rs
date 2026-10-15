use anchor_lang::prelude::*;

use crate::constants::{CONFIG_SEED, EVM_ADDR_BIND_SEED, EVM_BIND_SEED, MINER_SEED};
use crate::error::ErrorCode;
use crate::events::EvmAddressBound;
use crate::state::{Config, EvmAddressBinding, EvmBinding, MinerState};

/// A miner names the EVM address holding its Base bond; the vault's `lockBond(solanaPubkey)` names it
/// back, and validators attest only when both agree. Set-once both ways like `bind_hotkey` (V-M1): a
/// rebind could move a seizure mid-swap. Gated exactly like `bind_hotkey`, so claims stay priced.
#[derive(Accounts)]
#[instruction(evm_address: [u8; 20])]
pub struct BindEvmAddress<'info> {
    #[account(mut)]
    pub miner: Signer<'info>,

    #[account(seeds = [CONFIG_SEED], bump = config.bump)]
    pub config: Box<Account<'info, Config>>,

    /// Present on the miner path; whitelisted validators pass the program id (anchor's None).
    #[account(
        seeds = [MINER_SEED, miner.key().as_ref()],
        bump = miner_state.bump,
        constraint = miner_state.miner == miner.key(),
    )]
    pub miner_state: Option<Account<'info, MinerState>>,

    #[account(
        init_if_needed,
        payer = miner,
        space = 8 + EvmBinding::INIT_SPACE,
        seeds = [EVM_BIND_SEED, miner.key().as_ref()],
        bump,
    )]
    pub evm_binding: Account<'info, EvmBinding>,

    #[account(
        init_if_needed,
        payer = miner,
        space = 8 + EvmAddressBinding::INIT_SPACE,
        seeds = [EVM_ADDR_BIND_SEED, evm_address.as_ref()],
        bump,
    )]
    pub evm_address_binding: Account<'info, EvmAddressBinding>,

    pub system_program: Program<'info, System>,
}

pub fn handler(ctx: Context<BindEvmAddress>, evm_address: [u8; 20]) -> Result<()> {
    let signer = ctx.accounts.miner.key();
    let is_validator = ctx.accounts.config.validators.iter().any(|v| v.key == signer);
    let miner_ok = ctx
        .accounts
        .miner_state
        .as_ref()
        .is_some_and(|ms| ms.collateral >= ctx.accounts.config.min_collateral);
    require!(is_validator || miner_ok, ErrorCode::InsufficientCollateral);
    // The zero address pads to the zero bond key, which the verdict reads as "never bound".
    require!(evm_address != [0u8; 20], ErrorCode::EvmAddressZero);

    let marker = &mut ctx.accounts.evm_address_binding;
    if marker.miner == Pubkey::default() {
        marker.miner = signer;
        marker.bump = ctx.bumps.evm_address_binding;
    } else {
        require!(marker.miner == signer, ErrorCode::EvmAddressAlreadyBound);
    }

    let binding = &mut ctx.accounts.evm_binding;
    if binding.miner != Pubkey::default() {
        // Same-address re-call is a no-op, so a retried bind tx can't fail a scripted onboarding.
        require!(binding.evm_address == evm_address, ErrorCode::EvmAddressChangeForbidden);
        return Ok(());
    }
    let now = Clock::get()?.unix_timestamp;
    binding.miner = signer;
    binding.evm_address = evm_address;
    binding.bound_at = now;
    binding.bump = ctx.bumps.evm_binding;

    emit!(EvmAddressBound { miner: signer, evm_address, bound_at: now });
    Ok(())
}

/// The 32-byte bond key naming a Base bond (the vault keys bonds by miner address): the EVM address
/// left-padded, exactly as `abi.encode(address)` lays it out.
pub fn evm_bond_key(evm_address: &[u8; 20]) -> [u8; 32] {
    let mut key = [0u8; 32];
    key[12..].copy_from_slice(evm_address);
    key
}
