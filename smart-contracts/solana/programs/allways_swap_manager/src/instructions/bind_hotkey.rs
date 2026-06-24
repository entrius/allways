use anchor_lang::prelude::*;

use crate::constants::BIND_SEED;
use crate::events::HotkeyBound;
use crate::state::Binding;

/// A miner links its Solana pubkey to its Bittensor hotkey by storing the hotkey + an sr25519 signature
/// (by the hotkey, over the miner pubkey). The contract only STORES these — sr25519 verification is too
/// costly on-chain, so the validator verifies off-chain and enforces 1:1 + first-seen pinning. Signed by
/// the miner (proves the Solana side); overwrites in place on re-bind (the validator pins policy). Not
/// halt-gated — identity, no value movement.
#[derive(Accounts)]
pub struct BindHotkey<'info> {
    #[account(mut)]
    pub miner: Signer<'info>,

    #[account(
        init_if_needed,
        payer = miner,
        space = 8 + Binding::INIT_SPACE,
        seeds = [BIND_SEED, miner.key().as_ref()],
        bump,
    )]
    pub binding: Account<'info, Binding>,

    pub system_program: Program<'info, System>,
}

pub fn handler(ctx: Context<BindHotkey>, hotkey: [u8; 32], hotkey_sig: [u8; 64]) -> Result<()> {
    let now = Clock::get()?.unix_timestamp;
    let miner = ctx.accounts.miner.key();
    let bump = ctx.bumps.binding;

    let binding = &mut ctx.accounts.binding;
    if binding.miner == Pubkey::default() {
        binding.miner = miner;
        binding.bump = bump;
    }
    binding.hotkey = hotkey;
    binding.hotkey_sig = hotkey_sig;
    binding.bound_at = now;

    emit!(HotkeyBound { miner, hotkey, bound_at: now });
    Ok(())
}
