use anchor_lang::prelude::*;

use crate::constants::{BIND_SEED, CONFIG_SEED, HOTKEY_BIND_SEED, MINER_SEED};
use crate::error::ErrorCode;
use crate::events::HotkeyBound;
use crate::state::{Binding, Config, HotkeyBinding, MinerState};

/// A miner links its Solana pubkey to its Bittensor hotkey by storing the hotkey + an sr25519 signature
/// (by the hotkey, over the miner pubkey). The contract only STORES these — sr25519 verification is too
/// costly on-chain, so the validator verifies off-chain. Signed by the miner (proves the Solana side).
/// The set-once `hotkey_binding` marker enforces hotkey→≤1 pubkey on-chain: the first pubkey to claim a
/// hotkey owns it forever, so a struck pubkey can't rotate + re-bind the same hotkey to dodge strikes.
/// The same miner may re-bind in place (refresh sig / change hotkey). Not halt-gated — identity, no value.
///
/// Gated to registered miners (MinerState exists + collateral >= min_collateral) and whitelisted
/// validators (Config.validators — they bind so peers can attribute their stake for the weights
/// vote): the set-once marker is claimable exactly once per hotkey, so a free, unauthenticated
/// `bind_hotkey` would let anyone enumerate metagraph hotkeys and squat them all, locking real
/// miners out of their identity. Both paths price a claimed hotkey at something scarce and
/// attributable — a full collateral stake, or a hand-approved validator slot.
/// Residual: a gated caller can still claim a hotkey that isn't theirs — the validator's off-chain
/// sr25519 verify rejects the binding for scoring, so the squat earns nothing but still blocks; that
/// deeper fix (validator-attested claims) was weighed and deferred.
#[derive(Accounts)]
#[instruction(hotkey: [u8; 32])]
pub struct BindHotkey<'info> {
    #[account(mut)]
    pub miner: Signer<'info>,

    #[account(seeds = [CONFIG_SEED], bump = config.bump)]
    pub config: Account<'info, Config>,

    /// Present on the miner path (created by `post_collateral`); whitelisted validators have none
    /// and pass the program id in its place (anchor's optional-account None).
    #[account(
        seeds = [MINER_SEED, miner.key().as_ref()],
        bump = miner_state.bump,
        constraint = miner_state.miner == miner.key(),
    )]
    pub miner_state: Option<Account<'info, MinerState>>,

    #[account(
        init_if_needed,
        payer = miner,
        space = 8 + Binding::INIT_SPACE,
        seeds = [BIND_SEED, miner.key().as_ref()],
        bump,
    )]
    pub binding: Account<'info, Binding>,

    /// Set-once reverse marker keyed by the hotkey. `init_if_needed` so the same miner can re-bind; the
    /// handler rejects a different pubkey trying to claim an already-owned hotkey.
    #[account(
        init_if_needed,
        payer = miner,
        space = 8 + HotkeyBinding::INIT_SPACE,
        seeds = [HOTKEY_BIND_SEED, hotkey.as_ref()],
        bump,
    )]
    pub hotkey_binding: Account<'info, HotkeyBinding>,

    pub system_program: Program<'info, System>,
}

pub fn handler(ctx: Context<BindHotkey>, hotkey: [u8; 32], hotkey_sig: [u8; 64]) -> Result<()> {
    // Identity gate: claiming a hotkey requires a live collateral stake (registered miner) or a
    // whitelisted-validator slot, so squatting the metagraph stays priced instead of rent-cheap.
    let signer = ctx.accounts.miner.key();
    let is_validator = ctx.accounts.config.validators.iter().any(|v| v.key == signer);
    let miner_ok = ctx
        .accounts
        .miner_state
        .as_ref()
        .is_some_and(|ms| ms.collateral >= ctx.accounts.config.min_collateral);
    require!(is_validator || miner_ok, ErrorCode::InsufficientCollateral);

    let now = Clock::get()?.unix_timestamp;
    let miner = ctx.accounts.miner.key();
    let binding_bump = ctx.bumps.binding;
    let hk_bump = ctx.bumps.hotkey_binding;

    // Set-once reverse pin: first pubkey to claim this hotkey owns it; anyone else is rejected.
    let hk = &mut ctx.accounts.hotkey_binding;
    if hk.miner == Pubkey::default() {
        hk.miner = miner;
        hk.bump = hk_bump;
    } else {
        require!(hk.miner == miner, ErrorCode::HotkeyAlreadyBound);
    }

    let binding = &mut ctx.accounts.binding;
    if binding.miner == Pubkey::default() {
        binding.miner = miner;
        binding.bump = binding_bump;
    }
    binding.hotkey = hotkey;
    binding.hotkey_sig = hotkey_sig;
    binding.bound_at = now;

    emit!(HotkeyBound { miner, hotkey, bound_at: now });
    Ok(())
}
