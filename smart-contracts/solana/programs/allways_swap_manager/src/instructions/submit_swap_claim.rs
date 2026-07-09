use anchor_lang::prelude::*;
use solana_keccak_hasher::hashv;

use crate::consensus::ensure_validator;
use crate::constants::{CONFIG_SEED, MAX_TX_LEN, RESV_SEED, SWAP_SEED};
use crate::error::ErrorCode;
use crate::events::SwapClaimed;
use crate::state::{Config, Reservation, Swap, SwapStatus};

/// Validator-relayed: a whitelisted validator records the winner's source-tx hash on-chain (the user
/// flags one down with their deposit; only validators can settle anyway, as the BTC oracle), creating
/// the Swap in `PendingAttestation`. Gating the caller to validators removes the front-run/squat surface
/// of a permissionless claim — an anonymous attacker has no instruction to spam. All terms (incl. the
/// pinned user/payout) are still copied from the immutable Reservation, never from args. Sets NO miner
/// obligation; validators attest via `vote_initiate`. One live claim per reservation
/// (`Reservation.claimed_swap_key`).
#[derive(Accounts)]
#[instruction(swap_key: [u8; 32])]
pub struct SubmitSwapClaim<'info> {
    #[account(mut)]
    pub caller: Signer<'info>,

    #[account(seeds = [CONFIG_SEED], bump = config.bump)]
    pub config: Account<'info, Config>,

    /// CHECK: bound via the reservation/swap PDA seeds.
    pub miner: UncheckedAccount<'info>,

    #[account(mut, seeds = [RESV_SEED, miner.key().as_ref()], bump = reservation.bump)]
    pub reservation: Box<Account<'info, Reservation>>,

    /// Boxed (String-heavy) to keep it off the BPF stack.
    #[account(
        init,
        payer = caller,
        space = 8 + Swap::INIT_SPACE,
        seeds = [SWAP_SEED, swap_key.as_ref()],
        bump,
    )]
    pub swap: Box<Account<'info, Swap>>,

    pub system_program: Program<'info, System>,
}

pub fn handler(
    ctx: Context<SubmitSwapClaim>,
    swap_key: [u8; 32],
    from_tx_hash: String,
    from_tx_block: u32,
) -> Result<()> {
    // Validator-relay gate: only a whitelisted validator may put a deposit on-chain (no anonymous
    // claim to squat / make others RPC-chase). Participation stays open — anyone wins the pool draw;
    // the winner just flags down a validator to relay + attest.
    ensure_validator(&ctx.accounts.config, &ctx.accounts.caller.key())?;

    require!(from_tx_hash.len() <= MAX_TX_LEN, ErrorCode::StringTooLong);
    require!(
        swap_key == hashv(&[from_tx_hash.as_bytes()]).to_bytes(),
        ErrorCode::SwapKeyMismatch
    );

    let now = Clock::get()?.unix_timestamp;
    {
        let resv = &ctx.accounts.reservation;
        require!(
            resv.reserved_until != 0 && resv.reserved_until >= now,
            ErrorCode::NoReservation
        );
        require!(resv.claimed_swap_key == [0u8; 32], ErrorCode::ClaimAlreadyExists);
    }

    let miner_key = ctx.accounts.miner.key();
    let swap_bump = ctx.bumps.swap;

    // Snapshot the pinned terms out of the reservation (immutable source of truth).
    let (user, from_chain, to_chain, user_from_addr, user_to_addr, miner_from_addr, miner_to_addr, rate, collateral_amount, from_amount, to_amount) = {
        let r = &ctx.accounts.reservation;
        (
            r.user,
            r.from_chain.clone(),
            r.to_chain.clone(),
            r.from_addr.clone(),
            r.user_to_addr.clone(),
            r.miner_from_addr.clone(),
            r.miner_to_addr.clone(),
            r.rate,
            r.collateral_amount,
            r.from_amount,
            r.to_amount,
        )
    };

    let swap = &mut ctx.accounts.swap;
    swap.user = user;
    swap.miner = miner_key;
    swap.from_chain = from_chain;
    swap.to_chain = to_chain;
    swap.user_from_addr = user_from_addr;
    swap.user_to_addr = user_to_addr;
    swap.miner_from_addr = miner_from_addr;
    swap.miner_to_addr = miner_to_addr;
    swap.rate = rate;
    swap.collateral_amount = collateral_amount;
    swap.from_amount = from_amount;
    swap.to_amount = to_amount;
    swap.from_tx_hash = from_tx_hash.clone();
    swap.from_tx_block = from_tx_block;
    swap.to_tx_hash = String::new();
    swap.to_tx_block = 0;
    swap.status = SwapStatus::PendingAttestation;
    swap.initiated_at = 0;
    swap.timeout_at = 0;
    swap.max_extend_at = 0;
    swap.fulfilled_at = 0;
    swap.bump = swap_bump;

    ctx.accounts.reservation.claimed_swap_key = swap_key;

    emit!(SwapClaimed {
        swap_key,
        miner: miner_key,
        user,
        from_tx_hash,
        from_tx_block,
    });
    Ok(())
}
