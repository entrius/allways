use anchor_lang::prelude::*;
use solana_keccak_hasher::hashv;

use crate::constants::{MAX_TX_LEN, RESV_SEED, SWAP_SEED};
use crate::error::ErrorCode;
use crate::events::SwapClaimed;
use crate::state::{Reservation, Swap, SwapStatus};

/// Permissionless: the reservation holder records their source-tx hash on-chain, creating the Swap in
/// `PendingAttestation`. All terms (incl. the pinned user/payout) are copied from the immutable
/// Reservation — never from args — so a front-runner can't redirect the payout. Sets NO miner
/// obligation (no `timeout_at`, no `miner_state` write); validators attest via `vote_initiate`. One
/// live claim per reservation, enforced by `Reservation.claimed_swap_key`.
#[derive(Accounts)]
#[instruction(swap_key: [u8; 32])]
pub struct SubmitSwapClaim<'info> {
    #[account(mut)]
    pub caller: Signer<'info>,

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
    let (user, from_chain, to_chain, user_from_addr, user_to_addr, miner_from_addr, miner_to_addr, rate, sol_amount, from_amount, to_amount) = {
        let r = &ctx.accounts.reservation;
        (
            r.user,
            r.from_chain.clone(),
            r.to_chain.clone(),
            r.from_addr.clone(),
            r.user_to_addr.clone(),
            r.miner_from_addr.clone(),
            r.miner_to_addr.clone(),
            r.rate.clone(),
            r.sol_amount,
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
    swap.sol_amount = sol_amount;
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
