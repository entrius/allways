use anchor_lang::prelude::*;

use crate::constants::{RESV_SEED, SWAP_SEED};
use crate::error::ErrorCode;
use crate::events::StaleClaimClosed;
use crate::state::{Reservation, Swap, SwapStatus};

/// Permissionless reaper for an orphaned `PendingAttestation` claim whose reservation has expired (or
/// was superseded). Closes the Swap (rent → caller) and frees the reservation's claim slot. No
/// `miner_state` change — the claim never set one.
#[derive(Accounts)]
#[instruction(swap_key: [u8; 32])]
pub struct CloseStaleClaim<'info> {
    #[account(mut)]
    pub caller: Signer<'info>,

    /// CHECK: bound via the swap `has_one` + the reservation PDA seeds.
    pub miner: UncheckedAccount<'info>,

    #[account(mut, seeds = [RESV_SEED, miner.key().as_ref()], bump = reservation.bump)]
    pub reservation: Box<Account<'info, Reservation>>,

    #[account(
        mut,
        seeds = [SWAP_SEED, swap_key.as_ref()],
        bump = swap.bump,
        has_one = miner,
    )]
    pub swap: Box<Account<'info, Swap>>,
}

pub fn handler(ctx: Context<CloseStaleClaim>, swap_key: [u8; 32]) -> Result<()> {
    require!(
        ctx.accounts.swap.status == SwapStatus::PendingAttestation,
        ErrorCode::NotPending
    );

    let now = Clock::get()?.unix_timestamp;
    {
        let resv = &mut ctx.accounts.reservation;
        // Stale = reservation expired, OR its live-claim slot no longer points at this swap (the
        // reservation was re-resolved / consumed by a different claim).
        let stale = resv.reserved_until < now || resv.claimed_swap_key != swap_key;
        require!(stale, ErrorCode::ClaimNotExpired);
        if resv.claimed_swap_key == swap_key {
            resv.claimed_swap_key = [0u8; 32];
        }
    }

    let miner = ctx.accounts.swap.miner;
    ctx.accounts.swap.close(ctx.accounts.caller.to_account_info())?;
    emit!(StaleClaimClosed { swap_key, miner });
    Ok(())
}
