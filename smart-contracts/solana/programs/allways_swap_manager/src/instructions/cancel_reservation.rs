use anchor_lang::prelude::*;

use crate::constants::{CONFIG_SEED, MINER_SEED, RESV_SEED};
use crate::events::ReservationCancelled;
use crate::state::{Config, MinerState, Reservation};

/// Admin clears a miner's reservation and frees its busy lock — the escape hatch for a miner stranded
/// in a stuck/abandoned reservation (otherwise it only clears at TTL expiry; review #4). Restores the
/// pre-#485 admin recourse, now also resetting the state-based `busy_until`.
#[derive(Accounts)]
pub struct CancelReservation<'info> {
    pub admin: Signer<'info>,

    #[account(seeds = [CONFIG_SEED], bump = config.bump, has_one = admin)]
    pub config: Account<'info, Config>,

    /// CHECK: identified by address only; used in PDA seeds + the miner_state constraint.
    pub miner: UncheckedAccount<'info>,

    #[account(
        mut,
        seeds = [MINER_SEED, miner.key().as_ref()],
        bump = miner_state.bump,
        constraint = miner_state.miner == miner.key(),
    )]
    pub miner_state: Account<'info, MinerState>,

    // Canonical bump (not stored) so this works whether the reservation slot was populated or not.
    #[account(mut, seeds = [RESV_SEED, miner.key().as_ref()], bump)]
    pub reservation: Account<'info, Reservation>,
}

pub fn handler(ctx: Context<CancelReservation>) -> Result<()> {
    let r = &mut ctx.accounts.reservation;
    r.bound_hash = [0u8; 32];
    r.from_addr = String::new();
    r.from_chain = String::new();
    r.to_chain = String::new();
    r.sol_amount = 0;
    r.from_amount = 0;
    r.to_amount = 0;
    r.miner_from_addr = String::new();
    r.miner_to_addr = String::new();
    r.rate = String::new();
    r.reserved_until = 0;

    // Free the busy lock so the miner can open new pools / deactivate / withdraw — but never override
    // an in-flight swap's lock (that has its own confirm/timeout clear path).
    if !ctx.accounts.miner_state.has_active_swap {
        ctx.accounts.miner_state.busy_until = 0;
    }

    emit!(ReservationCancelled { miner: ctx.accounts.miner.key() });
    Ok(())
}
