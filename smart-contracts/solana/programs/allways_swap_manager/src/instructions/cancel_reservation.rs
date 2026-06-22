use anchor_lang::prelude::*;

use crate::constants::{CONFIG_SEED, MINER_SEED, RESV_SEED};
use crate::error::ErrorCode;
use crate::events::ReservationCancelled;
use crate::state::{Config, MinerState, Reservation};

/// Admin clears a miner's ACTIVE reservation and frees its busy lock — the escape hatch for a miner
/// stranded in an abandoned reservation (else it only clears at TTL expiry; review #4). Requires an
/// active reservation (use `cancel_pool` for an un-resolved open pool); also resets `busy_until`.
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

    // Canonical bump (not stored). The PDA must already exist (it does once a pool was opened/resolved).
    #[account(mut, seeds = [RESV_SEED, miner.key().as_ref()], bump)]
    pub reservation: Account<'info, Reservation>,
}

pub fn handler(ctx: Context<CancelReservation>) -> Result<()> {
    // Only an ACTIVE reservation is cancellable. An active reservation implies resolve_pool already
    // reset the pool, so freeing busy_until here can't strand an open pool that resolve_pool would
    // later match an inactive miner against — preserving the busy ⟹ active invariant. For an
    // un-resolved open pool, the admin uses `cancel_pool` instead.
    require!(
        ctx.accounts.reservation.reserved_until != 0,
        ErrorCode::NoReservation
    );

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

    // Safe to free the busy lock: an active reservation can't coexist with an in-flight swap
    // (vote_initiate consumes the reservation when it creates the swap).
    ctx.accounts.miner_state.busy_until = 0;

    emit!(ReservationCancelled { miner: ctx.accounts.miner.key() });
    Ok(())
}
