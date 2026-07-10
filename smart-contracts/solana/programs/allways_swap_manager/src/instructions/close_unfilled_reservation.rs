use anchor_lang::prelude::*;

use crate::constants::{MINER_SEED, RESV_SEED};
use crate::error::ErrorCode;
use crate::events::UnfilledReservationClosed;
use crate::state::{MinerState, Reservation};

/// Permissionless: reap a reservation that was drawn but never filled once its finalize window has
/// passed, freeing the miner immediately instead of letting it idle out the full bid-time busy lock.
/// The seat winner's reservation fee is already sunk (identical anti-grief economics to an abandoned
/// reserve). Does NOT close the account — `Reservation` is a reused per-miner PDA.
#[derive(Accounts)]
pub struct CloseUnfilledReservation<'info> {
    pub caller: Signer<'info>,

    /// CHECK: bound via the reservation/miner_state PDA seeds.
    pub miner: UncheckedAccount<'info>,

    #[account(
        mut,
        seeds = [MINER_SEED, miner.key().as_ref()],
        bump = miner_state.bump,
        constraint = miner_state.miner == miner.key(),
    )]
    pub miner_state: Account<'info, MinerState>,

    #[account(mut, seeds = [RESV_SEED, miner.key().as_ref()], bump = reservation.bump)]
    pub reservation: Account<'info, Reservation>,
}

pub fn handler(ctx: Context<CloseUnfilledReservation>) -> Result<()> {
    let now = Clock::get()?.unix_timestamp;
    let router = {
        let r = &ctx.accounts.reservation;
        // Reapable iff DRAWN (finalize_by != 0), never FILLED (reserved_until == 0 && created_at == 0),
        // and past its finalize deadline. `created_at == 0` is the load-bearing guard: a reservation
        // filled then consumed by vote_initiate also has reserved_until == 0, but created_at != 0 — so
        // this can't free a miner whose swap is in flight.
        require!(
            r.finalize_by != 0
                && r.reserved_until == 0
                && r.created_at == 0
                && now > r.finalize_by,
            ErrorCode::NotReapable
        );
        r.router
    };

    // Free the miner now (the bid set busy_until far ahead to cover a finalize that never came).
    ctx.accounts.miner_state.busy_until = now;

    let r = &mut ctx.accounts.reservation;
    r.router = Pubkey::default();
    r.finalize_by = 0;

    emit!(UnfilledReservationClosed {
        miner: ctx.accounts.miner.key(),
        router,
    });
    Ok(())
}
