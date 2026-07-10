use anchor_lang::prelude::*;

use crate::consensus::ensure_validator;
use crate::constants::{CONFIG_SEED, MINER_SEED, RESV_SEED};
use crate::error::ErrorCode;
use crate::events::ReservationExtended;
use crate::state::{Config, MinerState, Reservation};

/// A single validator slides a reservation's `reserved_until` forward while it waits on slow source-
/// chain confirmation. No quorum — an extension moves no funds, so worst case it only delays a slash
/// (still quorum-gated) up to the frozen ceiling. Monotonic + ceiling are the only guards; ignores
/// `halted` so in-flight swaps can finish.
#[derive(Accounts)]
pub struct ExtendReservation<'info> {
    pub validator: Signer<'info>,

    #[account(seeds = [CONFIG_SEED], bump = config.bump)]
    pub config: Account<'info, Config>,

    /// CHECK: identified by address only; bound via PDA seeds + the miner_state constraint.
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

pub fn handler(ctx: Context<ExtendReservation>, target_at: i64) -> Result<()> {
    let validator = ctx.accounts.validator.key();
    ensure_validator(&ctx.accounts.config, &validator)?;

    let now = Clock::get()?.unix_timestamp;
    let resv = &mut ctx.accounts.reservation;

    // Must still be live — don't resurrect an expired (overwritable) reservation.
    require!(resv.reserved_until != 0 && resv.reserved_until >= now, ErrorCode::NoReservation);
    require!(target_at > resv.reserved_until, ErrorCode::ExtensionNotLater);
    require!(target_at <= resv.max_extend_at, ErrorCode::ExtensionExceedsCeiling);

    resv.reserved_until = target_at;
    ctx.accounts.miner_state.busy_until = target_at;

    emit!(ReservationExtended {
        miner: ctx.accounts.miner.key(),
        validator,
        reserved_until: target_at,
    });
    Ok(())
}
