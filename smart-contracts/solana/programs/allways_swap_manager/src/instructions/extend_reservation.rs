use anchor_lang::prelude::*;
use solana_keccak_hasher::hashv;

use crate::consensus::ensure_validator;
use crate::constants::{CONFIG_SEED, MINER_SEED, RESV_SEED, SRCLOCK_SEED};
use crate::error::ErrorCode;
use crate::events::ReservationExtended;
use crate::state::{Config, MinerState, Reservation, SourceLock};

/// A single validator slides a reservation's `reserved_until` forward while it waits on slow source-
/// chain confirmation. No quorum — an extension moves no funds, so worst case it only delays a slash
/// (still quorum-gated) up to the frozen ceiling. Monotonic + ceiling are the only guards; ignores
/// `halted` so in-flight swaps can finish.
#[derive(Accounts)]
#[instruction(target_at: i64, from_addr_hash: [u8; 32])]
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

    #[account(
        mut,
        seeds = [RESV_SEED, miner.key().as_ref(), reservation.collateral_chain.as_bytes()],
        bump = reservation.bump,
    )]
    pub reservation: Account<'info, Reservation>,

    /// V-C2: this reservation's source lock (created at finalize). Slid forward with reserved_until so an
    /// extended, still-unclaimed reservation can't outlive its lock and re-open the cross-hub collision.
    #[account(
        mut,
        seeds = [SRCLOCK_SEED, miner.key().as_ref(), reservation.from_chain.as_bytes(), from_addr_hash.as_ref()],
        bump = source_lock.bump,
    )]
    pub source_lock: Account<'info, SourceLock>,
}

pub fn handler(ctx: Context<ExtendReservation>, target_at: i64, from_addr_hash: [u8; 32]) -> Result<()> {
    let validator = ctx.accounts.validator.key();
    ensure_validator(&ctx.accounts.config, &validator)?;

    let now = Clock::get()?.unix_timestamp;
    // Bind from_addr_hash to the reservation's real source before it seeds the lock (swap_key idiom).
    require!(
        from_addr_hash == hashv(&[ctx.accounts.reservation.from_addr.as_bytes()]).to_bytes(),
        ErrorCode::SourceHashMismatch
    );
    let resv = &mut ctx.accounts.reservation;

    // Must still be live — don't resurrect an expired (overwritable) reservation.
    require!(resv.reserved_until != 0 && resv.reserved_until >= now, ErrorCode::NoReservation);
    require!(target_at > resv.reserved_until, ErrorCode::ExtensionNotLater);
    require!(target_at <= resv.max_extend_at, ErrorCode::ExtensionExceedsCeiling);

    resv.reserved_until = target_at;
    // V-C2: slide the source lock in lockstep so the extended reservation never outlives it.
    ctx.accounts.source_lock.reserved_until = target_at;
    // Forward-only on the hub's own slot: an extension may never shorten another obligation's lock.
    let bit = crate::backing::backing_bit(&resv.collateral_chain)?;
    ctx.accounts.miner_state.extend_busy(bit, target_at);

    emit!(ReservationExtended {
        miner: ctx.accounts.miner.key(),
        validator,
        reserved_until: target_at,
        collateral_chain: ctx.accounts.reservation.collateral_chain.clone(),
    });
    Ok(())
}
