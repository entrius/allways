use anchor_lang::prelude::*;

use crate::constants::{
    required_collateral, CONFIG_SEED, MAX_ADDR_LEN, MINER_SEED, NUMERAIRE_CHAIN, RESV_SEED,
};
use crate::error::ErrorCode;
use crate::events::ReservationFilled;
use crate::state::{Config, MinerState, Reservation};

/// The seat winner (`reservation.router`) fills the reservation it won at the draw: names the taker +
/// amounts and sets `reserved_until`, making the reservation live (sendable/claimable). Only the router
/// may call this. The swap-size bounds + collateral gate + the collateral bind run here — the amount
/// is unknown at bid time, so these moved from `open_or_request`.
#[derive(Accounts)]
pub struct FinalizeReservation<'info> {
    /// The seat winner. Must equal `reservation.router` (constraint below).
    pub router: Signer<'info>,

    #[account(seeds = [CONFIG_SEED], bump = config.bump)]
    pub config: Account<'info, Config>,

    /// CHECK: identified by address only; bound via the reservation/miner_state PDA seeds.
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
        seeds = [RESV_SEED, miner.key().as_ref()],
        bump = reservation.bump,
        constraint = reservation.router == router.key() @ ErrorCode::NoReservation,
    )]
    pub reservation: Account<'info, Reservation>,
}

pub fn handler(
    ctx: Context<FinalizeReservation>,
    user: Pubkey,
    user_from_addr: String,
    user_to_addr: String,
    collateral_amount: u64,
    from_amount: u128,
    to_amount: u128,
) -> Result<()> {
    require!(!ctx.accounts.config.halted, ErrorCode::SystemHalted);
    require!(
        !user_from_addr.is_empty() && !user_to_addr.is_empty(),
        ErrorCode::EmptyField
    );
    require!(
        user_from_addr.len() <= MAX_ADDR_LEN && user_to_addr.len() <= MAX_ADDR_LEN,
        ErrorCode::StringTooLong
    );

    let now = Clock::get()?.unix_timestamp;
    let cfg = &ctx.accounts.config;

    // Fill exactly once, and only inside the finalize window.
    require!(
        ctx.accounts.reservation.reserved_until == 0,
        ErrorCode::AlreadyFilled
    );
    require!(
        now <= ctx.accounts.reservation.finalize_by,
        ErrorCode::FinalizeWindowExpired
    );

    // Swap-size bounds (moved from open_or_request — the amount is only known now).
    require!(
        cfg.min_swap_amount == 0 || collateral_amount >= cfg.min_swap_amount,
        ErrorCode::AmountBelowMin
    );
    require!(
        cfg.max_swap_amount == 0 || collateral_amount <= cfg.max_swap_amount,
        ErrorCode::AmountAboveMax
    );

    // SOL-collateral module: bind `collateral_amount` to the SOL leg. This is what closes the
    // understated-collateral hole. NOT a global "sol leg required" rule — scoped to SOL-collateralized
    // swaps so a future TAO-collateral module is an added branch, not an untangle.
    let expected: u128 = if ctx.accounts.reservation.from_chain == NUMERAIRE_CHAIN {
        from_amount
    } else {
        to_amount
    };
    require!(collateral_amount as u128 == expected, ErrorCode::InvalidAmount);

    // Over-collateralization gate: hold 1.10× THIS fill up front. Collateral only rises while busy
    // (withdraw is locked), so passing here means vote_initiate's identical gate can't later strand a
    // user who has already sent source funds.
    require!(
        ctx.accounts.miner_state.collateral >= required_collateral(collateral_amount),
        ErrorCode::InsufficientCollateral
    );

    let ttl = cfg.reservation_ttl_secs;
    let extension_budget = cfg.max_total_extension_secs;
    let miner_key = ctx.accounts.miner.key();
    let router_key = ctx.accounts.router.key();

    let (from_chain, to_chain, reserved_until) = {
        let r = &mut ctx.accounts.reservation;
        r.user = user; // pin taker + payout so the validator-relayed claim can't redirect it
        r.from_addr = user_from_addr;
        r.user_to_addr = user_to_addr;
        r.collateral_amount = collateral_amount;
        r.from_amount = from_amount;
        r.to_amount = to_amount;
        r.created_at = now; // source-freshness floor: the deposit must postdate the FILL, not the draw
        r.reserved_until = now.saturating_add(ttl);
        r.max_extend_at = r.reserved_until.saturating_add(extension_budget);
        (r.from_chain.clone(), r.to_chain.clone(), r.reserved_until)
    };

    // Tighten the busy lock to the filled reservation's actual life. The bid set it conservatively to
    // cover the whole finalize window; now that we've filled, `now + ttl` is exact (never shorter than
    // reserved_until, so no live-reservation hole).
    ctx.accounts.miner_state.busy_until = reserved_until;

    emit!(ReservationFilled {
        miner: miner_key,
        router: router_key,
        user,
        from_chain,
        to_chain,
        collateral_amount,
        from_amount,
        to_amount,
        reserved_until,
    });
    Ok(())
}
