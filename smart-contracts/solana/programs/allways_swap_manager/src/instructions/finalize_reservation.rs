use anchor_lang::prelude::*;

use crate::backing;
use crate::constants::{required_collateral, CONFIG_SEED, MAX_ADDR_LEN, MINER_SEED, RESV_SEED};
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

    // Self-dealing guard: a miner may not be its own taker. Two tiny self-swaps would otherwise buy
    // permanent eligibility (successful_swaps >= 2) and pad fill volume at zero real cost. A sybil
    // (second wallet, same operator) is out of on-chain reach — the scorer's volume exclusion owns
    // that half. Also reject the default pubkey: a timeout would "refund" the slash to the burn address.
    require!(user != ctx.accounts.miner.key(), ErrorCode::SelfSwapNotAllowed);
    require!(user != Pubkey::default(), ErrorCode::InvalidUser);

    let now = Clock::get()?.unix_timestamp;
    let cfg = &ctx.accounts.config;

    // Fill exactly once, and only inside the finalize window. Both sentinels are load-bearing:
    // `reserved_until == 0` alone means "not currently live", which a reservation CONSUMED by
    // `vote_initiate` also satisfies (it zeroes reserved_until and frees the claim slot). Only
    // `created_at == 0` says "drawn but never filled". Without it, the seat winner could re-fill a
    // consumed reservation while `finalize_by` is still ahead, minting a second live hold on a miner
    // that already has an active swap — and each fill's 1.10x collateral gate is checked in isolation.
    // Same guard `close_unfilled_reservation` relies on; do not let these two drift apart.
    require!(
        ctx.accounts.reservation.reserved_until == 0 && ctx.accounts.reservation.created_at == 0,
        ErrorCode::AlreadyFilled
    );
    require!(
        now <= ctx.accounts.reservation.finalize_by,
        ErrorCode::FinalizeWindowExpired
    );

    // Everything below routes off the reservation's pinned backing — never off the pair (D4).
    let backing = &ctx.accounts.reservation.collateral_chain;

    // Swap-size bounds (moved from open_or_request — the amount is only known now), in the BACKING
    // asset's own units: lamports for "sol", rao for "tao". Never converted through the rate.
    let (min_swap, max_swap) = backing::swap_bounds(cfg, backing)?;
    require!(
        min_swap == 0 || collateral_amount >= min_swap,
        ErrorCode::AmountBelowMin
    );
    require!(
        max_swap == 0 || collateral_amount <= max_swap,
        ErrorCode::AmountAboveMax
    );

    // Bind `collateral_amount` to the leg denominated in the backing asset — the leg lookup that
    // closes the understated-collateral hole for any backing, not just SOL.
    let expected = backing::collateral_leg_amount(
        backing,
        &ctx.accounts.reservation.from_chain,
        from_amount,
        &ctx.accounts.reservation.to_chain,
        to_amount,
    )?;
    require!(collateral_amount as u128 == expected, ErrorCode::InvalidAmount);

    // Over-collateralization gate: hold 1.10× THIS fill in the backing purse up front. Collateral only
    // rises while busy (withdraw is locked), so passing here means vote_initiate's identical gate can't
    // later strand a user who has already sent source funds.
    let purse = backing::backing_purse(backing, &ctx.accounts.miner_state)?;
    require!(
        purse >= required_collateral(collateral_amount),
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
