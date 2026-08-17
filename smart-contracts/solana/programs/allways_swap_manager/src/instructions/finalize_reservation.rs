use anchor_lang::prelude::*;

use crate::backing;
use solana_keccak_hasher::hashv;

use crate::constants::{
    required_collateral, ATTEST_SEED, CONFIG_SEED, MAX_ADDR_LEN, MINER_SEED, RESV_SEED, SRCLOCK_SEED,
};
use crate::error::ErrorCode;
use crate::events::ReservationFilled;
use crate::state::{BondAttestation, Config, MinerState, Reservation, SourceLock};

/// The seat winner (`reservation.router`) fills the reservation it won at the draw: names the taker +
/// amounts and sets `reserved_until`, making the reservation live (sendable/claimable). Only the router
/// may call this. The swap-size bounds + collateral gate + the collateral bind run here — the amount
/// is unknown at bid time, so these moved from `open_or_request`.
#[derive(Accounts)]
#[instruction(
    user: Pubkey,
    user_from_addr: String,
    user_to_addr: String,
    collateral_amount: u64,
    from_amount: u128,
    to_amount: u128,
    from_addr_hash: [u8; 32],
)]
pub struct FinalizeReservation<'info> {
    /// The seat winner. Must equal `reservation.router` (constraint below).
    #[account(mut)]
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
        seeds = [RESV_SEED, miner.key().as_ref(), reservation.collateral_chain.as_bytes()],
        bump = reservation.bump,
        constraint = reservation.router == router.key() @ ErrorCode::NoReservation,
    )]
    pub reservation: Account<'info, Reservation>,

    /// The bond attestation for the reservation's pinned backing — required for any backing but "sol",
    /// which reads the local vault ledger instead. Seeds bind it to (this miner, that backing).
    #[account(
        seeds = [ATTEST_SEED, miner.key().as_ref(), reservation.collateral_chain.as_bytes()],
        bump,
    )]
    pub attestation: Option<Account<'info, BondAttestation>>,

    /// V-C2 source lock, keyed by (miner, from_chain, keccak(from_addr)) — NOT by backing, so it is the
    /// SAME PDA for a colliding reservation on ANY other hub sharing that source chain, but distinct for a
    /// concurrent swap on a different from_chain (a deposit is chain-specific; one can't satisfy both).
    /// `init_if_needed` reuses a stale lock; the handler rejects a still-live one and verifies the hash.
    #[account(
        init_if_needed,
        payer = router,
        space = 8 + SourceLock::INIT_SPACE,
        seeds = [SRCLOCK_SEED, miner.key().as_ref(), reservation.from_chain.as_bytes(), from_addr_hash.as_ref()],
        bump,
    )]
    pub source_lock: Account<'info, SourceLock>,

    pub system_program: Program<'info, System>,
}

pub fn handler(
    ctx: Context<FinalizeReservation>,
    user: Pubkey,
    user_from_addr: String,
    user_to_addr: String,
    collateral_amount: u64,
    from_amount: u128,
    to_amount: u128,
    from_addr_hash: [u8; 32],
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
    // V-C2: `from_addr_hash` seeds the source_lock PDA, so bind it to the real address (as swap_key binds
    // to from_tx_hash) — else a caller could seed a lock for a DIFFERENT address than it declares.
    require!(
        from_addr_hash == hashv(&[user_from_addr.as_bytes()]).to_bytes(),
        ErrorCode::SourceHashMismatch
    );

    // Self-dealing guard: a miner may not be its own taker. Two tiny self-swaps would otherwise buy
    // permanent eligibility (successful_swaps >= 2) and pad fill volume at zero real cost. A sybil
    // (second wallet, same operator) is out of on-chain reach — the scorer's volume exclusion owns
    // that half. Also reject the default pubkey: a timeout would "refund" the slash to the burn address.
    require!(user != ctx.accounts.miner.key(), ErrorCode::SelfSwapNotAllowed);
    require!(user != Pubkey::default(), ErrorCode::InvalidUser);
    // H1 (V-H1) on-chain backstop: a taker's payout address must differ from the miner's own delivery
    // address (pinned on the reservation at draw). Equal addresses make the miner "deliver to itself" —
    // a self-represented taker (who skips the validator's normalized reserve-time gate) could poison it
    // into a false non-delivery slash or a collusion volume-farm. Raw compare: fully effective on
    // case-sensitive chains; EVM case-variants stay covered by the validator's normalized routed check.
    require!(
        user_to_addr != ctx.accounts.reservation.miner_to_addr,
        ErrorCode::DestEqualsMinerAddr
    );

    let now = Clock::get()?.unix_timestamp;
    // V-C2: the source_lock's seeds omit the backing, so it is shared across hubs. A lock still live
    // (reserved_until in the future) means another unclaimed reservation on this miner already holds this
    // (from_chain, from_addr) — reject the duplicate. A stale/`< now` (or freshly-init 0) lock is ours.
    require!(
        ctx.accounts.source_lock.reserved_until < now,
        ErrorCode::DuplicateSourceAddr
    );
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

    // Entry fuse for a backing that settles elsewhere: the relay must be provably alive (or the purse
    // read below is trusting a snapshot nobody is refreshing), and the miner must not still owe a
    // penalty on that chain. Both are no-ops for "sol", which settles inside `timeout_swap`.
    backing::check_entry_gates(cfg, &ctx.accounts.miner_state, backing, now)?;

    // Over-collateralization gate: hold 1.10× THIS fill in the backing purse up front, NET of in-flight
    // reservations. An attested purse (TAO) FALLS as fees settle, so `vote_set_attestation` refuses a
    // downward write while the hub is held — that, not any "only rises", holds the gate through initiate.
    let purse = backing::backing_purse(
        backing,
        &ctx.accounts.miner_state,
        ctx.accounts.attestation.as_deref(),
    )?;
    let hub_bit = backing::backing_bit(backing)?;
    require!(
        purse.saturating_sub(ctx.accounts.miner_state.reserved(hub_bit))
            >= required_collateral(collateral_amount),
        ErrorCode::InsufficientCollateral
    );

    let ttl = cfg.reservation_ttl_secs;
    let extension_budget = cfg.max_total_extension_secs;
    let miner_key = ctx.accounts.miner.key();
    let router_key = ctx.accounts.router.key();

    let (from_chain, to_chain, reserved_until, event_backing) = {
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
        (r.from_chain.clone(), r.to_chain.clone(), r.reserved_until, r.collateral_chain.clone())
    };

    // V-C2: hold this (from_chain, from_addr) for the fill's live window. A colliding finalize (any hub)
    // seeds the same lock and reverts above until this lapses.
    ctx.accounts.source_lock.reserved_until = reserved_until;
    ctx.accounts.source_lock.bump = ctx.bumps.source_lock;

    // Tighten the hub's busy lock to the filled reservation's actual life. The bid set it
    // conservatively to cover the whole finalize window; now that we've filled, `now + ttl` is exact
    // (never shorter than reserved_until, so no live-reservation hole).
    let bit = backing::backing_bit(&ctx.accounts.reservation.collateral_chain)?;
    ctx.accounts.miner_state.set_busy(bit, reserved_until);

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
        collateral_chain: event_backing,
    });
    Ok(())
}
