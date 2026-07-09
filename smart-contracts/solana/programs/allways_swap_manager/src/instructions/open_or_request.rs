use anchor_lang::prelude::*;
use anchor_lang::system_program::{transfer, Transfer};

use crate::constants::{
    CONFIG_SEED, MAX_CHAIN_LEN, MAX_VALIDATORS, MINER_SEED, POOL_SEED, QUOTE_SEED, RESV_SEED,
    TREASURY_SEED,
};
use crate::error::ErrorCode;
use crate::events::{PoolOpened, ReservationRequested};
use crate::state::{Config, MinerQuote, MinerState, Pool, Request, Reservation, Treasury};

/// Any account (validator OR plain user — entry is permissionless) opens or joins a per-miner
/// reservation-lottery pool for a pair. First caller opens and pins the miner's quote; later in-window
/// callers add one request each (same pinned pair). Every fresh entry pays a flat, non-refundable
/// reservation fee (router -> treasury) — the anti-spam gate. A non-validator router gets lottery
/// weight 0 (loses to validators / uniform among users) and, if it wins, flags down a validator to
/// claim + attest (those are validator-gated).
#[derive(Accounts)]
#[instruction(from_chain: String, to_chain: String)]
pub struct OpenOrRequest<'info> {
    /// The router of this request — a whitelisted validator OR a plain user (entry is permissionless).
    #[account(mut)]
    pub router: Signer<'info>,

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

    /// The miner's standing quote for this pair — must exist; pinned into the Pool snapshot at open.
    #[account(
        seeds = [QUOTE_SEED, miner.key().as_ref(), from_chain.as_bytes(), to_chain.as_bytes()],
        bump = quote.bump,
    )]
    pub quote: Account<'info, MinerQuote>,

    #[account(
        init_if_needed,
        payer = router,
        space = 8 + Pool::INIT_SPACE,
        seeds = [POOL_SEED, miner.key().as_ref()],
        bump,
    )]
    pub pool: Account<'info, Pool>,

    /// Subnet-revenue sink for the reservation fee (kept separate from the collateral vault).
    #[account(mut, seeds = [TREASURY_SEED], bump = treasury.bump)]
    pub treasury: Account<'info, Treasury>,

    /// The per-miner reservation slot — checked so a new contest can't be opened while a reservation
    /// is still active (it would overwrite the winner's hold). Populated by `resolve_pool`.
    #[account(
        init_if_needed,
        payer = router,
        space = 8 + Reservation::INIT_SPACE,
        seeds = [RESV_SEED, miner.key().as_ref()],
        bump,
    )]
    pub reservation: Account<'info, Reservation>,

    pub system_program: Program<'info, System>,
}

/// A BID carries only the router competing for the seat. The taker + amounts are named later by the
/// seat winner in `finalize_reservation`; the swap-size bounds + collateral gate move there too (the
/// amount isn't known here). Miner-eligibility gates (active, not busy, min collateral) stay.
pub fn handler(ctx: Context<OpenOrRequest>, from_chain: String, to_chain: String) -> Result<()> {
    require!(!ctx.accounts.config.halted, ErrorCode::SystemHalted);
    require!(
        !from_chain.is_empty() && !to_chain.is_empty(),
        ErrorCode::EmptyField
    );
    require!(
        from_chain.len() <= MAX_CHAIN_LEN && to_chain.len() <= MAX_CHAIN_LEN,
        ErrorCode::StringTooLong
    );

    require!(ctx.accounts.miner_state.active, ErrorCode::MinerNotActive);
    require!(
        !ctx.accounts.miner_state.has_active_swap,
        ErrorCode::MinerHasActiveSwap
    );
    require!(
        ctx.accounts.miner_state.collateral >= ctx.accounts.config.min_collateral,
        ErrorCode::InsufficientCollateral
    );

    let clock = Clock::get()?;
    let now = clock.unix_timestamp;

    // Can't open a contest while the miner is still held. Two holds block a new draw (which would
    // overwrite the reservation): a FILLED reservation still within its TTL, and a drawn-but-UNFILLED
    // reservation still inside its finalize window — the seat winner has the exclusive right to fill it
    // and must not be evicted by a fresh contest. Once `finalize_by` passes unfilled, re-open is allowed
    // (the abandoned slot is reapable).
    let resv = &ctx.accounts.reservation;
    let active_reservation = resv.reserved_until != 0 && resv.reserved_until >= now;
    let pending_finalize = resv.reserved_until == 0 && resv.finalize_by != 0 && now <= resv.finalize_by;
    require!(!active_reservation && !pending_finalize, ErrorCode::MinerReserved);

    // Flat, non-refundable anti-spam fee: router → treasury (subnet revenue). Charged only on a
    // fresh entry (open or first join) — a same-router in-window bid UPDATE is free, so refining a
    // bid against the pinned rate isn't taxed.
    let is_update = ctx.accounts.pool.opened_at != 0
        && ctx
            .accounts
            .pool
            .requests
            .iter()
            .any(|r| r.router == ctx.accounts.router.key());
    if !is_update {
        let fee = ctx.accounts.config.reservation_fee_lamports;
        transfer(
            CpiContext::new(
                ctx.accounts.system_program.key(),
                Transfer {
                    from: ctx.accounts.router.to_account_info(),
                    to: ctx.accounts.treasury.to_account_info(),
                },
            ),
            fee,
        )?;
        ctx.accounts.treasury.total = ctx
            .accounts
            .treasury
            .total
            .checked_add(fee)
            .ok_or(ErrorCode::Overflow)?;
    }

    let miner_key = ctx.accounts.miner.key();
    let router_key = ctx.accounts.router.key();
    let req = Request { router: router_key };

    let pool_bump = ctx.bumps.pool;
    if ctx.accounts.pool.opened_at == 0 {
        // OPEN — pin the pair + the miner's on-chain quote snapshot.
        let q = &ctx.accounts.quote;
        let (mfrom, mto, rate) = (
            q.miner_from_addr.clone(),
            q.miner_to_addr.clone(),
            q.rate,
        );
        let window = ctx.accounts.config.pool_window_secs;
        let closes_at = now.saturating_add(window);

        // Busy from the moment the pool opens: covers the window + the finalize window + the eventual
        // reservation TTL. Set conservatively here so the draw/finalize never SHORTEN it — otherwise a
        // miner would read as free during the finalize window while holding an about-to-fill reservation.
        ctx.accounts.miner_state.busy_until = closes_at
            .saturating_add(ctx.accounts.config.finalize_window_secs)
            .saturating_add(ctx.accounts.config.reservation_ttl_secs);

        let pool = &mut ctx.accounts.pool;
        pool.miner = miner_key;
        pool.from_chain = from_chain.clone();
        pool.to_chain = to_chain.clone();
        pool.miner_from_addr = mfrom;
        pool.miner_to_addr = mto;
        pool.rate = rate;
        pool.opened_at = now;
        pool.closes_at = closes_at;
        // Unpinned: `resolve_pool` arms the seed slot after the window shuts, so the draw's entropy
        // cannot be read (or predicted) while bids are still being placed.
        pool.seed_slot = 0;
        pool.requests.clear();
        pool.requests.push(req);
        pool.bump = pool_bump;

        emit!(PoolOpened {
            miner: miner_key,
            opener: router_key,
            from_chain,
            to_chain,
            closes_at,
            seed_slot: 0, // armed later by resolve_pool; kept in the event for schema stability
        });
    } else {
        // JOIN or UPDATE — must be within the window and match the pinned pair.
        let pool = &mut ctx.accounts.pool;
        require!(now <= pool.closes_at, ErrorCode::PoolClosed);
        require!(
            pool.from_chain == from_chain && pool.to_chain == to_chain,
            ErrorCode::MinerBusyDifferentPair
        );
        // Upsert: a repeat call from the same router updates its bid in place (dynamic bidding
        // while the window is open); a new router is appended, subject to the set cap.
        if let Some(existing) = pool.requests.iter_mut().find(|r| r.router == router_key) {
            *existing = req;
        } else {
            require!(pool.requests.len() < MAX_VALIDATORS, ErrorCode::ValidatorSetFull);
            pool.requests.push(req);
        }

        emit!(ReservationRequested {
            miner: miner_key,
            router: router_key,
            requests: pool.requests.len() as u8,
        });
    }
    Ok(())
}
