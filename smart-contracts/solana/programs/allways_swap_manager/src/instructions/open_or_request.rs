use anchor_lang::prelude::*;
use anchor_lang::system_program::{transfer, Transfer};

use crate::constants::{
    CONFIG_SEED, MAX_ADDR_LEN, MAX_CHAIN_LEN, MAX_VALIDATORS, MINER_SEED, POOL_SEED, QUOTE_SEED,
    RESV_SEED, SLOT_MS, TREASURY_SEED,
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

#[allow(clippy::too_many_arguments)]
pub fn handler(
    ctx: Context<OpenOrRequest>,
    from_chain: String,
    to_chain: String,
    user: Pubkey,
    user_from_addr: String,
    user_to_addr: String,
    sol_amount: u64,
    from_amount: u128,
    to_amount: u128,
) -> Result<()> {
    require!(!ctx.accounts.config.halted, ErrorCode::SystemHalted);
    require!(
        !from_chain.is_empty()
            && !to_chain.is_empty()
            && !user_from_addr.is_empty()
            && !user_to_addr.is_empty(),
        ErrorCode::EmptyField
    );
    require!(
        from_chain.len() <= MAX_CHAIN_LEN && to_chain.len() <= MAX_CHAIN_LEN,
        ErrorCode::StringTooLong
    );
    require!(
        user_from_addr.len() <= MAX_ADDR_LEN && user_to_addr.len() <= MAX_ADDR_LEN,
        ErrorCode::StringTooLong
    );

    let cfg = &ctx.accounts.config;
    require!(
        cfg.min_swap_amount == 0 || sol_amount >= cfg.min_swap_amount,
        ErrorCode::AmountBelowMin
    );
    require!(
        cfg.max_swap_amount == 0 || sol_amount <= cfg.max_swap_amount,
        ErrorCode::AmountAboveMax
    );
    require!(ctx.accounts.miner_state.active, ErrorCode::MinerNotActive);
    require!(
        !ctx.accounts.miner_state.has_active_swap,
        ErrorCode::MinerHasActiveSwap
    );
    require!(
        ctx.accounts.miner_state.collateral >= cfg.min_collateral,
        ErrorCode::InsufficientCollateral
    );
    // Over-collateralization gate at entry: hold 1.10× THIS request's size up front. Collateral only
    // rises while busy (withdraw is locked), so passing here means vote_initiate's identical gate
    // can't later strand a user who has already sent source funds (review #1).
    require!(
        ctx.accounts.miner_state.collateral >= crate::constants::required_collateral(sol_amount),
        ErrorCode::InsufficientCollateral
    );

    let clock = Clock::get()?;
    let now = clock.unix_timestamp;

    // Can't open a contest while an active reservation already holds the miner (would overwrite it).
    let resv = &ctx.accounts.reservation;
    let active_reservation = resv.reserved_until != 0 && resv.reserved_until >= now;
    require!(!active_reservation, ErrorCode::MinerReserved);

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
    let req = Request {
        router: router_key,
        user,
        user_from_addr,
        user_to_addr,
        sol_amount,
        from_amount,
        to_amount,
    };

    let pool_bump = ctx.bumps.pool;
    if ctx.accounts.pool.opened_at == 0 {
        // OPEN — pin the pair + the miner's on-chain quote snapshot.
        let q = &ctx.accounts.quote;
        let (mfrom, mto, rate) = (
            q.miner_from_addr.clone(),
            q.miner_to_addr.clone(),
            q.rate.clone(),
        );
        let window = ctx.accounts.config.pool_window_secs;
        let seed_slot = clock
            .slot
            .saturating_add((window as u64).saturating_mul(1000) / SLOT_MS);
        let closes_at = now.saturating_add(window);

        // Busy from the moment the pool opens: covers the window + the eventual reservation TTL.
        ctx.accounts.miner_state.busy_until =
            closes_at.saturating_add(ctx.accounts.config.reservation_ttl_secs);

        let pool = &mut ctx.accounts.pool;
        pool.miner = miner_key;
        pool.from_chain = from_chain.clone();
        pool.to_chain = to_chain.clone();
        pool.miner_from_addr = mfrom;
        pool.miner_to_addr = mto;
        pool.rate = rate;
        pool.opened_at = now;
        pool.closes_at = closes_at;
        pool.seed_slot = seed_slot;
        pool.requests.clear();
        pool.requests.push(req);
        pool.bump = pool_bump;

        emit!(PoolOpened {
            miner: miner_key,
            opener: router_key,
            from_chain,
            to_chain,
            closes_at,
            seed_slot,
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
            user,
            requests: pool.requests.len() as u8,
        });
    }
    Ok(())
}
