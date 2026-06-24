use anchor_lang::prelude::*;

use crate::constants::{CONFIG_SEED, MINER_SEED, POOL_SEED, RESV_SEED};

/// SlotHashes sysvar address (not re-exported by anchor's solana_program facade in this version).
const SLOT_HASHES_ID: Pubkey = Pubkey::from_str_const("SysvarS1otHashes111111111111111111111111111");
use crate::error::ErrorCode;
use crate::events::PoolResolved;
use crate::lottery::{draw_seed, pick_weighted};
use crate::state::{Config, MinerState, Pool, Reservation};

/// Permissionless. After a pool's window closes, run the stake-weighted draw over its requests and
/// create the `Reservation` for the winner, then reset the pool for reuse.
///
/// Randomness: `seed = keccak(pool_key || SlotHashes[seed_slot])` — a future slot pinned at open,
/// unknown then, fixed once produced. If that slot rolled off the SlotHashes window (or the sysvar is
/// empty, e.g. LiteSVM) a deterministic fallback keeps the pool resolvable; residual grind is bounded
/// since a reservation only grants a hold — moving funds still needs `vote_initiate` consensus + a
/// real on-chain tx.
#[derive(Accounts)]
pub struct ResolvePool<'info> {
    #[account(mut)]
    pub caller: Signer<'info>,

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

    #[account(mut, seeds = [POOL_SEED, miner.key().as_ref()], bump = pool.bump)]
    pub pool: Account<'info, Pool>,

    #[account(
        init_if_needed,
        payer = caller,
        space = 8 + Reservation::INIT_SPACE,
        seeds = [RESV_SEED, miner.key().as_ref()],
        bump,
    )]
    pub reservation: Account<'info, Reservation>,

    /// CHECK: the SlotHashes sysvar, validated by address; parsed manually (not Sysvar::get-able).
    #[account(address = SLOT_HASHES_ID)]
    pub slot_hashes: UncheckedAccount<'info>,

    pub system_program: Program<'info, System>,
}

pub fn handler(ctx: Context<ResolvePool>) -> Result<()> {
    let now = Clock::get()?.unix_timestamp;

    require!(ctx.accounts.pool.opened_at != 0, ErrorCode::NoRequests);
    require!(!ctx.accounts.pool.requests.is_empty(), ErrorCode::NoRequests);
    require!(now > ctx.accounts.pool.closes_at, ErrorCode::PoolNotClosed);
    // No active check: a pool opens only on an active miner and a busy miner can't be deactivated, so
    // any miner with an open pool is still active here. vote_initiate re-checks `active` before funds
    // move — the backstop if a future busy-clearing path ever breaks this invariant.

    let pool_key = ctx.accounts.pool.key();
    let seed_slot = ctx.accounts.pool.seed_slot;

    // Resolve the seed slot's hash from SlotHashes (descending by slot, 40 bytes/entry after an
    // 8-byte count). The time window (now > closes_at) guarantees the pinned slot is produced on a
    // real chain, so the exact match is the normal path; absence (rolled off, or empty under LiteSVM)
    // takes the deterministic fallback below.
    let slot_hash: Option<[u8; 32]> = {
        let data = ctx.accounts.slot_hashes.try_borrow_data()?;
        let mut found = None;
        if data.len() >= 8 {
            let n = u64::from_le_bytes(data[0..8].try_into().unwrap()) as usize;
            for i in 0..n {
                let off = 8 + i * 40;
                if off + 40 > data.len() {
                    break;
                }
                let slot = u64::from_le_bytes(data[off..off + 8].try_into().unwrap());
                if slot == seed_slot {
                    let mut h = [0u8; 32];
                    h.copy_from_slice(&data[off + 8..off + 40]);
                    found = Some(h);
                    break;
                }
                if slot < seed_slot {
                    break; // descending order — target not present (not yet produced / rolled off)
                }
            }
        }
        found
    };
    let seed = match slot_hash {
        Some(h) => draw_seed(&pool_key, &h),
        None => draw_seed(&pool_key, &seed_slot.to_le_bytes()),
    };

    // Weight per request = its validator's config weight (0 if removed). pick_weighted falls back to
    // uniform if every weight is 0.
    let weights: Vec<u64> = ctx
        .accounts
        .pool
        .requests
        .iter()
        .map(|r| {
            ctx.accounts
                .config
                .validators
                .iter()
                .find(|v| v.key == r.validator)
                .map(|v| v.weight)
                .unwrap_or(0)
        })
        .collect();
    let idx = pick_weighted(seed, &weights);
    let winner = ctx.accounts.pool.requests[idx].clone();
    let req_count = ctx.accounts.pool.requests.len() as u8;

    // Pinned miner-quote snapshot from the pool.
    let (from_chain, to_chain, miner_from_addr, miner_to_addr, rate) = {
        let p = &ctx.accounts.pool;
        (
            p.from_chain.clone(),
            p.to_chain.clone(),
            p.miner_from_addr.clone(),
            p.miner_to_addr.clone(),
            p.rate.clone(),
        )
    };

    // Create the Reservation for the winner — same shape vote_reserve produced, so everything
    // downstream is unchanged.
    let ttl = ctx.accounts.config.reservation_ttl_secs;
    let extension_budget = ctx.accounts.config.max_total_extension_secs;
    let reservation_bump = ctx.bumps.reservation;
    let r = &mut ctx.accounts.reservation;
    r.bound_hash = [0u8; 32]; // unused post-lottery (no consensus binding); reserved_until is the sentinel
    r.from_addr = winner.user_from_addr;
    r.user = winner.user; // pin taker + payout so a permissionless claim can't redirect it
    r.user_to_addr = winner.user_to_addr;
    r.from_chain = from_chain;
    r.to_chain = to_chain;
    r.sol_amount = winner.sol_amount;
    r.from_amount = winner.from_amount;
    r.to_amount = winner.to_amount;
    r.miner_from_addr = miner_from_addr;
    r.miner_to_addr = miner_to_addr;
    r.rate = rate;
    r.reserved_until = now.saturating_add(ttl);
    r.max_extend_at = r.reserved_until.saturating_add(extension_budget);
    r.claimed_swap_key = [0u8; 32]; // fresh contest → no live claim (clears any stale prior key)
    r.bump = reservation_bump;

    // Lock the miner busy for the reservation's life (read by deactivate/withdraw, non-bypassable).
    ctx.accounts.miner_state.busy_until = now.saturating_add(ttl);

    // Reset the pool for the next contest (rent stays parked).
    let miner_key = ctx.accounts.miner.key();
    let pool = &mut ctx.accounts.pool;
    pool.opened_at = 0;
    pool.requests.clear();

    emit!(PoolResolved {
        miner: miner_key,
        winner: winner.validator,
        user: winner.user,
        requests: req_count,
    });
    Ok(())
}
