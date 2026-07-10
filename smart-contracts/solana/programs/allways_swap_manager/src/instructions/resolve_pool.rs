use anchor_lang::prelude::*;

use crate::constants::{CONFIG_SEED, MINER_SEED, POOL_SEED, RESV_SEED, SEED_SLOT_DELAY_SLOTS};

/// SlotHashes sysvar address (not re-exported by anchor's solana_program facade in this version).
const SLOT_HASHES_ID: Pubkey = Pubkey::from_str_const("SysvarS1otHashes111111111111111111111111111");
use crate::error::ErrorCode;
use crate::events::{PoolDrawArmed, PoolResolved};
use crate::lottery::{draw_seed, pick_weighted};
use crate::state::{Config, MinerState, Pool, Reservation};

/// Permissionless, two-phase. After a pool's window closes the first call *arms* the draw by pinning
/// `seed_slot` to a slot the chain has not produced yet; a later call resolves against that slot's
/// hash, runs the stake-weighted draw, creates the winner's `Reservation`, and resets the pool.
///
/// Randomness: `seed = keccak(pool_key || SlotHashes[first slot >= seed_slot])`.
///
/// Arming *after* the window shuts is the point: pinning at open would make the entropy readable
/// while bids were still being placed (a bidder could then join only when it would win), and any
/// fixed slot-time assumption used to place that slot is wrong the moment real slot time drifts.
///
/// The lookup takes the lowest produced slot at-or-after `seed_slot`, so a skipped seed slot is
/// tolerated without falling back to a predictable seed. If `seed_slot` rolls out of SlotHashes
/// (~512 slots) before any crank resolves, the draw is re-armed rather than resolved from a stale
/// or caller-chosen hash.
///
/// Residual: the leader of the seed slot can bias its own block's hash. Bounded — a reservation only
/// grants a hold; moving funds still needs `vote_initiate` consensus + a real on-chain tx.
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

/// Outcome of resolving `seed_slot` against the SlotHashes ring buffer.
enum SeedLookup {
    /// Hash of the lowest produced slot at-or-after `seed_slot`.
    Found([u8; 32]),
    /// The chain has not reached `seed_slot` yet — retry.
    NotYetProduced,
    /// `seed_slot` aged out of the 512-entry window — re-arm, never guess.
    RolledOff,
}

/// SlotHashes layout: 8-byte little-endian count, then 40-byte `(slot, hash)` entries sorted by slot
/// DESCENDING. Scanning down, the last entry still `>= seed_slot` is the lowest such slot.
fn find_seed_hash(data: &[u8], seed_slot: u64) -> SeedLookup {
    if data.len() < 8 {
        return SeedLookup::NotYetProduced; // sysvar unpopulated
    }
    let declared = u64::from_le_bytes(data[0..8].try_into().unwrap()) as usize;
    let capacity = (data.len() - 8) / 40;
    let n = declared.min(capacity);
    if n == 0 {
        return SeedLookup::NotYetProduced;
    }

    let entry = |i: usize| -> (u64, [u8; 32]) {
        let off = 8 + i * 40;
        let slot = u64::from_le_bytes(data[off..off + 8].try_into().unwrap());
        let mut h = [0u8; 32];
        h.copy_from_slice(&data[off + 8..off + 40]);
        (slot, h)
    };

    if entry(0).0 < seed_slot {
        return SeedLookup::NotYetProduced;
    }
    if entry(n - 1).0 > seed_slot {
        return SeedLookup::RolledOff;
    }

    let mut found = [0u8; 32];
    for i in 0..n {
        let (slot, h) = entry(i);
        if slot < seed_slot {
            break;
        }
        found = h; // descending: keep overwriting, so the last kept is the lowest slot >= seed_slot
    }
    SeedLookup::Found(found)
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
    let miner_key = ctx.accounts.miner.key();
    let cur_slot = Clock::get()?.slot;

    // Phase 1 — arm. The window has shut, so no further bids can react to the entropy we pin here,
    // and the slot itself does not exist yet, so nobody (including this caller) knows its hash.
    if ctx.accounts.pool.seed_slot == 0 {
        let seed_slot = cur_slot.saturating_add(SEED_SLOT_DELAY_SLOTS);
        ctx.accounts.pool.seed_slot = seed_slot;
        emit!(PoolDrawArmed { miner: miner_key, seed_slot });
        return Ok(());
    }

    // Phase 2 — resolve against the armed slot.
    let seed_slot = ctx.accounts.pool.seed_slot;
    let lookup = {
        let data = ctx.accounts.slot_hashes.try_borrow_data()?;
        find_seed_hash(&data, seed_slot)
    };
    let slot_hash = match lookup {
        SeedLookup::Found(h) => h,
        SeedLookup::NotYetProduced => return Err(ErrorCode::SeedSlotNotYetProduced.into()),
        SeedLookup::RolledOff => {
            // Only reachable after a ~512-slot stall. Re-arm rather than draw from a hash the caller
            // could have waited for.
            let seed_slot = cur_slot.saturating_add(SEED_SLOT_DELAY_SLOTS);
            ctx.accounts.pool.seed_slot = seed_slot;
            emit!(PoolDrawArmed { miner: miner_key, seed_slot });
            return Ok(());
        }
    };
    let seed = draw_seed(&pool_key, &slot_hash);

    // Weight per request = its router's config weight (0 if the router isn't a whitelisted validator —
    // e.g. a plain user). pick_weighted falls back to uniform if every weight is 0.
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
                .find(|v| v.key == r.router)
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
            p.rate,
        )
    };

    // Create the UNFILLED reservation for the seat winner: pin `router` + the pool's miner-quote
    // snapshot + the finalize deadline. The taker + amounts are named later by `finalize_reservation`.
    let finalize_window = ctx.accounts.config.finalize_window_secs;
    let reservation_bump = ctx.bumps.reservation;
    let winner_router = winner.router;
    let r = &mut ctx.accounts.reservation;
    r.router = winner_router; // the ONLY signer allowed to finalize
    r.from_chain = from_chain; // pinned pair — the finalize collateral bind reads from_chain
    r.to_chain = to_chain;
    r.miner_from_addr = miner_from_addr;
    r.miner_to_addr = miner_to_addr;
    r.rate = rate;
    r.finalize_by = now.saturating_add(finalize_window);
    r.reserved_until = 0; // UNFILLED until finalize (also the sentinel that keeps it non-claimable)
    r.claimed_swap_key = [0u8; 32];
    r.bump = reservation_bump;
    // Clear any stale fill data from a prior reused reservation. Invisible while `reserved_until == 0`,
    // but reset so nothing reads a stale taker/amount during the finalize window.
    r.from_addr = String::new();
    r.user = Pubkey::default();
    r.user_to_addr = String::new();
    r.collateral_amount = 0;
    r.from_amount = 0;
    r.to_amount = 0;
    r.created_at = 0;
    r.max_extend_at = 0;

    // Do NOT write busy_until here: the bid set it to cover closes_at + finalize_window + ttl. Writing
    // `now + ttl` would SHORTEN it and free the miner during the finalize window (a live-reservation
    // hole where deactivate/withdraw_collateral could fire mid-fill).

    // Reset the pool for the next contest (rent stays parked). seed_slot back to 0 so the next
    // pool re-arms instead of inheriting this draw's slot.
    let pool = &mut ctx.accounts.pool;
    pool.opened_at = 0;
    pool.seed_slot = 0;
    pool.requests.clear();

    emit!(PoolResolved {
        miner: miner_key,
        winner: winner_router,
        requests: req_count,
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a SlotHashes buffer: 8-byte count + 40-byte (slot, hash) entries, slots DESCENDING.
    /// Each slot's hash is [slot as u8; 32] so we can assert which entry was picked.
    fn sysvar(slots: &[u64]) -> Vec<u8> {
        let mut v = (slots.len() as u64).to_le_bytes().to_vec();
        for &s in slots {
            v.extend_from_slice(&s.to_le_bytes());
            v.extend_from_slice(&[s as u8; 32]);
        }
        v
    }

    fn found(data: &[u8], seed: u64) -> u8 {
        match find_seed_hash(data, seed) {
            SeedLookup::Found(h) => h[0],
            _ => panic!("expected Found"),
        }
    }

    #[test]
    fn exact_seed_slot_is_used() {
        let d = sysvar(&[105, 104, 103, 102, 101]);
        assert_eq!(found(&d, 103), 103);
    }

    #[test]
    fn skipped_seed_slot_takes_lowest_produced_above() {
        // 103 was skipped; the draw must use 104, never a fallback.
        let d = sysvar(&[105, 104, 102, 101]);
        assert_eq!(found(&d, 103), 104);
    }

    #[test]
    fn oldest_entry_equal_to_seed_slot_is_found() {
        let d = sysvar(&[105, 104, 103]);
        assert_eq!(found(&d, 103), 103);
    }

    #[test]
    fn newest_entry_equal_to_seed_slot_is_found() {
        let d = sysvar(&[105, 104, 103]);
        assert_eq!(found(&d, 105), 105);
    }

    #[test]
    fn not_yet_produced_when_chain_is_behind_seed_slot() {
        let d = sysvar(&[105, 104, 103]);
        assert!(matches!(find_seed_hash(&d, 106), SeedLookup::NotYetProduced));
    }

    #[test]
    fn rolled_off_when_seed_slot_predates_the_window() {
        let d = sysvar(&[105, 104, 103]);
        assert!(matches!(find_seed_hash(&d, 102), SeedLookup::RolledOff));
    }

    #[test]
    fn empty_or_short_sysvar_never_falls_back() {
        assert!(matches!(find_seed_hash(&[], 10), SeedLookup::NotYetProduced));
        assert!(matches!(find_seed_hash(&sysvar(&[]), 10), SeedLookup::NotYetProduced));
        assert!(matches!(find_seed_hash(&[0u8; 4], 10), SeedLookup::NotYetProduced));
    }

    #[test]
    fn declared_count_beyond_buffer_is_clamped() {
        // A lying count must not read out of bounds.
        let mut d = sysvar(&[105, 104]);
        d[0..8].copy_from_slice(&999u64.to_le_bytes());
        assert_eq!(found(&d, 104), 104);
    }

    #[test]
    fn pick_is_stable_as_newer_slots_arrive() {
        // Once the seed slot exists, later slots must not change the chosen hash — otherwise a
        // cranker could grind by choosing when to call.
        let seed = 103;
        let a = found(&sysvar(&[104, 103, 102]), seed);
        let b = found(&sysvar(&[110, 109, 108, 107, 106, 105, 104, 103, 102]), seed);
        assert_eq!(a, b, "chosen hash must not depend on when resolve is called");
    }
}
