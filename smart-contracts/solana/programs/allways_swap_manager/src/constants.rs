use anchor_lang::prelude::*;

/// PDA seed for the singleton config account (`seeds = [CONFIG_SEED]`).
#[constant]
pub const CONFIG_SEED: &[u8] = b"config";

/// PDA seed for the singleton native-SOL collateral vault (`seeds = [VAULT_SEED]`).
#[constant]
pub const VAULT_SEED: &[u8] = b"vault";

/// PDA seed prefix for per-miner state (`seeds = [MINER_SEED, miner_pubkey]`).
#[constant]
pub const MINER_SEED: &[u8] = b"miner";

/// PDA seed prefix for a consensus vote round
/// (`seeds = [VOTE_SEED, &[request_type], target_pubkey]`).
#[constant]
pub const VOTE_SEED: &[u8] = b"vote";

/// PDA seed prefix for a confirmed reservation (`seeds = [RESV_SEED, miner_pubkey]`).
#[constant]
pub const RESV_SEED: &[u8] = b"resv";

/// PDA seed prefix for a swap (`seeds = [SWAP_SEED, swap_key]`, swap_key = keccak(from_tx_hash)).
#[constant]
pub const SWAP_SEED: &[u8] = b"swap";

/// PDA seed prefix for the permanent source-tx replay marker (`seeds = [TX_SEED, swap_key]`).
#[constant]
pub const TX_SEED: &[u8] = b"tx";

/// PDA seed prefix for a miner's standing per-pair quote
/// (`seeds = [QUOTE_SEED, miner_pubkey, from_chain, to_chain]`).
#[constant]
pub const QUOTE_SEED: &[u8] = b"quote";

/// PDA seed prefix for a per-miner reservation-lottery pool (`seeds = [POOL_SEED, miner]`).
#[constant]
pub const POOL_SEED: &[u8] = b"pool";

/// PDA seed for the singleton subnet-revenue treasury (`seeds = [TREASURY_SEED]`) — held entirely
/// separate from the collateral vault so miner collateral is never commingled with subnet income.
#[constant]
pub const TREASURY_SEED: &[u8] = b"treasury";

/// On-chain schema/version for upgrade tracking, bumped as phases land. v6: runtime config setters
/// (fee/window/interval promoted to Config); see git history for the v2–v5 progression.
pub const CONFIG_VERSION: u32 = 6;

/// Max validators in the whitelist (bounds the Config `validators` Vec and a round's voters).
pub const MAX_VALIDATORS: usize = 16;

/// A vote round older than this (seconds) is treated as stale and reset before recording a new vote.
pub const VOTE_ROUND_TTL_SECS: i64 = 1800;

/// Request types (keys into a vote round). REQ_RESERVE is gone: reservations are lottery-based.
pub const REQ_ACTIVATE: u8 = 0;
pub const REQ_INITIATE: u8 = 2;
pub const REQ_DEACTIVATE: u8 = 5;
pub const REQ_CONFIRM: u8 = 6;
pub const REQ_TIMEOUT: u8 = 7;
/// Global (non-per-target) round for the validator-weight vector.
pub const REQ_SET_WEIGHTS: u8 = 8;

/// Solana slot time (ms) — a chain property, used with `POOL_WINDOW_SECS` (economic-levers section
/// below) to pin the draw's future seed slot from the window duration.
pub const SLOT_MS: u64 = 400;

/// Bounded max lengths for stored strings.
pub const MAX_ADDR_LEN: usize = 80;
pub const MAX_CHAIN_LEN: usize = 16;
pub const MAX_RATE_LEN: usize = 32;
pub const MAX_TX_LEN: usize = 128;

/// Basis-points denominator (10_000 bps = 1.00×). Shared by every ×-multiplier below.
pub const BPS_DENOMINATOR: u64 = 10_000;

/// Collateral a miner must hold to back a swap, as a fraction of swap size in bps (10_000 = 1.00×,
/// 11_000 = 1.10×). >1.00× reserves a slash buffer to make a wronged user whole and penalize the
/// miner (v2 #4). Bounded to [MIN, MAX] below — enforced at compile time + by unit test.
pub const COLLATERAL_REQUIREMENT_BPS: u64 = 11_000; // 1.10× — current setting

/// Hard floor: a swap must always be at least fully collateralized.
pub const COLLATERAL_REQUIREMENT_BPS_MIN: u64 = 10_000; // 1.0×
/// Hard ceiling: more than 2× would price out honest miners with no extra safety payoff.
pub const COLLATERAL_REQUIREMENT_BPS_MAX: u64 = 20_000; // 2.0×

// Compile-time guard: the build won't compile if the setting leaves [1.0×, 2.0×].
const _: () = assert!(
    COLLATERAL_REQUIREMENT_BPS >= COLLATERAL_REQUIREMENT_BPS_MIN
        && COLLATERAL_REQUIREMENT_BPS <= COLLATERAL_REQUIREMENT_BPS_MAX,
    "COLLATERAL_REQUIREMENT_BPS must be within [1.0x, 2.0x] (10_000..=20_000 bps)"
);

/// Collateral (lamports) to back a swap of `sol_amount` = `sol_amount × COLLATERAL_REQUIREMENT_BPS
/// / 10_000`, rounded up. u128 math clamped to `u64::MAX` so an extreme size can't wrap.
pub fn required_collateral(sol_amount: u64) -> u64 {
    let numer = (sol_amount as u128).saturating_mul(COLLATERAL_REQUIREMENT_BPS as u128);
    // round up (ceil-div): require at least the exact fraction.
    let req = numer
        .saturating_add(BPS_DENOMINATOR as u128 - 1)
        .checked_div(BPS_DENOMINATOR as u128)
        .unwrap_or(u128::MAX);
    req.min(u64::MAX as u128) as u64
}

// Quote-update churn fee (anti-flashing): overwriting a standing quote too soon costs a treasury-
// bound, decaying fee (free once it's stood long enough; first creation is always free). Stepwise
// tiers by seconds since last update — see `quote_update_fee` for the cutoffs; all fees → treasury.
pub const QUOTE_UPDATE_FEE_TIER1_LAMPORTS: u64 = 10_000_000; // 0.01 SOL — churn within 5 min
pub const QUOTE_UPDATE_FEE_TIER1_MAX_SECS: i64 = 300; // 5 min
pub const QUOTE_UPDATE_FEE_TIER2_LAMPORTS: u64 = 1_000_000; // 0.001 SOL — 5–10 min
pub const QUOTE_UPDATE_FEE_TIER2_MAX_SECS: i64 = 600; // 10 min → free thereafter

// Sanity: windows must increase and the fee must not increase as time passes (monotone decay).
const _: () = assert!(
    QUOTE_UPDATE_FEE_TIER1_MAX_SECS < QUOTE_UPDATE_FEE_TIER2_MAX_SECS
        && QUOTE_UPDATE_FEE_TIER1_LAMPORTS >= QUOTE_UPDATE_FEE_TIER2_LAMPORTS,
    "quote-update fee tiers must decay over increasing windows"
);

/// Fee (lamports) for updating a standing quote `elapsed_secs` after its previous write. A negative
/// or zero elapsed (clock skew / same-second churn) falls into the most-expensive tier. Applies only
/// to updates — the caller charges nothing on first creation.
pub fn quote_update_fee(elapsed_secs: i64) -> u64 {
    if elapsed_secs < QUOTE_UPDATE_FEE_TIER1_MAX_SECS {
        QUOTE_UPDATE_FEE_TIER1_LAMPORTS
    } else if elapsed_secs < QUOTE_UPDATE_FEE_TIER2_MAX_SECS {
        QUOTE_UPDATE_FEE_TIER2_LAMPORTS
    } else {
        0
    }
}

// --- Protocol fees & timing ---

/// Protocol fee divisor — 1% (immutable policy), `fee = sol_amount / FEE_DIVISOR`. Compile-time
/// only (not promoted to a runtime setter).
pub const FEE_DIVISOR: u64 = 100;

// The next three are initial seed defaults — `initialize` copies them into `Config`, then they're
// runtime-tunable via the #486 admin setters. Handlers read the live `Config`, not these consts.

/// Initial flat anti-spam fee (lamports) per reservation request (`open_or_request`), validator →
/// the Treasury PDA, non-refundable. Seeds `Config.reservation_fee_lamports`. 0.02 SOL — sized so a
/// pool-open (which now busies the miner for the window + reservation TTL, #485) isn't cheap to grief.
pub const RESERVATION_FEE_LAMPORTS: u64 = 20_000_000;

/// Initial reservation-lottery pooling window (seconds). Seeds `Config.pool_window_secs` — how long
/// a pool gathers contending requests before the stake-weighted draw. Must stay well below the
/// reservation TTL; paired with `SLOT_MS` to pin the draw's seed slot.
pub const POOL_WINDOW_SECS: i64 = 60;

/// Initial minimum seconds between successful validator-weight updates (Phase 10) — an anti-thrash
/// floor, not a schedule. Seeds `Config.weights_update_min_interval_secs`.
pub const WEIGHTS_UPDATE_MIN_INTERVAL_SECS: i64 = 3600;

/// Canonical deploy value for `fulfillment_timeout_secs` — 4h. Sized so the slowest chain (BTC
/// confirmations) plus validator confirm fits before timeout fires, avoiding over-slashing an
/// honest-but-unconfirmed miner. Pass at `initialize` (or `set_fulfillment_timeout`); not per-chain yet.
pub const DEFAULT_FULFILLMENT_TIMEOUT_SECS: i64 = 14_400;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_update_fee_decays_by_tier() {
        // Same-second / rapid churn → top tier.
        assert_eq!(quote_update_fee(0), QUOTE_UPDATE_FEE_TIER1_LAMPORTS);
        assert_eq!(quote_update_fee(299), QUOTE_UPDATE_FEE_TIER1_LAMPORTS);
        // 5-min boundary drops to tier 2.
        assert_eq!(quote_update_fee(300), QUOTE_UPDATE_FEE_TIER2_LAMPORTS);
        assert_eq!(quote_update_fee(599), QUOTE_UPDATE_FEE_TIER2_LAMPORTS);
        // 10 min and beyond → free.
        assert_eq!(quote_update_fee(600), 0);
        assert_eq!(quote_update_fee(86_400), 0);
        // Clock skew (negative elapsed) → most-expensive tier, never free.
        assert_eq!(quote_update_fee(-100), QUOTE_UPDATE_FEE_TIER1_LAMPORTS);
    }

    #[test]
    fn collateral_requirement_within_bounds() {
        assert!(
            (COLLATERAL_REQUIREMENT_BPS_MIN..=COLLATERAL_REQUIREMENT_BPS_MAX)
                .contains(&COLLATERAL_REQUIREMENT_BPS),
            "COLLATERAL_REQUIREMENT_BPS {} outside [{}, {}] (1.0x..=2.0x)",
            COLLATERAL_REQUIREMENT_BPS,
            COLLATERAL_REQUIREMENT_BPS_MIN,
            COLLATERAL_REQUIREMENT_BPS_MAX,
        );
    }

    #[test]
    fn required_collateral_scales_by_multiplier() {
        // At the shipped 1.10× a 2 SOL swap needs 2.2 SOL of collateral.
        assert_eq!(required_collateral(2_000_000_000), 2_200_000_000);
        // 1.0× floor would be identity.
        assert_eq!(
            required_collateral(0),
            0,
            "zero swap requires zero collateral"
        );
    }

    #[test]
    fn required_collateral_never_under_one_x() {
        // Whatever the setting, you can never be asked for less than the swap size itself.
        for amt in [1u64, 1_000, 1_000_000_000, u64::MAX / 4] {
            assert!(required_collateral(amt) >= amt, "under-collateralized at {amt}");
        }
    }

    #[test]
    fn required_collateral_saturates_not_wraps() {
        // Extreme size must clamp, not wrap to a tiny value.
        assert_eq!(required_collateral(u64::MAX), u64::MAX);
    }
}
