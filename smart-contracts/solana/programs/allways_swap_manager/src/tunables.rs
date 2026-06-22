//! Deploy-time economic knobs (change-and-redeploy). One file so every
//! lever is visible; each is range-checked at compile time + a unit test.

pub const BPS_DENOMINATOR: u64 = 10_000;

/// Collateral a miner must hold per swap, in bps (11_000 = 1.10x). The >1x
/// buffer lets a failed swap make the user whole and still penalize the miner.
pub const COLLATERAL_REQUIREMENT_BPS: u64 = 11_000;
pub const COLLATERAL_REQUIREMENT_BPS_MIN: u64 = 10_000; // floor 1.0x
pub const COLLATERAL_REQUIREMENT_BPS_MAX: u64 = 20_000; // ceiling 2.0x

// Compile-time guard: the build won't compile if the setting leaves [1.0x, 2.0x].
const _: () = assert!(
    COLLATERAL_REQUIREMENT_BPS >= COLLATERAL_REQUIREMENT_BPS_MIN
        && COLLATERAL_REQUIREMENT_BPS <= COLLATERAL_REQUIREMENT_BPS_MAX,
    "COLLATERAL_REQUIREMENT_BPS must be within [1.0x, 2.0x] (10_000..=20_000 bps)"
);
/// Collateral (lamports) for a swap of `sol_amount`. Ceil-div so it's never under-met;
/// u128 math clamped to u64::MAX so an extreme size can't wrap.
pub fn required_collateral(sol_amount: u64) -> u64 {
    let numer = (sol_amount as u128).saturating_mul(COLLATERAL_REQUIREMENT_BPS as u128);
    // round up (ceil-div): require at least the exact fraction.
    let req = numer
        .saturating_add(BPS_DENOMINATOR as u128 - 1)
        .checked_div(BPS_DENOMINATOR as u128)
        .unwrap_or(u128::MAX);
    req.min(u64::MAX as u128) as u64
}

// Quote-update churn fee (anti-flashing): updating a standing quote soon after its
// last write costs a treasury-bound fee that decays to zero the longer it has stood.
// Creation is free; stepwise tiers (not continuous) keep it deterministic on-chain.
pub const QUOTE_UPDATE_FEE_TIER1_LAMPORTS: u64 = 10_000_000; // 0.01 SOL, churn within 5 min
pub const QUOTE_UPDATE_FEE_TIER1_MAX_SECS: i64 = 300; // 5 min
pub const QUOTE_UPDATE_FEE_TIER2_LAMPORTS: u64 = 1_000_000; // 0.001 SOL, 5-10 min
pub const QUOTE_UPDATE_FEE_TIER2_MAX_SECS: i64 = 600; // 10 min, free thereafter

// Sanity: windows must increase and the fee must not increase as time passes (monotone decay).
const _: () = assert!(
    QUOTE_UPDATE_FEE_TIER1_MAX_SECS < QUOTE_UPDATE_FEE_TIER2_MAX_SECS
        && QUOTE_UPDATE_FEE_TIER1_LAMPORTS >= QUOTE_UPDATE_FEE_TIER2_LAMPORTS,
    "quote-update fee tiers must decay over increasing windows"
);

/// Fee (lamports) for updating a standing quote `elapsed_secs` after its previous write.
/// Negative/zero elapsed (clock skew) falls into the most-expensive tier; creation is free.
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

/// Protocol fee divisor: 1% (immutable policy), `fee = sol_amount / FEE_DIVISOR`.
pub const FEE_DIVISOR: u64 = 100;

// The next three are initial seed defaults: `initialize` copies them into `Config`, then
// they're runtime-tunable via admin setters. Handlers read live `Config`, not these consts.

/// Initial flat anti-spam fee (lamports) per reservation request, non-refundable to treasury.
/// 0.02 SOL: a pool-open busies the miner for the window + TTL, so it shouldn't be cheap to grief.
pub const RESERVATION_FEE_LAMPORTS: u64 = 20_000_000;

/// Initial reservation-lottery pooling window (seconds). Longer = more validators for a fairer
/// stake-weighted draw, at the cost of latency. Must stay well below the reservation TTL.
pub const POOL_WINDOW_SECS: i64 = 60;

/// Initial min seconds between successful validator-weight updates: an anti-thrash floor, not
/// a schedule. Seeds `Config.weights_update_min_interval_secs`.
pub const WEIGHTS_UPDATE_MIN_INTERVAL_SECS: i64 = 3600;

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
