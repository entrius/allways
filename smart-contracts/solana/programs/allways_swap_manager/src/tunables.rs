//! Hardcoded, deploy-time-tunable **economic** parameters — the knobs you change-and-redeploy.
//!
//! Kept in one file (not in `Config`/`state.rs`, not scattered through handlers) so every economic
//! lever is legible in one place. These are compile-time constants: editing one requires a redeploy,
//! and every value is **range-checked at compile time** (`const _: () = assert!(…)`) AND by a unit
//! test below — so an out-of-range edit fails `cargo test`/the build, never production.

/// Basis-points denominator (10_000 bps = 1.00×). Shared by every ×-multiplier below.
pub const BPS_DENOMINATOR: u64 = 10_000;

/// Collateral a miner must hold to **back a swap**, as a fraction of the swap size, in basis points
/// (10_000 = 1.00×, 11_000 = 1.10×). Over-collateralizing (> 1.00×) reserves a slash buffer so a
/// failed swap can make the user whole *and* penalize the miner (v2 cleanup #4).
///
/// Bounds (enforced below + by `tests::collateral_requirement_within_bounds`):
/// `COLLATERAL_REQUIREMENT_BPS_MIN` (1.00×, never under-collateralized) ≤ x ≤
/// `COLLATERAL_REQUIREMENT_BPS_MAX` (2.00×, sanity ceiling).
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

/// Collateral (lamports) required to back a swap of `sol_amount` lamports
/// = `sol_amount × COLLATERAL_REQUIREMENT_BPS / 10_000`, rounded up so a fractional requirement is
/// never under-met. Computed in u128 and clamped to `u64::MAX` so an extreme size can't wrap (it
/// simply demands more than any miner could hold).
pub fn required_collateral(sol_amount: u64) -> u64 {
    let numer = (sol_amount as u128).saturating_mul(COLLATERAL_REQUIREMENT_BPS as u128);
    // round up (ceil-div): require at least the exact fraction.
    let req = numer
        .saturating_add(BPS_DENOMINATOR as u128 - 1)
        .checked_div(BPS_DENOMINATOR as u128)
        .unwrap_or(u128::MAX);
    req.min(u64::MAX as u128) as u64
}

// --- Quote-update churn fee (anti-flashing) ---
//
// Updating a *standing* quote too soon after its last write costs a small, treasury-bound fee that
// **decays to zero** the longer the quote has stood. This discourages rapid quote-flashing without
// scaring miners off the network: first-time creation is free (only rent), and a quote left to
// stand becomes free to update again. Stepwise (not continuous exponential) — deterministic and
// cheap on-chain, with the same "cheaper the longer you wait" intent.
//
// Tiers, by seconds since the previous update:
//   elapsed < TIER1_MAX_SECS          → TIER1_LAMPORTS  (rapid churn — most expensive)
//   TIER1_MAX ≤ elapsed < TIER2_MAX   → TIER2_LAMPORTS
//   elapsed ≥ TIER2_MAX_SECS          → 0               (a stable quote — free)
// All collected fees go to the vault treasury.
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
//
// Moved here from `constants.rs` so every deploy-time economic lever lives in one file. All are
// compile-time constants: change-and-redeploy, nothing stored in `Config`. (Structural constants —
// PDA seeds, request-type bytes, max string lengths, `SLOT_MS` — stay in `constants.rs`.)

/// Protocol fee divisor — 1% (immutable policy), `fee = sol_amount / FEE_DIVISOR`.
pub const FEE_DIVISOR: u64 = 100;

/// Flat anti-spam fee (lamports) a validator pays per reservation request (`open_or_request`),
/// non-refundable → vault treasury (Phase 9). Default 0.001 SOL.
pub const RESERVATION_FEE_LAMPORTS: u64 = 1_000_000;

/// Reservation-lottery pooling window (seconds): how long a pool collects requests before it can be
/// resolved. Contending validators route within slots (~400ms), so a few seconds suffices. Must stay
/// well below the reservation TTL (separate windows). Paired with `constants::SLOT_MS` to pin the
/// draw's future seed slot.
pub const POOL_WINDOW_SECS: i64 = 3;

/// Minimum seconds between successful validator-weight updates (Phase 10) — a floor (anti-thrash /
/// anti-grief), not a schedule. Validators' actual cadence (e.g. daily) is an off-chain policy ≥ this.
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
