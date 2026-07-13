use anchor_lang::prelude::*;

/// PDA seed for the singleton config account (`seeds = [CONFIG_SEED]`).
#[constant]
pub const CONFIG_SEED: &[u8] = b"config";

/// PDA seed prefix for a per-miner native-SOL collateral vault (`seeds = [COLLATERAL_SEED, miner]`) —
/// each miner's collateral in its own account: trustless custody + no shared-vault contention.
#[constant]
pub const COLLATERAL_SEED: &[u8] = b"collateral";

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


/// PDA seed prefix for a miner's standing per-pair quote
/// (`seeds = [QUOTE_SEED, miner_pubkey, from_chain, to_chain]`).
#[constant]
pub const QUOTE_SEED: &[u8] = b"quote";

/// PDA seed prefix for a miner's realized per-direction stats
/// (`seeds = [STATS_SEED, miner_pubkey, from_chain, to_chain]`).
#[constant]
pub const STATS_SEED: &[u8] = b"stats";

/// PDA seed prefix for a miner's hotkey↔pubkey identity binding (`seeds = [BIND_SEED, miner_pubkey]`).
#[constant]
pub const BIND_SEED: &[u8] = b"bind";

/// PDA seed prefix for the set-once hotkey→pubkey reverse marker (`seeds = [HOTKEY_BIND_SEED, hotkey]`).
/// Enforces hotkey→≤1 pubkey on-chain: a hotkey can be claimed by exactly one pubkey, ever, so a struck
/// pubkey can't rotate to a fresh one and re-bind the same hotkey to dodge strikes.
#[constant]
pub const HOTKEY_BIND_SEED: &[u8] = b"hkbind";

/// PDA seed prefix for a per-miner reservation-lottery pool (`seeds = [POOL_SEED, miner]`).
#[constant]
pub const POOL_SEED: &[u8] = b"pool";

/// PDA seed for the singleton subnet-revenue treasury (`seeds = [TREASURY_SEED]`) — held entirely
/// separate from the collateral vault so miner collateral is never commingled with subnet income.
#[constant]
pub const TREASURY_SEED: &[u8] = b"treasury";

/// On-chain schema/version for upgrade tracking, bumped as phases land. v10: A4 source-replay via
/// freshness — removed the permanent `TxMarker`, added `Reservation.created_at` as the source bound.
/// v9 = A5 binding; v8 = scoring read-surface + #493 extensions; see git history for v2–v9.
pub const CONFIG_VERSION: u32 = 10;

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

/// Slots the draw's seed slot is pinned ahead of the arming crank. Small: the pool has already
/// closed, so this only has to be far enough ahead that the slot is unproduced when pinned.
pub const SEED_SLOT_DELAY_SLOTS: u64 = 4;

/// Bounded max lengths for stored strings.
pub const MAX_ADDR_LEN: usize = 80;
pub const MAX_CHAIN_LEN: usize = 16;
pub const MAX_TX_LEN: usize = 128;

/// The collateral currency for the SOL-collateral module (this program). `finalize_reservation` binds
/// `collateral_amount` to the leg denominated in this chain: `collateral_amount == (from_chain ==
/// NUMERAIRE_CHAIN ? from_amount : to_amount)`. NOT a global "every swap must have a SOL leg" rule —
/// it scopes the bind to SOL-collateralized swaps, so a future TAO-collateral module is an added branch.
pub const NUMERAIRE_CHAIN: &str = "sol";

/// Fixed-point scale for the miner rate: the stored `rate` integer = display_rate × RATE_PRECISION
/// (e.g. "345" TAO/BTC → `345 × 1e18`). Matches the off-chain `RATE_PRECISION`, so the stored value
/// IS the off-chain `rate_fixed` — no decimal-string parse on either side (replaces the old free-form
/// `rate: String`, which let an unparseable-but-lucrative rate score yet never reserve). The contract
/// only stores/copies the value; routability/validity is judged off-chain (`is_executable_rate`).
pub const RATE_PRECISION: u128 = 1_000_000_000_000_000_000; // 1e18

/// Significant figures every posted rate is floored to on-chain (`quantize_rate_sig_figs`). The crown
/// is ranked off-chain on the raw stored rate, so without a tick two miners can undercut in a
/// sub-perceptible digit to capture the whole crown for free; flooring to display precision makes any
/// crown-winning improvement one a taker can actually see. Mirrored off-chain as `RATE_SIG_FIGS`.
pub const RATE_SIG_FIGS: u32 = 5;

/// Floor `rate` (fixed-point, display × RATE_PRECISION) to RATE_SIG_FIGS significant figures — zeros
/// every digit below the top RATE_SIG_FIGS. Pure integer math (no floats — non-deterministic in BPF);
/// floor not round, so a rate can never gain a tick by rounding and the reconstruction can't overflow.
pub fn quantize_rate_sig_figs(rate: u128) -> u128 {
    if rate == 0 {
        return 0;
    }
    let digits = rate.ilog10() + 1;
    if digits <= RATE_SIG_FIGS {
        return rate;
    }
    let pow = 10u128.pow(digits - RATE_SIG_FIGS);
    rate / pow * pow
}

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

/// Collateral (lamports) to back a swap of `collateral_amount` = `collateral_amount × COLLATERAL_REQUIREMENT_BPS
/// / 10_000`, rounded up. u128 math clamped to `u64::MAX` so an extreme size can't wrap.
pub fn required_collateral(collateral_amount: u64) -> u64 {
    let numer = (collateral_amount as u128).saturating_mul(COLLATERAL_REQUIREMENT_BPS as u128);
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

/// Protocol fee divisor — 1% (immutable policy), `fee = collateral_amount / FEE_DIVISOR`. Compile-time
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
/// reservation TTL. Runtime-adjustable via `set_pool_window` (dev seeds 5s for fast swaps).
pub const POOL_WINDOW_SECS: i64 = 30;

/// Initial seconds the seat winner has after the draw to `finalize_reservation` (name the fill) before
/// the unfilled reservation can be reaped. Seeds `Config.finalize_window_secs`; runtime-tunable within
/// [MIN, MAX]. Must cover a validator's internal auction + tx landing without letting a winner park a
/// miner for free (the reservation fee is already sunk on abandon).
pub const FINALIZE_WINDOW_SECS: i64 = 150;
pub const FINALIZE_WINDOW_SECS_MIN: i64 = 15;
pub const FINALIZE_WINDOW_SECS_MAX: i64 = 300; // 5 min

const _: () = assert!(
    FINALIZE_WINDOW_SECS >= FINALIZE_WINDOW_SECS_MIN && FINALIZE_WINDOW_SECS <= FINALIZE_WINDOW_SECS_MAX,
    "FINALIZE_WINDOW_SECS must be within [15s, 300s]"
);

/// Initial minimum seconds between successful validator-weight updates (Phase 10) — an anti-thrash
/// floor, not a schedule. Seeds `Config.weights_update_min_interval_secs`.
pub const WEIGHTS_UPDATE_MIN_INTERVAL_SECS: i64 = 3600;

/// Canonical deploy value for `fulfillment_timeout_secs` — 10 min (mirrors ink!'s 50-block default).
/// Deliberately tight: the miner must broadcast within it, then validators extend the deadline as the
/// destination tx confirms (see the extension system). Pass at `initialize` / `set_fulfillment_timeout`.
pub const DEFAULT_FULFILLMENT_TIMEOUT_SECS: i64 = 600; // 10 min

/// Canonical deploy value for `reservation_ttl_secs` — 10 min (mirrors ink!). Same model as the
/// fulfillment timeout: a tight base, extended while the source tx confirms.
pub const DEFAULT_RESERVATION_TTL_SECS: i64 = 600; // 10 min

/// Total seconds a reservation/swap deadline may be slid forward across all extensions, frozen into
/// each at creation as `deadline + this`. Seeds `Config.max_total_extension_secs`; runtime-tunable
/// within [MIN, MAX]. 140 min gives edge-case BTC headroom: a run of back-to-back slow blocks can
/// leave an honest, adequately-fee'd payout waiting >90 min for two confirmations, and the ceiling
/// is the only bound on extensions (no per-swap count cap), so it must cover the slow tail.
pub const MAX_TOTAL_EXTENSION_SECS: i64 = 8_400; // 140 min
pub const MAX_TOTAL_EXTENSION_SECS_MIN: i64 = 1_800; // 30 min — two 15-min BTC blocks
pub const MAX_TOTAL_EXTENSION_SECS_MAX: i64 = 8_400; // 140 min — hard lid

const _: () = assert!(
    MAX_TOTAL_EXTENSION_SECS >= MAX_TOTAL_EXTENSION_SECS_MIN
        && MAX_TOTAL_EXTENSION_SECS <= MAX_TOTAL_EXTENSION_SECS_MAX,
    "MAX_TOTAL_EXTENSION_SECS must be within [30 min, 140 min]"
);

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
    fn quantize_rate_floors_to_sig_figs() {
        let p = RATE_PRECISION;
        // Zero and mantissas already within RATE_SIG_FIGS pass through untouched.
        assert_eq!(quantize_rate_sig_figs(0), 0);
        assert_eq!(quantize_rate_sig_figs(12_345), 12_345);
        assert_eq!(quantize_rate_sig_figs(345 * p), 345 * p);
        // Floor, never round: 1.23459 → 1.2345 (not 1.2346).
        assert_eq!(quantize_rate_sig_figs(1_234_590_000_000_000_000), 1_234_500_000_000_000_000);
        assert_eq!(quantize_rate_sig_figs(123_456), 123_450);
        // Sub-perceptible undercuts within one 5-sf bucket collapse to the SAME value → tie & split,
        // never a free crown steal. 5.00001 and 5.00002 both floor to 5.0.
        assert_eq!(quantize_rate_sig_figs(5_000_010_000_000_000_000), 5 * p);
        assert_eq!(quantize_rate_sig_figs(5_000_020_000_000_000_000), 5 * p);
        // A genuine 5-sf improvement survives as a distinct (better) bucket.
        assert_ne!(quantize_rate_sig_figs(5 * p), quantize_rate_sig_figs(4_999_900_000_000_000_000));
    }

    #[test]
    fn total_extension_default_within_bounds() {
        assert!(
            (MAX_TOTAL_EXTENSION_SECS_MIN..=MAX_TOTAL_EXTENSION_SECS_MAX)
                .contains(&MAX_TOTAL_EXTENSION_SECS),
            "MAX_TOTAL_EXTENSION_SECS {} outside [{}, {}]",
            MAX_TOTAL_EXTENSION_SECS,
            MAX_TOTAL_EXTENSION_SECS_MIN,
            MAX_TOTAL_EXTENSION_SECS_MAX,
        );
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
