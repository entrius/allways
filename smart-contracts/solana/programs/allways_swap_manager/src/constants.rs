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

/// PDA seed prefix for a confirmed reservation (`seeds = [RESV_SEED, miner_pubkey, backing]`) —
/// one slot per (miner, hub) since v3.1, so each hub's contest holds only its own slot.
#[constant]
pub const RESV_SEED: &[u8] = b"resv";

/// PDA seed prefix for the V-C2 source-address lock
/// (`seeds = [SRCLOCK_SEED, miner_pubkey, from_chain, keccak(from_addr)]`) — one live-unclaimed
/// reservation per `(miner, from_chain, from_addr)`, enforced for routed and self-represented alike.
pub const SRCLOCK_SEED: &[u8] = b"srclock";

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

/// PDA seed prefix for a reservation-lottery pool (`seeds = [POOL_SEED, miner, backing]`) —
/// one contest slot per (miner, hub) since v3.1.
#[constant]
pub const POOL_SEED: &[u8] = b"pool";

/// PDA seed for the singleton subnet-revenue treasury (`seeds = [TREASURY_SEED]`) — held entirely
/// separate from the collateral vault so miner collateral is never commingled with subnet income.
#[constant]
pub const TREASURY_SEED: &[u8] = b"treasury";

/// PDA seed prefix for a miner's bond attestation on one backing chain
/// (`seeds = [ATTEST_SEED, miner_pubkey, chain_id]`). One per (miner, hub) — the quorum's assertion
/// about a bond this program cannot read.
#[constant]
pub const ATTEST_SEED: &[u8] = b"attest";

/// On-chain schema/version for upgrade tracking, bumped as phases land. v14: v3.1 per-hub swap
/// concurrency — per-hub transient state on MinerState, per-hub Pool/Reservation seeds. v13 = W2b
/// quote-level backing; v12 = W2 bond attestation; v11 = W1 seam; v10 = A4 freshness replay.
pub const CONFIG_VERSION: u32 = 15;

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
/// Per-(miner, backing chain) round writing a `BondAttestation`.
pub const REQ_SET_ATTESTATION: u8 = 9;
/// Global round bumping `Config.last_attest_heartbeat` (the dead-man fuse's liveness signal).
pub const REQ_ATTEST_HEARTBEAT: u8 = 10;
/// Per-swap round for the no-fault cancel terminal (destination provably cannot receive the payout).
/// Distinct from REQ_TIMEOUT so the two terminals accrue votes in separate rounds; the first to reach
/// quorum closes the swap and the loser reverts on the gone PDA.
pub const REQ_CANCEL: u8 = 11;

/// Advisory `SwapCancelled.reason` discriminants — observability only, NEVER a consensus input (not
/// bound into the vote hash), so validators co-count the verdict rather than fragmenting over the label.
pub const CANCEL_REASON_EVM_REVERT: u8 = 0;
pub const CANCEL_REASON_ERC20_BLACKLIST: u8 = 1;
pub const CANCEL_REASON_ERC20_PAUSED: u8 = 2;
pub const CANCEL_REASON_SOL_RESERVED: u8 = 3;
pub const CANCEL_REASON_OTHER: u8 = 255;

/// Slots the draw's seed slot is pinned ahead of the arming crank. Three leader windows (4 slots
/// each) ahead, so the seed slot never lands in the window of the leader who included the arming tx.
/// Residual: the seed slot's own leader can still bias its block hash (schedule is public), but that
/// only buys a reservation hold — funds still need a real deposit + vote quorum. Must stay far below
/// the ~512-slot SlotHashes window or every draw would re-arm forever.
pub const SEED_SLOT_DELAY_SLOTS: u64 = 12;

/// Bounded max lengths for stored strings.
pub const MAX_ADDR_LEN: usize = 80;
pub const MAX_CHAIN_LEN: usize = 16;
pub const MAX_TX_LEN: usize = 128;

/// The default collateral currency: the backing every reservation is pinned to at the draw. The
/// finalize bind then sizes `collateral_amount` against whichever leg the pinned backing names — a
/// per-swap lookup, NOT a global "every swap must have a SOL leg" rule.
pub const NUMERAIRE_CHAIN: &str = "sol";

/// Backing (collateral) chain ids, lowercase like every other chain id at intake. "sol" is the local
/// per-miner vault this program custodies; "tao" is the Bittensor bond vault, whose purse read lands
/// in W2 — until then `backing::backing_purse` refuses it. A new hub is a new id, never a new branch.
pub const BACKING_CHAIN_SOL: &str = NUMERAIRE_CHAIN;
pub const BACKING_CHAIN_TAO: &str = "tao";

/// One bit per backing in `MinerState.active_backings` — a miner activates each purse separately, so a
/// deficient bond disables only its own quotes. 8 hubs before a program upgrade (realistic ceiling ~4).
/// The legacy `active` bool is the OR of these bits; see `MinerState::set_backing`.
pub const BACKING_BIT_SOL: u8 = 1 << 0;
pub const BACKING_BIT_TAO: u8 = 1 << 1;

/// Every known backing as `(bit, chain id)` — the one enumeration of the registry, for the rare code
/// that must walk the whole mask rather than answer about one backing (a full self-exit emitting one
/// event per purse). A new hub is a new row here and a new match arm in `backing.rs`; nothing else.
pub const BACKINGS: [(u8, &str); 2] = [
    (BACKING_BIT_SOL, BACKING_CHAIN_SOL),
    (BACKING_BIT_TAO, BACKING_CHAIN_TAO),
];

/// Width of the per-hub transient arrays on `MinerState` (busy/settling/reserved) — one slot per
/// possible `active_backings` bit, indexed by bit position (`MinerState::backing_slot`).
pub const MAX_BACKING_SLOTS: usize = 8;

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

/// Floor for `set_reservation_fee` — the fee is the only anti-grief brake on pool-opens (each one
/// busies a miner for window + finalize + TTL), so the admin key must not be able to zero it.
pub const RESERVATION_FEE_LAMPORTS_MIN: u64 = 1_000_000; // 0.001 SOL

const _: () = assert!(
    RESERVATION_FEE_LAMPORTS >= RESERVATION_FEE_LAMPORTS_MIN,
    "seed reservation fee must satisfy its own floor"
);

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

/// Deploy TAO-backed swap-size bounds, in rao (`Config.tao_min/max_swap_amount`; 0 max = unbounded).
/// Min = the fee-meaningfulness floor (a 1% fee on 0.1 τ dwarfs ~0.0002 τ of settlement postage).
/// Max = a deliberately conservative 1 τ start while the quorum that can slash bonds is small.
pub const TAO_MIN_SWAP_AMOUNT_RAO: u64 = 100_000_000; // 0.1 τ
pub const TAO_MAX_SWAP_AMOUNT_RAO: u64 = 1_000_000_000; // 1 τ — raised via set_tao_swap_bounds

// Same rules the setters enforce (validate::min_swap_amount / swap_bounds), checked at compile time so
// the seed can't be a value a later setter would reject.
const _: () = assert!(
    TAO_MIN_SWAP_AMOUNT_RAO >= 1_000
        && (TAO_MAX_SWAP_AMOUNT_RAO == 0 || TAO_MIN_SWAP_AMOUNT_RAO <= TAO_MAX_SWAP_AMOUNT_RAO),
    "seed TAO swap bounds must satisfy the min-swap floor and be non-contradictory"
);

/// Initial `Config.settlement_grace_secs` — how long a non-locally-backed timeout keeps the miner busy
/// while the penalty settles on the backing chain (verdict here, seizure there). Runtime-tunable within
/// [MIN, MAX]; 15 min covers a relay round trip with headroom.
pub const SETTLEMENT_GRACE_SECS: i64 = 900;
pub const SETTLEMENT_GRACE_SECS_MIN: i64 = 60;
pub const SETTLEMENT_GRACE_SECS_MAX: i64 = 7_200; // 2 h — a stuck relay must not hold a miner longer

const _: () = assert!(
    SETTLEMENT_GRACE_SECS >= SETTLEMENT_GRACE_SECS_MIN
        && SETTLEMENT_GRACE_SECS <= SETTLEMENT_GRACE_SECS_MAX,
    "SETTLEMENT_GRACE_SECS must be within [1 min, 2 h]"
);

/// Deploy `Config.tao_min_collateral` — the attested effective bond (rao) a miner must hold to
/// activate its TAO backing, the rao twin of the lamport `min_collateral`. Anchored to the smallest
/// bond that can serve one min swap: 1.1 × TAO_MIN_SWAP_AMOUNT_RAO = 0.11 τ, floored up to 0.25 τ.
pub const TAO_MIN_COLLATERAL_RAO: u64 = 250_000_000; // 0.25 τ

/// Initial `Config.attest_max_age_secs` — the dead-man fuse: TAO-backed entry (finalize/initiate) is
/// refused once the global attestation heartbeat is older than this. A circuit breaker, not a cadence,
/// so it sits at ≥2× the 12–24 h heartbeat interval. Runtime-tunable within [MIN, MAX].
pub const ATTEST_MAX_AGE_SECS: i64 = 86_400; // 24 h
pub const ATTEST_MAX_AGE_SECS_MIN: i64 = 3_600; // 1 h — below one heartbeat interval it fuses constantly
pub const ATTEST_MAX_AGE_SECS_MAX: i64 = 172_800; // 48 h — past this it stops being a circuit breaker

const _: () = assert!(
    ATTEST_MAX_AGE_SECS >= ATTEST_MAX_AGE_SECS_MIN && ATTEST_MAX_AGE_SECS <= ATTEST_MAX_AGE_SECS_MAX,
    "ATTEST_MAX_AGE_SECS must be within [1 h, 48 h]"
);

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

// Confirmation grace granted by `mark_fulfilled`: a miner who broadcast the dest tx near the deadline
// must not be slashable while that tx confirms, so fulfillment slides `timeout_at` to at least
// now + the dest chain's expected confirmation window (never shortened, capped by `max_extend_at`).
// Sized to each chain's finality gate: BTC 2 confs (~20 min) + one slow block; SOL 32 slots (~13 s);
// TAO 6 blocks (~72 s).
pub const FULFILL_GRACE_BTC_SECS: i64 = 1_800;
pub const FULFILL_GRACE_SOL_SECS: i64 = 120;
pub const FULFILL_GRACE_TAO_SECS: i64 = 180;
/// Unknown dest chain: one conservative default rather than 0 (0 would silently reopen the
/// paid-and-slashed window the moment a new chain is added off-chain before this table learns it).
pub const FULFILL_GRACE_DEFAULT_SECS: i64 = 600;

/// Confirmation grace (seconds) for a destination chain, by chain id. Case-insensitive so a
/// differently-cased chain string degrades to the right window, not silently to the default.
pub fn fulfillment_grace_secs(to_chain: &str) -> i64 {
    if to_chain.eq_ignore_ascii_case("btc") {
        FULFILL_GRACE_BTC_SECS
    } else if to_chain.eq_ignore_ascii_case("sol") {
        FULFILL_GRACE_SOL_SECS
    } else if to_chain.eq_ignore_ascii_case("tao") {
        FULFILL_GRACE_TAO_SECS
    } else {
        FULFILL_GRACE_DEFAULT_SECS
    }
}

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
    fn fulfillment_grace_covers_every_chain() {
        assert_eq!(fulfillment_grace_secs("btc"), FULFILL_GRACE_BTC_SECS);
        assert_eq!(fulfillment_grace_secs("BTC"), FULFILL_GRACE_BTC_SECS);
        assert_eq!(fulfillment_grace_secs("sol"), FULFILL_GRACE_SOL_SECS);
        assert_eq!(fulfillment_grace_secs("tao"), FULFILL_GRACE_TAO_SECS);
        // An unknown chain must get a real grace, never 0 (that would reopen paid-and-slashed).
        assert_eq!(fulfillment_grace_secs("eth"), FULFILL_GRACE_DEFAULT_SECS);
        assert!(FULFILL_GRACE_DEFAULT_SECS > 0);
    }

    #[test]
    fn seed_slot_delay_clears_a_leader_window_but_not_slothashes() {
        // Must clear at least one full 4-slot leader window past the arming tx's window...
        assert!(SEED_SLOT_DELAY_SLOTS > 8);
        // ...and stay far inside the ~512-slot SlotHashes ring or draws would re-arm forever.
        assert!(SEED_SLOT_DELAY_SLOTS < 128);
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
