use anchor_lang::prelude::*;

use crate::constants::{MAX_ADDR_LEN, MAX_CHAIN_LEN, MAX_TX_LEN, MAX_VALIDATORS};

/// A whitelisted validator and its draw weight. `weight` (default 1, admin-set) is the
/// stake-weight seam consumed ONLY by the reservation-lottery draw; consensus stays count-based.
#[derive(AnchorSerialize, AnchorDeserialize, Clone, InitSpace)]
pub struct ValidatorInfo {
    pub key: Pubkey,
    pub weight: u64,
}

/// Singleton config PDA (`seeds = [CONFIG_SEED]`). Amounts are in the named asset's smallest unit
/// (lamports unless the field says otherwise), durations in seconds.
#[account]
#[derive(InitSpace)]
pub struct Config {
    /// Admin authority (treasury withdrawals + config setters).
    pub admin: Pubkey,
    /// On-chain schema version, for upgrade tracking.
    pub version: u32,
    /// Minimum collateral a miner must hold to be activatable (lamports).
    pub min_collateral: u64,
    /// Maximum collateral a miner may post (lamports). 0 = no cap.
    pub max_collateral: u64,
    /// Swap fulfillment timeout (seconds); withdrawal cooldown = 2x this.
    pub fulfillment_timeout_secs: i64,
    /// Swap-size bounds on the collateral-backed (SOL) amount, in lamports. 0 = unbounded.
    pub min_swap_amount: u64,
    pub max_swap_amount: u64,
    /// The same bounds for TAO-backed swaps, in rao. Selected by `collateral_chain` — bounds are always
    /// compared in the backing asset's own units, never converted through the rate (that would smuggle
    /// a price oracle into a guard). Each future hub adds its own pair here.
    pub tao_min_swap_amount: u64,
    pub tao_max_swap_amount: u64,
    /// Attested effective bond (rao) needed to activate the TAO backing — the rao twin of
    /// `min_collateral`. Each future hub adds its own floor here.
    pub tao_min_collateral: u64,
    /// Seconds a miner stays busy after a non-locally-backed timeout, while the penalty settles on the
    /// backing chain (busy-until-settled). Unused by the "sol" path, which settles atomically.
    pub settlement_grace_secs: i64,
    /// Unix timestamp of the last `vote_attest_heartbeat` quorum (0 = never). Liveness is a GLOBAL
    /// question, so the fuse reads this one value rather than each attestation's own age — a quiet
    /// miner's old-but-correct attestation is not staleness.
    pub last_attest_heartbeat: i64,
    /// How stale the heartbeat may get before non-locally-backed entry (finalize/initiate) is refused.
    pub attest_max_age_secs: i64,
    /// How long a reservation holds a miner exclusive, in seconds.
    pub reservation_ttl_secs: i64,
    /// Quorum threshold, percent of the whitelisted validator set (e.g. 66).
    pub consensus_threshold_percent: u8,
    /// Whitelisted validator set (consensus participants) + draw weights, capped at MAX_VALIDATORS.
    #[max_len(MAX_VALIDATORS)]
    pub validators: Vec<ValidatorInfo>,
    /// Unix timestamp of the last consensus weight update (0 = never). Gates the update cadence floor.
    pub last_weights_update: i64,
    /// Emergency halt: when true, new deposits / activations / reservation pools are rejected.
    pub halted: bool,
    /// Flat anti-spam fee per reservation request, lamports (runtime-tunable; 0 disables).
    pub reservation_fee_lamports: u64,
    /// Reservation-lottery pooling window, seconds (runtime-tunable).
    pub pool_window_secs: i64,
    /// Seconds the seat winner has after the draw to fill (finalize) its reservation before it can be
    /// reaped. Runtime-tunable within [MIN, MAX] (see constants.rs). The internal auction runs here.
    pub finalize_window_secs: i64,
    /// Minimum seconds between consensus weight updates (runtime-tunable anti-thrash floor).
    pub weights_update_min_interval_secs: i64,
    /// Total seconds a reservation/swap deadline may be slid forward, frozen into each at creation as
    /// its `max_extend_at` ceiling. Runtime-tunable within [MIN, MAX] (see constants.rs).
    pub max_total_extension_secs: i64,
    /// Stored PDA bump.
    pub bump: u8,
}

/// Per-miner native-SOL collateral vault PDA (`seeds = [COLLATERAL_SEED, miner]`), program-owned.
///
/// Each miner's collateral lives in its OWN account — trustless custody (leaves only via the owning
/// miner's `withdraw_collateral` or a slash to the wronged user) and no shared-vault write contention.
/// The amount is `MinerState.collateral`; invariant: lamports == rent_exempt + collateral.
#[account]
#[derive(InitSpace)]
pub struct CollateralVault {
    /// Stored PDA bump.
    pub bump: u8,
}

/// Singleton subnet-revenue treasury PDA (`seeds = [TREASURY_SEED]`), program-owned, admin-withdrawable.
///
/// Holds ONLY subnet income — swap-completion fees, requote (anti-flash) fees, reservation fees —
/// kept entirely separate from collateral. Invariant: treasury.lamports == rent_exempt + total.
#[account]
#[derive(InitSpace)]
pub struct Treasury {
    /// Accrued protocol revenue (lamports), excludes the rent reserve. Drained by `withdraw_treasury`.
    pub total: u64,
    /// Stored PDA bump.
    pub bump: u8,
}

/// Per-miner state PDA (`seeds = [MINER_SEED, miner]`).
#[account]
#[derive(InitSpace)]
pub struct MinerState {
    /// The miner (hotkey-equivalent) this state belongs to.
    pub miner: Pubkey,
    /// Collateral credited to this miner (lamports). Backed 1:1 by lamports in the miner's collateral vault.
    pub collateral: u64,
    /// Whether the miner is active on ANY backing — the OR view of `active_backings`, kept so every
    /// pre-W2 read site works unchanged. Never written on its own; see `set_backing`/`clear_backings`.
    pub active: bool,
    /// Per-backing activation bitmask (`BACKING_BIT_*`). A deficient purse disables only its own
    /// quotes, not the miner (D2).
    pub active_backings: u8,
    /// Whether the miner currently has an in-flight swap.
    pub has_active_swap: bool,
    /// Unix ts the miner is busy until (open pool, held reservation, or in-flight swap). Self-clearing
    /// (`now >= busy_until` = free); the non-bypassable busy lock for deactivate/withdraw_collateral.
    pub busy_until: i64,
    /// Unix ts a non-locally-settled penalty is still settling on the backing chain (0 = clear). The
    /// ENTRY lock, read only by finalize/vote_initiate — `busy_until` is the exit lock and entry paths
    /// never read it, so the two must stay separate fields.
    pub settling_until: i64,
    /// Unix timestamp of last deactivation (0 = never). Gates the withdrawal cooldown.
    pub deactivation_at: i64,
    /// Lifetime swaps completed (confirm_swap quorum). Monotonic. Off-chain emissions warm-up gate:
    /// a miner earns nothing until `successful_swaps >= 2`.
    pub successful_swaps: u32,
    /// Lifetime swaps failed (timeout_swap quorum). Monotonic, never resets. Off-chain strike-out gate:
    /// `failed_swaps > 2` => no emissions (recover by re-registering).
    pub failed_swaps: u32,
    /// Stored PDA bump.
    pub bump: u8,
}

impl MinerState {
    /// Set or clear one backing's bit. `active` is re-derived here and nowhere else, so the OR-view
    /// invariant (`active == (active_backings != 0)`) has no path to drift. Returns the new OR view.
    pub fn set_backing(&mut self, bit: u8, on: bool) -> bool {
        if on {
            self.active_backings |= bit;
        } else {
            self.active_backings &= !bit;
        }
        self.active = self.active_backings != 0;
        self.active
    }

    /// Drop every backing at once — the miner leaves entirely (self-deactivate, penalty auto-deactivation).
    pub fn clear_backings(&mut self) {
        self.active_backings = 0;
        self.active = false;
    }
}

/// A miner's bond on one backing chain, as asserted by validator quorum
/// (`seeds = [ATTEST_SEED, miner, chain_id]`). This program cannot read the vault holding the bond, so
/// the quorum writes down what it read — the guards then work off a single agreed snapshot instead of
/// N private reads that would never agree.
#[account]
#[derive(InitSpace)]
pub struct BondAttestation {
    /// The miner this bond belongs to (== seed; stored for `getProgramAccounts` convenience).
    pub miner: Pubkey,
    /// Backing chain id (== seed), e.g. "tao".
    #[max_len(MAX_CHAIN_LEN)]
    pub chain: String,
    /// EFFECTIVE bond in the backing asset's smallest unit (rao for "tao") = vault gross − accrued fees
    /// − voted-but-unapplied slash verdicts. Debited at verdict time, so it deliberately leads the vault
    /// pessimistically — that is what keeps the 1.1× guards arithmetically true.
    pub effective_balance: u64,
    /// Whether the bond is locked on the backing chain. An unlocked bond backs nothing.
    pub locked: bool,
    /// The vault's monotonic lock epoch. A write at an older epoch is refused, so a stale round can
    /// never restore a lock state the vault has already moved past.
    pub epoch: u64,
    /// Quorum timestamp. Observability only — never a guard input (the fuse reads the global heartbeat).
    pub attested_at: i64,
    /// Stored PDA bump.
    pub bump: u8,
}

/// A consensus vote round PDA (`seeds = [VOTE_SEED, &[request_type], target]`).
///
/// `bound_hash` binds every voter to identical request params (keccak of the canonical request),
/// preventing bait-and-switch on requests whose params aren't fully in the seeds (reserve/initiate).
#[account]
#[derive(InitSpace)]
pub struct VoteRound {
    /// keccak-256 of the canonical request params; set by the first voter, checked by the rest.
    pub bound_hash: [u8; 32],
    /// Validators who have voted this round (deduplicated), capped at MAX_VALIDATORS.
    #[max_len(MAX_VALIDATORS)]
    pub voters: Vec<Pubkey>,
    /// Unix timestamp the round opened (0 = empty/available). Used for TTL reset.
    pub created_at: i64,
    /// Stored PDA bump.
    pub bump: u8,
}

/// Confirmed reservation for a miner (`seeds = [RESV_SEED, miner]`).
///
/// Created UNFILLED by `resolve_pool` (lottery draw: pins `router` + miner quote, `reserved_until = 0`);
/// filled by `finalize_reservation` (the winning router names the taker + amounts, sets `reserved_until`);
/// consumed by `vote_initiate` or reaped (`close_unfilled_reservation` / expiry).
/// `reserved_until`: 0 = unfilled OR empty, >= now = active, 0 < it < now = expired (overwritable).
/// `from_addr` is kept so initiate can verify the initiating user matches the reserver.
#[account]
#[derive(InitSpace)]
pub struct Reservation {
    /// The seat winner (winning lottery Request's router). The ONLY signer permitted to
    /// `finalize_reservation` (name the fill). Pinned at draw; a bid carries nothing else.
    pub router: Pubkey,
    /// User's source-chain address (the reserver). Written at finalize.
    #[max_len(MAX_ADDR_LEN)]
    pub from_addr: String,
    /// Pinned taker + payout address (named at finalize) — copied to the Swap at claim so the
    /// validator-relayed `submit_swap_claim` can't redirect the payout (front-run defense).
    pub user: Pubkey,
    #[max_len(MAX_ADDR_LEN)]
    pub user_to_addr: String,
    #[max_len(MAX_CHAIN_LEN)]
    pub from_chain: String,
    #[max_len(MAX_CHAIN_LEN)]
    pub to_chain: String,
    /// Chain whose asset backs this swap ("sol" = the local vault). Pinned at the draw and copied to
    /// the Swap; every collateral guard reads the leg + purse it names, never the pair (D4).
    #[max_len(MAX_CHAIN_LEN)]
    pub collateral_chain: String,
    /// Collateral-backed swap size, in `collateral_chain`'s smallest unit. Bounded by that backing's
    /// Config swap bounds; must equal the leg denominated in it (the finalize bind).
    pub collateral_amount: u64,
    /// Off-chain leg amounts in their own assets (u128 to cover wei-scale).
    pub from_amount: u128,
    pub to_amount: u128,
    /// Pinned miner quote — hash-bound at reserve time. `vote_initiate` MUST honor these (not the
    /// miner's live commitment): closes the rate-swing / deposit-address-theft total-loss bug.
    #[max_len(MAX_ADDR_LEN)]
    pub miner_from_addr: String,
    #[max_len(MAX_ADDR_LEN)]
    pub miner_to_addr: String,
    /// Canonical rate (see `MinerQuote::rate`); fixed-point = display_rate × RATE_PRECISION (1e18).
    pub rate: u128,
    /// Reservation creation time, unix seconds. The **source-freshness lower bound**: the user's
    /// deposit must be mined after this (a replayed prior-swap deposit predates it → rejected by the
    /// validator's freshness check, which replaces the source `TxMarker`).
    pub created_at: i64,
    /// Expiry, unix seconds (0 = unfilled OR empty). Set by `finalize_reservation` = now + ttl.
    pub reserved_until: i64,
    /// Fill deadline, unix seconds. Set at draw = now + `finalize_window_secs`. While `reserved_until
    /// == 0 && now > finalize_by` the unfilled reservation may be reaped (`close_unfilled_reservation`).
    pub finalize_by: i64,
    /// Absolute ceiling `reserved_until` may be extended to (unix seconds). Frozen at creation =
    /// initial deadline + the Config budget then, so a later retune can't move an in-flight ceiling.
    pub max_extend_at: i64,
    /// The one live claim's swap_key (`[0;32]` = none). Enforces one pending claim per reservation:
    /// set by `submit_swap_claim`, cleared on `vote_initiate` consume / `close_stale_claim` / a new
    /// `resolve_pool`.
    pub claimed_swap_key: [u8; 32],
    /// Stored PDA bump.
    pub bump: u8,
}

/// Swap lifecycle status. `PendingAttestation` = source-tx claim recorded, not yet attested (no miner
/// obligation). Terminal states (Completed/TimedOut) aren't stored — the Swap PDA is closed on
/// confirm/timeout. New variant appended last to keep Active/Fulfilled discriminants stable.
#[derive(AnchorSerialize, AnchorDeserialize, Clone, Copy, PartialEq, Eq, Debug, InitSpace)]
pub enum SwapStatus {
    Active,
    Fulfilled,
    PendingAttestation,
}

/// An in-flight swap (`seeds = [SWAP_SEED, swap_key]`, swap_key = keccak(from_tx_hash)).
/// Created by `vote_initiate` on quorum; closed by `confirm_swap` / `timeout_swap`. Chains/amounts/
/// miner-quote copied from the immutable Reservation; user-side fields from the hash-bound initiate vote.
#[account]
#[derive(InitSpace)]
pub struct Swap {
    pub user: Pubkey,
    pub miner: Pubkey,
    #[max_len(MAX_CHAIN_LEN)]
    pub from_chain: String,
    #[max_len(MAX_CHAIN_LEN)]
    pub to_chain: String,
    #[max_len(MAX_ADDR_LEN)]
    pub user_from_addr: String,
    #[max_len(MAX_ADDR_LEN)]
    pub user_to_addr: String,
    #[max_len(MAX_ADDR_LEN)]
    pub miner_from_addr: String,
    #[max_len(MAX_ADDR_LEN)]
    pub miner_to_addr: String,
    /// Canonical rate (see `MinerQuote::rate`); fixed-point = display_rate × RATE_PRECISION (1e18).
    pub rate: u128,
    /// Backing chain, copied from the Reservation at claim and immutable thereafter (`Reservation::
    /// collateral_chain`). `timeout_swap` reads it to know whether the penalty settles locally.
    #[max_len(MAX_CHAIN_LEN)]
    pub collateral_chain: String,
    /// Collateral-backed swap size, collateral-currency smallest unit (SOL lamports) — fee/slash basis.
    pub collateral_amount: u64,
    pub from_amount: u128,
    pub to_amount: u128,
    #[max_len(MAX_TX_LEN)]
    pub from_tx_hash: String,
    pub from_tx_block: u32,
    #[max_len(MAX_TX_LEN)]
    pub to_tx_hash: String,
    pub to_tx_block: u32,
    pub status: SwapStatus,
    pub initiated_at: i64,
    pub timeout_at: i64,
    /// Absolute ceiling `timeout_at` may be extended to (unix seconds). Frozen at creation =
    /// initial timeout + the Config budget then, so a later retune can't move an in-flight ceiling.
    pub max_extend_at: i64,
    pub fulfilled_at: i64,
    pub bump: u8,
}

// (Removed: the permanent `TxMarker` source-replay marker — A4. Source replay is now blocked by a
// validator freshness check: a deposit must be mined after `Reservation.created_at`; an old (replayed)
// deposit predates any later reservation. See SOLANA_VALIDATOR_OFFLOAD.md "Tx-hash replay protection".)

/// A miner's standing on-chain quote for one pair-direction and one backing
/// (`seeds = [QUOTE_SEED, miner, from_chain, to_chain, collateral_chain]`).
///
/// Replaces the off-chain Bittensor commitment string: the `(from_chain, to_chain)` ordering encodes
/// direction, so no `counter_rate`. The backing is in the seeds because it is part of the offer, not a
/// property of the miner: a dual-purse miner posts the same hub↔hub direction twice, once per backing,
/// at different rates (D2 — one market per pair, mixed by rate). Written by the owning miner
/// (`set_quote`, overwrites in place); pools pin whatever's current, so staleness is the miner's
/// problem. Closed + rent-refunded via `remove_quote`.
#[account]
#[derive(InitSpace)]
pub struct MinerQuote {
    /// The miner (signer) that owns this quote.
    pub miner: Pubkey,
    #[max_len(MAX_CHAIN_LEN)]
    pub from_chain: String,
    #[max_len(MAX_CHAIN_LEN)]
    pub to_chain: String,
    /// Chain whose asset backs any swap drawn from this quote ("sol" = the local vault). Validated at
    /// `set_quote` as a hub backing that is one of the two legs, and carried through Pool → Reservation
    /// → Swap; every collateral guard downstream reads it rather than the pair.
    #[max_len(MAX_CHAIN_LEN)]
    pub collateral_chain: String,
    /// Where the miner receives the source asset (on `from_chain`).
    #[max_len(MAX_ADDR_LEN)]
    pub miner_from_addr: String,
    /// Where the miner sends the destination asset (on `to_chain`).
    #[max_len(MAX_ADDR_LEN)]
    pub miner_to_addr: String,
    /// Offered rate, canonical 'dest per 1 canonical source' (hub pinned as source) in BOTH direction
    /// PDAs — never per-direction; direction is applied off-chain via `is_reverse`. Fixed-point =
    /// display_rate × RATE_PRECISION (1e18) — exact, no string parse; see constants::RATE_PRECISION.
    pub rate: u128,
    /// Advertised depth in the asset's own units (u128 to cover wei-scale).
    pub liquidity: u128,
    /// Unix timestamp of the last write (staleness signal for off-chain consumers).
    pub updated_at: i64,
    /// Stored PDA bump.
    pub bump: u8,
}

/// A miner's realized per-direction track record (`seeds = [STATS_SEED, miner, from_chain, to_chain]`).
///
/// Accrued by `confirm_swap` on quorum (one row per (miner, from_chain, to_chain)); never closed. Lets
/// the off-chain validator read realized volume + the executed rate via `getProgramAccounts` instead of
/// a local ledger. Realized VWAP for the direction = `total_to_amount / total_from_amount` (exact
/// integer math, no on-chain rate-string parse). Both fields are **asset-pure** (from/to in their own
/// chain's units) — kept deliberately asset-agnostic so the PDA survives split-collateral; the validator
/// derives any common-unit (SOL-notional) volume off-chain from its price feed, and the at-time notional
/// stays in the `SwapCompleted` event.
#[account]
#[derive(InitSpace)]
pub struct MinerDirectionStats {
    pub miner: Pubkey,
    #[max_len(MAX_CHAIN_LEN)]
    pub from_chain: String,
    #[max_len(MAX_CHAIN_LEN)]
    pub to_chain: String,
    /// Count of completed (confirmed) swaps in this direction.
    pub completed: u32,
    /// Sum of the source/destination leg amounts over completed swaps (asset-native units).
    pub total_from_amount: u128,
    pub total_to_amount: u128,
    /// Stored PDA bump.
    pub bump: u8,
}

/// Per-miner identity binding (`seeds = [BIND_SEED, miner]`): links a miner's Solana pubkey to its
/// Bittensor hotkey. `hotkey_sig` is an sr25519 signature by the hotkey over the miner's Solana pubkey;
/// the contract only STORES it (sr25519 verify is too costly on-chain) — the validator verifies it
/// off-chain. This PDA enforces pubkey→≤1 hotkey structurally; the reverse (hotkey→≤1 pubkey) is enforced
/// by the `HotkeyBinding` marker below. The miner may re-bind in place (refresh sig / change hotkey).
#[account]
#[derive(InitSpace)]
pub struct Binding {
    /// The miner's Solana pubkey (== seed; stored for `getProgramAccounts` convenience).
    pub miner: Pubkey,
    /// Bittensor hotkey (sr25519 public key).
    pub hotkey: [u8; 32],
    /// sr25519 signature by `hotkey` over the miner pubkey — validator-verified off-chain.
    pub hotkey_sig: [u8; 64],
    /// Unix timestamp of the last (re)bind (staleness signal for off-chain consumers).
    pub bound_at: i64,
    /// Stored PDA bump.
    pub bump: u8,
}

/// Set-once hotkey→pubkey reverse marker (`seeds = [HOTKEY_BIND_SEED, hotkey]`): the first pubkey to bind
/// a hotkey claims it permanently. A second, different pubkey trying the same hotkey is rejected, so the
/// strike-dodge (struck pubkey rotates to a fresh one and re-binds the same hotkey) is closed on-chain
/// rather than relying on every validator's off-chain first-seen pin. Never closed — one tiny rent-funded
/// marker per identity (bounded by hotkey churn, not per-event).
#[account]
#[derive(InitSpace)]
pub struct HotkeyBinding {
    /// The pubkey that first claimed this hotkey (== the `Binding.miner`); also a reverse lookup.
    pub miner: Pubkey,
    /// Stored PDA bump.
    pub bump: u8,
}

/// One bid into a reservation lottery `Pool`. A bid is JUST the router competing for the seat — no
/// taker, no amounts. The winner names the fill later via `finalize_reservation`. The miner quote is
/// the pool's pinned snapshot, not per-request.
#[derive(AnchorSerialize, AnchorDeserialize, Clone, InitSpace)]
pub struct Request {
    /// The account that routed this bid — a whitelisted validator OR a plain user (entry is
    /// permissionless). Also the lottery weight key (0 if not whitelisted) and the dedup key.
    pub router: Pubkey,
}

/// A reservation-lottery contest for one idle miner (`seeds = [POOL_SEED, miner]`).
///
/// Opened by the first router to route a request (pinning the miner's quote for the chosen pair);
/// later in-window requests must match that pair. `resolve_pool` runs a stake-weighted draw after
/// `closes_at` and creates the winner's `Reservation`. Keyed per-miner; the account is reused across
/// contests (`opened_at == 0` = available), reset rather than closed by `resolve_pool`.
#[account]
#[derive(InitSpace)]
pub struct Pool {
    pub miner: Pubkey,
    /// Pinned pair + miner-quote snapshot, copied from the `MinerQuote` PDA at open.
    #[max_len(MAX_CHAIN_LEN)]
    pub from_chain: String,
    #[max_len(MAX_CHAIN_LEN)]
    pub to_chain: String,
    /// The quote's declared backing, pinned at open beside the rate — it is what `resolve_pool` copies
    /// into the Reservation, so the contest and the swap it produces can never disagree about which
    /// purse is on the hook. Late bids must match it (a different backing is a different offer).
    #[max_len(MAX_CHAIN_LEN)]
    pub collateral_chain: String,
    #[max_len(MAX_ADDR_LEN)]
    pub miner_from_addr: String,
    #[max_len(MAX_ADDR_LEN)]
    pub miner_to_addr: String,
    /// Canonical rate (see `MinerQuote::rate`); fixed-point = display_rate × RATE_PRECISION (1e18).
    pub rate: u128,
    /// Unix seconds the pool opened (0 = available/empty slot).
    pub opened_at: i64,
    /// Unix seconds the request window closes; `resolve_pool` is callable after this.
    pub closes_at: i64,
    /// Future slot whose SlotHash seeds the draw (pinned at open).
    pub seed_slot: u64,
    /// Requests this contest (deduped by router), capped at MAX_VALIDATORS.
    #[max_len(MAX_VALIDATORS)]
    pub requests: Vec<Request>,
    /// Stored PDA bump.
    pub bump: u8,
}
