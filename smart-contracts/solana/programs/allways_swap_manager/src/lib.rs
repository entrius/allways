pub mod backing;
pub mod constants;
pub mod consensus;
pub mod error;
pub mod events;
pub mod instructions;
pub mod lottery;
pub mod penalty;
pub mod state;
pub mod validate;

use anchor_lang::prelude::*;

pub use constants::*;
pub use instructions::*;
pub use state::*;

declare_id!("6JVBEj5w27J2SVjERmv2c7wXgFee9nSSBKUJevHehyBD");

#[program]
pub mod allways_swap_manager {
    use super::*;

    /// Create Config + Treasury with collateral bounds, timeout, consensus threshold,
    /// swap-size bounds, and reservation TTL.
    #[allow(clippy::too_many_arguments)]
    pub fn initialize(
        ctx: Context<Initialize>,
        min_collateral: u64,
        max_collateral: u64,
        fulfillment_timeout_secs: i64,
        consensus_threshold_percent: u8,
        min_swap_amount: u64,
        max_swap_amount: u64,
        reservation_ttl_secs: i64,
    ) -> Result<()> {
        initialize::handler(
            ctx,
            min_collateral,
            max_collateral,
            fulfillment_timeout_secs,
            consensus_threshold_percent,
            min_swap_amount,
            max_swap_amount,
            reservation_ttl_secs,
        )
    }

    /// Miner deposits SOL collateral into the vault.
    pub fn post_collateral(ctx: Context<PostCollateral>, amount: u64) -> Result<()> {
        post_collateral::handler(ctx, amount)
    }

    /// Miner withdraws SOL collateral (subject to inactive / no-swap / cooldown guards).
    pub fn withdraw_collateral(ctx: Context<WithdrawCollateral>, amount: u64) -> Result<()> {
        withdraw_collateral::handler(ctx, amount)
    }

    // --- Validator-set admin (with per-validator draw weight) ---
    pub fn add_validator(ctx: Context<AdminConfig>, validator: Pubkey, weight: u64) -> Result<()> {
        admin::add_validator(ctx, validator, weight)
    }
    pub fn remove_validator(ctx: Context<AdminConfig>, validator: Pubkey) -> Result<()> {
        admin::remove_validator(ctx, validator)
    }
    pub fn set_consensus_threshold(ctx: Context<AdminConfig>, percent: u8) -> Result<()> {
        admin::set_consensus_threshold(ctx, percent)
    }
    /// Emergency halt: admin toggles the global halt flag (gates deposits / activations /
    /// reservation pools).
    pub fn set_halted(ctx: Context<AdminConfig>, halted: bool) -> Result<()> {
        admin::set_halted(ctx, halted)
    }

    // --- Runtime config setters (admin) ---
    pub fn set_min_collateral(ctx: Context<AdminConfig>, amount: u64) -> Result<()> {
        admin::set_min_collateral(ctx, amount)
    }
    pub fn set_max_collateral(ctx: Context<AdminConfig>, amount: u64) -> Result<()> {
        admin::set_max_collateral(ctx, amount)
    }
    pub fn set_fulfillment_timeout(ctx: Context<AdminConfig>, secs: i64) -> Result<()> {
        admin::set_fulfillment_timeout(ctx, secs)
    }
    pub fn set_min_swap_amount(ctx: Context<AdminConfig>, amount: u64) -> Result<()> {
        admin::set_min_swap_amount(ctx, amount)
    }
    pub fn set_max_swap_amount(ctx: Context<AdminConfig>, amount: u64) -> Result<()> {
        admin::set_max_swap_amount(ctx, amount)
    }
    /// TAO-backed swap bounds, in rao (the lamport pair above is SOL-backed only).
    pub fn set_tao_min_swap_amount(ctx: Context<AdminConfig>, amount: u64) -> Result<()> {
        admin::set_tao_min_swap_amount(ctx, amount)
    }
    pub fn set_tao_max_swap_amount(ctx: Context<AdminConfig>, amount: u64) -> Result<()> {
        admin::set_tao_max_swap_amount(ctx, amount)
    }
    /// How long a non-locally-backed timeout keeps the miner busy while the penalty settles on the
    /// backing chain (busy-until-settled).
    pub fn set_settlement_grace(ctx: Context<AdminConfig>, secs: i64) -> Result<()> {
        admin::set_settlement_grace(ctx, secs)
    }
    /// Attested bond floor for activating the TAO backing, in rao.
    pub fn set_tao_min_collateral(ctx: Context<AdminConfig>, amount: u64) -> Result<()> {
        admin::set_tao_min_collateral(ctx, amount)
    }
    /// How stale the global attestation heartbeat may get before non-local backings fuse off at entry.
    pub fn set_attest_max_age(ctx: Context<AdminConfig>, secs: i64) -> Result<()> {
        admin::set_attest_max_age(ctx, secs)
    }
    pub fn set_reservation_ttl(ctx: Context<AdminConfig>, secs: i64) -> Result<()> {
        admin::set_reservation_ttl(ctx, secs)
    }
    pub fn set_reservation_fee(ctx: Context<AdminConfig>, lamports: u64) -> Result<()> {
        admin::set_reservation_fee(ctx, lamports)
    }
    pub fn set_pool_window(ctx: Context<AdminConfig>, secs: i64) -> Result<()> {
        admin::set_pool_window(ctx, secs)
    }
    pub fn set_finalize_window(ctx: Context<AdminConfig>, secs: i64) -> Result<()> {
        admin::set_finalize_window(ctx, secs)
    }
    pub fn set_weights_update_min_interval(ctx: Context<AdminConfig>, secs: i64) -> Result<()> {
        admin::set_weights_update_min_interval(ctx, secs)
    }
    pub fn set_max_total_extension(ctx: Context<AdminConfig>, secs: i64) -> Result<()> {
        admin::set_max_total_extension(ctx, secs)
    }
    // --- Consensus-governed validator weights ---
    /// A validator submits the full weight vector (index-aligned to `Config.validators`) plus
    /// `round_key` = the snapshot hash, which keys the vote-round PDA (competing proposals coexist);
    /// on quorum the weights are saved. Validators read stake off-chain and converge on one snapshot.
    pub fn vote_set_weights(
        ctx: Context<VoteSetWeights>,
        weights: Vec<u64>,
        round_key: [u8; 32],
    ) -> Result<()> {
        vote_set_weights::handler(ctx, weights, round_key)
    }

    // --- Miner activation consensus (per backing purse) ---
    /// A validator votes to activate one of a miner's backings ("sol" | "tao"); its bit is set on quorum.
    pub fn vote_activate(ctx: Context<VoteActivate>, backing: String) -> Result<()> {
        vote_activate::handler(ctx, backing)
    }
    /// A validator votes to force-deactivate one of a miner's backings; its bit is cleared on quorum.
    pub fn vote_deactivate(ctx: Context<VoteDeactivate>, backing: String) -> Result<()> {
        vote_deactivate::handler(ctx, backing)
    }

    // --- Bond attestation (the collateral mirror for backings this program can't read) ---
    /// A validator attests a miner's effective bond on one backing chain; written on quorum. The full
    /// payload is hash-bound, so divergent attestations conflict instead of co-counting.
    pub fn vote_set_attestation(
        ctx: Context<VoteSetAttestation>,
        chain: String,
        effective_balance: u64,
        locked: bool,
        epoch: u64,
    ) -> Result<()> {
        vote_set_attestation::handler(ctx, chain, effective_balance, locked, epoch)
    }
    /// A validator bumps the global attestation heartbeat; on quorum it advances to the chain's clock.
    /// While it is stale, entry into any non-locally-backed swap is fused off.
    pub fn vote_attest_heartbeat(ctx: Context<VoteAttestHeartbeat>) -> Result<()> {
        vote_attest_heartbeat::handler(ctx)
    }
    /// Miner self-deactivation (no consensus). `backing = Some(chain)` drops that purse only; `None`
    /// drops every purse (the full exit).
    pub fn deactivate(ctx: Context<Deactivate>, backing: Option<String>) -> Result<()> {
        deactivate::handler(ctx, backing)
    }

    // --- Reservation lottery (two-phase: bid → draw → finalize) ---
    /// A router bids into (or opens) a per-miner reservation-lottery pool for a pair. First caller pins
    /// the miner's on-chain quote; every fresh router pays a flat anti-spam fee to treasury. A bid
    /// carries no taker and no amounts — the seat winner names those in `finalize_reservation`.
    pub fn open_or_request(
        ctx: Context<OpenOrRequest>,
        from_chain: String,
        to_chain: String,
        collateral_chain: String,
    ) -> Result<()> {
        open_or_request::handler(ctx, from_chain, to_chain, collateral_chain)
    }
    /// Permissionless: after the window closes, run the stake-weighted draw and create the winner's
    /// UNFILLED reservation (pins router + miner quote; `reserved_until = 0`).
    pub fn resolve_pool(ctx: Context<ResolvePool>) -> Result<()> {
        resolve_pool::handler(ctx)
    }
    /// The seat winner (`reservation.router`) names the taker + amounts, filling the reservation
    /// (`reserved_until = now + ttl`). Runs the swap-size bounds + collateral gate + the collateral bind.
    pub fn finalize_reservation(
        ctx: Context<FinalizeReservation>,
        user: Pubkey,
        user_from_addr: String,
        user_to_addr: String,
        collateral_amount: u64,
        from_amount: u128,
        to_amount: u128,
    ) -> Result<()> {
        finalize_reservation::handler(
            ctx,
            user,
            user_from_addr,
            user_to_addr,
            collateral_amount,
            from_amount,
            to_amount,
        )
    }
    /// Permissionless: reap an unfilled reservation past its finalize deadline, freeing the miner.
    pub fn close_unfilled_reservation(ctx: Context<CloseUnfilledReservation>) -> Result<()> {
        close_unfilled_reservation::handler(ctx)
    }

    // --- Swap lifecycle ---
    /// Permissionless: the reservation holder records their source-tx hash on-chain, creating the swap
    /// in `PendingAttestation` (all terms copied from the immutable reservation; no miner obligation).
    pub fn submit_swap_claim(
        ctx: Context<SubmitSwapClaim>,
        swap_key: [u8; 32],
        from_tx_hash: String,
        from_tx_block: u32,
    ) -> Result<()> {
        submit_swap_claim::handler(ctx, swap_key, from_tx_hash, from_tx_block)
    }
    /// Validators attest a pending claim (`PendingAttestation` → `Active` on quorum); the miner's
    /// obligation deadline starts here.
    pub fn vote_initiate(ctx: Context<VoteInitiate>, swap_key: [u8; 32]) -> Result<()> {
        vote_initiate::handler(ctx, swap_key)
    }
    /// Permissionless: reap an orphaned `PendingAttestation` claim whose reservation expired (rent →
    /// caller; frees the reservation's claim slot).
    pub fn close_stale_claim(ctx: Context<CloseStaleClaim>, swap_key: [u8; 32]) -> Result<()> {
        close_stale_claim::handler(ctx, swap_key)
    }
    /// Miner records destination fulfillment (tx hash/block); Active → Fulfilled.
    pub fn mark_fulfilled(
        ctx: Context<MarkFulfilled>,
        swap_key: [u8; 32],
        to_tx_hash: String,
        to_tx_block: u32,
    ) -> Result<()> {
        mark_fulfilled::handler(ctx, swap_key, to_tx_hash, to_tx_block)
    }
    /// Validators confirm a fulfilled swap (1% fee → treasury; swap closed) on quorum.
    pub fn confirm_swap(
        ctx: Context<ConfirmSwap>,
        swap_key: [u8; 32],
        from_chain: String,
        to_chain: String,
    ) -> Result<()> {
        confirm_swap::handler(ctx, swap_key, from_chain, to_chain)
    }
    /// Validators time out an overdue swap (slash → user refund; swap closed) on quorum.
    pub fn timeout_swap(ctx: Context<TimeoutSwap>, swap_key: [u8; 32]) -> Result<()> {
        timeout_swap::handler(ctx, swap_key)
    }

    // --- Deadline extensions (single validator, no quorum; bounded by a frozen ceiling) ---
    /// A validator slides a reservation's deadline forward while it waits on slow source-chain
    /// confirmation. Bounded by the per-reservation ceiling frozen at creation.
    pub fn extend_reservation(ctx: Context<ExtendReservation>, target_at: i64) -> Result<()> {
        extend_reservation::handler(ctx, target_at)
    }
    /// A validator slides a swap's fulfillment timeout forward while it waits on slow destination-chain
    /// confirmation. Bounded by the per-swap ceiling frozen at creation.
    pub fn extend_timeout(
        ctx: Context<ExtendTimeout>,
        swap_key: [u8; 32],
        target_at: i64,
    ) -> Result<()> {
        extend_timeout::handler(ctx, swap_key, target_at)
    }

    // --- Treasury ---
    /// Admin withdraws accrued protocol fees from the treasury PDA to a recipient.
    pub fn withdraw_treasury(ctx: Context<WithdrawTreasury>, amount: u64) -> Result<()> {
        withdraw_treasury::handler(ctx, amount)
    }

    // --- On-chain miner quotes ---
    /// Miner publishes/overwrites its standing quote for one pair-direction and one backing (the
    /// reverse direction, and the same direction on the other purse, are separate quotes). The backing
    /// must be a hub on one of the legs and already activated for this miner.
    #[allow(clippy::too_many_arguments)]
    pub fn set_quote(
        ctx: Context<SetQuote>,
        from_chain: String,
        to_chain: String,
        collateral_chain: String,
        miner_from_addr: String,
        miner_to_addr: String,
        rate: u128,
        liquidity: u128,
    ) -> Result<()> {
        set_quote::handler(
            ctx,
            from_chain,
            to_chain,
            collateral_chain,
            miner_from_addr,
            miner_to_addr,
            rate,
            liquidity,
        )
    }
    /// Miner removes one of its quotes; the PDA is closed and its rent refunded to the miner.
    pub fn remove_quote(
        ctx: Context<RemoveQuote>,
        from_chain: String,
        to_chain: String,
        collateral_chain: String,
    ) -> Result<()> {
        remove_quote::handler(ctx, from_chain, to_chain, collateral_chain)
    }
    /// Permissionless: reap a quote stranded at the pre-W2b derivation, refunding its rent to the
    /// miner that paid it. Cannot touch a live quote (see the handler's address proof).
    pub fn close_legacy_quote(ctx: Context<CloseLegacyQuote>) -> Result<()> {
        close_legacy_quote::handler(ctx)
    }
    /// Permissionless: reap a `Pool` stranded at the retired pre-v3.1 `[pool, miner]` address (the
    /// live seeds carry the backing), refunding rent to the miner. Any historical layout — the
    /// address itself is the legacy proof.
    pub fn close_legacy_pool(ctx: Context<CloseLegacyPool>) -> Result<()> {
        close_legacy_slot::close_pool(ctx)
    }
    /// The same reap for a `Reservation` stranded at the retired `[resv, miner]` address.
    pub fn close_legacy_reservation(ctx: Context<CloseLegacyReservation>) -> Result<()> {
        close_legacy_slot::close_reservation(ctx)
    }
    /// The same reap for an initiate `VoteRound` stranded at the retired per-miner address (live
    /// rounds key by swap_key). Rent → caller: rounds were validator-funded, not miner-funded.
    pub fn close_legacy_initiate_round(ctx: Context<CloseLegacyInitiateRound>) -> Result<()> {
        close_legacy_slot::close_initiate_round(ctx)
    }

    // --- Identity binding ---
    /// A miner links its Solana pubkey to its Bittensor hotkey (stores the hotkey + an sr25519 sig the
    /// validator verifies off-chain). Overwrites in place on re-bind.
    pub fn bind_hotkey(
        ctx: Context<BindHotkey>,
        hotkey: [u8; 32],
        hotkey_sig: [u8; 64],
    ) -> Result<()> {
        bind_hotkey::handler(ctx, hotkey, hotkey_sig)
    }

    // --- v3 upgrade cranks (admin, idempotent; run migrate_config first) ---
    /// Rewrite the v10 Config into the v12 layout, seeding the W1+W2 fields with their defaults.
    pub fn migrate_config(ctx: Context<MigrateConfig>) -> Result<()> {
        migrate::migrate_config(ctx)
    }
    /// Rewrite one v10 MinerState into the v12 layout, carrying the legacy `active` bool into the SOL
    /// activation bit.
    pub fn migrate_miner_state(ctx: Context<MigrateMinerState>) -> Result<()> {
        migrate::migrate_miner_state(ctx)
    }
}
