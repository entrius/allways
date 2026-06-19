pub mod constants;
pub mod consensus;
pub mod error;
pub mod events;
pub mod instructions;
pub mod lottery;
pub mod penalty;
pub mod state;

use anchor_lang::prelude::*;

pub use constants::*;
pub use instructions::*;
pub use state::*;

declare_id!("BtVm5a1hKvMrrEHQ876Ev23dYVZAkWpYkf86VZi3z1Li");

#[program]
pub mod allways_swap_manager {
    use super::*;

    /// Phase 0–3: create Config + Vault with collateral bounds, timeout, consensus threshold,
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

    /// Phase 1: miner deposits SOL collateral into the vault.
    pub fn post_collateral(ctx: Context<PostCollateral>, amount: u64) -> Result<()> {
        post_collateral::handler(ctx, amount)
    }

    /// Phase 1: miner withdraws SOL collateral (subject to inactive / no-swap / cooldown guards).
    pub fn withdraw_collateral(ctx: Context<WithdrawCollateral>, amount: u64) -> Result<()> {
        withdraw_collateral::handler(ctx, amount)
    }

    // --- Phase 2: validator-set admin (Phase 8: per-validator draw weight) ---
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
    /// reservation pools, mirroring the ink! `set_halted`).
    pub fn set_halted(ctx: Context<AdminConfig>, halted: bool) -> Result<()> {
        admin::set_halted(ctx, halted)
    }
    /// Phase 8: set a validator's draw weight (admin bootstrap/fallback; superseded for routine use
    /// by the Phase 10 consensus path below).
    pub fn set_validator_weight(
        ctx: Context<AdminConfig>,
        validator: Pubkey,
        weight: u64,
    ) -> Result<()> {
        admin::set_validator_weight(ctx, validator, weight)
    }

    // --- Phase 10: consensus-governed validator weights ---
    /// A validator submits the full weight vector (index-aligned to `Config.validators`); on quorum
    /// the weights are saved. Validators read stake off-chain and converge on one snapshot.
    pub fn vote_set_weights(ctx: Context<VoteSetWeights>, weights: Vec<u64>) -> Result<()> {
        vote_set_weights::handler(ctx, weights)
    }

    // --- Phase 2: miner activation consensus ---
    /// A validator votes to activate a miner (active on quorum).
    pub fn vote_activate(ctx: Context<VoteActivate>) -> Result<()> {
        vote_activate::handler(ctx)
    }
    /// A validator votes to force-deactivate a miner (deactivated on quorum).
    pub fn vote_deactivate(ctx: Context<VoteDeactivate>) -> Result<()> {
        vote_deactivate::handler(ctx)
    }
    /// Miner self-deactivation (no consensus).
    pub fn deactivate(ctx: Context<Deactivate>) -> Result<()> {
        deactivate::handler(ctx)
    }

    // --- Phase 9: reservation lottery (replaces vote_reserve) ---
    /// A validator opens or joins a per-miner reservation-lottery pool for a pair. First caller pins
    /// the miner's on-chain quote; every caller pays a flat anti-spam fee → treasury.
    #[allow(clippy::too_many_arguments)]
    pub fn open_or_request(
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
        open_or_request::handler(
            ctx,
            from_chain,
            to_chain,
            user,
            user_from_addr,
            user_to_addr,
            sol_amount,
            from_amount,
            to_amount,
        )
    }
    /// Permissionless: after the window closes, run the stake-weighted draw and create the winner's
    /// reservation.
    pub fn resolve_pool(ctx: Context<ResolvePool>) -> Result<()> {
        resolve_pool::handler(ctx)
    }

    // --- Phase 4: swap lifecycle ---
    /// A validator votes to initiate a swap against an active reservation (created on quorum).
    pub fn vote_initiate(
        ctx: Context<VoteInitiate>,
        swap_key: [u8; 32],
        from_tx_hash: String,
        from_tx_block: u32,
        user: Pubkey,
        user_from_address: String,
        user_to_address: String,
    ) -> Result<()> {
        vote_initiate::handler(
            ctx,
            swap_key,
            from_tx_hash,
            from_tx_block,
            user,
            user_from_address,
            user_to_address,
        )
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
    pub fn confirm_swap(ctx: Context<ConfirmSwap>, swap_key: [u8; 32]) -> Result<()> {
        confirm_swap::handler(ctx, swap_key)
    }
    /// Validators time out an overdue swap (slash → user refund; swap closed) on quorum.
    pub fn timeout_swap(ctx: Context<TimeoutSwap>, swap_key: [u8; 32]) -> Result<()> {
        timeout_swap::handler(ctx, swap_key)
    }

    // --- Phase 6: treasury ---
    /// Admin withdraws accrued protocol fees from the vault treasury to a recipient.
    pub fn withdraw_treasury(ctx: Context<WithdrawTreasury>, amount: u64) -> Result<()> {
        withdraw_treasury::handler(ctx, amount)
    }

    // --- Phase 8: on-chain miner quotes ---
    /// Miner publishes/overwrites its standing quote for one pair-direction
    /// (`(from_chain, to_chain)` encodes direction; the reverse direction is a separate quote).
    /// Permissionless: any signer may post; the validator/UI filters to registered miners.
    pub fn set_quote(
        ctx: Context<SetQuote>,
        from_chain: String,
        to_chain: String,
        miner_from_addr: String,
        miner_to_addr: String,
        rate: String,
        liquidity: u128,
    ) -> Result<()> {
        set_quote::handler(
            ctx,
            from_chain,
            to_chain,
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
    ) -> Result<()> {
        remove_quote::handler(ctx, from_chain, to_chain)
    }
}
