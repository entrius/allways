use anchor_lang::prelude::*;

use crate::consensus::{record_vote, swap_request_hash};
use crate::constants::{
    ATTEST_SEED, CONFIG_SEED, MINER_SEED, REQ_INITIATE, RESV_SEED, SWAP_SEED, VOTE_SEED,
};
use crate::error::ErrorCode;
use crate::events::SwapInitiated;
use crate::state::{BondAttestation, Config, MinerState, Reservation, Swap, SwapStatus, VoteRound};

/// Validators attest a `PendingAttestation` claim: confirm the source-chain deposit is real and, on
/// quorum, promote the swap to `Active` — where the miner's obligation (`timeout_at`) begins. All terms
/// are already on the claim-created Swap (copied from the immutable reservation), so the bound hash is
/// trivial (`swap_key`) and no payout can be redirected at attestation.
#[derive(Accounts)]
#[instruction(swap_key: [u8; 32])]
pub struct VoteInitiate<'info> {
    #[account(mut)]
    pub validator: Signer<'info>,

    #[account(seeds = [CONFIG_SEED], bump = config.bump)]
    pub config: Account<'info, Config>,

    /// CHECK: identified by address only; bound via seeds + miner_state constraint + swap `has_one`.
    pub miner: UncheckedAccount<'info>,

    #[account(
        mut,
        seeds = [MINER_SEED, miner.key().as_ref()],
        bump = miner_state.bump,
        constraint = miner_state.miner == miner.key(),
    )]
    pub miner_state: Account<'info, MinerState>,

    #[account(
        mut,
        seeds = [RESV_SEED, miner.key().as_ref(), reservation.collateral_chain.as_bytes()],
        bump,
    )]
    pub reservation: Box<Account<'info, Reservation>>,

    /// Keyed by swap_key like the confirm/timeout rounds (v3.1): per-hub for free (a swap pins its
    /// backing), and closable at quorum since the unique seed is never reused.
    #[account(
        init_if_needed,
        payer = validator,
        space = 8 + VoteRound::INIT_SPACE,
        seeds = [VOTE_SEED, &[REQ_INITIATE], swap_key.as_ref()],
        bump,
    )]
    pub vote_round: Account<'info, VoteRound>,

    /// The claim-created swap (must be `PendingAttestation`). Boxed (String-heavy) off the BPF stack.
    #[account(
        mut,
        seeds = [SWAP_SEED, swap_key.as_ref()],
        bump = swap.bump,
        has_one = miner,
    )]
    pub swap: Box<Account<'info, Swap>>,

    /// The bond attestation for the swap's pinned backing — required for any backing but "sol".
    #[account(
        seeds = [ATTEST_SEED, miner.key().as_ref(), swap.collateral_chain.as_bytes()],
        bump,
    )]
    pub attestation: Option<Box<Account<'info, BondAttestation>>>,

    pub system_program: Program<'info, System>,
}

pub fn handler(ctx: Context<VoteInitiate>, swap_key: [u8; 32]) -> Result<()> {
    require!(
        ctx.accounts.swap.status == SwapStatus::PendingAttestation,
        ErrorCode::NotPending
    );
    // Source-replay defense is now a validator freshness check (deposit must be mined after
    // `Reservation.created_at`), not an on-chain marker — see SOLANA_VALIDATOR_OFFLOAD.md.

    let now = Clock::get()?.unix_timestamp;

    {
        let resv = &ctx.accounts.reservation;
        require!(
            resv.reserved_until != 0 && resv.reserved_until >= now,
            ErrorCode::NoReservation
        );
        require!(resv.claimed_swap_key == swap_key, ErrorCode::NotPending);
        // Never obligate a removed miner (defense-in-depth; resolve_pool also refuses an inactive miner).
        require!(ctx.accounts.miner_state.active, ErrorCode::MinerNotActive);
        // Self-dealing backstop (finalize_reservation is the primary guard; this also covers
        // reservations filled before that guard deployed).
        require!(ctx.accounts.swap.user != ctx.accounts.swap.miner, ErrorCode::SelfSwapNotAllowed);
        // Re-check the entry fuse: the heartbeat can go stale, or a penalty can land, between the fill
        // and the attestation — and this is the last gate before the miner is obligated.
        crate::backing::check_entry_gates(
            &ctx.accounts.config,
            &ctx.accounts.miner_state,
            &ctx.accounts.swap.collateral_chain,
            now,
        )?;
        // Obligation gate: the miner must hold the over-collateralization requirement in the purse the
        // swap pinned as its backing (same leg-lookup discipline as finalize), NET of what in-flight
        // obligations already reserve, before being bound.
        let purse = crate::backing::backing_purse(
            &ctx.accounts.swap.collateral_chain,
            &ctx.accounts.miner_state,
            ctx.accounts.attestation.as_deref().map(|a| &**a),
        )?;
        let hub_bit = crate::backing::backing_bit(&ctx.accounts.swap.collateral_chain)?;
        require!(
            purse.saturating_sub(ctx.accounts.miner_state.reserved(hub_bit))
                >= crate::constants::required_collateral(ctx.accounts.swap.collateral_amount),
            ErrorCode::InsufficientCollateral
        );
    }

    let bound = swap_request_hash(REQ_INITIATE, &swap_key);
    let validator = ctx.accounts.validator.key();
    let round_bump = ctx.bumps.vote_round;

    let quorum = record_vote(
        &mut ctx.accounts.vote_round,
        &ctx.accounts.config,
        validator,
        bound,
        round_bump,
        now,
    )?;

    if quorum {
        let timeout_at = now.saturating_add(ctx.accounts.config.fulfillment_timeout_secs);
        let max_extend_at = timeout_at.saturating_add(ctx.accounts.config.max_total_extension_secs);

        // Event values (read before the mutable borrow below). A3: all terms already live on the
        // claim-created swap (copied from the reservation at submit_swap_claim) — no re-copy here.
        let user = ctx.accounts.swap.user;
        let miner = ctx.accounts.swap.miner;
        let collateral_amount = ctx.accounts.swap.collateral_amount;
        let from_amount = ctx.accounts.swap.from_amount;
        let to_amount = ctx.accounts.swap.to_amount;
        let collateral_chain = ctx.accounts.swap.collateral_chain.clone();

        let swap = &mut ctx.accounts.swap;
        swap.status = SwapStatus::Active;
        swap.initiated_at = now;
        swap.timeout_at = timeout_at;
        swap.max_extend_at = max_extend_at;

        let bit = crate::backing::backing_bit(&ctx.accounts.swap.collateral_chain)?;
        ctx.accounts.miner_state.set_swap(bit, true);
        ctx.accounts.miner_state.set_busy(bit, timeout_at); // hub stays busy through the swap deadline
        // Reserve the obligation now that it binds; released at confirm/timeout quorum. Every
        // obligation terminates via an instruction, so the sum can never leak via a passive expiry.
        ctx.accounts
            .miner_state
            .add_reserved(bit, crate::constants::required_collateral(collateral_amount))?;
        ctx.accounts.reservation.reserved_until = 0; // consume the reservation
        ctx.accounts.reservation.claimed_swap_key = [0u8; 32];
        // Unique swap_key seed → the round is never reused; close it and refund rent, like the
        // confirm/timeout rounds. A straggler's late vote reverts on the NotPending status gate
        // above before it could re-create the round.
        ctx.accounts.vote_round.close(ctx.accounts.validator.to_account_info())?;

        emit!(SwapInitiated {
            swap_key,
            user,
            miner,
            collateral_amount,
            from_amount,
            to_amount,
            initiated_at: now,
            collateral_chain,
        });
    }
    Ok(())
}
