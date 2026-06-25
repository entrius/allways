use anchor_lang::prelude::*;

use crate::consensus::{record_vote, reset_round, swap_request_hash};
use crate::constants::{CONFIG_SEED, MINER_SEED, REQ_INITIATE, RESV_SEED, SWAP_SEED, VOTE_SEED};
use crate::error::ErrorCode;
use crate::events::SwapInitiated;
use crate::state::{Config, MinerState, Reservation, Swap, SwapStatus, VoteRound};

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

    #[account(mut, seeds = [RESV_SEED, miner.key().as_ref()], bump)]
    pub reservation: Box<Account<'info, Reservation>>,

    #[account(
        init_if_needed,
        payer = validator,
        space = 8 + VoteRound::INIT_SPACE,
        seeds = [VOTE_SEED, &[REQ_INITIATE], miner.key().as_ref()],
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
        // Obligation gate: miner must hold the over-collateralization requirement before being bound.
        require!(
            ctx.accounts.miner_state.collateral
                >= crate::constants::required_collateral(ctx.accounts.swap.sol_amount),
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
        let sol_amount = ctx.accounts.swap.sol_amount;
        let from_amount = ctx.accounts.swap.from_amount;
        let to_amount = ctx.accounts.swap.to_amount;

        let swap = &mut ctx.accounts.swap;
        swap.status = SwapStatus::Active;
        swap.initiated_at = now;
        swap.timeout_at = timeout_at;
        swap.max_extend_at = max_extend_at;

        ctx.accounts.miner_state.has_active_swap = true;
        ctx.accounts.miner_state.busy_until = timeout_at; // stay busy through the swap deadline
        ctx.accounts.reservation.reserved_until = 0; // consume the reservation
        ctx.accounts.reservation.claimed_swap_key = [0u8; 32];
        reset_round(&mut ctx.accounts.vote_round);

        emit!(SwapInitiated {
            swap_key,
            user,
            miner,
            sol_amount,
            from_amount,
            to_amount,
            initiated_at: now,
        });
    }
    Ok(())
}
