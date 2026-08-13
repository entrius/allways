use anchor_lang::prelude::*;

use crate::consensus::{record_vote, swap_request_hash};
use crate::constants::{CONFIG_SEED, MINER_SEED, REQ_CANCEL, SWAP_SEED, VOTE_SEED};
use crate::error::ErrorCode;
use crate::events::SwapCancelled;
use crate::state::{Config, MinerState, Swap, SwapStatus, VoteRound};

/// Validators cancel a swap whose destination provably cannot receive the payout — an EVM contract that
/// reverts a correctly-gassed transfer, an ERC-20 blacklisted/paused destination, a Solana reserved
/// account. It is `timeout_swap` minus every punitive action: on quorum the Swap is closed and the miner
/// freed, with NO slash, NO fee, NO strike, and NO fund movement.
///
/// The refusal verdict is reached OFF-CHAIN, by the same quorum that would otherwise time the swap out,
/// via a per-chain evidence rule every validator runs deterministically (EVM: a mined reverted delivery
/// tx at/above the gas floor; ERC-20: attested issuer blacklist/pause; Solana: reserved-account
/// membership). The contract can neither parse an EVM receipt nor make an RPC call, so it carries NO
/// evidence on-chain — it trusts the supermajority here exactly as `timeout_swap` trusts it. Cancel and
/// timeout share the same 2/3 denominator over the same set, so the honest majority picks which terminal
/// fires and a minority can carry neither.
#[derive(Accounts)]
#[instruction(swap_key: [u8; 32])]
pub struct CancelSwap<'info> {
    #[account(mut)]
    pub validator: Signer<'info>,

    #[account(seeds = [CONFIG_SEED], bump = config.bump)]
    pub config: Account<'info, Config>,

    /// CHECK: bound via miner_state seeds + the swap `has_one`.
    pub miner: UncheckedAccount<'info>,

    #[account(
        mut,
        seeds = [MINER_SEED, miner.key().as_ref()],
        bump = miner_state.bump,
        constraint = miner_state.miner == miner.key(),
    )]
    pub miner_state: Account<'info, MinerState>,

    // No collateral_vault, treasury, or user: cancel moves no funds. There is no source custody on
    // Solana (the taker paid the miner directly, off-chain), so "miner keeps the source" is realized by
    // cancel simply never creating a refund path — zero contract action is required for it.

    // `swap` MUST resolve before `vote_round`: once a competing terminal has closed the Swap PDA, a
    // straggler cancel fails these seeds/`has_one` before `init_if_needed` could re-create the round.
    // This is what makes "first terminal to close the Swap wins" true — do not reorder.
    #[account(
        mut,
        seeds = [SWAP_SEED, swap_key.as_ref()],
        bump = swap.bump,
        has_one = miner,
    )]
    pub swap: Account<'info, Swap>,

    #[account(
        init_if_needed,
        payer = validator,
        space = 8 + VoteRound::INIT_SPACE,
        seeds = [VOTE_SEED, &[REQ_CANCEL], swap_key.as_ref()],
        bump,
    )]
    pub vote_round: Account<'info, VoteRound>,

    pub system_program: Program<'info, System>,
}

pub fn handler(ctx: Context<CancelSwap>, swap_key: [u8; 32], reason: u8) -> Result<()> {
    let now = Clock::get()?.unix_timestamp;

    // Status gate ONLY — deliberately no `now >= timeout_at`. Timeout's deadline gate is a fairness
    // guarantee protecting the miner from a premature *slash*; cancel is strictly lenient (no slash,
    // fee, or strike, miner keeps the source), so nothing needs protecting and it may fire the instant
    // refusal evidence is attested — ahead of the deadline — to bound hub occupancy. PendingAttestation
    // is excluded: no obligation yet, nothing to cancel (reap via close_stale_claim instead).
    require!(
        ctx.accounts.swap.status == SwapStatus::Active || ctx.accounts.swap.status == SwapStatus::Fulfilled,
        ErrorCode::InvalidStatus
    );

    // `reason` is NOT bound here — voters co-count the verdict "cancel this swap" even if they'd label
    // the refusal differently, so a bogus reason cannot split a legitimate quorum.
    let bound = swap_request_hash(REQ_CANCEL, &swap_key);
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
        let collateral_amount = ctx.accounts.swap.collateral_amount;
        let miner = ctx.accounts.swap.miner;
        let collateral_chain = ctx.accounts.swap.collateral_chain.clone();
        let bit = crate::backing::backing_bit(&collateral_chain)?;

        // Free the hub for EVERY backing, immediately. Nothing is owed on any chain — there is no
        // seizure to wait on — so, unlike timeout's vaulted branch, we set neither a busy-until-settled
        // nor a settling window: a hard 0, identical to confirm_swap's success close. This swap is the
        // sole holder of the hub bit (its reservation was consumed at vote_initiate).
        ctx.accounts.miner_state.set_swap(bit, false);
        ctx.accounts.miner_state.set_busy(bit, 0);
        // Deliberately NO set_settling(...) — nothing settles on a no-fault cancel.
        ctx.accounts
            .miner_state
            .release_reserved(bit, crate::constants::required_collateral(collateral_amount));
        // NOT touched: miner_state.collateral, treasury, failed_swaps. No apply_penalty, no lamport moves.

        // Close the per-swap round + swap; rent → validator. The straggler in the losing round reverts on
        // the now-gone swap before it could re-create this round (see the account-ordering note above).
        ctx.accounts.vote_round.close(ctx.accounts.validator.to_account_info())?;
        ctx.accounts.swap.close(ctx.accounts.validator.to_account_info())?;

        emit!(SwapCancelled {
            swap_key,
            miner,
            collateral_chain,
            collateral_amount,
            reason,
        });
    }
    Ok(())
}
