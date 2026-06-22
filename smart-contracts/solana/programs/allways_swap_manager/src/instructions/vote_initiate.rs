use anchor_lang::prelude::*;
use solana_keccak_hasher::hashv;

use crate::consensus::{initiate_hash, record_vote, reset_round};
use crate::constants::{
    CONFIG_SEED, MAX_ADDR_LEN, MAX_TX_LEN, MINER_SEED, REQ_INITIATE, RESV_SEED, SWAP_SEED, TX_SEED,
    VOTE_SEED,
};
use crate::error::ErrorCode;
use crate::events::SwapInitiated;
use crate::state::{Config, MinerState, Reservation, Swap, SwapStatus, TxMarker, VoteRound};

/// A validator votes to initiate a swap against an active reservation. Miner quote terms are sourced
/// from the immutable Reservation — never from args; the bound hash binds the user-side payout fields.
/// On quorum the Swap is created, the source-tx replay marker is set, and the reservation is consumed.
#[derive(Accounts)]
#[instruction(swap_key: [u8; 32])]
pub struct VoteInitiate<'info> {
    #[account(mut)]
    pub validator: Signer<'info>,

    #[account(seeds = [CONFIG_SEED], bump = config.bump)]
    pub config: Account<'info, Config>,

    /// CHECK: identified by address only; bound via seeds + miner_state constraint.
    pub miner: UncheckedAccount<'info>,

    #[account(
        mut,
        seeds = [MINER_SEED, miner.key().as_ref()],
        bump = miner_state.bump,
        constraint = miner_state.miner == miner.key(),
    )]
    pub miner_state: Account<'info, MinerState>,

    #[account(mut, seeds = [RESV_SEED, miner.key().as_ref()], bump)]
    pub reservation: Account<'info, Reservation>,

    #[account(
        init_if_needed,
        payer = validator,
        space = 8 + VoteRound::INIT_SPACE,
        seeds = [VOTE_SEED, &[REQ_INITIATE], miner.key().as_ref()],
        bump,
    )]
    pub vote_round: Account<'info, VoteRound>,

    #[account(
        init_if_needed,
        payer = validator,
        space = 8 + TxMarker::INIT_SPACE,
        seeds = [TX_SEED, swap_key.as_ref()],
        bump,
    )]
    pub tx_marker: Account<'info, TxMarker>,

    #[account(
        init_if_needed,
        payer = validator,
        space = 8 + Swap::INIT_SPACE,
        seeds = [SWAP_SEED, swap_key.as_ref()],
        bump,
    )]
    pub swap: Account<'info, Swap>,

    pub system_program: Program<'info, System>,
}

#[allow(clippy::too_many_arguments)]
pub fn handler(
    ctx: Context<VoteInitiate>,
    swap_key: [u8; 32],
    from_tx_hash: String,
    from_tx_block: u32,
    user: Pubkey,
    user_from_address: String,
    user_to_address: String,
) -> Result<()> {
    require!(from_tx_hash.len() <= MAX_TX_LEN, ErrorCode::StringTooLong);
    require!(user_from_address.len() <= MAX_ADDR_LEN, ErrorCode::StringTooLong);
    require!(user_to_address.len() <= MAX_ADDR_LEN, ErrorCode::StringTooLong);

    // swap_key integrity + permanent replay guard.
    require!(
        swap_key == hashv(&[from_tx_hash.as_bytes()]).to_bytes(),
        ErrorCode::SwapKeyMismatch
    );
    require!(!ctx.accounts.tx_marker.used, ErrorCode::DuplicateSourceTx);

    let now = Clock::get()?.unix_timestamp;

    // Reservation must be active, and the caller must be the original reserver.
    {
        let resv = &ctx.accounts.reservation;
        require!(
            resv.reserved_until != 0 && resv.reserved_until >= now,
            ErrorCode::NoReservation
        );
        require!(user_from_address == resv.from_addr, ErrorCode::UserMismatch);
        // Over-collateralization gate: miner must hold the swap size x the tunable requirement,
        // so a failed swap has a slash buffer beyond 1:1.
        require!(
            ctx.accounts.miner_state.collateral >= crate::constants::required_collateral(resv.sol_amount),
            ErrorCode::InsufficientCollateral
        );
    }

    let miner_key = ctx.accounts.miner.key();
    let bound = initiate_hash(
        &miner_key,
        &user,
        &user_from_address,
        &user_to_address,
        &from_tx_hash,
        from_tx_block,
    );
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
        let tx_marker_bump = ctx.bumps.tx_marker;
        let swap_bump = ctx.bumps.swap;

        // Copy the pinned quote out of the reservation (immutable terms).
        let (from_chain, to_chain, miner_from_addr, miner_to_addr, rate, sol_amount, from_amount, to_amount) = {
            let r = &ctx.accounts.reservation;
            (
                r.from_chain.clone(),
                r.to_chain.clone(),
                r.miner_from_addr.clone(),
                r.miner_to_addr.clone(),
                r.rate.clone(),
                r.sol_amount,
                r.from_amount,
                r.to_amount,
            )
        };

        let swap = &mut ctx.accounts.swap;
        swap.user = user;
        swap.miner = miner_key;
        swap.from_chain = from_chain;
        swap.to_chain = to_chain;
        swap.user_from_addr = user_from_address;
        swap.user_to_addr = user_to_address;
        swap.miner_from_addr = miner_from_addr;
        swap.miner_to_addr = miner_to_addr;
        swap.rate = rate;
        swap.sol_amount = sol_amount;
        swap.from_amount = from_amount;
        swap.to_amount = to_amount;
        swap.from_tx_hash = from_tx_hash;
        swap.from_tx_block = from_tx_block;
        swap.to_tx_hash = String::new();
        swap.to_tx_block = 0;
        swap.status = SwapStatus::Active;
        swap.initiated_at = now;
        swap.timeout_at = timeout_at;
        swap.fulfilled_at = 0;
        swap.bump = swap_bump;

        let tx_marker = &mut ctx.accounts.tx_marker;
        tx_marker.used = true;
        tx_marker.bump = tx_marker_bump;

        ctx.accounts.miner_state.has_active_swap = true;
        ctx.accounts.miner_state.busy_until = timeout_at; // stay busy through the swap deadline
        ctx.accounts.reservation.reserved_until = 0; // consume the reservation
        reset_round(&mut ctx.accounts.vote_round);

        emit!(SwapInitiated {
            swap_key,
            user,
            miner: miner_key,
            sol_amount,
            from_amount,
            to_amount,
            initiated_at: now,
        });
    }
    Ok(())
}
