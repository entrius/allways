use anchor_lang::prelude::*;

use crate::constants::{fulfillment_grace_secs, MAX_TX_LEN, MINER_SEED, SWAP_SEED};
use crate::error::ErrorCode;
use crate::events::{FulfillmentGraceApplied, SwapFulfilled};
use crate::state::{MinerState, Swap, SwapStatus};

/// Miner records that they sent the destination funds. Records only the tx hash/block — `to_amount`
/// is the pinned reservation value and is no longer miner-supplied (v2 cleanup). Active → Fulfilled.
///
/// Fulfillment also slides `timeout_at` to at least now + the destination chain's confirmation window
/// (capped by the frozen `max_extend_at` ceiling, never shortened): a miner who broadcast just before
/// the deadline was previously slashable while the tx confirmed — paid AND slashed. The grace is
/// one-shot (Active → Fulfilled happens once), so a fake fulfillment buys a bounded delay at most —
/// which is why `timeout_swap` deliberately stays callable on Fulfilled swaps.
#[derive(Accounts)]
#[instruction(swap_key: [u8; 32])]
pub struct MarkFulfilled<'info> {
    pub miner: Signer<'info>,

    #[account(
        mut,
        seeds = [MINER_SEED, miner.key().as_ref()],
        bump = miner_state.bump,
        constraint = miner_state.miner == miner.key(),
    )]
    pub miner_state: Account<'info, MinerState>,

    #[account(
        mut,
        seeds = [SWAP_SEED, swap_key.as_ref()],
        bump = swap.bump,
        has_one = miner,
    )]
    pub swap: Account<'info, Swap>,
}

pub fn handler(
    ctx: Context<MarkFulfilled>,
    swap_key: [u8; 32],
    to_tx_hash: String,
    to_tx_block: u32,
) -> Result<()> {
    require!(to_tx_hash.len() <= MAX_TX_LEN, ErrorCode::StringTooLong);

    let swap = &mut ctx.accounts.swap;
    require!(swap.status == SwapStatus::Active, ErrorCode::InvalidStatus);

    let now = Clock::get()?.unix_timestamp;
    let miner = swap.miner;
    let to_amount = swap.to_amount;

    swap.to_tx_hash = to_tx_hash.clone();
    swap.to_tx_block = to_tx_block;
    swap.status = SwapStatus::Fulfilled;
    swap.fulfilled_at = now;

    // Confirmation grace: forward-only and ceiling-capped, mirroring extend_timeout's invariants.
    // Also covers a late fulfillment (now past timeout_at with no timeout quorum yet) — exactly the
    // delivered-then-slashed case this exists for. busy_until follows so the miner stays locked
    // (no deactivate/withdraw) through the extended deadline.
    let target = now
        .saturating_add(fulfillment_grace_secs(&swap.to_chain))
        .min(swap.max_extend_at);
    if target > swap.timeout_at {
        swap.timeout_at = target;
        ctx.accounts.miner_state.busy_until = ctx.accounts.miner_state.busy_until.max(target);
        emit!(FulfillmentGraceApplied { swap_key, miner, timeout_at: target });
    }

    emit!(SwapFulfilled {
        swap_key,
        miner,
        to_tx_hash,
        to_amount,
    });
    Ok(())
}
