use anchor_lang::prelude::*;

use crate::constants::{MAX_TX_LEN, SWAP_SEED};
use crate::error::ErrorCode;
use crate::events::SwapFulfilled;
use crate::state::{Swap, SwapStatus};

/// Miner records that they sent the destination funds. Records only the tx hash/block — `to_amount`
/// is the pinned reservation value and is no longer miner-supplied (v2 cleanup). Active → Fulfilled.
#[derive(Accounts)]
#[instruction(swap_key: [u8; 32])]
pub struct MarkFulfilled<'info> {
    pub miner: Signer<'info>,

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

    emit!(SwapFulfilled {
        swap_key,
        miner,
        to_tx_hash,
        to_amount,
    });
    Ok(())
}
