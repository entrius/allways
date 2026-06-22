use anchor_lang::prelude::*;
use anchor_lang::system_program::{transfer, Transfer};

use crate::constants::{quote_update_fee, QUOTE_SEED, TREASURY_SEED};
use crate::error::ErrorCode;
use crate::events::QuoteRemoved;
use crate::state::{MinerQuote, Treasury};

/// Miner removes one of its quotes — the PDA is closed (`close = miner`) and rent refunded. Charges
/// the same decaying churn fee an in-place update would (by how long the quote stood) → treasury, so
/// `remove_quote` + `set_quote` can't dodge the fee; a long-standing quote still removes free (#488).
#[derive(Accounts)]
#[instruction(from_chain: String, to_chain: String)]
pub struct RemoveQuote<'info> {
    #[account(mut)]
    pub miner: Signer<'info>,

    #[account(
        mut,
        close = miner,
        has_one = miner,
        seeds = [QUOTE_SEED, miner.key().as_ref(), from_chain.as_bytes(), to_chain.as_bytes()],
        bump = quote.bump,
    )]
    pub quote: Account<'info, MinerQuote>,

    /// Treasury sink for the remove churn fee — subnet revenue, separate from collateral.
    #[account(mut, seeds = [TREASURY_SEED], bump = treasury.bump)]
    pub treasury: Account<'info, Treasury>,

    pub system_program: Program<'info, System>,
}

pub fn handler(ctx: Context<RemoveQuote>, from_chain: String, to_chain: String) -> Result<()> {
    let now = Clock::get()?.unix_timestamp;
    // Same decaying fee an in-place overwrite would cost, keyed on how long this quote stood.
    let fee = quote_update_fee(now.saturating_sub(ctx.accounts.quote.updated_at));
    if fee > 0 {
        transfer(
            CpiContext::new(
                ctx.accounts.system_program.key(),
                Transfer {
                    from: ctx.accounts.miner.to_account_info(),
                    to: ctx.accounts.treasury.to_account_info(),
                },
            ),
            fee,
        )?;
        ctx.accounts.treasury.total = ctx
            .accounts
            .treasury
            .total
            .checked_add(fee)
            .ok_or(ErrorCode::Overflow)?;
    }

    emit!(QuoteRemoved {
        miner: ctx.accounts.miner.key(),
        from_chain,
        to_chain,
        remove_fee: fee,
    });
    Ok(())
}
