use anchor_lang::prelude::*;

use crate::constants::QUOTE_SEED;
use crate::events::QuoteRemoved;
use crate::state::MinerQuote;

/// Miner removes one of its quotes. The PDA is closed (`close = miner`) and its rent refunded to the
/// miner. `has_one = miner` ensures only the owner can remove it.
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
}

pub fn handler(ctx: Context<RemoveQuote>, from_chain: String, to_chain: String) -> Result<()> {
    emit!(QuoteRemoved {
        miner: ctx.accounts.miner.key(),
        from_chain,
        to_chain,
    });
    Ok(())
}
