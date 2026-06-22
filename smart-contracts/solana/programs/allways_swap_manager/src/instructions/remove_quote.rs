use anchor_lang::prelude::*;
use anchor_lang::system_program::{transfer, Transfer};

use crate::constants::{QUOTE_SEED, VAULT_SEED};
use crate::error::ErrorCode;
use crate::events::QuoteRemoved;
use crate::state::{MinerQuote, Vault};
use crate::tunables::quote_update_fee;

/// Miner removes one of its quotes. The PDA is closed (`close = miner`) and its rent refunded to the
/// miner. `has_one = miner` ensures only the owner can remove it. Removing a still-fresh quote pays
/// the same decaying anti-flashing fee as an in-place update (`set_quote`), so close-then-recreate
/// can't dodge the churn fee.
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

    /// Treasury sink for the churn fee (native lamports accrue here).
    #[account(mut, seeds = [VAULT_SEED], bump = vault.bump)]
    pub vault: Account<'info, Vault>,

    pub system_program: Program<'info, System>,
}

pub fn handler(ctx: Context<RemoveQuote>, from_chain: String, to_chain: String) -> Result<()> {
    let now = Clock::get()?.unix_timestamp;
    let fee = quote_update_fee(now.saturating_sub(ctx.accounts.quote.updated_at));
    if fee > 0 {
        transfer(
            CpiContext::new(
                ctx.accounts.system_program.key(),
                Transfer {
                    from: ctx.accounts.miner.to_account_info(),
                    to: ctx.accounts.vault.to_account_info(),
                },
            ),
            fee,
        )?;
        let vault = &mut ctx.accounts.vault;
        vault.treasury_total = vault
            .treasury_total
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
