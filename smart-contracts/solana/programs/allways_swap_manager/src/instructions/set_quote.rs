use anchor_lang::prelude::*;
use anchor_lang::system_program::{transfer, Transfer};

use crate::constants::{quote_update_fee, MAX_ADDR_LEN, MAX_CHAIN_LEN, QUOTE_SEED, TREASURY_SEED};
use crate::error::ErrorCode;
use crate::events::QuoteSet;
use crate::state::{MinerQuote, Treasury};

/// Miner publishes (or overwrites) its standing quote for one pair-direction. Permissionless: any
/// signer may post — a quote is advertised data that can't move funds, rent self-limits spam, and
/// the validator/UI filters to registered miners. `(from_chain, to_chain)` ordering encodes the
/// direction, so the reverse direction is a separate quote (no `counter_rate`). First call lazily
/// creates the PDA (miner pays rent) and is otherwise free; subsequent calls overwrite in place and
/// pay a **decaying anti-flashing fee** (`constants::quote_update_fee`) into the treasury PDA — high
/// for rapid churn, zero once a quote has stood long enough.
#[derive(Accounts)]
#[instruction(from_chain: String, to_chain: String)]
pub struct SetQuote<'info> {
    #[account(mut)]
    pub miner: Signer<'info>,

    #[account(
        init_if_needed,
        payer = miner,
        space = 8 + MinerQuote::INIT_SPACE,
        seeds = [QUOTE_SEED, miner.key().as_ref(), from_chain.as_bytes(), to_chain.as_bytes()],
        bump,
    )]
    pub quote: Account<'info, MinerQuote>,

    /// Treasury sink for the quote-update churn fee — subnet revenue, separate from collateral.
    #[account(mut, seeds = [TREASURY_SEED], bump = treasury.bump)]
    pub treasury: Account<'info, Treasury>,

    pub system_program: Program<'info, System>,
}

pub fn handler(
    ctx: Context<SetQuote>,
    from_chain: String,
    to_chain: String,
    miner_from_addr: String,
    miner_to_addr: String,
    rate: u128,
    liquidity: u128,
) -> Result<()> {
    // Mechanical sanity only — chains/addrs are opaque bounded strings. The rate is an opaque
    // fixed-point integer (display × RATE_PRECISION); the contract stores whatever the miner posts
    // and never computes with it — routability/validity is the off-chain layer's call
    // (`is_executable_rate`), so there is deliberately no on-chain rate check.
    require!(
        !from_chain.is_empty()
            && !to_chain.is_empty()
            && !miner_from_addr.is_empty()
            && !miner_to_addr.is_empty(),
        ErrorCode::EmptyField
    );
    require!(
        from_chain.len() <= MAX_CHAIN_LEN && to_chain.len() <= MAX_CHAIN_LEN,
        ErrorCode::StringTooLong
    );
    require!(
        miner_from_addr.len() <= MAX_ADDR_LEN && miner_to_addr.len() <= MAX_ADDR_LEN,
        ErrorCode::StringTooLong
    );
    require!(from_chain != to_chain, ErrorCode::SameChain);

    let now = Clock::get()?.unix_timestamp;
    let miner_key = ctx.accounts.miner.key();
    let bump = ctx.bumps.quote;

    // Anti-flashing churn fee on UPDATES only — creation is free (no onboarding barrier), decaying to
    // zero the longer the prior quote stood; fee → treasury. The remove + re-create dodge is closed on
    // the remove side (see `remove_quote`), so creation needn't be charged.
    let fee = if ctx.accounts.quote.miner != Pubkey::default() {
        quote_update_fee(now.saturating_sub(ctx.accounts.quote.updated_at))
    } else {
        0
    };
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
        let treasury = &mut ctx.accounts.treasury;
        treasury.total = treasury
            .total
            .checked_add(fee)
            .ok_or(ErrorCode::Overflow)?;
    }

    let quote = &mut ctx.accounts.quote;
    quote.miner = miner_key;
    quote.from_chain = from_chain.clone();
    quote.to_chain = to_chain.clone();
    quote.miner_from_addr = miner_from_addr;
    quote.miner_to_addr = miner_to_addr;
    quote.rate = rate;
    quote.liquidity = liquidity;
    quote.updated_at = now;
    quote.bump = bump;

    emit!(QuoteSet {
        miner: miner_key,
        from_chain,
        to_chain,
        rate,
        liquidity,
        updated_at: now,
        update_fee: fee,
    });
    Ok(())
}
