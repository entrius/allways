use anchor_lang::prelude::*;
use anchor_lang::system_program::{transfer, Transfer};

use crate::constants::{quote_update_fee, MAX_ADDR_LEN, MAX_CHAIN_LEN, MAX_RATE_LEN, QUOTE_SEED, VAULT_SEED};
use crate::error::ErrorCode;
use crate::events::QuoteSet;
use crate::state::{MinerQuote, Vault};

/// Miner publishes (or overwrites) its standing quote for one pair-direction. Permissionless: any
/// signer may post — a quote is advertised data that can't move funds, rent self-limits spam, and
/// the validator/UI filters to registered miners. `(from_chain, to_chain)` ordering encodes the
/// direction, so the reverse direction is a separate quote (no `counter_rate`). First call lazily
/// creates the PDA (miner pays rent) and is otherwise free; subsequent calls overwrite in place and
/// pay a **decaying anti-flashing fee** (`constants::quote_update_fee`) into the vault treasury — high
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

    /// Treasury sink for the quote-update churn fee (native lamports accrue here).
    #[account(mut, seeds = [VAULT_SEED], bump = vault.bump)]
    pub vault: Account<'info, Vault>,

    pub system_program: Program<'info, System>,
}

pub fn handler(
    ctx: Context<SetQuote>,
    from_chain: String,
    to_chain: String,
    miner_from_addr: String,
    miner_to_addr: String,
    rate: String,
    liquidity: u128,
) -> Result<()> {
    // Mechanical sanity only — chain identity/validity is the off-chain layer's call (chains are
    // opaque bounded strings on-chain).
    require!(
        !from_chain.is_empty()
            && !to_chain.is_empty()
            && !miner_from_addr.is_empty()
            && !miner_to_addr.is_empty()
            && !rate.is_empty(),
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
    require!(rate.len() <= MAX_RATE_LEN, ErrorCode::StringTooLong);
    require!(from_chain != to_chain, ErrorCode::SameChain);

    let now = Clock::get()?.unix_timestamp;
    let miner_key = ctx.accounts.miner.key();
    let bump = ctx.bumps.quote;

    // Anti-flashing churn fee → vault treasury (preserves the vault invariant). Charged on BOTH
    // create and overwrite so `remove_quote` + `set_quote` can't dodge it (review #7): a fresh/just-
    // removed PDA is treated as elapsed=0 → top tier; a quote that has stood past the window is free.
    let elapsed = if ctx.accounts.quote.miner != Pubkey::default() {
        now.saturating_sub(ctx.accounts.quote.updated_at)
    } else {
        0
    };
    let fee = quote_update_fee(elapsed);
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

    let quote = &mut ctx.accounts.quote;
    quote.miner = miner_key;
    quote.from_chain = from_chain.clone();
    quote.to_chain = to_chain.clone();
    quote.miner_from_addr = miner_from_addr;
    quote.miner_to_addr = miner_to_addr;
    quote.rate = rate.clone();
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
