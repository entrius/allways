use anchor_lang::prelude::*;
use anchor_lang::system_program::{transfer, Transfer};

use crate::backing;
use crate::constants::{
    quote_update_fee, quantize_rate_sig_figs, MAX_ADDR_LEN, MAX_CHAIN_LEN, MINER_SEED, QUOTE_SEED,
    TREASURY_SEED,
};
use crate::error::ErrorCode;
use crate::events::QuoteSet;
use crate::state::{MinerQuote, MinerState, Treasury};

/// Miner publishes (or overwrites) its standing quote for one pair-direction and one backing. The
/// signer owns the quote and must already have that purse activated — a quote is a public promise the
/// miner will honor with a specific bond, so it can't be posted against a purse the miner isn't
/// serving from. `(from_chain, to_chain)` ordering encodes the direction, so the reverse direction is a
/// separate quote (no `counter_rate`), and the backing is in the seeds, so a dual-purse miner can stand
/// both offers on one hub↔hub direction at different rates (D2). First call lazily creates the PDA
/// (miner pays rent) and is otherwise free; subsequent calls overwrite in place and pay a **decaying
/// anti-flashing fee** (`constants::quote_update_fee`) into the treasury PDA — high for rapid churn,
/// zero once a quote has stood long enough.
#[derive(Accounts)]
#[instruction(from_chain: String, to_chain: String, collateral_chain: String)]
pub struct SetQuote<'info> {
    #[account(mut)]
    pub miner: Signer<'info>,

    /// Seeded by the signer, so the activation check below reads the poster's OWN purses and no other.
    #[account(
        seeds = [MINER_SEED, miner.key().as_ref()],
        bump = miner_state.bump,
        has_one = miner,
    )]
    pub miner_state: Account<'info, MinerState>,

    #[account(
        init_if_needed,
        payer = miner,
        space = 8 + MinerQuote::INIT_SPACE,
        seeds = [
            QUOTE_SEED,
            miner.key().as_ref(),
            from_chain.as_bytes(),
            to_chain.as_bytes(),
            collateral_chain.as_bytes(),
        ],
        bump,
    )]
    pub quote: Account<'info, MinerQuote>,

    /// Treasury sink for the quote-update churn fee — subnet revenue, separate from collateral.
    #[account(mut, seeds = [TREASURY_SEED], bump = treasury.bump)]
    pub treasury: Account<'info, Treasury>,

    pub system_program: Program<'info, System>,
}

#[allow(clippy::too_many_arguments)]
pub fn handler(
    ctx: Context<SetQuote>,
    from_chain: String,
    to_chain: String,
    collateral_chain: String,
    miner_from_addr: String,
    miner_to_addr: String,
    rate: u128,
    liquidity: u128,
) -> Result<()> {
    // Mechanical sanity only — chains/addrs are opaque bounded strings. The rate is an opaque
    // fixed-point integer (display × RATE_PRECISION); the contract stores whatever the miner posts
    // (floored to RATE_SIG_FIGS, below) and never computes with it — routability/validity is the
    // off-chain layer's call (`is_executable_rate`), so there is no on-chain rate *validity* check.
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
    crate::validate::chain_ids_lowercase(&from_chain, &to_chain)?;

    // The backing must be a hub AND one of the legs, and the miner must already be serving that purse.
    // Leg membership is also what keeps the seed lowercase without a third casing check.
    // The first rule is what makes the choice automatic on a one-hub pair and free on a hub↔hub one;
    // the second stops a quote advertising a guarantee no live bond stands behind.
    let bit = backing::declarable_bit(&collateral_chain, &from_chain, &to_chain)?;
    require!(
        ctx.accounts.miner_state.active_backings & bit != 0,
        ErrorCode::MinerNotActive
    );

    // Floor to RATE_SIG_FIGS significant figures before it is stored OR emitted, so the pinned swap
    // rate, the off-chain crown ranking, and the indexer/UI all read the same canonical value. A
    // sub-perceptible undercut collapses into the incumbent's bucket (tie & split) instead of stealing
    // the crown for free; a real 5-sf improvement still wins.
    let rate = quantize_rate_sig_figs(rate);

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
    quote.collateral_chain = collateral_chain.clone();
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
        collateral_chain,
        rate,
        liquidity,
        updated_at: now,
        update_fee: fee,
    });
    Ok(())
}
