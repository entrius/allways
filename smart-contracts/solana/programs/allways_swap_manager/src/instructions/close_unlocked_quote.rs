use anchor_lang::prelude::*;

use crate::constants::ADDR_LOCK_SEED;
use crate::error::ErrorCode;
use crate::events::QuoteRemoved;
use crate::state::{addr_lock_key, AddrLock, MinerQuote};

/// Permissionless reap of a quote whose wallets aren't both locked to its miner — quotes posted before
/// wallet locks existed. Rent goes back to the miner, so anyone may run it and nobody profits; once
/// reaped, every standing quote's wallets belong to its miner.
#[derive(Accounts)]
pub struct CloseUnlockedQuote<'info> {
    /// Anyone — the caller only pays the transaction.
    pub caller: Signer<'info>,

    /// CHECK: must equal `quote.miner` (constraint below); receives the rent refund.
    #[account(mut)]
    pub miner: UncheckedAccount<'info>,

    #[account(mut, close = miner, constraint = quote.miner == miner.key() @ ErrorCode::NotMiner)]
    pub quote: Account<'info, MinerQuote>,

    /// CHECK: address and contents verified in the handler; may not exist yet.
    pub from_addr_lock: UncheckedAccount<'info>,

    /// CHECK: address and contents verified in the handler; may not exist yet.
    pub to_addr_lock: UncheckedAccount<'info>,
}

/// Whether `lock` is `addr`'s wallet lock and names `miner` as owner.
fn locked_to(lock: &AccountInfo, addr: &str, miner: &Pubkey, program_id: &Pubkey) -> Result<bool> {
    let (expected, _) = Pubkey::find_program_address(&[ADDR_LOCK_SEED, &addr_lock_key(addr)], program_id);
    require!(lock.key() == expected, ErrorCode::AddrLockKeyMismatch);
    if lock.owner != program_id {
        return Ok(false);
    }
    let data = lock.try_borrow_data()?;
    Ok(AddrLock::try_deserialize(&mut &data[..]).is_ok_and(|l| l.owner == *miner))
}

pub fn handler(ctx: Context<CloseUnlockedQuote>) -> Result<()> {
    let q = &ctx.accounts.quote;
    let locked = locked_to(&ctx.accounts.from_addr_lock, &q.miner_from_addr, &q.miner, ctx.program_id)?
        && locked_to(&ctx.accounts.to_addr_lock, &q.miner_to_addr, &q.miner, ctx.program_id)?;
    require!(!locked, ErrorCode::QuoteLocked);
    emit!(QuoteRemoved {
        miner: q.miner,
        from_chain: q.from_chain.clone(),
        to_chain: q.to_chain.clone(),
        collateral_chain: q.collateral_chain.clone(),
        remove_fee: 0,
    });
    Ok(())
}
