use anchor_lang::prelude::*;
use anchor_lang::Discriminator;

use crate::constants::QUOTE_SEED;
use crate::error::ErrorCode;
use crate::events::QuoteRemoved;
use crate::state::MinerQuote;

/// `MinerQuote` as the pre-W2b program wrote it: no `collateral_chain`, and one PDA per direction
/// rather than per (direction, backing). Field order is the contract — this mirror is the only way to
/// read an account the live type can no longer parse.
#[derive(AnchorDeserialize)]
struct MinerQuoteLegacy {
    miner: Pubkey,
    from_chain: String,
    to_chain: String,
    miner_from_addr: String,
    miner_to_addr: String,
    rate: u128,
    liquidity: u128,
    updated_at: i64,
    bump: u8,
}

/// Permissionless reap of a quote stranded at the OLD derivation by the W2b seed change. Rent goes
/// back to the miner who paid it, so this is a favor anyone can do and nobody can profit from.
///
/// The proof that an account is genuinely orphaned is self-contained: re-derive the LEGACY seeds from
/// the account's own stored fields and require they land on this address. A live (post-W2b) quote sits
/// at a five-seed address that no four-seed derivation can reproduce, so it can never be closed here —
/// no fee is charged either, since the miner is not choosing to retract anything.
#[derive(Accounts)]
pub struct CloseLegacyQuote<'info> {
    /// Anyone — the caller only pays the transaction.
    pub caller: Signer<'info>,

    /// CHECK: must equal the `miner` stored in the account below; receives the rent refund.
    #[account(mut)]
    pub miner: UncheckedAccount<'info>,

    /// CHECK: owner, discriminator and legacy-address derivation are all verified in the handler; the
    /// live `MinerQuote` type cannot parse it, so it arrives unchecked.
    #[account(mut)]
    pub quote: UncheckedAccount<'info>,
}

pub fn handler(ctx: Context<CloseLegacyQuote>) -> Result<()> {
    let info = ctx.accounts.quote.to_account_info();
    let legacy = {
        let data = info.try_borrow_data()?;
        require!(
            info.owner == &crate::ID && data.len() >= 8,
            ErrorCode::InvalidAccountForMigration
        );
        require!(
            data[..8] == MinerQuote::DISCRIMINATOR[..],
            ErrorCode::InvalidAccountForMigration
        );
        MinerQuoteLegacy::deserialize(&mut &data[8..])?
    };

    require!(
        legacy.miner == ctx.accounts.miner.key(),
        ErrorCode::NotMiner
    );
    let (legacy_addr, _) = Pubkey::find_program_address(
        &[
            QUOTE_SEED,
            legacy.miner.as_ref(),
            legacy.from_chain.as_bytes(),
            legacy.to_chain.as_bytes(),
        ],
        &crate::ID,
    );
    require!(
        legacy_addr == info.key(),
        ErrorCode::InvalidAccountForMigration
    );

    // Manual close: zero the discriminator so the account can never be re-parsed, then drain it.
    {
        let mut data = info.try_borrow_mut_data()?;
        data[..8].fill(0);
    }
    let lamports = info.lamports();
    info.sub_lamports(lamports)?;
    ctx.accounts.miner.to_account_info().add_lamports(lamports)?;

    emit!(QuoteRemoved {
        miner: legacy.miner,
        from_chain: legacy.from_chain,
        to_chain: legacy.to_chain,
        // Pre-W2b quotes predate the declaration; they backed swaps out of the local vault by default.
        collateral_chain: crate::constants::BACKING_CHAIN_SOL.to_string(),
        remove_fee: 0,
    });
    msg!("legacy quote reaped: {}", info.key());
    Ok(())
}
