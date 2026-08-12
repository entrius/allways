//! Break-glass terminal for a `Swap` that survived the upgrade at the pre-v3 (v10) byte layout. v3
//! inserted `collateral_chain` mid-struct, so a v10-layout Swap no longer deserializes under the live
//! type — `timeout_swap`/`confirm_swap` both fail on it and its collateral would strand.
//!
//! The drain gate in `migrate_devnet.py` makes this unreachable in the normal flow; this exists only if
//! an upgrade races the drain. It walks the frozen v10 layout (implicitly SOL-backed) with bounded
//! reads — never Borsh's unbounded alloc, which a misaligned current-layout account would OOM on —
//! reaps the undecodable PDA, and emits the terms so the off-chain slash relay settles the SOL refund.

use anchor_lang::prelude::*;
use anchor_lang::system_program::System;
use anchor_lang::Discriminator;

use crate::constants::{CONFIG_SEED, MAX_ADDR_LEN, MAX_CHAIN_LEN, MAX_TX_LEN, SWAP_SEED};
use crate::error::ErrorCode;
use crate::events::LegacySwapClosed;
use crate::state::{Config, Swap};

#[derive(Accounts)]
#[instruction(swap_key: [u8; 32])]
pub struct CloseLegacySwap<'info> {
    /// Admin only — a one-shot recovery run by the migration operator, not a permissionless reap.
    #[account(mut)]
    pub admin: Signer<'info>,

    #[account(seeds = [CONFIG_SEED], bump = config.bump, has_one = admin)]
    pub config: Account<'info, Config>,

    /// CHECK: the decoded swap's `user`; also the rent-refund destination (the wronged party).
    #[account(mut)]
    pub user: UncheckedAccount<'info>,

    /// CHECK: the Swap PDA; owner + discriminator + the frozen v10 walk are verified in the handler.
    #[account(mut, seeds = [SWAP_SEED, swap_key.as_ref()], bump)]
    pub swap: UncheckedAccount<'info>,
}

/// A bounded reader over the raw account bytes — every read is length-checked, so a misaligned
/// (current-layout) account errors out instead of allocating a garbage-sized `String`.
struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self.pos.checked_add(n).ok_or(ErrorCode::InvalidAccountForMigration)?;
        require!(end <= self.data.len(), ErrorCode::InvalidAccountForMigration);
        let out = &self.data[self.pos..end];
        self.pos = end;
        Ok(out)
    }
    /// Read a Borsh `String` (u32 length prefix) whose length must not exceed `max`.
    fn string(&mut self, max: usize) -> Result<String> {
        let len = u32::from_le_bytes(self.take(4)?.try_into().unwrap()) as usize;
        require!(len <= max, ErrorCode::InvalidAccountForMigration);
        Ok(String::from_utf8_lossy(self.take(len)?).into_owned())
    }
    fn pubkey(&mut self) -> Result<Pubkey> {
        Ok(Pubkey::try_from(self.take(32)?).unwrap())
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
}

pub fn handler(ctx: Context<CloseLegacySwap>, swap_key: [u8; 32]) -> Result<()> {
    let info = ctx.accounts.swap.to_account_info();
    require!(info.owner == ctx.program_id, ErrorCode::InvalidAccountForMigration);

    let (user, miner, collateral_amount, from_tx_hash) = {
        let data = info.try_borrow_data()?;
        require!(
            data.len() >= 8 && data[..8] == Swap::DISCRIMINATOR[..],
            ErrorCode::InvalidAccountForMigration
        );
        let mut r = Reader { data: &data, pos: 8 };
        let user = r.pubkey()?;
        let miner = r.pubkey()?;
        r.string(MAX_CHAIN_LEN)?; // from_chain
        r.string(MAX_CHAIN_LEN)?; // to_chain
        r.string(MAX_ADDR_LEN)?; // user_from_addr
        r.string(MAX_ADDR_LEN)?; // user_to_addr
        r.string(MAX_ADDR_LEN)?; // miner_from_addr
        r.string(MAX_ADDR_LEN)?; // miner_to_addr
        r.take(16)?; // rate
        let collateral_amount = r.u64()?;
        r.take(32)?; // from_amount + to_amount (u128 each)
        let from_tx_hash = r.string(MAX_TX_LEN)?;
        r.take(4)?; // from_tx_block
        r.string(MAX_TX_LEN)?; // to_tx_hash
        r.take(4 + 1 + 8 + 8 + 8 + 8 + 1)?; // to_tx_block, status, initiated/timeout/max_extend/fulfilled, bump
        // A current-layout account (collateral_chain inserted) never ends exactly here — the mismatch
        // is what rejects it, so this only reaps a genuine v10 Swap.
        require!(r.pos == data.len(), ErrorCode::InvalidAccountForMigration);
        (user, miner, collateral_amount, from_tx_hash)
    };
    require!(user == ctx.accounts.user.key(), ErrorCode::UserMismatch);

    // Reap: rent → user (the wronged party). Same three steps Anchor's `close` takes, on an account
    // no typed accessor can safely hold.
    let refund_to = ctx.accounts.user.to_account_info();
    let lamports = info.lamports();
    info.sub_lamports(lamports)?;
    refund_to.add_lamports(lamports)?;
    info.assign(&System::id());
    info.resize(0)?;

    emit!(LegacySwapClosed { swap_key, miner, user, collateral_amount, from_tx_hash });
    Ok(())
}
