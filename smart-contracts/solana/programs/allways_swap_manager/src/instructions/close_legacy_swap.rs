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

use crate::constants::{
    BACKING_BIT_SOL, CONFIG_SEED, MAX_ADDR_LEN, MAX_CHAIN_LEN, MAX_TX_LEN, MINER_SEED, SWAP_SEED,
};
use crate::error::ErrorCode;
use crate::events::LegacySwapClosed;
use crate::state::{Config, MinerState, Swap, SwapStatus};

/// `Swap` as the v10 program wrote it: no `collateral_chain` (v3 inserted it mid-struct). Frozen —
/// its `INIT_SPACE` fixes the exact byte length a genuine v10 Swap PDA was allocated at (`init` pads
/// every `#[max_len]` String to its maximum), so a current-layout account — longer by exactly
/// `collateral_chain` — is rejected on length alone. Field order is the contract; do not reorder.
/// Only `INIT_SPACE` is read — the fields exist to fix the layout, hence `dead_code` is expected.
#[derive(InitSpace)]
#[allow(dead_code)]
struct SwapV10 {
    user: Pubkey,
    miner: Pubkey,
    #[max_len(MAX_CHAIN_LEN)]
    from_chain: String,
    #[max_len(MAX_CHAIN_LEN)]
    to_chain: String,
    #[max_len(MAX_ADDR_LEN)]
    user_from_addr: String,
    #[max_len(MAX_ADDR_LEN)]
    user_to_addr: String,
    #[max_len(MAX_ADDR_LEN)]
    miner_from_addr: String,
    #[max_len(MAX_ADDR_LEN)]
    miner_to_addr: String,
    rate: u128,
    collateral_amount: u64,
    from_amount: u128,
    to_amount: u128,
    #[max_len(MAX_TX_LEN)]
    from_tx_hash: String,
    from_tx_block: u32,
    #[max_len(MAX_TX_LEN)]
    to_tx_hash: String,
    to_tx_block: u32,
    status: SwapStatus,
    initiated_at: i64,
    timeout_at: i64,
    max_extend_at: i64,
    fulfilled_at: i64,
    bump: u8,
}

/// The exact on-chain byte length of a v10 Swap PDA: 8-byte discriminator + the padded struct.
const V10_SWAP_LEN: usize = 8 + SwapV10::INIT_SPACE;

#[derive(Accounts)]
#[instruction(swap_key: [u8; 32], miner: Pubkey)]
pub struct CloseLegacySwap<'info> {
    /// Admin only — a one-shot recovery run by the migration operator, not a permissionless reap.
    #[account(mut)]
    pub admin: Signer<'info>,

    #[account(seeds = [CONFIG_SEED], bump = config.bump, has_one = admin)]
    pub config: Account<'info, Config>,

    /// CHECK: the decoded swap's `user`; also the rent-refund destination (the wronged party).
    #[account(mut)]
    pub user: UncheckedAccount<'info>,

    /// The miner whose hub the reaped swap froze. Bound by the `miner` arg and re-checked against the
    /// decoded `swap.miner` in the handler, so the caller can't free a different miner's state. Freeing
    /// the SOL hub here is what actually unblocks the miner — reaping the PDA alone leaves it bricked.
    #[account(mut, seeds = [MINER_SEED, miner.as_ref()], bump = miner_state.bump)]
    pub miner_state: Account<'info, MinerState>,

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

pub fn handler(ctx: Context<CloseLegacySwap>, swap_key: [u8; 32], miner: Pubkey) -> Result<()> {
    let info = ctx.accounts.swap.to_account_info();
    require!(info.owner == ctx.program_id, ErrorCode::InvalidAccountForMigration);

    let (user, decoded_miner, collateral_amount, from_tx_hash) = {
        let data = info.try_borrow_data()?;
        // Discriminate by the fixed allocation length, NOT by where the walk ends. `init` zero-pads
        // every String to its `#[max_len]`, so a genuine v10 Swap's walked fields stop well short of
        // `data.len()`; keying on `r.pos == data.len()` would reject every real account. A current
        // (collateral_chain) Swap is longer by that field, so length alone rejects it here.
        require!(
            data.len() == V10_SWAP_LEN && data[..8] == Swap::DISCRIMINATOR[..],
            ErrorCode::InvalidAccountForMigration
        );
        let mut r = Reader { data: &data, pos: 8 };
        let user = r.pubkey()?;
        let decoded_miner = r.pubkey()?;
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
        // Everything past the walked fields is the fixed allocation's zero padding. Requiring it be
        // zero rejects a length-matched but corrupt account (a real v10 Swap has a clean padded tail).
        require!(data[r.pos..].iter().all(|&b| b == 0), ErrorCode::InvalidAccountForMigration);
        (user, decoded_miner, collateral_amount, from_tx_hash)
    };
    require!(user == ctx.accounts.user.key(), ErrorCode::UserMismatch);
    // The `miner_state` account is seeded on the `miner` arg; pin it to the decoded swap so the caller
    // can't reap swap A while freeing miner B's hub.
    require!(decoded_miner == miner, ErrorCode::InvalidAccountForMigration);

    // Free the miner's SOL hub (v10 swaps are implicitly SOL-backed): clear the in-flight bit, drop the
    // per-hub locks, and release the reservation this swap held. Without this the PDA is reaped but the
    // hub stays busy forever — `open_or_request`/`deactivate`/`withdraw_collateral` all gate on the bit.
    let ms = &mut ctx.accounts.miner_state;
    ms.set_swap(BACKING_BIT_SOL, false);
    ms.set_busy(BACKING_BIT_SOL, 0);
    ms.set_settling(BACKING_BIT_SOL, 0);
    let owed = ms.reserved(BACKING_BIT_SOL);
    ms.release_reserved(BACKING_BIT_SOL, owed);
    ms.failed_swaps = ms.failed_swaps.saturating_add(1);

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
