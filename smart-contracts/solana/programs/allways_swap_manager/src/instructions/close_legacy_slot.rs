//! Permissionless reap of the two reused per-miner slots the v3 upgrade left unreadable. `Pool` and
//! `Reservation` gained `collateral_chain` mid-struct but — unlike the quote PDAs — their SEEDS did
//! not change, so a pre-v3 miner's records sit where `open_or_request` looks and lock it out forever.
//!
//! Legacy-ness is proven by SIZE: `open_or_request` is the only writer of these addresses and always
//! allocates the live length, so a shorter allocation can only be a genuine leftover and a live slot
//! can never match. See the design doc's migration runbook for the full argument.

use anchor_lang::prelude::*;
use anchor_lang::system_program::System;
use anchor_lang::Discriminator;

use crate::constants::{MAX_CHAIN_LEN, POOL_SEED, RESV_SEED};
use crate::error::ErrorCode;
use crate::state::{Pool, Reservation};

/// Bytes v3 added to each of these accounts: the borsh encoding of one `#[max_len(MAX_CHAIN_LEN)]`
/// String (4-byte length prefix + the bytes). Derived, never a literal, so it tracks `MAX_CHAIN_LEN`.
const COLLATERAL_CHAIN_LEN: usize = 4 + MAX_CHAIN_LEN;

/// Byte offset of the leading `Pubkey` field (past the discriminator).
const FIRST_PUBKEY_OFFSET: usize = 8;

#[derive(Accounts)]
pub struct CloseLegacyPool<'info> {
    /// Anyone — the caller only pays the transaction.
    pub caller: Signer<'info>,

    /// CHECK: the PDA seed for the account below, and the rent refund destination. Deriving the slot
    /// address from this key is what proves the two belong together.
    #[account(mut)]
    pub miner: UncheckedAccount<'info>,

    /// CHECK: owner, discriminator, legacy length and stored miner are all verified in the handler;
    /// the live `Pool` type cannot parse a v10 record, so it arrives unchecked.
    #[account(mut, seeds = [POOL_SEED, miner.key().as_ref()], bump)]
    pub pool: UncheckedAccount<'info>,
}

#[derive(Accounts)]
pub struct CloseLegacyReservation<'info> {
    pub caller: Signer<'info>,

    /// CHECK: as above — seed and rent destination.
    #[account(mut)]
    pub miner: UncheckedAccount<'info>,

    /// CHECK: owner, discriminator and legacy length are verified in the handler. Unlike `Pool` this
    /// struct stores no miner field (its leading Pubkey is the `router`), so the PDA derivation above
    /// is the whole ownership proof — which is exactly what it is for the live account too.
    #[account(mut, seeds = [RESV_SEED, miner.key().as_ref()], bump)]
    pub reservation: UncheckedAccount<'info>,
}

pub fn close_pool(ctx: Context<CloseLegacyPool>) -> Result<()> {
    let info = ctx.accounts.pool.to_account_info();
    let miner = ctx.accounts.miner.to_account_info();
    require_legacy(
        &info,
        ctx.program_id,
        Pool::DISCRIMINATOR,
        8 + Pool::INIT_SPACE,
    )?;
    // Pool carries the miner as its first field, so the seed proof can be checked twice over. A
    // mismatch means the layout mirror has drifted, not that the caller named the wrong miner.
    {
        let data = info.try_borrow_data()?;
        require!(
            data[FIRST_PUBKEY_OFFSET..FIRST_PUBKEY_OFFSET + 32] == miner.key().to_bytes()[..],
            ErrorCode::InvalidAccountForMigration
        );
    }
    reap(&info, &miner)?;
    msg!("legacy pool reaped: {}", info.key());
    Ok(())
}

pub fn close_reservation(ctx: Context<CloseLegacyReservation>) -> Result<()> {
    let info = ctx.accounts.reservation.to_account_info();
    let miner = ctx.accounts.miner.to_account_info();
    require_legacy(
        &info,
        ctx.program_id,
        Reservation::DISCRIMINATOR,
        8 + Reservation::INIT_SPACE,
    )?;
    reap(&info, &miner)?;
    msg!("legacy reservation reaped: {}", info.key());
    Ok(())
}

/// The whole safety argument. `live_len` is what this program allocates today; a pre-v3 record is
/// exactly `COLLATERAL_CHAIN_LEN` shorter. Requiring that length refuses a live account structurally,
/// not by policy — no argument a caller can pass makes a v3 slot reapable.
fn require_legacy(
    info: &AccountInfo,
    program_id: &Pubkey,
    discriminator: &[u8],
    live_len: usize,
) -> Result<()> {
    // Checked against the address this program is ACTUALLY running at, not the compiled-in
    // `crate::ID`: the two differ on any non-canonical deployment, and an ownership proof that only
    // holds for one address isn't a proof.
    require!(info.owner == program_id, ErrorCode::InvalidAccountForMigration);
    let data = info.try_borrow_data()?;
    require!(data.len() >= 8, ErrorCode::InvalidAccountForMigration);
    require!(data[..8] == *discriminator, ErrorCode::InvalidAccountForMigration);
    require!(
        data.len() == live_len - COLLATERAL_CHAIN_LEN,
        ErrorCode::InvalidAccountForMigration
    );
    Ok(())
}

/// Close for real: rent to the miner, then hand the address back to the system program with no data,
/// so the next `open_or_request` re-creates the slot fresh under the current layout. Same three steps
/// Anchor's own `close` takes, on an account no typed accessor can hold.
fn reap(info: &AccountInfo, miner: &AccountInfo) -> Result<()> {
    let lamports = info.lamports();
    info.sub_lamports(lamports)?;
    miner.add_lamports(lamports)?;
    info.assign(&System::id());
    info.resize(0)?;
    Ok(())
}
