//! Permissionless reap of the pre-v3.1 per-miner slots orphaned by the per-hub re-seed. `Pool`,
//! `Reservation` and the initiate `VoteRound` moved to backing-qualified (or swap-keyed) seeds in
//! v14, so anything still at the old `[seed, miner]` address is unreachable by the live program.
//!
//! That makes address derivation itself the whole legacy proof: `open_or_request`/`vote_initiate`
//! can never again write these addresses, so owner + discriminator there can only be a genuine
//! leftover — v10-shaped or v13-shaped alike, which is why no size check remains (it would only
//! split one dead address space into "reapable" and "stuck").

use anchor_lang::prelude::*;
use anchor_lang::system_program::System;
use anchor_lang::Discriminator;

use crate::constants::{POOL_SEED, REQ_INITIATE, RESV_SEED, VOTE_SEED};
use crate::error::ErrorCode;
use crate::state::{Pool, Reservation, VoteRound};

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

    /// CHECK: at the RETIRED `[POOL_SEED, miner]` address (live pools carry the backing in their
    /// seeds), so the derivation is the legacy proof; owner + discriminator verified in the handler.
    #[account(mut, seeds = [POOL_SEED, miner.key().as_ref()], bump)]
    pub pool: UncheckedAccount<'info>,
}

#[derive(Accounts)]
pub struct CloseLegacyReservation<'info> {
    pub caller: Signer<'info>,

    /// CHECK: as above — seed and rent destination.
    #[account(mut)]
    pub miner: UncheckedAccount<'info>,

    /// CHECK: at the RETIRED `[RESV_SEED, miner]` address — same argument as the pool above.
    #[account(mut, seeds = [RESV_SEED, miner.key().as_ref()], bump)]
    pub reservation: UncheckedAccount<'info>,
}

#[derive(Accounts)]
pub struct CloseLegacyInitiateRound<'info> {
    /// Anyone — and also the rent destination: the round was validator-funded, not miner-funded,
    /// so unlike the slots above its rent does not belong to the miner.
    #[account(mut)]
    pub caller: Signer<'info>,

    /// CHECK: the PDA seed for the retired round address below.
    pub miner: UncheckedAccount<'info>,

    /// CHECK: at the RETIRED `[VOTE_SEED, [REQ_INITIATE], miner]` address (live initiate rounds key
    /// by swap_key); owner + discriminator verified in the handler.
    #[account(mut, seeds = [VOTE_SEED, &[REQ_INITIATE], miner.key().as_ref()], bump)]
    pub vote_round: UncheckedAccount<'info>,
}

pub fn close_pool(ctx: Context<CloseLegacyPool>) -> Result<()> {
    let info = ctx.accounts.pool.to_account_info();
    let miner = ctx.accounts.miner.to_account_info();
    require_legacy(&info, ctx.program_id, Pool::DISCRIMINATOR)?;
    // Pool carries the miner as its first field in every historical layout, so the seed proof can be
    // checked twice over. A mismatch means a layout drift, not a caller error.
    {
        let data = info.try_borrow_data()?;
        require!(
            data.len() >= FIRST_PUBKEY_OFFSET + 32
                && data[FIRST_PUBKEY_OFFSET..FIRST_PUBKEY_OFFSET + 32] == miner.key().to_bytes()[..],
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
    require_legacy(&info, ctx.program_id, Reservation::DISCRIMINATOR)?;
    reap(&info, &miner)?;
    msg!("legacy reservation reaped: {}", info.key());
    Ok(())
}

pub fn close_initiate_round(ctx: Context<CloseLegacyInitiateRound>) -> Result<()> {
    let info = ctx.accounts.vote_round.to_account_info();
    let caller = ctx.accounts.caller.to_account_info();
    require_legacy(&info, ctx.program_id, VoteRound::DISCRIMINATOR)?;
    reap(&info, &caller)?;
    msg!("legacy initiate round reaped: {}", info.key());
    Ok(())
}

/// The safety argument: the seeds constraint above already pinned the account to a RETIRED address
/// the live program can never write, so owner + discriminator is all that separates a leftover from
/// an empty (never-created) account.
fn require_legacy(info: &AccountInfo, program_id: &Pubkey, discriminator: &[u8]) -> Result<()> {
    // Checked against the address this program is ACTUALLY running at, not the compiled-in
    // `crate::ID`: the two differ on any non-canonical deployment, and an ownership proof that only
    // holds for one address isn't a proof.
    require!(info.owner == program_id, ErrorCode::InvalidAccountForMigration);
    let data = info.try_borrow_data()?;
    require!(data.len() >= 8, ErrorCode::InvalidAccountForMigration);
    require!(data[..8] == *discriminator, ErrorCode::InvalidAccountForMigration);
    Ok(())
}

/// Close for real: rent to the refund destination, then hand the address back to the system program
/// with no data. Same three steps Anchor's own `close` takes, on an account no typed accessor can
/// safely hold (historical layouts differ).
fn reap(info: &AccountInfo, refund_to: &AccountInfo) -> Result<()> {
    let lamports = info.lamports();
    info.sub_lamports(lamports)?;
    refund_to.add_lamports(lamports)?;
    info.assign(&System::id());
    info.resize(0)?;
    Ok(())
}
