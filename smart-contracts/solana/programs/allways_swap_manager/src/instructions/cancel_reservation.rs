use anchor_lang::prelude::*;

use crate::constants::{CONFIG_SEED, RESV_SEED};
use crate::state::{Config, Reservation};

/// Admin clears a miner's active reservation (Phase 9: reservations come from the lottery; there is no
/// reserve vote round to reset anymore — pools are reset via `cancel_pool`).
#[derive(Accounts)]
pub struct CancelReservation<'info> {
    pub admin: Signer<'info>,

    #[account(seeds = [CONFIG_SEED], bump = config.bump, has_one = admin)]
    pub config: Account<'info, Config>,

    /// CHECK: identified by address only; used in PDA seeds.
    pub miner: UncheckedAccount<'info>,

    // Canonical bump (not stored) so this works whether the reservation was populated yet or not.
    #[account(mut, seeds = [RESV_SEED, miner.key().as_ref()], bump)]
    pub reservation: Account<'info, Reservation>,
}

pub fn handler(ctx: Context<CancelReservation>) -> Result<()> {
    let r = &mut ctx.accounts.reservation;
    r.bound_hash = [0u8; 32];
    r.from_addr = String::new();
    r.from_chain = String::new();
    r.to_chain = String::new();
    r.sol_amount = 0;
    r.from_amount = 0;
    r.to_amount = 0;
    r.miner_from_addr = String::new();
    r.miner_to_addr = String::new();
    r.rate = String::new();
    r.reserved_until = 0;

    msg!("reservation cancelled: {}", ctx.accounts.miner.key());
    Ok(())
}
