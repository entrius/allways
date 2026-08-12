use anchor_lang::prelude::*;

use crate::backing;
use crate::consensus::{attestation_hash, record_vote, reset_round};
use crate::constants::{
    required_collateral, ATTEST_SEED, CONFIG_SEED, MINER_SEED, REQ_SET_ATTESTATION, RESV_SEED, VOTE_SEED,
};
use crate::error::ErrorCode;
use crate::events::BondAttested;
use crate::state::{BondAttestation, Config, MinerState, Reservation, VoteRound};

/// Validators write a miner's effective bond on one backing chain. Solana can't read the vault holding
/// it, so the quorum's assertion IS the value every collateral guard reads.
///
/// Deliberately NOT halt-gated: relay instructions are exit-path, and a halt that froze attestations
/// would freeze the settlement of every in-flight TAO-backed swap along with it.
#[derive(Accounts)]
#[instruction(chain: String, effective_balance: u64, locked: bool, epoch: u64)]
pub struct VoteSetAttestation<'info> {
    #[account(mut)]
    pub validator: Signer<'info>,

    #[account(seeds = [CONFIG_SEED], bump = config.bump)]
    pub config: Account<'info, Config>,

    /// CHECK: identified by address only; bound via the attestation/round PDA seeds.
    pub miner: UncheckedAccount<'info>,

    /// Read for this hub's in-flight-swap reserve — a downward write is refused only if it would drop
    /// the purse below a LIVE obligation (F5). Every attestable miner is registered, so this always exists.
    #[account(
        seeds = [MINER_SEED, miner.key().as_ref()],
        bump = miner_state.bump,
        constraint = miner_state.miner == miner.key(),
    )]
    pub miner_state: Account<'info, MinerState>,

    /// CHECK: the miner's reservation PDA for THIS hub, read-only. A FILLED reservation's 1.10× is an
    /// obligation the bond must still cover (`vote_initiate` re-checks it), so it floors a downward write
    /// — but a mere open pool carries NO obligation and must not, else it can be spammed to keep a
    /// just-slashed bond advertised stale-high (F5 #4). May be uninitialized; handled in the handler.
    #[account(seeds = [RESV_SEED, miner.key().as_ref(), chain.as_bytes()], bump)]
    pub reservation: UncheckedAccount<'info>,

    /// One-time rent per (miner, hub), paid by whichever validator's vote reaches quorum first.
    #[account(
        init_if_needed,
        payer = validator,
        space = 8 + BondAttestation::INIT_SPACE,
        seeds = [ATTEST_SEED, miner.key().as_ref(), chain.as_bytes()],
        bump,
    )]
    pub attestation: Account<'info, BondAttestation>,

    /// Reusable per-(miner, chain) round — one bond, one open proposal at a time.
    #[account(
        init_if_needed,
        payer = validator,
        space = 8 + VoteRound::INIT_SPACE,
        seeds = [VOTE_SEED, &[REQ_SET_ATTESTATION], miner.key().as_ref(), chain.as_bytes()],
        bump,
    )]
    pub vote_round: Account<'info, VoteRound>,

    pub system_program: Program<'info, System>,
}

pub fn handler(
    ctx: Context<VoteSetAttestation>,
    chain: String,
    effective_balance: u64,
    locked: bool,
    epoch: u64,
) -> Result<()> {
    // A local purse needs no attestation — writing one would create a second, unread source of truth.
    require!(
        !backing::settles_locally(&chain),
        ErrorCode::BackingSettlesLocally
    );
    let bit = backing::backing_bit(&chain)?;

    // Monotonic epochs: a stale round must never restore a lock state the vault has moved past. Equal
    // epochs are the normal case — balance moves (fees, slashes, posts) don't bump the vault's epoch.
    // Checked per vote, so the last voter (the one that applies the write) has checked it too.
    require!(
        epoch >= ctx.accounts.attestation.epoch,
        ErrorCode::AttestationEpochStale
    );

    let now = Clock::get()?.unix_timestamp;

    // F5: refuse a write that LOWERS the bond only when it would drop the purse below what LIVE
    // obligations already reserved against it — an in-flight swap's reserve, or a filled reservation's
    // 1.10× (which `vote_initiate` re-checks, so a low bond would strand the deposit). An OPEN POOL
    // carries no obligation (amount unknown, nothing reserved), so it does NOT block the write: gating
    // on "hub busy" instead let a spammed open re-arm busy each tick and hold the bond stale-high (F5 #4).
    // A fee-shrink write that still covers every obligation lands regardless of hub state.
    let filled_resv_obligation = {
        let info = ctx.accounts.reservation.to_account_info();
        if info.owner == ctx.program_id && !info.data_is_empty() {
            let resv = Reservation::try_deserialize(&mut &info.try_borrow_data()?[..])?;
            // A drawn-and-filled, still-live reservation (not the unfilled/consumed/expired states).
            if resv.created_at != 0 && resv.reserved_until >= now {
                required_collateral(resv.collateral_amount)
            } else {
                0
            }
        } else {
            0
        }
    };
    let live_obligation = ctx.accounts.miner_state.reserved(bit).saturating_add(filled_resv_obligation);
    require!(
        effective_balance >= ctx.accounts.attestation.effective_balance || effective_balance >= live_obligation,
        ErrorCode::AttestationWouldStrandSwap
    );
    let miner_key = ctx.accounts.miner.key();
    let bound = attestation_hash(&miner_key, &chain, effective_balance, locked, epoch);
    let validator = ctx.accounts.validator.key();
    let round_bump = ctx.bumps.vote_round;

    let quorum = record_vote(
        &mut ctx.accounts.vote_round,
        &ctx.accounts.config,
        validator,
        bound,
        round_bump,
        now,
    )?;

    if quorum {
        let attestation = &mut ctx.accounts.attestation;
        attestation.miner = miner_key;
        attestation.chain = chain.clone();
        attestation.effective_balance = effective_balance;
        attestation.locked = locked;
        attestation.epoch = epoch;
        // From the chain's clock at quorum, never a validator-supplied value.
        attestation.attested_at = now;
        attestation.bump = ctx.bumps.attestation;
        reset_round(&mut ctx.accounts.vote_round);
        emit!(BondAttested {
            miner: miner_key,
            chain,
            effective_balance,
            locked,
            epoch,
            attested_at: now,
        });
    }
    Ok(())
}
