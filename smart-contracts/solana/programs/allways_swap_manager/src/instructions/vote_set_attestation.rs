use anchor_lang::prelude::*;

use crate::backing;
use crate::consensus::{attestation_hash, record_vote, reset_round};
use crate::constants::{ATTEST_SEED, CONFIG_SEED, MINER_SEED, REQ_SET_ATTESTATION, VOTE_SEED};
use crate::error::ErrorCode;
use crate::events::BondAttested;
use crate::state::{BondAttestation, Config, MinerState, VoteRound};

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

    /// Read to see whether this hub is holding a live obligation — a downward write is refused while it
    /// is (F5). Every attestable miner is registered, so this state always exists.
    #[account(
        seeds = [MINER_SEED, miner.key().as_ref()],
        bump = miner_state.bump,
        constraint = miner_state.miner == miner.key(),
    )]
    pub miner_state: Account<'info, MinerState>,

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

    // F5: while this hub is held (open pool, filled reservation, or in-flight swap), refuse a write
    // that LOWERS the bond — it could drop the purse under the 1.1× gate a filled reservation already
    // passed, stranding the deposit at initiate. Fee shrinkage settles when idle, so it isn't blocked.
    require!(
        effective_balance >= ctx.accounts.attestation.effective_balance
            || ctx.accounts.miner_state.busy_slot(bit) <= now,
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
