use anchor_lang::prelude::*;
use solana_keccak_hasher::hashv;

use crate::constants::{MAX_VALIDATORS, REQ_SET_WEIGHTS, VOTE_ROUND_TTL_SECS};
use crate::error::ErrorCode;
use crate::state::{Config, ValidatorInfo, VoteRound};

/// Canonical bound hash for a vote round, from (request_type, target pubkey).
pub fn request_hash(request_type: u8, target: &Pubkey) -> [u8; 32] {
    hashv(&[&[request_type], target.as_ref()]).to_bytes()
}

/// Bound hash for a swap-keyed round (initiate-attest/confirm/timeout). All params live in the seeds
/// (`swap_key`), so the binding is trivial.
pub fn swap_request_hash(request_type: u8, swap_key: &[u8; 32]) -> [u8; 32] {
    hashv(&[&[request_type], swap_key]).to_bytes()
}

/// Bound hash for the validator-weight round: binds the full snapshot —
/// `REQ_SET_WEIGHTS || (each validator key in config order) || (each weight LE)`. Binding the keys
/// means a validator-set change between voters invalidates the round (hash mismatch), so a stale
/// vector can't be applied to a changed set. `validators` and `weights` are index-aligned.
pub fn weights_hash(validators: &[ValidatorInfo], weights: &[u64]) -> [u8; 32] {
    let mut parts: Vec<Vec<u8>> = Vec::with_capacity(1 + validators.len() + weights.len());
    parts.push(vec![REQ_SET_WEIGHTS]);
    for v in validators {
        parts.push(v.key.as_ref().to_vec());
    }
    for w in weights {
        parts.push(w.to_le_bytes().to_vec());
    }
    let refs: Vec<&[u8]> = parts.iter().map(|p| p.as_slice()).collect();
    hashv(&refs).to_bytes()
}

/// Assert `validator` is whitelisted — for consensus-free validator actions (deadline extensions)
/// that gate on membership but open no vote round.
pub fn ensure_validator(config: &Config, validator: &Pubkey) -> Result<()> {
    require!(
        config.validators.iter().any(|v| &v.key == validator),
        ErrorCode::NotValidator
    );
    Ok(())
}

/// Record a validator's vote into `round`; returns true iff quorum is now reached.
/// (Re)initializes a fresh or stale round, binds params via `bound_hash`, and dedupes voters.
pub fn record_vote<'info>(
    round: &mut Account<'info, VoteRound>,
    config: &Config,
    validator: Pubkey,
    bound_hash: [u8; 32],
    round_bump: u8,
    now: i64,
) -> Result<bool> {
    require!(!config.validators.is_empty(), ErrorCode::NoValidators);
    require!(
        config.validators.iter().any(|v| v.key == validator),
        ErrorCode::NotValidator
    );

    // Empty voter list means no open round (fresh or reset) — robust regardless of the clock's
    // absolute value. A non-empty but expired round is stale and reopened.
    let stale =
        !round.voters.is_empty() && now.saturating_sub(round.created_at) > VOTE_ROUND_TTL_SECS;
    if round.voters.is_empty() || stale {
        round.bound_hash = bound_hash;
        round.created_at = now;
        round.bump = round_bump;
        round.voters.clear();
    } else {
        require!(round.bound_hash == bound_hash, ErrorCode::VoteHashMismatch);
    }

    require!(!round.voters.contains(&validator), ErrorCode::AlreadyVoted);
    require!(round.voters.len() < MAX_VALIDATORS, ErrorCode::ValidatorSetFull);
    round.voters.push(validator);

    let votes = round.voters.len() as u64;
    let total = config.validators.len() as u64;
    let threshold = config.consensus_threshold_percent as u64;
    Ok(votes.saturating_mul(100) >= threshold.saturating_mul(total))
}

/// Reset a round to empty so its PDA is reusable for the next round (rent stays parked).
pub fn reset_round(round: &mut Account<VoteRound>) {
    round.bound_hash = [0u8; 32];
    round.voters.clear();
    round.created_at = 0;
}
