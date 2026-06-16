use anchor_lang::prelude::*;
use solana_keccak_hasher::hashv;

use crate::constants::{MAX_VALIDATORS, VOTE_ROUND_TTL_SECS};
use crate::error::ErrorCode;
use crate::state::{Config, VoteRound};

/// Canonical bound hash for a vote round, from (request_type, target pubkey).
/// Later phases extend the preimage with amounts/addresses the seeds don't cover.
pub fn request_hash(request_type: u8, target: &Pubkey) -> [u8; 32] {
    hashv(&[&[request_type], target.as_ref()]).to_bytes()
}

/// Bound hash for an initiate round — binds the user-side payout fields the seeds/reservation don't
/// cover (closes v2 #2 / #411: `user`, `user_from_addr`, `user_to_addr`, `from_tx_block`). The
/// miner quote + amounts come from the immutable reservation, so they need not be re-bound here.
#[allow(clippy::too_many_arguments)]
pub fn initiate_hash(
    miner: &Pubkey,
    user: &Pubkey,
    user_from_addr: &str,
    user_to_addr: &str,
    from_tx_hash: &str,
    from_tx_block: u32,
) -> [u8; 32] {
    hashv(&[
        &[crate::constants::REQ_INITIATE],
        miner.as_ref(),
        user.as_ref(),
        user_from_addr.as_bytes(),
        user_to_addr.as_bytes(),
        from_tx_hash.as_bytes(),
        &from_tx_block.to_le_bytes(),
    ])
    .to_bytes()
}

/// Bound hash for a swap-keyed round (confirm/timeout). All params live in the seeds (`swap_key`),
/// so this is a trivial binding like activate/deactivate.
pub fn swap_request_hash(request_type: u8, swap_key: &[u8; 32]) -> [u8; 32] {
    hashv(&[&[request_type], swap_key]).to_bytes()
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

    // An empty voter list means no open round (freshly allocated, or reset after a prior
    // round closed) — robust regardless of the clock's absolute value. A non-empty but
    // expired round is treated as stale and reopened.
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
