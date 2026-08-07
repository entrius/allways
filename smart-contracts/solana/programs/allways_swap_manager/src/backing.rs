//! Backing (collateral-chain) lookups — the D4 layer. Every collateral decision keys off the swap's
//! `collateral_chain` and nothing else: no function here may branch on the PAIR. A new hub is a new
//! chain id plus a match arm, which is what makes "TAO↔BTC ships with no program diff" true.

use anchor_lang::prelude::*;

use crate::constants::{
    BACKING_BIT_SOL, BACKING_BIT_TAO, BACKING_CHAIN_SOL, BACKING_CHAIN_TAO,
};
use crate::error::ErrorCode;
use crate::state::{BondAttestation, Config, MinerState};

/// The leg denominated in `collateral_chain` — the amount the collateral is sized against. Validity is
/// "backing ∈ legs": a swap whose legs don't include the backing asset has nothing to size against.
pub fn collateral_leg_amount(
    collateral_chain: &str,
    from_chain: &str,
    from_amount: u128,
    to_chain: &str,
    to_amount: u128,
) -> Result<u128> {
    if collateral_chain == from_chain {
        Ok(from_amount)
    } else if collateral_chain == to_chain {
        Ok(to_amount)
    } else {
        err!(ErrorCode::BackingNotInLegs)
    }
}

/// Swap-size bounds for a backing, in that asset's own smallest unit (0 max = unbounded).
pub fn swap_bounds(config: &Config, collateral_chain: &str) -> Result<(u64, u64)> {
    match collateral_chain {
        BACKING_CHAIN_SOL => Ok((config.min_swap_amount, config.max_swap_amount)),
        BACKING_CHAIN_TAO => Ok((config.tao_min_swap_amount, config.tao_max_swap_amount)),
        _ => err!(ErrorCode::BackingNotSupported),
    }
}

/// The activation bit for a backing. Doubles as the "is this a known backing" check — an unknown chain
/// has no bit, no purse and no attestation.
pub fn backing_bit(collateral_chain: &str) -> Result<u8> {
    match collateral_chain {
        BACKING_CHAIN_SOL => Ok(BACKING_BIT_SOL),
        BACKING_CHAIN_TAO => Ok(BACKING_BIT_TAO),
        _ => err!(ErrorCode::BackingNotSupported),
    }
}

/// The purse a miner must hold to activate this backing, in that asset's own smallest unit — lamports
/// for "sol", rao for "tao". Never converted through a rate; each hub carries its own floor.
pub fn activation_floor(config: &Config, collateral_chain: &str) -> Result<u64> {
    match collateral_chain {
        BACKING_CHAIN_SOL => Ok(config.min_collateral),
        BACKING_CHAIN_TAO => Ok(config.tao_min_collateral),
        _ => err!(ErrorCode::BackingNotSupported),
    }
}

/// The purse backing `collateral_chain`, in that asset's smallest unit. This one function is the whole
/// switch: "sol" reads the local vault ledger, every other backing reads its quorum-written attestation
/// (the EFFECTIVE bond, already net of fees and voted slashes — which is why the 1.1× guards above it
/// need no extra headroom). `attestation` is the PDA for exactly this (miner, chain), or None.
pub fn backing_purse(
    collateral_chain: &str,
    miner_state: &MinerState,
    attestation: Option<&BondAttestation>,
) -> Result<u64> {
    match collateral_chain {
        BACKING_CHAIN_SOL => Ok(miner_state.collateral),
        BACKING_CHAIN_TAO => attested_purse(attestation),
        _ => err!(ErrorCode::BackingNotSupported),
    }
}

/// The attested effective bond, refusing anything an unlocked or missing attestation would let through.
fn attested_purse(attestation: Option<&BondAttestation>) -> Result<u64> {
    let a = attestation.ok_or(error!(ErrorCode::AttestationMissing))?;
    require!(a.locked, ErrorCode::BondNotLocked);
    Ok(a.effective_balance)
}

/// Whether a penalty against this backing settles here, atomically. False = Solana reaches the verdict
/// only; the seizure is a separate quorum on the backing chain (busy-until-settled applies).
pub fn settles_locally(collateral_chain: &str) -> bool {
    collateral_chain == BACKING_CHAIN_SOL
}

/// The dead-man fuse: refuse non-local backings once the global attestation heartbeat goes stale.
/// A never-set heartbeat (0) is stale by construction, so a fresh deployment fuses closed until the
/// relayers prove they are alive.
pub fn require_fresh_heartbeat(config: &Config, now: i64) -> Result<()> {
    require!(
        now.saturating_sub(config.last_attest_heartbeat) <= config.attest_max_age_secs,
        ErrorCode::AttestationStale
    );
    Ok(())
}

/// Entry gates for a swap backed off-chain: the fuse above, plus busy-until-settled. A locally-settled
/// backing passes both by construction — its penalty already moved inside `timeout_swap`.
pub fn check_entry_gates(
    config: &Config,
    miner_state: &MinerState,
    collateral_chain: &str,
    now: i64,
) -> Result<()> {
    // Reject an unknown chain here rather than letting the fuse answer for it — same error at every
    // gate, whether or not the heartbeat happens to be fresh.
    backing_bit(collateral_chain)?;
    if settles_locally(collateral_chain) {
        return Ok(());
    }
    require_fresh_heartbeat(config, now)?;
    require!(now >= miner_state.settling_until, ErrorCode::MinerSettling);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn miner_with(collateral: u64) -> MinerState {
        MinerState {
            miner: Pubkey::default(),
            collateral,
            active: true,
            active_backings: BACKING_BIT_SOL,
            has_active_swap: false,
            busy_until: 0,
            settling_until: 0,
            deactivation_at: 0,
            successful_swaps: 0,
            failed_swaps: 0,
            bump: 255,
        }
    }

    fn attestation(effective_balance: u64, locked: bool) -> BondAttestation {
        BondAttestation {
            miner: Pubkey::default(),
            chain: BACKING_CHAIN_TAO.to_string(),
            effective_balance,
            locked,
            epoch: 1,
            attested_at: 0,
            bump: 255,
        }
    }

    #[test]
    fn leg_lookup_finds_the_backing_leg_on_either_side() {
        // btc→sol with SOL backing: the dest leg. sol→btc: the source leg. Same rule, no pair branch.
        assert_eq!(collateral_leg_amount("sol", "btc", 7, "sol", 11).unwrap(), 11);
        assert_eq!(collateral_leg_amount("sol", "sol", 7, "btc", 11).unwrap(), 7);
        // A TAO-backed leg is found by exactly the same lookup — the reason a TAO hub costs no diff.
        assert_eq!(collateral_leg_amount("tao", "tao", 7, "btc", 11).unwrap(), 7);
        assert_eq!(collateral_leg_amount("tao", "btc", 7, "tao", 11).unwrap(), 11);
    }

    #[test]
    fn leg_lookup_refuses_a_backing_that_is_not_a_leg() {
        // btc→eth backed by SOL has no SOL amount to size collateral against.
        assert!(collateral_leg_amount("sol", "btc", 7, "eth", 11).is_err());
    }

    #[test]
    fn sol_reads_local_collateral_and_tao_reads_the_attestation() {
        // "sol" ignores any attestation handed to it; "tao" ignores the local vault entirely.
        let a = attestation(42, true);
        assert_eq!(backing_purse("sol", &miner_with(7), Some(&a)).unwrap(), 7);
        assert_eq!(backing_purse("tao", &miner_with(7), Some(&a)).unwrap(), 42);
        assert!(backing_purse("btc", &miner_with(7), Some(&a)).is_err());
    }

    #[test]
    fn a_tao_purse_needs_an_attestation_that_is_locked() {
        assert!(backing_purse("tao", &miner_with(9_000), None).is_err());
        assert!(backing_purse("tao", &miner_with(9_000), Some(&attestation(42, false))).is_err());
    }

    #[test]
    fn only_sol_settles_locally() {
        assert!(settles_locally("sol"));
        assert!(!settles_locally("tao"));
    }

    #[test]
    fn every_known_backing_has_a_distinct_bit() {
        assert_eq!(backing_bit("sol").unwrap(), BACKING_BIT_SOL);
        assert_eq!(backing_bit("tao").unwrap(), BACKING_BIT_TAO);
        assert_ne!(BACKING_BIT_SOL, BACKING_BIT_TAO);
        assert!(backing_bit("btc").is_err());
    }
}
