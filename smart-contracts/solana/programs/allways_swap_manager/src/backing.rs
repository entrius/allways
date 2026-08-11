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

/// The user's own address on the backing chain — whom a penalty settled there is owed to. Same leg
/// lookup as `collateral_leg_amount`, but total: a timeout is terminal, so a backing that is somehow
/// not a leg (finalize already rejects that) yields no payee rather than an error that wedges it.
pub fn collateral_leg_user_addr(
    collateral_chain: &str,
    from_chain: &str,
    user_from_addr: &str,
    to_chain: &str,
    user_to_addr: &str,
) -> String {
    if collateral_chain == from_chain {
        user_from_addr.to_string()
    } else if collateral_chain == to_chain {
        user_to_addr.to_string()
    } else {
        String::new()
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

/// Whether a quote may declare this backing for this pair, returning the backing's bit. Validity is
/// "backing is a known hub AND backing ∈ legs" — the same two facts everywhere, which is what forces
/// TAO-backing on a one-hub pair (tao↔btc) and leaves the per-quote choice on a hub↔hub pair (sol↔tao)
/// without any function here knowing that either pair exists.
pub fn declarable_bit(collateral_chain: &str, from_chain: &str, to_chain: &str) -> Result<u8> {
    let bit = backing_bit(collateral_chain)?;
    require!(
        collateral_chain == from_chain || collateral_chain == to_chain,
        ErrorCode::BackingNotInLegs
    );
    Ok(bit)
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

/// Entry gates for a new swap: busy-until-settled for EVERY backing, plus the dead-man fuse for the
/// ones that settle elsewhere.
pub fn check_entry_gates(
    config: &Config,
    miner_state: &MinerState,
    collateral_chain: &str,
    now: i64,
) -> Result<()> {
    // Reject an unknown chain here rather than letting the gates answer for it — same error at every
    // door, whether or not the heartbeat happens to be fresh.
    backing_bit(collateral_chain)?;
    // A penalty still settling on a backing chain freezes the WHOLE miner, not just that purse: a
    // pending seizure means the miner owes money nobody has taken yet. `settling_until` is 0 for a
    // SOL-only miner and for any locally-settled timeout (those seize atomically), so the SOL path
    // only ever meets this gate when a TAO slash really is outstanding.
    require!(now >= miner_state.settling_until, ErrorCode::MinerSettling);
    // The fuse stays non-local: it is an attestation-freshness question, and a locally-settled
    // backing reads no attestation.
    if settles_locally(collateral_chain) {
        return Ok(());
    }
    require_fresh_heartbeat(config, now)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constants::ATTEST_MAX_AGE_SECS;

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

    fn config_with(last_attest_heartbeat: i64, attest_max_age_secs: i64) -> Config {
        Config {
            admin: Pubkey::default(),
            version: 0,
            min_collateral: 0,
            max_collateral: 0,
            fulfillment_timeout_secs: 0,
            min_swap_amount: 0,
            max_swap_amount: 0,
            tao_min_swap_amount: 0,
            tao_max_swap_amount: 0,
            tao_min_collateral: 0,
            settlement_grace_secs: 0,
            last_attest_heartbeat,
            attest_max_age_secs,
            reservation_ttl_secs: 0,
            consensus_threshold_percent: 100,
            validators: vec![],
            last_weights_update: 0,
            halted: false,
            reservation_fee_lamports: 0,
            pool_window_secs: 0,
            finalize_window_secs: 0,
            weights_update_min_interval_secs: 0,
            max_total_extension_secs: 0,
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
    fn the_payee_is_the_user_address_on_the_backing_leg() {
        // Same lookup, same both-sides symmetry as the amount: tao→btc pays the source-side address,
        // btc→tao the destination one. Nothing here knows either pair exists.
        assert_eq!(collateral_leg_user_addr("tao", "tao", "u_src", "btc", "u_dst"), "u_src");
        assert_eq!(collateral_leg_user_addr("tao", "btc", "u_src", "tao", "u_dst"), "u_dst");
        assert_eq!(collateral_leg_user_addr("sol", "sol", "u_src", "tao", "u_dst"), "u_src");
    }

    #[test]
    fn a_payee_lookup_off_the_legs_is_empty_rather_than_an_error() {
        // Unreachable live (finalize rejects a backing that is not a leg), and it must stay unable to
        // fail: this feeds a terminal timeout, where an error would strand the swap forever.
        assert_eq!(collateral_leg_user_addr("tao", "btc", "u_src", "eth", "u_dst"), "");
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

    // A plausible wall-clock `now`. The zero-heartbeat case is only stale because a real unix
    // timestamp dwarfs any max age — at `now` near the epoch a never-set heartbeat reads as fresh.
    const NOW: i64 = 1_760_000_000;

    #[test]
    fn a_deployment_that_has_never_heard_from_a_relayer_is_fused_shut() {
        // heartbeat 0 is not "no opinion", it is stale — TAO entry stays shut until the relay fleet
        // proves it is alive. The migration leaves it at 0 precisely so the door starts closed.
        assert!(require_fresh_heartbeat(&config_with(0, ATTEST_MAX_AGE_SECS), NOW).is_err());
    }

    #[test]
    fn the_fuse_trips_the_second_the_heartbeat_ages_past_its_limit() {
        // Exactly at the limit is still alive; one second more is not. Unreachable end-to-end —
        // `validate::attest_max_age` floors the age at an hour — so this is where it gets proven.
        let cfg = config_with(1_000, 3_600);
        assert!(require_fresh_heartbeat(&cfg, 4_600).is_ok());
        assert!(require_fresh_heartbeat(&cfg, 4_601).is_err());
    }

    #[test]
    fn a_dead_fuse_gates_tao_entry_and_leaves_sol_alone() {
        // D6 in one assertion: a stale heartbeat is a TAO-entry gate, not a subnet-wide halt.
        let dead = config_with(0, ATTEST_MAX_AGE_SECS);
        assert!(check_entry_gates(&dead, &miner_with(7), "sol", NOW).is_ok());
        assert!(check_entry_gates(&dead, &miner_with(7), "tao", NOW).is_err());
    }

    #[test]
    fn a_live_fuse_still_will_not_let_a_settling_miner_take_new_work() {
        // The two gates are independent: proving the relay alive must not also clear busy-until-settled.
        let cfg = config_with(1_000, ATTEST_MAX_AGE_SECS);
        let mut settling = miner_with(7);
        settling.settling_until = 2_000;
        assert!(check_entry_gates(&cfg, &settling, "tao", 1_999).is_err());
        assert!(check_entry_gates(&cfg, &settling, "tao", 2_000).is_ok());
    }

    #[test]
    fn a_pending_off_chain_seizure_freezes_the_sol_path_too() {
        // A miner owing an untaken TAO seizure is frozen whole — the settle gate is above the
        // locally-settling early return, so it applies to a SOL-backed open as well.
        let cfg = config_with(1_000, ATTEST_MAX_AGE_SECS);
        let mut settling = miner_with(7);
        settling.settling_until = 2_000;
        assert!(check_entry_gates(&cfg, &settling, "sol", 1_999).is_err());
        assert!(check_entry_gates(&cfg, &settling, "sol", 2_000).is_ok());
        // And it costs an unencumbered SOL-only miner nothing: settling_until is 0 for them, and a
        // locally-settled timeout never writes it.
        assert!(check_entry_gates(&cfg, &miner_with(7), "sol", 1).is_ok());
    }

    #[test]
    fn a_one_hub_pair_can_only_declare_its_hub() {
        // tao↔btc: BTC is not a hub and SOL is not a leg, so "tao" is the only declarable backing.
        assert_eq!(declarable_bit("tao", "tao", "btc").unwrap(), BACKING_BIT_TAO);
        assert!(declarable_bit("sol", "tao", "btc").is_err()); // not a leg
        assert!(declarable_bit("btc", "tao", "btc").is_err()); // a leg, but not a hub
    }

    #[test]
    fn a_hub_to_hub_pair_can_declare_either_leg() {
        assert_eq!(declarable_bit("sol", "sol", "tao").unwrap(), BACKING_BIT_SOL);
        assert_eq!(declarable_bit("tao", "sol", "tao").unwrap(), BACKING_BIT_TAO);
    }
}
