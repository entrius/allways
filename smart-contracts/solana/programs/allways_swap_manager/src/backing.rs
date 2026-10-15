//! Backing (collateral-chain) lookups — the D4 layer. Every collateral decision keys off the swap's
//! `collateral_chain` and nothing else: no function here may branch on the PAIR. A new hub is a new
//! chain id plus a match arm, which is what makes "TAO↔BTC ships with no program diff" true.

use anchor_lang::prelude::*;

use crate::constants::{
    BACKING_BIT_BASEUSDC, BACKING_BIT_SOL, BACKING_BIT_TAO, BACKING_CHAIN_BASEUSDC, BACKING_CHAIN_SOL,
    BACKING_CHAIN_TAO,
};
use crate::error::ErrorCode;
use crate::state::{BondAttestation, Config, MinerState};

/// The backing family a chain settles in. An `sn<N>` alpha shares subtensor's account space, so a TAO
/// penalty pays its holder directly — which is why the (quorum-bound) chain id may be trusted for this.
pub fn family(chain: &str) -> &str {
    match chain.strip_prefix("sn") {
        Some(n) if !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()) => BACKING_CHAIN_TAO,
        _ => chain,
    }
}

/// Whether the backing amount is provable from the leg or declared off-chain.
#[derive(Debug, PartialEq)]
pub enum LegBind {
    Exact(u128),
    Declared,
}

/// Bind `collateral_amount` to the leg denominated in the backing: exact when that leg IS the backing
/// asset, declared when it merely settles in its family. Validity is "backing ∈ leg families".
pub fn collateral_leg_bind(
    backing: &str,
    from_chain: &str,
    from_amount: u128,
    to_chain: &str,
    to_amount: u128,
) -> Result<LegBind> {
    // Exact legs first: sn7→tao still gets the free on-chain check on its tao leg.
    if backing == from_chain {
        Ok(LegBind::Exact(from_amount))
    } else if backing == to_chain {
        Ok(LegBind::Exact(to_amount))
    } else if backing == family(from_chain) || backing == family(to_chain) {
        Ok(LegBind::Declared)
    } else {
        err!(ErrorCode::BackingNotInLegs)
    }
}

/// The user's address on the backing leg (exact leg first, as `collateral_leg_bind`) — whom a penalty
/// settled there is owed to. Total: a timeout is terminal, so no leg yields no payee rather than an error.
pub fn collateral_leg_user_addr(
    collateral_chain: &str,
    from_chain: &str,
    user_from_addr: &str,
    to_chain: &str,
    user_to_addr: &str,
) -> String {
    let legs = [(from_chain, user_from_addr), (to_chain, user_to_addr)];
    legs.iter()
        .find(|(chain, _)| collateral_chain == *chain)
        .or_else(|| legs.iter().find(|(chain, _)| collateral_chain == family(chain)))
        .map_or_else(String::new, |(_, addr)| addr.to_string())
}

/// Swap-size bounds for a backing, in that asset's own smallest unit (0 max = unbounded).
pub fn swap_bounds(config: &Config, collateral_chain: &str) -> Result<(u64, u64)> {
    match collateral_chain {
        BACKING_CHAIN_SOL => Ok((config.min_swap_amount, config.max_swap_amount)),
        BACKING_CHAIN_TAO => Ok((config.tao_min_swap_amount, config.tao_max_swap_amount)),
        BACKING_CHAIN_BASEUSDC => Ok((config.usdc_min_swap_amount, config.usdc_max_swap_amount)),
        _ => err!(ErrorCode::BackingNotSupported),
    }
}

/// The activation bit for a backing. Doubles as the "is this a known backing" check — an unknown chain
/// has no bit, no purse and no attestation.
pub fn backing_bit(collateral_chain: &str) -> Result<u8> {
    match collateral_chain {
        BACKING_CHAIN_SOL => Ok(BACKING_BIT_SOL),
        BACKING_CHAIN_TAO => Ok(BACKING_BIT_TAO),
        BACKING_CHAIN_BASEUSDC => Ok(BACKING_BIT_BASEUSDC),
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
        collateral_chain == family(from_chain) || collateral_chain == family(to_chain),
        ErrorCode::BackingNotInLegs
    );
    Ok(bit)
}

/// The purse a miner must hold to activate this backing, in that asset's own smallest unit — lamports
/// for "sol", rao for "tao", µUSDC for "baseusdc". Never converted through a rate; each hub has its own.
pub fn activation_floor(config: &Config, collateral_chain: &str) -> Result<u64> {
    match collateral_chain {
        BACKING_CHAIN_SOL => Ok(config.min_collateral),
        BACKING_CHAIN_TAO => Ok(config.tao_min_collateral),
        BACKING_CHAIN_BASEUSDC => Ok(config.usdc_min_collateral),
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
        BACKING_CHAIN_TAO | BACKING_CHAIN_BASEUSDC => attested_purse(attestation),
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

/// The dead-man fuse: refuse a non-local backing once ITS hub's heartbeat goes stale, so one stalled
/// relay never fuses another hub. A never-set heartbeat (0) is stale by construction, so a hub fuses
/// closed until its relayers prove they are alive.
pub fn require_fresh_heartbeat(config: &Config, collateral_chain: &str, now: i64) -> Result<()> {
    let slot = MinerState::backing_slot(backing_bit(collateral_chain)?);
    require!(
        now.saturating_sub(config.last_attest_heartbeat[slot]) <= config.attest_max_age_secs[slot],
        ErrorCode::AttestationStale
    );
    Ok(())
}

/// A remote hub whose size floor or bond floor was never set is closed, not unbounded: the upgrade
/// that adds a hub must change nothing live until the admin opens it. The 0-max escape is untouched.
pub fn require_configured(config: &Config, collateral_chain: &str) -> Result<()> {
    if settles_locally(collateral_chain) {
        return Ok(());
    }
    let (min_swap, _) = swap_bounds(config, collateral_chain)?;
    require!(
        min_swap != 0 && activation_floor(config, collateral_chain)? != 0,
        ErrorCode::BackingNotConfigured
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
    let bit = backing_bit(collateral_chain)?;
    // A pending seizure freezes only ITS hub (v3.1, reversing the v3 whole-miner freeze): the debt is
    // owed by one bond, and the other hub's pot neither owes nor backs it. Zero for a SOL-only miner
    // and for locally-settled timeouts (those seize atomically).
    require!(now >= miner_state.settling_slot(bit), ErrorCode::MinerSettling);
    // The fuse stays non-local: it is an attestation-freshness question, and a locally-settled
    // backing reads no attestation.
    if settles_locally(collateral_chain) {
        return Ok(());
    }
    require_configured(config, collateral_chain)?;
    require_fresh_heartbeat(config, collateral_chain, now)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constants::{ATTEST_MAX_AGE_SECS, MAX_BACKING_SLOTS};

    fn miner_with(collateral: u64) -> MinerState {
        MinerState {
            miner: Pubkey::default(),
            collateral,
            active: true,
            active_backings: BACKING_BIT_SOL,
            has_active_swap: false,
            active_swap_backings: 0,
            busy_until: [0; crate::constants::MAX_BACKING_SLOTS],
            settling_until: [0; crate::constants::MAX_BACKING_SLOTS],
            reserved_collateral: [0; crate::constants::MAX_BACKING_SLOTS],
            deactivation_at: 0,
            successful_swaps: 0,
            failed_swaps: 0,
            bump: 255,
        }
    }

    /// Both remote hubs configured and sharing one fuse reading — the per-hub tests then move one slot.
    fn config_with(last_attest_heartbeat: i64, attest_max_age_secs: i64) -> Config {
        let mut slots = [0; MAX_BACKING_SLOTS];
        let mut ages = [0; MAX_BACKING_SLOTS];
        for bit in [BACKING_BIT_TAO, BACKING_BIT_BASEUSDC] {
            slots[MinerState::backing_slot(bit)] = last_attest_heartbeat;
            ages[MinerState::backing_slot(bit)] = attest_max_age_secs;
        }
        Config {
            admin: Pubkey::default(),
            version: 0,
            min_collateral: 0,
            max_collateral: 0,
            fulfillment_timeout_secs: 0,
            min_swap_amount: 0,
            max_swap_amount: 0,
            tao_min_swap_amount: 100_000_000,
            tao_max_swap_amount: 0,
            tao_min_collateral: 250_000_000,
            usdc_min_swap_amount: 1_000_000,
            usdc_max_swap_amount: 0,
            usdc_min_collateral: 25_000_000,
            settlement_grace_secs: 0,
            last_attest_heartbeat: slots,
            attest_max_age_secs: ages,
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
            vault_generation: [0; MAX_BACKING_SLOTS],
        }
    }

    fn slot(chain: &str) -> usize {
        MinerState::backing_slot(backing_bit(chain).unwrap())
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
    fn subnet_ids_belong_to_the_tao_family() {
        for chain in ["sn7", "sn74"] {
            assert_eq!(family(chain), "tao");
        }
        for chain in ["sn", "sn7x", "snx", "tao", "sol", "btc"] {
            assert_eq!(family(chain), chain);
        }
    }

    #[test]
    fn exact_leg_binding_matches_the_legacy_lookup() {
        fn legacy_leg_amount(
            backing: &str, from: &str, from_amt: u128, to: &str, to_amt: u128,
        ) -> Result<u128> {
            if backing == from {
                Ok(from_amt)
            } else if backing == to {
                Ok(to_amt)
            } else {
                err!(ErrorCode::BackingNotInLegs)
            }
        }
        let pairs = [
            ("sol", "sol", "btc"), ("sol", "eth", "sol"),
            ("tao", "tao", "btc"), ("tao", "eth", "tao"),
            ("sol", "sol", "tao"), ("tao", "sol", "tao"),
        ];
        for (backing, from, to) in pairs {
            let legacy = legacy_leg_amount(backing, from, 7, to, 11).unwrap();
            assert_eq!(collateral_leg_bind(backing, from, 7, to, 11).unwrap(), LegBind::Exact(legacy));
        }
    }

    #[test]
    fn alpha_leg_binding_is_declared_only_for_tao_backing() {
        assert_eq!(collateral_leg_bind("tao", "sn7", 7, "avax", 11).unwrap(), LegBind::Declared);
        assert_eq!(collateral_leg_bind("tao", "sn7", 7, "sn74", 11).unwrap(), LegBind::Declared);
        assert_eq!(collateral_leg_bind("sol", "sn7", 7, "sol", 11).unwrap(), LegBind::Exact(11));
        // An alpha↔tao pair keeps the free on-chain check on its tao leg in BOTH directions.
        assert_eq!(collateral_leg_bind("tao", "sn7", 7, "tao", 11).unwrap(), LegBind::Exact(11));
        assert_eq!(collateral_leg_bind("tao", "tao", 7, "sn7", 11).unwrap(), LegBind::Exact(7));
        assert!(collateral_leg_bind("sol", "sn7", 7, "avax", 11).is_err());
    }

    #[test]
    fn the_payee_is_the_user_address_on_the_backing_leg() {
        // Same lookup, same both-sides symmetry as the amount: tao→btc pays the source-side address,
        // btc→tao the destination one. Nothing here knows either pair exists.
        assert_eq!(collateral_leg_user_addr("tao", "tao", "u_src", "btc", "u_dst"), "u_src");
        assert_eq!(collateral_leg_user_addr("tao", "btc", "u_src", "tao", "u_dst"), "u_dst");
        assert_eq!(collateral_leg_user_addr("sol", "sol", "u_src", "tao", "u_dst"), "u_src");
        assert_eq!(collateral_leg_user_addr("tao", "sn7", "u_src", "avax", "u_dst"), "u_src");
        assert_eq!(collateral_leg_user_addr("tao", "sn7", "u_src", "tao", "u_dst"), "u_dst");
    }

    #[test]
    fn a_payee_lookup_off_the_legs_is_empty_rather_than_an_error() {
        // Unreachable live (finalize rejects a backing that is not a leg), and it must stay unable to
        // fail: this feeds a terminal timeout, where an error would strand the swap forever.
        assert_eq!(collateral_leg_user_addr("tao", "btc", "u_src", "eth", "u_dst"), "");
    }

    #[test]
    fn sol_reads_local_collateral_and_tao_reads_the_attestation() {
        // "sol" ignores any attestation handed to it; "tao" ignores the local vault entirely.
        let a = attestation(42, true);
        assert_eq!(backing_purse("sol", &miner_with(7), Some(&a)).unwrap(), 7);
        assert_eq!(backing_purse("tao", &miner_with(7), Some(&a)).unwrap(), 42);
        assert_eq!(backing_purse("baseusdc", &miner_with(7), Some(&a)).unwrap(), 42);
        assert!(backing_purse("baseusdc", &miner_with(7), None).is_err());
        assert!(backing_purse("baseusdc", &miner_with(7), Some(&attestation(42, false))).is_err());
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
        assert!(!settles_locally("baseusdc"));
    }

    #[test]
    fn every_known_backing_has_a_distinct_bit() {
        assert_eq!(backing_bit("sol").unwrap(), BACKING_BIT_SOL);
        assert_eq!(backing_bit("tao").unwrap(), BACKING_BIT_TAO);
        assert_eq!(backing_bit("baseusdc").unwrap(), BACKING_BIT_BASEUSDC);
        assert_eq!(BACKING_BIT_BASEUSDC, 1 << 2);
        let bits = crate::constants::BACKINGS.map(|(bit, _)| bit);
        assert_eq!(bits.iter().fold(0u8, |m, b| m | b).count_ones() as usize, bits.len());
        for (bit, chain) in crate::constants::BACKINGS {
            assert_eq!(backing_bit(chain).unwrap(), bit, "{chain}: registry row and match arm agree");
        }
        // Other USDC chains are spokes, never hubs.
        for chain in ["btc", "arbusdc", "solusdc", "usdc"] {
            assert!(backing_bit(chain).is_err());
        }
    }

    #[test]
    fn each_hub_reads_its_own_bounds_and_floor() {
        let cfg = config_with(0, ATTEST_MAX_AGE_SECS);
        assert_eq!(swap_bounds(&cfg, "tao").unwrap(), (100_000_000, 0));
        assert_eq!(swap_bounds(&cfg, "baseusdc").unwrap(), (1_000_000, 0));
        assert_eq!(activation_floor(&cfg, "tao").unwrap(), 250_000_000);
        assert_eq!(activation_floor(&cfg, "baseusdc").unwrap(), 25_000_000);
    }

    #[test]
    fn a_stale_base_heartbeat_never_fuses_tao_and_the_other_way_round() {
        let mut cfg = config_with(NOW, ATTEST_MAX_AGE_SECS);
        cfg.last_attest_heartbeat[slot("baseusdc")] = 0;
        assert!(check_entry_gates(&cfg, &miner_with(7), "tao", NOW).is_ok());
        assert!(check_entry_gates(&cfg, &miner_with(7), "baseusdc", NOW).is_err());

        let mut cfg = config_with(NOW, ATTEST_MAX_AGE_SECS);
        cfg.last_attest_heartbeat[slot("tao")] = 0;
        assert!(check_entry_gates(&cfg, &miner_with(7), "tao", NOW).is_err());
        assert!(check_entry_gates(&cfg, &miner_with(7), "baseusdc", NOW).is_ok());
    }

    #[test]
    fn each_hub_ages_against_its_own_max_age() {
        let mut cfg = config_with(NOW - 7_200, 3_600);
        cfg.attest_max_age_secs[slot("baseusdc")] = 10_800;
        assert!(require_fresh_heartbeat(&cfg, "tao", NOW).is_err());
        assert!(require_fresh_heartbeat(&cfg, "baseusdc", NOW).is_ok());
    }

    #[test]
    fn sol_stays_exempt_from_every_fuse_slot() {
        // Every heartbeat dead, every max age zero, even the unused "sol" slot: SOL entry is untouched.
        let mut cfg = config_with(0, 0);
        cfg.usdc_min_swap_amount = 0;
        cfg.usdc_min_collateral = 0;
        assert!(check_entry_gates(&cfg, &miner_with(7), "sol", NOW).is_ok());
    }

    #[test]
    fn an_unconfigured_base_hub_is_closed_even_with_a_live_heartbeat() {
        // The post-upgrade state: usdc_* are 0. A heartbeat alone must not open the hub.
        let mut cfg = config_with(NOW, ATTEST_MAX_AGE_SECS);
        cfg.usdc_min_swap_amount = 0;
        assert!(check_entry_gates(&cfg, &miner_with(7), "baseusdc", NOW).is_err());
        let mut cfg = config_with(NOW, ATTEST_MAX_AGE_SECS);
        cfg.usdc_min_collateral = 0;
        assert!(check_entry_gates(&cfg, &miner_with(7), "baseusdc", NOW).is_err());
        assert!(check_entry_gates(&cfg, &miner_with(7), "tao", NOW).is_ok(), "TAO is unaffected");
    }

    #[test]
    fn a_never_beaten_base_slot_is_fused_shut() {
        // Fresh upgrade: Base slot heartbeat 0 and max age 0, everything else configured and live.
        let mut cfg = config_with(NOW, ATTEST_MAX_AGE_SECS);
        cfg.last_attest_heartbeat[slot("baseusdc")] = 0;
        cfg.attest_max_age_secs[slot("baseusdc")] = 0;
        assert!(check_entry_gates(&cfg, &miner_with(7), "baseusdc", NOW).is_err());
    }

    // A plausible wall-clock `now`. The zero-heartbeat case is only stale because a real unix
    // timestamp dwarfs any max age — at `now` near the epoch a never-set heartbeat reads as fresh.
    const NOW: i64 = 1_760_000_000;

    #[test]
    fn a_deployment_that_has_never_heard_from_a_relayer_is_fused_shut() {
        // heartbeat 0 is not "no opinion", it is stale — TAO entry stays shut until the relay fleet
        // proves it is alive. The migration leaves it at 0 precisely so the door starts closed.
        assert!(require_fresh_heartbeat(&config_with(0, ATTEST_MAX_AGE_SECS), "tao", NOW).is_err());
    }

    #[test]
    fn the_fuse_trips_the_second_the_heartbeat_ages_past_its_limit() {
        // Exactly at the limit is still alive; one second more is not. Unreachable end-to-end —
        // `validate::attest_max_age` floors the age at an hour — so this is where it gets proven.
        let cfg = config_with(1_000, 3_600);
        assert!(require_fresh_heartbeat(&cfg, "tao", 4_600).is_ok());
        assert!(require_fresh_heartbeat(&cfg, "tao", 4_601).is_err());
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
        settling.set_settling(BACKING_BIT_TAO, 2_000);
        assert!(check_entry_gates(&cfg, &settling, "tao", 1_999).is_err());
        assert!(check_entry_gates(&cfg, &settling, "tao", 2_000).is_ok());
    }

    #[test]
    fn a_pending_tao_seizure_leaves_the_sol_path_open() {
        // v3.1 reverses the v3 whole-miner freeze: the debt is the TAO bond's, and the SOL pot
        // neither owes nor backs it — so only TAO entry is gated while the seizure settles.
        let cfg = config_with(1_000, ATTEST_MAX_AGE_SECS);
        let mut settling = miner_with(7);
        settling.set_settling(BACKING_BIT_TAO, 2_000);
        assert!(check_entry_gates(&cfg, &settling, "tao", 1_999).is_err());
        assert!(check_entry_gates(&cfg, &settling, "sol", 1_999).is_ok(), "sol keeps trading");
        // And it costs an unencumbered SOL-only miner nothing: settling slots are 0 for them, and a
        // locally-settled timeout never writes one.
        assert!(check_entry_gates(&cfg, &miner_with(7), "sol", 1).is_ok());
    }

    #[test]
    fn a_one_hub_pair_can_only_declare_its_hub() {
        // tao↔btc: BTC is not a hub and SOL is not a leg, so "tao" is the only declarable backing.
        assert_eq!(declarable_bit("tao", "tao", "btc").unwrap(), BACKING_BIT_TAO);
        assert_eq!(declarable_bit("tao", "sn7", "avax").unwrap(), BACKING_BIT_TAO);
        assert!(declarable_bit("tao", "btc", "avax").is_err());
        assert!(declarable_bit("sol", "tao", "btc").is_err()); // not a leg
        assert!(declarable_bit("btc", "tao", "btc").is_err()); // a leg, but not a hub
    }

    #[test]
    fn a_hub_to_hub_pair_can_declare_either_leg() {
        assert_eq!(declarable_bit("sol", "sol", "tao").unwrap(), BACKING_BIT_SOL);
        assert_eq!(declarable_bit("tao", "sol", "tao").unwrap(), BACKING_BIT_TAO);
        for (a, b) in [("sol", "baseusdc"), ("tao", "baseusdc"), ("baseusdc", "sol"), ("baseusdc", "tao")] {
            assert!(declarable_bit(a, a, b).is_ok());
            assert!(declarable_bit(b, a, b).is_ok());
        }
    }

    #[test]
    fn baseusdc_backs_only_pairs_it_is_a_leg_of() {
        assert_eq!(declarable_bit("baseusdc", "baseusdc", "btc").unwrap(), BACKING_BIT_BASEUSDC);
        assert_eq!(declarable_bit("baseusdc", "eth", "baseusdc").unwrap(), BACKING_BIT_BASEUSDC);
        assert_eq!(declarable_bit("baseusdc", "baseusdc", "sn7").unwrap(), BACKING_BIT_BASEUSDC);
        // Never a declared family backing: other USDC spokes and alpha pairs it is not a leg of.
        for (from, to) in [("arbusdc", "btc"), ("btc", "solusdc"), ("sol", "sn7"), ("sn7", "tao"), ("tao", "btc")] {
            assert!(declarable_bit("baseusdc", from, to).is_err(), "{from}->{to}");
        }
    }

    #[test]
    fn baseusdc_always_binds_to_an_exact_leg() {
        assert_eq!(collateral_leg_bind("baseusdc", "baseusdc", 7, "sn7", 11).unwrap(), LegBind::Exact(7));
        assert_eq!(collateral_leg_bind("baseusdc", "btc", 7, "baseusdc", 11).unwrap(), LegBind::Exact(11));
        assert!(collateral_leg_bind("baseusdc", "arbusdc", 7, "btc", 11).is_err());
        assert_eq!(collateral_leg_user_addr("baseusdc", "btc", "u_src", "baseusdc", "u_dst"), "u_dst");
    }

    #[test]
    fn sol_to_alpha_backing_is_unchanged_by_the_base_hub() {
        // TAO via the alpha family, as before. The program also still accepts "sol" here because sol
        // is a leg; keeping sol↔snN TAO-only is the validator's quote policy, not this function's.
        assert_eq!(declarable_bit("tao", "sol", "sn7").unwrap(), BACKING_BIT_TAO);
        assert_eq!(declarable_bit("sol", "sol", "sn7").unwrap(), BACKING_BIT_SOL);
        assert!(declarable_bit("baseusdc", "sol", "sn7").is_err());
        // baseusdc↔snN: the program allows either; USDC-only on that lane is also validator policy.
        assert_eq!(declarable_bit("tao", "baseusdc", "sn7").unwrap(), BACKING_BIT_TAO);
    }
}
