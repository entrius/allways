//! Backing (collateral-chain) lookups — the D4 layer. Every collateral decision keys off the swap's
//! `collateral_chain` and nothing else: no function here may branch on the PAIR. A new hub is a new
//! chain id plus a match arm, which is what makes "TAO↔BTC ships with no program diff" true.

use anchor_lang::prelude::*;

use crate::constants::{BACKING_CHAIN_SOL, BACKING_CHAIN_TAO};
use crate::error::ErrorCode;
use crate::state::{Config, MinerState};

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

/// The purse backing `collateral_chain`, in that asset's smallest unit.
///
/// W1 SEAM — this one function is the whole switch. "sol" reads the local vault ledger; every other
/// backing is refused until W2 points it at the `BondAttestation` mirror (effective bond, not gross).
pub fn backing_purse(collateral_chain: &str, miner_state: &MinerState) -> Result<u64> {
    require!(
        collateral_chain == BACKING_CHAIN_SOL,
        ErrorCode::BackingNotSupported
    );
    Ok(miner_state.collateral)
}

/// Whether a penalty against this backing settles here, atomically. False = Solana reaches the verdict
/// only; the seizure is a separate quorum on the backing chain (busy-until-settled applies).
pub fn settles_locally(collateral_chain: &str) -> bool {
    collateral_chain == BACKING_CHAIN_SOL
}

#[cfg(test)]
mod tests {
    use super::*;

    fn miner_with(collateral: u64) -> MinerState {
        MinerState {
            miner: Pubkey::default(),
            collateral,
            active: true,
            has_active_swap: false,
            busy_until: 0,
            deactivation_at: 0,
            successful_swaps: 0,
            failed_swaps: 0,
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
    fn only_sol_has_a_purse_until_w2() {
        assert_eq!(backing_purse("sol", &miner_with(42)).unwrap(), 42);
        assert!(backing_purse("tao", &miner_with(42)).is_err());
        assert!(backing_purse("btc", &miner_with(42)).is_err());
    }

    #[test]
    fn only_sol_settles_locally() {
        assert!(settles_locally("sol"));
        assert!(!settles_locally("tao"));
    }
}
