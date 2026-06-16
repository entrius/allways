//! Reservation-lottery draw primitives (Phase 9). Pure + deterministic so the same winner is
//! computed by everyone resolving the same pool; unit-tested standalone (LiteSVM may not populate the
//! SlotHashes sysvar, so the on-chain seed read is integration-tested while these are unit-tested).

use anchor_lang::prelude::*;
use solana_keccak_hasher::hashv;

/// Derive the draw seed from the pool key and the pinned slot's hash bytes:
/// `seed = keccak(pool_key || slothash)`, taking the first 16 bytes as a little-endian u128.
pub fn draw_seed(pool_key: &Pubkey, slothash: &[u8]) -> u128 {
    let h = hashv(&[pool_key.as_ref(), slothash]).to_bytes();
    let mut buf = [0u8; 16];
    buf.copy_from_slice(&h[..16]);
    u128::from_le_bytes(buf)
}

/// Stake-weighted pick: cumulative-bucket over `weights`, `x = seed % total_weight`, return the index
/// whose bucket contains `x`. If `total_weight == 0` (e.g. all entrants unweighted), fall back to a
/// uniform pick by index. Caller guarantees `weights` is non-empty.
pub fn pick_weighted(seed: u128, weights: &[u64]) -> usize {
    let total: u128 = weights.iter().map(|&w| w as u128).sum();
    if total == 0 {
        return (seed % weights.len() as u128) as usize;
    }
    let mut x = seed % total;
    for (i, &w) in weights.iter().enumerate() {
        let w = w as u128;
        if x < w {
            return i;
        }
        x -= w;
    }
    weights.len() - 1 // unreachable (x < total), kept for total safety
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_entrant_always_wins() {
        for seed in [0u128, 1, 7, u128::MAX] {
            assert_eq!(pick_weighted(seed, &[5]), 0);
        }
    }

    #[test]
    fn buckets_map_correctly() {
        // weights [1,2,1] → total 4; buckets: 0→[0,1), 1,2→[1,3), 3→[3,4)
        assert_eq!(pick_weighted(0, &[1, 2, 1]), 0);
        assert_eq!(pick_weighted(1, &[1, 2, 1]), 1);
        assert_eq!(pick_weighted(2, &[1, 2, 1]), 1);
        assert_eq!(pick_weighted(3, &[1, 2, 1]), 2);
        assert_eq!(pick_weighted(4, &[1, 2, 1]), 0); // wraps via modulo
    }

    #[test]
    fn zero_weight_never_picked() {
        // middle bucket has weight 0 → indices 0 and 2 only.
        for seed in 0..100u128 {
            assert_ne!(pick_weighted(seed, &[3, 0, 3]), 1);
        }
    }

    #[test]
    fn all_zero_falls_back_to_uniform() {
        assert_eq!(pick_weighted(0, &[0, 0, 0]), 0);
        assert_eq!(pick_weighted(1, &[0, 0, 0]), 1);
        assert_eq!(pick_weighted(2, &[0, 0, 0]), 2);
        assert_eq!(pick_weighted(3, &[0, 0, 0]), 0);
    }

    #[test]
    fn weight_share_is_proportional() {
        // index 0 has 9x the weight of index 1 → wins ~90% across the seed space.
        let weights = [9u64, 1];
        let mut wins0 = 0;
        for seed in 0..1000u128 {
            if pick_weighted(seed, &weights) == 0 {
                wins0 += 1;
            }
        }
        assert_eq!(wins0, 900, "9:1 weight → exactly 900/1000 buckets to index 0");
    }
}
