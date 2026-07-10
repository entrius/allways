"""Tests for allways.chains — chain registry, canonical pairing, and the seconds-based extension target."""

import pytest

from allways.chains import (
    CHAIN_BTC,
    CHAIN_TAO,
    EXTENSION_BUCKET_SECONDS,
    canonical_pair,
    compute_extension_target_secs,
    get_chain,
)


class TestGetChain:
    def test_btc(self):
        assert get_chain('btc') is CHAIN_BTC

    def test_tao(self):
        assert get_chain('tao') is CHAIN_TAO

    def test_unsupported_raises(self):
        with pytest.raises(KeyError):
            get_chain('eth')


class TestCanonicalPair:
    def test_already_canonical(self):
        assert canonical_pair('btc', 'tao') == ('btc', 'tao')

    def test_reversed_input(self):
        assert canonical_pair('tao', 'btc') == ('btc', 'tao')

    def test_tao_always_dest(self):
        # TAO preference: even when a chain sorts after "tao", TAO is dest
        assert canonical_pair('thor', 'tao') == ('thor', 'tao')
        assert canonical_pair('tao', 'thor') == ('thor', 'tao')

    def test_no_tao_alphabetical(self):
        assert canonical_pair('eth', 'btc') == ('btc', 'eth')
        assert canonical_pair('btc', 'eth') == ('btc', 'eth')

    def test_sol_always_source(self):
        # SOL is the hub: always canonical source, outranking the TAO-dest rule.
        assert canonical_pair('sol', 'btc') == ('sol', 'btc')
        assert canonical_pair('btc', 'sol') == ('sol', 'btc')
        assert canonical_pair('sol', 'tao') == ('sol', 'tao')
        assert canonical_pair('tao', 'sol') == ('sol', 'tao')

    def test_non_sol_pairs_unchanged(self):
        # Re-anchor must not perturb the active tao<->btc directions.
        assert canonical_pair('btc', 'tao') == ('btc', 'tao')
        assert canonical_pair('tao', 'btc') == ('btc', 'tao')


class TestComputeExtensionTargetSecs:
    # Unix-seconds target = now + max(0, min_confirmations - confs) * seconds_per_block + 120s padding,
    # bucketed up to the native 600s grid, clamped to the contract ceiling (max_extend_at).
    NOW = 10_000
    CEILING = 10_000_000

    def test_btc_zero_confs(self):
        # BTC needs 2 confs: remaining=2, raw = 10000 + 2*600 + 120 = 11320, bucket up to 11400.
        assert compute_extension_target_secs('btc', 0, self.NOW, self.CEILING) == 11_400

    def test_btc_one_conf(self):
        # remaining=1, raw = 10000 + 600 + 120 = 10720, bucket up to 10800.
        assert compute_extension_target_secs('btc', 1, self.NOW, self.CEILING) == 10_800

    def test_btc_fully_confirmed_only_padding(self):
        # remaining clamps to 0: raw = 10000 + 120 = 10120, bucket up to 10200.
        assert compute_extension_target_secs('btc', 5, self.NOW, self.CEILING) == 10_200

    def test_tao_remaining_confs(self):
        # TAO needs 6 confs, 12s each: remaining=6, raw = 10000 + 72 + 120 = 10192, bucket up to 10200.
        assert compute_extension_target_secs('tao', 0, self.NOW, self.CEILING) == 10_200

    def test_target_is_strictly_after_now(self):
        target = compute_extension_target_secs('btc', 0, self.NOW, self.CEILING)
        assert target > self.NOW

    def test_result_is_bucket_aligned(self):
        for confs in range(0, 4):
            target = compute_extension_target_secs('btc', confs, self.NOW, self.CEILING)
            assert target % EXTENSION_BUCKET_SECONDS == 0

    def test_clamped_to_ceiling(self):
        # A ceiling below the computed target wins — the contract caps target_at at max_extend_at.
        ceiling = self.NOW + 500
        assert compute_extension_target_secs('btc', 0, self.NOW, ceiling) == ceiling

    def test_unsupported_chain_raises(self):
        with pytest.raises(KeyError):
            compute_extension_target_secs('eth', 0, self.NOW, self.CEILING)
