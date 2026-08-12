"""Tests for allways.chains — chain registry, canonical pairing, and the seconds-based extension target."""

import re

import pytest

from allways.chains import (
    CHAIN_ARBUSDC,
    CHAIN_BTC,
    CHAIN_ETH,
    CHAIN_HYPE,
    CHAIN_TAO,
    EXTENSION_BUCKET_SECONDS,
    SUPPORTED_CHAINS,
    canonical_pair,
    compute_extension_target_secs,
    get_chain_def,
)


class TestGetChain:
    def test_btc(self):
        assert get_chain_def('btc') is CHAIN_BTC

    def test_tao(self):
        assert get_chain_def('tao') is CHAIN_TAO

    def test_eth(self):
        assert get_chain_def('eth') is CHAIN_ETH

    def test_arbusdc(self):
        assert get_chain_def('arbusdc') is CHAIN_ARBUSDC

    def test_hype(self):
        assert get_chain_def('hype') is CHAIN_HYPE

    def test_unsupported_raises(self):
        with pytest.raises(KeyError):
            get_chain_def('doge')

    def test_ids_are_lowercase_and_fit_the_wire(self):
        """Every registry id must be lowercase (the program rejects cased ids at intake, PDAs
        and the grace table key off exact strings) AND <=10 chars (every chain column across
        the DB is VARCHAR(10) — an 11-16 char id passes on-chain then errors on insert)."""
        for chain_id, chain in SUPPORTED_CHAINS.items():
            assert re.fullmatch(r'[a-z0-9]{1,10}', chain_id), chain_id
            assert chain.id == chain_id

    def test_layered_fields_default_none_for_native_assets(self):
        for chain_id in ('btc', 'tao', 'sol', 'eth', 'hype'):
            assert get_chain_def(chain_id).host_chain is None
            assert get_chain_def(chain_id).asset_locator is None
        assert CHAIN_ARBUSDC.host_chain == 'arbitrum'
        assert CHAIN_ARBUSDC.asset_locator.startswith('0x')


class TestCanonicalPair:
    def test_tao_hub_is_source(self):
        # TAO is a hub: canonical source of every tao↔spoke pair, so the rate
        # reads 'X per 1 TAO' — same shape as every other hub pair.
        assert canonical_pair('tao', 'btc') == ('tao', 'btc')
        assert canonical_pair('btc', 'tao') == ('tao', 'btc')
        assert canonical_pair('tao', 'eth') == ('tao', 'eth')
        assert canonical_pair('eth', 'tao') == ('tao', 'eth')
        assert canonical_pair('thor', 'tao') == ('tao', 'thor')

    def test_no_hub_alphabetical(self):
        assert canonical_pair('eth', 'btc') == ('btc', 'eth')
        assert canonical_pair('btc', 'eth') == ('btc', 'eth')

    def test_sol_always_source(self):
        # SOL is the first hub: always canonical source, outranking TAO on the
        # hub↔hub pair (grandfathered — sol↔tao quotes keep their convention).
        assert canonical_pair('sol', 'btc') == ('sol', 'btc')
        assert canonical_pair('btc', 'sol') == ('sol', 'btc')
        assert canonical_pair('sol', 'tao') == ('sol', 'tao')
        assert canonical_pair('tao', 'sol') == ('sol', 'tao')
        assert canonical_pair('sol', 'eth') == ('sol', 'eth')
        assert canonical_pair('eth', 'sol') == ('sol', 'eth')


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

    def test_eth_remaining_confs(self):
        # ETH needs 32 confs, 12s each: remaining=32, raw = 10000 + 384 + 120 = 10504, bucket up to 10800.
        assert compute_extension_target_secs('eth', 0, self.NOW, self.CEILING) == 10_800

    def test_arbusdc_remaining_confs(self):
        # arbusdc needs 90 confs, 1s each: remaining=90, raw = 10000 + 90 + 120 = 10210, bucket up to 10800.
        assert compute_extension_target_secs('arbusdc', 0, self.NOW, self.CEILING) == 10_800

    def test_hype_remaining_confs(self):
        # hype needs 2 confs, 1s each: remaining=2, raw = 10000 + 2 + 120 = 10122, bucket up to 10200.
        assert compute_extension_target_secs('hype', 0, self.NOW, self.CEILING) == 10_200

    def test_unsupported_chain_raises(self):
        with pytest.raises(KeyError):
            compute_extension_target_secs('doge', 0, self.NOW, self.CEILING)
