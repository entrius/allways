"""Tests for allways.chains — chain registry, canonical pairing, and the seconds-based extension target."""

import re

import pytest

from allways.assets import ASSET_REGISTRY
from allways.assets.erc20 import Erc20
from allways.assets.evm import EVM_NETWORKS
from allways.chains import (
    CHAIN_ARBUSDC,
    CHAIN_ASTER,
    CHAIN_AVAX,
    CHAIN_BASEUSDC,
    CHAIN_BNB,
    CHAIN_BTC,
    CHAIN_CRO,
    CHAIN_ETH,
    CHAIN_ETHUSDC,
    CHAIN_HYPE,
    CHAIN_POL,
    CHAIN_POLUSDC,
    CHAIN_QNT,
    CHAIN_SOL,
    CHAIN_SOLUSDC,
    CHAIN_TAO,
    CHAIN_UNI,
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

    def test_bnb(self):
        assert get_chain_def('bnb') is CHAIN_BNB

    def test_avax(self):
        assert get_chain_def('avax') is CHAIN_AVAX

    def test_baseusdc(self):
        assert get_chain_def('baseusdc') is CHAIN_BASEUSDC

    def test_ethusdc(self):
        assert get_chain_def('ethusdc') is CHAIN_ETHUSDC

    def test_cro(self):
        assert get_chain_def('cro') is CHAIN_CRO

    def test_aster(self):
        assert get_chain_def('aster') is CHAIN_ASTER

    def test_uni(self):
        assert get_chain_def('uni') is CHAIN_UNI

    def test_qnt(self):
        assert get_chain_def('qnt') is CHAIN_QNT

    def test_pol(self):
        assert get_chain_def('pol') is CHAIN_POL

    def test_polusdc(self):
        assert get_chain_def('polusdc') is CHAIN_POLUSDC

    def test_solusdc(self):
        assert get_chain_def('solusdc') is CHAIN_SOLUSDC

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

    def test_assets_on_one_network_share_its_env_identity(self):
        """A network is configured once, by EXACTLY ONE of its rows. Rows sharing a host_chain
        MUST share env_prefix, and exactly one of them declares ``networks`` — no more, because
        two would render duplicate CLI rows writing the same var, and no fewer, because a network
        nobody declares gets no CLI row at all: `alw config set env testnet` never writes its
        {PREFIX}_NETWORK, EvmChain defaults it to mainnet, and a testnet miner pays real funds
        against test swaps."""
        prefixes: dict[str, set[str]] = {}
        declared: dict[str, list[str]] = {}
        owners: dict[str, list[str]] = {}
        for chain in SUPPORTED_CHAINS.values():
            if chain.host_chain:
                prefixes.setdefault(chain.host_chain, set()).add(chain.env_prefix)
                declared.setdefault(chain.host_chain, [])
            if chain.networks:
                owners.setdefault(chain.env_prefix, []).append(chain.id)
                if chain.host_chain:
                    declared[chain.host_chain].append(chain.id)
        for host, found in prefixes.items():
            assert len(found) == 1, f'{host} assets disagree on env_prefix: {sorted(found)}'
        for host, ids in declared.items():
            if host not in EVM_NETWORKS:
                # Solana's cluster is genesis-hash-guarded, never picked by name: no row declares
                # `networks`, and none may (it would render a dead CLI key beside `solana-network`).
                assert ids == [], f'{host} is not name-selected; {ids} must not declare networks'
                continue
            assert len(ids) == 1, f'{host} needs exactly one networks-declaring row, found {ids}'
        for prefix, ids in owners.items():
            assert len(ids) == 1, f'{prefix}_NETWORK is declared by more than one row: {ids}'

    def test_every_token_row_declares_its_refusal_checks(self):
        """Declaring is what keeps a token's miners slashable. () claims no freeze surface;
        only None is undeclared, and a token row must never be None."""
        for spec in ASSET_REGISTRY:
            declared = get_chain_def(spec.chain_id).refusal_checks is not None
            assert declared is issubclass(spec.cls, Erc20), spec.chain_id

    def test_only_self_hosted_assets_lack_a_host_chain(self):
        """btc/tao/sol ARE their own network; every hosted row names the network it rides — an
        EVM_NETWORKS key, or 'solana' for an SPL token beside native SOL. Only the self-hosted list
        is enumerated — a new hosted asset needs no edit here."""
        for chain_id, chain in SUPPORTED_CHAINS.items():
            if chain_id in ('btc', 'tao', 'sol'):
                assert chain.host_chain is None, chain_id
            else:
                assert chain.host_chain in EVM_NETWORKS or chain.host_chain == 'solana', chain_id
        # asset_locator is the token-only field: a native coin has no contract/mint to pin.
        assert CHAIN_ARBUSDC.asset_locator.startswith('0x')
        assert all(
            get_chain_def(c).asset_locator is None for c in ('btc', 'tao', 'sol', 'eth', 'hype', 'bnb', 'avax', 'cro')
        )
        # Ethereum hosts several assets (eth, ethusdc, uni). Every rider takes CHAIN_ETH's network
        # row — same prefix, no networks of its own — so one ETH_NETWORK moves all of them.
        riders = [c for c in SUPPORTED_CHAINS.values() if c.host_chain == CHAIN_ETH.host_chain and c is not CHAIN_ETH]
        assert CHAIN_ETHUSDC in riders and CHAIN_UNI in riders
        for chain in riders:
            assert (chain.env_prefix, chain.networks) == (CHAIN_ETH.env_prefix, ()), chain.id

    def test_spl_token_rows_ride_solana(self):
        """An SPL token is hosted on 'solana', pins a base58 mint, shares SOL's env identity and
        finality depth, and declares no `networks` (the cluster is genesis-hash-guarded, not named)."""
        from solders.pubkey import Pubkey

        from allways.assets.spl_token import SplToken

        rows = [get_chain_def(spec.chain_id) for spec in ASSET_REGISTRY if issubclass(spec.cls, SplToken)]
        assert CHAIN_SOLUSDC in rows
        for chain in rows:
            assert chain.host_chain == 'solana', chain.id
            Pubkey.from_string(chain.asset_locator)  # a mint, not an 0x contract
            assert chain.env_prefix == CHAIN_SOL.env_prefix and chain.networks == (), chain.id
            assert chain.min_confirmations == CHAIN_SOL.min_confirmations, chain.id


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

    def test_bnb_remaining_confs(self):
        # bnb needs 15 confs, 1s each: remaining=15, raw = 10000 + 15 + 120 = 10135, bucket up to 10200.
        assert compute_extension_target_secs('bnb', 0, self.NOW, self.CEILING) == 10_200

    def test_baseusdc_remaining_confs(self):
        # baseusdc needs 120 confs, 2s each: remaining=120, raw = 10000 + 240 + 120 = 10360, bucket up to 10800.
        assert compute_extension_target_secs('baseusdc', 0, self.NOW, self.CEILING) == 10_800

    def test_cro_remaining_confs(self):
        # cro needs 2 confs, 1s each: remaining=2, raw = 10000 + 2 + 120 = 10122, bucket up to 10200.
        assert compute_extension_target_secs('cro', 0, self.NOW, self.CEILING) == 10_200

    def test_pol_remaining_confs(self):
        # pol needs 100 confs at the stored 1s: raw = 10000 + 100 + 120 = 10220, bucket up to 10800.
        assert compute_extension_target_secs('pol', 0, self.NOW, self.CEILING) == 10_800
        # What covers the 50s the integer floor under-counts is the 120s padding, NOT the bucket:
        # the bucket's contribution is phase-dependent and is 0 at the `now` below. 220s of cover
        # against a 150s real need is the whole margin, and it is what breaks first if
        # min_confirmations is ever raised past 240 — the literal above would not notice.
        worst_phase = 9_980  # now + remaining + padding lands exactly on a bucket boundary
        cover = compute_extension_target_secs('pol', 0, worst_phase, self.CEILING) - worst_phase
        assert cover == 220
        assert cover >= CHAIN_POL.min_confirmations * 1.5

    def test_polusdc_matches_the_chain_it_shares(self):
        # A token and its host coin must extend identically — same confs, same stored block time.
        assert compute_extension_target_secs('polusdc', 0, self.NOW, self.CEILING) == compute_extension_target_secs(
            'pol', 0, self.NOW, self.CEILING
        )

    def test_solusdc_matches_the_chain_it_shares(self):
        assert compute_extension_target_secs('solusdc', 0, self.NOW, self.CEILING) == compute_extension_target_secs(
            'sol', 0, self.NOW, self.CEILING
        )

    def test_unsupported_chain_raises(self):
        with pytest.raises(KeyError):
            compute_extension_target_secs('doge', 0, self.NOW, self.CEILING)


class TestReplayGrace:
    """Freshness gates on block_time >= floor - grace, where the floor is stamped by the HUB
    clock. Spoke timestamps — however well-behaved on their own chain — say nothing about
    hub-vs-spoke skew, so a zero grace strands an honest deposit stamped just behind the floor."""

    def test_every_evm_chain_carries_the_skew_grace(self):
        # Ethereum's monotonic slot timestamps once justified 0 here; the floor is hub-stamped,
        # so ETH (and ethusdc with it) need the same allowance as every other EVM row.
        for chain in SUPPORTED_CHAINS.values():
            if chain.host_chain in EVM_NETWORKS:
                assert chain.replay_grace_secs == 60, chain.id
        # An SPL token shares the hub's ledger AND clock: its leg's blockTime and the reservation
        # floor are stamped by the same chain, so there is no hub-vs-spoke skew to absorb.
        assert CHAIN_SOLUSDC.replay_grace_secs == 0

    def test_freshness_consumer_absorbs_the_eth_grace(self):
        # The validator reads grace off chain_def (solana_swap_loop._is_fresh) — a deposit
        # stamped inside the grace window is fresh; one predating it is still a replay.
        from types import SimpleNamespace

        from allways.validator.solana_swap_loop import is_tx_fresh

        floor = 1_755_000_000
        grace = CHAIN_ETH.replay_grace_secs
        assert is_tx_fresh(SimpleNamespace(block_time=floor - grace), floor, grace)
        assert not is_tx_fresh(SimpleNamespace(block_time=floor - grace - 1), floor, grace)


class TestApplyTestnetNetworkDefaults:
    """A testnet neuron must never fall through to a provider's mainnet default for an unset
    {PREFIX}_NETWORK — that verifies test swaps against mainnet or spends real mainnet funds."""

    def test_unset_spokes_default_to_testnet(self, monkeypatch):
        from allways.chains import apply_testnet_network_defaults

        for chain in SUPPORTED_CHAINS.values():
            if chain.networks:
                monkeypatch.delenv(f'{chain.env_prefix}_NETWORK', raising=False)
        applied = apply_testnet_network_defaults()
        # BTC's testnet is testnet4; ETH's is sepolia — a representative name-selected pair.
        assert applied['BTC_NETWORK'] == CHAIN_BTC.testnet_network == 'testnet4'
        assert applied['ETH_NETWORK'] == CHAIN_ETH.testnet_network == 'sepolia'
        import os

        assert os.environ['BTC_NETWORK'] == 'testnet4'

    def test_explicit_env_var_wins(self, monkeypatch):
        from allways.chains import apply_testnet_network_defaults

        monkeypatch.setenv('ETH_NETWORK', 'mainnet')  # operator opts one spoke back to mainnet
        applied = apply_testnet_network_defaults()
        assert 'ETH_NETWORK' not in applied
        import os

        assert os.environ['ETH_NETWORK'] == 'mainnet'

    def test_skips_chains_without_a_named_testnet(self, monkeypatch):
        # sol/tao pick their network by RPC/bittensor, not {PREFIX}_NETWORK; a shared-prefix
        # secondary (polusdc under POL) carries no networks of its own. Neither is touched.
        from allways.chains import apply_testnet_network_defaults

        monkeypatch.delenv('POL_NETWORK', raising=False)
        applied = apply_testnet_network_defaults()
        assert CHAIN_TAO.env_prefix + '_NETWORK' not in applied
        assert CHAIN_POLUSDC.env_prefix + '_NETWORK' in applied  # POL (its own row) IS named
        # polusdc itself contributes no separate var — POL owns the prefix.
        assert CHAIN_POLUSDC.networks == ()
