"""The per-chain network CLI contract, pinned at the command surface.

Every chain's `<id>-network` key, the `env` bundles and the `alw config` rows are derived from
the registry. This module pins the literal bundles (a wrong bundle value silently points a miner
at the wrong network), that `env` switches every key at once, and that an unknown name writes
nothing.
"""

import json
import os

import pytest
from click.testing import CliRunner

from allways.chains import SUPPORTED_CHAINS
from allways.cli import main
from allways.cli.swap_commands import helpers
from allways.cli.swap_commands.helpers import ENV_BUNDLES, NAME_SELECTED_CHAINS, network_key


@pytest.fixture
def config_file(tmp_path, monkeypatch):
    path = tmp_path / 'config.json'
    monkeypatch.setattr(main, 'CONFIG_FILE', path)
    monkeypatch.setattr(helpers, 'CONFIG_FILE', path)
    monkeypatch.setattr(helpers, '_CLI_OVERRIDES', {})
    # The shim writes {PREFIX}_NETWORK straight into os.environ, and monkeypatch cannot undo a
    # write it did not make — isolate the process env so it can't follow the session into an
    # asset constructor (a leaked ETH_NETWORK=sepolia builds every ETH asset on the wrong network).
    monkeypatch.setattr(os, 'environ', os.environ.copy())
    for chain in SUPPORTED_CHAINS.values():
        monkeypatch.delenv(f'{chain.env_prefix}_NETWORK', raising=False)
    for var in ('SOLANA_RPC_URL', 'SOLANA_RPC_API_KEY', 'SOLANA_KEYPAIR_PATH', 'ALLWAYS_PROGRAM_ID'):
        monkeypatch.delenv(var, raising=False)
    return path


def run(*args):
    return CliRunner().invoke(main.config_group, list(args))


class TestPinnedContract:
    """Literal expectations — the derivation must reproduce exactly these, or an operator's
    muscle memory (and every doc that quotes them) breaks."""

    def test_testnet_bundle(self):
        assert ENV_BUNDLES['testnet'] == {
            'network': 'test',
            'solana-network': 'devnet',
            'btc-network': 'testnet4',
            'eth-network': 'sepolia',
            'arb-network': 'sepolia',
            'hype-network': 'testnet',
            'bnb-network': 'testnet',
            'avax-network': 'fuji',
            'base-network': 'sepolia',
            'cro-network': 'testnet',
            'pol-network': 'amoy',
            'netuid': '19',
            'router': '5HicmHG7fjbxrtx8FZNdv4xxS5jSN84KGpMnTHsKtKv9peao',
        }

    def test_mainnet_bundle(self):
        assert ENV_BUNDLES['mainnet'] == {
            'network': 'finney',
            'solana-network': 'mainnet',
            'btc-network': 'mainnet',
            'eth-network': 'mainnet',
            'arb-network': 'mainnet',
            'hype-network': 'mainnet',
            'bnb-network': 'mainnet',
            'avax-network': 'mainnet',
            'base-network': 'mainnet',
            'cro-network': 'mainnet',
            'pol-network': 'mainnet',
            'netuid': '7',
            'router': '',
        }

    def test_testnet_pick_is_valid_where_declared(self):
        # A declared testnet must be an accepted name and must never be mainnet — `env testnet`
        # silently leaving a chain on mainnet spends real funds on test swaps. Leaving it
        # undeclared is the honest option for a chain with no testnet (testnet_name falls back
        # to the default then), so empty is allowed and every live chain declares one.
        for chain in NAME_SELECTED_CHAINS:
            if chain.testnet_network:
                assert chain.testnet_network in chain.networks
                assert chain.testnet_network != 'mainnet'
        assert all(c.testnet_network for c in NAME_SELECTED_CHAINS)


class TestConfigSet:
    def test_unknown_name_is_rejected_and_writes_nothing(self, config_file):
        for chain in NAME_SELECTED_CHAINS:
            result = run('set', network_key(chain), 'nope')
            assert result.exit_code == 0
            assert f'Unknown {network_key(chain)}' in result.output
            assert list(chain.networks)[0] in result.output  # the expected names are shown
            assert not config_file.exists() or network_key(chain) not in json.loads(config_file.read_text())

    def test_env_bundle_writes_every_chain_key(self, config_file):
        assert run('set', 'env', 'testnet').exit_code == 0
        written = json.loads(config_file.read_text())
        assert written == ENV_BUNDLES['testnet']

    def test_env_mainnet_overwrites_a_testnet_config(self, config_file):
        run('set', 'env', 'testnet')
        run('set', 'env', 'mainnet')
        assert json.loads(config_file.read_text()) == ENV_BUNDLES['mainnet']


class TestConfigShow:
    def test_env_var_beats_default_and_config_beats_env(self, config_file, monkeypatch):
        chain = NAME_SELECTED_CHAINS[0]
        monkeypatch.setenv(f'{chain.env_prefix}_NETWORK', chain.networks[1])
        rows = {k: (v, src) for k, v, src in main._effective_settings({})}
        assert rows[network_key(chain)] == (chain.networks[1], 'env')

        rows = {k: (v, src) for k, v, src in main._effective_settings({network_key(chain): chain.networks[0]})}
        assert rows[network_key(chain)] == (chain.networks[0], 'config')
