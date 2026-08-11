"""The per-chain network CLI contract, pinned at the command surface.

Every chain's `<id>-network` key, its accepted names, the `env` bundles, the `alw config`
rows and the `config set` help are derived from the registry now. Deriving them is only
safe if the derived values are exactly the ones operators already type, so this module
pins the literal contract: a wrong bundle value silently points a miner at the wrong
network, and a dropped key breaks a documented command.
"""

import json

import pytest
from click.testing import CliRunner

from allways.chains import SUPPORTED_CHAINS
from allways.cli import main
from allways.cli.swap_commands import helpers
from allways.cli.swap_commands.helpers import CHAIN_NETWORK_KEYS, ENV_BUNDLES, NAME_SELECTED_CHAINS, network_key


@pytest.fixture
def config_file(tmp_path, monkeypatch):
    path = tmp_path / 'config.json'
    monkeypatch.setattr(main, 'CONFIG_FILE', path)
    monkeypatch.setattr(helpers, 'CONFIG_FILE', path)
    monkeypatch.setattr(helpers, '_CLI_OVERRIDES', {})
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

    def test_settable_keys(self):
        assert main.VALID_CONFIG_KEYS == (
            'wallet',
            'hotkey',
            'network',
            'netuid',
            'program-id',
            'solana-rpc',
            'solana-network',
            'solana-keypair',
            'btc-network',
            'eth-network',
            'arb-network',
            'hype-network',
            'router',
            'env',
        )

    def test_chain_network_keys(self):
        assert CHAIN_NETWORK_KEYS == ('btc-network', 'eth-network', 'arb-network', 'hype-network')

    def test_testnet_bundle(self):
        assert ENV_BUNDLES['testnet'] == {
            'network': 'test',
            'solana-network': 'devnet',
            'btc-network': 'testnet4',
            'eth-network': 'sepolia',
            'arb-network': 'sepolia',
            'hype-network': 'testnet',
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

    def test_accepted_network_names(self):
        accepted = {c.id: c.networks for c in NAME_SELECTED_CHAINS}
        assert accepted == {
            'btc': ('mainnet', 'testnet', 'testnet4', 'signet'),
            'eth': ('mainnet', 'sepolia'),
            'arbusdc': ('mainnet', 'sepolia'),
            'hype': ('mainnet', 'testnet'),
        }


class TestConfigSet:
    def test_every_chain_key_accepts_every_declared_name(self, config_file):
        for chain in NAME_SELECTED_CHAINS:
            for name in chain.networks:
                result = run('set', network_key(chain), name)
                assert result.exit_code == 0, result.output
                assert json.loads(config_file.read_text())[network_key(chain)] == name

    def test_unknown_name_is_rejected_and_writes_nothing(self, config_file):
        for chain in NAME_SELECTED_CHAINS:
            result = run('set', network_key(chain), 'nope')
            assert result.exit_code == 0
            assert f'Unknown {network_key(chain)}' in result.output
            assert list(chain.networks)[0] in result.output  # the expected names are shown
            assert not config_file.exists() or network_key(chain) not in json.loads(config_file.read_text())

    def test_unknown_key_is_rejected_by_click(self, config_file):
        result = run('set', 'doge-network', 'mainnet')
        assert result.exit_code != 0

    def test_env_bundle_writes_every_chain_key(self, config_file):
        assert run('set', 'env', 'testnet').exit_code == 0
        written = json.loads(config_file.read_text())
        assert written == ENV_BUNDLES['testnet']

    def test_env_mainnet_overwrites_a_testnet_config(self, config_file):
        run('set', 'env', 'testnet')
        run('set', 'env', 'mainnet')
        assert json.loads(config_file.read_text()) == ENV_BUNDLES['mainnet']


class TestConfigShow:
    def test_defaults_to_mainnet_per_chain(self, config_file):
        result = run()
        assert result.exit_code == 0, result.output
        for chain in NAME_SELECTED_CHAINS:
            assert network_key(chain) in result.output

    def test_env_var_beats_default_and_config_beats_env(self, config_file, monkeypatch):
        chain = NAME_SELECTED_CHAINS[0]
        monkeypatch.setenv(f'{chain.env_prefix}_NETWORK', chain.networks[1])
        rows = {k: (v, src) for k, v, src in main._effective_settings({})}
        assert rows[network_key(chain)] == (chain.networks[1], 'env')

        rows = {k: (v, src) for k, v, src in main._effective_settings({network_key(chain): chain.networks[0]})}
        assert rows[network_key(chain)] == (chain.networks[0], 'config')

    def test_default_row_when_neither_is_set(self, config_file):
        rows = {k: (v, src) for k, v, src in main._effective_settings({})}
        for chain in NAME_SELECTED_CHAINS:
            assert rows[network_key(chain)] == ('mainnet', 'default')


class TestHelp:
    def test_lists_every_chain_key_and_its_networks(self):
        result = run('set', '--help')
        assert result.exit_code == 0
        for chain in NAME_SELECTED_CHAINS:
            assert network_key(chain) in result.output
            for name in chain.networks:
                assert name in result.output


class TestProviderHandoff:
    """The config only matters because it reaches the providers through {PREFIX}_NETWORK."""

    def test_shim_feeds_every_chain(self, config_file, monkeypatch):
        import os

        helpers.apply_chain_network_env({network_key(c): c.networks[1] for c in NAME_SELECTED_CHAINS})
        for chain in NAME_SELECTED_CHAINS:
            assert os.environ[f'{chain.env_prefix}_NETWORK'] == chain.networks[1]

    def test_shim_never_overrides_an_explicit_env(self, config_file, monkeypatch):
        import os

        for chain in NAME_SELECTED_CHAINS:
            monkeypatch.setenv(f'{chain.env_prefix}_NETWORK', 'mainnet')
        helpers.apply_chain_network_env({network_key(c): c.networks[1] for c in NAME_SELECTED_CHAINS})
        for chain in NAME_SELECTED_CHAINS:
            assert os.environ[f'{chain.env_prefix}_NETWORK'] == 'mainnet'
