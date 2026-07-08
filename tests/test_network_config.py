"""Per-chain network config — each chain takes a network NAME resolved to an endpoint in code.

Raw-URL escape hatches (SOLANA_RPC_URL env / solana-rpc config, BTC_NETWORK env) win for
paid/custom endpoints; otherwise the name maps to a public default. `env` is a one-liner bundle
that sets all three chains' networks + netuid at once.
"""

from allways.cli.swap_commands.helpers import (
    BTC_NETWORKS,
    ENV_BUNDLES,
    SOLANA_NETWORKS,
    apply_btc_network_env,
    resolve_solana_rpc,
)


def test_solana_name_resolves_to_endpoint(monkeypatch):
    monkeypatch.delenv('SOLANA_RPC_URL', raising=False)
    assert resolve_solana_rpc({'solana-network': 'devnet'}) == SOLANA_NETWORKS['devnet']
    assert resolve_solana_rpc({'solana-network': 'mainnet'}) == SOLANA_NETWORKS['mainnet']


def test_solana_rpc_config_is_escape_hatch(monkeypatch):
    monkeypatch.delenv('SOLANA_RPC_URL', raising=False)
    # A custom/paid URL in solana-rpc wins over the network name.
    assert resolve_solana_rpc({'solana-rpc': 'https://paid.rpc/x', 'solana-network': 'devnet'}) == 'https://paid.rpc/x'


def test_solana_env_wins_over_everything(monkeypatch):
    monkeypatch.setenv('SOLANA_RPC_URL', 'https://env.rpc/x')
    assert resolve_solana_rpc({'solana-rpc': 'https://paid.rpc/x', 'solana-network': 'mainnet'}) == 'https://env.rpc/x'


def test_solana_unset_defaults_localnet(monkeypatch):
    monkeypatch.delenv('SOLANA_RPC_URL', raising=False)
    assert resolve_solana_rpc({}) == 'http://127.0.0.1:8899'


def test_solana_unknown_name_falls_back_localnet(monkeypatch):
    monkeypatch.delenv('SOLANA_RPC_URL', raising=False)
    assert resolve_solana_rpc({'solana-network': 'nope'}) == 'http://127.0.0.1:8899'


def test_env_bundles_cover_all_three_chains():
    for name in ('testnet', 'mainnet'):
        b = ENV_BUNDLES[name]
        assert set(b) == {'network', 'solana-network', 'btc-network', 'netuid'}
        assert b['solana-network'] in SOLANA_NETWORKS
        assert b['btc-network'] in BTC_NETWORKS


def test_btc_shim_sets_env_when_unset(monkeypatch):
    monkeypatch.delenv('BTC_NETWORK', raising=False)
    apply_btc_network_env({'btc-network': 'testnet4'})
    import os

    assert os.environ.get('BTC_NETWORK') == 'testnet4'


def test_btc_shim_respects_real_env(monkeypatch):
    monkeypatch.setenv('BTC_NETWORK', 'mainnet')
    apply_btc_network_env({'btc-network': 'testnet4'})
    import os

    assert os.environ['BTC_NETWORK'] == 'mainnet'  # explicit env wins
