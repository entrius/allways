"""Per-chain network config — each chain takes a network NAME resolved to an endpoint in code.

Raw-URL escape hatches (SOLANA_RPC_URL env / solana-rpc config, BTC_NETWORK env) win for
paid/custom endpoints; otherwise the name maps to a public default. `env` is a one-liner bundle
that sets all three chains' networks + netuid at once. The Solana signer resolves the same way:
SOLANA_KEYPAIR_PATH env > solana-keypair config > ~/.solana/id.json.
"""

import json

import pytest

from allways.cli.swap_commands.helpers import (
    CHAIN_NETWORK_KEYS,
    ENV_BUNDLES,
    NAME_SELECTED_CHAINS,
    SOLANA_NETWORKS,
    apply_chain_network_env,
    load_cli_keypair,
    network_key,
    resolve_solana_keypair_path,
    resolve_solana_rpc,
)


@pytest.fixture(autouse=True)
def _no_ambient_api_key(monkeypatch):
    monkeypatch.delenv('SOLANA_RPC_API_KEY', raising=False)


def test_solana_name_resolves_to_endpoint(monkeypatch):
    monkeypatch.delenv('SOLANA_RPC_URL', raising=False)
    assert resolve_solana_rpc({'solana-network': 'devnet'}) == SOLANA_NETWORKS['devnet']
    assert resolve_solana_rpc({'solana-network': 'mainnet'}) == SOLANA_NETWORKS['mainnet']


def test_solana_env_wins_over_everything(monkeypatch):
    monkeypatch.setenv('SOLANA_RPC_URL', 'https://env.rpc/x')
    assert resolve_solana_rpc({'solana-rpc': 'https://paid.rpc/x', 'solana-network': 'mainnet'}) == 'https://env.rpc/x'


def test_solana_unknown_name_falls_back_localnet(monkeypatch):
    monkeypatch.delenv('SOLANA_RPC_URL', raising=False)
    assert resolve_solana_rpc({'solana-network': 'nope'}) == 'http://127.0.0.1:8899'


def test_api_key_composes_onto_resolved_endpoint(monkeypatch):
    monkeypatch.setenv('SOLANA_RPC_URL', 'https://mainnet.helius-rpc.com')
    monkeypatch.setenv('SOLANA_RPC_API_KEY', 'k1')
    assert resolve_solana_rpc({}) == 'https://mainnet.helius-rpc.com?api-key=k1'


def test_api_key_leaves_already_keyed_url_alone(monkeypatch):
    monkeypatch.setenv('SOLANA_RPC_URL', 'https://rpc.example/?api-key=inline')
    monkeypatch.setenv('SOLANA_RPC_API_KEY', 'k1')
    assert resolve_solana_rpc({}) == 'https://rpc.example/?api-key=inline'


def test_env_bundles_cover_all_chains():
    for name in ('testnet', 'mainnet'):
        b = ENV_BUNDLES[name]
        assert set(b) == {'network', 'solana-network', 'netuid', 'router', *CHAIN_NETWORK_KEYS}
        assert b['solana-network'] in SOLANA_NETWORKS
        for chain in NAME_SELECTED_CHAINS:
            assert b[network_key(chain)] in chain.networks


def test_mainnet_bundle_picks_mainnet_and_testnet_bundle_never_does():
    for chain in NAME_SELECTED_CHAINS:
        assert ENV_BUNDLES['mainnet'][network_key(chain)] == 'mainnet'
        # A spoke with a testnet must not be left on mainnet by `env testnet` (real funds
        # against test swaps); one declaring mainnet only is the sole honest exception.
        assert ENV_BUNDLES['testnet'][network_key(chain)] != 'mainnet' or len(chain.networks) == 1


@pytest.mark.parametrize('chain', NAME_SELECTED_CHAINS, ids=lambda c: c.id)
def test_chain_shim_sets_env_when_unset(monkeypatch, chain):
    import os

    monkeypatch.delenv(f'{chain.env_prefix}_NETWORK', raising=False)
    testnet = chain.networks[-1]
    apply_chain_network_env({network_key(chain): testnet})
    assert os.environ.get(f'{chain.env_prefix}_NETWORK') == testnet


@pytest.mark.parametrize('chain', NAME_SELECTED_CHAINS, ids=lambda c: c.id)
def test_chain_shim_respects_real_env(monkeypatch, chain):
    import os

    monkeypatch.setenv(f'{chain.env_prefix}_NETWORK', 'mainnet')
    apply_chain_network_env({network_key(chain): chain.networks[-1]})
    assert os.environ[f'{chain.env_prefix}_NETWORK'] == 'mainnet'  # explicit env wins


def test_keypair_env_wins_over_config(monkeypatch):
    monkeypatch.setenv('SOLANA_KEYPAIR_PATH', '/env/id.json')
    assert resolve_solana_keypair_path({'solana-keypair': '/cfg/id.json'}) == '/env/id.json'


def test_configured_missing_keypair_fails_not_generates(monkeypatch, tmp_path):
    monkeypatch.delenv('SOLANA_KEYPAIR_PATH', raising=False)
    missing = tmp_path / 'nope.json'
    with pytest.raises(SystemExit):
        load_cli_keypair({'solana-keypair': str(missing)})
    assert not missing.exists()  # must NOT silently mint a fresh key at an explicit path


def test_env_missing_keypair_fails_not_generates(monkeypatch, tmp_path):
    missing = tmp_path / 'nope.json'
    monkeypatch.setenv('SOLANA_KEYPAIR_PATH', str(missing))
    with pytest.raises(SystemExit):
        load_cli_keypair({})
    assert not missing.exists()


def test_configured_keypair_loads(monkeypatch, tmp_path):
    from solders.keypair import Keypair

    kp = Keypair()
    p = tmp_path / 'id.json'
    p.write_text(json.dumps(list(bytes(kp))))
    monkeypatch.delenv('SOLANA_KEYPAIR_PATH', raising=False)
    assert load_cli_keypair({'solana-keypair': str(p)}).pubkey() == kp.pubkey()
