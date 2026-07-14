"""Per-chain network config — each chain takes a network NAME resolved to an endpoint in code.

Raw-URL escape hatches (SOLANA_RPC_URL env / solana-rpc config, BTC_NETWORK env) win for
paid/custom endpoints; otherwise the name maps to a public default. `env` is a one-liner bundle
that sets all three chains' networks + netuid at once. The Solana signer resolves the same way:
SOLANA_KEYPAIR_PATH env > solana-keypair config > ~/.solana/id.json.
"""

import json
from pathlib import Path

import pytest

from allways.cli.swap_commands.helpers import (
    BTC_NETWORKS,
    ENV_BUNDLES,
    SOLANA_NETWORKS,
    apply_btc_network_env,
    load_cli_keypair,
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


def test_api_key_composes_onto_resolved_endpoint(monkeypatch):
    monkeypatch.setenv('SOLANA_RPC_URL', 'https://mainnet.helius-rpc.com')
    monkeypatch.setenv('SOLANA_RPC_API_KEY', 'k1')
    assert resolve_solana_rpc({}) == 'https://mainnet.helius-rpc.com?api-key=k1'


def test_api_key_appends_with_ampersand_when_query_present(monkeypatch):
    monkeypatch.setenv('SOLANA_RPC_URL', 'https://rpc.example/x?tier=pro')
    monkeypatch.setenv('SOLANA_RPC_API_KEY', 'k1')
    assert resolve_solana_rpc({}) == 'https://rpc.example/x?tier=pro&api-key=k1'


def test_api_key_leaves_already_keyed_url_alone(monkeypatch):
    monkeypatch.setenv('SOLANA_RPC_URL', 'https://rpc.example/?api-key=inline')
    monkeypatch.setenv('SOLANA_RPC_API_KEY', 'k1')
    assert resolve_solana_rpc({}) == 'https://rpc.example/?api-key=inline'


def test_api_key_composes_onto_network_name(monkeypatch):
    monkeypatch.delenv('SOLANA_RPC_URL', raising=False)
    monkeypatch.setenv('SOLANA_RPC_API_KEY', 'k1')
    assert resolve_solana_rpc({'solana-network': 'devnet'}) == SOLANA_NETWORKS['devnet'] + '?api-key=k1'


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


def test_keypair_env_wins_over_config(monkeypatch):
    monkeypatch.setenv('SOLANA_KEYPAIR_PATH', '/env/id.json')
    assert resolve_solana_keypair_path({'solana-keypair': '/cfg/id.json'}) == '/env/id.json'


def test_keypair_config_used_when_env_unset(monkeypatch):
    monkeypatch.delenv('SOLANA_KEYPAIR_PATH', raising=False)
    assert resolve_solana_keypair_path({'solana-keypair': '/cfg/id.json'}) == '/cfg/id.json'


def test_keypair_defaults_to_solana_cli_path(monkeypatch):
    monkeypatch.delenv('SOLANA_KEYPAIR_PATH', raising=False)
    assert resolve_solana_keypair_path({}) == str(Path.home() / '.solana' / 'id.json')


def test_keypair_config_tilde_expands(monkeypatch):
    monkeypatch.delenv('SOLANA_KEYPAIR_PATH', raising=False)
    assert resolve_solana_keypair_path({'solana-keypair': '~/keys/id.json'}) == str(Path.home() / 'keys' / 'id.json')


def test_keypair_env_tilde_expands(monkeypatch):
    monkeypatch.setenv('SOLANA_KEYPAIR_PATH', '~/env-keys/id.json')
    assert resolve_solana_keypair_path({}) == str(Path.home() / 'env-keys' / 'id.json')


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


def test_bare_default_still_auto_generates(monkeypatch, tmp_path):
    # No env, no config → the solana-CLI default path keeps its dev convenience of minting a key.
    monkeypatch.delenv('SOLANA_KEYPAIR_PATH', raising=False)
    monkeypatch.setenv('HOME', str(tmp_path))
    kp = load_cli_keypair({})
    default = tmp_path / '.solana' / 'id.json'
    assert default.exists()
    assert json.loads(default.read_text()) == list(bytes(kp))
