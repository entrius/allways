"""`alw miner init` — the pure setup helpers, and the unattended configure-only path end to end
against a tmp project dir with the chain + wallet layers stubbed out."""

import json
import os
import stat
from types import SimpleNamespace

import pytest
from click.testing import CliRunner

from allways.cli.swap_commands import helpers, miner_init
from allways.cli.swap_commands import setup_env as se
from allways.constants import TAO_HUB_VAULT_ADDRESSES

# ─── key families ────────────────────────────────────────────────────────────


# ─── key generation ──────────────────────────────────────────────────────────


@pytest.mark.parametrize('network,prefix', [('mainnet', 'bc1q'), ('testnet4', 'tb1q')])
def test_btc_key_roundtrips_for_network(network, prefix):
    wif, addr = se.generate_btc_key(network)
    assert addr.startswith(prefix)
    assert se.btc_address(wif, network) == addr
    assert se.btc_address('garbage', network) is None


# ─── .env editing ────────────────────────────────────────────────────────────

TEMPLATE = """# header
NETUID=7                            # 7 mainnet | 19 testnet
WALLET_NAME=default
# SOLANA_KEYPAIR_PATH=
BTC_NETWORK=mainnet                 # mainnet | testnet
ETH_PRIVATE_KEY=
"""


def test_write_env_starts_from_template_and_is_private(tmp_path):
    template = tmp_path / '.env.example'
    template.write_text(TEMPLATE)
    env = tmp_path / '.env'
    se.write_env(env, {'WALLET_NAME': 'w'}, template=template)
    assert stat.S_IMODE(env.stat().st_mode) == 0o600
    assert se.read_env(env) == {'NETUID': '7', 'WALLET_NAME': 'w', 'BTC_NETWORK': 'mainnet', 'ETH_PRIVATE_KEY': ''}
    se.write_env(env, {'NETUID': '19'})  # second write edits, never re-templates
    assert se.read_env(env)['WALLET_NAME'] == 'w' and se.read_env(env)['NETUID'] == '19'


# ─── the wizard, unattended ──────────────────────────────────────────────────


@pytest.fixture
def sandbox(tmp_path, monkeypatch):
    """Tmp config + wallets + project dir; wallet objects and the preflight are stubbed."""
    allways_dir = tmp_path / 'allways'
    config = allways_dir / 'config.json'
    monkeypatch.setattr(miner_init, 'ALLWAYS_DIR', allways_dir)
    monkeypatch.setattr(miner_init, 'CONFIG_FILE', config)
    monkeypatch.setattr(helpers, 'CONFIG_FILE', config)
    monkeypatch.setattr(helpers, '_CLI_OVERRIDES', {})
    for var in ('SOLANA_RPC_URL', 'SOLANA_KEYPAIR_PATH', 'WALLET_PATH', 'ETH_NETWORK', 'BTC_NETWORK'):
        monkeypatch.delenv(var, raising=False)

    wallets = tmp_path / 'wallets'
    (wallets / 'w' / 'hotkeys').mkdir(parents=True)
    (wallets / 'w' / 'coldkeypub.txt').write_text('{}')
    (wallets / 'w' / 'hotkeys' / 'h').write_text('{}')
    monkeypatch.setattr(se, 'wallets_root', lambda: wallets)

    stub = SimpleNamespace(
        coldkeypub=SimpleNamespace(ss58_address='5Cold'), hotkey=SimpleNamespace(ss58_address='5Hot')
    )
    monkeypatch.setattr(miner_init, '_bt_wallet', lambda name, hotkey: stub)
    monkeypatch.setattr(miner_init, 'run_doctor', lambda project_dir, container=True: [(True, 'stub', 'ok')])

    project = tmp_path / 'proj'
    project.mkdir()
    (project / 'docker-compose.miner.yml').write_text('services: {}')
    (project / '.env.example').write_text(TEMPLATE)
    return SimpleNamespace(config=config, project=project, wallets=wallets)


def _run(sandbox, *extra):
    args = [
        '--network', 'testnet', '--wallet', 'w', '--hotkey', 'h', '--backing', 'sol', '--chains', 'eth,btc',
        '--solana-rpc', 'http://rpc.test', '--project-dir', str(sandbox.project), '--configure-only', '-y', *extra,
    ]  # fmt: skip
    return CliRunner().invoke(miner_init.init_command, args)


def test_configure_only_writes_config_env_and_keys(sandbox):
    result = _run(sandbox)
    assert result.exit_code == 0, result.output

    config = json.loads(sandbox.config.read_text())
    assert config['netuid'] == '19' and config['network'] == 'test'
    assert config['vault-address'] == TAO_HUB_VAULT_ADDRESSES['test']
    assert config['wallet'] == 'w' and config['hotkey'] == 'h'
    keypair = sandbox.project / 'data' / 'solana' / 'id.json'
    assert config['solana-keypair'] == str(keypair) and keypair.exists()
    assert stat.S_IMODE(keypair.stat().st_mode) == 0o600

    env = se.read_env(sandbox.project / '.env')
    assert env['NETUID'] == '19' and env['SUBTENSOR_NETWORK'] == 'test' and env['ALLWAYS_IMAGE_TAG'] == 'test'
    assert env['WALLET_NAME'] == 'w' and env['HOTKEY_NAME'] == 'h' and env['WALLET_PATH'] == str(sandbox.wallets)
    assert env['SOLANA_RPC_URL'] == 'http://rpc.test'
    assert env['ETH_NETWORK'] == 'sepolia' and env['BTC_NETWORK'] == 'testnet4'
    assert se.evm_address(env['ETH_PRIVATE_KEY']) is not None
    assert se.btc_address(env['BTC_PRIVATE_KEY'], 'testnet4') is not None
    assert env['PORT'] == '8091' and env['LOG_LEVEL'] == 'info'
    assert 'MINER_BITTENSOR_COLDKEY_PASSWORD' not in env
    assert stat.S_IMODE((sandbox.project / '.env').stat().st_mode) == 0o600

    # keys are never echoed; addresses and the summary box are
    assert env['ETH_PRIVATE_KEY'] not in result.output and env['BTC_PRIVATE_KEY'] not in result.output
    assert se.evm_address(env['ETH_PRIVATE_KEY']) in result.output
    assert '5Cold' in result.output and 'Configured' in result.output


def test_rerun_keeps_existing_keys(sandbox):
    assert _run(sandbox).exit_code == 0
    first = se.read_env(sandbox.project / '.env')
    result = _run(sandbox)
    assert result.exit_code == 0, result.output
    second = se.read_env(sandbox.project / '.env')
    assert second['ETH_PRIVATE_KEY'] == first['ETH_PRIVATE_KEY']
    assert second['BTC_PRIVATE_KEY'] == first['BTC_PRIVATE_KEY']
    assert 'kept from .env' in result.output


def test_yes_defaults_to_the_only_wallet_and_refuses_to_create_one(sandbox):
    result = CliRunner().invoke(
        miner_init.init_command,
        ['--network', 'testnet', '--backing', 'sol', '--chains', '', '--project-dir', str(sandbox.project), '-y'],
    )
    assert result.exit_code == 0, result.output
    assert json.loads(sandbox.config.read_text())['wallet'] == 'w'

    result = _run(sandbox, '--wallet', 'ghost')
    assert result.exit_code != 0
    assert 'ghost' in result.output and 'mnemonic' in result.output


def _run_all_chains(sandbox, *extra):
    """The default path: no --chains, so every spoke family gets a key and nothing is asked."""
    args = [
        '--network', 'testnet', '--wallet', 'w', '--hotkey', 'h', '--backing', 'sol',
        '--solana-rpc', 'http://rpc.test', '--project-dir', str(sandbox.project), '--configure-only', '-y', *extra,
    ]  # fmt: skip
    return CliRunner().invoke(miner_init.init_command, args)


def test_every_family_network_follows_the_environment_even_unkeyed(sandbox):
    assert _run(sandbox, '--chains', 'eth').exit_code == 0
    env = se.read_env(sandbox.project / '.env')
    for fam in se.optional_families():
        if fam.network_chain:
            want = miner_init.ENV_BUNDLES['testnet'].get(miner_init.network_key(fam.network_chain))
            assert env[fam.network_env] == want, fam.prefix
    assert 'ARB_PRIVATE_KEY' not in env  # narrowed by --chains: networks set, no key


def test_an_existing_evm_key_is_shared_to_the_unkeyed_families(sandbox):
    key, addr = se.generate_evm_key()
    (sandbox.project / '.env').write_text(f'ARB_PRIVATE_KEY={key}\n')
    assert _run_all_chains(sandbox).exit_code == 0
    env = se.read_env(sandbox.project / '.env')
    assert all(env[f.key_env] == key for f in se.optional_families() if f.kind == 'evm')


def test_second_miner_on_a_serving_identity_needs_a_typed_word(monkeypatch):
    monkeypatch.setattr(miner_init, 'container_running', lambda: False)
    monkeypatch.setattr(miner_init.click, 'prompt', lambda *a, **k: 'yes')
    started = []
    monkeypatch.setattr(miner_init.subprocess, 'run', lambda *a, **k: started.append(a))
    s = miner_init.Setup(project_dir=os.getcwd())
    assert miner_init._run_container(s, serving=True) is False
    assert not started


def test_fund_survives_a_failed_balance_read(monkeypatch):
    s = miner_init.Setup(project_dir=os.getcwd(), backing='sol', yes=True)

    def boom():
        raise ConnectionError('rpc down')

    assert miner_init._fund(s, boom, {}) is False


def test_rpc_api_key_is_never_echoed(sandbox, monkeypatch):
    keyed = 'https://devnet.helius-rpc.com/?api-key=SECRETKEY0123456789'
    result = _run_all_chains(sandbox, '--solana-rpc', keyed)
    assert result.exit_code == 0, result.output
    assert 'SECRETKEY0123456789' not in result.output
    assert se.read_env(sandbox.project / '.env')['SOLANA_RPC_URL'] == keyed  # stored whole, shown masked

    shown = []
    monkeypatch.setattr(miner_init.click, 'prompt', lambda text, **k: shown.append(text) or k['default'])
    s = miner_init.Setup(project_dir=sandbox.project, env='testnet')
    monkeypatch.setenv('SOLANA_RPC_URL', keyed)
    miner_init.step_rpc(s, None)
    assert shown and 'SECRETKEY0123456789' not in shown[0] and 'api-key=***' in shown[0]
    assert s.env_values['SOLANA_RPC_URL'] == keyed


def test_keypair_outside_the_mount_is_copied_where_the_container_reads_it(sandbox, tmp_path):
    from allways.solana import keys

    outside = tmp_path / 'elsewhere' / 'miner.json'
    outside.parent.mkdir()
    kp = keys.load_or_create(str(outside))
    result = _run_all_chains(sandbox, '--solana-keypair', str(outside))
    assert result.exit_code == 0, result.output
    mounted = sandbox.project / 'data' / 'solana' / 'id.json'
    assert keys.load_keypair(str(mounted)).pubkey() == kp.pubkey()
    assert stat.S_IMODE(mounted.stat().st_mode) == 0o600
    assert 'copy this key' not in result.output


def test_rpc_errors_never_quote_the_keyed_url():
    err = ConnectionError(
        '401 Client Error: Unauthorized for url: https://devnet.helius-rpc.com/?api-key=LEAKME0123456789'
    )
    assert 'LEAKME0123456789' not in miner_init._scrub(err) and 'api-key=***' in miner_init._scrub(err)
