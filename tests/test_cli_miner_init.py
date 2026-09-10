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


def test_key_families_group_assets_by_network_prefix():
    fams = {f.prefix: f for f in se.key_families()}
    assert fams['ETH'].kind == 'evm'
    assert set(fams['ETH'].assets) >= {'eth', 'ethusdc', 'uni', 'qnt', 'paxg'}
    assert fams['ETH'].network_chain.id == 'eth'
    assert fams['BTC'].kind == 'btc'
    assert fams['SOL'].kind == 'solana' and 'solusdc' in fams['SOL'].assets
    assert fams['TAO'].kind == 'tao'
    assert fams['BNB'].assets == ('bnb', 'aster')


def test_optional_families_exclude_sol_and_tao():
    prefixes = {f.prefix for f in se.optional_families()}
    assert 'SOL' not in prefixes and 'TAO' not in prefixes
    assert {'BTC', 'ETH', 'ARB'} <= prefixes


def test_parse_family_list_normalizes_and_rejects_unknown():
    assert se.parse_family_list(' btc, eth,ARB,eth ') == ['BTC', 'ETH', 'ARB']
    assert se.parse_family_list('') == []
    with pytest.raises(ValueError, match='DOGE'):
        se.parse_family_list('btc,doge')


# ─── key generation ──────────────────────────────────────────────────────────


def test_evm_key_roundtrips_to_its_address():
    key, addr = se.generate_evm_key()
    assert key.startswith('0x') and len(key) == 66
    assert se.evm_address(key) == addr
    assert se.evm_address('nope') is None


@pytest.mark.parametrize('network,prefix', [('mainnet', 'bc1q'), ('testnet4', 'tb1q'), ('signet', 'tb1q')])
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


def test_upsert_env_replaces_in_place_and_keeps_trailing_comment():
    out = se.upsert_env(TEMPLATE, {'NETUID': '19', 'BTC_NETWORK': 'testnet4', 'ETH_PRIVATE_KEY': '0xabc'})
    assert 'NETUID=19                            # 7 mainnet | 19 testnet' in out
    assert 'BTC_NETWORK=testnet4                 # mainnet | testnet' in out
    assert 'ETH_PRIVATE_KEY=0xabc\n' in out
    assert out.count('NETUID=') == 1
    assert se.WIZARD_SECTION not in out


def test_upsert_env_uncomments_and_appends_missing_keys():
    out = se.upsert_env(TEMPLATE, {'SOLANA_KEYPAIR_PATH': '/k.json', 'NEW_KEY': 'v', 'SPACED': 'a b'})
    assert '\nSOLANA_KEYPAIR_PATH=/k.json\n' in out
    assert '# SOLANA_KEYPAIR_PATH' not in out
    assert out.endswith(f'{se.WIZARD_SECTION}\nNEW_KEY=v\nSPACED="a b"\n')


def test_write_env_starts_from_template_and_is_private(tmp_path):
    template = tmp_path / '.env.example'
    template.write_text(TEMPLATE)
    env = tmp_path / '.env'
    se.write_env(env, {'WALLET_NAME': 'w'}, template=template)
    assert stat.S_IMODE(env.stat().st_mode) == 0o600
    assert se.read_env(env) == {'NETUID': '7', 'WALLET_NAME': 'w', 'BTC_NETWORK': 'mainnet', 'ETH_PRIVATE_KEY': ''}
    se.write_env(env, {'NETUID': '19'})  # second write edits, never re-templates
    assert se.read_env(env)['WALLET_NAME'] == 'w' and se.read_env(env)['NETUID'] == '19'


def test_list_wallets_reads_coldkeypub_dirs_and_hotkeys(tmp_path):
    (tmp_path / 'a' / 'hotkeys').mkdir(parents=True)
    (tmp_path / 'a' / 'coldkeypub.txt').write_text('{}')
    (tmp_path / 'a' / 'hotkeys' / 'h2').write_text('{}')
    (tmp_path / 'a' / 'hotkeys' / 'h1').write_text('{}')
    (tmp_path / 'a' / 'hotkeys' / 'h1pub.txt').write_text('{}')  # public half, not a hotkey
    (tmp_path / 'b').mkdir()  # no coldkeypub → not a wallet
    (tmp_path / 'c' / 'coldkeypub.txt').parent.mkdir()
    (tmp_path / 'c' / 'coldkeypub.txt').write_text('{}')
    assert se.list_wallets(tmp_path) == {'a': ['h1', 'h2'], 'c': []}
    assert se.list_wallets(tmp_path / 'missing') == {}


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
    monkeypatch.setattr(miner_init, 'run_doctor', lambda project_dir: [(True, 'stub', 'ok')])

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


def test_coldkey_password_flag_lands_in_env(sandbox):
    assert _run(sandbox, '--coldkey-password', 'hunter 2').exit_code == 0
    assert se.read_env(sandbox.project / '.env')['MINER_BITTENSOR_COLDKEY_PASSWORD'] == 'hunter 2'


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


def test_unknown_chain_fails_under_yes(sandbox):
    result = _run(sandbox, '--chains', 'btc,doge')
    assert result.exit_code != 0
    assert 'DOGE' in result.output


def _run_all_chains(sandbox, *extra):
    """The default path: no --chains, so every spoke family gets a key and nothing is asked."""
    args = [
        '--network', 'testnet', '--wallet', 'w', '--hotkey', 'h', '--backing', 'sol',
        '--solana-rpc', 'http://rpc.test', '--project-dir', str(sandbox.project), '--configure-only', '-y', *extra,
    ]  # fmt: skip
    return CliRunner().invoke(miner_init.init_command, args)


def test_default_keys_every_family_with_one_shared_evm_key(sandbox):
    result = _run_all_chains(sandbox)
    assert result.exit_code == 0, result.output
    env = se.read_env(sandbox.project / '.env')
    evm = [f for f in se.optional_families() if f.kind == 'evm']
    keys = {env[f.key_env] for f in evm}
    assert len(keys) == 1 and se.evm_address(keys.pop()) in result.output  # one address to fund, printed once
    assert se.btc_address(env['BTC_PRIVATE_KEY'], env['BTC_NETWORK']) is not None
    assert 'Chains (comma' not in result.output


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


def test_container_is_informational_until_a_purse_serves(monkeypatch):
    monkeypatch.setattr(miner_init, 'container_running', lambda: False)
    assert miner_init._container_row(serving=False)[0] is None
    assert miner_init._container_row(serving=True)[0] is False
    monkeypatch.setattr(miner_init, 'container_running', lambda: True)
    assert miner_init._container_row(serving=True)[0] is True


def test_second_miner_on_a_serving_identity_needs_a_typed_word(monkeypatch):
    monkeypatch.setattr(miner_init, 'container_running', lambda: False)
    monkeypatch.setattr(miner_init.click, 'prompt', lambda *a, **k: 'yes')
    started = []
    monkeypatch.setattr(miner_init.subprocess, 'run', lambda *a, **k: started.append(a))
    s = miner_init.Setup(project_dir=os.getcwd())
    assert miner_init._run_container(s, serving=True) is False
    assert not started


def test_typed_confirm_requires_the_exact_word(monkeypatch):
    s = miner_init.Setup(project_dir=os.getcwd())
    monkeypatch.setattr(miner_init.click, 'prompt', lambda *a, **k: 'nope')
    assert miner_init._typed_confirm(s, 'bind', 'why') is False
    monkeypatch.setattr(miner_init.click, 'prompt', lambda *a, **k: ' bind ')
    assert miner_init._typed_confirm(s, 'bind', 'why') is True
    s.yes = True
    assert miner_init._typed_confirm(s, 'bind', 'why') is True


def test_activate_retries_across_the_metagraph_sync_window(monkeypatch):
    """After a fresh registration, activation keeps retrying (bounded) until validators resync."""
    s = miner_init.Setup(project_dir=os.getcwd(), registered_now=True)
    calls = []

    class Ctx:
        def invoke(self, cmd, **kw):
            calls.append(kw['backing'])
            if len(calls) < 3:
                raise SystemExit(1)

    slept = []
    monkeypatch.setattr(miner_init.time, 'sleep', lambda secs: slept.append(secs))
    assert miner_init._activate_with_retry(Ctx(), s, 'sol') is True
    assert calls == ['sol', 'sol', 'sol'] and len(slept) == 2

    # not freshly registered → one attempt only
    s2 = miner_init.Setup(project_dir=os.getcwd())
    calls.clear()
    assert miner_init._activate_with_retry(Ctx(), s2, 'sol') is False
    assert calls == ['sol']


def test_fund_rechecks_on_enter_and_stops_on_q(monkeypatch):
    s = miner_init.Setup(project_dir=os.getcwd(), backing='sol')
    checks = iter([[(False, 'Solana keypair', 'short')], [(True, 'Solana keypair', 'ok')]])
    answers = iter([''])
    monkeypatch.setattr(miner_init.click, 'prompt', lambda *a, **k: next(answers))
    assert miner_init._fund(s, lambda: next(checks), {}) is True  # Enter re-checked, now funded

    monkeypatch.setattr(miner_init.click, 'prompt', lambda *a, **k: 'q')
    assert miner_init._fund(s, lambda: [(False, 'coldkey', 'short')], {}) is False


def test_fund_under_yes_stops_instead_of_waiting(monkeypatch):
    s = miner_init.Setup(project_dir=os.getcwd(), backing='sol', yes=True)
    monkeypatch.setattr(miner_init.click, 'prompt', lambda *a, **k: pytest.fail('must not prompt under -y'))
    assert miner_init._fund(s, lambda: [(False, 'Solana keypair', 'short')], {}) is False


def test_capacity_note_names_max_swap_and_full_capacity_per_backing(capsys):
    from allways.constants import required_collateral

    bounds = {'sol': (100_000_000, 5_000_000_000), 'tao': (50_000_000, 2_000_000_000)}
    with miner_init.console.capture() as cap:
        miner_init._capacity_note(('sol', 'tao'), bounds)
    out = cap.get()
    assert 'max swap 5.0000 SOL' in out and 'max swap 2.0000 TAO' in out
    assert f'{required_collateral(5_000_000_000) / 1e9:.4f} SOL' in out
    assert 'More emissions' in out


def test_preflight_gate_ignores_what_go_live_does_itself():
    for label in (
        'SOL balance',
        'SOL collateral',
        'hotkey binding',
        'registered on SN19',
        'TAO bond',
        'sol purse',
        'miner container',
    ):
        assert miner_init._go_live_row(label), label
    for label in ('solana rpc + program', 'coldkey', 'EVM key', 'docker', 'WALLET_PATH'):
        assert not miner_init._go_live_row(label), label


def test_finish_states_the_activity_window_and_strike_rule(sandbox):
    result = _run_all_chains(sandbox)
    assert result.exit_code == 0, result.output
    out = ' '.join(result.output.split())  # rich wraps to the terminal width
    assert '12 hours' in out and '3 failed swaps' in out


def test_fund_survives_a_failed_balance_read(monkeypatch):
    s = miner_init.Setup(project_dir=os.getcwd(), backing='sol', yes=True)

    def boom():
        raise ConnectionError('rpc down')

    assert miner_init._fund(s, boom, {}) is False


def test_reusing_an_existing_evm_key_is_reported_as_shared_not_generated(sandbox):
    key, _ = se.generate_evm_key()
    (sandbox.project / '.env').write_text(f'ARB_PRIVATE_KEY={key}\n')
    result = _run_all_chains(sandbox)
    assert result.exit_code == 0, result.output
    out = ' '.join(result.output.split())
    assert 'your ARB key now also covers: eth' in out
    assert 'generated: btc' in out  # only the BTC key was minted


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
