"""alw miner init — step-by-step miner onboarding; alw doctor — the preflight on its own.

Every step reads its "done" state from disk or chain (never a local flag), so re-running resumes
where the operator left off, and every prompt has a flag so the whole thing can run unattended.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import click

from allways.cli import ui
from allways.cli.help import StyledCommand
from allways.cli.swap_commands import setup_env as se
from allways.cli.swap_commands.helpers import (
    ALLWAYS_DIR,
    CONFIG_FILE,
    ENV_BUNDLES,
    SOLANA_NETWORKS,
    apply_chain_network_env,
    console,
    fail,
    from_lamports,
    from_rao,
    get_effective_config,
    get_solana_cli_context,
    load_cli_config,
    network_key,
    purse_states,
    resolve_solana_keypair_path,
)
from allways.cli.swap_commands.swap_intake import floors_from_config
from allways.constants import TAO_HUB_VAULT_ADDRESSES
from allways.solana.pdas import BACKING_CHAIN_SOL, BACKING_CHAIN_TAO

COMPOSE_FILE = 'docker-compose.miner.yml'
IMAGE_TAGS = {'testnet': 'test', 'mainnet': 'latest'}  # entrius/allways:<tag>, read by the compose file
CONTAINER = 'aw-miner'
# Validators learn of a new registration on their next metagraph sync (epoch_length 150 blocks ≈ 30 min),
# so an activation right after registering must be retried across that window.
ACTIVATE_RETRY_SECS = 180
ACTIVATE_WAIT_AFTER_REGISTER_MINS = 35
Check = Tuple[Optional[bool], str, str]


@dataclass
class Setup:
    project_dir: Path
    yes: bool = False
    env: str = ''  # testnet | mainnet
    wallet: str = ''
    hotkey: str = ''
    backing: str = ''  # sol | tao | both
    families: List[str] = field(default_factory=list)
    solana_keypair: Optional[Path] = None
    env_values: Dict[str, str] = field(default_factory=dict)
    addresses: Dict[str, str] = field(default_factory=dict)
    registered_now: bool = False
    activate_wait_mins: Optional[int] = None  # None = auto: wait only after a fresh registration

    @property
    def env_path(self) -> Path:
        return self.project_dir / '.env'

    @property
    def bundle(self) -> dict:
        return ENV_BUNDLES[self.env]

    @property
    def backings(self) -> Tuple[str, ...]:
        return (BACKING_CHAIN_SOL, BACKING_CHAIN_TAO) if self.backing == 'both' else (self.backing,)


def _save_config(updates: dict) -> dict:
    ALLWAYS_DIR.mkdir(parents=True, exist_ok=True)
    config = load_cli_config()
    config.update(updates)
    CONFIG_FILE.write_text(json.dumps(config, indent=2))
    return config


def _bt_wallet(name: str, hotkey: str):
    import bittensor as bt

    return bt.Wallet(name=name, hotkey=hotkey, path=str(se.wallets_root()))


def _pub_ss58(wallet, which: str) -> Optional[str]:
    """ss58 off the public key file only — never unlocks anything."""
    try:
        return wallet.coldkeypub.ss58_address if which == 'coldkey' else wallet.hotkey.ss58_address
    except Exception:
        return None


def _prompt(s: Setup, text: str, default=None, **kw):
    """Interactive prompt, or the default under --yes (a missing default is then an error)."""
    if s.yes:
        if default is None or default == '':
            fail(f'--yes needs a value for "{text}" — pass the matching flag.')
        return default
    return click.prompt(text, default=default, **kw)


def _typed_confirm(s: Setup, word: str, why: str) -> bool:
    """Irreversible actions take a typed word, not a y/n — unless --yes was passed explicitly."""
    if s.yes:
        return True
    console.print(f'  [yellow]{why}[/yellow]')
    return click.prompt(f'  Type "{word}" to continue', default='', show_default=False).strip() == word


# ─── Steps 1–6: local configuration ─────────────────────────────────────────


def step_network(s: Setup, preset: Optional[str]) -> None:
    ui.draw_step(
        console,
        1,
        'Choose a network',
        'testnet = SN19 + Solana devnet, to rehearse with play money. mainnet = SN7, real funds.',
    )
    existing = load_cli_config()
    current = {'19': 'testnet', '7': 'mainnet'}.get(str(existing.get('netuid', '')))
    s.env = preset or _prompt(s, 'Network', default=current or 'testnet', type=click.Choice(list(ENV_BUNDLES)))
    bundle = dict(s.bundle)
    bundle['vault-address'] = TAO_HUB_VAULT_ADDRESSES[bundle['network']]
    _save_config(bundle)
    apply_chain_network_env(get_effective_config())
    s.env_values.update(
        NETUID=bundle['netuid'], SUBTENSOR_NETWORK=bundle['network'], ALLWAYS_IMAGE_TAG=IMAGE_TAGS[s.env]
    )
    ui.draw_done(
        console,
        f'{s.env}: netuid {bundle["netuid"]} · subtensor {bundle["network"]} · solana {bundle["solana-network"]}',
    )


def _create_wallet_part(name: str, hotkey: str, which: str) -> None:
    """bittensor-wallet prints the mnemonic and (for a coldkey) asks for a password itself."""
    wallet = _bt_wallet(name, hotkey)
    console.print(f'  [bold]Creating {which} — write the mnemonic down; it is shown once.[/bold]')
    if which == 'coldkey':
        wallet.create_new_coldkey(n_words=12, use_password=True, overwrite=False)
    else:
        wallet.create_new_hotkey(n_words=12, use_password=False, overwrite=False)


def step_wallet(s: Setup, wallet: Optional[str], hotkey: Optional[str]) -> None:
    ui.draw_step(
        console,
        2,
        'Bittensor wallet',
        'The hotkey is your subnet identity; the coldkey registers it and signs TAO payouts.',
    )
    wallets = se.list_wallets()
    existing = load_cli_config()
    if wallets:
        ui.draw_kv(console, ((n, ', '.join(h) or '[dim]no hotkeys[/dim]') for n, h in wallets.items()))
        console.print('  [dim]Name an existing coldkey, or "new" to create one.[/dim]')
    default_wallet = existing.get('wallet') if existing.get('wallet') in wallets else next(iter(wallets), 'new')
    name = wallet or _prompt(s, 'Coldkey', default=default_wallet)
    if name == 'new':
        name = _prompt(s, 'New coldkey name', default='miner')
    if name not in wallets:
        if s.yes:
            fail(
                f'No coldkey named {name!r} under {se.wallets_root()}; creating one needs a mnemonic + password prompt.'
            )
        _create_wallet_part(name, 'default', 'coldkey')
        wallets[name] = []

    hotkeys = wallets.get(name, [])
    default_hotkey = existing.get('hotkey') if existing.get('hotkey') in hotkeys else (hotkeys[0] if hotkeys else 'new')
    hk = hotkey or _prompt(s, 'Hotkey', default=default_hotkey)
    if hk == 'new':
        hk = _prompt(s, 'New hotkey name', default='default')
    if hk not in hotkeys:
        _create_wallet_part(name, hk, 'hotkey')

    s.wallet, s.hotkey = name, hk
    _save_config({'wallet': name, 'hotkey': hk})
    s.env_values.update(WALLET_NAME=name, HOTKEY_NAME=hk, WALLET_PATH=str(se.wallets_root()))
    w = _bt_wallet(name, hk)
    ui.draw_kv(console, [('coldkey', _pub_ss58(w, 'coldkey') or '?'), ('hotkey', _pub_ss58(w, 'hotkey') or '?')])


def step_backing(s: Setup, backing: Optional[str], chains: Optional[str]) -> None:
    ui.draw_step(
        console,
        3,
        'Backing',
        'SOL purse: a failed delivery refunds the user instantly in SOL. TAO bond: reimburses in TAO after timeout.',
    )
    s.backing = backing or _prompt(s, 'Backing', default='sol', type=click.Choice(['sol', 'tao', 'both']))
    # Every spoke gets a key: an unfunded key costs nothing, and what you quote is decided later by
    # `alw miner post` and by which addresses you fund. --chains narrows it for operators who want fewer.
    if chains is None:
        s.families = [f.prefix for f in se.optional_families()]
    else:
        try:
            s.families = se.parse_family_list(chains)
        except ValueError as e:
            fail(str(e))
    ui.draw_done(console, f'{s.backing} backing')


def _solana_keypair(s: Setup, flag: Optional[str]) -> None:
    from allways.solana import keys

    default = s.project_dir / 'data' / 'solana' / 'id.json'
    current = load_cli_config().get('solana-keypair')
    raw = flag or _prompt(s, 'Solana keypair path', default=current or str(default))
    path = Path(raw).expanduser().resolve()
    created = not path.exists()
    kp = keys.load_or_create(str(path))
    if created:
        os.chmod(path, 0o600)
    s.solana_keypair = path
    _save_config({'solana-keypair': str(path)})
    ui.draw_done(console, f'{"generated" if created else "using"} Solana keypair {path}')
    if path.parent != default.parent.resolve():
        ui.draw_warn(console, f'docker mounts ./data/solana as the miner keypair dir — copy this key to {default}')
    s.addresses['SOL'] = str(kp.pubkey())


def _family_network(s: Setup, fam: se.KeyFamily) -> str:
    return s.bundle.get(network_key(fam.network_chain)) if fam.network_chain else 'mainnet'


def _usable_key(s: Setup, fam: se.KeyFamily, env: Dict[str, str]) -> Optional[str]:
    """The family's key from .env if it derives an address; a broken one is replaced only on a yes."""
    existing = env.get(fam.key_env)
    if not existing:
        return None
    derive = (lambda k: se.btc_address(k, _family_network(s, fam))) if fam.kind == 'btc' else se.evm_address
    if derive(existing) is not None:
        return existing
    ui.draw_warn(console, f'{fam.key_env} in .env is not a usable key')
    if _prompt(s, f'  Replace {fam.key_env} with a fresh key?', default=False, type=bool):
        return None
    return existing


def step_keys(s: Setup, solana_keypair: Optional[str]) -> None:
    ui.draw_step(
        console,
        4,
        'Signing keys',
        'Generated keys are written to .env only and never printed. Fund the addresses shown at the end.',
    )
    _solana_keypair(s, solana_keypair)
    coldkey = _pub_ss58(_bt_wallet(s.wallet, s.hotkey), 'coldkey')
    s.addresses['TAO'] = coldkey or '?'
    env = se.read_env(s.env_path)
    families = [f for f in se.optional_families() if f.prefix in s.families]
    # Every family's network follows the chosen environment, keyed or not — a testnet .env never
    # carries a mainnet spoke network waiting for someone to paste a key next to it.
    for fam in se.optional_families():
        if fam.network_chain:
            s.env_values[fam.network_env] = _family_network(s, fam)
    # One EVM key serves every EVM chain (nonces are per chain, and the keys share one .env anyway),
    # so the operator funds one address. A family that already has its own key keeps it.
    existing = {f.prefix: _usable_key(s, f, env) for f in families}
    evm_key = next((existing[f.prefix] for f in families if f.kind == 'evm' and existing[f.prefix]), None)
    generated: List[str] = []
    if evm_key is None and any(f.kind == 'evm' for f in families):
        evm_key, _ = se.generate_evm_key()
    evm_groups: Dict[str, List[str]] = {}  # address → families, so a shared key prints once
    for fam in families:
        key = existing[fam.prefix]
        if key is None:
            key = evm_key if fam.kind == 'evm' else se.generate_btc_key(_family_network(s, fam))[0]
            s.env_values[fam.key_env] = key
            generated.append(fam.prefix.lower())
        if fam.kind == 'btc':
            s.addresses['BTC'] = se.btc_address(key, _family_network(s, fam)) or '?'
        else:
            evm_groups.setdefault(se.evm_address(key) or '?', []).append(fam.prefix.lower())
    for i, (addr, prefixes) in enumerate(evm_groups.items()):
        s.addresses['EVM' if i == 0 else f'EVM {i + 1}'] = f'{addr}  ({", ".join(prefixes)})'
    kept = [f.prefix.lower() for f in families if existing[f.prefix]]
    if generated:
        ui.draw_done(console, f'generated spoke keys: {", ".join(generated)}')
    if kept:
        ui.draw_done(console, f'kept from .env: {", ".join(kept)}')
    console.print()
    ui.draw_kv(console, s.addresses.items())
    console.print(
        '  [dim]Fund only the chains you will quote: each spoke address needs the asset it pays out plus gas;'
        ' the Solana keypair covers fees. Unfunded keys cost nothing.[/dim]'
    )


def step_rpc(s: Setup, solana_rpc: Optional[str]) -> None:
    ui.draw_step(
        console,
        5,
        'Solana RPC',
        'The miner polls the Allways program here every 12 s and sends its fulfillment transactions through it.',
        'Public endpoints throttle it. Use a keyed one (Helius, Triton, QuickNode…): a free Helius key is'
        " enough to rehearse on testnet; a 24/7 mainnet miner outgrows the free tier's monthly credits.",
    )
    current = se.read_env(s.env_path).get('SOLANA_RPC_URL') or os.environ.get('SOLANA_RPC_URL')
    default = current or SOLANA_NETWORKS[s.bundle['solana-network']]
    url = solana_rpc or _prompt(s, 'Solana RPC URL', default=default)
    s.env_values['SOLANA_RPC_URL'] = url
    os.environ['SOLANA_RPC_URL'] = url
    ui.draw_done(console, f'Solana RPC {url}')
    console.print(
        '  [dim]Spoke RPCs ({PREFIX}_RPC_URLS, BTC_ESPLORA_URLS) keep public defaults — edit .env to add keyed ones.[/dim]'
    )


def _coldkey_encrypted(s: Setup) -> bool:
    """True unless the coldkey keyfile is readable and plainly unencrypted (then there's nothing to store)."""
    try:
        return bool(_bt_wallet(s.wallet, s.hotkey).coldkey_file.is_encrypted())
    except Exception:
        return True


def step_write_env(s: Setup, coldkey_password: Optional[str]) -> None:
    ui.draw_step(console, 6, 'Write configuration', f'{s.env_path} feeds docker compose, the miner, and alw.')
    s.env_values.setdefault('PORT', '8091')
    s.env_values.setdefault('LOG_LEVEL', 'info')
    if coldkey_password is None and not s.yes and _coldkey_encrypted(s):
        # Whatever the backing: the miner pays TAO out of the coldkey on every TAO-delivering fill, so it
        # unlocks it at boot — and the container runs detached, with no terminal to answer a prompt.
        console.print(
            '  [dim]The miner signs TAO payouts with your coldkey and unlocks it at boot. The container runs'
            ' detached and cannot prompt, so an encrypted coldkey needs its password stored here.[/dim]'
        )
        coldkey_password = click.prompt(
            'Coldkey password (stored in .env, mode 600)', default='', hide_input=True, show_default=False
        )
        if not coldkey_password:
            ui.draw_warn(
                console,
                'No password stored: the miner exits at boot until MINER_BITTENSOR_COLDKEY_PASSWORD is set in .env.',
            )
    if coldkey_password:
        s.env_values['MINER_BITTENSOR_COLDKEY_PASSWORD'] = coldkey_password
    se.write_env(s.env_path, s.env_values, template=s.project_dir / '.env.example')
    ui.draw_done(console, f'wrote {s.env_path} (mode 600): {", ".join(s.env_values)}')
    ui.draw_done(console, f'wrote {CONFIG_FILE}')


# ─── Step 7: preflight (alw doctor) ─────────────────────────────────────────


def _container_row(serving: bool) -> Check:
    """The container is a post-go-live fact: before any purse serves it is not a failure (go-live starts it);
    once one serves, a missing miner means reservations on it time out."""
    if container_running():
        return (True, 'miner container', 'running')
    if serving:
        return (
            False,
            'miner container',
            'not running here, yet a purse is serving — its swaps time out unless your miner runs elsewhere',
        )
    return (None, 'miner container', 'not started yet (go-live starts it)')


def _check_docker(project_dir: Path, env: Dict[str, str], serving: bool = False) -> List[Check]:
    rows: List[Check] = []
    docker = shutil.which('docker')
    rows.append((bool(docker), 'docker', docker or 'not on PATH — install Docker Engine + compose plugin'))
    compose = project_dir / COMPOSE_FILE
    rows.append((compose.is_file(), COMPOSE_FILE, str(compose) if compose.is_file() else f'not in {project_dir}'))
    env_file = project_dir / '.env'
    rows.append((env_file.is_file(), '.env', str(env_file) if env_file.is_file() else 'missing — run alw miner init'))
    missing = [
        k for k in ('NETUID', 'WALLET_NAME', 'HOTKEY_NAME', 'SUBTENSOR_NETWORK', 'PORT', 'LOG_LEVEL') if not env.get(k)
    ]
    rows.append((not missing, 'required vars', 'all set' if not missing else f'missing {", ".join(missing)}'))
    wp = env.get('WALLET_PATH', '')
    ok = bool(wp) and Path(wp).expanduser().is_dir() and not wp.startswith('~')
    rows.append(
        (
            ok,
            'WALLET_PATH',
            wp if ok else f'{wp or "unset"} — must be an absolute existing dir (compose does not expand ~)',
        )
    )
    rows.append(_container_row(serving))
    return rows


def _check_keys(env: Dict[str, str], config: dict) -> List[Check]:
    rows: List[Check] = []
    path = resolve_solana_keypair_path(config)
    try:
        from allways.solana import keys

        rows.append((True, 'solana keypair', f'{keys.load_keypair(path).pubkey()}  {path}'))
    except Exception as e:
        rows.append((False, 'solana keypair', f'{path}: {e}'))
    by_address: Dict[str, List[str]] = {}  # one row per distinct spoke address, however many chains share it
    unset: List[str] = []
    for fam in se.optional_families():
        key = env.get(fam.key_env)
        if not key:
            unset.append(fam.prefix.lower())
            continue
        net = env.get(fam.network_env) or config.get(network_key(fam.network_chain), '') if fam.network_chain else ''
        addr = se.btc_address(key, net or 'mainnet') if fam.kind == 'btc' else se.evm_address(key)
        if addr is None:
            rows.append((False, fam.key_env, 'unusable key'))
            continue
        by_address.setdefault(addr, []).append(f'{fam.prefix.lower()} ({net})' if net else fam.prefix.lower())
    for addr, fams in by_address.items():
        label = 'BTC key' if addr and fams[0].startswith('btc') else 'EVM key'
        rows.append((True, label, f'{addr}  {", ".join(fams)}'))
    if unset:
        rows.append((None, 'spoke keys not set', f'{", ".join(unset)} (fine unless you quote them)'))
    return rows


def _check_wallet(config: dict) -> Tuple[List[Check], Optional[str]]:
    name, hk = config.get('wallet'), config.get('hotkey')
    if not name or not hk:
        return [(False, 'bittensor wallet', 'wallet/hotkey not configured — alw config set wallet/hotkey')], None
    w = _bt_wallet(name, hk)
    cold, hot = _pub_ss58(w, 'coldkey'), _pub_ss58(w, 'hotkey')
    rows: List[Check] = [
        (cold is not None, 'coldkey', cold or f'{name}: coldkeypub.txt unreadable under {se.wallets_root()}'),
        (hot is not None, 'hotkey', hot or f'{name}/{hk}: hotkey file unreadable'),
    ]
    return rows, hot


def _check_chain(config: dict, hotkey_ss58: Optional[str]) -> List[Check]:
    """Solana program + subtensor state. Every read is best-effort: one dead RPC yields one ✗ row."""
    import bittensor as bt

    from allways.solana.rpc import assert_cluster_safe, redact_rpc_url

    rows: List[Check] = []
    netuid = int(config.get('netuid', 7))
    try:
        _, client = get_solana_cli_context(need_keypair=False)
        cfg = client.get_config()
        assert_cluster_safe(client.rpc, client.program_id, netuid, role='miner')
        rows.append((cfg is not None, 'solana rpc + program', redact_rpc_url(client.rpc.url)))
    except Exception as e:
        return rows + [(False, 'solana rpc + program', str(e))]
    if cfg is None:
        return rows
    floors = floors_from_config(cfg)
    try:
        from allways.solana import keys

        pubkey = keys.load_keypair(resolve_solana_keypair_path(config)).pubkey()
    except Exception:
        return rows
    bal = client.rpc.get_account_lamports(pubkey) or 0
    need = floors[BACKING_CHAIN_SOL]
    rows.append(
        (bal > 0, 'SOL balance', f'{from_lamports(bal):.4f} SOL  (min collateral {from_lamports(need):.4f} + fees)')
    )
    ms = client.get_miner_state(pubkey)
    have = client.get_collateral_lamports(pubkey) or 0
    rows.append(
        (
            have >= need,
            'SOL collateral',
            f'{from_lamports(have):.4f} / {from_lamports(need):.4f} SOL' + ('' if ms else '  (no miner state yet)'),
        )
    )
    binding = client.get_binding(pubkey)
    if binding is None:
        rows.append((False, 'hotkey binding', 'not bound — alw bind-hotkey (AFTER collateral, BEFORE register)'))
    else:
        from allways.cli.swap_commands.helpers import hotkey_bytes_to_ss58

        bound = hotkey_bytes_to_ss58(bytes(binding.hotkey))
        rows.append(
            (bound == hotkey_ss58, 'hotkey binding', bound + ('' if bound == hotkey_ss58 else '  ≠ configured hotkey!'))
        )
    if ms is not None:
        for st in purse_states(client, pubkey, ms, cfg):
            rows.append((st.lit, f'{st.backing} purse', 'serving' if st.lit else 'not serving'))
    if hotkey_ss58:
        try:
            sub = bt.Subtensor(network=config.get('network', 'finney'))
            uid = sub.get_uid_for_hotkey_on_subnet(hotkey_ss58, netuid)
            rows.append(
                (uid is not None, f'registered on SN{netuid}', f'uid {uid}' if uid is not None else 'not registered')
            )
            if config.get('vault-address'):
                from allways.vault import BondVaultClient

                vault = BondVaultClient.from_config(sub, config)
                bond = vault.get_collateral(hotkey_ss58) or 0
                lock = vault.get_lock_state(hotkey_ss58)
                locked = bool(lock and lock[0])
                rows.append(
                    (
                        None if bond == 0 else locked,
                        'TAO bond',
                        f'{from_rao(bond):.4f} TAO  {"locked" if locked else "unlocked"}  (floor {from_rao(floors[BACKING_CHAIN_TAO]):.4f})',
                    )
                )
        except Exception as e:
            rows.append((False, 'subtensor', str(e)))
    return rows


def container_running() -> bool:
    if not shutil.which('docker'):
        return False
    try:
        out = subprocess.run(
            ['docker', 'ps', '--filter', f'name=^{CONTAINER}$', '--format', '{{.Status}}'],
            capture_output=True,
            text=True,
            timeout=15,
        )
        return out.stdout.strip().startswith('Up')
    except Exception:
        return False


def run_doctor(project_dir: Path) -> List[Check]:
    config = get_effective_config()
    env = se.read_env(project_dir / '.env')
    rows: List[Check] = [
        (
            bool(config.get('netuid')),
            'alw config',
            f'netuid {config.get("netuid", "?")} · {config.get("network", "finney")}',
        ),
    ]
    wallet_rows, hot = _check_wallet(config)
    rows += wallet_rows
    rows += _check_keys(env, config)
    chain_rows = _check_chain(config, hot)
    rows += chain_rows
    serving = any(ok for ok, label, _ in chain_rows if label.endswith(' purse'))
    rows += _check_docker(project_dir, env, serving)
    return rows


def step_doctor(project_dir: Path, number: int = 7) -> List[Check]:
    ui.draw_step(console, number, 'Preflight', 'Every check reads live state; re-run any time with `alw doctor`.')
    with console.status('[cyan]Checking...[/cyan]', spinner='dots'):
        rows = run_doctor(project_dir)
    console.print(ui.check_table(rows))
    return rows


# ─── Step 8: go live ─────────────────────────────────────────────────────────


def _resume_hint(cmd: str) -> None:
    console.print(f'\n  [dim]Fix the above, then re-run `alw miner init` (it resumes) or `{cmd}` directly.[/dim]')


def _deposit(ctx, s: Setup, client, pubkey, floors) -> bool:
    from allways.cli.swap_commands.collateral import collateral_deposit

    need = floors[BACKING_CHAIN_SOL]
    have = client.get_collateral_lamports(pubkey) or 0
    what = (
        'SOL collateral'
        if BACKING_CHAIN_SOL in s.backings
        else 'identity deposit (TAO-only miners still stake the minimum once)'
    )
    ui.draw_step(console, 8, f'Deposit {what}', 'Binding requires a live stake, so this comes first.')
    if have >= need and client.get_miner_state(pubkey) is not None:
        ui.draw_done(console, f'{from_lamports(have):.4f} SOL already posted (floor {from_lamports(need):.4f})')
        return True
    shortfall = from_lamports(max(need - have, 0))
    amount = float(_prompt(s, 'Amount to deposit (SOL)', default=f'{shortfall:.4f}' if shortfall else '1.0'))
    try:
        ctx.invoke(collateral_deposit, amount=amount, yes=True)
    except SystemExit:
        _resume_hint(f'alw collateral deposit --amount {amount}')
        return False
    return True


def _bind(ctx, s: Setup, client, pubkey, wallet) -> bool:
    from allways.cli.swap_commands.bind import bind_hotkey_command
    from allways.cli.swap_commands.helpers import hotkey_bytes_to_ss58

    ui.draw_step(
        console,
        9,
        'Bind hotkey ↔ Solana pubkey',
        'Permanent in both directions. Bind BEFORE registering so nobody can squat your hotkey.',
    )
    hot = wallet.hotkey.ss58_address
    binding = client.get_binding(pubkey)
    if binding is not None:
        bound = hotkey_bytes_to_ss58(bytes(binding.hotkey))
        if bound == hot:
            ui.draw_done(console, f'already bound to {hot}')
            return True
        fail(
            f'This Solana keypair is permanently bound to {bound}, not {hot}. Use that hotkey, or start over with a fresh keypair.'
        )
    other = client.get_hotkey_binding(bytes(wallet.hotkey.public_key))
    if other is not None:
        fail(
            f'Hotkey {hot} is already bound to Solana pubkey {other.miner}. Point solana-keypair at that key, or use another hotkey.'
        )
    if not _typed_confirm(s, 'bind', f'Binding {hot} → {pubkey} cannot be undone.'):
        console.print('  [yellow]Skipped.[/yellow]')
        return False
    try:
        ctx.invoke(bind_hotkey_command, yes=True)
    except SystemExit:
        _resume_hint('alw bind-hotkey')
        return False
    return True


def _unlock_coldkey(s: Setup, wallet) -> bool:
    """Cache the coldkey password the way the miner does, so burned_register never prompts under -y."""
    if not wallet.coldkey_file.is_encrypted():
        return True
    password = s.env_values.get('MINER_BITTENSOR_COLDKEY_PASSWORD') or os.environ.get(
        'MINER_BITTENSOR_COLDKEY_PASSWORD'
    )
    if not password and s.yes:
        console.print(
            '  [red]Coldkey is encrypted; pass --coldkey-password (or unset -y) so registration can sign.[/red]'
        )
        return False
    if password:
        wallet.coldkey_file.save_password_to_env(password)
    try:
        wallet.unlock_coldkey()
    except Exception as e:
        console.print(f'  [red]Could not unlock coldkey: {e}[/red]')
        return False
    return True


def _register(s: Setup, subtensor, wallet, netuid: int) -> bool:
    hot = wallet.hotkey.ss58_address
    ui.draw_step(
        console,
        10,
        f'Register on subnet {netuid}',
        'Burns the registration cost from the coldkey. An encrypted coldkey asks for its password here.',
    )
    uid = subtensor.get_uid_for_hotkey_on_subnet(hot, netuid)
    if uid is not None:
        ui.draw_done(console, f'already registered as uid {uid}')
        return True
    try:
        console.print(f'  Registration cost: [bold]{subtensor.recycle(netuid)}[/bold]')
    except Exception:
        pass
    manual = f'btcli subnet register --netuid {netuid} --wallet.name {s.wallet} --wallet.hotkey {s.hotkey} --network {s.bundle["network"]}'
    if not s.yes and not click.confirm('  Register now?', default=True):
        _resume_hint(manual)
        return False
    if not _unlock_coldkey(s, wallet):
        _resume_hint(manual)
        return False
    resp = subtensor.burned_register(wallet, netuid)
    ok = bool(getattr(resp, 'success', resp))
    if not ok:
        console.print(f'  [red]{getattr(resp, "message", "registration failed")}[/red]')
        _resume_hint(manual)
        return False
    uid = subtensor.get_uid_for_hotkey_on_subnet(hot, netuid)
    s.registered_now = True
    ui.draw_done(console, f'registered as uid {uid}')
    return True


def _bond(ctx, s: Setup, subtensor, config, wallet, floors) -> bool:
    from allways.cli.swap_commands.vault import vault_deposit, vault_lock
    from allways.vault import BondVaultClient

    ui.draw_step(
        console,
        11,
        'Post and lock the TAO bond',
        'The hotkey signs vault calls. Locking is not self-service to undo: exit is deactivate → settle → unlock.',
    )
    hot = wallet.hotkey.ss58_address
    vault = BondVaultClient.from_config(subtensor, config)
    have = vault.get_collateral(hot) or 0
    need = floors[BACKING_CHAIN_TAO]
    if have < need:
        amount = float(_prompt(s, 'Bond amount (TAO)', default=f'{from_rao(need - have):.4f}'))
        try:
            ctx.invoke(vault_deposit, amount=amount)
        except SystemExit:
            _resume_hint(f'alw vault deposit --amount {amount}')
            return False
    else:
        ui.draw_done(console, f'{from_rao(have):.4f} TAO already bonded (floor {from_rao(need):.4f})')
    lock = vault.get_lock_state(hot)
    if lock and lock[0]:
        ui.draw_done(console, 'bond already locked')
        return True
    if not _typed_confirm(
        s, 'lock', 'Locking enters service; validators unlock you only after you deactivate and settle.'
    ):
        console.print('  [yellow]Skipped.[/yellow]')
        return False
    try:
        ctx.invoke(vault_lock)
    except SystemExit:
        _resume_hint('alw vault lock')
        return False
    console.print(
        '  [dim]Validators mirror the lock to Solana on their cadence — TAO activation may need a minute.[/dim]'
    )
    return True


def _run_container(s: Setup, serving: bool = False) -> bool:
    ui.draw_step(
        console,
        12,
        'Start the miner',
        'Start BEFORE activating: an active purse that nobody serves gets reserved, times out, and is slashed.',
    )
    if container_running():
        ui.draw_done(console, f'{CONTAINER} is running')
        return True
    if serving and not _typed_confirm(
        s,
        'start',
        'A purse is already serving but no miner runs here, so this identity is probably mining elsewhere.'
        ' Two miners on one identity both fulfill the same swaps.',
    ):
        console.print('  [yellow]Skipped — stop the other miner first, or run this on its box.[/yellow]')
        return False
    cmd = ['docker', 'compose', '-f', COMPOSE_FILE, 'up', '-d']
    console.print(f'  $ {" ".join(cmd)}')
    if not s.yes and not click.confirm('  Start it now?', default=True):
        _resume_hint(' '.join(cmd))
        return False
    try:
        subprocess.run(cmd, cwd=s.project_dir, check=True)
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        console.print(f'  [red]{e}[/red]')
        _resume_hint(' '.join(cmd))
        return False
    with console.status('[cyan]Waiting for the container...[/cyan]', spinner='dots'):
        for _ in range(10):
            time.sleep(2)
            if container_running():
                break
    if not container_running():
        console.print(f'  [red]{CONTAINER} is not up — check `docker logs {CONTAINER}`.[/red]')
        return False
    ui.draw_done(console, f'{CONTAINER} up · logs: docker logs -f {CONTAINER}')
    return True


def _activate_with_retry(ctx, s: Setup, backing: str) -> bool:
    """Broadcast activation; after a fresh registration keep retrying until validators have resynced."""
    from allways.cli.swap_commands.miner_commands import miner_activate

    wait_mins = s.activate_wait_mins
    if wait_mins is None:
        wait_mins = ACTIVATE_WAIT_AFTER_REGISTER_MINS if s.registered_now else 0
    deadline = time.time() + wait_mins * 60
    attempt = 0
    while True:
        attempt += 1
        try:
            ctx.invoke(miner_activate, backing=backing)
            return True
        except SystemExit:
            pass
        remaining = deadline - time.time()
        if remaining <= 0:
            return False
        pause = min(ACTIVATE_RETRY_SECS, remaining)
        console.print(
            f'  [dim]Validators pick up a new registration on their next metagraph sync (up to ~30 min). '
            f'Retrying in {max(1, round(pause / 60))} min — attempt {attempt}, {round(remaining / 60)} min left.[/dim]'
        )
        time.sleep(pause)


def _activate(ctx, s: Setup, client, pubkey) -> bool:
    ui.draw_step(
        console, 13, 'Activate', 'Tells validators each purse is ready; they verify and vote it live on-chain.'
    )
    ms = client.get_miner_state(pubkey)
    states = {st.backing: st for st in purse_states(client, pubkey, ms, client.get_config())}
    ok = True
    for backing in s.backings:
        if states[backing].lit:
            ui.draw_done(console, f'{backing} purse already serving')
            continue
        if not _activate_with_retry(ctx, s, backing):
            ok = False
            _resume_hint(f'alw miner activate --backing {backing}')
    return ok


def go_live(ctx, s: Setup) -> bool:
    import bittensor as bt

    config, client = get_solana_cli_context()
    pubkey = client.keypair.pubkey()
    wallet = _bt_wallet(s.wallet, s.hotkey)
    netuid = int(config.get('netuid', 7))
    floors = floors_from_config(client.get_config())
    subtensor = bt.Subtensor(network=config.get('network', 'finney'))

    if not _deposit(ctx, s, client, pubkey, floors):
        return False
    if not _bind(ctx, s, client, pubkey, wallet):
        return False
    if not _register(s, subtensor, wallet, netuid):
        return False
    if BACKING_CHAIN_TAO in s.backings and not _bond(ctx, s, subtensor, config, wallet, floors):
        return False
    ms = client.get_miner_state(pubkey)
    serving = ms is not None and any(st.lit for st in purse_states(client, pubkey, ms, client.get_config()))
    if not _run_container(s, serving):
        return False
    return _activate(ctx, s, client, pubkey)


# ─── Commands ────────────────────────────────────────────────────────────────

# The CLI strips --wallet/--hotkey/--network/--netuid from argv as global overrides before Click runs.
# Here they mean the same thing as the wizard's own flags, so take them back (and drop them from the
# effective config, which the wizard rewrites from scratch).
_NETWORK_ALIASES = {'test': 'testnet', 'finney': 'mainnet', 'testnet': 'testnet', 'mainnet': 'mainnet'}


def _take_global_flags(network, wallet, hotkey):
    from allways.cli.swap_commands import helpers

    ov = helpers._CLI_OVERRIDES
    raw_network = ov.pop('network', None)
    ov.pop('netuid', None)
    if not network and raw_network:
        network = _NETWORK_ALIASES.get(raw_network)
        if network is None:
            fail(f'--network must be testnet or mainnet (got {raw_network!r}).')
    return network, wallet or ov.pop('wallet', None), hotkey or ov.pop('hotkey', None)


@click.command('init', cls=StyledCommand, show_disclaimer=True)
@click.option('--network', type=click.Choice(list(ENV_BUNDLES)), default=None, help='testnet | mainnet')
@click.option('--wallet', default=None, help='Coldkey name (created interactively if missing)')
@click.option('--hotkey', default=None, help='Hotkey name (created if missing)')
@click.option(
    '--backing', type=click.Choice(['sol', 'tao', 'both']), default=None, help='Which purse(s) back your quotes'
)
@click.option(
    '--chains',
    default=None,
    help='Only key these spoke families (default: all): btc,eth,arb,hype,bnb,avax,base,cro,pol',
)
@click.option(
    '--solana-keypair', default=None, help='Keypair path (default ./data/solana/id.json; generated if missing)'
)
@click.option('--solana-rpc', default=None, help='Solana RPC URL')
@click.option('--coldkey-password', default=None, help='Store for headless starts (MINER_BITTENSOR_COLDKEY_PASSWORD)')
@click.option(
    '--project-dir',
    type=click.Path(file_okay=False, path_type=Path),
    default='.',
    help='Checkout holding .env + docker-compose.miner.yml',
)
@click.option(
    '--activate-wait',
    type=int,
    default=None,
    help='Minutes to keep retrying activation (default: 35 after a fresh registration, else 0)',
)
@click.option(
    '--configure-only', is_flag=True, help='Stop after writing config + preflight; skip the on-chain go-live steps'
)
@click.option(
    '--yes', '-y', is_flag=True, help='Take every default and skip confirmations (including the irreversible ones)'
)
@click.pass_context
def init_command(
    ctx,
    network,
    wallet,
    hotkey,
    backing,
    chains,
    solana_keypair,
    solana_rpc,
    coldkey_password,
    project_dir,
    activate_wait,
    configure_only,
    yes,
):
    """Set up a miner step by step: network, wallet, keys, .env, preflight, then the go-live sequence.

    [dim]Resumable — every step checks disk/chain state and skips what is already done. Each prompt has a flag,
    so `alw miner init --network testnet --wallet w --hotkey h --backing sol --chains eth -y` runs unattended.[/dim]

    [dim]Examples:
        $ alw miner init
        $ alw miner init --configure-only
        $ alw doctor[/dim]
    """
    network, wallet, hotkey = _take_global_flags(network, wallet, hotkey)
    s = Setup(project_dir=project_dir.resolve(), yes=yes, activate_wait_mins=activate_wait)
    ui.draw_logo(console, 'Welcome to the Allways miner setup!')
    console.print('[dim]Order matters on-chain: deposit → bind → register → (bond) → run → activate → quote.[/dim]')
    if not (s.project_dir / COMPOSE_FILE).is_file():
        ui.draw_warn(
            console,
            f'{COMPOSE_FILE} not found in {s.project_dir} — run this from your allways checkout (or --project-dir).',
        )

    step_network(s, network)
    step_wallet(s, wallet, hotkey)
    step_backing(s, backing, chains)
    step_keys(s, solana_keypair)
    step_rpc(s, solana_rpc)
    step_write_env(s, coldkey_password)
    rows = step_doctor(s.project_dir)

    if configure_only:
        _finish(s, live=False)
        return
    failed = [c for ok, c, _ in rows if ok is False]
    if (
        failed
        and not yes
        and not click.confirm(f'\n{len(failed)} check(s) failed. Continue to go-live anyway?', default=False)
    ):
        _finish(s, live=False)
        return
    if not yes and not click.confirm(
        '\nContinue to go-live (deposit → bind → register → bond → run → activate)?', default=True
    ):
        _finish(s, live=False)
        return
    _finish(s, live=go_live(ctx, s))


def _finish(s: Setup, live: bool) -> None:
    rows = [('network', s.env), ('wallet', f'{s.wallet} / {s.hotkey}'), ('backing', s.backing), *s.addresses.items()]
    width = max(len(k) for k, _ in rows)
    lines = [f'{k.ljust(width)}  {v}' for k, v in rows]
    ui.draw_success_box(console, lines, title='Miner live' if live else 'Configured')
    if live:
        ui.draw_next_steps(
            console,
            [
                ('alw miner post', 'post your first quote (interactive) — emission starts when you hold the best rate'),
                ('alw miner status', 'collateral, purses, quotes, active swaps'),
                (f'docker logs -f {CONTAINER}', 'watch the miner'),
            ],
        )
    else:
        ui.draw_next_steps(
            console,
            [
                ('alw doctor', 're-run the preflight after funding'),
                ('alw miner init', 'resume the go-live sequence'),
            ],
        )
    console.print(
        f'[dim]Back up: {se.wallets_root()}/{s.wallet}, {s.solana_keypair}, and {s.env_path} — they ARE the miner.[/dim]\n'
    )


@click.command('doctor', cls=StyledCommand)
@click.option(
    '--project-dir',
    type=click.Path(file_okay=False, path_type=Path),
    default='.',
    help='Checkout holding .env + docker-compose.miner.yml',
)
def doctor_command(project_dir):
    """Preflight a miner setup: config, wallet, keys, RPC, collateral, binding, registration, docker.

    [dim]Examples:
        $ alw doctor[/dim]
    """
    rows = run_doctor(project_dir.resolve())
    console.print(ui.check_table(rows))
    bad = sum(1 for ok, _, _ in rows if ok is False)
    console.print(f'[{"green" if not bad else "red"}]{len(rows) - bad}/{len(rows)} checks passed[/]\n')
    if bad:
        raise SystemExit(1)
