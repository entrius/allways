import json
import os
import sys
import time
from pathlib import Path
from typing import Optional, Tuple

import bittensor as bt
import click
import requests
from rich.console import Console
from rich.text import Text

from allways.classes import SwapStatus
from allways.constants import NETUID_FINNEY, TAO_TO_RAO

ALLWAYS_DIR = Path.home() / '.allways'
CONFIG_FILE = ALLWAYS_DIR / 'config.json'
PENDING_SWAP_FILE = ALLWAYS_DIR / 'pending_swap.json'

console = Console()


class CliError(Exception):
    """Local CLI/Solana error type — replaces the deleted ink! contract error."""


def phase9_unavailable(what: str) -> None:
    """Stub for taker reservation/swap-intake commands pending the Phase-9 port.

    The on-chain reservation flow (open_or_request/resolve_pool lottery) replaces the
    old ink! reserve→confirm path; the taker CLI intake is not wired yet."""
    console.print(
        f'[yellow]{what} is not available yet.[/yellow]\n'
        '[dim]Reservations and swap intake moved on-chain to Solana and land with the Phase 9 '
        'reservation pool (open_or_request/resolve_pool). The taker CLI for it is not wired yet.[/dim]'
    )


SECONDS_PER_BLOCK = 12

# --- Miner reliability (swap success rate) -------------------------------
# Per-miner success rate is not on-chain. `view rates` and `swap now` pull a
# pre-aggregated per-direction completed/total map from the allways API and
# color-code it. Override the host with ALLWAYS_API_URL for testnet or a
# self-hosted indexer.
DEFAULT_API_URL = 'https://api.all-ways.io'
RELIABILITY_CACHE_TTL = 600  # seconds — stats move slowly; avoid refetching every call


def _api_url() -> str:
    return os.environ.get('ALLWAYS_API_URL', DEFAULT_API_URL).rstrip('/')


def fetch_miner_reliability(use_cache: bool = True) -> Optional[dict]:
    """Per-miner, per-direction swap success counts from the allways API.

    Returns ``{hotkey: {'btc->tao': (completed, total), ...}}`` from
    ``/miners/reliability`` — resolved swaps only (COMPLETED + TIMED_OUT) over
    the API's credibility window. Returns ``None`` if the API is unreachable:
    callers must degrade gracefully, since `view rates` and `swap now` have to
    work whether or not the indexer is up.
    """
    cache_file = ALLWAYS_DIR / 'miner_reliability_cache.json'
    api_url = _api_url()
    if use_cache and cache_file.exists():
        try:
            cached = json.loads(cache_file.read_text())
            fresh = time.time() - cached.get('fetched_at', 0) < RELIABILITY_CACHE_TTL
            # A cache from a different API host must not be reused.
            if fresh and cached.get('api_url') == api_url:
                return {hk: {d: tuple(v) for d, v in dirs.items()} for hk, dirs in cached['stats'].items()}
        except (json.JSONDecodeError, KeyError, OSError):
            pass  # stale/corrupt cache — fall through and refetch

    # The API rejects unknown user agents; identify ourselves explicitly.
    headers = {'User-Agent': f'allways-cli/{__import__("allways").__version__}'}
    try:
        resp = requests.get(f'{api_url}/miners/reliability', headers=headers, timeout=10)
        resp.raise_for_status()
        rows = resp.json()
    except (requests.RequestException, ValueError):
        return None
    # A JSON error object (dict) instead of a list means no usable data.
    if not isinstance(rows, list):
        return {}

    stats: dict = {}
    for r in rows:
        hk = r.get('minerHotkey')
        src = r.get('sourceChain')
        dst = r.get('destChain')
        if not hk or not src or not dst:
            continue
        stats.setdefault(hk, {})[f'{src}->{dst}'] = (int(r.get('completed') or 0), int(r.get('total') or 0))

    try:
        ALLWAYS_DIR.mkdir(parents=True, exist_ok=True)
        cache_file.write_text(
            json.dumps(
                {
                    'fetched_at': int(time.time()),
                    'api_url': api_url,
                    'stats': {hk: {d: list(v) for d, v in dirs.items()} for hk, dirs in stats.items()},
                }
            )
        )
    except OSError:
        pass  # cache write is best-effort
    return stats


def reliability_text(hotkey: str, src: str, dst: str, reliability: Optional[dict]) -> Text:
    """Colored ``completed/total`` for one swap direction.

    Green ≥90%, yellow ≥50%, red below; dim ``—`` when reliability is
    unavailable or the miner has no resolved swap in that direction.
    """
    if reliability is None:
        return Text('—', style='dim')
    comp, tot = reliability.get(hotkey, {}).get(f'{src}->{dst}', (0, 0))
    if tot == 0:
        return Text('—', style='dim')
    pct = comp / tot
    style = 'green' if pct >= 0.9 else 'yellow' if pct >= 0.5 else 'red'
    return Text(f'{comp}/{tot}', style=style)


def blocks_to_minutes_str(blocks: int) -> str:
    """Render a block count as an approximate minutes string like '~5 min'."""
    return f'~{blocks * SECONDS_PER_BLOCK / 60:.0f} min'


SWAP_STATUS_COLORS = {
    SwapStatus.ACTIVE: 'yellow',
    SwapStatus.FULFILLED: 'blue',
    SwapStatus.COMPLETED: 'green',
    SwapStatus.TIMED_OUT: 'red',
}


def loading(message: str, spinner: str = 'dots', color: str = 'cyan'):
    """Return a Rich spinner context manager for long-running operations."""
    return console.status(f'[{color}]{message}[/{color}]', spinner=spinner, spinner_style=color)


def sign_or_prompt_external(
    provider,
    address: str,
    message: str,
    key=None,
    chain: str = '',
    skip_confirm: bool = False,
) -> str:
    """Sign a proof-of-ownership message, falling back to externally-pasted signature.

    Tries internal signing first (env var WIF, wallet coldkey, Bitcoin Core RPC).
    On failure for BTC source swaps in interactive mode, prompts the user to
    sign the exact message in an external wallet (Electrum, Sparrow, Trezor,
    Bitcoin Core) and paste the base64 BIP-137 signature. Verifies the pasted
    signature before returning it so a typo fails here rather than at the
    validator.

    Returns an empty string when no valid signature is obtained.
    """
    try:
        signature = provider.sign_from_proof(address, message, key)
    except Exception as e:
        bt.logging.warning(f'Internal signing failed ({type(e).__name__}): {e}')
        signature = ''

    if signature:
        return signature

    if skip_confirm or chain != 'btc':
        return ''

    console.print('\n  [bold yellow]External signature required[/bold yellow]')
    console.print(
        '  [dim]No BTC signing key loaded. Sign the message below in your wallet\n'
        '  (Electrum: Tools -> Sign/verify message; Sparrow, Trezor, Bitcoin Core\n'
        '  all support this) and paste the base64 signature back.[/dim]'
    )
    console.print(f'\n  Address: [cyan]{address}[/cyan]')
    console.print(f'  Message: [yellow]{message}[/yellow]\n')

    pasted = click.prompt('  Paste signature (blank to cancel)', default='', show_default=False).strip()
    if not pasted:
        return ''

    try:
        verified = provider.verify_from_proof(address, message, pasted)
    except Exception as e:
        console.print(f'[red]Signature verification errored: {e}[/red]')
        return ''

    if not verified:
        console.print(
            '[red]Signature did not verify for this address/message. Make sure you signed the exact\n'
            'message shown above with the private key for that address.[/red]'
        )
        return ''

    console.print('[green]  Signature verified.[/green]')
    return pasted


def is_valid_ss58(address: str) -> bool:
    """Check if a string is a syntactically valid SS58 address.

    Does not verify the account exists on-chain — only that the encoding is
    well-formed. Useful as a pre-flight guard before submitting an admin
    extrinsic whose typo would silently fail.
    """
    try:
        from bittensor.utils import ss58_decode

        ss58_decode(address)
        return True
    except Exception:
        return False


# Global flags that can appear anywhere in the command line.
# Maps CLI flag names to config keys.
_GLOBAL_FLAGS = {
    '--network': 'network',
    '--wallet': 'wallet',
    '--wallet.name': 'wallet',
    '--wallet-name': 'wallet',
    '--hotkey': 'hotkey',
    '--wallet.hotkey': 'hotkey',
    '--netuid': 'netuid',
}


def is_local_network(network: str) -> bool:
    """Check if the network config points to a local dev environment."""
    if network == 'local':
        return True
    return any(host in network for host in ('127.0.0.1', 'localhost', '0.0.0.0'))


PROD_DASHBOARD_URL = 'https://all-ways.io'
TEST_DASHBOARD_URL = 'https://test.all-ways.io'


def dashboard_url(network: Optional[str] = None) -> str:
    """Resolve the dashboard base URL for the active network.

    finney maps to the mainnet dashboard; every other network (test, local,
    custom endpoints) maps to the testnet dashboard. ALLWAYS_DASHBOARD_URL
    overrides everything for staging/local use.
    """
    override = os.environ.get('ALLWAYS_DASHBOARD_URL')
    if override:
        return override.rstrip('/')
    if network is None:
        network = get_effective_config().get('network', 'finney')
    return (PROD_DASHBOARD_URL if network == 'finney' else TEST_DASHBOARD_URL).rstrip('/')


def to_rao(amount_tao: float) -> int:
    """Convert TAO to rao."""
    return int(amount_tao * TAO_TO_RAO)


def from_rao(amount_rao: int) -> float:
    """Convert rao to TAO."""
    return amount_rao / TAO_TO_RAO


LAMPORTS_PER_SOL = 1_000_000_000


def to_lamports(amount_sol: float) -> int:
    """Convert SOL to lamports."""
    return int(amount_sol * LAMPORTS_PER_SOL)


def from_lamports(amount_lamports: int) -> float:
    """Convert lamports to SOL."""
    return amount_lamports / LAMPORTS_PER_SOL


def get_solana_cli_context(need_keypair: bool = True):
    """Solana CLI setup for the B4-repointed miner/admin commands → (config, solana_client).

    The miner/admin identity is the Solana keypair (SOLANA_KEYPAIR_PATH / ~/.solana/id.json), NOT the bt
    wallet — collateral, quotes, and config are keyed by that pubkey on the program. The bt wallet is only
    needed where a command links the two identities (`alw miner bind-hotkey`).
    """
    from solders.pubkey import Pubkey

    from allways.solana import keys, pdas
    from allways.solana.client import AllwaysSolanaClient

    config = get_effective_config()
    rpc_url = os.environ.get('SOLANA_RPC_URL') or config.get('solana-rpc') or 'http://127.0.0.1:8899'
    program_id = pdas.PROGRAM_ID
    configured = config.get('program-id') or config.get('contract')
    if configured:
        try:
            program_id = Pubkey.from_string(configured)
        except (ValueError, TypeError):
            console.print(
                f'[yellow]Ignoring invalid program-id config {configured!r}; using default {program_id}[/yellow]'
            )
    keypair = keys.load_or_create() if need_keypair else None
    return config, AllwaysSolanaClient(rpc_url, program_id=program_id, keypair=keypair)


def load_cli_config() -> dict:
    """Load CLI configuration from ~/.allways/config.json."""
    if not CONFIG_FILE.exists():
        return {}
    try:
        return json.loads(CONFIG_FILE.read_text())
    except Exception:
        return {}


def parse_global_flags() -> dict:
    """Extract global flags (--network, --wallet, etc.) from sys.argv.

    Strips matched flags and their values from sys.argv so Click
    subcommands don't choke on unknown options.
    """
    overrides = {}
    new_argv = [sys.argv[0]]
    i = 1
    while i < len(sys.argv):
        arg = sys.argv[i]
        # Handle --flag=value form
        if '=' in arg:
            flag, value = arg.split('=', 1)
            if flag in _GLOBAL_FLAGS:
                overrides[_GLOBAL_FLAGS[flag]] = value
                i += 1
                continue
        # Handle --flag value form
        if arg in _GLOBAL_FLAGS:
            if i + 1 < len(sys.argv):
                overrides[_GLOBAL_FLAGS[arg]] = sys.argv[i + 1]
                i += 2
                continue
        new_argv.append(arg)
        i += 1
    sys.argv[:] = new_argv
    return overrides


_CLI_OVERRIDES: dict = {}


def apply_global_flags():
    """Parse and strip global flags from sys.argv. Must be called after argv is restored."""
    global _CLI_OVERRIDES
    _CLI_OVERRIDES = parse_global_flags()


def get_effective_config() -> dict:
    """Merge file config with CLI global overrides (CLI flags win)."""
    config = load_cli_config()
    config.update(_CLI_OVERRIDES)
    return config


def get_cli_context(
    need_wallet: bool = True,
    need_client: bool = False,
) -> Tuple[dict, Optional[bt.Wallet], bt.Subtensor, None]:
    """Standard bt-side CLI context: config, wallet, subtensor (no contract client).

    The ink! contract client is gone (B6); the 4th tuple slot stays ``None`` so the
    bt-wallet callers keep their unpacking. ``need_client`` is accepted for call-site
    compatibility but no longer builds anything."""
    config = get_effective_config()
    network = config.get('network', 'finney')
    with console.status(
        f'[cyan]Synchronizing with chain [dim]{network}[/dim]...[/cyan]', spinner='dots', spinner_style='cyan'
    ):
        subtensor = bt.Subtensor(network=network)
        wallet = None
        if need_wallet:
            wallet = bt.Wallet(
                name=config.get('wallet', 'default'),
                hotkey=config.get('hotkey', 'default'),
            )
    # Ensure netuid is resolved for callers
    if 'netuid' not in config:
        config['netuid'] = NETUID_FINNEY
    else:
        config['netuid'] = int(config['netuid'])
    return config, wallet, subtensor, None
