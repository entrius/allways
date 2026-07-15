"""
Allways CLI - Main entry point

Usage:
    alw config              - Show/set CLI configuration
    alw miner post          - Post a trading pair
    alw collateral          - Manage collateral
    alw swap quote          - Preview miner rates for a swap
    alw swap now            - Originate a swap (flag-driven)
    alw view                - View swaps, miners, rates
"""

import os  # noqa: E402

# Prevent bittensor from hijacking --help via its argparse config.
# Must happen before any bittensor import.
import sys as _sys

_saved_argv = _sys.argv[:]
_sys.argv = [_sys.argv[0]]

# Stub heavy imports during shell completion
if os.environ.get('_ALW_COMPLETE'):
    from unittest.mock import MagicMock as _MagicMock

    _mock = _MagicMock()
    for _pkg in ['bittensor', 'async_substrate_interface']:
        for _suffix in [
            '',
            '.core',
            '.core.subtensor',
            '.core.synapse',
            '.utils',
            '.utils.balance',
            '.utils.ss58',
            '.exceptions',
        ]:
            _sys.modules[_pkg + _suffix] = _mock

import json  # noqa: E402
from pathlib import Path  # noqa: E402

import click  # noqa: E402
from click.shell_completion import get_completion_class  # noqa: E402
from dotenv import find_dotenv, load_dotenv  # noqa: E402
from rich.table import Table  # noqa: E402

# Precedence: shell env > project .env (CWD walk-up) > ~/.allways/.env. override=False makes earlier loads win.
load_dotenv(find_dotenv(usecwd=True), override=False)
load_dotenv(Path.home() / '.allways' / '.env', override=False)

from allways.cli.help import StyledAliasGroup, StyledGroup  # noqa: E402
from allways.cli.swap_commands.helpers import (  # noqa: E402
    ALLWAYS_DIR,
    BTC_NETWORKS,
    CONFIG_FILE,
    ENV_BUNDLES,
    SOLANA_NETWORKS,
    apply_btc_network_env,
    apply_global_flags,
    console,
    fail,
    get_effective_config,
)

# Feed a configured btc-network into the BTC provider (which reads BTC_NETWORK from env; a real env wins).
apply_btc_network_env(get_effective_config())

# Restore original argv now that bittensor has been imported
_sys.argv = _saved_argv

# Strip global flags (--wallet, --hotkey, --network, --netuid) from argv
# before Click processes commands. Must happen after argv is restored above.
apply_global_flags()


@click.group(cls=StyledAliasGroup, show_disclaimer=True)
@click.version_option(version=__import__('allways').__version__, prog_name='allways')
def cli():
    """Universal Transaction Layer"""
    pass


@click.group(name='config', invoke_without_command=True, cls=StyledGroup)
@click.pass_context
def config_group(ctx):
    """CLI configuration management.

    [dim]Show current configuration (default) or set config values.[/dim]
    """
    if ctx.invoked_subcommand is None:
        show_config()


def show_config():
    """Show current CLI configuration"""
    console.print('\n[bold]Allways CLI Configuration[/bold]\n')

    if not CONFIG_FILE.exists():
        console.print('[yellow]No config file found at ~/.allways/config.json[/yellow]')
        console.print('[dim]Run `alw config set <key> <value>` to create config[/dim]')
        return

    try:
        config = json.loads(CONFIG_FILE.read_text())

        table = Table(show_header=True)
        table.add_column('Setting', style='cyan')
        table.add_column('Value', style='green')

        for key, value in config.items():
            if key in HIDDEN_CONFIG_KEYS:
                continue  # legacy substrate key, dead on Solana — hidden but tolerated in the file
            table.add_row(key, str(value))

        console.print(table)

        # Show which key will actually sign (env > solana-keypair config > ~/.solana/id.json),
        # so a wrong signer is visible here instead of as a cryptic on-chain failure.
        signer = _resolved_signer_line(config)
        if signer:
            console.print(f'\n[dim]Solana signer:[/dim] {signer}')
        console.print(f'\n[dim]Config file: {CONFIG_FILE}[/dim]\n')

    except json.JSONDecodeError:
        console.print('[red]Error: Invalid JSON in config file[/red]')
    except Exception as e:
        console.print(f'[red]Error reading config: {e}[/red]')


def _resolved_signer_line(config: dict) -> str | None:
    """Resolved Solana signer as 'pubkey (path)', or a not-found note; None only on import trouble."""
    try:
        from allways.cli.swap_commands.helpers import resolve_solana_keypair_path
        from allways.solana import keys

        path = resolve_solana_keypair_path(config)
        if not os.path.isfile(path):
            return f'[yellow]no keypair at {path}[/yellow]'
        return f'{keys.load_keypair(path).pubkey()} [dim]({path})[/dim]'
    except Exception:
        return None


KNOWN_NETWORKS = {
    'finney': 'wss://entrypoint-finney.opentensor.ai:443',
    'test': 'wss://test.finney.opentensor.ai:443',
    'local': 'ws://127.0.0.1:9944',
}

VALID_CONFIG_KEYS = (
    'wallet',
    'hotkey',
    'network',
    'netuid',
    'program-id',
    'solana-rpc',
    'solana-network',
    'solana-keypair',
    'btc-network',
    'router',
    'env',
)

# Legacy keys still tolerated in an existing config file but never shown or settable (dead on Solana).
HIDDEN_CONFIG_KEYS = ('contract-address', 'contract')


@config_group.command('set')
@click.argument('key', type=click.Choice(VALID_CONFIG_KEYS, case_sensitive=False))
@click.argument('value', type=str)
def config_set(key: str, value: str):
    """Set a configuration value.

    [dim]Valid keys:
        env                 One-liner bundle: sets network + solana-network + btc-network + netuid + router
        wallet              Bittensor wallet name
        hotkey              Bittensor hotkey name
        network             Bittensor network name (test/finney/local) or ws:// endpoint
        netuid              Subnet UID
        solana-network      Solana network name (devnet/mainnet/localnet) → RPC resolved in code
        solana-rpc          Custom Solana RPC URL (escape hatch; SOLANA_RPC_URL env wins)
        solana-keypair      Path to the Solana keypair that signs miner/admin ops (SOLANA_KEYPAIR_PATH env wins)
        btc-network         Bitcoin network name (mainnet/testnet4/testnet/signet)
        router              Validator hotkey (ss58) to route reservations through; "" = self-represent
        program-id          Solana program ID (miner/admin commands)[/dim]

    [dim]Networks per chain:
        env:            testnet | mainnet   (sets all three chains at once)
        network:        finney | test | local | ws://...
        solana-network: devnet | mainnet | localnet   (or set a custom solana-rpc URL)
        btc-network:    mainnet | testnet4 | testnet | signet[/dim]

    [dim]Examples:
        $ alw config set env testnet          # bittensor test + solana devnet + btc testnet4 + netuid 19
        $ alw config set wallet alice
        $ alw config set solana-network devnet
        $ alw config set solana-keypair ~/.config/solana/dev.json
        $ alw config set network finney[/dim]
    """
    ALLWAYS_DIR.mkdir(parents=True, exist_ok=True)

    config = {}
    if CONFIG_FILE.exists():
        try:
            config = json.loads(CONFIG_FILE.read_text())
        except json.JSONDecodeError:
            console.print('[yellow]Warning: Existing config was invalid, starting fresh[/yellow]')

    # env bundle: expand one name into all three chains' networks + netuid in a single write.
    if key == 'env':
        bundle = ENV_BUNDLES.get(value)
        if not bundle:
            console.print(f'[red]Unknown env {value!r}; expected {list(ENV_BUNDLES)}.[/red]')
            return
        config.update(bundle)
        CONFIG_FILE.write_text(json.dumps(config, indent=2))
        console.print(f'[green]Set env {value}:[/green] ' + ', '.join(f'{k}={v}' for k, v in bundle.items()))
        return

    # Validate name-based network keys — the raw-URL escape hatches are solana-rpc / SOLANA_RPC_URL.
    if key == 'solana-network' and value not in SOLANA_NETWORKS:
        console.print(
            f'[red]Unknown solana-network {value!r}; expected {list(SOLANA_NETWORKS)} (or set a custom solana-rpc).[/red]'
        )
        return
    if key == 'btc-network' and value not in BTC_NETWORKS:
        console.print(f'[red]Unknown btc-network {value!r}; expected {list(BTC_NETWORKS)}.[/red]')
        return

    # Validate the keypair at set time and echo its pubkey, so a typo'd path or wrong file
    # fails here — not later as an unfunded/non-authority signer on a live command.
    keypair_pubkey = None
    if key == 'solana-keypair':
        from allways.solana import keys

        value = os.path.expanduser(value)
        if not os.path.isfile(value):
            fail(f'solana-keypair {value} not found.')
        try:
            keypair_pubkey = keys.load_keypair(value).pubkey()
        except Exception as e:
            fail(f'solana-keypair {value} is not a loadable Solana keypair file: {e}')

    # Normalize network: reverse-map known endpoints to names
    if key == 'network':
        for name, endpoint in KNOWN_NETWORKS.items():
            if value == endpoint:
                value = name
                break

    old_value = config.get(key)
    config[key] = value

    CONFIG_FILE.write_text(json.dumps(config, indent=2))

    display = value
    if key == 'network' and value in KNOWN_NETWORKS:
        display = f'{value} ({KNOWN_NETWORKS[value]})'
    if keypair_pubkey is not None:
        display = f'{value} → signs as {keypair_pubkey}'

    if old_value is not None:
        console.print(f'[green]Updated {key}:[/green] {old_value} -> {display}')
    else:
        console.print(f'[green]Set {key}:[/green] {display}')


def _detect_shell():
    """Detect the current shell from the SHELL environment variable"""
    shell_path = os.environ.get('SHELL', '')
    shell_name = os.path.basename(shell_path)
    if shell_name in ('bash', 'zsh', 'fish'):
        return shell_name
    return None


@cli.command('completion')
@click.argument('shell', type=click.Choice(['bash', 'zsh', 'fish']), default=None, required=False)
def completion(shell):
    """Generate shell completion script

    Install completions:
        bash:  eval "$(alw completion bash)"
        zsh:   eval "$(alw completion zsh)"
        fish:  alw completion fish | source

    If shell is omitted, auto-detects from the SHELL environment variable.
    """
    if shell is None:
        shell = _detect_shell()
        if shell is None:
            raise click.UsageError('Cannot detect shell. Please specify one of: bash, zsh, fish')
    cls = get_completion_class(shell)
    if cls is None:
        raise click.UsageError(f'Unsupported shell: {shell}')
    comp = cls(cli, ctx_args={}, prog_name='alw', complete_var='_ALW_COMPLETE')
    click.echo(comp.source())


# Register config group
cli.add_command(config_group)

# Import and register swap commands
from allways.cli.swap_commands import register_commands  # noqa: E402

register_commands(cli)


def main():
    """Main entry point for the CLI"""
    cli()


if __name__ == '__main__':
    main()
