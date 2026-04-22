"""
Allways CLI - Main entry point

Usage:
    alw config              - Show/set CLI configuration
    alw miner post          - Post a trading pair
    alw collateral          - Manage collateral
    alw swap now        - Execute a swap (guided interactive)
    alw swap post-tx    - Submit tx hash for pending swap
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
    for _pkg in ['bittensor', 'substrateinterface']:
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

import click  # noqa: E402
from click.shell_completion import get_completion_class  # noqa: E402
from dotenv import load_dotenv  # noqa: E402
from rich.table import Table  # noqa: E402

load_dotenv()

from allways.cli.help import StyledAliasGroup, StyledGroup  # noqa: E402
from allways.cli.swap_commands.helpers import ALLWAYS_DIR, CONFIG_FILE, apply_global_flags, console  # noqa: E402

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
            table.add_row(key, str(value))

        console.print(table)
        console.print(f'\n[dim]Config file: {CONFIG_FILE}[/dim]\n')

    except json.JSONDecodeError:
        console.print('[red]Error: Invalid JSON in config file[/red]')
    except Exception as e:
        console.print(f'[red]Error reading config: {e}[/red]')


KNOWN_NETWORKS = {
    'finney': 'wss://entrypoint-finney.opentensor.ai:443',
    'test': 'wss://test.finney.opentensor.ai:443',
    'local': 'ws://127.0.0.1:9944',
}

VALID_CONFIG_KEYS = ('wallet', 'hotkey', 'network', 'netuid', 'contract-address')


@config_group.command('set')
@click.argument('key', type=click.Choice(VALID_CONFIG_KEYS, case_sensitive=False))
@click.argument('value', type=str)
def config_set(key: str, value: str):
    """Set a configuration value.

    [dim]Valid keys:
        wallet              Wallet name
        hotkey              Hotkey name
        contract-address    Contract address
        network             Network name or endpoint URL
        netuid              Subnet UID[/dim]

    [dim]Networks:
        finney              Production  (wss://entrypoint-finney.opentensor.ai:443)
        test                Test        (wss://test.finney.opentensor.ai:443)
        local               Local dev   (ws://127.0.0.1:9944)
        ws://...            Custom endpoint[/dim]

    [dim]Examples:
        $ alw config set wallet alice
        $ alw config set contract-address 5Cxxx...
        $ alw config set network finney
        $ alw config set network local[/dim]
    """
    ALLWAYS_DIR.mkdir(parents=True, exist_ok=True)

    config = {}
    if CONFIG_FILE.exists():
        try:
            config = json.loads(CONFIG_FILE.read_text())
        except json.JSONDecodeError:
            console.print('[yellow]Warning: Existing config was invalid, starting fresh[/yellow]')

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
