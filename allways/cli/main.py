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

# Prevent bittensor from hijacking --help via its argparse config.
# Must happen before any bittensor import.
import sys as _sys

_saved_argv = _sys.argv[:]
_sys.argv = [_sys.argv[0]]

import json  # noqa: E402

import rich_click as click  # noqa: E402
from dotenv import load_dotenv  # noqa: E402
from rich.table import Table  # noqa: E402

click.rich_click.USE_RICH_MARKUP = True
click.rich_click.SHOW_ARGUMENTS = True
click.rich_click.GROUP_ARGUMENTS_OPTIONS = True
click.rich_click.STYLE_EPILOG = 'dim'
click.rich_click.FOOTER_TEXT = '\u2764 Ventura Labs'

load_dotenv()

from allways.cli.swap_commands.helpers import ALLWAYS_DIR, CONFIG_FILE, console, parse_global_flags  # noqa: E402

# Restore original argv now that bittensor has been imported
_sys.argv = _saved_argv

# Strip global flags (--wallet, --hotkey, --network, --netuid) from argv
# before Click processes commands. Must happen after argv is restored above.
parse_global_flags()


DISCLAIMER = (
    'Allways is permissionless, open-source, beta software. The protocol facilitates trustless'
    ' peer-to-peer transactions — the creators and contributors do not custody, control, or'
    ' intermediate any funds. Use at your own risk. No warranty. Not financial advice.'
)


@click.group(epilog=DISCLAIMER)
@click.version_option(version=__import__('allways').__version__, prog_name='allways')
def cli():
    """Universal Transaction Layer"""
    pass


@click.group(name='config', invoke_without_command=True)
@click.pass_context
def config_group(ctx):
    """CLI configuration management.

    Show current configuration (default) or set config values.

    \b
    Subcommands:
        set <key> <value>    Set a config value
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
            str_val = str(value)
            if len(str_val) > 25:
                str_val = str_val[:12] + '...' + str_val[-10:]
            table.add_row(key, str_val)

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


@config_group.command('set')
@click.argument('key', type=str)
@click.argument('value', type=str)
def config_set(key: str, value: str):
    """Set a configuration value.

    \b
    Common keys:
        wallet              Wallet name
        hotkey              Hotkey name
        contract-address    Contract address
        network             Network name or endpoint URL
        netuid              Subnet UID

    \b
    Networks:
        finney              Production  (wss://entrypoint-finney.opentensor.ai:443)
        test                Test        (wss://test.finney.opentensor.ai:443)
        local               Local dev   (ws://127.0.0.1:9944)
        ws://...            Custom endpoint

    \b
    Examples:
        alw config set wallet alice
        alw config set contract-address 5Cxxx...
        alw config set network finney
        alw config set network local
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
