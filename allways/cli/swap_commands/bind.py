"""alw bind-hotkey — link a Bittensor hotkey to a Solana pubkey (miners and validators)."""

import click

from allways.cli.swap_commands.helpers import (
    console,
    fail,
    get_cli_context,
    get_solana_cli_context,
    loading,
)
from allways.solana.client import SolanaClientError


@click.command('bind-hotkey')
@click.option('--yes', '-y', is_flag=True, help='Skip confirmation prompt')
def bind_hotkey_command(yes: bool):
    """Bind your Bittensor hotkey to your Solana pubkey on-chain.

    [dim]The hotkey (sr25519) signs your Solana pubkey; the program stores it so validators attribute
    on-chain state to your metagraph UID — miner collateral/swaps/stats and validator stake weight
    alike. Idempotent.[/dim]

    [dim]Examples:
        $ alw bind-hotkey[/dim]
    """
    import bittensor as bt

    _, wallet, _, _ = get_cli_context(need_client=False)
    _, client = get_solana_cli_context()
    pubkey = client.keypair.pubkey()

    console.print('\n[bold]Bind Hotkey ↔ Solana Pubkey[/bold]\n')
    console.print(f'  Hotkey:  {wallet.hotkey.ss58_address}')
    console.print(f'  Pubkey:  {pubkey}\n')

    if client.get_binding(pubkey) is not None:
        console.print('[yellow]This pubkey is already bound.[/yellow]\n')
        return

    hotkey_bytes = bytes(wallet.hotkey.public_key)
    sig = wallet.hotkey.sign(bytes(pubkey))
    if not bt.Keypair(public_key='0x' + hotkey_bytes.hex()).verify(bytes(pubkey), sig):
        fail('Local signature verify failed; not submitting.')

    if not yes and not click.confirm('Confirm binding?'):
        console.print('[yellow]Cancelled[/yellow]')
        return

    try:
        with loading('Submitting transaction...'):
            client.bind_hotkey(hotkey_bytes, sig)
        console.print(f'[green]Bound {wallet.hotkey.ss58_address} → {pubkey}[/green]\n')
    except SolanaClientError as e:
        fail(f'Failed to bind hotkey: {e}')
