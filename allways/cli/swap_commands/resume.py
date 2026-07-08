"""alw swap resume-reservation - Recover an interrupted pre-initiate reservation flow.

Deferred: resuming a reservation submits + verifies the source deposit against the source chain (a
chain-provider check the CLI taker path does not do yet). Exits non-zero; use the browser flow."""

from typing import Optional

import click

from allways.cli.swap_commands.helpers import not_implemented


@click.command('resume-reservation')
@click.option('--from-tx-hash', 'from_tx_hash_opt', default=None, help='Source tx hash (skip fund sending)')
@click.option(
    '--send',
    'auto_send',
    is_flag=True,
    help='Broadcast source funds automatically (TAO via wallet, BTC via BTC_PRIVATE_KEY)',
)
@click.option('--yes', 'skip_confirm', is_flag=True, help='Skip confirmation prompts')
def resume_reservation_command(from_tx_hash_opt: Optional[str], auto_send: bool, skip_confirm: bool):
    """Resume an interrupted pre-initiate reservation.

    \b
    Picks up a reservation that was opened by `alw swap now` but never made
    it to vote_initiate — submits the source transaction hash and confirms
    with validators. If the reservation has expired, guides the user to
    start fresh with `alw swap now`.

    \b
    Interactive mode:
        alw swap resume-reservation

    \b
    Non-interactive mode (for scripting/agents):
        alw swap resume-reservation --from-tx-hash abc123... --yes
        alw swap resume-reservation --send --yes
    """
    not_implemented('Swap resume-reservation (CLI fund relay)')
