"""alw swap resume-reservation - Recover an interrupted pre-initiate reservation flow.

Deferred: resuming a reservation submits + verifies the source deposit against the source chain (a
chain-provider check the CLI taker path does not do yet). Exits non-zero; use the browser flow."""

import click

from allways.cli.swap_commands.helpers import not_implemented


@click.command('resume-reservation')
def resume_reservation_command():
    """Resume an interrupted pre-initiate reservation (not yet available from the CLI).

    Advancing a reservation submits + verifies the source deposit against the source chain, which the
    CLI taker path does not do yet. Use the browser swap flow to resume; inspect a reservation's state
    with `alw view reservation --miner <pubkey>`.
    """
    not_implemented('Swap resume-reservation (CLI fund relay)')
