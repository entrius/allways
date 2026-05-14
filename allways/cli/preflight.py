"""Pre-broadcast safety gate for source funds.

Sits outside ``swap_commands/`` so importing it does not trigger that
package's eager command-registration ``__init__.py`` (which pulls in
bittensor via helpers). That keeps the wiring unit-testable without the
whole CLI stack. swap.py and resume.py both import the public
``preflight_send_runway`` and call it after the user has approved the
send but before any funds leave the wallet.
"""

import click
from rich.console import Console

from allways.chains import (
    RUNWAY_EXTENSION_REQUIRED,
    RUNWAY_TOO_SHORT,
    classify_send_runway,
    get_chain,
)
from allways.constants import EXTEND_THRESHOLD_BLOCKS

# Local Console rather than helpers.console: helpers.py imports bittensor at
# module load and would defeat the whole point of keeping this module light.
# Both Consoles target stdout — output ordering is unaffected.
_console = Console()

_SECONDS_PER_BLOCK = 12


def _blocks_to_minutes_str(blocks: int) -> str:
    return f'~{blocks * _SECONDS_PER_BLOCK / 60:.0f} min'


def preflight_send_runway(
    subtensor,
    from_chain: str,
    reserved_until_block: int,
    skip_confirm: bool,
) -> bool:
    """Refuse / hard-warn before broadcasting source funds into a doomed reservation.

    Re-reads the current subtensor block (the user may have idled at the
    summary panel) and classifies the remaining TTL against the validator
    auto-extension floor. Returns True if the caller should proceed with
    the broadcast, False if the caller should abort. See
    ``classify_send_runway`` in allways/chains.py for the categories.
    """
    current_block = subtensor.get_current_block()
    status, remaining = classify_send_runway(from_chain, current_block, reserved_until_block, EXTEND_THRESHOLD_BLOCKS)
    if status == RUNWAY_TOO_SHORT:
        chain = get_chain(from_chain)
        confs_min = chain.min_confirmations * chain.seconds_per_block // 60
        _console.print(
            f'\n[red]Refusing to send: only {_blocks_to_minutes_str(max(0, remaining))} '
            f'left on the reservation — below the {EXTEND_THRESHOLD_BLOCKS}-block '
            f'floor needed for validators to auto-extend. {chain.min_confirmations} '
            f'{from_chain.upper()} confirmation(s) take ~{confs_min} min, so the '
            f'reservation will expire before your tx confirms and the swap will fail.[/red]'
        )
        _console.print('[yellow]Start fresh with a new reservation: [cyan]alw swap now[/cyan][/yellow]')
        return False
    if status == RUNWAY_EXTENSION_REQUIRED:
        chain = get_chain(from_chain)
        confs_min = chain.min_confirmations * chain.seconds_per_block // 60
        _console.print(
            f'\n[yellow]Reservation has {_blocks_to_minutes_str(remaining)} left, less '
            f'than the ~{confs_min} min needed for {chain.min_confirmations} '
            f'{from_chain.upper()} confirmation(s). Validators will auto-extend once '
            f'your tx is visible — but if they miss the window, the swap will expire '
            f'before confirmation and your funds may be stranded.[/yellow]'
        )
        if not skip_confirm and not click.confirm('  Send anyway?', default=False):
            _console.print('[yellow]Cancelled. Start fresh with: alw swap now[/yellow]')
            return False
    return True
