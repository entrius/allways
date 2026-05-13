from allways.cli.swap_commands.admin import admin_group
from allways.cli.swap_commands.claim import claim_command
from allways.cli.swap_commands.collateral import collateral_group
from allways.cli.swap_commands.history import history_command
from allways.cli.swap_commands.miner_commands import miner_group
from allways.cli.swap_commands.pair import post_pair
from allways.cli.swap_commands.post_tx import post_tx_command
from allways.cli.swap_commands.quote import quote_command
from allways.cli.swap_commands.resume import resume_reservation_command
from allways.cli.swap_commands.status import status_command
from allways.cli.swap_commands.swap import swap_group
from allways.cli.swap_commands.view import view_group

# Register post under the miner group
miner_group.add_command(post_pair, 'post')

# Register post-tx, quote, resume-reservation under the swap group
swap_group.add_command(post_tx_command, 'post-tx')
swap_group.add_command(quote_command, 'quote')
swap_group.add_command(resume_reservation_command, 'resume-reservation')
swap_group.add_command(history_command, 'history')


def register_commands(cli):
    """Register all swap commands with the CLI."""
    cli.add_command(collateral_group, 'collateral')
    cli.add_command(swap_group, 'swap')
    cli.add_command(view_group, 'view')
    cli.add_command(claim_command, 'claim')
    cli.add_command(miner_group, 'miner')
    cli.add_command(admin_group, 'admin')
    cli.add_command(status_command, 'status')
