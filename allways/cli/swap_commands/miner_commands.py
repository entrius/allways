"""alw miner - Miner dashboard commands."""

import asyncio
import time

import click
from rich.table import Table

from allways.cli.dendrite_lite import discover_validators, resolve_dendrite_timeout
from allways.cli.help import StyledGroup
from allways.cli.swap_commands.helpers import (
    activation_prerequisites,
    backing_label,
    console,
    fail,
    from_lamports,
    get_cli_context,
    get_solana_cli_context,
    loading,
    purse_states,
    resolve_activation_backing,
)
from allways.cli.swap_commands.swap_intake import rate_display_from_fixed
from allways.cli.validator_rejections import render_and_aggregate
from allways.constants import TAO_TO_RAO
from allways.solana.client import SolanaClientError, swap_from_solana, swap_key_from_tx_hash
from allways.solana.pdas import BACKING_BIT_SOL, BACKING_BIT_TAO, BACKING_BITS
from allways.utils.rate import directional_rate


def _purse_state(lit: bool) -> str:
    """Purses are activated independently, so each one says whether it is serving (D2)."""
    return '  [green](serving)[/green]' if lit else '  [dim](not serving)[/dim]'


@click.group('miner', cls=StyledGroup)
def miner_group():
    """Miner dashboard commands."""
    pass


@miner_group.command('status')
@click.option('--pubkey', default=None, type=str, help='Miner Solana pubkey to check (default: your keypair)')
def miner_status(pubkey: str):
    """View miner status: collateral, posted quotes, and active swaps.

    [dim]Examples:
        $ alw miner status
        $ alw miner status --pubkey 7xKX...[/dim]
    """
    _, client = get_solana_cli_context()
    target = pubkey or str(client.keypair.pubkey())

    console.print(f'\n[bold]Miner Status — {target[:16]}...[/bold]\n')

    # Section 1: Collateral & Status
    try:
        with loading('Reading miner status...'):
            collateral_lamports = client.get_collateral_lamports(target) or 0
            config = client.get_config()
            ms = client.get_miner_state(target)
    except SolanaClientError as e:
        fail(f'Failed to read miner data: {e}')

    is_active = bool(ms and ms.active)
    has_active_swap = bool(ms and ms.has_active_swap)
    min_required = config.min_collateral if config is not None else 0
    mask = int(getattr(ms, 'active_backings', 0) or 0) if ms is not None else 0

    table = Table(title='Collateral & Status', show_header=True)
    table.add_column('Field', style='cyan')
    table.add_column('Value', style='green')

    sol_lit = bool(mask & BACKING_BIT_SOL)
    table.add_row('SOL purse', f'{from_lamports(collateral_lamports):.4f} SOL' + _purse_state(sol_lit))
    table.add_row('Min Required', f'{from_lamports(min_required):.4f} SOL')

    # The TAO purse is NOT readable from Solana — what's shown is the quorum's attestation, which is
    # already net of accrued fees and voted slashes. Say so: an operator comparing it to `alw vault
    # status` (which prints the vault's GROSS counter) must know why the two differ.
    tao_bit = bool(mask & BACKING_BIT_TAO)
    tao_floor = int(getattr(config, 'tao_min_collateral', 0) or 0) if config is not None else 0
    att = None
    if tao_bit or ms is not None:
        try:
            att = client.get_bond_attestation(target, 'tao')
        except SolanaClientError:
            att = None
    # Shown whenever the deployment backs TAO at all, not only once an attestation exists: a miner
    # whose bond has not been mirrored yet is exactly the one who needs to be told that is why.
    if att is not None or tao_bit or tao_floor:
        bond = (
            f'{int(att.effective_balance) / TAO_TO_RAO:.9f} TAO'
            if att is not None
            else '(no attestation — validators mirror a LOCKED bond on their own cadence)'
        )
        if att is not None and not att.locked:
            bond += ' [yellow](attested UNLOCKED — backs nothing)[/yellow]'
        table.add_row('TAO purse (effective)', bond + _purse_state(tao_bit))
        table.add_row('Min Required (TAO)', f'{tao_floor / TAO_TO_RAO:.9f} TAO')

    table.add_row('Status', '[green]Active[/green]' if is_active else '[red]Inactive[/red]')
    table.add_row('Has Active Swap', '[yellow]Yes[/yellow]' if has_active_swap else '[dim]No[/dim]')
    if ms is not None:
        table.add_row('Successful / Failed', f'{ms.successful_swaps} / {ms.failed_swaps}')

    console.print(table)
    if att is not None:
        console.print(
            "[dim]TAO purse is the validators' attested EFFECTIVE bond — vault gross minus accrued"
            ' fees and voted slashes.\nIt is deliberately lower than `alw vault status`, which prints'
            " the vault's gross counter.[/dim]"
        )
    console.print()

    # Section 2: Posted Quotes (on-chain MinerQuote PDAs)
    try:
        with loading('Reading posted quotes...'):
            quotes = [q for _pda, q in client.get_all('MinerQuote') if str(q.miner) == target]
    except SolanaClientError as e:
        console.print(f'[yellow]Could not read quotes: {e}[/yellow]')
        quotes = []

    if quotes:
        console.print('[bold]Posted Quotes[/bold]\n')
        for q in quotes:
            src_up, dst_up = q.from_chain.upper(), q.to_chain.upper()
            rd = directional_rate(q.from_chain, q.to_chain, rate_display_from_fixed(q.rate))
            label = backing_label(getattr(q, 'collateral_chain', None))
            console.print(f'  {src_up} → {dst_up} [cyan]{label}[/cyan]: [green]{rd} {dst_up}/{src_up}[/green]')
            console.print(f'    receive on {src_up}: [dim]{q.miner_from_addr}[/dim]')
            console.print(f'    send on {dst_up}:    [dim]{q.miner_to_addr}[/dim]')
    else:
        console.print('[yellow]No posted quotes found[/yellow]')

    console.print()

    # Section 3: Active Swaps
    try:
        with loading('Reading active swaps...'):
            swaps = [
                swap_from_solana(s)
                for status in ('Active', 'Fulfilled')
                for _pda, s in client.get_swaps(status=status)
                if str(s.miner) == target
            ]
    except SolanaClientError as e:
        console.print(f'[yellow]Could not read active swaps: {e}[/yellow]')
        swaps = []

    if not swaps:
        console.print('[dim]No active swaps[/dim]\n')
        return

    swap_table = Table(title='Active Swaps', show_header=True)
    swap_table.add_column('Swap Key', style='cyan')
    swap_table.add_column('Pair', style='green')
    swap_table.add_column('Amount', style='yellow')
    swap_table.add_column('Status', style='bold')

    for swap in swaps:
        swap_table.add_row(
            swap.key_hex[:16],
            f'{swap.from_chain.upper()}/{swap.to_chain.upper()}',
            str(swap.from_amount),
            swap.status,
        )

    console.print(swap_table)
    console.print(f'\n[dim]Total: {len(swaps)} active swaps[/dim]\n')


@miner_group.command('activate')
@click.option(
    '--backing',
    default=None,
    type=str,
    help='Which purse to light: sol | tao. Inferred when only one is funded and dark.',
)
def miner_activate(backing: str):
    """Activate one of your purses via dendrite broadcast to all validators.

    [dim]Purses activate separately, so this lights one. Without --backing it picks the single
    funded, not-yet-serving purse; with two candidates it asks you to name one. Each validator
    independently re-checks that purse before voting, and activation requires quorum.[/dim]

    [dim]Examples:
        $ alw miner activate
        $ alw miner activate --backing tao[/dim]
    """
    import bittensor as bt

    from allways.synapses import MinerActivateSynapse

    config, wallet, subtensor, _ = get_cli_context(need_client=False)
    _, client = get_solana_cli_context()
    netuid = config['netuid']
    hotkey = wallet.hotkey.ss58_address

    def _miner_pubkey():
        """The miner's Solana identity: hotkey → HotkeyBinding → pubkey. None when unbound."""
        try:
            hk_bytes = bytes.fromhex(bt.Keypair(ss58_address=hotkey).public_key.hex())
            binding = client.get_hotkey_binding(hk_bytes)
            return None if binding is None else binding.miner
        except Exception:
            return None

    def _is_lit(target, want: str) -> bool:
        """Whether THAT purse is serving. The `active` OR view would call a SOL-serving miner done
        the moment it asks for its TAO purse, which is the one question this has to answer."""
        try:
            ms = client.get_miner_state(target)
            return bool(ms) and bool(int(getattr(ms, 'active_backings', 0) or 0) & BACKING_BITS[want])
        except Exception:
            return False

    console.print(f'\n[bold]Miner Activate: {hotkey[:16]}...[/bold]\n')

    miner_pk = _miner_pubkey()
    if miner_pk is None:
        fail('Your hotkey is not bound to a Solana pubkey yet — run `alw miner bind-hotkey` first.')
    try:
        with loading('Reading your purses...'):
            miner_state = client.get_miner_state(miner_pk)
            states = purse_states(client, miner_pk, miner_state, client.get_config())
    except SolanaClientError as e:
        fail(f'Failed to read miner data: {e}')
    if miner_state is None:
        fail('You have no on-chain miner state yet — run `alw collateral deposit` first.')

    backing = resolve_activation_backing(states, backing)
    console.print(f'Lighting your [cyan]{backing.upper()}[/cyan] purse.\n')

    # Discover serving validators from metagraph (on-chain whitelist enforced at vote time)
    dendrite = bt.Dendrite(wallet=wallet)
    with loading('Discovering validators...'):
        validator_axons = discover_validators(subtensor, netuid)

    if not validator_axons:
        fail('No validators found on metagraph')

    # Broadcast, re-signing a fresh timestamp each attempt. On a 429 the request
    # was rejected at the edge proxy and never reached the validator — back off
    # and retry rather than surfacing a transient rate-limit as a failure.
    timeout = resolve_dendrite_timeout(60.0)
    activate_max_retries = 2
    for attempt in range(activate_max_retries + 1):
        timestamp = str(int(time.time()))
        message = f'activate:{hotkey}:{timestamp}'
        signature = wallet.hotkey.sign(message.encode()).hex()
        synapse = MinerActivateSynapse(hotkey=hotkey, signature=signature, message=message, backing=backing)

        with loading(f'Broadcasting activation to {len(validator_axons)} validators...'):
            responses = asyncio.get_event_loop().run_until_complete(
                dendrite(axons=validator_axons, synapse=synapse, deserialize=False, timeout=timeout)
            )

        info = render_and_aggregate(console, responses, label='V', context={'miner_hotkey': hotkey})

        if info.category == 'rate_limited' and attempt < activate_max_retries:
            backoff_s = 6
            with console.status(f'[yellow]Rate limited by validator(s) — retrying in {backoff_s}s...[/yellow]'):
                time.sleep(backoff_s)
            continue
        break

    accepted = info.accepted
    no_response = info.no_response
    if accepted == 0 and info.headline:
        console.print(f'\n[red]{info.headline}[/red]')
    console.print(f'\n{accepted}/{len(validator_axons)} validators accepted')

    # Poll chain — the on-chain flag is authoritative. A validator can
    # finalize vote_activate after the dendrite response times out, so
    # synapse accepted counts aren't reliable on their own.
    activated = False
    with loading('Checking on-chain activation...'):
        for _ in range(15):
            time.sleep(2)
            if _is_lit(miner_pk, backing):
                activated = True
                break

    if activated:
        console.print(f'[green]{backing.upper()} purse activated successfully[/green]\n')
        return

    if accepted == 0 and no_response == len(validator_axons):
        console.print('[dim]The chain may be slow — the vote could still land after this check.[/dim]')
        console.print('[dim]Retry with a longer timeout: ALW_DENDRITE_TIMEOUT=90 alw miner activate[/dim]')
        console.print('[dim]Or re-run `alw miner status` in a minute to see if activation completed.[/dim]')
    elif accepted == 0 and info.category in ('', 'mixed', 'unmatched'):
        # Translator couldn't pin down a single cause — fall back to the prerequisites checklist.
        console.print('[dim]Prerequisites for activation:[/dim]')
        console.print('[dim]  - Hotkey registered on this subnet (btcli subnets register)[/dim]')
        for line in activation_prerequisites(backing):
            console.print(f'[dim]  - {line}[/dim]')
        console.print(
            '[dim]  - Quotes come AFTER activation: alw miner post/quotes needs the purse already serving[/dim]'
        )
        console.print('[dim]Run `alw miner status` to see which are missing.[/dim]')
    elif accepted > 0:
        console.print('[dim]Votes submitted but quorum not yet reached. Check status with: alw miner status[/dim]')
    fail('Activation not confirmed on-chain.')


@miner_group.command('deactivate')
@click.option(
    '--backing',
    default=None,
    type=str,
    help='Leave one purse only (sol|tao), keeping the rest trading. Default: leave every purse.',
)
def miner_deactivate(backing: str):
    """Deactivate miner directly on the program (permissionless self-deactivate).

    [dim]Calls deactivate() — no validator needed. Without --backing this is the full exit: every
    purse goes dark. With --backing only that purse stops serving; your quotes on the others keep
    trading. The SOL withdrawal cooldown (2 * fulfillment_timeout) starts when the SOL purse drops —
    a TAO-side exit is gated on the vault's own unlock path instead.[/dim]

    [dim]Examples:
        $ alw miner deactivate
        $ alw miner deactivate --backing tao[/dim]
    """
    _, client = get_solana_cli_context()
    pubkey = client.keypair.pubkey()

    console.print(f'\n[bold]Miner Deactivate: {str(pubkey)[:16]}...[/bold]\n')

    if backing is not None:
        backing = backing.lower()
        if backing not in BACKING_BITS:
            fail(f'--backing must be one of: {", ".join(BACKING_BITS)} (got "{backing}")')

    # Pre-flight: the program guards deactivate() on no-active-swap + past busy_until (both global —
    # one swap at a time spans purses).
    try:
        now = int(time.time())
        ms = client.get_miner_state(pubkey)
        if ms is None or not ms.active:
            console.print('[yellow]Miner is not active.[/yellow]\n')
            return
        if backing is not None and not int(ms.active_backings) & BACKING_BITS[backing]:
            console.print(f'[yellow]Your {backing.upper()} purse is already not serving.[/yellow]\n')
            return
        if ms.has_active_swap:
            console.print('[dim]Wait for it to complete or time out, then try again.[/dim]')
            fail('Cannot deactivate: you have an active swap.')
        if ms.busy_until > now:
            remaining = ms.busy_until - now
            fail(f'Cannot deactivate: you are busy (open pool / held reservation), ~{remaining}s left.')
    except SolanaClientError as e:
        fail(f'Failed to read miner state: {e}')

    try:
        with loading('Submitting transaction...'):
            sig = client.deactivate(backing=backing)
        what = f'{backing.upper()} purse deactivated' if backing else 'Deactivated successfully'
        console.print(f'[green]{what}[/green] (sig: {sig[:16]}...)')
        config = client.get_config()
        if config is not None and backing != 'tao':
            cooldown = config.fulfillment_timeout_secs * 2
            console.print(f'[dim]Collateral withdrawal available after {cooldown}s (~{cooldown // 60} min)[/dim]\n')
        elif backing == 'tao':
            console.print(
                '[dim]Your SOL purse keeps trading. Unlocking the TAO bond is a separate,'
                ' validator-quorumed step on the vault (`alw vault status`).[/dim]\n'
            )
    except SolanaClientError as e:
        fail(f'Failed to deactivate: {e}')


@miner_group.command('mark-fulfilled')
@click.option('--from-tx-hash', required=True, type=str, help='Source-chain tx hash that identifies the swap')
@click.option('--to-tx-hash', required=True, type=str, help='Destination chain transaction hash')
@click.option('--to-tx-block', default=0, type=int, help='Destination chain block number (0 = validators scan)')
@click.option('--yes', '-y', is_flag=True, help='Skip confirmation prompt')
def miner_mark_fulfilled(from_tx_hash: str, to_tx_hash: str, to_tx_block: int, yes: bool):
    """Manually mark a swap as fulfilled on the program.

    [dim]Use this when you've sent destination funds manually and need to notify the program.
    The swap is identified by its source tx hash (swap_key = keccak(from_tx_hash)). The payout
    amount is the pinned reservation value — not operator-supplied.[/dim]

    [dim]Examples:
        $ alw miner mark-fulfilled --from-tx-hash abc... --to-tx-hash def...[/dim]
    """
    _, client = get_solana_cli_context()
    pubkey = client.keypair.pubkey()
    swap_key = swap_key_from_tx_hash(from_tx_hash)

    # Preflight: the program rejects mark_fulfilled unless status == Active and the swap is ours.
    try:
        acct = client.get_swap(swap_key)
    except SolanaClientError as e:
        fail(f'Failed to read swap: {e}')
    if acct is None:
        fail(f'Swap {swap_key.hex()[:16]} not found on-chain.')
    swap = swap_from_solana(acct, swap_key)

    if str(swap.miner) != str(pubkey):
        fail(f'Swap {swap.key_hex[:16]} is assigned to a different miner, not you.')
    if swap.status != 'Active':
        console.print('[dim]mark_fulfilled is only accepted while the swap is Active.[/dim]')
        fail(f'Swap {swap.key_hex[:16]} is not Active — current status: {swap.status}.')

    console.print(f'\n[bold]Mark Fulfilled — {swap.key_hex[:16]}[/bold]\n')
    console.print(f'  Pair:        {swap.from_chain.upper()} → {swap.to_chain.upper()}')
    console.print(f'  Payout:      {swap.to_amount} (pinned)')
    console.print(f'  Dest tx:     {to_tx_hash}')
    console.print(f'  Dest block:  {to_tx_block if to_tx_block else "(validators will scan)"}\n')

    if not yes and not click.confirm('Confirm marking swap as fulfilled?'):
        console.print('[yellow]Cancelled[/yellow]')
        return

    try:
        with loading('Submitting transaction...'):
            sig = client.mark_fulfilled(swap_key=swap_key, to_tx_hash=to_tx_hash, to_tx_block=to_tx_block)
        console.print(f'[green]Swap {swap.key_hex[:16]} marked as fulfilled[/green] (sig: {sig[:16]}...)\n')
    except SolanaClientError as e:
        fail(f'Failed to mark fulfilled: {e}')
