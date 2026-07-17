"""alw view - Inspect miners, rates, swaps, validators, config, and reservations.

All views read the on-chain Solana program: `config`/`validators` read the Config account; `miners`/`rates`
aggregate MinerQuote + MinerState; `active-swaps`/`swap` read Swap accounts; `reservation` reads a per-miner
Reservation. Reads need no keypair. Per-miner reliability is keyed by bittensor hotkey (not the Solana miner
pubkey these views key on), so it is intentionally omitted here rather than shown against the wrong key.
"""

import json
import time

import click
from rich.table import Table
from rich.text import Text
from solders.pubkey import Pubkey

from allways.chains import SUPPORTED_CHAINS, get_chain
from allways.cli.help import StyledGroup
from allways.cli.swap_commands.helpers import (
    FINITE_FLOAT,
    PENDING_SWAP_FILE,
    STATUS_SORT_ORDER,
    STATUS_STYLES,
    ZERO_SWAP_KEY,
    console,
    fail,
    from_lamports,
    get_solana_cli_context,
    load_miner_book,
    miner_runtime_status,
    print_json,
    safe_read,
    secs_str,
    set_json_output,
)
from allways.cli.swap_commands.swap_intake import rate_display_from_fixed
from allways.solana.client import swap_from_solana
from allways.utils.rate import directional_rate

MINER_SORT_FIELDS = ['uid', 'rate', 'capacity', 'status']
MINER_STATUS_CHOICES = ['available', 'offline', 'in-swap', 'reserved', 'cooldown']
RATES_SORT_FIELDS = ['rate', 'capacity', 'pair', 'uid']
# On-chain Swap accounts only ever hold these live statuses; completed/timed-out swaps are closed on-chain.
SWAP_STATUS_CHOICES = ['active', 'fulfilled', 'pending-attestation']
_SWAP_STATUS_VARIANTS = {'active': 'Active', 'fulfilled': 'Fulfilled', 'pending-attestation': 'PendingAttestation'}


@click.group('view', cls=StyledGroup)
def view_group():
    """View swaps, miners, and rates."""
    pass


def _short(s: str, full: bool, n: int = 8) -> str:
    return s if full else (f'{s[:n]}…' if len(s) > n else s)


def _quote_dir(q) -> str:
    return f'{q.from_chain.upper()}→{q.to_chain.upper()}'


def _rate(q) -> str:
    """Directional 'to per 1 from' rate for display — see utils.rate.directional_rate."""
    return directional_rate(q.from_chain, q.to_chain, rate_display_from_fixed(q.rate))


def _max_rate(entry) -> float:
    """Highest DIRECTIONAL rate the miner posts (coarse cross-direction proxy for `--sort rate`),
    matching the numbers the rate column renders."""
    rates = []
    for q in entry.quotes:
        try:
            rates.append(float(_rate(q)))
        except (TypeError, ValueError):
            continue
    return max(rates) if rates else 0.0


@view_group.command('miners')
@click.option('--full', is_flag=True, help='Show untruncated pubkeys and addresses')
@click.option(
    '--sort',
    'sort_by',
    type=click.Choice(MINER_SORT_FIELDS, case_sensitive=False),
    default='uid',
    show_default=True,
    help='Sort field. uid = stable pubkey order; rate/capacity descend; status groups '
    'available→reserved→in-swap→cooldown→offline.',
)
@click.option(
    '--status',
    'status_filter',
    type=click.Choice(MINER_STATUS_CHOICES, case_sensitive=False),
    default=None,
    help='Only show miners in a given runtime state.',
)
@click.option(
    '--min-capacity', type=FINITE_FLOAT, default=None, help='Only show miners with at least this much collateral (SOL).'
)
@click.option(
    '--search',
    default=None,
    type=str,
    help='Case-insensitive substring match against pubkey and posted addresses.',
)
@click.option('--json', 'as_json', is_flag=True, help='Emit machine-readable JSON instead of a table.')
def view_miners(full, sort_by, status_filter, min_capacity, search, as_json):
    """List miners with their runtime status, collateral, and posted directions/rates.

    [dim]Miners are Solana pubkeys (no metagraph uid here); '#' is a display index and `--sort uid` means
    stable pubkey order.[/dim]"""
    set_json_output(as_json)
    _, client = get_solana_cli_context(need_keypair=False)
    now = int(time.time())
    book = load_miner_book(client, with_reservation=True)

    rows = []
    for e in book:
        status = miner_runtime_status(e.state, e.reservation, now)
        addrs = ' '.join(f'{q.miner_from_addr} {q.miner_to_addr}' for q in e.quotes)
        rows.append((e, status, addrs))

    if status_filter:
        rows = [r for r in rows if r[1] == status_filter.lower()]
    if min_capacity is not None:
        rows = [r for r in rows if from_lamports(r[0].collateral) >= min_capacity]
    if search:
        needle = search.lower()
        rows = [r for r in rows if needle in (r[0].pubkey_str + ' ' + r[2]).lower()]

    if sort_by == 'uid':
        rows.sort(key=lambda r: r[0].pubkey_str)
    elif sort_by == 'rate':
        rows.sort(key=lambda r: _max_rate(r[0]), reverse=True)
    elif sort_by == 'capacity':
        rows.sort(key=lambda r: r[0].collateral, reverse=True)
    elif sort_by == 'status':
        rows.sort(key=lambda r: STATUS_SORT_ORDER.index(r[1]))

    if as_json:
        print_json(
            [
                {
                    'miner': e.pubkey_str,
                    'status': status,
                    'collateral_sol': from_lamports(e.collateral),
                    'quotes': [
                        {
                            'from': q.from_chain,
                            'to': q.to_chain,
                            'rate': _rate(q),
                            'miner_from_addr': q.miner_from_addr,
                            'miner_to_addr': q.miner_to_addr,
                        }
                        for q in e.quotes
                    ],
                }
                for e, status, _addrs in rows
            ]
        )
        return

    if not rows:
        console.print('[yellow]No miners match those filters.[/yellow]')
        return

    table = Table(title=f'Miners ({len(rows)})', show_header=True, show_lines=True)
    table.add_column('#', style='dim', justify='right')
    table.add_column('Miner', style='cyan')
    table.add_column('Status')
    table.add_column('Collateral', style='green', justify='right')
    table.add_column('Directions', style='white')
    for i, (e, status, _addrs) in enumerate(rows, 1):
        dirs = Text()
        for j, q in enumerate(e.quotes):
            if j:
                dirs.append('\n')
            dirs.append(f'{_quote_dir(q)} @ {_rate(q)}')
            if full:
                dirs.append(f'  ({q.miner_from_addr}→{q.miner_to_addr})', style='dim')
        table.add_row(
            str(i),
            _short(e.pubkey_str, full),
            Text(status, style=STATUS_STYLES.get(status, 'white')),
            f'{from_lamports(e.collateral):.4f} SOL',
            dirs,
        )
    console.print(table)


def _parse_pair(pair: str):
    """Parse `sol-btc` → ('sol','btc'). Fail (non-zero) on malformed/unknown pair."""
    parts = pair.lower().split('-')
    if len(parts) != 2 or parts[0] not in SUPPORTED_CHAINS or parts[1] not in SUPPORTED_CHAINS:
        fail(f'Invalid --pair {pair!r}. Use <from>-<to> with supported chains, e.g. sol-btc.')
    return parts[0], parts[1]


@view_group.command('rates')
@click.option('--pair', default=None, type=str, help='Filter by direction (e.g. sol-btc)')
@click.option('--full', is_flag=True, help='Show untruncated pubkeys and addresses')
@click.option(
    '--sort',
    'sort_by',
    type=click.Choice(RATES_SORT_FIELDS, case_sensitive=False),
    default='rate',
    show_default=True,
    help='Sort field. rate descends; capacity descends; pair groups by direction; uid = stable pubkey order.',
)
@click.option(
    '--min-capacity', type=FINITE_FLOAT, default=None, help='Only show miners with at least this much collateral (SOL).'
)
@click.option(
    '--search',
    default=None,
    type=str,
    help='Case-insensitive substring match against pubkey and posted addresses.',
)
@click.option('--json', 'as_json', is_flag=True, help='Emit machine-readable JSON instead of a table.')
def view_rates(pair, full, sort_by, min_capacity, search, as_json):
    """Show posted miner rates, one row per direction."""
    set_json_output(as_json)
    want = _parse_pair(pair) if pair else None
    _, client = get_solana_cli_context(need_keypair=False)
    now = int(time.time())
    book = load_miner_book(client, with_reservation=True)

    rows = []  # (entry, quote, status)
    for e in book:
        status = miner_runtime_status(e.state, e.reservation, now)
        if min_capacity is not None and from_lamports(e.collateral) < min_capacity:
            continue
        for q in e.quotes:
            if want and (q.from_chain, q.to_chain) != want:
                continue
            if search:
                needle = search.lower()
                hay = f'{e.pubkey_str} {q.miner_from_addr} {q.miner_to_addr}'.lower()
                if needle not in hay:
                    continue
            rows.append((e, q, status))

    if sort_by == 'rate':
        rows.sort(key=lambda r: float(_rate(r[1]) or 0), reverse=True)
    elif sort_by == 'capacity':
        rows.sort(key=lambda r: r[0].collateral, reverse=True)
    elif sort_by == 'pair':
        rows.sort(key=lambda r: (r[1].from_chain, r[1].to_chain))
    elif sort_by == 'uid':
        rows.sort(key=lambda r: r[0].pubkey_str)

    if as_json:
        print_json(
            [
                {
                    'miner': e.pubkey_str,
                    'from': q.from_chain,
                    'to': q.to_chain,
                    'rate': _rate(q),
                    'collateral_sol': from_lamports(e.collateral),
                    'status': status,
                    'miner_from_addr': q.miner_from_addr,
                    'miner_to_addr': q.miner_to_addr,
                }
                for e, q, status in rows
            ]
        )
        return

    if not rows:
        console.print('[yellow]No posted rates match those filters.[/yellow]')
        return

    table = Table(title=f'Posted Rates ({len(rows)})', show_header=True)
    table.add_column('Direction', style='cyan')
    table.add_column('Rate (to/from)', style='green', justify='right')
    table.add_column('Miner', style='white')
    table.add_column('Collateral', style='green', justify='right')
    table.add_column('Status')
    if full:
        table.add_column('Addresses', style='dim')
    for e, q, status in rows:
        cells = [
            _quote_dir(q),
            _rate(q),
            _short(e.pubkey_str, full),
            f'{from_lamports(e.collateral):.4f} SOL',
            Text(status, style=STATUS_STYLES.get(status, 'white')),
        ]
        if full:
            cells.append(f'{q.miner_from_addr} → {q.miner_to_addr}')
        table.add_row(*cells)
    console.print(table)


def _swap_json(s):
    return {
        'swap_key': s.key_hex,
        'miner': str(s.miner),
        'user': str(s.user),
        'from_chain': s.from_chain,
        'to_chain': s.to_chain,
        'from_amount': s.from_amount,
        'to_amount': s.to_amount,
        'collateral_amount': s.collateral_amount,
        'status': s.status,
        'from_tx_hash': s.from_tx_hash,
        'to_tx_hash': s.to_tx_hash,
        'initiated_at': s.initiated_at,
        'timeout_at': s.timeout_at,
        'fulfilled_at': s.fulfilled_at,
    }


@view_group.command('active-swaps')
@click.option(
    '--status',
    'status_filter',
    default=None,
    type=click.Choice(SWAP_STATUS_CHOICES, case_sensitive=False),
    help='Filter by on-chain status (active, fulfilled, pending-attestation).',
)
@click.option('--json', 'as_json', is_flag=True, help='Emit machine-readable JSON instead of a table.')
def view_active_swaps(status_filter, as_json):
    """List swaps currently open on-chain (completed/timed-out swaps are closed and not listed)."""
    set_json_output(as_json)
    _, client = get_solana_cli_context(need_keypair=False)
    variant = _SWAP_STATUS_VARIANTS.get(status_filter.lower()) if status_filter else None
    raw = safe_read(lambda: client.get_swaps(status=variant), what='read swaps')
    swaps = [swap_from_solana(s) for _pk, s in raw]

    if as_json:
        print_json([_swap_json(s) for s in swaps])
        return

    if not swaps:
        console.print('[yellow]No swaps currently open on-chain.[/yellow]')
        return

    table = Table(title=f'Open Swaps ({len(swaps)})', show_header=True)
    table.add_column('Swap Key', style='cyan')
    table.add_column('Pair', style='green')
    table.add_column('From Amt', justify='right')
    table.add_column('To Amt', justify='right')
    table.add_column('Status', style='bold')
    for s in swaps:
        table.add_row(
            s.key_hex[:16],
            f'{s.from_chain.upper()}→{s.to_chain.upper()}',
            str(s.from_amount),
            str(s.to_amount),
            s.status,
        )
    console.print(table)


def _render_swap_detail(s):
    to_dec = get_chain(s.to_chain).decimals
    from_dec = get_chain(s.from_chain).decimals
    console.print(f'\n[bold]Swap {s.key_hex[:16]}[/bold]\n')
    console.print(f'  Status:      [bold]{s.status}[/bold]')
    console.print(f'  Pair:        {s.from_chain.upper()} → {s.to_chain.upper()}')
    console.print(f'  Miner:       {s.miner}')
    console.print(f'  User:        {s.user}')
    console.print(f'  Send:        {s.from_amount / 10**from_dec:g} {s.from_chain.upper()}')
    console.print(f'  Receive:     {s.to_amount / 10**to_dec:g} {s.to_chain.upper()} (pinned payout)')
    console.print(f'  User from:   {s.user_from_addr}')
    console.print(f'  Miner to:    {s.miner_to_addr}')
    console.print(f'  Source tx:   {s.from_tx_hash or "[dim](none)[/dim]"}')
    console.print(f'  Dest tx:     {s.to_tx_hash or "[dim](none)[/dim]"}')
    console.print(f'  Initiated:   {s.initiated_at}')
    console.print(f'  Timeout at:  {s.timeout_at}')
    console.print(f'  Fulfilled:   {s.fulfilled_at or "[dim](not yet)[/dim]"}\n')


@view_group.command('swap')
@click.argument('swap_key_hex', type=str)
@click.option('--watch', '-w', is_flag=True, help='Poll and refresh until swap completes or times out')
@click.option('--json', 'as_json', is_flag=True, help='Emit machine-readable JSON instead of detail text.')
def view_swap(swap_key_hex: str, watch: bool, as_json: bool):
    """Inspect a single swap by its 32-byte hex swap_key."""
    set_json_output(as_json)
    try:
        key = bytes.fromhex(swap_key_hex)
    except ValueError:
        fail(f'Invalid swap_key {swap_key_hex!r}: expected hex (the 32-byte swap_key, not an integer id).')
    if len(key) != 32:
        # A truncated paste must not read as "swap finished" — it never existed under that key.
        fail(f'Invalid swap_key: expected 32 bytes (64 hex chars), got {len(key)}.')

    _, client = get_solana_cli_context(need_keypair=False)

    def _load():
        acct = safe_read(lambda: client.get_swap(key), what='read swap')
        return swap_from_solana(acct, key) if acct is not None else None

    s = _load()
    if s is None:
        # Not an error: Swap accounts close at resolution, so a missing account usually MEANS
        # "finished". Say so in both modes instead of failing a normal terminal probe.
        note = (
            'No swap account on-chain for this swap_key — swaps close when resolved '
            '(Completed or TimedOut), so this swap either finished or never existed.'
        )
        if as_json:
            print_json({'found': False, 'note': note})
        else:
            console.print(f'[yellow]{note}[/yellow]')
        return

    if as_json:
        print_json(_swap_json(s))
        return

    if not watch:
        _render_swap_detail(s)
        return

    terminal = {'PendingAttestation'}
    while True:
        console.clear()
        _render_swap_detail(s)
        if s.status in terminal:
            console.print('[dim]Swap reached a terminal on-chain status.[/dim]')
            return
        time.sleep(5)
        s = _load()
        if s is None:
            console.print('[dim]Swap account closed (resolved and cleaned up on-chain).[/dim]')
            return


def _sol_or(amount: int, zero_label: str) -> str:
    return f'{from_lamports(amount):.4f} SOL' + (f' ({zero_label})' if amount == 0 else '')


def _votes_needed(cfg) -> int:
    """Votes required for consensus — mirrors the program's headcount check
    (consensus.rs: votes*100 >= threshold*total), i.e. ceil(threshold*total/100)."""
    total = len(cfg.validators)
    return -(-cfg.consensus_threshold_percent * total // 100)


def _threshold_line(cfg) -> str:
    return f'{cfg.consensus_threshold_percent}% ({_votes_needed(cfg)} of {len(cfg.validators)} validator votes)'


@view_group.command('config')
@click.option('--json', 'as_json', is_flag=True, help='Emit machine-readable JSON instead of text.')
def view_config(as_json):
    """Show the current on-chain program Config (bounds, fees, windows, threshold). Read-only."""
    set_json_output(as_json)
    _, client = get_solana_cli_context(need_keypair=False)
    cfg = safe_read(lambda: client.get_config(), what='read config')
    if cfg is None:
        print_json({'initialized': False}) if as_json else console.print(
            '[yellow]Program is not initialized (no Config account).[/yellow]'
        )
        return
    if as_json:
        print_json(
            {
                'admin': str(cfg.admin),
                'version': cfg.version,
                'halted': bool(cfg.halted),
                'consensus_threshold_percent': cfg.consensus_threshold_percent,
                'votes_needed': _votes_needed(cfg),
                'reservation_fee_sol': from_lamports(cfg.reservation_fee_lamports),
                'min_collateral_sol': from_lamports(cfg.min_collateral),
                'max_collateral_sol': from_lamports(cfg.max_collateral),
                'min_swap_amount_sol': from_lamports(cfg.min_swap_amount),
                'max_swap_amount_sol': from_lamports(cfg.max_swap_amount),
                'fulfillment_timeout_secs': cfg.fulfillment_timeout_secs,
                'reservation_ttl_secs': cfg.reservation_ttl_secs,
                'pool_window_secs': cfg.pool_window_secs,
                'weights_update_min_interval_secs': cfg.weights_update_min_interval_secs,
                'max_total_extension_secs': cfg.max_total_extension_secs,
                'validator_count': len(cfg.validators),
            }
        )
        return
    console.print('\n[bold]On-chain Program Config[/bold] [dim](read-only — set by the program admin)[/dim]\n')
    console.print(f'  Admin:                {cfg.admin}')
    console.print(f'  Version:              {cfg.version}')
    console.print(f'  Halted:               {"yes" if cfg.halted else "no"}')
    console.print(f'  Consensus threshold:  {_threshold_line(cfg)}')
    console.print(f'  Reservation fee:      {from_lamports(cfg.reservation_fee_lamports):.6f} SOL')
    console.print(f'  Min collateral:       {from_lamports(cfg.min_collateral):.4f} SOL')
    console.print(f'  Max collateral:       {_sol_or(cfg.max_collateral, "unlimited")}')
    console.print(f'  Min swap amount:      {_sol_or(cfg.min_swap_amount, "no minimum")}')
    console.print(f'  Max swap amount:      {_sol_or(cfg.max_swap_amount, "no maximum")}')
    console.print(f'  Fulfillment timeout:  {secs_str(cfg.fulfillment_timeout_secs)}')
    console.print(f'  Reservation TTL:      {secs_str(cfg.reservation_ttl_secs)}')
    console.print(f'  Pool window:          {secs_str(cfg.pool_window_secs)}')
    console.print(f'  Weights interval:     {secs_str(cfg.weights_update_min_interval_secs)}')
    console.print(f'  Max total extension:  {secs_str(cfg.max_total_extension_secs)}')
    console.print(f'  Validators:           {len(cfg.validators)}\n')
    console.print('[dim]Local CLI settings: `alw config`[/dim]\n')


@view_group.command('validators')
@click.option('--json', 'as_json', is_flag=True, help='Emit machine-readable JSON instead of text.')
def view_validators(as_json):
    """List the registered validators with their lottery weights and the consensus threshold."""
    set_json_output(as_json)
    _, client = get_solana_cli_context(need_keypair=False)
    cfg = safe_read(lambda: client.get_config(), what='read config')
    if cfg is None:
        print_json({'initialized': False, 'validators': []}) if as_json else console.print(
            '[yellow]Program is not initialized (no Config account).[/yellow]'
        )
        return
    validators = [{'pubkey': str(Pubkey.from_bytes(bytes(v.key))), 'weight': v.weight} for v in cfg.validators]
    if as_json:
        print_json(
            {
                'consensus_threshold_percent': cfg.consensus_threshold_percent,
                'votes_needed': _votes_needed(cfg),
                'validators': validators,
            }
        )
        return
    console.print('\n[bold]Validators[/bold]\n')
    console.print(f'  Consensus threshold: {_threshold_line(cfg)}\n')
    if not validators:
        console.print('  [yellow]No validators registered.[/yellow]\n')
        return
    for v in validators:
        console.print(f'  {v["pubkey"]}  weight={v["weight"]}')
    console.print()


def _pending_miner():
    """Return the miner pubkey string saved by `alw swap now` in pending_swap.json, or None."""
    if not PENDING_SWAP_FILE.exists():
        return None
    try:
        return json.loads(PENDING_SWAP_FILE.read_text()).get('miner')
    except (json.JSONDecodeError, OSError):
        return None


@view_group.command('reservation')
@click.option('--miner', 'miner_pk', default=None, type=str, help='Miner pubkey whose reservation to inspect')
@click.option('--json', 'as_json', is_flag=True, help='Emit machine-readable JSON instead of detail text.')
def view_reservation(miner_pk, as_json):
    """Show the reservation held on a miner (reservations are keyed by the miner pubkey)."""
    set_json_output(as_json)
    target = miner_pk or _pending_miner()
    if not target:
        fail('No miner specified and no saved swap found. Pass --miner <pubkey>.')
    try:
        miner = Pubkey.from_string(target)
    except (ValueError, TypeError):
        fail(f'Invalid miner pubkey: {target}')

    _, client = get_solana_cli_context(need_keypair=False)
    resv = safe_read(lambda: client.get_reservation(miner), what='read reservation')

    if resv is None:
        if as_json:
            print_json({'miner': str(miner), 'reservation': None})
            return
        console.print(f'[yellow]No active reservation on miner {target}.[/yellow]')
        return

    now = int(time.time())
    remaining = max(0, int(resv.reserved_until) - now)
    claimed = bytes(resv.claimed_swap_key) != ZERO_SWAP_KEY
    to_dec = get_chain(resv.to_chain).decimals
    from_dec = get_chain(resv.from_chain).decimals

    if as_json:
        print_json(
            {
                'miner': str(miner),
                'user': str(resv.user),
                'from_chain': resv.from_chain,
                'to_chain': resv.to_chain,
                'from_amount': resv.from_amount,
                'to_amount': resv.to_amount,
                'collateral_amount': resv.collateral_amount,
                'reserved_until': int(resv.reserved_until),
                'remaining_secs': remaining,
                'deposit_claimed': claimed,
                'miner_from_addr': resv.miner_from_addr,
            }
        )
        return

    console.print('\n[bold]Reservation[/bold]\n')
    console.print(f'  Miner:       {miner}')
    console.print(f'  User:        {resv.user}')
    console.print(f'  Pair:        {resv.from_chain.upper()} → {resv.to_chain.upper()}')
    console.print(f'  Send:        {resv.from_amount / 10**from_dec:g} {resv.from_chain.upper()}')
    console.print(f'  Receive:     {resv.to_amount / 10**to_dec:g} {resv.to_chain.upper()}')
    console.print(f'  Send to:     {resv.miner_from_addr}')
    console.print(f'  Reserved:    {"expired" if remaining == 0 else secs_str(remaining) + " remaining"}')
    console.print(f'  Deposit:     {"claimed" if claimed else "[dim]not yet sent[/dim]"}\n')
