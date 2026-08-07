"""alw vault - TAO bond vault commands (Bittensor side of the v2 split-collateral design).

Talks to the `allways_bond_vault` ink! contract: the permissionless fee recycle, miner
bond operations (signed by the wallet HOTKEY — the vault keys bonds by hotkey, joined to
the Solana pubkey via the A5 binding), and the owner admin setters. Validator relay
rounds (vote_unlock / vote_slash / vote_collect_fees_batch) belong to the validator
loop, not the CLI.

Configuration: `alw config set vault-address <ss58>` (or ALLWAYS_VAULT_ADDRESS env);
metadata JSON path via vault-metadata / ALLWAYS_VAULT_METADATA, defaulting to the
in-repo build artifact. ALLWAYS_VAULT_SURI overrides the signer for scripting
(e.g. //Alice on a localnet).
"""

import json
import os
from pathlib import Path

import click

from allways.cli.help import StyledGroup
from allways.cli.swap_commands.helpers import console, fail, get_cli_context, loading
from allways.constants import TAO_TO_RAO

# Repo-relative default: the cargo-contract build artifact.
_DEFAULT_METADATA = (
    Path(__file__).resolve().parents[3]
    / 'smart-contracts' / 'ink-bond-vault' / 'target' / 'ink' / 'allways_bond_vault.json'
)

# Dry-runs use a generous fixed budget; actual charge is by weight used.
_GAS = {'ref_time': 300_000_000_000, 'proof_size': 2_000_000}


# ─── Metadata / encoding helpers ─────────────────────────────────────────────

def _vault_address(config) -> str:
    addr = os.environ.get('ALLWAYS_VAULT_ADDRESS') or config.get('vault-address')
    if not addr:
        fail('Vault address not configured — `alw config set vault-address <ss58>` or set ALLWAYS_VAULT_ADDRESS')
    return addr


def _selectors() -> dict:
    """{message label: selector bytes} from the ink! metadata JSON."""
    path = os.environ.get('ALLWAYS_VAULT_METADATA') or str(_DEFAULT_METADATA)
    try:
        meta = json.loads(Path(path).read_text())
    except OSError as e:
        fail(f'Vault metadata not readable at {path} ({e}) — build the contract or set ALLWAYS_VAULT_METADATA')
    return {
        m['label']: bytes.fromhex(m['selector'][2:])
        for m in meta['spec']['messages']
    }


def _account_bytes(ss58: str) -> bytes:
    import bittensor as bt
    try:
        return bytes(bt.Keypair(ss58_address=ss58).public_key)
    except Exception:
        fail(f'Not a valid ss58 address: {ss58}')


def _u64(n: int) -> bytes:
    return int(n).to_bytes(8, 'little')


def _compact(n: int) -> bytes:
    # SCALE compact for the small lengths we emit (< 2**14 is plenty).
    if n < 64:
        return bytes([n << 2])
    return ((n << 2) | 0b01).to_bytes(2, 'little')


def _msg(sel: dict, label: str, *args: bytes) -> bytes:
    if label not in sel:
        fail(f'Message `{label}` missing from vault metadata — artifact out of date?')
    return sel[label] + b''.join(args)


def _signer(wallet, use_coldkey: bool):
    suri = os.environ.get('ALLWAYS_VAULT_SURI')
    if suri:
        import bittensor as bt
        return bt.Keypair.create_from_uri(suri)
    return wallet.coldkey if use_coldkey else wallet.hotkey


# ─── Chain interaction ───────────────────────────────────────────────────────

def _submit(subtensor, keypair, dest: str, data: bytes, value: int = 0):
    call = subtensor.substrate.compose_call(
        call_module='Contracts',
        call_function='call',
        call_params={
            'dest': dest,
            'value': value,
            'gas_limit': _GAS,
            'storage_deposit_limit': None,
            'data': '0x' + data.hex(),
        },
    )
    ext = subtensor.substrate.create_signed_extrinsic(call=call, keypair=keypair)
    return subtensor.substrate.submit_extrinsic(ext, wait_for_inclusion=True)


def _event_names(receipt):
    names = []
    try:
        for ev in receipt.triggered_events:
            v = ev.value if hasattr(ev, 'value') else ev
            e = v.get('event', v) if isinstance(v, dict) else v
            mod = e.get('module_id') or e.get('module') or '?'
            eid = e.get('event_id') or e.get('name') or '?'
            names.append(f'{mod}.{eid}')
    except Exception:
        pass
    return names


def _report(receipt, ok_msg: str):
    names = _event_names(receipt)
    failed = any('ExtrinsicFailed' in n for n in names)
    if failed or not getattr(receipt, 'is_success', True):
        # pallet-contracts surfaces a contract Err as a ContractReverted
        # dispatch error; name it when we can extract it.
        err = None
        try:
            em = receipt.error_message
            err = em.get('name') if isinstance(em, dict) else None
        except Exception:
            pass
        if err == 'ContractReverted':
            console.print('[yellow]The vault rejected the call (contract reverted) — e.g. empty pot, locked bond, or insufficient balance.[/yellow]')
        else:
            console.print(f'[red]Call failed[/red]{f" [dim]({err})[/dim]" if err else ""}')
    else:
        console.print(f'[green]{ok_msg}[/green]')
    if names:
        console.print(f'  [dim]events: {", ".join(names)}[/dim]')
    console.print(f'  [dim]extrinsic: {getattr(receipt, "extrinsic_hash", "?")}[/dim]\n')


def _dry_read(subtensor, origin_ss58: str, dest: str, data: bytes):
    """Best-effort contract read via ContractsApi. Returns raw return bytes or None
    (some node versions' ContractResult isn't decodable client-side)."""
    try:
        res = subtensor.substrate.runtime_call(
            'ContractsApi', 'call',
            {
                'origin': origin_ss58,
                'dest': dest,
                'value': 0,
                'gas_limit': None,
                'storage_deposit_limit': None,
                'input_data': '0x' + data.hex(),
            },
        )
        v = res.value if hasattr(res, 'value') else res
        payload = v['result']['Ok']['data']
        raw = bytes.fromhex(payload[2:]) if isinstance(payload, str) else bytes(payload)
        # ink! wraps returns in Result<T, LangError>: leading 0x00 = Ok.
        return raw[1:] if raw and raw[0] == 0 else None
    except Exception:
        return None


def _fmt_tao(rao: int) -> str:
    return f'{rao / TAO_TO_RAO:.9f} τ'


# ─── Command group ───────────────────────────────────────────────────────────

@click.group('vault', cls=StyledGroup, show_disclaimer=True)
def vault_group():
    """TAO bond vault (Bittensor-side collateral for TAO-hub pairs).

    [dim]Miner ops sign with the wallet hotkey (bonds are keyed by hotkey).
    Set ALLWAYS_VAULT_SURI to override the signer for scripting.
    Balances shown from the vault are GROSS of fees accrued since the last
    settle round — the effective (netted) bond is what the Solana mirror shows.[/dim]"""


@vault_group.command('recycle', show_disclaimer=True)
@click.option('--force', is_flag=True, help='Submit even if the pot reads as empty/unreadable.')
def vault_recycle(force):
    """Drain the settled fee pot into the SN7 pool (permissionless, caller pays ~0.003 τ).

    [dim]The pot only fills at true-up boundaries / exits / slash surpluses — cron this
    at a fixed offset AFTER the true-up cadence; more often is wasted postage.[/dim]
    """
    config, wallet, subtensor, _ = get_cli_context()
    sel = _selectors()
    dest = _vault_address(config)
    keypair = _signer(wallet, use_coldkey=False)

    raw = _dry_read(subtensor, keypair.ss58_address, dest, _msg(sel, 'get_accumulated_fees'))
    if raw is not None and len(raw) >= 8:
        pot = int.from_bytes(raw[:8], 'little')
        console.print(f'  Pot: [bold]{_fmt_tao(pot)}[/bold]')
        if pot == 0 and not force:
            console.print('[yellow]Pot is empty — nothing to recycle (use --force to submit anyway)[/yellow]\n')
            return
    elif not force:
        console.print('[dim]Pot unreadable on this node (dry-run decode); submitting anyway.[/dim]')

    with loading('Submitting recycle_fees...'):
        receipt = _submit(subtensor, keypair, dest, _msg(sel, 'recycle_fees'))
    _report(receipt, 'Recycle submitted — pot staked into the subnet pool')


@vault_group.command('status', show_disclaimer=True)
@click.argument('miner', required=False)
def vault_status(miner):
    """Show vault totals, and a miner's bond/lock/settled state if MINER (ss58) is given."""
    config, wallet, subtensor, _ = get_cli_context()
    sel = _selectors()
    dest = _vault_address(config)
    origin = _signer(wallet, use_coldkey=False).ss58_address

    def read_u64(label, *args):
        raw = _dry_read(subtensor, origin, dest, _msg(sel, label, *args))
        return int.from_bytes(raw[:8], 'little') if raw and len(raw) >= 8 else None

    console.print(f'\n[bold]Vault[/bold] [dim]{dest}[/dim]\n')
    pot = read_u64('get_accumulated_fees')
    if pot is None:
        console.print('[yellow]Reads unavailable on this node (ContractResult decode) — write commands still work.[/yellow]\n')
        return
    console.print(f'  Fee pot (settled, unrecycled): {_fmt_tao(pot)}')
    total = read_u64('get_total_recycled_fees')
    if total is not None:
        console.print(f'  Recycled to date:              {_fmt_tao(total)}')
    floor = read_u64('get_min_collateral')
    if floor is not None:
        console.print(f'  Min collateral:                {_fmt_tao(floor)}')

    if miner:
        mb = _account_bytes(miner)
        console.print(f'\n[bold]Miner[/bold] [dim]{miner}[/dim]\n')
        bond = read_u64('get_collateral', mb)
        if bond is not None:
            console.print(f'  Bond (gross, on vault books):  {_fmt_tao(bond)}')
        locked = None
        raw = _dry_read(subtensor, origin, dest, _msg(sel, 'get_lock_state', mb))
        if raw and len(raw) >= 9:
            locked, epoch = raw[0] == 1, int.from_bytes(raw[1:9], 'little')
            console.print(f'  Lock: {"[red]LOCKED[/red]" if locked else "[green]unlocked[/green]"} (epoch {epoch})')
        settled = read_u64('get_settled_total', mb)
        if settled is not None:
            console.print(f'  Fees settled (cumulative):     {_fmt_tao(settled)}')
        if locked:
            console.print(
                '\n  [yellow]⚠ Gross figure: protocol fees accrued since the last settle are'
                ' netted off-chain\n  by validators and NOT reflected above. Your withdrawable'
                ' amount after exit is this\n  figure minus the exit residual settle'
                ' (≤ one fee cadence, often zero).[/yellow]'
            )
    console.print()


@vault_group.command('post-collateral', show_disclaimer=True)
@click.argument('amount_tao', type=float)
def vault_post_collateral(amount_tao):
    """Post AMOUNT_TAO into the vault as bond (signed by the wallet hotkey)."""
    config, wallet, subtensor, _ = get_cli_context()
    sel = _selectors()
    keypair = _signer(wallet, use_coldkey=False)
    rao = int(amount_tao * TAO_TO_RAO)
    if rao <= 0:
        fail('Amount must be > 0')
    with loading(f'Posting {_fmt_tao(rao)} to the vault...'):
        receipt = _submit(subtensor, keypair, _vault_address(config), _msg(sel, 'post_collateral'), value=rao)
    _report(receipt, f'Posted {_fmt_tao(rao)}')


@vault_group.command('lock', show_disclaimer=True)
def vault_lock():
    """Lock the bond (enter service): required before Solana can activate you for TAO-backed pairs.

    [dim]Unlocking is NOT self-service: deactivate the TAO side on Solana and validators
    unlock you after settlement (quit -> settle -> unlock -> withdraw).[/dim]
    """
    config, wallet, subtensor, _ = get_cli_context()
    sel = _selectors()
    keypair = _signer(wallet, use_coldkey=False)
    with loading('Locking bond...'):
        receipt = _submit(subtensor, keypair, _vault_address(config), _msg(sel, 'lock_bond'))
    _report(receipt, 'Bond locked — mirror will pick it up next relay')


@vault_group.command('withdraw', show_disclaimer=True)
@click.argument('amount_tao', type=float)
def vault_withdraw(amount_tao):
    """Withdraw AMOUNT_TAO from an UNLOCKED bond back to the hotkey.

    [dim]The exit residual fee settle runs BEFORE validators unlock you, so once you
    are unlocked the vault balance is exact and fully withdrawable. If the call is
    refused, re-check `alw vault status <hotkey>` for the post-settle figure.[/dim]
    """
    config, wallet, subtensor, _ = get_cli_context()
    sel = _selectors()
    keypair = _signer(wallet, use_coldkey=False)
    rao = int(amount_tao * TAO_TO_RAO)
    if rao <= 0:
        fail('Amount must be > 0')
    with loading(f'Withdrawing {_fmt_tao(rao)}...'):
        receipt = _submit(subtensor, keypair, _vault_address(config), _msg(sel, 'withdraw_collateral', _u64(rao)))
    _report(receipt, f'Withdrew {_fmt_tao(rao)}')


@vault_group.command('claim-slash', show_disclaimer=True)
@click.argument('swap_ref')
def vault_claim_slash(swap_ref):
    """Claim a parked slash reimbursement (only needed if the direct payout transfer failed)."""
    config, wallet, subtensor, _ = get_cli_context()
    sel = _selectors()
    keypair = _signer(wallet, use_coldkey=False)
    ref = swap_ref[2:] if swap_ref.startswith('0x') else swap_ref
    if len(ref) != 64:
        fail('SWAP_REF must be 32 bytes of hex')
    with loading('Claiming...'):
        receipt = _submit(subtensor, keypair, _vault_address(config), _msg(sel, 'claim_slash', bytes.fromhex(ref)))
    _report(receipt, 'Claim submitted')


# ─── Admin (owner-only) ──────────────────────────────────────────────────────

def _admin_confirm(prompt: str) -> bool:
    ctx = click.get_current_context(silent=True)
    if (ctx is not None and ctx.obj and ctx.obj.get('yes')) or os.environ.get('ALW_ASSUME_YES'):
        return True
    return click.confirm(prompt)


def _admin_submit(label: str, args: bytes, prompt: str, ok: str, use_coldkey: bool):
    config, wallet, subtensor, _ = get_cli_context()
    sel = _selectors()
    keypair = _signer(wallet, use_coldkey=use_coldkey)
    console.print(f'  Signer: [dim]{keypair.ss58_address}[/dim] (must be the vault owner)')
    if not _admin_confirm(prompt):
        console.print('[yellow]Cancelled[/yellow]')
        return
    with loading('Submitting...'):
        receipt = _submit(subtensor, keypair, _vault_address(config), _msg(sel, label, args))
    _report(receipt, ok)


@vault_group.group('admin', cls=StyledGroup, show_disclaimer=True)
@click.option('--yes', '-y', 'assume_yes', is_flag=True, help='Skip confirmation prompts (for scripting).')
@click.option('--coldkey', 'use_coldkey', is_flag=True, help='Sign with the wallet coldkey instead of the hotkey.')
@click.pass_context
def vault_admin_group(ctx, assume_yes, use_coldkey):
    """Vault administration (owner-signed; validator-set/config only — no fund paths)."""
    ctx.obj = {'yes': assume_yes, 'coldkey': use_coldkey}


def _ck() -> bool:
    ctx = click.get_current_context(silent=True)
    return bool(ctx and ctx.obj and ctx.obj.get('coldkey'))


@vault_admin_group.command('add-validator', show_disclaimer=True)
@click.argument('ss58')
def admin_add_validator(ss58):
    """Add SS58 to the vault's validator set."""
    _admin_submit('add_validator', _account_bytes(ss58), f'Add validator {ss58}?', 'Validator added', _ck())


@vault_admin_group.command('remove-validator', show_disclaimer=True)
@click.argument('ss58')
def admin_remove_validator(ss58):
    """Remove SS58 from the vault's validator set."""
    _admin_submit('remove_validator', _account_bytes(ss58), f'Remove validator {ss58}?', 'Validator removed', _ck())


@vault_admin_group.command('set-threshold', show_disclaimer=True)
@click.argument('percent', type=int)
def admin_set_threshold(percent):
    """Set the consensus threshold percent (1-100)."""
    if not 0 < percent <= 100:
        fail('Percent must be 1-100')
    _admin_submit('set_consensus_threshold', bytes([percent]), f'Set threshold to {percent}%?', 'Threshold set', _ck())


@vault_admin_group.command('set-min-collateral', show_disclaimer=True)
@click.argument('amount_tao', type=float)
def admin_set_min_collateral(amount_tao):
    """Set the minimum bond required to lock (in TAO)."""
    rao = int(amount_tao * TAO_TO_RAO)
    _admin_submit('set_min_collateral', _u64(rao), f'Set min collateral to {_fmt_tao(rao)}?', 'Min collateral set', _ck())


@vault_admin_group.command('set-max-collateral', show_disclaimer=True)
@click.argument('amount_tao', type=float)
def admin_set_max_collateral(amount_tao):
    """Set the maximum bond (in TAO; 0 = unlimited)."""
    rao = int(amount_tao * TAO_TO_RAO)
    _admin_submit('set_max_collateral', _u64(rao), f'Set max collateral to {_fmt_tao(rao)}?', 'Max collateral set', _ck())


@vault_admin_group.command('set-round-ttl', show_disclaimer=True)
@click.argument('blocks', type=int)
def admin_set_round_ttl(blocks):
    """Set the vote-round TTL in blocks."""
    if blocks <= 0:
        fail('Blocks must be > 0')
    _admin_submit('set_vote_round_ttl', blocks.to_bytes(4, 'little'), f'Set round TTL to {blocks} blocks?', 'Round TTL set', _ck())


@vault_admin_group.command('set-halted', show_disclaimer=True)
@click.argument('halted', type=click.Choice(['true', 'false']))
def admin_set_halted(halted):
    """Halt or resume value ENTRY (post_collateral/lock_bond). Exit paths are never halted."""
    flag = halted == 'true'
    _admin_submit('set_halted', bytes([1 if flag else 0]), f'Set halted = {flag}?', f'Halted = {flag}', _ck())


@vault_admin_group.command('transfer-ownership', show_disclaimer=True)
@click.argument('new_owner')
def admin_transfer_ownership(new_owner):
    """Transfer vault ownership to NEW_OWNER (ss58). Irreversible without their cooperation."""
    _admin_submit(
        'transfer_ownership', _account_bytes(new_owner),
        f'Transfer vault ownership to {new_owner}? This cannot be undone unilaterally.',
        'Ownership transferred', _ck(),
    )
