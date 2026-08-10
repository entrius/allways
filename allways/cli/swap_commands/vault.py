"""alw vault - TAO bond vault commands (Bittensor side of the v2 split-collateral design).

Talks to the `allways_bond_vault` ink! contract through the shared `allways.vault` client: the
permissionless fee recycle, miner bond operations (signed by the wallet HOTKEY — the vault keys
bonds by hotkey, joined to the Solana pubkey via the A5 binding), and the owner admin setters.
Validator relay rounds (vote_unlock / vote_slash / vote_collect_fees_batch) use the same client
from the validator's relayer, not from here.

Configuration: `alw config set vault-address <ss58>` (or ALLWAYS_VAULT_ADDRESS env);
metadata JSON path via vault-metadata / ALLWAYS_VAULT_METADATA, defaulting to the
in-repo build artifact. ALLWAYS_VAULT_SURI overrides the signer for scripting
(e.g. //Alice on a localnet).
"""

import os

import click

from allways.cli.help import StyledGroup
from allways.cli.swap_commands.helpers import console, fail, get_cli_context, loading
from allways.constants import TAO_TO_RAO
from allways.vault import BondVaultClient, VaultConfigError, codec


def _client(use_coldkey: bool = False) -> BondVaultClient:
    """The configured vault client plus its signer, failing with setup guidance if unconfigured."""
    from allways.vault.client import resolve_signer

    config, wallet, subtensor, _ = get_cli_context()
    try:
        return BondVaultClient.from_config(subtensor, config, keypair=resolve_signer(wallet, use_coldkey))
    except (VaultConfigError, codec.VaultCodecError) as e:
        fail(str(e))


def _account_bytes(ss58: str) -> bytes:
    try:
        return codec.account_bytes(ss58)
    except codec.VaultCodecError:
        fail(f'Not a valid ss58 address: {ss58}')


def _report(result, ok_msg: str):
    if not result.ok:
        # pallet-contracts surfaces a contract Err as a ContractReverted
        # dispatch error; name it when we can extract it.
        if result.reverted:
            console.print('[yellow]The vault rejected the call (contract reverted) — e.g. empty pot, locked bond, or insufficient balance.[/yellow]')
        else:
            console.print(f'[red]Call failed[/red]{f" [dim]({result.error})[/dim]" if result.error else ""}')
    else:
        console.print(f'[green]{ok_msg}[/green]')
    if result.events:
        console.print(f'  [dim]events: {", ".join(result.events)}[/dim]')
    console.print(f'  [dim]extrinsic: {result.extrinsic_hash}[/dim]\n')


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
    vault = _client()

    pot = vault.get_accumulated_fees()
    if pot is not None:
        console.print(f'  Pot: [bold]{_fmt_tao(pot)}[/bold]')
        if pot == 0 and not force:
            console.print('[yellow]Pot is empty — nothing to recycle (use --force to submit anyway)[/yellow]\n')
            return
    elif not force:
        console.print('[dim]Pot unreadable on this node (dry-run decode); submitting anyway.[/dim]')

    with loading('Submitting recycle_fees...'):
        result = vault.recycle_fees()
    _report(result, 'Recycle submitted — pot staked into the subnet pool')


@vault_group.command('status', show_disclaimer=True)
@click.argument('miner', required=False)
def vault_status(miner):
    """Show vault totals, and a miner's bond/lock/settled state if MINER (ss58) is given."""
    vault = _client()

    console.print(f'\n[bold]Vault[/bold] [dim]{vault.address}[/dim]\n')
    pot = vault.get_accumulated_fees()
    if pot is None:
        console.print('[yellow]Reads unavailable on this node (ContractResult decode) — write commands still work.[/yellow]\n')
        return
    console.print(f'  Fee pot (settled, unrecycled): {_fmt_tao(pot)}')
    total = vault.get_total_recycled_fees()
    if total is not None:
        console.print(f'  Recycled to date:              {_fmt_tao(total)}')
    floor = vault.get_min_collateral()
    if floor is not None:
        console.print(f'  Min collateral:                {_fmt_tao(floor)}')

    if miner:
        _account_bytes(miner)  # reject a malformed address before four dry-runs against it
        console.print(f'\n[bold]Miner[/bold] [dim]{miner}[/dim]\n')
        bond = vault.get_collateral(miner)
        if bond is not None:
            console.print(f'  Bond (gross, on vault books):  {_fmt_tao(bond)}')
        lock = vault.get_lock_state(miner)
        locked = None
        if lock is not None:
            locked, epoch = lock
            console.print(f'  Lock: {"[red]LOCKED[/red]" if locked else "[green]unlocked[/green]"} (epoch {epoch})')
        settled = vault.get_settled_total(miner)
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
    vault = _client()
    rao = int(amount_tao * TAO_TO_RAO)
    if rao <= 0:
        fail('Amount must be > 0')
    with loading(f'Posting {_fmt_tao(rao)} to the vault...'):
        result = vault.post_collateral(rao)
    _report(result, f'Posted {_fmt_tao(rao)}')


@vault_group.command('lock', show_disclaimer=True)
def vault_lock():
    """Lock the bond (enter service): required before Solana can activate you for TAO-backed pairs.

    [dim]Unlocking is NOT self-service: deactivate the TAO side on Solana and validators
    unlock you after settlement (quit -> settle -> unlock -> withdraw).[/dim]
    """
    vault = _client()
    with loading('Locking bond...'):
        result = vault.lock_bond()
    _report(result, 'Bond locked — mirror will pick it up next relay')


@vault_group.command('withdraw', show_disclaimer=True)
@click.argument('amount_tao', type=float)
def vault_withdraw(amount_tao):
    """Withdraw AMOUNT_TAO from an UNLOCKED bond back to the hotkey.

    [dim]The exit residual fee settle runs BEFORE validators unlock you, so once you
    are unlocked the vault balance is exact and fully withdrawable. If the call is
    refused, re-check `alw vault status <hotkey>` for the post-settle figure.[/dim]
    """
    vault = _client()
    rao = int(amount_tao * TAO_TO_RAO)
    if rao <= 0:
        fail('Amount must be > 0')
    with loading(f'Withdrawing {_fmt_tao(rao)}...'):
        result = vault.withdraw_collateral(rao)
    _report(result, f'Withdrew {_fmt_tao(rao)}')


@vault_group.command('claim-slash', show_disclaimer=True)
@click.argument('swap_ref')
def vault_claim_slash(swap_ref):
    """Claim a parked slash reimbursement (only needed if the direct payout transfer failed)."""
    vault = _client()
    ref = swap_ref[2:] if swap_ref.startswith('0x') else swap_ref
    if len(ref) != 64:
        fail('SWAP_REF must be 32 bytes of hex')
    with loading('Claiming...'):
        result = vault.claim_slash(ref)
    _report(result, 'Claim submitted')


# ─── Admin (owner-only) ──────────────────────────────────────────────────────

def _admin_confirm(prompt: str) -> bool:
    ctx = click.get_current_context(silent=True)
    if (ctx is not None and ctx.obj and ctx.obj.get('yes')) or os.environ.get('ALW_ASSUME_YES'):
        return True
    return click.confirm(prompt)


def _admin_submit(label: str, args: bytes, prompt: str, ok: str, use_coldkey: bool):
    vault = _client(use_coldkey)
    console.print(f'  Signer: [dim]{vault.keypair.ss58_address}[/dim] (must be the vault owner)')
    if not _admin_confirm(prompt):
        console.print('[yellow]Cancelled[/yellow]')
        return
    with loading('Submitting...'):
        result = vault.admin_call(label, args)
    _report(result, ok)


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
    _admin_submit('set_consensus_threshold', codec.u8(percent), f'Set threshold to {percent}%?', 'Threshold set', _ck())


@vault_admin_group.command('set-min-collateral', show_disclaimer=True)
@click.argument('amount_tao', type=float)
def admin_set_min_collateral(amount_tao):
    """Set the minimum bond required to lock (in TAO)."""
    rao = int(amount_tao * TAO_TO_RAO)
    _admin_submit('set_min_collateral', codec.u64(rao), f'Set min collateral to {_fmt_tao(rao)}?', 'Min collateral set', _ck())


@vault_admin_group.command('set-max-collateral', show_disclaimer=True)
@click.argument('amount_tao', type=float)
def admin_set_max_collateral(amount_tao):
    """Set the maximum bond (in TAO; 0 = unlimited)."""
    rao = int(amount_tao * TAO_TO_RAO)
    _admin_submit('set_max_collateral', codec.u64(rao), f'Set max collateral to {_fmt_tao(rao)}?', 'Max collateral set', _ck())


@vault_admin_group.command('set-round-ttl', show_disclaimer=True)
@click.argument('blocks', type=int)
def admin_set_round_ttl(blocks):
    """Set the vote-round TTL in blocks."""
    if blocks <= 0:
        fail('Blocks must be > 0')
    _admin_submit('set_vote_round_ttl', codec.u32(blocks), f'Set round TTL to {blocks} blocks?', 'Round TTL set', _ck())


@vault_admin_group.command('set-halted', show_disclaimer=True)
@click.argument('halted', type=click.Choice(['true', 'false']))
def admin_set_halted(halted):
    """Halt or resume value ENTRY (post_collateral/lock_bond). Exit paths are never halted."""
    flag = halted == 'true'
    _admin_submit('set_halted', codec.boolean(flag), f'Set halted = {flag}?', f'Halted = {flag}', _ck())


@vault_admin_group.command('transfer-ownership', show_disclaimer=True)
@click.argument('new_owner')
def admin_transfer_ownership(new_owner):
    """Transfer vault ownership to NEW_OWNER (ss58). Irreversible without their cooperation."""
    _admin_submit(
        'transfer_ownership', _account_bytes(new_owner),
        f'Transfer vault ownership to {new_owner}? This cannot be undone unilaterally.',
        'Ownership transferred', _ck(),
    )
