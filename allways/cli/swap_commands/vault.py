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

# Mirrors MIN_VOTE_ROUND_TTL in the vault contract — fail locally with a clear
# message rather than eating an opaque ContractReverted.
MIN_VOTE_ROUND_TTL = 100


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
    """Drain the pot into the SN7 pool (permissionless, caller pays ~0.003 τ).

    [dim]The pot is settled fees PLUS any TAO sent straight to the vault address — donated
    TAO is swept automatically, nobody can move it anywhere else. Fees only fill at true-up
    boundaries / exits / slash surpluses; cron this at a fixed offset AFTER the true-up
    cadence, more often is wasted postage.[/dim]
    """
    vault = _client()

    pot = vault.get_recyclable_pot()
    if pot is not None:
        console.print(f'  Pot: [bold]{_fmt_tao(pot)}[/bold]')
        fees = vault.get_accumulated_fees()
        if fees is not None and pot > fees:
            console.print(f'  [dim]of which donated: {_fmt_tao(pot - fees)}[/dim]')
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
    fees = vault.get_accumulated_fees()
    if fees is None:
        console.print('[yellow]Reads unavailable on this node (ContractResult decode) — write commands still work.[/yellow]\n')
        return
    console.print(f'  Fee pot (settled, unrecycled): {_fmt_tao(fees)}')
    pot = vault.get_recyclable_pot()
    if pot is not None:
        console.print(f'  Donated (unattributed):        {_fmt_tao(max(pot - fees, 0))}')
        console.print(f'  [bold]Recyclable pot (total):        {_fmt_tao(pot)}[/bold]')
    owed = vault.get_total_collateral()
    if owed is not None:
        console.print(f'  Bonds owed to miners:          {_fmt_tao(owed)}')
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


# ─── Governance (validator quorum — there is no owner) ───────────────────────

def _admin_confirm(prompt: str) -> bool:
    ctx = click.get_current_context(silent=True)
    if (ctx is not None and ctx.obj and ctx.obj.get('yes')) or os.environ.get('ALW_ASSUME_YES'):
        return True
    return click.confirm(prompt)


def _admin_submit(label: str, *args: bytes, confirm: str, ok_msg: str):
    vault = _client(_ck())
    console.print(f'  Signer: [dim]{vault.keypair.ss58_address}[/dim]')
    if not _admin_confirm(confirm):
        console.print('[yellow]Cancelled[/yellow]')
        return
    with loading('Submitting...'):
        result = vault.admin_call(label, *args)
    _report(result, ok_msg)


def _vote_submit(label: str, args, prompt: str, peer_suffix, peer_cmd: str = None):
    """Cast one governance vote. At a single-validator set this reaches quorum and applies
    immediately; beyond that it records a vote and prints the identical command peers must run —
    governance rounds bind their full payload, so a differing command opens a separate round
    instead of joining this one."""
    vault = _client(_ck())
    validators = vault.get_validators()
    n = len(validators) if validators is not None else None
    console.print(f'  Signer: [dim]{vault.keypair.ss58_address}[/dim]')
    if n:
        console.print(f'  Validator set: [bold]{n}[/bold]')
    if not _admin_confirm(prompt):
        console.print('[yellow]Cancelled[/yellow]')
        return
    with loading('Submitting vote...'):
        result = vault.admin_call(label, *args)
    _report(result, 'Vote submitted' if n != 1 else 'Applied (single-validator set)')
    if result.ok and n and n > 1:
        cmd = peer_cmd or f'alw vault admin {peer_suffix}'
        console.print('  [dim]Every other validator must run exactly:[/dim]')
        console.print(f'    [bold]{cmd}[/bold]\n')


@vault_group.group('admin', cls=StyledGroup, show_disclaimer=True)
@click.option('--yes', '-y', 'assume_yes', is_flag=True, help='Skip confirmation prompts (for scripting).')
@click.option('--coldkey', 'use_coldkey', is_flag=True, help='Sign with the wallet coldkey instead of the hotkey.')
@click.pass_context
def vault_admin_group(ctx, assume_yes, use_coldkey):
    """Vault governance — validator-signed quorum rounds.

    [dim]This contract has NO owner and no admin key. Membership and config change only by a
    unanimous vote of the current validator set, and nothing here can move a miner's funds.
    At a single-validator set each command applies immediately.[/dim]"""
    ctx.obj = {'yes': assume_yes, 'coldkey': use_coldkey}


def _ck() -> bool:
    ctx = click.get_current_context(silent=True)
    return bool(ctx and ctx.obj and ctx.obj.get('coldkey'))


@vault_admin_group.command('add-validator', show_disclaimer=True)
@click.argument('ss58')
def admin_add_validator(ss58):
    """Vote to admit SS58 to the validator set (UNANIMOUS — every current validator).

    [dim]On quorum the candidate becomes PENDING; they join only once they run
    `alw vault admin accept` themselves, which proves they hold the key.[/dim]
    """
    _vote_submit('vote_add_validator', [_account_bytes(ss58)], f'Vote to admit validator {ss58}?', 'add-validator ' + ss58)


@vault_admin_group.command('accept', show_disclaimer=True)
def admin_accept_validator():
    """Accept your own pending admission to the validator set."""
    _admin_submit('accept_validator', confirm='Accept your admission to the validator set?',
                  ok_msg='Joined the validator set')



@vault_admin_group.command('remove-validator', show_disclaimer=True)
@click.argument('ss58')
def admin_remove_validator(ss58):
    """Vote to eject SS58 (every OTHER validator; refused below 3 validators).

    [dim]The target is barred from voting, so a dark or compromised key can
    still be removed by the rest of the set.[/dim]
    """
    _vote_submit('vote_remove_validator', [_account_bytes(ss58)], f'Vote to remove validator {ss58}?',
                 'remove-validator ' + ss58)


@vault_admin_group.command('set-config', show_disclaimer=True)
@click.option('--min-collateral', type=float, help='Minimum bond required to lock (TAO)')
@click.option('--max-collateral', type=float, help='Maximum bond (TAO; 0 = unlimited)')
@click.option('--threshold', type=int, help='Consensus threshold percent (1-100)')
@click.option('--round-ttl', type=int, help='Vote-round TTL in blocks')
def admin_set_config(min_collateral, max_collateral, threshold, round_ttl):
    """Vote the WHOLE vault config (UNANIMOUS — every current validator).

    [dim]Omitted flags keep their current on-chain value. The round binds the
    complete resulting config, so every validator must submit the identical
    command — it is printed for you to pass on.[/dim]
    """
    vault = _client()

    # Unspecified fields default to the live config so every validator's round
    # hash matches. An unreadable config must FAIL, never be guessed at — a
    # wrong default here would silently propose a config nobody intended.
    current = {
        'min': vault.get_min_collateral(),
        'max': vault.get_max_collateral(),
        'threshold': vault.get_consensus_threshold(),
        'ttl': vault.get_vote_round_ttl(),
    }
    if any(v is None for v in current.values()):
        fail('Cannot read the current config from this node — refusing to guess the '
             'unspecified fields. Pass every flag explicitly, or use a node whose '
             'contract dry-runs decode.')

    new_min = int(min_collateral * TAO_TO_RAO) if min_collateral is not None else current['min']
    new_max = int(max_collateral * TAO_TO_RAO) if max_collateral is not None else current['max']
    new_threshold = threshold if threshold is not None else current['threshold']
    new_ttl = round_ttl if round_ttl is not None else current['ttl']

    if not 0 < new_threshold <= 100:
        fail('Threshold must be 1-100')
    if new_ttl < MIN_VOTE_ROUND_TTL:
        fail(f'Round TTL must be >= {MIN_VOTE_ROUND_TTL} blocks (the contract refuses less)')

    console.print('\n[bold]Resulting config[/bold]')
    console.print(f'  Min collateral: {_fmt_tao(current["min"])} -> {_fmt_tao(new_min)}')
    console.print(f'  Max collateral: {_fmt_tao(current["max"])} -> {_fmt_tao(new_max)}')
    console.print(f'  Threshold:      {current["threshold"]}% -> {new_threshold}%')
    console.print(f'  Round TTL:      {current["ttl"]} -> {new_ttl} blocks\n')

    peer_cmd = (
        f'alw vault admin set-config --min-collateral {new_min / TAO_TO_RAO:g} '
        f'--max-collateral {new_max / TAO_TO_RAO:g} --threshold {new_threshold} --round-ttl {new_ttl}'
    )
    _vote_submit(
        'vote_set_config',
        [codec.u64(new_min), codec.u64(new_max), codec.u8(new_threshold), codec.u32(new_ttl)],
        'Vote this config?',
        None,
        peer_cmd=peer_cmd,
    )
