"""alw base-vault - USDC bond vault on Base (collateral for baseusdc-backed pairs).

Talks to the `AllwaysUsdcVault` Solidity contract through `allways.vault.base_client`. Bonds are
keyed by the miner's Base address — the baseusdc spoke key (BASE_PRIVATE_KEY), so there is no
second EVM key — and joined to the Solana pubkey both ways: `bind` names the Base address on
Solana, `lock` names the Solana pubkey on the vault, and validators attest only when they agree.

Configuration: `alw config set base-vault-address <0x…>` (or ALLWAYS_BASE_VAULT_ADDRESS env);
BASE_NETWORK / BASE_RPC_URLS pick the network and endpoints, exactly as for the spoke.
"""

from decimal import Decimal

import click

from allways.chains import CHAIN_BASEUSDC
from allways.cli.help import StyledGroup
from allways.cli.swap_commands.helpers import (
    FINITE_DECIMAL,
    console,
    fail,
    from_lamports,
    get_effective_config,
    get_solana_cli_context,
    loading,
)
from allways.constants import BASE_HUB_VAULT_ADDRESSES
from allways.solana.client import SolanaClientError
from allways.vault.base_client import BaseVaultClient, BaseVaultError, base_chain
from allways.vault.client import VaultConfigError

USDC_DECIMALS = CHAIN_BASEUSDC.decimals

# Plain-language versions of the vault reverts a miner can hit, for a simulation that slips past
# the local checks (e.g. state moved between the read and the send).
REVERT_HINTS = {
    'InvalidAmount': 'the amount must be above zero',
    'ExceedsMaxCollateral': "the bond would exceed the vault's max collateral",
    'InsufficientCollateral': 'the bond is below what this needs (min collateral to lock, or the withdraw amount)',
    'BondLocked': 'the bond is locked — see `alw base-vault withdraw --help` for the exit sequence',
    'LockStateUnchanged': 'the bond is already locked',
    'InvalidSolanaPubkey': 'the Solana pubkey is zero',
    'DepositMismatch': 'the token moved a different amount than asked',
}

EXIT_SEQUENCE = (
    'Unlocking is NOT self-service: stop serving the Base backing on Solana and validators unlock '
    'you after settlement (quit -> settle -> unlock -> withdraw). Re-check `alw base-vault status`.'
)


def _client() -> BaseVaultClient:
    """The configured vault client, signed by the BASE key, failing with setup guidance if unconfigured."""
    try:
        vault = BaseVaultClient.from_config(get_effective_config())
    except (VaultConfigError, ValueError) as e:
        fail(str(e))
    if vault.account is None:
        fail(f'No Base signer — set {vault.chain._key_env} (the same key the baseusdc spoke uses).')
    _warn_if_off_record(vault.address, vault.chain.network)
    return vault


def _warn_if_off_record(address: str, network) -> None:
    """A stale configured address posts bonds into a vault no validator watches — it reads back
    healthy but nothing ever attests, so the miner silently never activates."""
    expected = BASE_HUB_VAULT_ADDRESSES.get(str(network or ''))
    if expected and address.lower() != expected.lower():
        console.print(
            f'[yellow]⚠ Configured Base vault {address}\n  is not the of-record vault for Base "{network}" '
            f'({expected}).\n  A bond posted to an unwatched vault is never attested — '
            f'`alw config set base-vault-address` unless this is intentional.[/yellow]'
        )


def _base_account():
    """The miner's Base key, without needing a vault configured (bind runs before any deposit)."""
    try:
        chain = base_chain()
    except ValueError as e:
        fail(str(e))
    account = chain._account()
    if account is None:
        fail(f'No Base signer — set {chain._key_env} (the same key the baseusdc spoke uses).')
    return account


def _usdc_to_units(amount: Decimal) -> int:
    """Exact 6-decimal units; more precision than the token has is refused, never rounded."""
    units = amount.scaleb(USDC_DECIMALS)
    if units != units.to_integral_value():
        fail(f'USDC has {USDC_DECIMALS} decimals — {amount} has more precision than that.')
    if units <= 0:
        fail('Amount must be > 0')
    return int(units)


def _fmt_usdc(units: int) -> str:
    return f'{Decimal(units).scaleb(-USDC_DECIMALS):,.{USDC_DECIMALS}f} USDC'


def _fmt_max(units: int) -> str:
    """0 is UNLIMITED in the contract, not 'closed' — never print it as a bare bound."""
    return 'UNLIMITED (0)' if units == 0 else _fmt_usdc(units)


def _fmt_eth(wei: int) -> str:
    return f'{Decimal(wei).scaleb(-18):.8f} ETH'


def _addr_bytes(address: str) -> bytes:
    return bytes.fromhex(address[2:])


def _hex_addr(raw: bytes) -> str:
    from eth_utils import to_checksum_address

    return to_checksum_address('0x' + bytes(raw).hex())


def _solana_pubkey(raw: bytes):
    if not any(raw):
        return None
    from solders.pubkey import Pubkey

    return str(Pubkey.from_bytes(bytes(raw)))


def _refused(e: BaseVaultError) -> None:
    hint = REVERT_HINTS.get(e.revert or '')
    fail(f'{e}{f" — {hint}" if hint else ""}. Nothing was sent.')


def _unsure(e: Exception) -> None:
    """A transport error mid-write: the tx may or may not have gone out, so never say it didn't."""
    fail(f'Base call failed: {e}. Check `alw base-vault status` before retrying.')


def _report(results, ok_msg: str) -> None:
    for result in results:
        if not result.mined:
            console.print(f'[yellow]Sent, but no receipt yet[/yellow] [dim](tx: {result.tx_hash})[/dim]')
            console.print('[dim]It may still land — check `alw base-vault status` before retrying.[/dim]\n')
            return
        if not result.ok:
            console.print(f'[red]Transaction reverted on-chain[/red] [dim](tx: {result.tx_hash})[/dim]\n')
            return
        console.print(f'  [dim]tx: {result.tx_hash}[/dim]')
    console.print(f'[green]{ok_msg}[/green]\n')


def _bindings(client, pubkey, evm_address: str):
    """``(this pubkey's EvmBinding address or None, the pubkey that claimed this address or None)``."""
    binding = client.get_evm_binding(pubkey) if pubkey is not None else None
    marker = client.get_evm_address_binding(_addr_bytes(evm_address))
    bound_addr = _hex_addr(binding.evm_address) if binding is not None else None
    claimed_by = marker.miner if marker is not None else None
    return bound_addr, claimed_by


def _print_bindings(pubkey, evm_address: str, bound_addr, claimed_by) -> None:
    def mark(ok):
        return '[green]✓[/green]' if ok else '[red]✗[/red]'

    pk_ok = bound_addr is not None and bound_addr.lower() == evm_address.lower()
    addr_ok = claimed_by is not None and pubkey is not None and bytes(claimed_by) == bytes(pubkey)
    console.print(f'  Solana → Base:  {mark(pk_ok)} {bound_addr or "[dim]not bound[/dim]"}')
    console.print(f'  Base → Solana:  {mark(addr_ok)} {claimed_by or "[dim]not claimed[/dim]"}')


# ─── Command group ───────────────────────────────────────────────────────────


@click.group('base-vault', cls=StyledGroup, show_disclaimer=True)
def base_vault_group():
    """USDC bond vault on Base (collateral for baseusdc-backed pairs).

    [dim]Signs with BASE_PRIVATE_KEY, the baseusdc spoke key — every write needs a little Base ETH
    for gas and is refused up front if the key can't cover it. Onboarding:
    bind -> deposit -> lock, then activate once validators attest the bond.[/dim]"""


@base_vault_group.command('bind', show_disclaimer=True)
@click.option('--yes', '-y', is_flag=True, help='Skip confirmation prompt')
def base_vault_bind(yes: bool):
    """Bind your Solana pubkey to your Base address (set-once, both ways).

    [dim]Names the BASE_PRIVATE_KEY address on Solana; `alw base-vault lock` names your pubkey
    back on the vault. Neither side can ever be rebound, so check the address first.[/dim]
    """
    account = _base_account()
    _, client = get_solana_cli_context()
    pubkey = client.keypair.pubkey()

    console.print('\n[bold]Bind Solana Pubkey ↔ Base Address[/bold]\n')
    console.print(f'  Pubkey:  {pubkey}')
    console.print(f'  Base:    {account.address}\n')

    try:
        bound_addr, claimed_by = _bindings(client, pubkey, account.address)
    except SolanaClientError as e:
        fail(f'Failed to read bindings: {e}')
    _print_bindings(pubkey, account.address, bound_addr, claimed_by)
    console.print()

    if bound_addr is not None and bound_addr.lower() != account.address.lower():
        fail(
            f'Your pubkey is already bound to {bound_addr}, and a binding never changes. '
            f"Set BASE_PRIVATE_KEY to that address's key to bond from it."
        )
    if claimed_by is not None and bytes(claimed_by) != bytes(pubkey):
        fail(f'{account.address} is already claimed by another Solana pubkey ({claimed_by}). Use a different Base key.')
    if bound_addr is not None:
        console.print('[green]Already bound both ways.[/green]\n')
        return

    # The program prices a claim like bind-hotkey: SOL collateral at the floor, or a validator seat.
    try:
        cfg = client.get_config()
        ms = client.get_miner_state(pubkey)
    except SolanaClientError as e:
        fail(f'Failed to read miner state: {e}')
    is_validator = cfg is not None and any(bytes(v.key) == bytes(pubkey) for v in cfg.validators)
    floor = int(cfg.min_collateral) if cfg is not None else 0
    have = int(ms.collateral) if ms is not None else 0
    if not is_validator and (ms is None or have < floor):
        fail(
            f'Binding needs SOL collateral of at least {from_lamports(floor)} SOL on this pubkey '
            f'(you have {from_lamports(have)}) — `alw collateral deposit` first.'
        )

    if not yes and not click.confirm('Confirm binding? It can never be changed.'):
        console.print('[yellow]Cancelled[/yellow]')
        return

    try:
        with loading('Submitting transaction...'):
            client.bind_evm_address(_addr_bytes(account.address))
        bound_addr, claimed_by = _bindings(client, pubkey, account.address)
    except SolanaClientError as e:
        fail(f'Failed to bind Base address: {e}')
    console.print(f'[green]Bound {pubkey} → {account.address}[/green]\n')
    _print_bindings(pubkey, account.address, bound_addr, claimed_by)
    console.print('\n[dim]Next: `alw base-vault deposit`, then `alw base-vault lock`.[/dim]\n')


@base_vault_group.command('deposit', show_disclaimer=True)
@click.option('--amount', default=None, type=FINITE_DECIMAL, help='Amount in USDC')
@click.option('--approve', 'use_approve', is_flag=True, help='Use approve + post (two txs) instead of a one-tx permit.')
def base_vault_deposit(amount, use_approve: bool):
    """Deposit USDC collateral into the Base vault (signed by BASE_PRIVATE_KEY).

    [dim]One transaction by default: an EIP-2612 permit signed locally rides along with the post.
    --approve falls back to approve + post for a token or wallet that can't permit.
    Deposits are allowed while locked. Then `alw base-vault lock` to enter service.[/dim]

    [dim]Examples:
        $ alw base-vault deposit --amount 250
        $ alw base-vault deposit --amount 250 --approve[/dim]
    """
    if amount is None:
        amount = click.prompt('Amount to deposit (USDC)', type=FINITE_DECIMAL)
    units = _usdc_to_units(amount)
    vault = _client()
    owner = vault.signer_address

    try:
        with loading('Reading the vault...'):
            bond = vault.get_bond(owner)
            floor = vault.min_collateral()
            cap = vault.max_collateral()
            balance = vault.usdc_balance(owner)
    except BaseVaultError as e:
        fail(str(e))

    total = bond.collateral + units
    if cap != 0 and total > cap:
        fail(
            f'This would bring your bond to {_fmt_usdc(total)}, over the vault max of {_fmt_usdc(cap)} '
            f'(you hold {_fmt_usdc(bond.collateral)} there). Deposit at most {_fmt_usdc(max(cap - bond.collateral, 0))}.'
        )
    if balance < units:
        fail(f'{owner} holds {_fmt_usdc(balance)} on Base — not enough to deposit {_fmt_usdc(units)}.')
    if not bond.locked and total < floor:
        console.print(
            f'[yellow]Your bond will be {_fmt_usdc(total)}, under the {_fmt_usdc(floor)} needed to lock — '
            f'`lock` will refuse until you add {_fmt_usdc(floor - total)} more.[/yellow]'
        )

    try:
        with loading(f'Depositing {_fmt_usdc(units)}...'):
            results = vault.approve_and_post(units) if use_approve else [vault.post_collateral_with_permit(units)]
    except BaseVaultError as e:
        _refused(e)
    except Exception as e:
        _unsure(e)
    _report(results, f'Deposited {_fmt_usdc(units)} — bond now {_fmt_usdc(total)}')


@base_vault_group.command('lock', show_disclaimer=True)
def base_vault_lock():
    """Lock the bond and name your Solana pubkey on it: required before the Base backing can serve.

    [dim]Refused unless your Solana pubkey is already bound to this Base address
    (`alw base-vault bind`) — validators attest only when both sides name each other.[/dim]

    [dim]Unlocking is NOT self-service: stop serving the Base backing on Solana and validators
    unlock you after settlement (quit -> settle -> unlock -> withdraw).[/dim]
    """
    vault = _client()
    owner = vault.signer_address
    _, client = get_solana_cli_context()
    pubkey = client.keypair.pubkey()

    try:
        bound_addr, claimed_by = _bindings(client, pubkey, owner)
    except SolanaClientError as e:
        fail(f'Failed to read bindings: {e}')
    if bound_addr is None:
        fail(f'Your Solana pubkey {pubkey} is not bound to a Base address — run `alw base-vault bind` first.')
    if bound_addr.lower() != owner.lower() or claimed_by is None or bytes(claimed_by) != bytes(pubkey):
        fail(
            f"Your Solana pubkey is bound to {bound_addr}, not this key's address {owner}. Validators would "
            f"never attest this bond — set BASE_PRIVATE_KEY to the bound address's key."
        )

    try:
        bond = vault.get_bond(owner)
        floor = vault.min_collateral()
    except BaseVaultError as e:
        fail(str(e))
    if bond.locked:
        fail(f'Your bond is already locked (epoch {bond.epoch}).')
    if bond.collateral < floor:
        fail(
            f'Your bond is {_fmt_usdc(bond.collateral)}, under the {_fmt_usdc(floor)} needed to lock — '
            f'`alw base-vault deposit --amount {Decimal(floor - bond.collateral).scaleb(-USDC_DECIMALS)}` first.'
        )

    try:
        with loading('Locking bond...'):
            result = vault.lock_bond(bytes(pubkey))
    except BaseVaultError as e:
        _refused(e)
    except Exception as e:
        _unsure(e)
    _report([result], f'Bond locked for {pubkey} — validators attest it on their next relay')


@base_vault_group.command('status', show_disclaimer=True)
@click.argument('address', required=False)
def base_vault_status(address):
    """Show a bond (yours by default, or ADDRESS) with its Solana binding and the Base ETH for gas."""
    try:
        vault = BaseVaultClient.from_config(get_effective_config())
    except (VaultConfigError, ValueError) as e:
        fail(str(e))
    _warn_if_off_record(vault.address, vault.chain.network)
    owner = address or vault.signer_address
    if owner is None:
        fail(f'Pass an ADDRESS, or set {vault.chain._key_env} to see your own bond.')
    if not vault.chain.is_valid_address(owner):
        fail(f'Not a valid Base address: {owner}')

    console.print(f'\n[bold]Base vault[/bold] [dim]{vault.address} ({vault.chain.network})[/dim]\n')
    try:
        with loading('Reading the vault...'):
            floor = vault.min_collateral()
            cap = vault.max_collateral()
            bond = vault.get_bond(owner)
            gas = vault.eth_balance(owner)
            wallet = vault.usdc_balance(owner)
    except Exception as e:
        fail(f'Failed to read the vault: {e}')
    console.print(f'  Min collateral:  {_fmt_usdc(floor)}')
    console.print(f'  Max collateral:  {_fmt_max(cap)}')

    console.print(f'\n[bold]Bond[/bold] [dim]{owner}[/dim]\n')
    console.print(f'  Collateral:      {_fmt_usdc(bond.collateral)}')
    console.print(
        f'  Lock:            {"[red]LOCKED[/red]" if bond.locked else "[green]unlocked[/green]"} (epoch {bond.epoch})'
    )
    named = _solana_pubkey(bond.solana_pubkey)
    console.print(f'  Names pubkey:    {named or "[dim]none[/dim]"}')
    console.print(f'  Wallet USDC:     {_fmt_usdc(wallet)}')
    console.print(f'  Base ETH (gas):  {_fmt_eth(gas)}')

    console.print('\n[bold]Solana binding[/bold]\n')
    try:
        _, client = get_solana_cli_context(need_keypair=False)
        claimed_by = client.get_evm_address_binding(_addr_bytes(owner))
        miner = claimed_by.miner if claimed_by is not None else None
        bound_addr, _ = _bindings(client, miner, owner)
    except Exception as e:
        console.print(f'  [yellow]Unreadable: {e}[/yellow]\n')
        return
    _print_bindings(miner, owner, bound_addr, miner)
    if bond.locked and miner is not None and named is not None and named != str(miner):
        console.print(f'  [red]The bond names {named}, but this address is bound to {miner} — never attested.[/red]')
    console.print()


@base_vault_group.command('withdraw', show_disclaimer=True)
@click.option('--amount', default=None, type=FINITE_DECIMAL, help='Amount in USDC')
def base_vault_withdraw(amount):
    """Withdraw USDC collateral from an UNLOCKED bond back to the Base address.

    [dim]Unlocking is NOT self-service: stop serving the Base backing on Solana and validators
    unlock you after settlement (quit -> settle -> unlock -> withdraw). Once unlocked the vault
    balance is exact and fully withdrawable.[/dim]
    """
    if amount is None:
        amount = click.prompt('Amount to withdraw (USDC)', type=FINITE_DECIMAL)
    units = _usdc_to_units(amount)
    vault = _client()
    try:
        bond = vault.get_bond(vault.signer_address)
    except BaseVaultError as e:
        fail(str(e))
    if bond.locked:
        fail(f'Your bond is locked (epoch {bond.epoch}). {EXIT_SEQUENCE}')
    if units > bond.collateral:
        fail(f'Your bond is {_fmt_usdc(bond.collateral)} — cannot withdraw {_fmt_usdc(units)}.')

    try:
        with loading(f'Withdrawing {_fmt_usdc(units)}...'):
            result = vault.withdraw_collateral(units)
    except BaseVaultError as e:
        _refused(e)
    except Exception as e:
        _unsure(e)
    _report([result], f'Withdrew {_fmt_usdc(units)}')
