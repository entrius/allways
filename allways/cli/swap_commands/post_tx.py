"""alw swap post-tx - Confirm a swap by relaying your source-chain deposit to the validators.

After `alw swap now` reserves a miner and you've sent the source funds to that miner's address, this
command relays the source-tx hash to every serving validator via a ``SwapConfirmSynapse``. Each
validator independently verifies the deposit against the pinned on-chain ``Reservation`` — recipient =
the reserved miner address, amount = the reserved amount, and crucially **sender = the reservation's
pinned taker address** — then submits ``submit_swap_claim`` on-chain (→ ``PendingAttestation``).

Auth model: the confirm carries no taker identity. The relay is signed by a throwaway *ephemeral*
Bittensor hotkey (``dendrite_lite.get_ephemeral_wallet``) purely as transport — validators do NOT
blacklist on it. The real proof is the on-chain source deposit itself: it can only have come from the
taker who won the draw, because ``Reservation.from_addr`` is pinned at reserve time. So anyone may
relay the confirm, but only the winner's deposit verifies.
"""

import time

import click

from allways.cli.dendrite_lite import (
    broadcast_synapse,
    discover_validators,
    get_ephemeral_wallet,
)
from allways.cli.help import StyledCommand
from allways.cli.swap_commands.helpers import (
    clear_pending_swap,
    console,
    fail,
    get_cli_context,
    get_solana_cli_context,
    hotkey_bytes_to_ss58,
    live_unclaimed,
    load_pending_swap,
    loading,
)
from allways.cli.validator_rejections import render_and_aggregate
from allways.synapses import SwapConfirmSynapse

# Bounded auto-retry of the deposit relay. The relay is idempotent (validators re-verify the same
# on-chain deposit each time), so when nothing is accepted for a *non-deterministic* reason — the
# deposit hasn't propagated to validators' source-chain nodes yet (`tx_not_found`), a 429, or a
# timeout — we wait briefly and re-broadcast. A BTC deposit takes seconds to reach a validator's
# bitcoind, so `post-tx` fired immediately after broadcast otherwise fails on the first pass.
_RELAY_ATTEMPTS = 3
_RELAY_WAIT_SECS = 30


def _should_retry_relay(info) -> bool:
    """Retry a zero-accept relay only when the aggregator says the failure is NOT deterministic —
    i.e. a re-broadcast could plausibly succeed (propagation lag / rate-limit / timeout). A genuine
    mismatch (wrong amount/recipient, expired reservation) is `deterministic=True` → fail fast."""
    return info is not None and not info.accepted and not getattr(info, 'deterministic', False)


def _find_reservations(client, user, miner_hint, require_user):
    """Locate the live, unclaimed reservation(s) to confirm.

    Targeted (miner_hint set — an explicit --miner or the stash from `alw swap now`): return that
    one miner's reservation. Scan (no hint): walk every on-chain Binding (each carries its miner
    pubkey + hotkey) and collect reservations belonging to this user. ``require_user`` gates the
    user-match: True for auto-discovery ("find MY reservation"), False for an explicit --miner relay
    (confirm is permissionless — the on-chain deposit is the auth, not the caller). Returns a list of
    (miner_pubkey, hotkey_ss58, reservation) so the caller can disambiguate ties.
    """
    from solders.pubkey import Pubkey

    def _hotkey(miner_pk):
        b = client.get_binding(miner_pk)
        return hotkey_bytes_to_ss58(b.hotkey) if b else ''

    if miner_hint:
        try:
            mpk = Pubkey.from_string(miner_hint)
        except Exception:
            console.print(f'[red]Invalid miner pubkey: {miner_hint}[/red]')
            return []
        resv = client.get_reservation(mpk)
        if live_unclaimed(resv) and (not require_user or str(resv.user) == str(user)):
            return [(mpk, _hotkey(mpk), resv)]
        return []

    found = []
    for _pda, binding in client.get_all('Binding'):
        mpk = binding.miner
        resv = client.get_reservation(mpk)
        if live_unclaimed(resv) and str(resv.user) == str(user):
            found.append((mpk, hotkey_bytes_to_ss58(binding.hotkey), resv))
    return found


@click.command('post-tx', cls=StyledCommand, show_disclaimer=True)
@click.argument('tx_hash', required=False, default=None, type=str)
@click.option(
    '--block',
    'tx_block',
    type=int,
    default=0,
    help=(
        'Override the source-tx slot number. Usually unnecessary — the CLI looks it up '
        'automatically. Use this only when automatic lookup fails (e.g. the node has pruned the tx).'
    ),
)
@click.option(
    '--miner',
    'miner_hint',
    default=None,
    help='Miner Solana pubkey to confirm against (disambiguates if you hold multiple reservations).',
)
def post_tx_command(tx_hash: str, tx_block: int, miner_hint: str):
    """Confirm your swap by relaying the source-tx hash to the validators.

    [dim]Run this after `alw swap now` reserves a miner and you've sent your source funds to the
    miner's address. The validators verify the on-chain deposit against your pinned reservation and
    submit the claim; the miner then fulfils the destination leg.[/dim]

    [dim]Reservation context is read from ~/.allways/pending_swap.json (saved by `alw swap now`); if
    it's missing the CLI finds your live reservation on-chain.[/dim]

    [dim]Examples:
        $ alw swap post-tx 54foaURhGH...
        $ alw swap post-tx 54foaURhGH... --miner ER9Jt5...        (pick a specific reservation)
        $ alw swap post-tx 54foaURhGH... --block 371234567        (escape hatch)[/dim]
    """
    if not tx_hash:
        tx_hash = click.prompt('Source transaction hash').strip()
    else:
        tx_hash = tx_hash.strip()
    if not tx_hash:
        fail('A source transaction hash is required.')

    _config, client = get_solana_cli_context(need_keypair=True)
    user = client.keypair.pubkey()

    # Explicit --miner is a permissionless relay (confirm any live reservation for that miner);
    # otherwise auto-discover this keypair's own reservation, preferring the stash from `alw swap now`.
    pending = load_pending_swap() or {}
    if miner_hint:
        matches = _find_reservations(client, user, miner_hint, require_user=False)
    else:
        matches = _find_reservations(client, user, pending.get('miner'), require_user=True)
        if not matches and pending.get('miner'):
            # Stashed miner no longer valid (expired/claimed/superseded) — scan for any live one.
            matches = _find_reservations(client, user, None, require_user=True)

    if not matches:
        # fail (exit 1), not a friendly return — a scripted relay must see this as failure.
        fail(
            'No live, unclaimed reservation found for your address. Reserve one with '
            '`alw swap now` first, and confirm before it expires. Check status with `alw view reservation`.'
        )
    if len(matches) > 1:
        console.print('[yellow]You hold multiple live reservations — pick one with --miner <pubkey>:[/yellow]')
        for mpk, _hotkey, resv in matches:
            console.print(
                f'  [cyan]{mpk}[/cyan]  {resv.from_chain}->{resv.to_chain}  send to [dim]{resv.miner_from_addr}[/dim]'
            )
        fail('Ambiguous reservation — nothing was relayed.')

    miner_pk, miner_hotkey, resv = matches[0]
    if not miner_hotkey:
        fail(
            f'Miner {miner_pk} has no verifiable hotkey binding — validators cannot resolve the '
            'reservation. The miner must `alw miner bind-hotkey` before this swap can be confirmed.'
        )

    # Resolve the source-tx slot as a verification hint (validators can scan without it, but a slot
    # makes it O(1)). Best-effort: fall back to 0 (server-side lookup) or the --block override.
    if tx_block == 0:
        try:
            info = client.rpc.get_transaction(tx_hash)
            if info and info.get('slot'):
                tx_block = int(info['slot'])
        except Exception:
            pass

    on_behalf = '' if str(resv.user) == str(user) else f'  [dim](relaying on behalf of {str(resv.user)[:8]}…)[/dim]'
    console.print(
        f'\n[bold]Confirming deposit[/bold] for {resv.from_chain.upper()}->{resv.to_chain.upper()} swap{on_behalf}\n'
        f'  Miner:   [dim]{miner_hotkey[:16]}… ({str(miner_pk)[:8]}…)[/dim]\n'
        f'  Deposit: [cyan]{tx_hash}[/cyan]{f" [dim](slot {tx_block})[/dim]" if tx_block else ""}\n'
    )

    synapse = SwapConfirmSynapse(
        reservation_id=miner_hotkey,
        from_tx_hash=tx_hash,
        from_tx_proof='',  # Solana: the tx hash is the proof — validators look it up on-chain.
        from_address=resv.from_addr,
        from_tx_block=tx_block,
        to_address=resv.user_to_addr,
        from_chain=resv.from_chain,
        to_chain=resv.to_chain,
    )

    config, _wallet, subtensor, _ = get_cli_context(need_wallet=False)
    netuid = int(config['netuid'])
    validator_axons = discover_validators(subtensor, netuid)
    if not validator_axons:
        console.print('[red]No serving validators found on the metagraph.[/red]')
        return

    wallet = get_ephemeral_wallet()  # transport-only throwaway hotkey; auth is the on-chain deposit
    info = None
    for attempt in range(_RELAY_ATTEMPTS):
        with loading(f'Relaying deposit to {len(validator_axons)} validator(s)...'):
            responses = broadcast_synapse(wallet, validator_axons, synapse, timeout=60.0)
        info = render_and_aggregate(console, responses, label='V', context={'miner_hotkey': miner_hotkey})
        if attempt == _RELAY_ATTEMPTS - 1 or not _should_retry_relay(info):
            break
        console.print(
            f'  [dim]No validator has seen the deposit yet (propagation lag) — retrying in '
            f'{_RELAY_WAIT_SECS}s [{attempt + 1}/{_RELAY_ATTEMPTS - 1}]…[/dim]'
        )
        time.sleep(_RELAY_WAIT_SECS)

    if info.accepted:
        clear_pending_swap()
        console.print(
            f'\n[green]Deposit confirmed by {info.accepted} validator(s).[/green] '
            'The miner will fulfil the destination leg once the claim is attested.\n'
            '[dim]Track it with `alw view swap`.[/dim]'
        )
    else:
        # exit 1: a scripted relay must see "not accepted" as failure and re-run (idempotent).
        fail(
            'No validator accepted the confirm yet. Re-run `alw swap post-tx` with the same hash '
            'in a moment. If it keeps failing, check the reservation is still active with `alw view reservation`.'
        )
