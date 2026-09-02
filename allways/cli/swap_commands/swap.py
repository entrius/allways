"""alw swap - Execute and manage cross-chain swaps.

Origination is on-chain on Solana, two-phase: the taker BIDS into a per-miner reservation pool
(`open_or_request`, pair only), a permissionless stake-weighted draw (`resolve_pool`) seats a winner,
then the seat winner FILLS the reservation (`finalize_reservation`, naming the taker + amounts). `swap
now` runs either self-represented (the taker is its own router: bid → self-crank the draw → finalize
against the pinned rate) or validator-routed (a `SwapReserveSynapse` asks the configured router
validator to enter the pool with its stake weight and finalize the won seat with the taker pinned).
Either way the tail is the same: send source funds + `swap post-tx`."""

import json
import sys
import time
from decimal import Decimal
from typing import List, NamedTuple, Optional

import click

from allways.chains import SUPPORTED_CHAINS, get_chain_def, uses_solana_wallet
from allways.cli.dendrite_lite import (
    broadcast_synapse,
    find_validator_axon,
    get_ephemeral_wallet,
    invalidate_axon_cache,
    resolve_dendrite_timeout,
)
from allways.cli.help import StyledGroup
from allways.cli.swap_commands.helpers import (
    FINITE_DECIMAL,
    PENDING_SWAP_FILE,
    backing_label,
    console,
    fail,
    get_cli_context,
    get_solana_cli_context,
    hotkey_bytes_to_ss58,
    live_unclaimed,
    loading,
)
from allways.cli.swap_commands.quote import GUARANTEE
from allways.cli.swap_commands.swap_intake import (
    bounds_from_config,
    candidate_miners,
    compute_intake_amounts,
    hub_bounds,
    rate_display_from_fixed,
    select_best_miner,
    swap_viable,
    to_smallest_units,
    unviable_reason,
    viable_intakes,
)
from allways.cli.validator_rejections import render_and_aggregate
from allways.constants import FEE_DIVISOR, NETUID_FINNEY, NUMERAIRE_CHAIN, hub_leg
from allways.solana import pdas
from allways.solana.client import benign_marker, contract_reject_reason
from allways.solana.rpc import TransientRpcError
from allways.synapses import SwapReserveSynapse
from allways.utils.rate import apply_fee_deduction, directional_rate


@click.group('swap', cls=StyledGroup, show_disclaimer=True)
def swap_group():
    """Execute and manage cross-chain swaps."""


class PoolContention(NamedTuple):
    """A read-only snapshot of a miner's live bid pool, so a taker can see — BEFORE it pays the
    non-refundable reservation fee — whether it is bidding into an already-open, contested round and
    roughly what its draw odds are."""

    is_open: bool  # a pool round is open AND still in its bid window
    bidders: int  # distinct bids already in the round (before you join)
    closes_in: int  # seconds until the window closes
    weighted_rivals: int  # existing bidders that are weighted validators (0 => the draw is uniform)
    from_chain: str = ''  # the open round's pair + PINNED rate — every fill this round settles at it,
    to_chain: str = ''  # so a preview must quote it, not the (possibly drifted) live quote
    rate: int = 0


def _pool_contention(client, miner, cfg, backing) -> PoolContention:
    """Best-effort read of the miner's pool. Visibility only — never raises, so a decode/RPC hiccup
    can't block a bid; on any surprise it reports 'not open' and the taker proceeds as before."""
    try:
        pool = client.get_pool(miner, backing)
        now = int(time.time())
        if int(getattr(pool, 'opened_at', 0)) == 0 or now > int(getattr(pool, 'closes_at', 0)):
            return PoolContention(False, 0, 0, 0)
        reqs = list(getattr(pool, 'requests', []))
        weighted = {bytes(v.key) for v in getattr(cfg, 'validators', []) if int(getattr(v, 'weight', 0)) > 0}
        weighted_rivals = sum(1 for r in reqs if bytes(r.router) in weighted)
        return PoolContention(
            True,
            len(reqs),
            max(0, int(pool.closes_at) - now),
            weighted_rivals,
            str(getattr(pool, 'from_chain', '') or ''),
            str(getattr(pool, 'to_chain', '') or ''),
            int(getattr(pool, 'rate', 0) or 0),
        )
    except Exception:  # noqa: BLE001 - visibility only; a bad read must not stop the swap
        return PoolContention(False, 0, 0, 0)


# Minimum reservation life required before we'll instruct a send. The reservation must outlive
# broadcast -> mempool visibility -> post-tx relay -> submit_swap_claim landing, which is all the
# on-chain claim gate checks (`reserved_until >= now`, empty claim slot). Chain-independent by
# construction: 60s post-tx dendrite timeout + ~60s Solana claim landing + 60s slack.
_SEND_MARGIN_SECS = 180


# Benign crank races — `resolve_pool` lost to another cranker (the validator, or a peer taker) or ran
# before the draw was possible. A miss must not abort `swap now` — that abandons the taker's already-paid,
# since-drawn seat until it expires (fee forfeited, miner locked busy_until) — so benign_marker matches
# both string forms (pre-flight name, landed code-only).
_BENIGN_CRANK_NAMES = ('SeedSlotNotYetProduced', 'PoolNotClosed', 'NoRequests', 'AlreadyFilled')


def _self_crank_resolve(client, miner, backing) -> None:
    """Permissionless arm-then-draw crank. An unrouted taker cranks its own pool so the draw never
    waits on validator liveness. Benign races (window not closed, seed slot not produced yet, already
    resolved/filled) are expected and retried on the next poll."""
    try:
        client.resolve_pool(miner, backing)
    except TransientRpcError:
        return  # RPC hiccup while nudging the pool — the poll loop re-cranks and re-reads the real
        # outcome (the reservation). If this resolve_pool actually landed, the next pass sees the seat.
    except Exception as e:  # noqa: BLE001
        if not benign_marker(e, _BENIGN_CRANK_NAMES):
            raise


def _prompt_missing(value, prompt_text: str, opt: str, cast=str):
    """Interactive fallback for a missing input: prompt on a TTY, else fail script-safely.
    Mirrors `alw swap quote` so both commands feel the same when run bare."""
    if value not in (None, ''):
        return value
    if sys.stdin.isatty():
        return click.prompt(prompt_text, type=cast)
    fail(f'{opt} is required (no TTY to prompt).')


def _drawn_unfilled(resv) -> bool:
    """A seat freshly won by the draw and not yet named. `created_at == 0` is load-bearing: a
    reservation that was filled and then consumed also has `reserved_until == 0`, but carries a
    non-zero `created_at` — matching it would finalize against a dead window. Same guard as the
    contract's `close_unfilled_reservation`."""
    if resv is None:
        return False
    return int(resv.reserved_until) == 0 and int(resv.created_at) == 0 and int(resv.finalize_by) > time.time()


def _poll_drawn(client, miner, user, timeout_secs: int, backing: str):
    """Poll until THIS taker's bid draws its UNFILLED reservation, self-cranking `resolve_pool` each
    pass. Returns the drawn reservation, or None on timeout / if a different router won the seat."""
    us = str(user)
    deadline = time.time() + timeout_secs
    while time.time() < deadline:
        _self_crank_resolve(client, miner, backing)
        resv = client.get_reservation(miner, backing)
        if _drawn_unfilled(resv):
            return resv if str(resv.router) == us else None  # seated: us, or someone else won
        time.sleep(3)
    return None


def _lost_seat_to(client, miner, user, backing: str) -> Optional[str]:
    """Best-effort: after `_poll_drawn` returns None, tell the two failure modes apart. Returns the
    winning router (as a string) if a *different* taker holds the freshly-drawn seat — i.e. you lost
    the draw — or None if no seat is drawn yet (the draw simply didn't resolve in the window)."""
    try:
        resv = client.get_reservation(miner, backing)
    except Exception:  # noqa: BLE001 - message quality only; fall back to the generic reason
        return None
    if _drawn_unfilled(resv) and str(resv.router) != str(user):
        return str(resv.router)
    return None


def _poll_reservation(client, miner, timeout_secs: int, backing: str):
    """Poll until a live, unclaimed Reservation exists — the shared ``live_unclaimed`` predicate (same
    one `post-tx` uses). Post-finalize the reservation is live; this guards against a lagging read."""
    deadline = time.time() + timeout_secs
    while time.time() < deadline:
        resv = client.get_reservation(miner, backing)
        if live_unclaimed(resv):
            return resv
        time.sleep(3)
    return None


_MINER_PICK = '@pick'
MINER_RATE_WARN_FRACTION = 0.5


def _net_receive(to_amount: int, to_chain: str) -> float:
    return apply_fee_deduction(to_amount, FEE_DIVISOR) / 10 ** get_chain_def(to_chain).decimals


def _named_intake(miner_opt, candidates, viable, from_chain, to_chain, from_amount, min_swap, max_swap, bounds=None):
    """Resolve an explicit --miner pubkey against the same gates auto-select uses. Hard-fails with
    the specific reason — never silently falls back to another miner."""
    chosen = next((p for p in viable if str(p[0].miner) == miner_opt), None)
    if chosen:
        return chosen
    cand = next((c for c in candidates if str(c.miner) == miner_opt), None)
    if cand is None:
        fail(f'Miner {miner_opt[:8]}… is not active or not quoting {from_chain}->{to_chain}.')
    amts = compute_intake_amounts(from_chain, to_chain, from_amount, cand.rate_display, cand.backing)
    lo, hi = (bounds or {}).get(cand.backing, (min_swap, max_swap))
    _, reason = swap_viable(amts.collateral_amount, cand.collateral, lo, hi, cand.backing)
    fail(f'Miner {miner_opt[:8]}… cannot take this swap: {reason or "rate not executable"}.')


def _pick_intake(viable, from_chain, to_chain):
    """Interactive miner picker over the viable set, best rate first."""
    if not sys.stdin.isatty():
        fail('Bare --miner needs a terminal; pass --miner <pubkey> when scripting.')
    ranked = sorted(viable, key=lambda p: p[1].to_amount, reverse=True)
    console.print(f'\n  Miners quoting {from_chain.upper()}->{to_chain.upper()} (best first):')
    for i, (c, amts) in enumerate(ranked, 1):
        rate_disp = directional_rate(from_chain, to_chain, c.rate_display)
        # The purse is in the BACKING's own unit (lamports for sol, rao for tao) — label it as such.
        purse = c.collateral / 10 ** get_chain_def(c.backing).decimals
        console.print(
            f'  [{i}] [dim]{c.miner}[/dim]  rate {rate_disp} {to_chain.upper()}/{from_chain.upper()}'
            f'  receive ~[cyan]{_net_receive(amts.to_amount, to_chain):.8g} {to_chain.upper()}[/cyan]'
            f'  collateral {purse:g} {c.backing.upper()}'
        )
    idx = click.prompt('  Miner #', type=click.IntRange(1, len(ranked)))
    return ranked[idx - 1]


class _RoutedUnavailable(Exception):
    """A routed attempt failed BEFORE any pool was entered on our behalf (no axon, rejected, or the
    dendrite went unanswered) — safe to offer the self-represented path instead."""


_ROUTED_SLACK_SECS = 30


def _send_reserve_synapse(axon, synapse) -> tuple:
    """One dendrite round-trip to the router. Returns (accepted, reason, pool_closes_at)."""
    responses = broadcast_synapse(get_ephemeral_wallet(), [axon], synapse, resolve_dendrite_timeout(60.0))
    info = render_and_aggregate(console, responses, label='router')
    resp = responses[0] if responses else None
    accepted = bool(getattr(resp, 'accepted', False))
    reason = getattr(resp, 'rejection_reason', None) or info.headline or 'no response from the router'
    return accepted, reason, int(getattr(resp, 'pool_closes_at', 0) or 0)


def _reserve_routed(client, miner, user, router_hotkey, netuid, synapse, pool_window, finalize_window):
    """Validator-routed reservation: ask ``router_hotkey`` to enter the pool for us, then wait for the
    seat to go live with OUR pubkey pinned — no self-crank, no finalize (the router does both, #558).
    Raises ``_RoutedUnavailable`` for any failure before a pool was entered; terminal post-entry
    outcomes (lost, unresolved) ``fail`` with re-run guidance. The subtensor connection is lazy —
    a fresh axon-cache hit sends the synapse without ever syncing the chain."""
    memo = {}

    def _subtensor():
        if 'st' not in memo:
            _cfg, _wallet, memo['st'], _ = get_cli_context(need_wallet=False)
        return memo['st']

    axon = find_validator_axon(_subtensor, netuid, router_hotkey)
    if axon is None:
        raise _RoutedUnavailable(f'router {router_hotkey[:8]}… is not a serving validator on netuid {netuid}')

    accepted, reason, pool_closes_at = _send_reserve_synapse(axon, synapse)
    if not accepted:
        # A cached axon may be stale (validators move IPs): refresh once, then retry the send.
        fresh_axon = find_validator_axon(_subtensor, netuid, router_hotkey, fresh=True)
        if fresh_axon is not None and (fresh_axon.ip, fresh_axon.port) != (axon.ip, axon.port):
            accepted, reason, pool_closes_at = _send_reserve_synapse(fresh_axon, synapse)
        if not accepted:
            invalidate_axon_cache()
            raise _RoutedUnavailable(f'router declined/unreachable: {reason}')

    console.print(
        f'[green]  Routed[/green] — {router_hotkey[:8]}… entered the pool for you (it pays the entry fee) '
        'and will finalize if it wins.\n'
        '  [yellow]Do NOT send any funds yet[/yellow] — wait for the reservation to go live below; '
        'early deposits are rejected by freshness checks and are not recoverable.'
    )
    now = int(time.time())
    wait = max(pool_closes_at - now, pool_window) + finalize_window + _ROUTED_SLACK_SECS
    resv = _poll_routed_reservation(client, miner, user, timeout_secs=wait)
    if resv is None:
        fail(
            '  The routed reservation did not resolve in time. No funds moved — do NOT send anything. '
            'Re-run to try again (resume-safe), or use --no-router to self-represent.'
        )
    if str(resv.user) != str(user):
        fail(
            '  You lost this round — the seat went to another user. The router paid the entry fee; no funds '
            'of yours moved. Rates may have moved too — re-running re-quotes fresh for the next round.'
        )
    return resv


def _poll_routed_reservation(client, miner, user, timeout_secs: int):
    """Poll until the router's win goes LIVE (finalized). Returns the live reservation as soon as one
    exists — the caller checks whether it pins OUR pubkey (won) or another user's (lost the pick).
    The ROUTER chose the backing (pool join or best offer), so every hub's slot is scanned."""
    deadline = time.time() + timeout_secs
    while time.time() < deadline:
        for hub in pdas.BACKING_BITS:
            resv = client.get_reservation(miner, hub)
            if live_unclaimed(resv):
                return resv
        time.sleep(3)
    return None


@swap_group.command('now', show_disclaimer=True)
@click.option('--from', 'from_chain_opt', default=None, help='Source chain (e.g. btc, tao)')
@click.option('--to', 'to_chain_opt', default=None, help='Destination chain (e.g. btc, tao)')
@click.option('--amount', 'amount_opt', default=None, type=FINITE_DECIMAL, help='Amount to send in source chain units')
@click.option('--receive-address', 'receive_address_opt', default=None, help='Receive address on destination chain')
@click.option('--from-address', 'from_address_opt', default=None, help='Source address on source chain')
@click.option('--from-tx-hash', 'from_tx_hash_opt', default=None, help='Source tx hash (skip fund sending)')
@click.option(
    '--miner',
    'miner_opt',
    is_flag=False,
    flag_value=_MINER_PICK,
    default=None,
    help='Miner pubkey to use instead of auto-select; bare --miner opens an interactive picker',
)
@click.option('--yes', 'skip_confirm', is_flag=True, help='Skip confirmation prompts')
@click.option('--router', 'router_opt', default=None, help='Validator hotkey to route through (overrides config)')
@click.option('--no-router', 'no_router', is_flag=True, help='Self-represent even when a router is configured')
@click.option(
    '--send/--no-send',
    'auto_send',
    default=None,
    help='Auto-send the source funds from your configured wallet after reserving, then relay and '
    'watch to completion. Default: on when interactive (prompts), off for scripts (print instructions).',
)
@click.option(
    '--btc-fee-rate',
    'btc_fee_rate_opt',
    type=click.IntRange(min=1),
    default=None,
    metavar='SAT_PER_VB',
    help=(
        'Fee rate for the BTC source tx, in satoshis per virtual byte (sat/vB). '
        'Higher = faster confirmation. Typical mainnet values: 5-20. Default '
        'auto-estimates from the mempool. Lightweight wallet only.'
    ),
)
def swap_now_command(
    from_chain_opt: Optional[str],
    to_chain_opt: Optional[str],
    amount_opt: Optional[Decimal],
    receive_address_opt: Optional[str],
    from_address_opt: Optional[str],
    from_tx_hash_opt: Optional[str],
    miner_opt: Optional[str],
    skip_confirm: bool,
    router_opt: Optional[str],
    no_router: bool,
    auto_send: Optional[bool],
    btc_fee_rate_opt: Optional[int],
):
    """Originate a swap: reserve a miner on-chain, then send source funds.

    [dim]Routed by default when a router is configured (`alw config set router <validator-hotkey>`):
    the validator enters the pool with its stake weight and finalizes for you. --no-router
    self-represents (you bid, crank, and finalize yourself, paying the entry fee).

    On a TTY, omitted inputs are prompted for; scripted/non-TTY runs must pass every flag.
        alw swap now --from sol --to btc --amount 1.0 --receive-address <btc-addr> --yes[/dim]
    """
    chains = ', '.join(SUPPORTED_CHAINS)
    from_chain_opt = _prompt_missing(from_chain_opt, f'Source chain ({chains})', '--from')
    to_chain_opt = _prompt_missing(to_chain_opt, f'Destination chain ({chains})', '--to')
    from_chain = (from_chain_opt or '').lower()
    to_chain = (to_chain_opt or '').lower()
    if from_chain not in SUPPORTED_CHAINS or to_chain not in SUPPORTED_CHAINS:
        fail(f'--from/--to must each be one of: {", ".join(SUPPORTED_CHAINS)}')
    if from_chain == to_chain or hub_leg(from_chain, to_chain) is None:
        fail('A swap must have a hub leg (SOL or TAO) and two distinct chains — spoke<->spoke has no market.')
    if amount_opt is None:
        amount_opt = _prompt_missing(None, 'Amount (source units)', '--amount', cast=Decimal)
    if amount_opt is None or amount_opt <= 0:
        fail('--amount (source-chain units) must be positive.')
    if not receive_address_opt:
        receive_address_opt = _prompt_missing(
            None, f'Receive address (your {to_chain.upper()} address)', '--receive-address'
        )

    config, client = get_solana_cli_context(need_keypair=True)
    config = config or {}
    user = client.keypair.pubkey()
    router_hotkey = (router_opt or config.get('router') or '').strip()
    routed = bool(router_hotkey) and not no_router
    # A Solana-wallet source (SOL, or an SPL token beside it) is sent from the signer itself.
    if not uses_solana_wallet(from_chain) and not from_address_opt:
        from_address_opt = _prompt_missing(
            None, f'Source address (your {from_chain.upper()} address you send from)', '--from-address'
        )
    user_from_addr = str(user) if uses_solana_wallet(from_chain) else (from_address_opt or '')
    if not user_from_addr:
        fail(f'--from-address (your source-chain address) is required for a non-{NUMERAIRE_CHAIN.upper()} source.')
    # Canonical source form before anything commits it: the finalize hash + source-lock PDA are
    # byte-keyed on this string (V-C2), so a cased variant would mint a divergent lock on-chain.
    _src_gate = _gate_provider(from_chain, client, config)
    if _src_gate is not None:
        user_from_addr = _src_gate.chain.normalize_address(user_from_addr)

    cfg = client.get_config()
    bounds = bounds_from_config(cfg) if cfg else {}
    min_swap, max_swap = hub_bounds(bounds, from_chain, to_chain)
    pool_window = int(getattr(cfg, 'pool_window_secs', 60)) if cfg else 60
    finalize_window = int(getattr(cfg, 'finalize_window_secs', 150)) if cfg else 150

    from_amount = to_smallest_units(amount_opt, from_chain)
    candidates = candidate_miners(client, from_chain, to_chain)
    if not candidates:
        fail(f'No miners quoting {from_chain}->{to_chain} right now.')
    if miner_opt:
        viable = viable_intakes(candidates, from_chain, to_chain, from_amount, min_swap, max_swap, bounds)
        if not viable:
            reason = unviable_reason(candidates, from_chain, to_chain, from_amount, min_swap, max_swap, bounds)
            fail(f'No miner can take this swap: {reason}.')
        best_to = max(p[1].to_amount for p in viable)
        if miner_opt == _MINER_PICK:
            cand, amts = _pick_intake(viable, from_chain, to_chain)
        else:
            cand, amts = _named_intake(
                miner_opt, candidates, viable, from_chain, to_chain, from_amount, min_swap, max_swap, bounds
            )
        if amts.to_amount < best_to * (1 - MINER_RATE_WARN_FRACTION):
            pct = (1 - amts.to_amount / best_to) * 100
            console.print(
                f'  [yellow]This miner pays {pct:.0f}% less than the best available[/yellow] '
                f'(~{_net_receive(best_to, to_chain):.8g} {to_chain.upper()}).'
            )
    else:
        best = select_best_miner(candidates, from_chain, to_chain, from_amount, min_swap, max_swap, bounds)
        if best is None:
            reason = unviable_reason(candidates, from_chain, to_chain, from_amount, min_swap, max_swap, bounds)
            fail(f'No miner can take this swap: {reason}.')
        cand, amts = best

    # Deliverability screens — before the fee-charging entry AND before sending into a doomed swap.
    _screen_deliverability(client, config, cand, from_chain, to_chain, receive_address_opt, user_from_addr, from_amount)

    # Resume a seat this taker already holds rather than paying for a second bid: a prior run may have
    # bid + drawn (or even finalized) but crashed on a transient RPC before instructing the send. The
    # reused per-miner reservation makes `swap now` idempotent for THIS taker — recover it, don't re-bid.
    existing = client.get_reservation(cand.miner, cand.backing)
    resume_live = live_unclaimed(existing) and str(getattr(existing, 'user', '')) == str(user)
    resume_drawn = not resume_live and _drawn_unfilled(existing) and str(getattr(existing, 'router', '')) == str(user)
    resuming = resume_live or resume_drawn

    # Pool contention — surface it BEFORE the fee-charging entry so the taker isn't entering blind into
    # an already-open, contested round (skipped when resuming, since no new entry is placed).
    contention = _pool_contention(client, cand.miner, cfg, cand.backing)

    # Every fill in an open round settles at the round's PINNED rate — preview that, not the live
    # quote (which can drift after the pool opened and would show a receive the fill won't honor).
    pinned = contention.is_open and (contention.from_chain, contention.to_chain) == (from_chain, to_chain)
    if pinned and contention.rate > 0:
        amts = compute_intake_amounts(from_chain, to_chain, from_amount, rate_display_from_fixed(contention.rate))
    # Quote the NET dest leg — the miner delivers `to_amount` less the protocol fee, same as
    # `alw swap quote`. The gross `to_amount` is what gets pinned on-chain, not what you receive.
    recv = _net_receive(amts.to_amount, to_chain)

    rate_display = rate_display_from_fixed(contention.rate) if pinned and contention.rate > 0 else cand.rate_display
    rate_disp = directional_rate(from_chain, to_chain, rate_display)
    pinned_note = ' (pinned pool rate)' if pinned and contention.rate > 0 else ''
    console.print(
        f'\n  Swap [cyan]{amount_opt} {from_chain.upper()}[/cyan] -> ~[cyan]{recv:.8g} {to_chain.upper()}[/cyan]'
        f'  (miner [dim]{str(cand.miner)[:8]}…[/dim], rate {rate_disp} '
        f'{to_chain.upper()}/{from_chain.upper()}{pinned_note})\n'
    )
    # Which purse backs this offer decides what you're owed if the miner fails — disclose it.
    console.print(
        f'  [cyan]{backing_label(cand.backing)}[/cyan] — if the miner fails to deliver, '
        f'{GUARANTEE.get(cand.backing, "see docs")}.\n'
    )
    if contention.is_open and not resuming:
        fee_sol = int(getattr(cfg, 'reservation_fee_lamports', 0)) / 10 ** get_chain_def(NUMERAIRE_CHAIN).decimals
        if contention.weighted_rivals > 0:
            hopeless = '' if routed else ', so a self-represented taker is very unlikely to win this draw'
            console.print(
                f'  [yellow]This miner already has an open pool[/yellow]: {contention.bidders} entrant(s), '
                f'closes in {contention.closes_in}s — a weighted validator is competing{hopeless}.'
            )
        else:
            odds = 100.0 / (contention.bidders + 1)
            console.print(
                f'  [yellow]This miner already has an open pool[/yellow]: {contention.bidders} entrant(s), '
                f'closes in {contention.closes_in}s. Joining makes {contention.bidders + 1}; the draw '
                f'is uniform, so ~[cyan]{odds:.0f}%[/cyan] odds to win the seat.'
            )
        fee_note = (
            'Your router pays the entry fee, win or lose.'
            if routed
            else f'A losing entry still spends the {fee_sol:g} SOL reservation fee.'
        )
        console.print(f'  [dim]{fee_note}[/dim]')

    if not resuming and not skip_confirm:
        prompt = (
            f'  Ask {router_hotkey[:8]}… to reserve this miner for you? (the validator pays the entry fee)'
            if routed
            else '  Bid on this miner on-chain?'
        )
        if not click.confirm(prompt, default=False):
            return

    # Obtain a live reservation held by us: resume an existing seat, route through the validator,
    # or self-represent (bid, self-crank the draw, finalize against the PINNED rate).
    if resume_live:
        console.print('[green]  Resuming the reservation you already hold[/green] (skipping bid + finalize).')
        resv = existing
    elif routed and not resume_drawn:
        try:
            binding = client.get_binding(cand.miner)
            if binding is None:
                raise _RoutedUnavailable('miner has no hotkey binding to route by')
            console.print(f'  Routing via [cyan]{router_hotkey[:8]}…[/cyan] — the validator enters the pool for you.')
            synapse = SwapReserveSynapse(
                miner_hotkey=hotkey_bytes_to_ss58(bytes(binding.hotkey)),
                from_chain=from_chain,
                to_chain=to_chain,
                user_pubkey=str(user),
                user_from_addr=user_from_addr,
                user_to_addr=receive_address_opt,
                from_amount=from_amount,
            )
            netuid = int(config.get('netuid') or NETUID_FINNEY)
            resv = _reserve_routed(
                client, cand.miner, user, router_hotkey, netuid, synapse, pool_window, finalize_window
            )
        except _RoutedUnavailable as e:
            console.print(f'  [yellow]Routing failed[/yellow]: {e}')
            if not skip_confirm and not click.confirm(
                '  Enter the pool self-represented instead? You pay the reservation fee yourself.', default=False
            ):
                fail('  Aborted — no funds moved. Re-run to retry routing, or use --no-router.')
            resv = _reserve_self_represented(
                client,
                cand.miner,
                user,
                user_from_addr,
                receive_address_opt,
                from_chain,
                to_chain,
                from_amount,
                pool_window,
                drawn=None,
                backing=cand.backing,
            )
    else:
        resv = _reserve_self_represented(
            client,
            cand.miner,
            user,
            user_from_addr,
            receive_address_opt,
            from_chain,
            to_chain,
            from_amount,
            pool_window,
            drawn=existing if resume_drawn else None,
            backing=cand.backing,
        )
    recv = apply_fee_deduction(int(resv.to_amount), FEE_DIVISOR) / 10 ** get_chain_def(to_chain).decimals
    resv_backing = str(getattr(resv, 'collateral_chain', '') or '')
    console.print(
        f'[green]  Seat filled[/green] — receiving ~[cyan]{recv:.8g} {to_chain.upper()}[/cyan], '
        f'[cyan]{backing_label(resv_backing)}[/cyan].'
    )
    # Never instruct a send the reservation can't outlive: a deposit that lands after reserved_until
    # yields no claim, and the funds are stranded (straight to the miner — no escrow, no Swap, no
    # timeout, no refund). Confirmations accrue *after* the claim, so they don't belong in this margin.
    remaining = int(resv.reserved_until) - int(time.time())
    if remaining < _SEND_MARGIN_SECS:
        fail(
            f'  Reservation has only {remaining}s left — too short to land the claim for your '
            f'{from_chain.upper()} deposit (needs ~{_SEND_MARGIN_SECS}s to relay it on-chain). '
            'Do NOT send funds; re-run for a fresh reservation.'
        )

    _save_pending(cand.miner, from_chain, to_chain)

    # Wizard: auto-send the source leg from the configured wallet, relay, and watch to completion.
    # Default on when interactive (a Y/n confirm), off for scripts/agents unless --send. --no-send
    # forces the manual print-and-exit. Falls back to manual if this wallet can't sign the source.
    want_send = auto_send if auto_send is not None else sys.stdin.isatty()
    if want_send and _auto_send_wizard(
        client, config, resv, cand.miner, from_chain, to_chain, from_amount, skip_confirm, btc_fee_rate_opt
    ):
        return

    console.print(
        f'[green]  Reserved.[/green] Send [cyan]{amount_opt} {from_chain.upper()}[/cyan] to '
        f'[cyan]{resv.miner_from_addr}[/cyan], then run [bold]alw swap post-tx[/bold] with the tx hash.'
    )
    for line in _deadline_lines(int(resv.reserved_until), want_send):
        console.print(line)


def _deadline_lines(reserved_until: int, want_send: bool, now: Optional[int] = None) -> List[str]:
    """The send-deadline notice for the manual path, where the taker sends outside this process.

    `_SEND_MARGIN_SECS` is checked once, at reservation time. After that nothing re-checks it: the
    send and the relay happen elsewhere, and a deposit that lands after `reserved_until` yields no
    claim, no Swap, no timeout and no refund. So state the deadline as a wall-clock instant — a bare
    countdown is stale the moment it prints, and an agent that logs it can't tell when it was true.
    `reserved_until` is echoed raw so a script can parse it instead of the rendered time."""
    now = int(time.time()) if now is None else now
    at = time.strftime('%H:%M:%S UTC', time.gmtime(reserved_until))
    lines = [
        f'  [yellow]Deadline:[/yellow] post-tx must complete by [cyan]{at}[/cyan] '
        f'([cyan]{max(0, reserved_until - now)}s[/cyan] from now, reserved_until={reserved_until}). '
        'Miss it and the deposit is unrecoverable — there is no refund.'
    ]
    # The warning is unconditional: reaching this notice means the send happens outside `alw`, and
    # --send that fell back to manual (no creds, wrong wallet) is exactly the case that needs telling.
    # Only the hint is gated — pointless for someone who just asked for --send.
    lines.append('  [dim]Sending outside `alw` forfeits the pre-send safety check.[/dim]')
    if not want_send:
        lines.append('  [dim]`alw swap now --send` does the send, the relay and the watch in one step.[/dim]')
    return lines


def _gate_provider(chain: str, client, config):
    """Read-only provider for deliverability screens (no send creds, no startup check).
    None when it can't be built — the screens fail open; routed flows are re-gated by the
    validator either way."""
    from allways.assets import ASSET_REGISTRY

    spec = next((s for s in ASSET_REGISTRY if s.chain_id == chain), None)
    if spec is None:
        return None
    avail = {'solana_rpc_url': client.rpc.url, 'solana_keypair': client.keypair}
    try:
        return spec.cls(**{k: avail[k] for k in spec.kwarg_names if k in avail})
    except Exception:  # noqa: BLE001 - unbuildable provider (missing env) → screens fail open
        return None


def _screen_deliverability(client, config, cand, from_chain, to_chain, receive_addr, user_from_addr, from_amount):
    """Pre-reserve deliverability screens — bounce BEFORE any fee is spent.

    Self-represented flows have no validator to gate for them (F7), so mirror the
    validator's reserve gates locally, one leg at a time (a non-SOL hub pair has two screenable
    legs): the taker's receive address must be well-formed and accept the dest asset, and the
    miner's receive address must accept the source funds (T18). The source-address probe is a
    courtesy warning only — a frozen source just means the deposit fails and the reservation
    lapses unclaimed. A leg whose provider can't be built read-only fails open, as before."""
    dest_provider = _gate_provider(to_chain, client, config)
    quote = client.get_quote(cand.miner, from_chain, to_chain, cand.backing)
    if dest_provider is not None:
        # Validity only — deliverability is NOT predicted at reserve time (not a boundary; the sound
        # check is the delivery-time reverted-tx proof). A malformed address can never be delivered to.
        if not dest_provider.chain.is_valid_address(receive_addr):
            fail(f'  {receive_addr!r} is not a valid {to_chain.upper()} address. No funds moved.')
    src_provider = _gate_provider(from_chain, client, config)
    if src_provider is None:
        return
    miner_addr = getattr(quote, 'miner_from_addr', '') if quote else ''
    if miner_addr and (
        not src_provider.chain.is_valid_address(miner_addr) or not src_provider.can_deliver_to(miner_addr, from_amount)
    ):
        fail(
            f"  This miner's {from_chain.upper()} receive address cannot accept the source funds "
            '— pick another miner (--miner). No funds moved.'
        )
    if user_from_addr and not src_provider.can_deliver_to(user_from_addr, from_amount):
        console.print(
            f'  [yellow]Heads-up: your {from_chain.upper()} source address looks unable to move funds '
            '(frozen?). If the deposit fails, the reservation lapses unclaimed.[/yellow]'
        )


def _source_provider(from_chain: str, client, config):
    """Build ONLY the source chain's provider with this CLI's own signing creds (solana keypair /
    bt wallet / BTC WIF), from the same registry the neurons use — so a new spoke chain works with
    zero changes here. Uses the CLI's live RPC (never a fresh localhost default). Returns None
    (→ manual fallback) if the provider can't be built with send credentials."""
    from allways.assets import ASSET_REGISTRY

    spec = next((s for s in ASSET_REGISTRY if s.chain_id == from_chain), None)
    if spec is None:
        return None

    avail = {'solana_rpc_url': client.rpc.url, 'solana_keypair': client.keypair}
    if from_chain == 'tao':
        # Wallet only — do NOT unlock the coldkey here. `can_send_from` reads the public coldkeypub,
        # so we defer the (possibly interactive) unlock until AFTER the user confirms the send.
        _cfg, wallet, subtensor, _ = get_cli_context(need_wallet=True)
        avail.update(subtensor=subtensor, wallet=wallet)
    try:
        provider = spec.cls(**{k: avail[k] for k in spec.kwarg_names if k in avail})
        provider.check_connection(require_send=True)
    except Exception as e:  # noqa: BLE001 - missing creds (e.g. no BTC_PRIVATE_KEY) → manual fallback
        console.print(f'[dim]  Auto-send unavailable for {from_chain.upper()} ({e}); use the manual flow.[/dim]')
        return None
    return provider


def _unlock_coldkey_for_send(wallet) -> bool:
    """Unlock the bt coldkey to sign a TAO transfer — reading MINER_BITTENSOR_COLDKEY_PASSWORD from
    env first (seamless, no prompt), else prompting WITH context so the ask never reads as a bare,
    unexplained 'Enter your password:'. Returns False (→ manual fallback) if it can't unlock."""
    import os

    pw = os.environ.get('MINER_BITTENSOR_COLDKEY_PASSWORD')
    try:
        if pw:
            wallet.coldkey_file.save_password_to_env(pw)
        else:
            console.print(
                '  [dim]Unlock your Bittensor coldkey to sign the TAO transfer '
                '(set MINER_BITTENSOR_COLDKEY_PASSWORD to skip this):[/dim]'
            )
        wallet.unlock_coldkey()
        return True
    except Exception as e:  # noqa: BLE001 - wrong/absent password → fall back to manual send
        console.print(f'[dim]  Could not unlock coldkey ({e}); send the TAO yourself and run post-tx.[/dim]')
        return False


def _auto_send_wizard(client, config, resv, miner_pk, from_chain, to_chain, from_amount, skip_confirm, btc_fee_rate):
    """Send the source deposit from the configured wallet, relay it, and watch to a terminal state.
    Returns True if it drove the send (success or a clean stop), False to fall back to manual."""
    provider = _source_provider(from_chain, client, config)
    if provider is None:
        return False
    # SAFETY: the deposit MUST come from the reservation's pinned sender or the validator rejects it
    # (the exact wrong-key failure). Verify BEFORE moving any funds.
    if not provider.can_send_from(resv.from_addr):
        console.print(
            f'[dim]  Your configured {from_chain.upper()} wallet does not control the pinned source '
            f'address {resv.from_addr[:12]}… — sending it yourself and running post-tx.[/dim]'
        )
        return False

    amount_disp = int(resv.from_amount) / 10 ** get_chain_def(from_chain).decimals
    to_addr = resv.miner_from_addr
    wallet_label = {
        'tao': f'Bittensor coldkey ({config.get("wallet", "?")})',
        'btc': 'Bitcoin WIF wallet',
    }.get(from_chain, 'Solana keypair' if uses_solana_wallet(from_chain) else f'{from_chain.upper()} wallet')
    console.print(f'  [dim]Source: your configured {wallet_label}[/dim]  [cyan]{resv.from_addr}[/cyan]')
    if not skip_confirm and not click.confirm(
        f'  Send {amount_disp:g} {from_chain.upper()} to the miner now?',
        default=True,
    ):
        return False  # user declined auto-send → manual instructions

    # TAO leaves the encrypted coldkey — unlock it only now (after the confirm), with context.
    if from_chain == 'tao' and not _unlock_coldkey_for_send(provider.wallet):
        return False

    from allways.cli.swap_commands.post_tx import relay_deposit, resolve_relay_axons

    # Resolve validators BEFORE moving funds. This is the fragile network step — a subtensor websocket
    # connect + metagraph read — and it needs nothing from the send. Doing it first means a transient
    # connect failure aborts with the deposit untouched, instead of unwinding as a traceback *after* the
    # money is out (which stranded a deposit past its reservation TTL, with no claim, Swap, or refund).
    try:
        vconfig, _vw, subtensor, _ = get_cli_context(need_wallet=False)
        validator_axons, validator_names, accepts_needed = resolve_relay_axons(
            client, subtensor, int(vconfig['netuid'])
        )
    except Exception as e:  # noqa: BLE001 - funds still safe → clean fallback, nothing lost
        console.print(f'[yellow]  Could not reach the chain to resolve validators ({e}); no funds moved.[/yellow]')
        return False
    if not validator_axons:
        console.print('[yellow]  No serving validators found; no funds moved.[/yellow]')
        return False

    kw = {'from_address': resv.from_addr}
    if from_chain == 'btc' and btc_fee_rate is not None:
        kw['fee_rate_override'] = btc_fee_rate
    with loading(f'Sending {amount_disp:g} {from_chain.upper()} to the miner…'):
        sent = provider.send_amount(to_addr, int(resv.from_amount), **kw)
    if not sent:
        err = getattr(provider, 'last_send_error', None)
        fail(
            f'  Source send failed{f": {err}" if err else ""}. No claim relayed; your reservation is still live '
            '— retry `alw swap now` or send manually.'
        )
    tx_hash = sent[0]
    console.print(f'[green]  Sent[/green] {amount_disp:g} {from_chain.upper()} — [cyan]{tx_hash}[/cyan]')

    # Funds are OUT. Past this line no exception may escape as a traceback: the deposit is idempotent to
    # relay, so any failure must become a recoverable "re-run post-tx" instruction inside the TTL.
    miner_hotkey = _miner_hotkey(client, miner_pk)
    try:
        swap_key = relay_deposit(
            client,
            resv,
            miner_pk,
            miner_hotkey,
            tx_hash,
            validator_axons=validator_axons,
            validator_names=validator_names,
            accepts_needed=accepts_needed,
        )
    except Exception as e:  # noqa: BLE001 - money committed; convert any error into a re-run, never a crash
        fail(
            f'  Deposit sent ({tx_hash}) but the confirm relay errored: {e}. '
            f'Re-run `alw swap post-tx {tx_hash}` now, before the reservation expires.'
        )
    if swap_key is None:
        fail(
            f'  Deposit sent but no validator accepted the relay yet. Re-run `alw swap post-tx {tx_hash}` in a moment.'
        )
    _watch_swap(client, swap_key, to_chain)
    return True


def _miner_hotkey(client, miner_pk) -> str:
    b = client.get_binding(miner_pk)
    return hotkey_bytes_to_ss58(b.hotkey) if b else ''


# Plain-English gloss for each on-chain swap status, shown beside it while watching.
_STATUS_NOTE = {
    'PendingAttestation': 'validators verifying your deposit',
    'Active': 'deposit confirmed — miner is sending your funds',
    'Fulfilled': 'miner delivered — validators confirming both legs',
}


def _swap_verdict(client, key: bytes):
    """Read the closing verdict (SwapCompleted/SwapTimedOut) off the swap PDA's final tx, newest
    first. None when unresolvable (RPC hiccup, reaped claim) — callers must stay neutral then."""
    from allways.solana import pdas
    from allways.solana.events import decode_event

    try:
        pda = pdas.swap_pda(key, client.program_id)
        for entry in client.rpc.get_signatures_for_address(pda, limit=3):
            for raw in client.get_event_logs(entry['signature']):
                ev = decode_event(raw)
                if ev is not None and ev[0] in ('SwapCompleted', 'SwapTimedOut'):
                    return ev
    except Exception:
        return None
    return None


def _watch_swap(client, swap_key_hex: str, to_chain: str, timeout_secs: int = 900) -> None:
    """Watch a just-relayed swap to a terminal state, printing transitions. A closed account is the
    SUCCESS signal (swaps close on settle) — so once we've seen it live, its disappearance reads as
    COMPLETED, never the bare 'never existed' scare."""
    key = bytes.fromhex(swap_key_hex)
    resume = f'[dim]  Resume anytime with `alw view swap {swap_key_hex} --watch`.[/dim]'
    console.print(f'[dim]  Watching your swap — this settles on its own; Ctrl-C is safe to walk away.[/dim]\n{resume}')
    last, seen_live, seen_fulfilled = None, False, False
    deadline = time.time() + timeout_secs
    try:
        while time.time() < deadline:
            try:
                acct = client.get_swap(key)
            except Exception:
                acct = None
            if acct is not None:
                seen_live = True
                status = type(acct.status).__name__
                if status != last:
                    console.print(f'    {status:<19}[dim]{_STATUS_NOTE.get(status, "")}[/dim]')
                    last = status
                if status == 'Fulfilled':
                    seen_fulfilled = True
            elif seen_live:
                # Closed ≠ delivered: a timeout closes the account too (the fix for reading it as
                # success). Pull the closing event for the honest message; stay neutral if unreadable.
                verdict = _swap_verdict(client, key)
                if verdict is not None and verdict[0] == 'SwapTimedOut':
                    ev = verdict[1]
                    chain = str(ev.collateral_chain)
                    amt = int(ev.reimbursement) / 10 ** get_chain_def(chain).decimals
                    # 'sol' settles in the closing tx itself; other backings pay out via the vault
                    # relay a few minutes later — don't claim money moved before it has.
                    paid = 'You were reimbursed' if chain == NUMERAIRE_CHAIN else 'You are being reimbursed'
                    console.print(
                        f'[red]  ✗ MINER FAILED[/red] — it never delivered, and was slashed. {paid} '
                        f'[bold]{amt:g} {chain.upper()}[/bold] from its bond '
                        f'(your full payout + penalty) to [cyan]{ev.payee}[/cyan].'
                    )
                elif verdict is not None and verdict[0] == 'SwapCompleted':
                    console.print(
                        f'[green]  ✓ COMPLETED[/green] — settled on-chain, your {to_chain.upper()} was delivered.'
                    )
                else:
                    console.print(
                        f'[yellow]  Swap closed on-chain[/yellow] — verdict unreadable from here; '
                        f'check your {to_chain.upper()} balance or `alw view swap {swap_key_hex}`.'
                    )
                return
            time.sleep(4)
    except KeyboardInterrupt:
        console.print(f'\n[dim]  Stopped watching (the swap keeps settling on its own).[/dim]\n{resume}')
        return
    tail = 'delivered' if seen_fulfilled else 'check `alw status` for your balance'
    console.print(f'[yellow]  Still settling after {timeout_secs}s[/yellow] — {tail}.\n{resume}')


def _reserve_self_represented(
    client,
    miner,
    user,
    user_from_addr,
    user_to_addr,
    from_chain,
    to_chain,
    from_amount,
    pool_window,
    drawn=None,
    backing=NUMERAIRE_CHAIN,
):
    """Self-represented phases 1-3: bid (unless resuming a ``drawn`` seat), self-crank the draw, and
    finalize against the PINNED rate. Returns the live reservation or ``fail``s with send-safety
    guidance — a taker must never send funds without a live reservation pinning them."""
    if drawn is not None:
        console.print('[green]  Resuming the seat you already drew[/green] (skipping bid); finalizing…')
    else:
        # Phase 1 — BID (pair only; no taker, no amounts). A contract rejection (miner busy,
        # already reserved, …) is a normal outcome — fail with its message, never a traceback.
        try:
            sig = client.open_or_request(miner, from_chain, to_chain, backing)
        except Exception as e:
            reason = contract_reject_reason(e)
            if reason is None:
                raise
            fail(f'  Reservation rejected: {reason}. No funds moved; re-run shortly.')
        console.print(f'[green]  Bid placed[/green] (tx {sig[:16]}…). Cranking the draw…')

        # Phase 2 — self-crank the draw until we're seated (unfilled reservation, router == us).
        drawn = _poll_drawn(client, miner, user, timeout_secs=pool_window + 120, backing=backing)
        if drawn is None:
            winner = _lost_seat_to(client, miner, user, backing)
            if winner:
                fail(
                    f'  You lost the draw — the seat went to [dim]{winner[:8]}…[/dim]. Your reservation fee is '
                    'spent (non-refundable); do NOT send funds. Rates may have moved — re-running re-quotes '
                    'fresh for the next round.'
                )
            fail(
                '  The draw did not resolve in the bid window (no seat drawn yet). Do NOT send funds; re-run to try again.'
            )

    # Phase 3 — FINALIZE against the PINNED rate (not the live quote, which can drift after the bid).
    fill = compute_intake_amounts(from_chain, to_chain, from_amount, rate_display_from_fixed(drawn.rate), backing)
    try:
        client.finalize_reservation(
            miner,
            user,
            user_from_addr,
            user_to_addr,
            fill.collateral_amount,
            fill.from_amount,
            fill.to_amount,
            backing,
            from_chain=from_chain,
        )
    except Exception as e:
        reason = contract_reject_reason(e)
        if reason is None:
            raise
        fail(f'  Finalize rejected: {reason}. Do NOT send funds; re-run shortly.')
    resv = _poll_reservation(client, miner, timeout_secs=30, backing=backing)
    if resv is None or str(resv.user) != str(user):
        fail('  Finalize did not produce a live reservation for you. Do NOT send funds; re-run.')
    return resv


def _save_pending(miner, from_chain: str, to_chain: str) -> None:
    """Persist the reserved miner so `alw view reservation` / `alw status` can find it without a flag."""
    try:
        PENDING_SWAP_FILE.parent.mkdir(parents=True, exist_ok=True)
        PENDING_SWAP_FILE.write_text(json.dumps({'miner': str(miner), 'from_chain': from_chain, 'to_chain': to_chain}))
    except OSError:
        pass  # best-effort convenience; not required for the swap to proceed
