"""alw swap - Execute and manage cross-chain swaps.

Origination is on-chain on Solana, two-phase: the taker BIDS into a per-miner reservation pool
(`open_or_request`, pair only), a permissionless stake-weighted draw (`resolve_pool`) seats a winner,
then the seat winner FILLS the reservation (`finalize_reservation`, naming the taker + amounts). `swap
now` is the unrouted-taker path — the taker is its own router: bid → self-crank the draw → finalize
against the pinned rate → then send source funds + `swap post-tx`. Self-cranking the draw means an
unrouted taker never waits on validator liveness."""

import json
import time
from typing import NamedTuple, Optional

import click

from allways.chains import SUPPORTED_CHAINS, get_chain
from allways.cli.help import StyledGroup
from allways.cli.swap_commands.helpers import (
    FINITE_FLOAT,
    PENDING_SWAP_FILE,
    console,
    fail,
    get_solana_cli_context,
    live_unclaimed,
)
from allways.cli.swap_commands.swap_intake import (
    candidate_miners,
    compute_intake_amounts,
    rate_display_from_fixed,
    select_best_miner,
    to_smallest_units,
)
from allways.constants import FEE_DIVISOR, NUMERAIRE_CHAIN
from allways.solana.rpc import TransientRpcError
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


def _pool_contention(client, miner, cfg) -> PoolContention:
    """Best-effort read of the miner's pool. Visibility only — never raises, so a decode/RPC hiccup
    can't block a bid; on any surprise it reports 'not open' and the taker proceeds as before."""
    try:
        pool = client.get_pool(miner)
        now = int(time.time())
        if int(getattr(pool, 'opened_at', 0)) == 0 or now > int(getattr(pool, 'closes_at', 0)):
            return PoolContention(False, 0, 0, 0)
        reqs = list(getattr(pool, 'requests', []))
        weighted = {bytes(v.key) for v in getattr(cfg, 'validators', []) if int(getattr(v, 'weight', 0)) > 0}
        weighted_rivals = sum(1 for r in reqs if bytes(r.router) in weighted)
        return PoolContention(True, len(reqs), max(0, int(pool.closes_at) - now), weighted_rivals)
    except Exception:  # noqa: BLE001 - visibility only; a bad read must not stop the swap
        return PoolContention(False, 0, 0, 0)


# Minimum reservation life required before we'll instruct a send. The reservation must outlive
# broadcast -> mempool visibility -> post-tx relay -> submit_swap_claim landing, which is all the
# on-chain claim gate checks (`reserved_until >= now`, empty claim slot). Chain-independent by
# construction: 60s post-tx dendrite timeout + ~60s Solana claim landing + 60s slack.
_SEND_MARGIN_SECS = 180


# Benign crank races — `resolve_pool` lost to another cranker (the validator, or a peer taker) or ran
# before the draw was possible. Each surfaces in TWO string forms depending on where it was caught:
#   • Anchor error NAME — when the tx is rejected in pre-flight simulation (e.g. "PoolNotClosed").
#   • numeric CODE only — when the tx is submitted and *lands* in a failed state; the confirm path
#     stringifies `status["err"]` as `{'InstructionError': [0, {'Custom': 6044}]}`, with no name.
# Matching names alone (the old behavior) missed the code-only form, so a benign race that reached the
# chain re-raised and aborted `swap now` — abandoning the taker's already-paid, since-drawn seat until
# it expired (fee forfeited, miner locked busy_until). Match both forms. Codes are ErrorCode indices.
_BENIGN_CRANK_NAMES = ('SeedSlotNotYetProduced', 'PoolNotClosed', 'NoRequests', 'AlreadyFilled')
_BENIGN_CRANK_CODES = (6045, 6042, 6044, 6046)  # same order as the names above


def _self_crank_resolve(client, miner) -> None:
    """Permissionless arm-then-draw crank. An unrouted taker cranks its own pool so the draw never
    waits on validator liveness. Benign races (window not closed, seed slot not produced yet, already
    resolved/filled) are expected and retried on the next poll."""
    try:
        client.resolve_pool(miner)
    except TransientRpcError:
        return  # RPC hiccup while nudging the pool — the poll loop re-cranks and re-reads the real
        # outcome (the reservation). If this resolve_pool actually landed, the next pass sees the seat.
    except Exception as e:  # noqa: BLE001
        msg = str(e)
        benign = any(n in msg for n in _BENIGN_CRANK_NAMES) or any(f"'Custom': {c}" in msg for c in _BENIGN_CRANK_CODES)
        if not benign:
            raise


def _drawn_unfilled(resv) -> bool:
    """A seat freshly won by the draw and not yet named. `created_at == 0` is load-bearing: a
    reservation that was filled and then consumed also has `reserved_until == 0`, but carries a
    non-zero `created_at` — matching it would finalize against a dead window. Same guard as the
    contract's `close_unfilled_reservation`."""
    if resv is None:
        return False
    return int(resv.reserved_until) == 0 and int(resv.created_at) == 0 and int(resv.finalize_by) > time.time()


def _poll_drawn(client, miner, user, timeout_secs: int):
    """Poll until THIS taker's bid draws its UNFILLED reservation, self-cranking `resolve_pool` each
    pass. Returns the drawn reservation, or None on timeout / if a different router won the seat."""
    us = str(user)
    deadline = time.time() + timeout_secs
    while time.time() < deadline:
        _self_crank_resolve(client, miner)
        resv = client.get_reservation(miner)
        if _drawn_unfilled(resv):
            return resv if str(resv.router) == us else None  # seated: us, or someone else won
        time.sleep(3)
    return None


def _lost_seat_to(client, miner, user) -> Optional[str]:
    """Best-effort: after `_poll_drawn` returns None, tell the two failure modes apart. Returns the
    winning router (as a string) if a *different* taker holds the freshly-drawn seat — i.e. you lost
    the draw — or None if no seat is drawn yet (the draw simply didn't resolve in the window)."""
    try:
        resv = client.get_reservation(miner)
    except Exception:  # noqa: BLE001 - message quality only; fall back to the generic reason
        return None
    if _drawn_unfilled(resv) and str(resv.router) != str(user):
        return str(resv.router)
    return None


def _poll_reservation(client, miner, timeout_secs: int):
    """Poll until a live, unclaimed Reservation exists — the shared ``live_unclaimed`` predicate (same
    one `post-tx` uses). Post-finalize the reservation is live; this guards against a lagging read."""
    deadline = time.time() + timeout_secs
    while time.time() < deadline:
        resv = client.get_reservation(miner)
        if live_unclaimed(resv):
            return resv
        time.sleep(3)
    return None


@swap_group.command('now', show_disclaimer=True)
@click.option('--from', 'from_chain_opt', default=None, help='Source chain (e.g. btc, tao)')
@click.option('--to', 'to_chain_opt', default=None, help='Destination chain (e.g. btc, tao)')
@click.option('--amount', 'amount_opt', default=None, type=FINITE_FLOAT, help='Amount to send in source chain units')
@click.option('--receive-address', 'receive_address_opt', default=None, help='Receive address on destination chain')
@click.option('--from-address', 'from_address_opt', default=None, help='Source address on source chain')
@click.option('--from-tx-hash', 'from_tx_hash_opt', default=None, help='Source tx hash (skip fund sending)')
@click.option('--yes', 'skip_confirm', is_flag=True, help='Skip confirmation prompts')
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
    amount_opt: Optional[float],
    receive_address_opt: Optional[str],
    from_address_opt: Optional[str],
    from_tx_hash_opt: Optional[str],
    skip_confirm: bool,
    btc_fee_rate_opt: Optional[int],
):
    """Originate a swap: reserve a miner on-chain, then send source funds.

    [dim]Flag-driven form (interactive prompts + auto fund-sending land next):
        alw swap now --from sol --to btc --amount 1.0 --receive-address <btc-addr> --yes[/dim]
    """
    from_chain = (from_chain_opt or '').lower()
    to_chain = (to_chain_opt or '').lower()
    if from_chain not in SUPPORTED_CHAINS or to_chain not in SUPPORTED_CHAINS:
        fail(f'--from/--to must each be one of: {", ".join(SUPPORTED_CHAINS)}')
    if from_chain == to_chain or NUMERAIRE_CHAIN not in (from_chain, to_chain):
        fail(f'A launch swap must have a {NUMERAIRE_CHAIN.upper()} leg (every pair is hub<->spoke).')
    if amount_opt is None or amount_opt <= 0:
        fail('--amount (source-chain units) is required.')
    if not receive_address_opt:
        fail('--receive-address (destination chain) is required.')

    _config, client = get_solana_cli_context(need_keypair=True)
    user = client.keypair.pubkey()
    user_from_addr = str(user) if from_chain == NUMERAIRE_CHAIN else (from_address_opt or '')
    if not user_from_addr:
        fail(f'--from-address (your source-chain address) is required for a non-{NUMERAIRE_CHAIN.upper()} source.')

    cfg = client.get_config()
    min_swap = int(getattr(cfg, 'min_swap_amount', 0)) if cfg else 0
    max_swap = int(getattr(cfg, 'max_swap_amount', 0)) if cfg else 0
    pool_window = int(getattr(cfg, 'pool_window_secs', 60)) if cfg else 60

    from_amount = to_smallest_units(amount_opt, from_chain)
    candidates = candidate_miners(client, from_chain, to_chain)
    if not candidates:
        fail(f'No miners quoting {from_chain}->{to_chain} right now.')
    best = select_best_miner(candidates, from_chain, to_chain, from_amount, min_swap, max_swap)
    if best is None:
        fail('No miner can fund an executable swap for that amount within bounds.')
    cand, amts = best
    # Quote the NET dest leg — the miner delivers `to_amount` less the protocol fee, same as
    # `alw swap quote`. The gross `to_amount` is what gets pinned on-chain, not what you receive.
    recv = apply_fee_deduction(amts.to_amount, FEE_DIVISOR) / 10 ** get_chain(to_chain).decimals

    rate_disp = directional_rate(from_chain, to_chain, cand.rate_display)
    console.print(
        f'\n  Swap [cyan]{amount_opt} {from_chain.upper()}[/cyan] -> ~[cyan]{recv:.8g} {to_chain.upper()}[/cyan]'
        f'  (miner [dim]{str(cand.miner)[:8]}…[/dim], rate {rate_disp} {to_chain.upper()}/{from_chain.upper()})\n'
    )

    # Resume a seat this taker already holds rather than paying for a second bid: a prior run may have
    # bid + drawn (or even finalized) but crashed on a transient RPC before instructing the send. The
    # reused per-miner reservation makes `swap now` idempotent for THIS taker — recover it, don't re-bid.
    existing = client.get_reservation(cand.miner)
    resume_live = live_unclaimed(existing) and str(getattr(existing, 'user', '')) == str(user)
    resume_drawn = not resume_live and _drawn_unfilled(existing) and str(getattr(existing, 'router', '')) == str(user)
    resuming = resume_live or resume_drawn

    # Pool contention — surface it BEFORE the fee-charging bid so the taker isn't bidding blind into an
    # already-open, contested round (skipped when resuming, since no new bid is placed).
    contention = _pool_contention(client, cand.miner, cfg)
    if contention.is_open and not resuming:
        fee_sol = int(getattr(cfg, 'reservation_fee_lamports', 0)) / 10 ** get_chain(NUMERAIRE_CHAIN).decimals
        if contention.weighted_rivals > 0:
            console.print(
                f'  [yellow]This miner already has an open bid pool[/yellow]: {contention.bidders} bidder(s), '
                f'closes in {contention.closes_in}s — and a weighted validator is bidding, so an unrouted '
                f'taker is very unlikely to win this draw.'
            )
        else:
            odds = 100.0 / (contention.bidders + 1)
            console.print(
                f'  [yellow]This miner already has an open bid pool[/yellow]: {contention.bidders} taker(s) '
                f'bidding, closes in {contention.closes_in}s. Joining makes {contention.bidders + 1}; the draw '
                f'is uniform, so ~[cyan]{odds:.0f}%[/cyan] odds to win the seat.'
            )
        console.print(f'  [dim]A losing bid still spends the {fee_sol:g} SOL reservation fee.[/dim]')

    if not resuming and not skip_confirm and not click.confirm('  Bid on this miner on-chain?', default=False):
        return

    # Phases 1-3 — obtain a live reservation held by us: resume an existing seat if we have one,
    # otherwise bid, self-crank the draw, and finalize against the PINNED rate.
    if resume_live:
        console.print('[green]  Resuming the reservation you already hold[/green] (skipping bid + finalize).')
        resv = existing
    else:
        if resume_drawn:
            console.print('[green]  Resuming the seat you already drew[/green] (skipping bid); finalizing…')
            drawn = existing
        else:
            # Phase 1 — BID (pair only; no taker, no amounts).
            sig = client.open_or_request(cand.miner, from_chain, to_chain)
            console.print(f'[green]  Bid placed[/green] (tx {sig[:16]}…). Cranking the draw…')

            # Phase 2 — self-crank the draw until we're seated (unfilled reservation, router == us).
            drawn = _poll_drawn(client, cand.miner, user, timeout_secs=pool_window + 120)
            if drawn is None:
                winner = _lost_seat_to(client, cand.miner, user)
                if winner:
                    fail(
                        f'  You lost the draw — the seat went to [dim]{winner[:8]}…[/dim]. Your reservation fee is '
                        'spent (non-refundable); do NOT send funds. Re-run to bid on the next round.'
                    )
                fail(
                    '  The draw did not resolve in the bid window (no seat drawn yet). Do NOT send funds; re-run to try again.'
                )

        # Phase 3 — FINALIZE against the PINNED rate (not the live quote, which can drift after the bid).
        fill = compute_intake_amounts(from_chain, to_chain, from_amount, rate_display_from_fixed(drawn.rate))
        client.finalize_reservation(
            cand.miner,
            user,
            user_from_addr,
            receive_address_opt,
            fill.collateral_amount,
            fill.from_amount,
            fill.to_amount,
        )
        resv = _poll_reservation(client, cand.miner, timeout_secs=30)
        if resv is None or str(resv.user) != str(user):
            fail('  Finalize did not produce a live reservation for you. Do NOT send funds; re-run.')
    recv = apply_fee_deduction(int(resv.to_amount), FEE_DIVISOR) / 10 ** get_chain(to_chain).decimals
    console.print(f'[green]  Seat filled[/green] — receiving ~[cyan]{recv:.8g} {to_chain.upper()}[/cyan].')
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
    console.print(
        f'[green]  Reserved.[/green] Send [cyan]{amount_opt} {from_chain.upper()}[/cyan] to '
        f'[cyan]{resv.miner_from_addr}[/cyan], then run [bold]alw swap post-tx[/bold] with the tx hash.'
    )


def _save_pending(miner, from_chain: str, to_chain: str) -> None:
    """Persist the reserved miner so `alw view reservation` / `alw status` can find it without a flag."""
    try:
        PENDING_SWAP_FILE.parent.mkdir(parents=True, exist_ok=True)
        PENDING_SWAP_FILE.write_text(json.dumps({'miner': str(miner), 'from_chain': from_chain, 'to_chain': to_chain}))
    except OSError:
        pass  # best-effort convenience; not required for the swap to proceed
