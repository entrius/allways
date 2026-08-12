"""Transport-agnostic kernel ops for the on-behalf reservation flow.

One source of truth for reserve / confirm / rate / status, shared by the axon synapse handlers
(CLI transport) and the localhost HTTP seam (offering transport). Every op validates protocol
invariants before it signs — the caller (offering or CLI) is never trusted.
"""

import time
from dataclasses import dataclass, field
from typing import Optional

import bittensor as bt
from bittensor import Keypair
from solders.pubkey import Pubkey

from allways.assets.asset import ProviderUnreachableError
from allways.chains import SUPPORTED_CHAINS, canonical_pair
from allways.cli.swap_commands.swap_intake import (
    MinerCandidate,
    backing_purse,
    bounds_from_config,
    candidate_miners,
    compute_intake_amounts,
    hub_bounds,
    max_intake_from_amount,
    rate_display_from_fixed,
    select_best_miner,
    swap_viable,
    unviable_reason,
)
from allways.constants import NUMERAIRE_CHAIN
from allways.solana.client import contract_reject_reason, swap_key_from_tx_hash
from allways.solana.pdas import BACKING_BITS
from allways.validator.binding import hotkey_ss58, verify_binding

EMPTY_SWAP_KEY = b'\x00' * 32


def resolve_miner_pubkey(validator, miner_hotkey: str) -> Optional[Pubkey]:
    """Map a Bittensor hotkey (ss58) → the miner's bound Solana pubkey via the HotkeyBinding PDA.

    None if unbound or the sr25519 sig fails to verify (the contract stores it unverified, so a
    squatter could set a victim's marker with a garbage sig — we re-verify here, as scoring does)."""
    hotkey_bytes = bytes.fromhex(Keypair(ss58_address=miner_hotkey).public_key.hex())
    hk_binding = validator.solana_client.get_hotkey_binding(hotkey_bytes)
    if hk_binding is None:
        return None
    binding = validator.solana_client.get_binding(hk_binding.miner)
    if binding is None or bytes(binding.hotkey) != hotkey_bytes:
        return None
    if not verify_binding(hk_binding.miner, binding.hotkey, binding.hotkey_sig):
        bt.logging.warning(f'binding for {miner_hotkey}: invalid sr25519 sig, refusing to resolve')
        return None
    return hk_binding.miner


def _best_offer(client, miner_pk, miner_state, from_chain: str, to_chain: str, from_amount: int, bounds):
    """The offer of this miner's that gives the user the most — one market per pair, mixed by rate
    (D2), NOT a preference for either purse. Reuses the taker's selector, so a routed user and a
    self-represented one pick the same offer, including its exact-tie preference for "sol".
    Returns ``((quote, backing), '')`` or ``(None, reason)`` — the reason is the taker-facing one
    from the same gate set, so a routed rejection reads like a self-represented one."""
    offers = {}
    candidates = []
    for q in client.get_quotes_for_direction(miner_pk, from_chain, to_chain) or []:
        if q is None:
            continue
        backing = str(getattr(q, 'collateral_chain', NUMERAIRE_CHAIN) or NUMERAIRE_CHAIN)
        purse = backing_purse(client, miner_pk, miner_state, backing)
        if purse is None:
            continue  # no locked bond behind it — the contract's entry gate would refuse the bid
        offers[backing] = q
        candidates.append(MinerCandidate(miner_pk, rate_display_from_fixed(q.rate), purse, backing))
    if not candidates:
        return None, f'miner has no quote for {from_chain}->{to_chain}'
    hub_min, hub_max = hub_bounds(bounds, from_chain, to_chain)
    best = select_best_miner(candidates, from_chain, to_chain, from_amount, hub_min, hub_max, bounds)
    if best is None:
        return None, unviable_reason(candidates, from_chain, to_chain, from_amount, hub_min, hub_max, bounds)
    return (offers[best[0].backing], best[0].backing), ''


@dataclass
class ReserveResult:
    ok: bool
    reason: str = ''
    pool_closes_at: int = 0
    sig: str = ''


def reserve_on_behalf(
    validator,
    miner_hotkey: str,
    from_chain: str,
    to_chain: str,
    user_pubkey: str,
    user_from_addr: str,
    user_to_addr: str,
    from_amount: int,
) -> ReserveResult:
    """Enter the miner's reservation pool on the user's behalf (validator = router).

    Idempotent per miner within a window: a fresh miner OPENS the pool, a repeat call from this
    validator UPSERTs its request (free). ``to_amount`` is derived from the PINNED pool rate when a
    pool is open (so joiners stay rate-consistent for D1) else the miner's live quote (which the
    contract pins at open). Validates eligibility + bounds before paying the fee.
    """
    # Chain ids are lowercase everywhere (the program enforces it at intake); reject a
    # cased/unknown id here too so it can't derive a mismatched quote PDA or pool first.
    if from_chain not in SUPPORTED_CHAINS or to_chain not in SUPPORTED_CHAINS:
        return ReserveResult(False, f'unsupported chain pair {from_chain}->{to_chain} (chain ids are lowercase)')

    client = validator.solana_client
    miner_pk = resolve_miner_pubkey(validator, miner_hotkey)
    if miner_pk is None:
        return ReserveResult(False, 'miner hotkey is not bound to a Solana miner')

    miner_state = client.get_miner_state(miner_pk)
    if miner_state is None or not miner_state.active:
        return ReserveResult(False, 'miner is not active')

    now = int(time.time())
    cfg = client.get_config()
    bounds = bounds_from_config(cfg)
    quote = None
    # Contest slots are per (miner, hub) since v3.1 — a joinable pool may exist on any backing.
    pool = None
    joining = False
    backing = NUMERAIRE_CHAIN
    for candidate_backing in BACKING_BITS:
        p = client.get_pool(miner_pk, candidate_backing)
        if (
            p is not None
            and int(getattr(p, 'opened_at', 0) or 0) != 0
            and now <= int(getattr(p, 'closes_at', 0) or 0)
            and p.from_chain == from_chain
            and p.to_chain == to_chain
        ):
            pool, joining, backing = p, True, candidate_backing
            break
    if joining:
        rate_fixed = pool.rate  # pinned at open — joiners must quote against it
        quote = client.get_quote(miner_pk, from_chain, to_chain, backing)
    else:
        offer, why = _best_offer(client, miner_pk, miner_state, from_chain, to_chain, from_amount, bounds)
        if offer is None:
            return ReserveResult(False, why)
        quote, backing = offer
        rate_fixed = quote.rate
        # One swap per HUB: only the chosen offer's hub has to be idle — the other hub's swap
        # neither draws on this pot nor blocks it.
        if int(getattr(miner_state, 'active_swap_backings', 0)) & BACKING_BITS.get(backing, 0):
            return ReserveResult(False, 'miner is busy with another swap on that hub; try again shortly')

    try:
        amts = compute_intake_amounts(from_chain, to_chain, from_amount, rate_display_from_fixed(rate_fixed), backing)
    except ValueError as e:
        return ReserveResult(False, str(e))
    if amts.to_amount <= 0:
        return ReserveResult(False, 'non-positive dest amount for that source amount')

    purse = backing_purse(client, miner_pk, miner_state, backing)
    if purse is None:
        return ReserveResult(False, f'no locked {backing} bond backs that offer')
    min_swap, max_swap = bounds.get(backing, (0, 0))
    ok, reason = swap_viable(amts.collateral_amount, purse, min_swap, max_swap, backing)
    if not ok:
        return ReserveResult(False, reason)

    # Deliverability gates — BEFORE any funds move: a dest that can't take delivery (malformed
    # address, or one that provably refuses transfers) must bounce here, not strand a paid swap
    # later. Format first: it's offline and a malformed address can never be delivered to.
    providers = getattr(validator, 'axon_assets', {})
    provider = providers.get(to_chain)
    if provider is not None:
        if not provider.chain.is_valid_address(user_to_addr):
            return ReserveResult(False, f'destination is not a valid {to_chain} address')
        if not provider.can_deliver_to(user_to_addr, amts.to_amount):
            return ReserveResult(False, 'destination address rejects incoming transfers')
    # Same gate, source side (T18): the miner's receive address must accept the user's funds —
    # a miner quoting a malformed/rejecting/blacklisted address griefs takers into a burned fee.
    src_provider = providers.get(from_chain)
    if src_provider is not None:
        miner_quote = quote or client.get_quote(miner_pk, from_chain, to_chain, backing)
        miner_from_addr = getattr(miner_quote, 'miner_from_addr', '') if miner_quote else ''
        if miner_from_addr and (
            not src_provider.chain.is_valid_address(miner_from_addr)
            or not src_provider.can_deliver_to(miner_from_addr, from_amount)
        ):
            return ReserveResult(False, 'miner receive address cannot accept the source funds')

    try:
        user_pk = Pubkey.from_string(user_pubkey)
    except Exception:
        return ReserveResult(False, 'invalid user Solana pubkey')

    # Two-phase: this places a BID only (the pair). The taker + amounts computed above are a
    # pre-flight viability check; naming them on-chain is the winner's `finalize_reservation` step.
    try:
        sig = client.open_or_request(miner_pk, from_chain, to_chain, backing)
    except Exception as e:
        reason = contract_reject_reason(e)
        if reason is None:
            raise
        return ReserveResult(False, reason)
    # The entry landed — queue the user's details for `finalize_won_seats`. Persisted (not held in
    # memory) so a validator restart inside the pool/finalize window still honors the routing promise.
    validator.state_store.upsert_routed_request(
        str(miner_pk), from_chain, to_chain, backing, str(user_pk), user_from_addr, user_to_addr, from_amount, now
    )
    pool = client.get_pool(miner_pk, backing)
    closes_at = int(getattr(pool, 'closes_at', 0) or 0) if pool else 0
    return ReserveResult(True, '', closes_at, sig)


# Staleness backstop for queued routed requests: pool window + finalize window + generous slack.
# A queue whose reservation never materializes (miner never drawn, RPC blind spot) dies here.
ROUTED_REQUEST_TTL_SECS = 900


def draw_pool_winner(requests: list) -> dict:
    """Select which queued user gets a won seat. FIFO for now — a deliberate stub:
    selection policy (user stake weighting, priority fees, batching) evolves HERE
    without touching the sweep or the persistence around it."""
    return requests[0]


def finalize_won_seats(validator, now: int) -> list:
    """The routed-reservation sweep: for every miner-direction with queued requests, check the
    reservation and act once its outcome is known. Won a drawn seat → finalize it on-chain for
    ``draw_pool_winner``'s pick (amounts recomputed from the PINNED rate, mirroring the CLI's
    native Phase 3) and drop the queue — non-selected users' clients see another user pinned and
    re-request. Lost / filled by another router / finalize window lapsed → drop the queue. A
    transient RPC fault keeps the queue for the next step's retry (inside the finalize window).
    Returns the miners finalized this pass."""
    store = validator.state_store
    client = validator.solana_client
    read_only = validator.solana_swap_loop.read_only
    me = str(client.keypair.pubkey())
    finalized: list = []
    for miner, from_chain, to_chain, backing in store.distinct_routed_pools():
        queue = store.pending_routed_requests(miner, from_chain, to_chain, backing)
        if not queue:
            continue
        entered_at = queue[0]['created_at']
        try:
            resv = client.get_reservation(Pubkey.from_string(miner), backing)
        except Exception as e:
            bt.logging.warning(f'routed sweep {miner[:8]}: reservation read failed, retrying next step: {e}')
            continue
        # The Reservation PDA is reused across rounds: state older than our oldest queued request
        # is residue from a PREVIOUS round — our pool simply hasn't been drawn yet, so wait (the
        # TTL backstop clears a queue whose draw never comes). Treating residue as terminal
        # deleted every queue on the first sweep step and permanently dead-ended routed mode for
        # any miner-direction carrying an old lapsed seat.
        fresh = resv is not None and max(int(resv.reserved_until), int(resv.finalize_by)) >= entered_at
        drawn_unfilled = fresh and int(resv.reserved_until) == 0 and int(resv.finalize_by) != 0
        won = (
            drawn_unfilled
            and now <= int(resv.finalize_by)
            and str(resv.router) == me
            and resv.from_chain == from_chain
            and resv.to_chain == to_chain
        )
        if not won:
            # No seat we can fill: not drawn yet (open pool / residue) → wait; a fresh terminal
            # outcome (lost to another router, filled, lapsed) → drop.
            if drawn_unfilled and now <= int(resv.finalize_by):
                bt.logging.info(f'routed sweep {miner[:8]}: seat won by another router, dropping queue')
                store.delete_routed_requests(miner, from_chain, to_chain, backing)
            elif fresh:
                bt.logging.info(f'routed sweep {miner[:8]}: reservation filled or window lapsed, dropping queue')
                store.delete_routed_requests(miner, from_chain, to_chain, backing)
            continue
        req = draw_pool_winner(queue)
        if read_only:
            bt.logging.info(f'routed sweep {miner[:8]}: WOULD finalize for {req["user_pubkey"][:8]} (read-only)')
            continue
        try:
            # The reservation lives at the queue's backing-seeded address, so the stored chain can
            # only agree; the fill is sized against THAT leg or the purse gate reads the wrong side.
            fill = compute_intake_amounts(
                from_chain, to_chain, req['from_amount'], rate_display_from_fixed(resv.rate), backing
            )
            client.finalize_reservation(
                Pubkey.from_string(miner),
                Pubkey.from_string(req['user_pubkey']),
                req['user_from_addr'],
                req['user_to_addr'],
                fill.collateral_amount,
                fill.from_amount,
                fill.to_amount,
                backing,
            )
        except Exception as e:
            reason = contract_reject_reason(e) or (str(e) if isinstance(e, ValueError) else None)
            if reason is None:
                bt.logging.warning(f'routed sweep {miner[:8]}: finalize transport fault, retrying next step: {e}')
                continue
            bt.logging.warning(f'routed sweep {miner[:8]}: finalize rejected ({reason}), dropping queue')
            store.delete_routed_requests(miner, from_chain, to_chain, backing)
            continue
        bt.logging.info(f'routed sweep {miner[:8]}: finalized seat for {req["user_pubkey"][:8]} (FIFO of queue)')
        store.delete_routed_requests(miner, from_chain, to_chain, backing)
        finalized.append(miner)
    store.prune_routed_requests(now - ROUTED_REQUEST_TTL_SECS)
    return finalized


@dataclass
class ConfirmResult:
    ok: bool
    reason: str = ''
    swap_key: str = ''
    sig: str = ''


# Runway submit_swap_claim needs to land after a deposit verifies. Only the claim tx remains at this
# point — the send and the relay are already done — so this is deliberately much shorter than the
# CLI's pre-send margin. Once the Swap exists the crank owns every later extension.
CLAIM_RELAY_MARGIN_SECS = 90


def _live_unclaimed_reservation(client, miner_pk, now):
    """The miner's live unclaimed reservation and its hub, scanned across every per-hub slot: v3.1
    seeds one per (miner, backing), so a TAO deposit lives in the TAO slot — never assume SOL. Live
    == reserved_until >= now, no swap yet claimed. (resv, backing, '') on a hit; (None, None, reason)."""
    saw_any = False
    saw_live_claimed = False
    for backing in BACKING_BITS:
        resv = client.get_reservation(miner_pk, backing)
        if resv is None:
            continue
        saw_any = True
        if int(resv.reserved_until) == 0 or int(resv.reserved_until) < now:
            continue
        if bytes(resv.claimed_swap_key) != EMPTY_SWAP_KEY:
            saw_live_claimed = True
            continue
        return resv, backing, ''
    if saw_live_claimed:
        return None, None, 'Reservation already has a claimed swap'
    if saw_any:
        return None, None, 'Reservation is not active'
    return None, None, 'No reservation for this miner'


def _freshest_reservation(client, miner_pk):
    """The miner's most-alive reservation across per-hub slots (v3.1) — ranked by max(reserved_until,
    finalize_by) — so a tao-hub seat is seen by status, not just the SOL slot."""
    best, best_at = None, -1
    for backing in BACKING_BITS:
        resv = client.get_reservation(miner_pk, backing)
        if resv is None:
            continue
        at = max(int(getattr(resv, 'reserved_until', 0) or 0), int(getattr(resv, 'finalize_by', 0) or 0))
        if at > best_at:
            best, best_at = resv, at
    return best


def _extend_for_claim(client, miner_pk, reservation, backing) -> None:
    """Slide `reserved_until` forward so the pending claim can land, when a verified deposit arrives
    with little runway left.

    Evidence-gated on purpose: the caller has already matched this deposit against the pinned
    reservation, so we only ever extend for a taker who demonstrably sent funds — never on request.
    That is the same justification the crank extends under, one step earlier in the lifecycle: the
    crank can only help once a Swap exists, and here there is no Swap yet precisely because the claim
    has not landed.

    Best-effort. A failed extension must not sink the claim — the reservation may still have just
    enough runway, and a claim that lands is worth more than a clean error path."""
    # Re-read the clock rather than take the caller's: verify_transaction is a source-chain RPC that
    # can burn seconds, and a stale `now` both overstates the runway and undershoots the target.
    now = int(time.time())
    reserved_until = int(getattr(reservation, 'reserved_until', 0) or 0)
    ceiling = int(getattr(reservation, 'max_extend_at', 0) or 0)
    if reserved_until - now >= CLAIM_RELAY_MARGIN_SECS:
        return  # plenty of runway
    target = min(now + CLAIM_RELAY_MARGIN_SECS, ceiling)
    if target <= reserved_until:
        return  # already at the contract ceiling — nothing left to buy
    try:
        client.extend_reservation(miner_pk, target, backing)
        bt.logging.info(f'claim runway: extended reserved_until {reserved_until} -> {target} (+{target - now}s)')
    except Exception as e:  # noqa: BLE001 - never block the claim on a failed extension
        bt.logging.warning(f'claim runway: extend_reservation failed ({e}); attempting the claim anyway')


def confirm_deposit(validator, miner_hotkey: str, from_tx_hash: str, from_tx_block: int = 0) -> ConfirmResult:
    """Relay a user's source deposit into a claim: verify the tx against the pinned reservation, then
    submit_swap_claim (creating the Swap in PendingAttestation). Accepts a content-valid deposit even before
    it fully confirms — the crank defers voting until confirmations accrue. Fast-fails (no claim, so the short
    TTL frees the miner) only when the tx is absent or its content doesn't match the reservation."""
    from allways.validator.solana_swap_loop import is_tx_fresh

    # Reject empty/whitespace-only hashes and strip surrounding whitespace before use (#167).
    from_tx_hash = from_tx_hash.strip() if from_tx_hash else from_tx_hash
    if not from_tx_hash:
        return ConfirmResult(False, 'Missing source tx hash')
    client = validator.solana_client
    miner_pk = resolve_miner_pubkey(validator, miner_hotkey)
    if miner_pk is None:
        return ConfirmResult(False, 'Hotkey not bound to a Solana miner')

    now = int(time.time())
    reservation, backing, reason = _live_unclaimed_reservation(client, miner_pk, now)
    if reservation is None:
        return ConfirmResult(False, reason)

    provider = validator.axon_assets.get(reservation.from_chain)
    if provider is None:
        return ConfirmResult(False, f'Unsupported source chain: {reservation.from_chain}')

    try:
        tx_info = provider.verify_transaction(
            tx_hash=from_tx_hash,
            expected_recipient=reservation.miner_from_addr,
            expected_amount=int(reservation.from_amount),
            block_hint=from_tx_block,
            expected_sender=reservation.from_addr,
        )
    except ProviderUnreachableError:
        return ConfirmResult(False, 'Source-chain provider unreachable; resend shortly')
    if tx_info is None:
        # None = absent or content-mismatch; fast-fail (no claim) so the short TTL frees the miner.
        return ConfirmResult(False, 'Source tx not visible or does not match the reservation')

    # Deferred intake: accept a content-valid deposit pre-confirmation — the crank defers voting until it
    # confirms (source 'pending'->extend, 'ok'+fresh->attest). A 0-conf mempool tx has no block_time, so its
    # freshness is deferred too; only a mined tx is freshness-checked here (fast-fail a stale mined deposit).
    if tx_info.block_time is not None:
        grace = getattr(provider.chain_def, 'replay_grace_secs', 0)
        if not is_tx_fresh(tx_info, int(reservation.created_at), grace):
            return ConfirmResult(False, 'Source tx fails freshness — stale/replayed deposit')

    # The taker's funds are already on the source chain and this deposit just verified against the
    # pinned reservation — but submit_swap_claim needs reserved_until >= now, and once it lapses there
    # is no claim, no Swap, no timeout and no refund: the deposit is simply lost. Buy runway first.
    _extend_for_claim(client, miner_pk, reservation, backing)

    swap_key = swap_key_from_tx_hash(from_tx_hash)
    sig = client.submit_swap_claim(miner_pk, swap_key, from_tx_hash, tx_info.block_number or 0, backing)
    return ConfirmResult(True, '', swap_key.hex(), sig)


def scan_deposit(validator, miner_hotkey: str) -> Optional[str]:
    """Tx hash of a user deposit matching the miner's live unclaimed reservation. Every launch
    chain scans: BTC by esplora address index, SOL by signature index, TAO by an incremental
    head-follow block scan (Substrate has no address index). A hash-finder only —
    ``confirm_deposit`` remains the sole verifier, so a loose match can never mis-claim."""
    client = validator.solana_client
    miner_pk = resolve_miner_pubkey(validator, miner_hotkey)
    if miner_pk is None:
        return None
    reservation, _backing, _ = _live_unclaimed_reservation(client, miner_pk, time.time())
    if reservation is None:
        return None
    provider = validator.axon_assets.get(reservation.from_chain)
    scan = getattr(provider, 'find_recent_outgoing', None)
    if scan is None:
        return None
    return scan(reservation.from_addr, reservation.miner_from_addr, int(reservation.from_amount))


@dataclass
class BestQuote:
    miner_hotkey: str
    miner: str  # Solana pubkey (base58)
    rate_display: str
    collateral_amount: int
    from_amount: int
    to_amount: int
    # The backing the winning quote declared — it is the offer's failure guarantee, so a consumer
    # that shows a rate has to be able to show which promise comes with it.
    backing: str = NUMERAIRE_CHAIN


# Depth rungs served alongside /rate — enough to show the market, few enough to stay a glance.
RATE_LEVELS_LIMIT = 5


@dataclass
class RateQuote:
    quote: Optional[BestQuote]  # best executable quote for the asked size, None if nothing fits
    reason: str  # why quote is None ('' on a hit)
    levels: list  # top rate rungs [{rate_display, max_from_amount}], best first
    max_from_amount: int  # largest executable source amount across ALL quotes, not just shown rungs


def rate_quote(validator, from_chain: str, to_chain: str, from_amount: int) -> RateQuote:
    """Everything ``/rate`` serves, from ONE candidate scan: the best executable quote for
    ``from_amount`` (source smallest-units; mirrors ``select_best_miner`` so the displayed rate ==
    the reservable rate) plus the depth behind it. Rung order matches the selector's ranking (most
    dest per source); same-rate quotes collapse to the deepest. Capacities are per-rung maxima,
    never cumulative — a swap fills against a single miner."""
    client = validator.solana_client
    cfg = client.get_config()
    bounds = bounds_from_config(cfg)
    min_swap, max_swap = hub_bounds(bounds, from_chain, to_chain)
    cands = candidate_miners(client, from_chain, to_chain)
    best = select_best_miner(cands, from_chain, to_chain, from_amount, min_swap, max_swap, bounds)
    bq = _best_quote_result(validator, best) if best else None
    reason = '' if bq else unviable_reason(cands, from_chain, to_chain, from_amount, min_swap, max_swap, bounds)
    depth: dict = {}
    for cand in cands:
        cap = max_intake_from_amount(cand, from_chain, to_chain, min_swap, max_swap, bounds)
        if cap > 0:
            depth[cand.rate_display] = max(depth.get(cand.rate_display, 0), cap)
    # Rates are canonical 'dest per 1 canonical-source': when the taker sends the canonical
    # source (the hub leg), higher is better; sending the spoke side, lower is better.
    from_is_canon = from_chain == canonical_pair(from_chain, to_chain)[0]
    best_first = sorted(depth.items(), key=lambda kv: float(kv[0]), reverse=from_is_canon)
    levels = [{'rate_display': r, 'max_from_amount': m} for r, m in best_first[:RATE_LEVELS_LIMIT]]
    return RateQuote(bq, reason, levels, max(depth.values(), default=0))


def _best_quote_result(validator, best) -> Optional[BestQuote]:
    cand, amts = best
    hotkey = _miner_hotkey_for(validator, cand.miner)
    if hotkey is None:
        return None
    return BestQuote(
        hotkey,
        str(cand.miner),
        cand.rate_display,
        amts.collateral_amount,
        amts.from_amount,
        amts.to_amount,
        cand.backing,
    )


def _miner_hotkey_for(validator, miner_pk) -> Optional[str]:
    """Reverse a miner's Solana pubkey → its bound Bittensor hotkey (ss58) via the Binding PDA."""
    binding = validator.solana_client.get_binding(miner_pk)
    if binding is None:
        return None
    return hotkey_ss58(bytes(binding.hotkey))


@dataclass
class SwapStatus:
    """Seam ``/status`` payload. ``stage`` is the offering-facing lifecycle enum:

    none → reserved → claimed → active → fulfilled → { completed | timed_out }
    (a claim reaped stale before attestation ends at the terminal ``expired`` instead)

    ``completed``, ``timed_out``, and ``expired`` are terminal; ``timed_out`` means the miner was
    slashed, ``expired`` means the claim went stale pre-attestation (no funds moved, the Swap PDA
    was closed by ``close_stale_claim``). They are sourced from the live PDA status or, after the terminal PDA closes on-chain, from
    the validator's ``swap_outcomes`` event index. A closed PDA whose outcome isn't recorded
    yet reports ``fulfilled`` — transient, normally resolving within ~one forward step once the
    terminal event is ingested. Consumers keep polling on ``fulfilled`` and should apply their
    own reconcile deadline: in the wiped-state.db + RPC-pruned edge the outcome never lands, and
    the validator won't guess terminal truth it hasn't ingested (see ``_swap_stage``).

    Resolution: the consumer passes the ``swap_key`` it persisted at claim time to resolve the
    swap directly — required for post-attestation stages, because ``vote_initiate`` consumes
    the reservation at attestation quorum, so the reservation stops referencing the swap the
    moment it goes ``active``. Without ``swap_key``, resolution walks the miner's reservation
    and only the pre-attestation stages (``none``/``reserved``/``claimed``) are reliably visible."""

    stage: str  # none | reserved | claimed | active | fulfilled | completed | timed_out | expired
    reserved_until: int = 0
    user: str = ''
    swap_key: str = ''
    detail: dict = field(default_factory=dict)


def swap_status(validator, miner_hotkey: str, swap_key_hex: str = '') -> SwapStatus:
    """Current lifecycle stage for a reservation/swap — the offering polls this.

    With ``swap_key_hex`` the swap resolves by key (survives the reservation being consumed at
    attestation quorum); without it, via the miner's live reservation (pre-attestation stages)."""
    if swap_key_hex:
        return _swap_status_by_key(validator, swap_key_hex)
    client = validator.solana_client
    miner_pk = resolve_miner_pubkey(validator, miner_hotkey)
    if miner_pk is None:
        return SwapStatus('none')
    reservation = _freshest_reservation(client, miner_pk)
    if reservation is None or reservation.reserved_until == 0:
        return SwapStatus('none')
    swap_key = bytes(reservation.claimed_swap_key)
    # An expired UNCLAIMED reservation is dead — the pool can be re-entered over it. Reporting it as
    # 'reserved' with its stale user makes the offering's win-detection read "another validator's user
    # holds this miner" and mark won draws lost. A claimed one still speaks through its swap's stage.
    if swap_key == EMPTY_SWAP_KEY and int(reservation.reserved_until) < time.time():
        return SwapStatus('none')
    # detail carries what the offering needs to instruct the user (where + how much to send).
    detail = {
        'from_chain': reservation.from_chain,
        'to_chain': reservation.to_chain,
        'from_amount': int(reservation.from_amount),
        'to_amount': int(reservation.to_amount),
        'miner_from_addr': reservation.miner_from_addr,
    }
    if swap_key == EMPTY_SWAP_KEY:
        return SwapStatus('reserved', reservation.reserved_until, str(reservation.user), detail=detail)
    swap = client.get_swap(swap_key)
    if swap is not None:
        detail['from_tx_hash'] = swap.from_tx_hash
        detail['to_tx_hash'] = swap.to_tx_hash
    stage = _swap_stage(validator, swap, swap_key)
    return SwapStatus(stage, reservation.reserved_until, str(reservation.user), swap_key.hex(), detail)


def _swap_status_by_key(validator, swap_key_hex: str) -> SwapStatus:
    """Resolve directly by swap_key: a live PDA's status maps as usual; a closed PDA goes through
    the ``swap_outcomes`` disambiguation. ``reserved_until`` is 0 here — the reservation is
    already consumed (or irrelevant) once the consumer polls by key."""
    swap_key = bytes.fromhex(swap_key_hex)  # bad hex raises ValueError → seam answers 400
    if len(swap_key) != 32:
        raise ValueError('swap_key must be 32 bytes hex')
    swap = validator.solana_client.get_swap(swap_key)
    stage = _swap_stage(validator, swap, swap_key)
    if swap is None:
        # Closed PDA: the delivery hash survives in the fulfillment index — receipts read it
        # from here, since the consumer may only poll again after the account is gone.
        detail = {}
        to_tx_hash = validator.state_store.get_swap_fulfillment(swap_key_hex)
        if to_tx_hash:
            detail['to_tx_hash'] = to_tx_hash
        return SwapStatus(stage, swap_key=swap_key_hex, detail=detail)
    # Same detail shape as the reservation path — the Swap PDA carries the full legs.
    detail = {
        'from_chain': swap.from_chain,
        'to_chain': swap.to_chain,
        'from_amount': int(swap.from_amount),
        'to_amount': int(swap.to_amount),
        'miner_from_addr': swap.miner_from_addr,
        'from_tx_hash': swap.from_tx_hash,
        'to_tx_hash': swap.to_tx_hash,
    }
    return SwapStatus(stage, 0, str(swap.user), swap_key_hex, detail)


# On-chain Swap.status is a borsh enum object; map by its variant name (not int()).
_STAGE_BY_NAME = {
    'PendingAttestation': 'claimed',
    'Active': 'active',
    'Fulfilled': 'fulfilled',
    'Completed': 'completed',
    'TimedOut': 'timed_out',
}


def _swap_stage(validator, swap, swap_key: bytes) -> str:
    """Stage for a claimed swap. A closed PDA is terminal, but Completed and TimedOut swaps both
    close on-chain, so the on-chain account alone can't tell a completion from a slash — the
    validator's own event index (``swap_outcomes``, written on SwapCompleted/SwapTimedOut/
    StaleClaimClosed ingest) disambiguates. On an outcome miss, fall back NON-terminal to ``fulfilled``: the miss is
    normally ingest lag (another validator's quorum closed the PDA since our last forward-step
    ingest) and self-corrects at the next ingest, whereas a terminal guess would stop the
    consumer polling on a wrong answer — for a slash, exactly the bug this index exists to fix."""
    if swap is None:
        outcome = validator.state_store.get_swap_outcome(swap_key.hex())
        return outcome or 'fulfilled'
    return _STAGE_BY_NAME.get(type(getattr(swap, 'status', None)).__name__, 'claimed')
