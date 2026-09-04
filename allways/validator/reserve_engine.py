"""Transport-agnostic kernel ops for the on-behalf reservation flow.

One source of truth for reserve / confirm / rate / status, shared by the axon synapse handlers
(CLI transport) and the localhost HTTP seam (offering transport). Every op validates protocol
invariants before it signs — the caller (offering or CLI) is never trusted.
"""

import threading
import time
from dataclasses import dataclass, field
from itertools import islice
from typing import Optional

import bittensor as bt
from bittensor import Keypair
from solders.pubkey import Pubkey

from allways.assets.asset import ProviderUnreachableError
from allways.chains import SUPPORTED_CHAINS, canonical_pair, get_chain_def
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
    viable_intakes,
)
from allways.constants import NUMERAIRE_CHAIN, hub_leg
from allways.solana.client import contract_reject_reason, swap_key_from_tx_hash
from allways.solana.pdas import BACKING_BITS
from allways.utils.rate import max_from_for_to_cap
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

    # Canonical source form at intake: the finalize hash + source-lock PDA are byte-keyed on this
    # string (V-C2), so a case variant of a live source would mint a second lock over one deposit.
    src_asset = (getattr(validator, 'axon_assets', None) or {}).get(from_chain)
    if src_asset is not None:
        user_from_addr = src_asset.chain.normalize_address(user_from_addr)

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
    miner_quote = quote or client.get_quote(miner_pk, from_chain, to_chain, backing)
    verified = getattr(validator, 'assets', None)
    if verified is not None:
        missing = next(
            (chain for chain in (from_chain, to_chain) if chain not in verified or chain not in providers), None
        )
        if missing:
            return ReserveResult(False, f'this validator cannot verify {missing} right now')
    if provider is not None:
        # Validity only: NOT a deliverability prediction. Reserve-time deliverability isn't a security
        # boundary (a dest can pass here then revert later via 7702/conditional code); the sound check is
        # the delivery-time reverted-tx proof (cancel_swap). Fat-finger UX belongs in the client app.
        if not provider.chain.is_valid_address(user_to_addr):
            return ReserveResult(False, f'destination is not a valid {to_chain} address')
        # Reject user_to_addr == the miner's committed delivery address. The miner must deliver FROM
        # miner_to_addr, so this makes every delivery a from==to self-transfer that the anti-wash
        # verifier rejects → the leg never confirms → the miner is force-slashed with no cancel escape.
        miner_to_addr = getattr(miner_quote, 'miner_to_addr', '') if miner_quote else ''
        if miner_to_addr and provider.chain.normalize_address(user_to_addr) == provider.chain.normalize_address(
            miner_to_addr
        ):
            return ReserveResult(False, 'destination must differ from the miner delivery address')
    # Same gate, source side (T18): the miner's receive address must accept the user's funds —
    # a miner quoting a malformed/rejecting/blacklisted address griefs takers into a burned fee.
    src_provider = providers.get(from_chain)
    if src_provider is not None:
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
    if closes_at:
        schedule_crank(validator, closes_at, miner_pk)
    return ReserveResult(True, '', closes_at, sig)


CRANK_SKEW_SECS = 2  # the chain clock can trail wall time; fire a touch after the window shuts
SLOT_SECS = 0.4  # nominal slot time; the draw waits for the armed seed slot to be produced
DRAW_SKEW_SECS = 0.5
DRAW_RETRY_SECS = 1  # one extra crank if the seed slot hadn't been produced by the nominal time
# Staleness backstop for queued routed requests: pool window + finalize window + generous slack.
# A queue whose reservation never materializes (miner never drawn, RPC blind spot) dies here.
ROUTED_REQUEST_TTL_SECS = 900


def crank(validator, now: int) -> tuple:
    """Resolve closed pools, then finalize seats we won. Serialized with the forward step."""
    with validator.crank_lock:
        return validator.solana_swap_loop.resolve_pools_once(now), finalize_won_seats(validator, now)


def _timer(delay: float, fn, *args) -> threading.Timer:
    timer = threading.Timer(max(0.0, delay), fn, args)
    timer.daemon = True
    timer.start()
    return timer


class CrankScheduler:
    """Cranks each routed pool we reserved on exactly when its next stage is due, off the program's pushed
    events: close → arm the draw; `PoolDrawArmed` → draw once its seed slot is produced; `PoolResolved` →
    finalize at once, whoever resolved it. Missed events (socket down) are covered by the forward step's
    crank, which runs the same `crank()` every step."""

    def __init__(self, validator, feed) -> None:
        self.validator = validator
        self._lock = threading.Lock()
        self._pools: dict = {}  # miner → tracking deadline (unix)
        feed.on('PoolDrawArmed', self._on_armed)
        feed.on('PoolResolved', self._on_resolved)

    def schedule(self, miner, closes_at: int) -> threading.Timer:
        with self._lock:
            self._pools[str(miner)] = int(time.time()) + ROUTED_REQUEST_TTL_SECS
        return _timer(closes_at - time.time() + CRANK_SKEW_SECS, self._crank, 'arm')

    def _tracked(self, miner) -> bool:
        with self._lock:
            deadline = self._pools.get(str(miner))
            if deadline is not None and deadline < time.time():
                del self._pools[str(miner)]
                return False
            return deadline is not None

    # Feed handlers run on the feed thread: record, then hand the RPC work to a timer thread.
    def _on_armed(self, _name: str, ev) -> None:
        if self._tracked(ev.miner):
            _timer(0, self._schedule_draw, int(ev.seed_slot))

    def _on_resolved(self, _name: str, ev) -> None:
        if self._tracked(ev.miner):
            with self._lock:
                self._pools.pop(str(ev.miner), None)
            _timer(0, self._crank, 'finalize')

    def _schedule_draw(self, seed_slot: int) -> None:
        try:
            behind = seed_slot - self.validator.solana_client.rpc.get_slot()
        except Exception as e:
            bt.logging.warning(f'crank: slot read failed ({e}); drawing now')
            behind = 0
        _timer(behind * SLOT_SECS + DRAW_SKEW_SECS, self._crank, 'draw')
        _timer(behind * SLOT_SECS + DRAW_SKEW_SECS + DRAW_RETRY_SECS, self._crank, 'draw-retry')

    def _crank(self, stage: str) -> None:
        try:
            resolved, finalized = crank(self.validator, int(time.time()))
            bt.logging.info(f'crank[{stage}]: {len(resolved)} pool(s) resolved, {len(finalized)} seat(s) finalized')
        except Exception as e:
            bt.logging.warning(f'crank[{stage}] failed: {e}')


def schedule_crank(validator, closes_at: int, miner) -> threading.Timer:
    """Crank this pool at each stage as the chain reaches it (see CrankScheduler)."""
    return validator.crank_scheduler.schedule(miner, closes_at)


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
        # A source may back only ONE live-unclaimed reservation per miner: two reservations sharing
        # (from_chain, from_addr) are both matchable by a single `>=`-amount deposit, so an attacker
        # declaring a victim's from_addr on the miner's OTHER hub could siphon that deposit. Draw only from
        # queued users whose source doesn't already back a live seat — skip a colliding entry, don't drop
        # the whole queue (else one crafted front-of-queue request knocks out every honest queued user).
        providers = getattr(validator, 'axon_assets', {})
        src = providers.get(from_chain)
        try:
            live_slots, _ = _live_unclaimed_slots(client, Pubkey.from_string(miner), now)
        except Exception as e:
            bt.logging.warning(f'routed sweep {miner[:8]}: live-slot read failed, retrying next step: {e}')
            continue

        def _collides(addr: str) -> bool:
            want = src.chain.normalize_address(addr) if src else addr
            return any(
                s.from_chain == from_chain
                and (src.chain.normalize_address(s.from_addr) if src else s.from_addr) == want
                for s, _b in live_slots
            )

        eligible = [q for q in queue if not _collides(q['user_from_addr'])]
        if not eligible:
            bt.logging.warning(
                f'routed sweep {miner[:8]}: every queued source already backs a live seat — nothing to '
                f'finalize this pass (TTL prunes stale colliders)'
            )
            continue
        req = draw_pool_winner(eligible)
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
                # Normalized again at commit: rows queued before the intake normalization shipped
                # (or by an older validator) must still land canonical, or the lock PDA diverges.
                src.chain.normalize_address(req['user_from_addr']) if src else req['user_from_addr'],
                req['user_to_addr'],
                fill.collateral_amount,
                fill.from_amount,
                fill.to_amount,
                backing,
                from_chain=from_chain,
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


def _live_unclaimed_slots(client, miner_pk, now):
    """Every live-unclaimed per-hub reservation slot as ``[(resv, backing), ...]`` in hub order, plus a
    reason that explains an EMPTY list (``''`` when there were candidates). v3.1 seeds one reservation
    per (miner, backing) and a miner may hold several live at once (simultaneous swaps), so the
    deposit's hub is not knowable up front: the caller verifies the deposit against each candidate and
    claims the one it matches. Returning only the first live slot would reject a TAO deposit whenever a
    SOL slot is also live-unclaimed (V-1). Live == reserved_until >= now, no swap yet claimed."""
    slots = []
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
        slots.append((resv, backing))
    if slots:
        return slots, ''
    if saw_live_claimed:
        return slots, 'Reservation already has a claimed swap'
    if saw_any:
        return slots, 'Reservation is not active'
    return slots, 'No reservation for this miner'


def _freshest_reservation(client, miner_pk, from_chain: str = '', to_chain: str = ''):
    """The miner's most-alive reservation across per-hub slots (v3.1) — ranked by max(reserved_until,
    finalize_by) — so a tao-hub seat is seen by status, not just the SOL slot. With a pair, only
    the slot carrying that pair counts: a consumer tracking its own seat must not be answered with
    the miner's OTHER hub (a fresher stranger there read as "our seat was lost", and a same-pubkey
    stranger there was adopted as ours)."""
    best, best_at = None, -1
    for backing in BACKING_BITS:
        resv = client.get_reservation(miner_pk, backing)
        if resv is None:
            continue
        if from_chain and (resv.from_chain != from_chain or resv.to_chain != to_chain):
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
        client.extend_reservation(
            miner_pk, target, backing, from_chain=reservation.from_chain, from_addr=reservation.from_addr
        )
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
    slots, reason = _live_unclaimed_slots(client, miner_pk, now)
    if not slots:
        return ConfirmResult(False, reason)

    # A miner may hold several live-unclaimed reservations at once (v3.1 simultaneous swaps), so the
    # deposit's hub is not knowable up front. Verify the deposit against each slot and claim the one it
    # matches — picking the first live slot blindly would reject a TAO deposit whenever a SOL slot is
    # also live (V-1). `verify_transaction` is the authoritative matcher (pinned recipient/amount/sender).
    matches = []
    unreachable = stale = False
    for resv, resv_backing in slots:
        provider = validator.axon_assets.get(resv.from_chain)
        if provider is None:
            continue
        try:
            candidate = provider.verify_transaction(
                tx_hash=from_tx_hash,
                expected_recipient=resv.miner_from_addr,
                expected_amount=int(resv.from_amount),
                block_hint=from_tx_block,
                expected_sender=resv.from_addr,
            )
        except ProviderUnreachableError:
            unreachable = True  # this hub can't be judged now; another might still match
            continue
        if candidate is None:
            continue  # absent or content-mismatch for this hub — try the next live slot
        # Deferred intake: accept a content-valid deposit pre-confirmation — the crank defers voting
        # until it confirms. A 0-conf mempool tx has no block_time, so its freshness is deferred too;
        # only a mined tx is freshness-checked here. A matched-but-stale deposit is terminal for its
        # hub (its params are pinned), so it never claims — but a fresh match on another hub still can.
        if candidate.block_time is not None:
            grace = getattr(provider.chain_def, 'replay_grace_secs', 0)
            if not is_tx_fresh(candidate, int(resv.created_at), grace):
                stale = True
                continue
        # Deterministic pick when one deposit matches several slots (normalize-equal senders): a
        # canonical-form from_addr wins — honest lanes commit canonical, while a case variant is the
        # source-lock dodge (V-C2) — then the oldest reservation (the one the variant was copied from).
        canonical = resv.from_addr == provider.chain.normalize_address(resv.from_addr)
        matches.append(((0 if canonical else 1, int(resv.created_at)), resv, resv_backing, candidate))

    if not matches:
        if stale:
            return ConfirmResult(False, 'Source tx fails freshness — stale/replayed deposit')
        if unreachable:
            return ConfirmResult(False, 'Source-chain provider unreachable; resend shortly')
        # No live slot matched; fast-fail (no claim) so the short TTL frees the miner.
        return ConfirmResult(False, 'Source tx not visible or does not match the reservation')
    _, reservation, backing, tx_info = min(matches, key=lambda m: m[0])

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
    slots, _ = _live_unclaimed_slots(client, miner_pk, time.time())
    # Scan each live-unclaimed hub (v3.1 may hold several at once) and return the first hash found;
    # confirm_deposit stays the sole verifier, so a loose per-hub scan can never mis-claim.
    for reservation, _backing in slots:
        provider = validator.axon_assets.get(reservation.from_chain)
        scan = getattr(provider, 'find_recent_outgoing', None)
        if scan is None:
            continue
        found = scan(reservation.from_addr, reservation.miner_from_addr, int(reservation.from_amount))
        if found:
            return found
    return None


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
    min_from_amount: int  # the pair's hub minimum in source units; 0 = unset or unpriceable
    candidates: list  # top bound intakes for the asked size, selector order — a tolerance band in one scan


def rate_quote(validator, from_chain: str, to_chain: str, from_amount: int) -> RateQuote:
    """Everything ``/rate`` serves, from ONE candidate scan: the best executable quote for
    ``from_amount`` (source smallest-units; mirrors ``select_best_miner`` so the displayed rate ==
    the reservable rate), the runners-up for that size, and the depth behind them. Rung order
    matches the selector's ranking (most dest per source); same-rate quotes collapse to the deepest.
    Capacities are per-rung maxima, never cumulative — a swap fills against a single miner."""
    client = validator.solana_client
    cfg = client.get_config()
    bounds = bounds_from_config(cfg)
    min_swap, max_swap = hub_bounds(bounds, from_chain, to_chain)
    cands = candidate_miners(client, from_chain, to_chain)
    ranked = _ranked_intakes(cands, from_chain, to_chain, from_amount, min_swap, max_swap, bounds)
    bound = list(islice(_bound_intakes(validator, ranked), RATE_LEVELS_LIMIT))
    bq = _best_quote(*bound[0]) if bound else None
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
    candidates = [
        {
            'miner_hotkey': hotkey,
            'rate_display': cand.rate_display,
            'to_amount': amts.to_amount,
            'max_from_amount': max_intake_from_amount(cand, from_chain, to_chain, min_swap, max_swap, bounds),
        }
        for cand, amts, hotkey in bound
    ]
    min_from = _min_from_amount(min_swap, best_first[0][0] if best_first else None, from_chain, to_chain)
    return RateQuote(bq, reason, levels, max(depth.values(), default=0), min_from, candidates)


def _ranked_intakes(cands, from_chain: str, to_chain: str, from_amount: int, min_swap: int, max_swap: int, bounds):
    """``viable_intakes`` in ``select_best_miner``'s order (most dest, tie → "sol", then input order)."""
    viable = viable_intakes(cands, from_chain, to_chain, from_amount, min_swap, max_swap, bounds)
    return sorted(viable, key=lambda p: (p[1].to_amount, p[0].backing == NUMERAIRE_CHAIN), reverse=True)


def _bound_intakes(validator, ranked):
    """``ranked`` with each miner's hotkey; an unbound miner cannot be reserved through the seam, so it is skipped."""
    for cand, amts in ranked:
        hotkey = _miner_hotkey_for(validator, cand.miner)
        if hotkey:
            yield cand, amts, hotkey


def _min_from_amount(min_swap: int, best_rate: Optional[str], from_chain: str, to_chain: str) -> int:
    """The hub minimum in source units: as-is on the hub leg, else inverted at the best level rate (0 if no depth)."""
    if min_swap <= 0 or from_chain == hub_leg(from_chain, to_chain):
        return min_swap
    if best_rate is None:
        return 0
    canon_from, canon_to = canonical_pair(from_chain, to_chain)
    # One past the largest source whose hub leg still falls short of the minimum.
    under = max_from_for_to_cap(
        min_swap - 1,
        best_rate,
        from_chain != canon_from,
        get_chain_def(canon_to).decimals,
        get_chain_def(canon_from).decimals,
    )
    return under + 1


def _best_quote(cand: MinerCandidate, amts, hotkey: str) -> BestQuote:
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

    none → reserved → claimed → active → fulfilled → { completed | timed_out | cancelled }
    (a claim reaped stale before attestation ends at the terminal ``expired`` instead)

    ``completed``, ``timed_out``, ``cancelled``, and ``expired`` are terminal; ``timed_out`` means
    the miner was slashed, ``cancelled`` the no-fault cancel verdict (dest provably unpayable — no
    slash, the miner keeps the source), ``expired`` means the claim went stale pre-attestation (no
    funds moved, the Swap PDA was closed by ``close_stale_claim``). They are sourced from the live PDA status or, after the terminal PDA closes on-chain, from
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

    stage: str  # none | reserved | claimed | active | fulfilled | completed | timed_out | cancelled | expired
    reserved_until: int = 0
    user: str = ''
    swap_key: str = ''
    detail: dict = field(default_factory=dict)


def swap_status(
    validator, miner_hotkey: str, swap_key_hex: str = '', from_chain: str = '', to_chain: str = ''
) -> SwapStatus:
    """Current lifecycle stage for a reservation/swap — the offering polls this.

    With ``swap_key_hex`` the swap resolves by key (survives the reservation being consumed at
    attestation quorum); without it, via the miner's live reservation (pre-attestation stages) —
    the slot carrying ``from_chain``/``to_chain`` when given, else the freshest slot."""
    if swap_key_hex:
        return _swap_status_by_key(validator, swap_key_hex)
    client = validator.solana_client
    miner_pk = resolve_miner_pubkey(validator, miner_hotkey)
    if miner_pk is None:
        return SwapStatus('none')
    reservation = _freshest_reservation(client, miner_pk, from_chain, to_chain)
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
        refund = validator.state_store.get_swap_refund(swap_key_hex)
        if refund:
            detail.update(refund)
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
    SwapCancelled/StaleClaimClosed ingest) disambiguates. On an outcome miss, fall back NON-terminal to ``fulfilled``: the miss is
    normally ingest lag (another validator's quorum closed the PDA since our last forward-step
    ingest) and self-corrects at the next ingest, whereas a terminal guess would stop the
    consumer polling on a wrong answer — for a slash, exactly the bug this index exists to fix."""
    if swap is None:
        outcome = validator.state_store.get_swap_outcome(swap_key.hex())
        return outcome or 'fulfilled'
    return _STAGE_BY_NAME.get(type(getattr(swap, 'status', None)).__name__, 'claimed')
