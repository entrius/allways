"""Transport-agnostic kernel ops for the on-behalf reservation flow.

One source of truth for reserve / confirm / rate / status, shared by the axon synapse handlers
(CLI transport) and the localhost HTTP seam (offering transport). Every op validates protocol
invariants before it signs — the caller (offering or CLI) is never trusted.
"""

import re
import time
from dataclasses import dataclass, field
from typing import Optional

import bittensor as bt
from bittensor import Keypair
from solders.pubkey import Pubkey

from allways.chain_providers.base import ProviderUnreachableError
from allways.cli.swap_commands.swap_intake import (
    candidate_miners,
    compute_intake_amounts,
    rate_display_from_fixed,
    select_best_miner,
    swap_viable,
)
from allways.solana.client import swap_key_from_tx_hash
from allways.validator.binding import hotkey_ss58, verify_binding

EMPTY_SWAP_KEY = b'\x00' * 32


def _contract_reject_reason(err: Exception) -> Optional[str]:
    """A deliberate on-chain program rejection is a normal domain rejection — e.g. the miner got
    reserved between our pre-check and this tx (a race the contract, as final arbiter, closes). Return a
    clean human reason so the seam answers 422, not a 500. Returns None for a genuine transport/RPC
    fault, which must still surface as an error.

    A program rejection surfaces two ways: a PRE-FLIGHT simulation reject carries the Anchor name /
    'custom program error' text; a tx that is submitted and LANDS failed surfaces through the confirm
    path as `{'InstructionError': [0, {'Custom': N}]}` — a numeric code with neither phrase. Both are
    contract domain rejections (a transport fault has no Custom program code), so 422 for both."""
    s = str(err)
    sl = s.lower()
    if not ('custom program error' in sl or 'anchorerror' in sl or 'instructionerror' in sl or "'custom':" in sl):
        return None
    m = re.search(r'Error Message: ([^.\"\']+)', s)
    if m:
        return m.group(1).strip()
    code = re.search(r"'Custom':\s*(\d+)", s)  # landed-tx form has no human message — surface the code
    if code:
        return f'miner is not available for reservation right now (contract error {code.group(1)})'
    return 'miner is not available for reservation right now'


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
    client = validator.solana_client
    miner_pk = resolve_miner_pubkey(validator, miner_hotkey)
    if miner_pk is None:
        return ReserveResult(False, 'miner hotkey is not bound to a Solana miner')

    miner_state = client.get_miner_state(miner_pk)
    if miner_state is None or not miner_state.active:
        return ReserveResult(False, 'miner is not active')

    now = int(time.time())
    pool = client.get_pool(miner_pk)
    joining = (
        pool is not None
        and int(getattr(pool, 'opened_at', 0) or 0) != 0
        and now <= int(getattr(pool, 'closes_at', 0) or 0)
        and pool.from_chain == from_chain
        and pool.to_chain == to_chain
    )
    if joining:
        rate_fixed = pool.rate  # pinned at open — joiners must quote against it
    else:
        if miner_state.has_active_swap:
            return ReserveResult(False, 'miner is busy with another swap; try again shortly')
        quote = client.get_quote(miner_pk, from_chain, to_chain)
        if quote is None:
            return ReserveResult(False, f'miner has no quote for {from_chain}->{to_chain}')
        rate_fixed = quote.rate

    try:
        amts = compute_intake_amounts(from_chain, to_chain, from_amount, rate_display_from_fixed(rate_fixed))
    except ValueError as e:
        return ReserveResult(False, str(e))
    if amts.to_amount <= 0:
        return ReserveResult(False, 'non-positive dest amount for that source amount')

    cfg = client.get_config()
    min_swap = int(getattr(cfg, 'min_swap_amount', 0) or 0)
    max_swap = int(getattr(cfg, 'max_swap_amount', 0) or 0)
    collateral = client.get_collateral_lamports(miner_pk) or 0
    ok, reason = swap_viable(amts.collateral_amount, collateral, min_swap, max_swap)
    if not ok:
        return ReserveResult(False, reason)

    try:
        user_pk = Pubkey.from_string(user_pubkey)
    except Exception:
        return ReserveResult(False, 'invalid user Solana pubkey')

    # Two-phase: this places a BID only (the pair). The taker + amounts computed above are a
    # pre-flight viability check; naming them on-chain is the winner's `finalize_reservation` step.
    try:
        sig = client.open_or_request(miner_pk, from_chain, to_chain)
    except Exception as e:
        reason = _contract_reject_reason(e)
        if reason is None:
            raise
        return ReserveResult(False, reason)
    # The entry landed — queue the user's details for `finalize_won_seats`. Persisted (not held in
    # memory) so a validator restart inside the pool/finalize window still honors the routing promise.
    validator.state_store.upsert_routed_request(
        str(miner_pk), from_chain, to_chain, str(user_pk), user_from_addr, user_to_addr, from_amount, now
    )
    pool = client.get_pool(miner_pk)
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
    for miner, from_chain, to_chain in store.distinct_routed_pools():
        queue = store.pending_routed_requests(miner, from_chain, to_chain)
        if not queue:
            continue
        entered_at = queue[0]['created_at']
        try:
            resv = client.get_reservation(Pubkey.from_string(miner))
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
                store.delete_routed_requests(miner, from_chain, to_chain)
            elif fresh:
                bt.logging.info(f'routed sweep {miner[:8]}: reservation filled or window lapsed, dropping queue')
                store.delete_routed_requests(miner, from_chain, to_chain)
            continue
        req = draw_pool_winner(queue)
        if read_only:
            bt.logging.info(f'routed sweep {miner[:8]}: WOULD finalize for {req["user_pubkey"][:8]} (read-only)')
            continue
        try:
            fill = compute_intake_amounts(from_chain, to_chain, req['from_amount'], rate_display_from_fixed(resv.rate))
            client.finalize_reservation(
                Pubkey.from_string(miner),
                Pubkey.from_string(req['user_pubkey']),
                req['user_from_addr'],
                req['user_to_addr'],
                fill.collateral_amount,
                fill.from_amount,
                fill.to_amount,
            )
        except Exception as e:
            reason = _contract_reject_reason(e) or (str(e) if isinstance(e, ValueError) else None)
            if reason is None:
                bt.logging.warning(f'routed sweep {miner[:8]}: finalize transport fault, retrying next step: {e}')
                continue
            bt.logging.warning(f'routed sweep {miner[:8]}: finalize rejected ({reason}), dropping queue')
            store.delete_routed_requests(miner, from_chain, to_chain)
            continue
        bt.logging.info(f'routed sweep {miner[:8]}: finalized seat for {req["user_pubkey"][:8]} (FIFO of queue)')
        store.delete_routed_requests(miner, from_chain, to_chain)
        finalized.append(miner)
    store.prune_routed_requests(now - ROUTED_REQUEST_TTL_SECS)
    return finalized


@dataclass
class ConfirmResult:
    ok: bool
    reason: str = ''
    swap_key: str = ''
    sig: str = ''


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

    reservation = client.get_reservation(miner_pk)
    if reservation is None:
        return ConfirmResult(False, 'No reservation for this miner')
    now = int(time.time())
    if reservation.reserved_until == 0 or reservation.reserved_until < now:
        return ConfirmResult(False, 'Reservation is not active')
    if bytes(reservation.claimed_swap_key) != EMPTY_SWAP_KEY:
        return ConfirmResult(False, 'Reservation already has a claimed swap')

    provider = validator.axon_chain_providers.get(reservation.from_chain)
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
        grace = getattr(provider.get_chain(), 'replay_grace_secs', 0)
        if not is_tx_fresh(tx_info, int(reservation.created_at), grace):
            return ConfirmResult(False, 'Source tx fails freshness — stale/replayed deposit')

    swap_key = swap_key_from_tx_hash(from_tx_hash)
    sig = client.submit_swap_claim(miner_pk, swap_key, from_tx_hash, tx_info.block_number or 0)
    return ConfirmResult(True, '', swap_key.hex(), sig)


@dataclass
class BestQuote:
    miner_hotkey: str
    miner: str  # Solana pubkey (base58)
    rate_display: str
    collateral_amount: int
    from_amount: int
    to_amount: int


def best_quote(validator, from_chain: str, to_chain: str, from_amount: int) -> Optional[BestQuote]:
    """Best executable quote for ``from_amount`` (source smallest-units): the miner giving the most dest.

    Mirrors ``select_best_miner`` so the displayed rate == the reservable rate. None if none qualify."""
    client = validator.solana_client
    cfg = client.get_config()
    min_swap = int(getattr(cfg, 'min_swap_amount', 0) or 0)
    max_swap = int(getattr(cfg, 'max_swap_amount', 0) or 0)
    best = select_best_miner(
        candidate_miners(client, from_chain, to_chain), from_chain, to_chain, from_amount, min_swap, max_swap
    )
    if best is None:
        return None
    cand, amts = best
    hotkey = _miner_hotkey_for(validator, cand.miner)
    if hotkey is None:
        return None
    return BestQuote(
        hotkey, str(cand.miner), cand.rate_display, amts.collateral_amount, amts.from_amount, amts.to_amount
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
    reservation = client.get_reservation(miner_pk)
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
        return SwapStatus(stage, swap_key=swap_key_hex)
    # Same detail shape as the reservation path — the Swap PDA carries the full legs.
    detail = {
        'from_chain': swap.from_chain,
        'to_chain': swap.to_chain,
        'from_amount': int(swap.from_amount),
        'to_amount': int(swap.to_amount),
        'miner_from_addr': swap.miner_from_addr,
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
