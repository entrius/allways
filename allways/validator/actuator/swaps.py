"""Swap lifecycle decisions: a pure function of one swap and a context that answers its questions.

``decide`` holds the rules (attest, confirm, extend, timeout, refuse, reap) and asks ``SwapContext`` for
every fact it needs: the reservation, each leg's verdict, the ledgers, dest-chain evidence. It makes no
call of its own. A context that can't answer yet raises ``NotReady`` and the swap waits a tick, so an
unanswered question can never become a slash."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional, Protocol

from allways.chains import compute_extension_target_secs, get_chain_def
from allways.constants import CANCEL_REASON_INVALID_DEST, EXTENSION_PADDING_SECONDS, FEE_DIVISOR
from allways.solana.client import swap_key_from_tx_hash
from allways.utils.rate import apply_fee_deduction, expected_swap_amounts

COLLATERAL_REJECT_REASON = 'collateral does not match the alpha leg at fill block'


def is_tx_fresh(info: Any, floor_unix: int, grace: int = 0) -> bool:
    """Replay defense: mined at or after the on-chain floor (unix seconds, less the chain's grace). A deposit
    in the same second as the reservation is honest, so only one that predates the floor is a replay.
    Fails closed without a block time."""
    block_time = getattr(info, 'block_time', None)
    return block_time is not None and block_time >= floor_unix - grace


def attest_reject_reason(providers: Any, swap: Any, fee_divisor: int) -> Optional[str]:
    """Why a PendingAttestation claim must never attest, or None. Offline and deterministic (quorum votes)."""
    from_provider = providers.get(swap.from_chain)
    # Swap keys hash the raw string, so a claim must carry the chain's one canonical spelling.
    if from_provider is not None and from_provider.chain.canonical_tx_hash(swap.from_tx_hash) != swap.from_tx_hash:
        return 'source tx hash not in canonical form — refusing to attest'
    to_provider = providers.get(swap.to_chain)
    if to_provider is not None and not to_provider.chain.is_valid_address(swap.user_to_addr):
        return 'dest address invalid — refusing to attest'
    expected_to, _ = expected_swap_amounts(swap, fee_divisor)  # must agree with the pinned miner rate
    if expected_to == 0 or abs(int(swap.to_amount) - expected_to) > 1:
        return f'to_amount {swap.to_amount} != pinned-rate {expected_to}'
    # A self-transfer is slashed; normalize per chain to catch the EVM checksum variants a raw compare misses.
    norm = to_provider.chain.normalize_address if to_provider is not None else (lambda a: a)
    if swap.miner_to_addr and norm(swap.user_to_addr) == norm(swap.miner_to_addr):
        return 'dest == miner delivery address (poisoned) — refusing to attest'
    return None


class Decision(Enum):
    ATTEST = 'attest'  # PendingAttestation, source verified + fresh → vote_initiate
    CONFIRM = 'confirm'  # Fulfilled, dest verified + fresh → confirm_swap
    TIMEOUT = 'timeout'  # overdue and unproven → timeout_swap
    CANCEL = 'cancel'  # PendingAttestation whose reservation is gone → close_stale_claim (no slash)
    REFUSE = 'refuse'  # provably unpayable dest → cancel_swap (no slash, fee or strike)
    EXTEND_RESERVATION = 'extend_reservation'  # source valid but unconfirmed near expiry
    EXTEND_TIMEOUT = 'extend_timeout'  # dest valid but unconfirmed near timeout
    WAIT = 'wait'
    SKIP = 'skip'  # can't judge this tick (endpoint down, answer not in yet)
    REJECT = 'reject'  # a claim that can never attest; nothing to send


@dataclass(frozen=True)
class Action:
    decision: Decision
    target_at: Optional[int] = None  # the new deadline, for an extension
    reason: Optional[str] = None
    reason_code: Optional[int] = None  # CANCEL_REASON_* for a REFUSE


@dataclass(frozen=True)
class Leg:
    status: str  # 'ok' confirmed match | 'pending' match awaiting confirmations | 'no' absent/mismatch | 'down'
    info: Any = None
    checked_at: float = float('inf')  # when this verdict was read off the chain


class NotReady(Exception):
    """The context has asked but not yet heard back. The swap is skipped this tick, never judged on it."""


class SwapContext(Protocol):
    def provider(self, chain: str) -> Optional[Any]: ...

    def reservation(self, swap: Any) -> Optional[Any]: ...

    def leg(self, chain: str, tx_hash: str, recipient: str, amount: int, block_hint: int, sender: str) -> Leg: ...

    def cancel_evidence(self, swap: Any, amount: int) -> Optional[int]: ...

    def delivery_refused(self, swap: Any) -> bool: ...

    def collateral_ok(self, swap: Any, reservation: Any) -> bool: ...

    def deposit_claimed(self, swap: Any) -> bool: ...

    def payout_reuse(self, swap: Any) -> bool: ...

    def delivery_is_deposit(self, swap: Any) -> bool: ...

    def claim_delivery(self, swap: Any) -> bool: ...

    def owes_settlement(self, swap: Any) -> bool: ...


def status_of(swap: Any) -> str:
    s = swap.status
    return s if isinstance(s, str) else type(s).__name__


def confs(chain: str, info: Any) -> str:
    return f'{int(getattr(info, "confirmations", 0) or 0)}/{get_chain_def(chain).min_confirmations} confs'


def user_receives(swap: Any, fee_divisor: int = FEE_DIVISOR) -> int:
    """What the miner must deliver: the pinned to_amount less the protocol fee."""
    return apply_fee_deduction(int(swap.to_amount), fee_divisor)


def decide(swap: Any, ctx: SwapContext, now: int, fee_divisor: int = FEE_DIVISOR) -> Action:
    try:
        return _decide(swap, ctx, now, fee_divisor)
    except NotReady as e:
        return Action(Decision.SKIP, reason=f'waiting on {e or "a chain answer"}')


def _decide(swap: Any, ctx: SwapContext, now: int, fee_divisor: int) -> Action:
    status = status_of(swap)
    overdue = now >= int(swap.timeout_at)
    if status in ('Active', 'Fulfilled'):
        provider = ctx.provider(swap.to_chain)
        if provider is None:
            # Unverifiable here: wait while a peer with a provider may still resolve it, but past the ceiling
            # time out, or the miner's collateral stays locked forever.
            if now < int(swap.max_extend_at):
                return Action(Decision.SKIP, reason=f'no provider for dest {swap.to_chain} — cannot verify yet')
            return Action(
                Decision.TIMEOUT,
                reason=f'no provider for dest {swap.to_chain} past max_extend_at — terminal, else collateral freezes',
            )
        if not provider.chain.is_valid_address(swap.user_to_addr):
            return Action(
                Decision.REFUSE,
                reason='dest address invalid — cancel, no slash/fee/strike',
                reason_code=CANCEL_REASON_INVALID_DEST,
            )
    if status == 'PendingAttestation':
        return _pending_attestation(swap, ctx, now, fee_divisor)
    if status == 'Active':
        cancel = _cancel(swap, ctx, fee_divisor)
        if cancel is not None:
            return cancel
        if overdue and ctx.delivery_refused(swap):
            if now < int(swap.max_extend_at):
                return Action(Decision.WAIT, reason='overdue but dest may refuse — deferring, not slashing')
            return Action(
                Decision.TIMEOUT,
                reason='dest refuses past max_extend_at, no sound cancel evidence — terminal, slashing',
            )
        if overdue:
            return Action(Decision.TIMEOUT, reason='overdue, miner never fulfilled — slashing')
        return Action(Decision.WAIT, reason='awaiting miner mark_fulfilled')
    if status == 'Fulfilled':
        return _fulfilled(swap, ctx, now, overdue, fee_divisor)
    return Action(Decision.WAIT, reason=f'status {status} — no action')


def _cancel(swap: Any, ctx: SwapContext, fee_divisor: int) -> Optional[Action]:
    """REFUSE only on sound evidence that the dest can never be paid; a mere hint never cancels."""
    reason = ctx.cancel_evidence(swap, user_receives(swap, fee_divisor))
    if reason is None:
        return None
    return Action(
        Decision.REFUSE,
        reason=f'dest provably unpayable (reason={reason}) — cancel, no slash/fee/strike',
        reason_code=reason,
    )


def _fresh(ctx: SwapContext, chain: str, info: Any, floor: int) -> bool:
    provider = ctx.provider(chain)
    grace = getattr(provider.chain_def, 'replay_grace_secs', 0) if provider else 0
    return is_tx_fresh(info, floor, grace)


def _extend_target(chain: str, info: Any, deadline: int, ceiling: int, now: int) -> Optional[int]:
    """The deadline to extend to, or None: it must be live, near expiry, below the ceiling, and moved later."""
    if deadline <= 0 or deadline - now > EXTENSION_PADDING_SECONDS or deadline >= ceiling:
        return None
    target = compute_extension_target_secs(chain, int(info.confirmations), now, ceiling)
    return target if target > deadline else None


def _fulfilled(swap: Any, ctx: SwapContext, now: int, overdue: bool, fee_divisor: int) -> Action:
    # Only the dest leg is checked: Fulfilled implies the source was verified and frozen at attestation.
    dest = ctx.leg(
        swap.to_chain,
        swap.to_tx_hash,
        swap.user_to_addr,
        user_receives(swap, fee_divisor),
        int(getattr(swap, 'to_tx_block', 0)),
        swap.miner_to_addr,
    )
    if dest.status == 'down':
        if now < int(swap.max_extend_at):
            return Action(Decision.SKIP, reason='dest provider unreachable (src=ok)')
        return Action(
            Decision.TIMEOUT,
            reason='dest provider unreachable past max_extend_at (src=ok) — terminal, else collateral freezes',
        )
    if dest.status == 'pending':
        target = _extend_target(swap.to_chain, dest.info, int(swap.timeout_at), int(swap.max_extend_at), now)
        if target:
            return Action(
                Decision.EXTEND_TIMEOUT,
                target,
                reason=f'dest {confs(swap.to_chain, dest.info)} near timeout → extend timeout_at to {target} '
                f'(+{target - now}s)',
            )
    elif dest.status == 'ok' and _fresh(ctx, swap.to_chain, dest.info, int(swap.initiated_at)):
        if ctx.delivery_is_deposit(swap):
            return Action(
                Decision.TIMEOUT if overdue else Decision.WAIT,
                reason="delivery tx is another swap's source deposit" + (' — slashing' if overdue else ''),
            )
        if not ctx.claim_delivery(swap):
            return Action(
                Decision.TIMEOUT if overdue else Decision.WAIT,
                reason='delivery tx already confirmed another swap' + (' — slashing' if overdue else ''),
            )
        return Action(Decision.CONFIRM, reason='src=ok dst=ok dst-fresh — both legs verified')
    cancel = _cancel(swap, ctx, fee_divisor)
    if cancel is not None:
        return cancel
    why = f'src=ok dst={dest.status} [{confs(swap.to_chain, dest.info)}] timeout_in={int(swap.timeout_at) - now}s'
    if not overdue:
        return Action(Decision.WAIT, reason=f'{why} — awaiting a verifiable+fresh dest leg')
    # Only an answer that can still change needs a re-read: a confirmed leg is final, so re-checking it
    # would wait forever on a delivery that failed the freshness floor.
    if dest.status != 'ok' and dest.checked_at < int(swap.timeout_at):
        return Action(Decision.WAIT, reason=f'{why} — re-checking the dest after the deadline before any slash')
    if ctx.delivery_refused(swap):
        if now < int(swap.max_extend_at):
            return Action(Decision.WAIT, reason=f'{why} + overdue but dest may refuse — deferring, not slashing')
        return Action(
            Decision.TIMEOUT,
            reason=f'{why} + dest refuses past max_extend_at, no sound cancel evidence — terminal, slashing',
        )
    return Action(Decision.TIMEOUT, reason=f'{why} + overdue — dest unverifiable, slashing')


def _pending_attestation(swap: Any, ctx: SwapContext, now: int, fee_divisor: int) -> Action:
    reservation = ctx.reservation(swap)
    if reservation is not None and (
        int(reservation.reserved_until) < now
        or bytes(reservation.claimed_swap_key) != swap_key_from_tx_hash(swap.from_tx_hash)
    ):
        return Action(Decision.CANCEL, reason='reservation expired/superseded — reaping stale claim')
    reason = attest_reject_reason(_Providers(ctx), swap, fee_divisor)
    if reason is None and ctx.payout_reuse(swap):
        reason = "source tx is another swap's payout, not a deposit"
    if reason is None and ctx.deposit_claimed(swap):
        reason = 'source deposit already claimed by another swap'
    if reason:
        return Action(Decision.REJECT, reason=reason)
    if str(swap.collateral_chain) not in (swap.from_chain, swap.to_chain):
        if reservation is None:
            return Action(Decision.WAIT, reason='no reservation read')
        if not ctx.collateral_ok(swap, reservation):
            return Action(Decision.REJECT, reason=COLLATERAL_REJECT_REASON)
    source = ctx.leg(
        swap.from_chain,
        swap.from_tx_hash,
        swap.miner_from_addr,
        int(swap.from_amount),
        int(getattr(swap, 'from_tx_block', 0)),
        swap.user_from_addr,
    )
    if source.status == 'down':
        return Action(Decision.SKIP, reason='source provider unreachable')
    if source.status == 'pending':
        left = int(reservation.reserved_until) - now if reservation is not None else 0
        detail = f'source {confs(swap.from_chain, source.info)} reserved_until_in={left}s'
        target = None
        if reservation is not None:
            target = _extend_target(
                swap.from_chain, source.info, int(reservation.reserved_until), int(reservation.max_extend_at), now
            )
        if target:
            return Action(
                Decision.EXTEND_RESERVATION,
                target,
                reason=f'{detail} → extend reserved_until to {target} (+{target - now}s)',
            )
        return Action(Decision.WAIT, reason=f'{detail} — awaiting confirmations')
    if source.status != 'ok':
        return Action(Decision.WAIT, reason=f'source deposit {source.status} — awaiting user funds')
    if reservation is None:
        return Action(Decision.WAIT, reason='no reservation read')
    if not _fresh(ctx, swap.from_chain, source.info, int(reservation.created_at)):
        return Action(Decision.WAIT, reason='source deposit stale/replayed — never attest')
    if ctx.owes_settlement(swap):
        return Action(Decision.WAIT, reason='miner owes an unapplied vault debit — refusing to attest')
    return Action(Decision.ATTEST, reason='source verified + fresh')


class _Providers:
    """The ``providers.get(chain)`` view attest_reject_reason expects, over the context."""

    def __init__(self, ctx: SwapContext) -> None:
        self._ctx = ctx

    def get(self, chain: str) -> Optional[Any]:
        return self._ctx.provider(chain)
