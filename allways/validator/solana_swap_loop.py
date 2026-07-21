"""Contract-driven swap loop (B1 read path + B2 voting + freshness).

Discovers live swaps from the Solana contract (getProgramAccounts), decides per status, verifies via the
(unchanged) chain providers with replay-freshness gates, and casts the on-chain consensus vote
(vote_initiate / confirm_swap / timeout_swap). Set `read_only=True` for a dry run (logs "WOULD …", no
votes). Decoupled from the old SwapVerifier so it can be unit-tested in isolation.

Fee model = Option A (decided 2026-06-25): the user receives 99% of the on-chain `to_amount` (ink!-style
delivery haircut); the protocol's 1% is skimmed from the miner's SOL collateral by `confirm_swap`. So the
validator verifies the dest leg delivered `apply_fee_deduction(to_amount, FEE_DIVISOR)`.
"""

from enum import Enum
from typing import Any, Dict, List, NamedTuple, Optional, Set, Tuple

import bittensor as bt
from solders.pubkey import Pubkey

from allways import dev_signal
from allways.chain_providers.base import ProviderUnreachableError
from allways.chains import compute_extension_target_secs, get_chain
from allways.constants import EXTENSION_PADDING_SECONDS
from allways.solana import pdas
from allways.solana.client import benign_marker, swap_from_solana, swap_key_from_tx_hash
from allways.utils.rate import apply_fee_deduction, expected_swap_amounts


class SwapDecision(Enum):
    ATTEST = 'attest'  # PendingAttestation: source deposit verified -> would vote_initiate
    CONFIRM = 'confirm'  # Fulfilled: both legs verified -> would confirm_swap
    TIMEOUT = 'timeout'  # Active past its deadline -> would timeout_swap
    CANCEL = 'cancel'  # PendingAttestation whose reservation expired -> close_stale_claim (reap, no slash)
    EXTEND_RESERVATION = 'extend_reservation'  # source valid-but-unconfirmed near expiry -> slide reserved_until
    EXTEND_TIMEOUT = 'extend_timeout'  # dest valid-but-unconfirmed near timeout -> slide timeout_at
    WAIT = 'wait'  # in-flight, nothing to do yet
    SKIP = 'skip'  # provider unreachable / unverifiable this round
    REJECT = 'reject'  # to_amount inconsistent with pinned rate -> never attest (terminal no-op)


class SwapAction(NamedTuple):
    """A decision plus the extension target it implies (``target_at`` is None for non-extend decisions)
    and a human ``reason`` (leg tri-states / why) surfaced per pass so a WAITing swap isn't silent."""

    decision: SwapDecision
    target_at: Optional[int] = None
    reason: Optional[str] = None


# Decisions that drive an on-chain write this pass.
ACTIONABLE = frozenset(
    {
        SwapDecision.ATTEST,
        SwapDecision.CONFIRM,
        SwapDecision.TIMEOUT,
        SwapDecision.CANCEL,
        SwapDecision.EXTEND_RESERVATION,
        SwapDecision.EXTEND_TIMEOUT,
    }
)

# Contract errors / lost races that make an extension a benign no-op (another validator already slid it,
# or we're at the ceiling) — log quietly, never propagate.
_BENIGN_EXTEND_MARKERS = ('ExtensionNotLater', 'ExtensionExceedsCeiling')

# resolve_pool is permissionless, so every validator cranks every closed pool and the contract's
# first-wins idempotency handles the race. A loser's tx fails with one of these — a peer already
# resolved it (NoRequests: opened_at zeroed), the on-chain clock hasn't crossed closes_at yet
# (PoolNotClosed, clock skew), or the armed draw slot isn't on-chain yet (SeedSlotNotYetProduced —
# resolve is two-phase: arm, then draw). All expected, retried next pass. Real failures still surface.
_BENIGN_RESOLVE_MARKERS = ('NoRequests', 'PoolNotClosed', 'SeedSlotNotYetProduced')

# close_stale_claim is permissionless, so every validator cranks every stale claim and the first-wins race
# leaves losers with a benign failure: a peer already reaped it (the Swap PDA is gone), or the reservation is
# not yet expired on the on-chain clock (ClaimNotExpired, clock skew) — both expected, retried/settled next pass.
_BENIGN_CLOSE_MARKERS = ('ClaimNotExpired', 'NotPending', 'AccountNotInitialized', 'could not find account')


def _status_name(swap: Any) -> str:
    """Borsh enum decodes to an instance whose type name is the variant (Active/Fulfilled/...)."""
    s = swap.status
    return s if isinstance(s, str) else type(s).__name__


def _confs(chain_id: str, info: Any) -> str:
    """Confirmation progress of a leg, e.g. '1/2 confs'. Unmined or absent legs read 0."""
    have = int(getattr(info, 'confirmations', 0) or 0)
    return f'{have}/{get_chain(chain_id).min_confirmations} confs'


def _swap_key_hex(key: Any) -> str:
    return key.hex() if isinstance(key, (bytes, bytearray)) else str(key)


def is_tx_fresh(info: Any, floor_unix: int, grace: int = 0) -> bool:
    """Replay defense: the tx must be mined AT OR AFTER the on-chain floor (unix seconds).

    Fresh iff block_time >= floor - grace. A deposit sent immediately after reserving lands in the same
    unix second as the floor (block_time granularity is seconds), so a strict `>` would wrongly reject an
    honest same-second deposit; only a tx that *predates* the floor is a replay. Compares block_time, NOT
    block height (the floor is unix seconds). Fails closed when block_time is missing. Shared by the loop's
    CONFIRM/ATTEST gates and the axon claim relay so they agree."""
    block_time = getattr(info, 'block_time', None)
    if block_time is None:
        return False
    return block_time >= floor_unix - grace


class SolanaSwapLoop:
    def __init__(
        self,
        solana_client: Any,
        chain_providers: Dict[str, Any],
        fee_divisor: int = 100,
        read_only: bool = False,
    ):
        self.client = solana_client
        self.providers = chain_providers
        self.fee_divisor = fee_divisor
        self.read_only = read_only
        self.reject_warned: Set[str] = set()  # dedupe rate-reject warnings, one per swap key

    def expected_user_receives(self, swap: Any) -> int:
        """Dest amount the miner must deliver = 99% of the pinned to_amount (Option A)."""
        return apply_fee_deduction(int(swap.to_amount), self.fee_divisor)

    def _label(self, swap: Any) -> str:
        try:
            sk = swap_key_from_tx_hash(swap.from_tx_hash).hex()[:16]
        except Exception:
            sk = '?'
        return f'swap {sk} [{swap.from_chain}->{swap.to_chain}]'

    def _fetch_leg(
        self, chain: str, tx_hash: str, recipient: str, amount: int, block_hint: int = 0, sender: str = ''
    ) -> Tuple[str, Any]:
        """Tri-state leg status: ('ok', info) confirmed match, ('pending', info) all details match but
        awaiting confirmations (extendable), ('no', None) absent/mismatch/no-provider (slash-eligible),
        ('down', None) provider unreachable this round."""
        provider = self.providers.get(chain)
        if provider is None:
            bt.logging.warning(f'no chain provider for {chain}; cannot verify')
            return ('no', None)
        if not tx_hash:
            return ('no', None)
        try:
            info = provider.verify_transaction(
                tx_hash=tx_hash,
                expected_recipient=recipient,
                expected_amount=amount,
                block_hint=block_hint,
                expected_sender=sender or None,
            )
        except ProviderUnreachableError:
            return ('down', None)
        if info is None:
            return ('no', None)
        if not info.confirmed:
            return ('pending', info)
        return ('ok', info)

    def _is_fresh(self, info: Any, floor_unix: int, chain: str, label: str) -> bool:
        """Replay defense: the tx must be mined AT OR AFTER the on-chain floor (unix seconds).

        Compares block_time, NOT block height (the floor is unix seconds, so a height-vs-seconds compare
        would silently always-pass). Fresh iff block_time >= floor - grace; a per-chain GRACE (default 0)
        absorbs honest clock skew. Fails closed if block_time is missing."""
        provider = self.providers.get(chain)
        grace = getattr(provider.get_chain(), 'replay_grace_secs', 0) if provider else 0
        if not is_tx_fresh(info, floor_unix, grace):
            bt.logging.warning(
                f'{label}: {chain} tx block_time {getattr(info, "block_time", None)} predates floor '
                f'{floor_unix} (grace {grace}s) — replay/stale, rejecting'
            )
            return False
        return True

    def _get_reservation(self, miner: Any) -> Any:
        try:
            return self.client.get_reservation(miner)
        except Exception as e:
            bt.logging.warning(f'reservation read failed for miner: {e}')
            return None

    def _extend_target(self, chain: str, info: Any, deadline: int, ceiling: int, now: int) -> Optional[int]:
        """Unix-seconds target to extend `deadline` to, or None when no extension is warranted. Gates
        (shared by reservation + timeout): the deadline must be live, near expiry (within
        EXTENSION_PADDING_SECONDS), below the contract ceiling, and the bucketed target strictly later."""
        if deadline <= 0:
            return None  # no live reservation/deadline
        if deadline - now > EXTENSION_PADDING_SECONDS:
            return None  # plenty of runway — don't extend prematurely
        if deadline >= ceiling:
            return None  # no room below the contract ceiling (max_extend_at)
        target = compute_extension_target_secs(chain, int(info.confirmations), now, ceiling)
        return target if target > deadline else None

    def _extend_reservation_action(self, swap: Any, info: Any, now: int) -> SwapAction:
        """Source leg valid-but-unconfirmed near reservation expiry → slide reserved_until, else WAIT."""
        reservation = self._get_reservation(swap.miner)
        if reservation is None:
            return SwapAction(SwapDecision.WAIT)
        target = self._extend_target(
            swap.from_chain, info, int(reservation.reserved_until), int(reservation.max_extend_at), now
        )
        return SwapAction(SwapDecision.EXTEND_RESERVATION, target) if target else SwapAction(SwapDecision.WAIT)

    def _extend_timeout_action(self, swap: Any, info: Any, now: int) -> SwapAction:
        """Dest leg valid-but-unconfirmed near swap timeout → slide timeout_at, else WAIT."""
        target = self._extend_target(swap.to_chain, info, int(swap.timeout_at), int(swap.max_extend_at), now)
        return SwapAction(SwapDecision.EXTEND_TIMEOUT, target) if target else SwapAction(SwapDecision.WAIT)

    def _decide_fulfilled(self, swap: Any, now: int, overdue: bool) -> SwapAction:
        """Verify the DEST leg (miner delivered 99% to user, fresh) and decide. CONFIRM when dest verifies;
        EXTEND_TIMEOUT a valid-but-unconfirmed dest near timeout; TIMEOUT an overdue swap whose dest is
        absent/mismatched (the contract slashes Fulfilled too).

        The SOURCE leg is NOT re-fetched here. A swap can only reach Fulfilled via PendingAttestation →
        attest quorum (`vote_initiate`, which verifies source + freshness) → Active → `mark_fulfilled`, so
        the source was already verified, freshness-checked, and frozen — a confirmed tx can't regress.
        Re-fetching it every 12s pass added only RPC load, and worse: a transient node-view gap on that
        re-fetch could return non-`ok` and slash an already-attested payout (the same false-negative class
        as the dest-leg stale-view issue). Source is therefore a settled `ok` by construction."""
        s_status = 'ok'  # source verified + frozen at attestation (Fulfilled ⟹ attested); see docstring
        d_status, d_info = self._fetch_leg(
            swap.to_chain,
            swap.to_tx_hash,
            swap.user_to_addr,
            self.expected_user_receives(swap),
            block_hint=int(getattr(swap, 'to_tx_block', 0)),
            sender=swap.miner_to_addr,
        )
        if d_status == 'down':
            return SwapAction(SwapDecision.SKIP, reason=f'dest provider unreachable (src={s_status})')
        if d_status == 'pending':
            # Valid-but-unconfirmed payout near timeout → extend; if extension is exhausted/not yet due,
            # fall through to the same overdue rule (at the ceiling + overdue still slashes).
            action = self._extend_timeout_action(swap, d_info, now)
            if action.decision == SwapDecision.EXTEND_TIMEOUT:
                return action._replace(
                    reason=f'dest {_confs(swap.to_chain, d_info)} near timeout → extend timeout_at to '
                    f'{action.target_at} (+{action.target_at - now}s)'
                )
        # Dest freshness: payout must be mined after the swap was initiated on-chain (replay defense).
        elif (
            s_status == 'ok'
            and d_status == 'ok'
            and self._is_fresh(d_info, int(swap.initiated_at), swap.to_chain, self._label(swap))
        ):
            return SwapAction(SwapDecision.CONFIRM, reason='src=ok dst=ok dst-fresh — both legs verified')
        # Unverifiable/stale dest + overdue ⇒ TIMEOUT; else wait for the leg. Without the confirmation
        # count and remaining runway, a healthy deferral and an imminent slash render identically.
        why = (
            f'src={s_status} dst={d_status} [{_confs(swap.to_chain, d_info)}] timeout_in={int(swap.timeout_at) - now}s'
        )
        return (
            SwapAction(SwapDecision.TIMEOUT, reason=f'{why} + overdue — dest unverifiable, slashing')
            if overdue
            else SwapAction(SwapDecision.WAIT, reason=f'{why} — awaiting a verifiable+fresh dest leg')
        )

    def _reject_logged(self, swap: Any, expected_to: int) -> None:
        """Warn once per swap key that to_amount diverges from the pinned rate (security-relevant, greppable)."""
        key = _swap_key_hex(swap.swap_key)
        if key in self.reject_warned:
            return
        self.reject_warned.add(key)
        bt.logging.warning(
            f'{self._label(swap)}: REJECT — to_amount {swap.to_amount} inconsistent with pinned rate '
            f'{swap.rate} (expected {expected_to}); refusing to attest [swap_key {key}]'
        )
        dev_signal.emit('d1_reject', swap_key=key, expected=expected_to, got=int(swap.to_amount))

    def _claim_is_stale(self, reservation: Any, swap: Any, now: int) -> bool:
        """A PendingAttestation claim is orphaned when its reservation can no longer carry it to attestation:
        expired (reserved_until < now) or its claim slot no longer points at this swap (re-resolved/consumed).
        Mirrors the contract's close_stale_claim guard so we never send a tx it would reject. False when the
        reservation is unreadable this round (retry next pass)."""
        if reservation is None:
            return False
        if int(reservation.reserved_until) < now:
            return True
        return bytes(reservation.claimed_swap_key) != swap_key_from_tx_hash(swap.from_tx_hash)

    def _decide_pending_attestation(self, swap: Any, now: int) -> SwapAction:
        reservation = self._get_reservation(swap.miner)
        # An orphaned claim can never attest (vote_initiate needs a live reservation) — reap it (close_stale_claim)
        # to free the miner + reclaim rent. Covers a dropped/RBF'd source and one past its extension ceiling; a
        # source landing after the ceiling is the taker's tail risk (nothing moved on our side to refund).
        if self._claim_is_stale(reservation, swap, now):
            return SwapAction(SwapDecision.CANCEL, reason='reservation expired/superseded — reaping stale claim')
        # D1: refuse to attest if to_amount is inconsistent with the pinned (unforgeable) miner rate.
        expected_to, _ = expected_swap_amounts(swap, self.fee_divisor)
        if expected_to == 0 or abs(int(swap.to_amount) - expected_to) > 1:
            self._reject_logged(swap, expected_to)
            return SwapAction(SwapDecision.REJECT, reason=f'to_amount {swap.to_amount} != pinned-rate {expected_to}')
        # Source deposit must exist, confirm, be sent BY the reserved user, AND be fresh vs the
        # Reservation before we'd attest — sender pin matches the relay's confirm_deposit check.
        s_status, info = self._fetch_leg(
            swap.from_chain,
            swap.from_tx_hash,
            swap.miner_from_addr,
            int(swap.from_amount),
            block_hint=int(getattr(swap, 'from_tx_block', 0)),
            sender=swap.user_from_addr,
        )
        if s_status == 'down':
            return SwapAction(SwapDecision.SKIP, reason='source provider unreachable')
        if s_status == 'pending':
            # Valid-but-unconfirmed deposit near reservation expiry → extend so the honest miner isn't slashed.
            action = self._extend_reservation_action(swap, info, now)
            left = int(reservation.reserved_until) - now if reservation is not None else 0
            detail = f'source {_confs(swap.from_chain, info)} reserved_until_in={left}s'
            if action.decision == SwapDecision.EXTEND_RESERVATION:
                return action._replace(
                    reason=f'{detail} → extend reserved_until to {action.target_at} (+{action.target_at - now}s)'
                )
            return action._replace(reason=f'{detail} — awaiting confirmations')
        if s_status != 'ok':
            return SwapAction(SwapDecision.WAIT, reason=f'source deposit {s_status} — awaiting user funds')
        if reservation is None:
            bt.logging.warning(f'{self._label(swap)}: no reservation read — cannot check freshness, waiting')
            return SwapAction(SwapDecision.WAIT, reason='no reservation read')
        # Source freshness: deposit must be mined after the reservation was created (replay defense).
        if not self._is_fresh(info, int(reservation.created_at), swap.from_chain, self._label(swap)):
            return SwapAction(SwapDecision.WAIT, reason='source deposit stale/replayed — never attest')
        return SwapAction(SwapDecision.ATTEST, reason='source verified + fresh')

    def decide(self, swap: Any, now: int) -> SwapAction:
        """Per-status decision (+extension target where applicable). Verifies legs where needed; reads
        chain providers + the Reservation PDA."""
        status = _status_name(swap)
        overdue = now >= int(swap.timeout_at)
        if status == 'PendingAttestation':
            return self._decide_pending_attestation(swap, now)
        if status == 'Active':
            # Never extend: no mark_fulfilled = no broadcast evidence, so an overdue Active is slash-eligible.
            return SwapAction(
                SwapDecision.TIMEOUT if overdue else SwapDecision.WAIT,
                reason='overdue, miner never fulfilled — slashing' if overdue else 'awaiting miner mark_fulfilled',
            )
        if status == 'Fulfilled':
            return self._decide_fulfilled(swap, now, overdue)
        return SwapAction(SwapDecision.WAIT, reason=f'status {status} — no action')

    def _cast_vote(self, swap: Any, action: SwapAction) -> bool:
        """Submit the on-chain write for an actionable decision. Pre-checks the VoteRound so we don't
        re-submit a vote already cast; treats a lost race (swap already closed) as a no-op. Extensions
        tolerate the contract ceiling / lost extension races as no-ops. Never raises — one bad swap must
        not break the pass."""
        decision = action.decision
        swap_key = swap_key_from_tx_hash(swap.from_tx_hash)
        voter = self.client.keypair.pubkey()
        label = self._label(swap)
        try:
            if decision == SwapDecision.ATTEST:
                if self.client.has_voted(pdas.REQ_INITIATE, swap.miner, voter):
                    return False
                sig = self.client.vote_initiate(swap_key, swap.miner)
            elif decision == SwapDecision.CONFIRM:
                if self.client.has_voted(pdas.REQ_CONFIRM, swap_key, voter):
                    return False
                sig = self.client.confirm_swap(swap_key, swap.miner, swap.from_chain, swap.to_chain)
            elif decision == SwapDecision.TIMEOUT:
                if self.client.has_voted(pdas.REQ_TIMEOUT, swap_key, voter):
                    return False
                sig = self.client.timeout_swap(swap_key, swap.miner, swap.user)
            elif decision == SwapDecision.CANCEL:
                # Permissionless reap (no vote round) — first validator wins, peers no-op benignly.
                sig = self.client.close_stale_claim(swap.miner, swap_key)
            elif decision == SwapDecision.EXTEND_RESERVATION:
                sig = self.client.extend_reservation(swap.miner, action.target_at)
            elif decision == SwapDecision.EXTEND_TIMEOUT:
                sig = self.client.extend_timeout(swap_key, swap.miner, action.target_at)
            else:
                return False  # REJECT / non-actionable: cast nothing
        except Exception as e:
            if decision in (SwapDecision.EXTEND_RESERVATION, SwapDecision.EXTEND_TIMEOUT) and (
                m := benign_marker(e, _BENIGN_EXTEND_MARKERS)
            ):
                bt.logging.debug(f'{label}: {decision.value} no-op ({m})')
                return False
            if decision == SwapDecision.CANCEL and (m := benign_marker(e, _BENIGN_CLOSE_MARKERS)):
                bt.logging.debug(f'{label}: {decision.value} no-op ({m})')
                return False
            bt.logging.error(f'{label}: {decision.value} failed: {e}')
            return False
        bt.logging.success(f'{label}: {decision.value} submitted')
        dev_signal.emit('vote_cast', swap_key=_swap_key_hex(swap.swap_key), decision=decision.value, sig=sig)
        return True

    def resolve_pools_once(self, now: int) -> List[str]:
        """Permissionless crank: resolve every closed, non-empty, unresolved pool into a winner
        Reservation. `resolve_pool` is idempotent (first-wins: it zeroes `opened_at` + clears requests),
        so racing validators are safe — a loser's tx is a benign no-op, logged quietly. One bad pool never
        breaks the sweep. Returns the miners whose pools we resolved. (A per-pool cranker assignment to
        avoid the redundant losing txs is a deferred optimization — D-CRANK.)"""
        resolved: List[str] = []
        for _pubkey, pool in self.client.get_all('Pool'):
            if int(getattr(pool, 'opened_at', 0)) == 0:
                continue  # available/empty slot — already resolved or never opened
            if now <= int(pool.closes_at):
                continue  # window still open
            if not getattr(pool, 'requests', None):
                continue  # nothing to draw
            miner = pool.miner
            reqs = getattr(pool, 'requests', None) or []
            # Log the pool's participants (the routers/users bidding for this miner's slot) before the draw,
            # so the stake-weighted resolution is auditable — every contender, not just the winner.

            def _fmt(v: Any) -> str:
                # Request pubkey fields decode to raw 32-byte arrays; render base58 (readable) not b'\\x..'.
                try:
                    if isinstance(v, (bytes, bytearray, list)):
                        return str(Pubkey(bytes(v)))[:8]
                    return str(v)[:8]
                except Exception:
                    return '?'

            def _who(r: Any) -> str:
                parts = []
                for f in ('router', 'validator', 'user', 'requester'):
                    v = getattr(r, f, None)
                    if v:
                        parts.append(f'{f}={_fmt(v)}')
                return '/'.join(parts) or '?'

            bt.logging.info(
                f'pool {miner}: CLOSED @ {int(getattr(pool, "closes_at", 0))} — {len(reqs)} contender(s): '
                + ', '.join(_who(r) for r in reqs)
            )
            if self.read_only:
                bt.logging.info(f'pool {miner}: WOULD resolve_pool ({len(reqs)} req, read-only)')
                continue
            try:
                self.client.resolve_pool(miner)
            except Exception as e:  # one bad pool must not break the pass
                if m := benign_marker(e, _BENIGN_RESOLVE_MARKERS):
                    bt.logging.debug(f'pool {miner}: resolve_pool no-op ({m})')
                    continue
                bt.logging.error(f'pool {miner}: resolve_pool failed: {e}')
                continue
            bt.logging.success(f'pool {miner}: resolved ({len(pool.requests)} req)')
            dev_signal.emit('pool_resolved', miner=str(miner), requests=len(reqs))
            resolved.append(str(miner))
        return resolved

    def run_once(self, now: int) -> List[Tuple[str, SwapDecision]]:
        """One pass: discover live swaps, decide each, and cast the vote (or LOG when read_only).
        Returns the per-swap decisions for observability."""
        out: List[Tuple[str, SwapDecision]] = []
        for _pubkey, acct in self.client.get_swaps():
            # get_swaps returns raw accounts (no swap_key field); flatten like the miner does. Already-flat
            # views (carrying swap_key) pass through.
            swap = acct if hasattr(acct, 'swap_key') else swap_from_solana(acct)
            key = _swap_key_hex(swap.swap_key)
            try:
                action = self.decide(swap, now)
            except Exception as e:  # one bad swap must not break the pass
                bt.logging.error(f'swap {key}: decide failed: {e}')
                continue
            # Per-swap visibility EVERY pass — the story from the validator's lens: what it saw + decided this
            # round, including a plain WAIT (previously silent, which hid a stalled swap until it timed out).
            reason = f' — {action.reason}' if action.reason else ''
            bt.logging.info(f'swap {key} [{_status_name(swap)}]: {action.decision.value}{reason}')
            dev_signal.emit(
                'decision',
                swap_key=key,
                status=_status_name(swap),
                decision=action.decision.value,
                reason=action.reason,
            )
            if action.decision in ACTIONABLE:
                if self.read_only:
                    bt.logging.info(f'swap {key} [{_status_name(swap)}]: WOULD {action.decision.value} (read-only)')
                else:
                    self._cast_vote(swap, action)
            out.append((key, action.decision))
        return out
