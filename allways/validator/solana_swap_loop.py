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
from typing import Any, Dict, List, Optional, Set, Tuple

import bittensor as bt

from allways.chain_providers.base import ProviderUnreachableError
from allways.solana import pdas
from allways.solana.client import swap_key_from_tx_hash
from allways.utils.rate import apply_fee_deduction, expected_swap_amounts


class SwapDecision(Enum):
    ATTEST = 'attest'  # PendingAttestation: source deposit verified -> would vote_initiate
    CONFIRM = 'confirm'  # Fulfilled: both legs verified -> would confirm_swap
    TIMEOUT = 'timeout'  # Active past its deadline -> would timeout_swap
    WAIT = 'wait'  # in-flight, nothing to do yet
    SKIP = 'skip'  # provider unreachable / unverifiable this round
    REJECT = 'reject'  # to_amount inconsistent with pinned rate -> never attest (terminal no-op)


def _status_name(swap: Any) -> str:
    """Borsh enum decodes to an instance whose type name is the variant (Active/Fulfilled/...)."""
    s = swap.status
    return s if isinstance(s, str) else type(s).__name__


def _swap_key_hex(key: Any) -> str:
    return key.hex() if isinstance(key, (bytes, bytearray)) else str(key)


def is_tx_fresh(info: Any, floor_unix: int, grace: int = 0) -> bool:
    """Replay defense: the tx must be mined AFTER the on-chain floor (unix seconds).

    Fresh iff block_time > floor - grace. Compares block_time, NOT block height (the floor is unix
    seconds). Fails closed when block_time is missing. Shared by the loop's CONFIRM/ATTEST gates and the
    axon claim relay so they agree."""
    block_time = getattr(info, 'block_time', None)
    if block_time is None:
        return False
    return block_time > floor_unix - grace


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
        """Return ('ok', info) for a confirmed match, ('no', None) if absent/unconfirmed/no-provider,
        ('down', None) if the provider was unreachable this round."""
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
        if info is None or not info.confirmed:
            return ('no', None)
        return ('ok', info)

    def _is_fresh(self, info: Any, floor_unix: int, chain: str, label: str) -> bool:
        """Replay defense: the tx must be mined AFTER the on-chain floor (unix seconds).

        Compares block_time, NOT block height (the floor is unix seconds, so a height-vs-seconds compare
        would silently always-pass). Fresh iff block_time > floor - grace; a per-chain GRACE (default 0)
        absorbs honest clock skew. Fails closed if block_time is missing."""
        provider = self.providers.get(chain)
        grace = getattr(provider.get_chain(), 'replay_grace_secs', 0) if provider else 0
        if not is_tx_fresh(info, floor_unix, grace):
            bt.logging.warning(
                f'{label}: {chain} tx block_time {getattr(info, "block_time", None)} not after floor '
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

    def verify_fulfillment(self, swap: Any) -> Optional[bool]:
        """Verify source (user funded miner) + dest (miner delivered 99% to user, fresh). None = provider
        down. Source freshness was gated at attestation; dest freshness (vs swap.initiated_at) is here."""
        s_status, _ = self._fetch_leg(
            swap.from_chain,
            swap.from_tx_hash,
            swap.miner_from_addr,
            int(swap.from_amount),
            block_hint=int(getattr(swap, 'from_tx_block', 0)),
        )
        if s_status == 'down':
            return None
        if s_status != 'ok':
            return False
        d_status, d_info = self._fetch_leg(
            swap.to_chain,
            swap.to_tx_hash,
            swap.user_to_addr,
            self.expected_user_receives(swap),
            block_hint=int(getattr(swap, 'to_tx_block', 0)),
            sender=swap.miner_to_addr,
        )
        if d_status == 'down':
            return None
        if d_status != 'ok':
            return False
        # Dest freshness: payout must be mined after the swap was initiated on-chain (replay defense).
        if not self._is_fresh(d_info, int(swap.initiated_at), swap.to_chain, self._label(swap)):
            return False
        return True

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

    def decide(self, swap: Any, now: int) -> SwapDecision:
        """Per-status decision. Verifies legs where needed; reads chain providers + the Reservation PDA."""
        status = _status_name(swap)
        if status == 'PendingAttestation':
            # Refuse to attest if to_amount is inconsistent with the pinned (unforgeable) miner rate.
            expected_to, _ = expected_swap_amounts(swap, self.fee_divisor)
            if expected_to == 0 or abs(int(swap.to_amount) - expected_to) > 1:
                self._reject_logged(swap, expected_to)
                return SwapDecision.REJECT
            # Source deposit must exist, confirm, AND be fresh vs the Reservation before we'd attest.
            s_status, info = self._fetch_leg(
                swap.from_chain,
                swap.from_tx_hash,
                swap.miner_from_addr,
                int(swap.from_amount),
                block_hint=int(getattr(swap, 'from_tx_block', 0)),
            )
            if s_status == 'down':
                return SwapDecision.SKIP
            if s_status != 'ok':
                return SwapDecision.WAIT
            reservation = self._get_reservation(swap.miner)
            if reservation is None:
                bt.logging.warning(f'{self._label(swap)}: no reservation read — cannot check freshness, waiting')
                return SwapDecision.WAIT
            # Source freshness: deposit must be mined after the reservation was created (replay defense).
            if not self._is_fresh(info, int(reservation.created_at), swap.from_chain, self._label(swap)):
                return SwapDecision.WAIT  # replayed/stale deposit — never attest
            return SwapDecision.ATTEST
        overdue = now >= int(swap.timeout_at)
        if status == 'Active':
            return SwapDecision.TIMEOUT if overdue else SwapDecision.WAIT
        if status == 'Fulfilled':
            ok = self.verify_fulfillment(swap)
            if ok is None:
                return SwapDecision.SKIP
            if ok:
                return SwapDecision.CONFIRM
            # Unverifiable dest + overdue ⇒ TIMEOUT (contract slashes Fulfilled too); else wait for the leg.
            return SwapDecision.TIMEOUT if overdue else SwapDecision.WAIT
        return SwapDecision.WAIT

    def _cast_vote(self, swap: Any, decision: SwapDecision) -> bool:
        """Submit the on-chain consensus vote for an actionable decision. Pre-checks the VoteRound so we
        don't re-submit a vote already cast; treats a lost race (swap already closed) as a no-op. Never
        raises — one bad swap must not break the pass."""
        swap_key = swap_key_from_tx_hash(swap.from_tx_hash)
        voter = self.client.keypair.pubkey()
        label = self._label(swap)
        try:
            if decision == SwapDecision.ATTEST:
                if self.client.has_voted(pdas.REQ_INITIATE, swap.miner, voter):
                    return False
                self.client.vote_initiate(swap_key, swap.miner)
            elif decision == SwapDecision.CONFIRM:
                if self.client.has_voted(pdas.REQ_CONFIRM, swap_key, voter):
                    return False
                self.client.confirm_swap(swap_key, swap.miner, swap.from_chain, swap.to_chain)
            elif decision == SwapDecision.TIMEOUT:
                if self.client.has_voted(pdas.REQ_TIMEOUT, swap_key, voter):
                    return False
                self.client.timeout_swap(swap_key, swap.miner, swap.user)
            elif decision == SwapDecision.REJECT:
                return False  # rate-inconsistent claim: cast no vote, leave it to go stale and reap
            else:
                return False
        except Exception as e:
            bt.logging.error(f'{label}: {decision.value} vote failed: {e}')
            return False
        bt.logging.success(f'{label}: {decision.value} vote submitted')
        return True

    def resolve_pools_once(self, now: int) -> List[str]:
        """Permissionless crank: resolve every pool whose window has closed into a winner Reservation.
        Idempotent — `resolve_pool` zeroes `opened_at` + clears requests, so a resolved pool is skipped
        next pass. One bad pool never breaks the sweep. Returns the miners whose pools we resolved."""
        resolved: List[str] = []
        for _pubkey, pool in self.client.get_all('Pool'):
            if int(getattr(pool, 'opened_at', 0)) == 0:
                continue  # available/empty slot
            if now <= int(pool.closes_at):
                continue  # window still open
            if not getattr(pool, 'requests', None):
                continue  # nothing to draw
            miner = pool.miner
            if self.read_only:
                bt.logging.info(f'pool {miner}: WOULD resolve_pool ({len(pool.requests)} req, read-only)')
                continue
            try:
                self.client.resolve_pool(miner)
            except Exception as e:  # one bad pool must not break the pass
                bt.logging.error(f'pool {miner}: resolve_pool failed: {e}')
                continue
            bt.logging.success(f'pool {miner}: resolved ({len(pool.requests)} req)')
            resolved.append(str(miner))
        return resolved

    def run_once(self, now: int) -> List[Tuple[str, SwapDecision]]:
        """One pass: discover live swaps, decide each, and cast the vote (or LOG when read_only).
        Returns the per-swap decisions for observability."""
        out: List[Tuple[str, SwapDecision]] = []
        for pubkey, swap in self.client.get_swaps():
            key = _swap_key_hex(getattr(swap, 'swap_key', pubkey))
            try:
                decision = self.decide(swap, now)
            except Exception as e:  # one bad swap must not break the pass
                bt.logging.error(f'swap {key}: decide failed: {e}')
                continue
            if decision in (SwapDecision.ATTEST, SwapDecision.CONFIRM, SwapDecision.TIMEOUT):
                if self.read_only:
                    bt.logging.info(f'swap {key} [{_status_name(swap)}]: WOULD {decision.value} (read-only)')
                else:
                    self._cast_vote(swap, decision)
            out.append((key, decision))
        return out
