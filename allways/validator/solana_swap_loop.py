"""B1 — read-only contract-driven swap loop.

Discovers live swaps from the Solana contract (getProgramAccounts), decides per status, verifies via the
(unchanged) chain providers, and LOGS the decision — no on-chain votes (B2 un-stubs voting + adds
freshness). Decoupled from the old SwapVerifier so it can be unit-tested in isolation; reuses only the
chain-provider primitive (`verify_transaction`) + the fee math (`apply_fee_deduction`).

Fee model = Option A (decided 2026-06-25): the user receives 99% of the on-chain `to_amount` (ink!-style
delivery haircut); the protocol's 1% is skimmed from the miner's SOL collateral by `confirm_swap`. So the
validator verifies the dest leg delivered `apply_fee_deduction(to_amount, FEE_DIVISOR)`.
"""

from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import bittensor as bt

from allways.chain_providers.base import ProviderUnreachableError
from allways.utils.rate import apply_fee_deduction


class SwapDecision(Enum):
    ATTEST = 'attest'  # PendingAttestation: source deposit verified -> would vote_initiate
    CONFIRM = 'confirm'  # Fulfilled: both legs verified -> would confirm_swap
    TIMEOUT = 'timeout'  # Active past its deadline -> would timeout_swap
    WAIT = 'wait'  # in-flight, nothing to do yet
    SKIP = 'skip'  # provider unreachable / unverifiable this round


def _status_name(swap: Any) -> str:
    """Borsh enum decodes to an instance whose type name is the variant (Active/Fulfilled/...)."""
    s = swap.status
    return s if isinstance(s, str) else type(s).__name__


def _swap_key_hex(key: Any) -> str:
    return key.hex() if isinstance(key, (bytes, bytearray)) else str(key)


class SolanaSwapLoop:
    def __init__(self, solana_client: Any, chain_providers: Dict[str, Any], fee_divisor: int = 100):
        self.client = solana_client
        self.providers = chain_providers
        self.fee_divisor = fee_divisor

    def expected_user_receives(self, swap: Any) -> int:
        """Dest amount the miner must deliver = 99% of the pinned to_amount (Option A)."""
        return apply_fee_deduction(int(swap.to_amount), self.fee_divisor)

    def _verify_leg(
        self, chain: str, tx_hash: str, recipient: str, amount: int, block_hint: int = 0, sender: str = ''
    ) -> Optional[bool]:
        """True/False if verifiable this round; None if the provider was unreachable (skip)."""
        provider = self.providers.get(chain)
        if provider is None:
            bt.logging.warning(f'no chain provider for {chain}; cannot verify')
            return False
        if not tx_hash:
            return False
        try:
            info = provider.verify_transaction(
                tx_hash=tx_hash,
                expected_recipient=recipient,
                expected_amount=amount,
                block_hint=block_hint,
                expected_sender=sender or None,
            )
        except ProviderUnreachableError:
            return None
        return info is not None and info.confirmed

    def verify_fulfillment(self, swap: Any) -> Optional[bool]:
        """Verify source (user funded miner) + dest (miner delivered 99% to user). None = provider down."""
        source = self._verify_leg(
            swap.from_chain,
            swap.from_tx_hash,
            swap.miner_from_addr,
            int(swap.from_amount),
            block_hint=int(getattr(swap, 'from_tx_block', 0)),
        )
        dest = self._verify_leg(
            swap.to_chain,
            swap.to_tx_hash,
            swap.user_to_addr,
            self.expected_user_receives(swap),
            block_hint=int(getattr(swap, 'to_tx_block', 0)),
            sender=swap.miner_to_addr,
        )
        if source is None or dest is None:
            return None
        return source and dest

    def decide(self, swap: Any, now: int) -> SwapDecision:
        """Per-status decision. Verifies legs where needed; pure-ish (only reads chain providers)."""
        status = _status_name(swap)
        if status == 'PendingAttestation':
            # Source deposit must exist/confirm before we'd attest (full source freshness vs created_at = B2).
            ok = self._verify_leg(
                swap.from_chain,
                swap.from_tx_hash,
                swap.miner_from_addr,
                int(swap.from_amount),
                block_hint=int(getattr(swap, 'from_tx_block', 0)),
            )
            if ok is None:
                return SwapDecision.SKIP
            return SwapDecision.ATTEST if ok else SwapDecision.WAIT
        if status == 'Active':
            return SwapDecision.TIMEOUT if now > int(swap.timeout_at) else SwapDecision.WAIT
        if status == 'Fulfilled':
            ok = self.verify_fulfillment(swap)
            if ok is None:
                return SwapDecision.SKIP
            return SwapDecision.CONFIRM if ok else SwapDecision.WAIT
        return SwapDecision.WAIT

    def run_once(self, now: int) -> List[Tuple[str, SwapDecision]]:
        """One read-only pass: discover live swaps, decide each, LOG (no votes). Returns the decisions."""
        out: List[Tuple[str, SwapDecision]] = []
        for pubkey, swap in self.client.get_swaps():
            key = _swap_key_hex(getattr(swap, 'swap_key', pubkey))
            try:
                decision = self.decide(swap, now)
            except Exception as e:  # one bad swap must not break the pass
                bt.logging.error(f'swap {key}: decide failed: {e}')
                continue
            if decision in (SwapDecision.ATTEST, SwapDecision.CONFIRM, SwapDecision.TIMEOUT):
                bt.logging.info(f'swap {key} [{_status_name(swap)}]: WOULD {decision.value} (read-only B1)')
            out.append((key, decision))
        return out
