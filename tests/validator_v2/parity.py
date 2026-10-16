"""Runs the existing swap-loop decision tests against the new decision core.

ParityLoop has SolanaSwapLoop's constructor and ``decide``, but decides with ``swaps.decide`` over a
context that answers synchronously from the same fakes the old tests use: so every old expectation is a
check on the new rules."""

from typing import Any

from allways.assets.asset import ProviderUnreachableError
from allways.solana.client import swap_key_from_tx_hash
from allways.validator.solana_swap_loop import (
    SolanaSwapLoop,
    SwapAction,
    SwapDecision,
    collateral_verdict_for,
    payout_reuse_reason,
)
from allways.validator_v2.actuator.swaps import Leg, NotReady, decide


class LegacyContext:
    def __init__(self, loop: 'ParityLoop', now: int) -> None:
        self.loop, self.now = loop, now
        self.legs = {}  # chain → Leg, as the production context records them for the status report

    def provider(self, chain):
        return self.loop.providers.get(chain)

    def reservation(self, swap):
        try:
            return self.loop.client.get_reservation(swap.miner, str(swap.collateral_chain))
        except Exception:
            return None

    def leg(self, chain, tx_hash, recipient, amount, block_hint, sender):
        provider = self.provider(chain)
        if provider is None:
            return Leg('down')
        tx_hash = provider.chain.canonical_tx_hash(tx_hash)
        if tx_hash is None:
            return Leg('no')
        try:
            info = provider.verify_transaction(
                tx_hash=tx_hash,
                expected_recipient=recipient,
                expected_amount=amount,
                block_hint=block_hint,
                expected_sender=sender or None,
            )
        except ProviderUnreachableError:
            return Leg('down')
        leg = Leg('no') if info is None else Leg('ok' if info.confirmed else 'pending', info)
        self.legs[chain] = leg
        return leg

    def cancel_evidence(self, swap, amount):
        provider = self.provider(swap.to_chain)
        if provider is None:
            return None
        try:
            return provider.cancel_evidence(
                swap.user_to_addr,
                amount,
                getattr(swap, 'to_tx_hash', '') or None,
                from_address=getattr(swap, 'miner_to_addr', '') or None,
            )
        except Exception:
            return None

    def delivery_refused(self, swap):
        provider = self.provider(swap.to_chain)
        if provider is None:
            return False
        try:
            return provider.delivery_refused(swap.user_to_addr, int(swap.initiated_at))
        except Exception:
            return True

    def collateral_ok(self, swap, reservation):
        try:
            return collateral_verdict_for(
                self.loop.state_store,
                swap.miner,
                str(swap.collateral_chain),
                swap.from_chain,
                int(swap.from_amount),
                swap.to_chain,
                int(swap.to_amount),
                int(swap.collateral_amount),
                int(reservation.created_at),
                self.loop.providers,
            )
        except (ProviderUnreachableError, ValueError) as e:
            raise NotReady(f'alpha leg unpriceable at fill block: {e}')

    def deposit_claimed(self, swap):
        store = self.loop.state_store
        if store is None:
            return False
        key = swap_key_from_tx_hash(swap.from_tx_hash).hex()
        store.record_deposit_claim(swap.from_tx_hash, key, self.now)
        held_key, initiated = store.deposit_claim(swap.from_tx_hash)
        return held_key != key or initiated

    def payout_reuse(self, swap):
        provider = self.provider(swap.from_chain)
        if provider is None:
            return False
        return (
            payout_reuse_reason(
                self.loop.state_store,
                provider.chain,
                swap.from_chain,
                swap.user_from_addr,
                swap.miner_from_addr,
                swap.from_tx_hash,
            )
            is not None
        )

    def claim_delivery(self, swap):
        store = self.loop.state_store
        if store is None:
            return True
        return store.claim_delivery(
            str(swap.miner),
            swap.to_tx_hash,
            swap.swap_key.hex() if isinstance(swap.swap_key, bytes) else str(swap.swap_key),
            self.now,
        )

    def owes_settlement(self, swap):
        backing = str(getattr(swap, 'collateral_chain', '') or '').lower()
        if self.loop.relay is None or not backing or backing == 'sol':
            return False
        return self.loop.relay.has_pending_debit(str(swap.miner))


class ParityLoop(SolanaSwapLoop):
    """SolanaSwapLoop whose ``decide`` is the new core; everything else is inherited for the fixtures."""

    def decide(self, swap: Any, now: int) -> SwapAction:
        status = swap.status if isinstance(swap.status, str) else type(swap.status).__name__
        if status in ('Active', 'Fulfilled') and self.state_store is not None:
            key = swap_key_from_tx_hash(swap.from_tx_hash).hex()
            self.state_store.record_deposit_claim(swap.from_tx_hash, key, now)
            self.state_store.mark_deposit_initiated(key)
        ctx = LegacyContext(self, now)
        action = decide(swap, ctx, now, self.fee_divisor)
        for chain, leg in ctx.legs.items():
            self._publish_confs(swap, 'source' if chain == swap.from_chain else 'dest', chain, leg.info)
        if action.decision.value == 'reject':
            self._reject_logged(swap, action.reason)
        return SwapAction(SwapDecision(action.decision.value), action.target_at, action.reason, action.reason_code)
