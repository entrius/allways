"""Validator forward pass — timeout-extension orchestration.

Covers ``extend_fulfilled_near_timeout``, the per-step loop that finalizes a
pending timeout extension and may propose the next one. Regression: a propose
fired in the same step as a finalize anchors the new extension on the current
block instead of the freshly extended deadline, collapsing its runway. This
reproduces the swap 206 timeout.
"""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

from allways.classes import Swap, SwapStatus
from allways.validator.forward import extend_fulfilled_near_timeout, try_extend_reservation
from allways.validator.state_store import PendingConfirm, ReservationPin, ValidatorStateStore
from allways.validator.swap_tracker import SwapTracker

# Real swap 206 numbers: extension 2 was proposed at this block, and a BTC
# timeout extension targets current_block + 240.
PROPOSE_BLOCK = 8_207_273
EXT_TARGET = PROPOSE_BLOCK + 240


def make_fulfilled_swap(swap_id: int = 206, to_amount: int = 37_189) -> Swap:
    """A FULFILLED TAO->BTC swap with a visible dest tx, one block from timeout."""
    return Swap(
        id=swap_id,
        user_hotkey='user',
        miner_hotkey='miner',
        from_chain='tao',
        to_chain='btc',
        from_amount=100_000_000,
        to_amount=to_amount,
        tao_amount=100_000_000,
        user_from_address='5user',
        user_to_address='bc1q-user',
        miner_to_address='bc1q-miner',
        rate='268',
        to_tx_hash='dest-tx-hash',
        status=SwapStatus.FULFILLED,
        timeout_block=PROPOSE_BLOCK + 1,
    )


_UNSET = object()


def make_validator(swap: Swap, block: int, finalized_target, tx_info=_UNSET):
    """Stand-in Validator for ``extend_fulfilled_near_timeout``.

    ``finalized_target``: what ``maybe_finalize_timeout`` returns — an int for
    a finalize that lands this step, or ``None`` for no finalize.
    ``tx_info``: what the provider's ``verify_transaction`` returns. Default
    is a success namespace; pass ``None`` to simulate the provider rejecting
    the dest tx (e.g. amount below canonical payout or sender mismatch).
    """
    tracker = SwapTracker(client=MagicMock())
    tracker.active[swap.id] = swap

    ext = MagicMock()
    ext.fetch_pending_timeout.return_value = None
    ext.maybe_finalize_timeout.return_value = finalized_target
    ext.maybe_propose_timeout.return_value = False

    provider = MagicMock()
    provider.verify_transaction.return_value = SimpleNamespace(confirmations=1) if tx_info is _UNSET else tx_info
    provider.get_chain.return_value = SimpleNamespace(min_confirmations=3)

    contract_client = MagicMock()
    contract_client.get_swap_extension_count.return_value = 1

    return SimpleNamespace(
        swap_tracker=tracker,
        block=block,
        optimistic_extensions=ext,
        chain_providers={'btc': provider},
        contract_client=contract_client,
        swap_verifier=SimpleNamespace(fee_divisor=100),
    )


class TestExtendFulfilledNearTimeout:
    def test_skips_repropose_when_finalize_pushes_deadline_out(self):
        # The swap is one block from its deadline, so it enters the loop. The
        # finalize this step jumps the deadline to current_block + 240 — far
        # past the near-timeout window — so the step must not propose again.
        swap = make_fulfilled_swap()
        v = make_validator(swap, block=PROPOSE_BLOCK, finalized_target=EXT_TARGET)

        extend_fulfilled_near_timeout(v)

        v.optimistic_extensions.maybe_finalize_timeout.assert_called_once()
        assert swap.timeout_block == EXT_TARGET  # finalize applied
        # No longer near timeout → no second extension this step.
        v.optimistic_extensions.maybe_propose_timeout.assert_not_called()
        v.optimistic_extensions.maybe_challenge_timeout.assert_not_called()
        v.contract_client.get_swap_extension_count.assert_not_called()

    def test_proposes_when_still_near_timeout(self):
        # No finalize this step; the swap stays near its deadline, so the
        # propose path must still run — the guard must not block a legitimate
        # near-timeout extension.
        swap = make_fulfilled_swap()
        v = make_validator(swap, block=PROPOSE_BLOCK, finalized_target=None)

        extend_fulfilled_near_timeout(v)

        v.optimistic_extensions.maybe_propose_timeout.assert_called_once()

    def test_verifies_canonical_payout_and_miner_sender(self):
        # Extension evidence must mirror final-confirm: expected_amount is the
        # canonical payout from swap.rate (not the miner-controlled
        # swap.to_amount), and expected_sender is pinned to miner_to_address.
        # Without this, a dust mark_fulfilled buys timeout protection that
        # final-confirm itself would reject.
        swap = make_fulfilled_swap(to_amount=1)  # miner posted dust
        v = make_validator(swap, block=PROPOSE_BLOCK, finalized_target=None)

        extend_fulfilled_near_timeout(v)

        provider = v.chain_providers['btc']
        call = provider.verify_transaction.call_args
        assert call.kwargs['expected_sender'] == 'bc1q-miner'
        # Canonical payout derived from rate, not the dust to_amount the miner posted.
        assert call.kwargs['expected_amount'] != 1
        assert call.kwargs['expected_amount'] > 0

    def test_skips_extension_when_dest_tx_fails_canonical_check(self):
        # Provider returns None when the dest tx doesn't match the canonical
        # amount or expected sender. The extension path must then skip propose
        # so the swap proceeds to timeout naturally instead of being protected
        # by a fraudulent mark_fulfilled.
        swap = make_fulfilled_swap(to_amount=1)
        v = make_validator(swap, block=PROPOSE_BLOCK, finalized_target=None, tx_info=None)

        extend_fulfilled_near_timeout(v)

        v.optimistic_extensions.maybe_propose_timeout.assert_not_called()
        v.optimistic_extensions.maybe_challenge_timeout.assert_not_called()


def make_reservation_validator(store, current_block, finalized_target):
    """Stand-in Validator for ``try_extend_reservation`` carrying a real state
    store. ``maybe_finalize_reservation`` returns ``finalized_target`` (an int
    for an inline finalize this step)."""
    ext = MagicMock()
    ext.fetch_pending_reservation.return_value = MagicMock()
    ext.maybe_finalize_reservation.return_value = finalized_target

    subtensor = MagicMock()
    subtensor.get_current_block.return_value = current_block

    contract_client = MagicMock()
    contract_client.get_miner_reserved_until.return_value = 1000

    return SimpleNamespace(
        subtensor=subtensor,
        contract_client=contract_client,
        optimistic_extensions=ext,
        state_store=store,
    )


class TestTryExtendReservationPinSync:
    """Regression for #441: an inline reservation-extension finalize must bump
    BOTH the pending_confirms row and the reservation pin, so the same forward
    step's pin purge can't drop a still-live pin at its stale TTL."""

    def _seed(self, store):
        store.upsert_reservation_pin(
            ReservationPin(
                miner_hotkey='miner-1',
                reserve_block=900,
                from_chain='btc',
                to_chain='tao',
                rate_str='345',
                counter_rate_str='0.0029',
                miner_from_address='bc1-miner',
                miner_to_address='5miner',
                reserved_until=1000,
                created_at=1.0,
            )
        )
        item = PendingConfirm(
            miner_hotkey='miner-1',
            from_tx_hash='tx-1',
            from_chain='btc',
            to_chain='tao',
            from_address='bc1-user',
            to_address='5user',
            tao_amount=123,
            from_amount=456,
            to_amount=789,
            miner_from_address='bc1-miner',
            miner_to_address='5miner',
            rate_str='345',
            reserved_until=1000,
            queued_at=1.0,
        )
        store.enqueue(item)
        return item

    def test_inline_finalize_bumps_pin_so_same_step_purge_keeps_it(self, tmp_path: Path):
        # Current block is past the original reserved_until (1000), so the pin
        # purge would drop the pin unless the inline finalize bumped its TTL.
        store = ValidatorStateStore(db_path=tmp_path / 'state.db', current_block_fn=lambda: 1003)
        item = self._seed(store)
        v = make_reservation_validator(store, current_block=1003, finalized_target=1300)

        # tx_info=None returns right after the finalize block, so we exercise the
        # finalize path without the propose/challenge machinery.
        try_extend_reservation(v, item, current_block=1003, swap_label='X', miner_short='Y', tx_info=None)

        assert store.get_reservation_pin('miner-1').reserved_until == 1300
        assert store.get_all()[0].reserved_until == 1300
        assert store.purge_expired_reservation_pins() == 0
        assert store.get_reservation_pin('miner-1') is not None
        store.close()
