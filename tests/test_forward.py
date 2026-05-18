"""Validator forward pass — timeout-extension orchestration.

Covers ``extend_fulfilled_near_timeout``, the per-step loop that finalizes a
pending timeout extension and may propose the next one. Regression: a propose
fired in the same step as a finalize anchors the new extension on the current
block instead of the freshly extended deadline, collapsing its runway. This
reproduces the swap 206 timeout.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

from allways.classes import Swap, SwapStatus
from allways.validator.forward import extend_fulfilled_near_timeout
from allways.validator.swap_tracker import SwapTracker

# Real swap 206 numbers: extension 2 was proposed at this block, and a BTC
# timeout extension targets current_block + 240.
PROPOSE_BLOCK = 8_207_273
EXT_TARGET = PROPOSE_BLOCK + 240


def make_fulfilled_swap(swap_id: int = 206) -> Swap:
    """A FULFILLED TAO->BTC swap with a visible dest tx, one block from timeout."""
    return Swap(
        id=swap_id,
        user_hotkey='user',
        miner_hotkey='miner',
        from_chain='tao',
        to_chain='btc',
        from_amount=100_000_000,
        to_amount=37_189,
        tao_amount=100_000_000,
        user_from_address='5user',
        user_to_address='bc1q-user',
        to_tx_hash='dest-tx-hash',
        status=SwapStatus.FULFILLED,
        timeout_block=PROPOSE_BLOCK + 1,
    )


def make_validator(swap: Swap, block: int, finalized_target):
    """Stand-in Validator for ``extend_fulfilled_near_timeout``.

    ``finalized_target``: what ``maybe_finalize_timeout`` returns — an int for
    a finalize that lands this step, or ``None`` for no finalize.
    """
    tracker = SwapTracker(client=MagicMock())
    tracker.active[swap.id] = swap

    ext = MagicMock()
    ext.fetch_pending_timeout.return_value = None
    ext.maybe_finalize_timeout.return_value = finalized_target
    ext.maybe_propose_timeout.return_value = False

    provider = MagicMock()
    provider.verify_transaction.return_value = SimpleNamespace(confirmations=1)

    contract_client = MagicMock()
    contract_client.get_swap_extension_count.return_value = 1

    return SimpleNamespace(
        swap_tracker=tracker,
        block=block,
        optimistic_extensions=ext,
        chain_providers={'btc': provider},
        contract_client=contract_client,
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
