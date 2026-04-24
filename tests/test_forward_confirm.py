"""Tests for ``confirm_miner_fulfillments`` in allways.validator.forward.

Focus: the exception dispatch after ``asyncio.gather(..., return_exceptions=True)``
must distinguish transient provider failures (defer the swap) from programming
errors (surface them by re-raising), not lump them together as "verification
error" the way a broad Exception catch would.
"""

import asyncio
from dataclasses import dataclass
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from allways.chain_providers.base import ProviderUnreachableError
from allways.validator.forward import confirm_miner_fulfillments


@dataclass
class FakeSwap:
    id: int


class FakeTracker:
    def __init__(self, swaps, voted_ids=None, resolved=None):
        self._swaps = swaps
        self._voted = set(voted_ids or [])
        self.resolved = resolved if resolved is not None else []

    def get_fulfilled(self, current_block):
        return list(self._swaps)

    def is_voted(self, swap_id):
        return swap_id in self._voted

    def resolve(self, swap_id, status, block):
        self.resolved.append((swap_id, status, block))


class TestConfirmMinerFulfillmentsErrorHandling:
    def test_provider_unreachable_is_deferred(self):
        swap = FakeSwap(id=1)
        tracker = FakeTracker([swap])
        verifier = MagicMock()

        async def raises_unreachable(_swap):
            raise ProviderUnreachableError('btc node down')

        verifier.verify_miner_fulfillment = raises_unreachable
        self_ns = SimpleNamespace(contract_client=MagicMock(), wallet=MagicMock())

        uncertain = asyncio.run(
            confirm_miner_fulfillments(self_ns, tracker, verifier, current_block=100)
        )

        assert uncertain == {1}
        assert tracker.resolved == []

    def test_programming_error_is_reraised(self):
        """A typo / AttributeError / any non-provider exception must propagate,
        not get swallowed as a generic verification error. Otherwise a bug in
        the verifier silently skips votes across every affected swap."""
        swap = FakeSwap(id=2)
        tracker = FakeTracker([swap])
        verifier = MagicMock()

        async def raises_typo(_swap):
            raise AttributeError("'Swap' object has no attribute 'block_numebr'")

        verifier.verify_miner_fulfillment = raises_typo
        self_ns = SimpleNamespace(contract_client=MagicMock(), wallet=MagicMock())

        with pytest.raises(AttributeError, match='block_numebr'):
            asyncio.run(
                confirm_miner_fulfillments(self_ns, tracker, verifier, current_block=100)
            )

    def test_successful_verification_still_votes(self):
        """Regression guard: the strict exception policy must not disturb
        the happy path."""
        swap = FakeSwap(id=3)
        tracker = FakeTracker([swap])
        verifier = MagicMock()

        async def returns_true(_swap):
            return True

        verifier.verify_miner_fulfillment = returns_true

        voting_mock = MagicMock()
        voting_mock.confirm_swap.return_value = True

        self_ns = SimpleNamespace(contract_client=MagicMock(), wallet=MagicMock())

        import allways.validator.forward as forward_mod

        orig_voting = forward_mod.voting
        forward_mod.voting = voting_mock
        try:
            uncertain = asyncio.run(
                confirm_miner_fulfillments(self_ns, tracker, verifier, current_block=100)
            )
        finally:
            forward_mod.voting = orig_voting

        assert uncertain == set()
        assert len(tracker.resolved) == 1
        assert tracker.resolved[0][0] == 3
