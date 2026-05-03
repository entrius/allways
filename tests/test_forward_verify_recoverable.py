"""Regression tests for the asyncio.gather error-handling tightening (#178).

Previously ``confirm_miner_fulfillments`` used
``asyncio.gather(..., return_exceptions=True)`` with a catch-all
``isinstance(result, Exception)`` branch, so every exception — including
typos, ``AttributeError``, ``KeyError`` — was reduced to a logged warning
and the round kept going with silently-missing votes. Real bugs were
masked as flaky network conditions for the lifetime of the deployment.

These tests pin the new behavior:
- recoverable transport exceptions (``ProviderUnreachableError``,
  ``asyncio.TimeoutError``) are returned as values so the caller can
  defer verification;
- programming errors propagate out of the wrapper so the forward step
  fails loud.
"""

import asyncio
from unittest.mock import MagicMock

import pytest

from allways.chain_providers.base import ProviderUnreachableError
from allways.validator.forward import (
    RECOVERABLE_VERIFY_EXCEPTIONS,
    _verify_swallowing_only_recoverable,
)


class _StubVerifier:
    """Test double for SwapVerifier whose `verify_miner_fulfillment` is
    parameterised by the supplied awaitable."""

    def __init__(self, behavior):
        self._behavior = behavior

    async def verify_miner_fulfillment(self, swap):  # noqa: ARG002
        return await self._behavior(swap)


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro) if False else asyncio.run(coro)


class TestRecoverableVerifyExceptions:
    def test_provider_unreachable_is_recoverable(self):
        assert ProviderUnreachableError in RECOVERABLE_VERIFY_EXCEPTIONS

    def test_timeout_is_recoverable(self):
        assert asyncio.TimeoutError in RECOVERABLE_VERIFY_EXCEPTIONS

    def test_programming_errors_are_not_recoverable(self):
        # The whole point of #178: the catch-all `Exception` was the bug.
        # Anything outside the explicit allow-list must propagate.
        assert Exception not in RECOVERABLE_VERIFY_EXCEPTIONS
        assert AttributeError not in RECOVERABLE_VERIFY_EXCEPTIONS
        assert KeyError not in RECOVERABLE_VERIFY_EXCEPTIONS
        assert TypeError not in RECOVERABLE_VERIFY_EXCEPTIONS
        assert ValueError not in RECOVERABLE_VERIFY_EXCEPTIONS


class TestVerifySwallowingOnlyRecoverable:
    def test_passes_through_a_truthy_verification_result(self):
        async def verify_returns_true(_swap):
            return True

        verifier = _StubVerifier(verify_returns_true)
        result = _run(_verify_swallowing_only_recoverable(verifier, MagicMock()))
        assert result is True

    def test_passes_through_a_falsy_verification_result(self):
        async def verify_returns_false(_swap):
            return False

        verifier = _StubVerifier(verify_returns_false)
        result = _run(_verify_swallowing_only_recoverable(verifier, MagicMock()))
        assert result is False

    def test_provider_unreachable_is_returned_not_raised(self):
        sentinel = ProviderUnreachableError('blockstream timeout')

        async def verify_raises(_swap):
            raise sentinel

        verifier = _StubVerifier(verify_raises)
        result = _run(_verify_swallowing_only_recoverable(verifier, MagicMock()))
        assert result is sentinel

    def test_asyncio_timeout_is_returned_not_raised(self):
        async def verify_raises(_swap):
            raise asyncio.TimeoutError()

        verifier = _StubVerifier(verify_raises)
        result = _run(_verify_swallowing_only_recoverable(verifier, MagicMock()))
        assert isinstance(result, asyncio.TimeoutError)

    def test_attribute_error_propagates_unmasked(self):
        async def verify_raises(_swap):
            raise AttributeError("Swap object has no attribute 'unknown_field'")

        verifier = _StubVerifier(verify_raises)
        with pytest.raises(AttributeError):
            _run(_verify_swallowing_only_recoverable(verifier, MagicMock()))

    def test_key_error_propagates_unmasked(self):
        async def verify_raises(_swap):
            raise KeyError('missing_key')

        verifier = _StubVerifier(verify_raises)
        with pytest.raises(KeyError):
            _run(_verify_swallowing_only_recoverable(verifier, MagicMock()))

    def test_type_error_propagates_unmasked(self):
        async def verify_raises(_swap):
            raise TypeError("int() argument must be str, not 'NoneType'")

        verifier = _StubVerifier(verify_raises)
        with pytest.raises(TypeError):
            _run(_verify_swallowing_only_recoverable(verifier, MagicMock()))
