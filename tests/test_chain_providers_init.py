"""Tests for allways.chain_providers.create_chain_providers registry factory."""

from unittest.mock import MagicMock, patch

import pytest

from allways.chain_providers import create_chain_providers


class _FakeProvider:
    """Test provider that records constructor kwargs and controls check behavior."""

    instances: list = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.checked_require_send = None
        _FakeProvider.instances.append(self)

    def check_connection(self, require_send: bool = True) -> None:
        self.checked_require_send = require_send


class _FailingProvider:
    def __init__(self, **kwargs):
        raise RuntimeError('init failure')


class _CheckFailingProvider:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def check_connection(self, require_send: bool = True) -> None:
        raise RuntimeError('check failure')


@pytest.fixture(autouse=True)
def reset_fake():
    _FakeProvider.instances = []


class TestRegistryInstantiation:
    def test_instantiates_each_registered_provider(self):
        reg = (
            ('chain-a', _FakeProvider, ()),
            ('chain-b', _FakeProvider, ()),
        )
        with patch('allways.chain_providers.PROVIDER_REGISTRY', reg):
            providers = create_chain_providers()
        assert set(providers.keys()) == {'chain-a', 'chain-b'}

    def test_forwards_only_declared_kwargs(self):
        reg = (('chain-a', _FakeProvider, ('subtensor',)),)
        with patch('allways.chain_providers.PROVIDER_REGISTRY', reg):
            subtensor = MagicMock()
            wallet = MagicMock()
            providers = create_chain_providers(subtensor=subtensor, wallet=wallet)
        assert providers['chain-a'].kwargs == {'subtensor': subtensor}

    def test_missing_kwarg_not_passed(self):
        reg = (('chain-a', _FakeProvider, ('subtensor',)),)
        with patch('allways.chain_providers.PROVIDER_REGISTRY', reg):
            providers = create_chain_providers()
        assert providers['chain-a'].kwargs == {}


class TestCheckConnection:
    def test_check_true_invokes_check_connection(self):
        reg = (('chain-a', _FakeProvider, ()),)
        with patch('allways.chain_providers.PROVIDER_REGISTRY', reg):
            providers = create_chain_providers(check=True)
        assert providers['chain-a'].checked_require_send is True

    def test_require_send_false_propagates(self):
        reg = (('chain-a', _FakeProvider, ()),)
        with patch('allways.chain_providers.PROVIDER_REGISTRY', reg):
            providers = create_chain_providers(check=True, require_send=False)
        assert providers['chain-a'].checked_require_send is False

    def test_check_false_skips_check(self):
        reg = (('chain-a', _FakeProvider, ()),)
        with patch('allways.chain_providers.PROVIDER_REGISTRY', reg):
            providers = create_chain_providers(check=False)
        assert providers['chain-a'].checked_require_send is None


class TestFailureHandling:
    def test_check_true_raises_on_init_failure(self):
        reg = (('chain-a', _FailingProvider, ()),)
        with patch('allways.chain_providers.PROVIDER_REGISTRY', reg):
            with pytest.raises(RuntimeError, match='failed startup check'):
                create_chain_providers(check=True)

    def test_check_true_raises_on_check_failure(self):
        reg = (('chain-a', _CheckFailingProvider, ()),)
        with patch('allways.chain_providers.PROVIDER_REGISTRY', reg):
            with pytest.raises(RuntimeError, match='failed startup check'):
                create_chain_providers(check=True)

    def test_check_false_swallows_init_failure(self):
        reg = (
            ('chain-a', _FailingProvider, ()),
            ('chain-b', _FakeProvider, ()),
        )
        with patch('allways.chain_providers.PROVIDER_REGISTRY', reg):
            providers = create_chain_providers(check=False)
        assert 'chain-a' not in providers
        assert 'chain-b' in providers
