"""Provider startup checks: chains the miner doesn't quote degrade to a warning; quoted (required)
chains still fail hard — a tao<->sol miner must start without BTC creds, a btc miner must not."""

import pytest

from allways import chain_providers as cp


class _Boom:
    def __init__(self):
        raise RuntimeError('no creds')


class _Ok:
    def check_connection(self, require_send=True):
        pass

    def describe(self):
        return 'ok'


@pytest.fixture
def registry(monkeypatch):
    monkeypatch.setattr(cp, 'PROVIDER_REGISTRY', (('btc', _Boom, ()), ('sol', _Ok, ())))


def test_unrequired_failure_degrades_to_warning(registry):
    providers = cp.create_chain_providers(check=True, required_chains={'sol'})
    assert 'sol' in providers
    assert 'btc' not in providers


def test_required_failure_still_raises(registry):
    with pytest.raises(RuntimeError, match='failed startup check'):
        cp.create_chain_providers(check=True, required_chains={'btc', 'sol'})


def test_none_means_all_required(registry):
    with pytest.raises(RuntimeError, match='failed startup check'):
        cp.create_chain_providers(check=True)
