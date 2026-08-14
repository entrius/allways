"""Provider startup checks: chains the miner doesn't quote degrade to a warning; quoted (required)
chains still fail hard — a tao<->sol miner must start without BTC creds, a btc miner must not."""

import pytest

from allways import assets as cp


class _Boom:
    def __init__(self):
        raise RuntimeError('no creds')


class _Ok:
    @property
    def chain(self):
        return self

    def check_connection(self, require_send=True):
        pass

    def describe(self):
        return 'ok'


@pytest.fixture
def registry(monkeypatch):
    monkeypatch.setattr(cp, 'ASSET_REGISTRY', (('btc', _Boom, ()), ('sol', _Ok, ())))


def test_unrequired_failure_degrades_to_warning(registry):
    providers = cp.create_assets(check=True, required_chains={'sol'})
    assert 'sol' in providers
    assert 'btc' not in providers


def test_required_failure_still_raises(registry):
    with pytest.raises(RuntimeError, match='failed startup check'):
        cp.create_assets(check=True, required_chains={'btc', 'sol'})


def test_none_means_all_required(registry):
    with pytest.raises(RuntimeError, match='failed startup check'):
        cp.create_assets(check=True)


class _NoTestnet:
    def __init__(self):
        raise cp.MissingTestnetDeployment('No qnt token contract for network sepolia')


@pytest.fixture
def registry_with_missing_testnet(monkeypatch):
    monkeypatch.setattr(cp, 'ASSET_REGISTRY', (('qnt', _NoTestnet, ()), ('sol', _Ok, ())))


def test_missing_testnet_deployment_degrades_for_validators(registry_with_missing_testnet):
    # required_chains=None (validator: all chains required) must still boot — the spoke
    # simply doesn't exist on this network.
    providers = cp.create_assets(check=True)
    assert 'sol' in providers
    assert 'qnt' not in providers


def test_missing_testnet_deployment_still_fails_a_quoting_miner(registry_with_missing_testnet):
    with pytest.raises(RuntimeError, match='failed startup check'):
        cp.create_assets(check=True, required_chains={'qnt', 'sol'})


def test_evm_network_names_match_the_rpc_registry():
    """chains.py names the networks the CLI accepts; assets/evm.py names the chain ids the
    provider dials. One fact in two files, so CI compares them — for the row that declares the
    names. A row that declares none (ethusdc) rides the one that does and has nothing of its own."""
    for chain_id, cls, kwarg_names in cp.ASSET_REGISTRY:
        if kwarg_names:
            continue
        asset = cls()
        served = getattr(asset.chain, 'network_def', None)
        if served and asset.chain_def.networks:
            assert set(asset.chain_def.networks) == set(served.chain_ids), chain_id
