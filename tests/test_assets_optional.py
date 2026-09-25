"""Provider startup checks: chains the miner doesn't quote degrade to a warning; quoted (required)
chains still fail hard — a tao<->sol miner must start without BTC creds, a btc miner must not."""

from functools import partial
from unittest.mock import MagicMock

import pytest

from allways import assets as cp
from allways.assets import tao as tao_module
from allways.constants import HUB_CHAINS


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


def test_validator_requires_hubs_only(monkeypatch):
    monkeypatch.setattr(cp, 'ASSET_REGISTRY', (('btc', _Boom, ()), ('sol', _Ok, ()), ('tao', _Ok, ())))
    providers = cp.create_assets(check=True, required_chains=set(HUB_CHAINS))
    assert set(providers) == {'sol', 'tao'}


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


def test_a_partial_bound_provider_degrades_by_its_class_name(monkeypatch):
    """Alpha rows are `partial(Alpha, chain_def)`, which carries no __name__: the failure branches
    formatted `cls.__name__` and raised AttributeError instead of the intended warning/RuntimeError,
    so an optional alpha that failed its check (a subtensor hiccup at boot) killed the neuron."""
    monkeypatch.setattr(cp, 'ASSET_REGISTRY', (('sn7', partial(_Boom), ()), ('sol', _Ok, ())))
    assert set(cp.create_assets(check=True, required_chains={'sol'})) == {'sol'}
    with pytest.raises(RuntimeError, match='_Boom failed startup check'):
        cp.create_assets(check=True, required_chains={'sn7', 'sol'})


# ─── one subtensor, one check ───────────────────────────────────────────────


@pytest.fixture
def tao_and_alphas(monkeypatch):
    """The real TAO row and every alpha row, on one mocked subtensor (the neurons pass one to create_assets)."""
    rows = tuple(spec for spec in cp.ASSET_REGISTRY if spec.chain_id == 'tao' or spec.asset_cls is cp.Alpha)
    monkeypatch.setattr(cp, 'ASSET_REGISTRY', rows)
    connected = []
    monkeypatch.setattr(tao_module.bt.logging, 'success', lambda msg, *a, **k: connected.append(msg))
    monkeypatch.setattr(cp.bt.logging, 'warning', lambda *a, **k: None)  # 128 "Alpha disabled" lines
    subtensor = MagicMock(name='subtensor')
    subtensor.get_current_block.return_value = 123
    subtensor.chain_endpoint = 'wss://entrypoint-finney.opentensor.ai:443'
    return rows, subtensor, connected


def test_every_alpha_shares_the_tao_subtensor_check(tao_and_alphas):
    """Each alpha used to re-read the head on the same subtensor: 1 + 128 chain_getHeader calls per boot, now 1."""
    rows, subtensor, connected = tao_and_alphas
    providers = cp.create_assets(check=True, require_send=False, required_chains={'tao'}, subtensor=subtensor)
    assert set(providers) == {spec.chain_id for spec in rows} and len(rows) > 100
    assert subtensor.get_current_block.call_count == 1
    assert connected == ['[Subtensor] connected: block=123']


def test_a_failed_shared_check_disables_every_alpha_without_re_asking(tao_and_alphas):
    _, subtensor, _ = tao_and_alphas
    subtensor.get_current_block.side_effect = RuntimeError('throttled')
    assert cp.create_assets(check=True, require_send=False, required_chains={'sol'}, subtensor=subtensor) == {}
    assert subtensor.get_current_block.call_count == 1
    with pytest.raises(RuntimeError, match='Tao failed startup check: Cannot reach Subtensor: throttled'):
        cp.create_assets(check=True, require_send=False, required_chains={'tao'}, subtensor=subtensor)
    with pytest.raises(RuntimeError, match='Alpha failed startup check: Cannot reach Subtensor: throttled'):
        cp.create_assets(check=True, require_send=False, required_chains={'sn7'}, subtensor=subtensor)


def test_each_alpha_still_checks_its_own_send_wallet(tao_and_alphas):
    _, subtensor, _ = tao_and_alphas
    providers = cp.create_assets(check=True, require_send=True, required_chains={'tao'}, subtensor=subtensor)
    assert set(providers) == {'tao'}, 'the chain check is shared; the wallet requirement is per alpha'
    with pytest.raises(RuntimeError, match='sn7 send requires a wallet'):
        cp.create_assets(check=True, require_send=True, required_chains={'sn7'}, subtensor=subtensor)
