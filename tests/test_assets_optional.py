"""Provider startup checks: chains the miner doesn't quote degrade to a warning; quoted (required)
chains still fail hard — a tao<->sol miner must start without BTC creds, a btc miner must not."""

import threading
from collections import Counter
from functools import partial
from unittest.mock import MagicMock

import pytest
import requests

from allways import assets as cp
from allways.assets import evm as evm_module
from allways.assets import tao as tao_module
from allways.chains import get_chain_def
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
    # None means every chain is required.
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


# ─── one EVM network, one probe ─────────────────────────────────────────────


@pytest.fixture
def evm_assets(monkeypatch):
    """Every real EVM row on its mainnet defaults. Each asset builds an EvmChain of its own, so the JSON-RPC
    transport is faked on the class: ``calls`` records (env prefix, method), a prefix in ``down`` is unreachable."""
    rows = tuple(spec for spec in cp.ASSET_REGISTRY if issubclass(spec.asset_cls, evm_module.EvmAsset))
    monkeypatch.setattr(cp, 'ASSET_REGISTRY', rows)
    hosted = Counter(get_chain_def(spec.chain_id).env_prefix for spec in rows)
    for prefix in hosted:
        monkeypatch.setenv(f'{prefix}_NETWORK', 'mainnet')
        monkeypatch.delenv(f'{prefix}_RPC_URLS', raising=False)
    answers = {'eth_blockNumber': '0x10', 'eth_getCode': '0x60', 'eth_call': '0x0'}
    calls, down = [], set()

    def fake_rpc(self, method, params, timeout=15, **kw):
        calls.append((self.env_prefix, method))
        if self.env_prefix in down:
            raise requests.ConnectionError('down')
        return hex(self.chain_id) if method == 'eth_chainId' else answers[method]

    monkeypatch.setattr(evm_module.EvmChain, 'eth_rpc', fake_rpc)
    connected, warned = [], []
    monkeypatch.setattr(evm_module.bt.logging, 'success', lambda msg, *a, **k: connected.append(msg))
    monkeypatch.setattr(evm_module.bt.logging, 'warning', lambda msg, *a, **k: warned.append(msg))
    return rows, hosted, calls, down, connected, warned


def test_an_asset_on_a_shared_probe_ends_up_initialised_as_when_probed_alone(evm_assets):
    """The probe only reads, so an asset that skips it must hold the state of one that ran it."""
    rows = evm_assets[0]

    def state(provider):
        chain = {k: v for k, v in vars(provider.chain).items() if k != 'http'}  # a Session per instance
        return {**{k: v for k, v in vars(provider).items() if k != '_chain'}, 'chain': chain}

    shared = cp.create_assets(check=True, require_send=False, required_chains=set())
    for spec in rows:
        alone = spec.cls()
        untouched = state(alone)
        alone.check_connection(require_send=False)
        assert state(alone) == untouched, f'{spec.chain_id}: the probe left state on the instance'
        assert state(shared[spec.chain_id]) == untouched, spec.chain_id
        assert shared[spec.chain_id].describe() == alone.describe()


def test_a_failed_shared_probe_fails_every_asset_on_that_network_without_re_asking(evm_assets):
    rows, _, calls, down, _, warned = evm_assets
    down.add('ETH')
    on_ethereum = [spec for spec in rows if get_chain_def(spec.chain_id).env_prefix == 'ETH']
    providers = cp.create_assets(check=True, require_send=False, required_chains=set())
    assert list(providers) == [spec.chain_id for spec in rows if spec not in on_ethereum]
    ladder = len(evm_module.ETHEREUM.rpc_urls['mainnet'])
    assert Counter(calls)[('ETH', 'eth_chainId')] == ladder and ('ETH', 'eth_blockNumber') not in calls
    assert [line for line in warned if 'disabled' in line] == [
        f'{spec.asset_cls.__name__} disabled: Cannot reach any Ethereum RPC'
        f' — {spec.chain_id}-pair swaps cannot be fulfilled until this is fixed'
        for spec in on_ethereum
    ]
    with pytest.raises(RuntimeError, match='Ether failed startup check: Cannot reach any Ethereum RPC'):
        cp.create_assets(check=True, require_send=False, required_chains={'eth'})
    with pytest.raises(RuntimeError, match='EthUsdc failed startup check: Cannot reach any Ethereum RPC'):
        cp.create_assets(check=True, require_send=False, required_chains={'ethusdc'})


# ─── checks run per backend; the boot fails on the first row in registry order ─


def lanes(count, check):
    """``count`` provider classes whose check is ``check(provider)``. A lane is a chain class, so ``count`` lanes."""
    return [
        type(f'_Lane{i}', (_Ok,), {'check_connection': lambda self, require_send=True: check(self)})
        for i in range(count)
    ]


def test_the_first_failure_in_registry_order_fails_the_boot_whichever_lane_fails_first(monkeypatch):
    later_failed = threading.Event()

    def check(provider):
        if type(provider) is first:
            assert later_failed.wait(timeout=5)  # the registry's first row is the LAST to fail
            raise ConnectionError('first row')
        later_failed.set()
        raise ConnectionError('second row')

    first, second = lanes(2, check)
    monkeypatch.setattr(cp, 'ASSET_REGISTRY', (('btc', first, ()), ('sol', second, ())))
    with pytest.raises(RuntimeError, match='_Lane0 failed startup check: first row'):
        cp.create_assets(check=True)
    with pytest.raises(RuntimeError, match='_Lane1 failed startup check: second row'):
        cp.create_assets(check=True, required_chains={'sol'})
