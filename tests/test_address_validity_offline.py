"""Registry-wide proof that address validation is offline and the CLI gate builds every asset.

Blocks sockets before constructing each registered asset; no chain, no RPC."""

import socket
from types import SimpleNamespace
from unittest.mock import MagicMock

import bittensor as bt
import pytest
from solders.keypair import Keypair

from allways.assets import ASSET_REGISTRY
from allways.chains import get_chain_def
from allways.cli.swap_commands import swap

BTC_ADDRESS = 'bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh'
TAO_ADDRESS = bt.Keypair.create_from_seed('0x' + '11' * 32).ss58_address
SOL_ADDRESS = str(Keypair().pubkey())
EVM_ADDRESS = '0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266'


def _good_address(chain_id):
    if chain_id == 'btc':
        return BTC_ADDRESS
    if chain_id == 'tao':
        return TAO_ADDRESS
    if chain_id in ('sol', 'solusdc'):
        return SOL_ADDRESS
    return EVM_ADDRESS


def _constructor_kwargs(spec):
    available = {
        'solana_rpc_url': 'http://offline.invalid',
        'solana_keypair': MagicMock(),
        'subtensor': MagicMock(),
    }
    return {name: available[name] for name in spec.kwarg_names if name in available}


@pytest.fixture(autouse=True)
def stable_network_env(monkeypatch):
    for spec in ASSET_REGISTRY:
        prefix = get_chain_def(spec.chain_id).env_prefix
        if prefix:
            monkeypatch.setenv(f'{prefix}_NETWORK', 'mainnet')


@pytest.mark.parametrize('spec', ASSET_REGISTRY, ids=lambda spec: spec.chain_id)
def test_registered_asset_address_validation_is_offline(monkeypatch, spec):
    def offline(*args, **kwargs):
        raise AssertionError('network access during address validation')

    monkeypatch.setattr(socket, 'socket', offline)
    monkeypatch.setattr(socket, 'create_connection', offline)
    monkeypatch.setattr(socket, 'getaddrinfo', offline)
    provider = spec.cls(**_constructor_kwargs(spec))
    chain = provider.chain
    for entry_point in ('eth_rpc', 'btc_api_get'):
        monkeypatch.setattr(chain, entry_point, offline, raising=False)

    good = chain.is_valid_address(_good_address(spec.chain_id))
    bad = chain.is_valid_address('not-an-address')
    assert good is True
    assert bad is False


@pytest.mark.parametrize('spec', ASSET_REGISTRY, ids=lambda spec: spec.chain_id)
def test_gate_provider_builds_every_registered_asset(spec):
    client = MagicMock()
    client.rpc.url = 'http://offline.invalid'
    client.keypair = MagicMock()
    assert swap._gate_provider(spec.chain_id, client, {}, lambda: MagicMock()) is not None


def test_gate_provider_reports_constructor_failure(monkeypatch):
    class Broken:
        def __init__(self):
            raise RuntimeError('cannot build')

    monkeypatch.setattr('allways.assets.ASSET_REGISTRY', (SimpleNamespace(chain_id='bad', cls=Broken, kwarg_names=()),))
    console = MagicMock()
    monkeypatch.setattr(swap, 'console', console)

    assert swap._gate_provider('bad', MagicMock(), {}, lambda: MagicMock()) is None
    console.print.assert_called_once_with('  [yellow]could not check BAD address here[/yellow]')


def test_non_tao_gate_provider_does_not_get_subtensor():
    getter = MagicMock(side_effect=AssertionError('subtensor requested for a non-tao chain'))
    swap._gate_provider('btc', MagicMock(), {}, getter)
    getter.assert_not_called()
