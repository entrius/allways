"""Registry-wide proof that address validation is offline, rejects what cannot be paid, and that the
CLI gate builds every asset.

A destination that passes validation but cannot be paid (wrong network, a look-alike, a typo the
checksum would have caught) leaves an honest miner unable to deliver — so each chain family is run
through the cases that matter, against every registered asset of that family. Sockets are blocked
before any asset is constructed; no chain, no RPC."""

import socket
from types import SimpleNamespace
from unittest.mock import MagicMock

import bittensor as bt
import pytest
from scalecodec.utils.ss58 import ss58_encode
from solders.keypair import Keypair

from allways.assets import ASSET_REGISTRY
from allways.assets.tao import BETA_ESCROW
from allways.chains import get_chain_def
from allways.cli.swap_commands import helpers, swap

TAO_KEY = bt.Keypair.create_from_seed('0x' + '11' * 32)
TAO_ADDRESS = TAO_KEY.ss58_address
SOL_ADDRESS = str(Keypair.from_seed(bytes([1] * 32)).pubkey())
BTC_ADDRESS = 'bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh'
BTC_LEGACY = '1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa'
EVM_ADDRESS = '0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266'


def family_of(chain_id):
    if chain_id == 'btc':
        return 'btc'
    if chain_id == 'tao' or chain_id.startswith('sn'):
        return 'tao'  # alpha is held by a subtensor coldkey: TAO's address space
    if chain_id in ('sol', 'solusdc'):
        return 'sol'
    return 'evm'


# (address, valid?) — a valid form, then malformed, wrong-network, look-alike and case variants.
CASES = {
    'btc': (
        (BTC_ADDRESS, True),
        (BTC_ADDRESS.upper(), True),  # bech32 is case-insensitive as a whole...
        (BTC_ADDRESS[:10] + BTC_ADDRESS[10].upper() + BTC_ADDRESS[11:], False),  # ...but never mixed
        (BTC_LEGACY, True),
        (BTC_LEGACY[:-1] + 'b', False),  # base58check typo
        ('tb1qw508d6qejxtdg4y5r3zarvary0c5xw7kxpjzsx', False),  # testnet bech32 on mainnet
        ('mipcBbFg9gMiCh81Kj8tqqdgoZub1ZJRfn', False),  # testnet legacy on mainnet
        (BTC_ADDRESS + '\n', False),
        (' ' + BTC_ADDRESS, False),
        ('not-an-address', False),
    ),
    'tao': (
        (TAO_ADDRESS, True),
        (ss58_encode(TAO_KEY.public_key, 0), False),  # same key, Polkadot's network prefix
        (ss58_encode(TAO_KEY.public_key, 2), False),  # same key, Kusama's network prefix
        (BETA_ESCROW, False),  # a well-formed address nobody can be paid at
        (TAO_ADDRESS[:-1] + ('a' if TAO_ADDRESS[-1] != 'a' else 'b'), False),  # checksum typo
        (TAO_ADDRESS.lower(), False),
        (TAO_ADDRESS + '\n', False),
        ('not-an-address', False),
    ),
    'sol': (
        (SOL_ADDRESS, True),
        (SOL_ADDRESS[:-4], False),  # truncated
        ('0' + SOL_ADDRESS[1:], False),  # '0' is not in the base58 alphabet (look-alike of 'o'/'O')
        (SOL_ADDRESS.lower(), False),
        (SOL_ADDRESS + '\n', False),
        ('not-an-address', False),
    ),
    'evm': (
        (EVM_ADDRESS, True),
        (EVM_ADDRESS.lower(), True),  # EIP-55 is optional...
        (EVM_ADDRESS.replace('aad', 'aaD'), False),  # ...but mixed case must verify
        ('0x' + '00' * 20, False),  # a transfer() to zero reverts and no delivery gate catches it
        (EVM_ADDRESS[2:], False),  # no 0x
        ('0x1234', False),
        (EVM_ADDRESS.lower() + '\n', False),
        ('', False),
        (None, False),
    ),
}


def _constructor_kwargs(spec):
    available = {
        'solana_rpc_url': 'http://offline.invalid',
        'solana_keypair': MagicMock(),
        'subtensor': MagicMock(),
    }
    return {name: available[name] for name in spec.kwarg_names if name in available}


@pytest.fixture(autouse=True)
def offline_mainnet(monkeypatch):
    for spec in ASSET_REGISTRY:
        prefix = get_chain_def(spec.chain_id).env_prefix
        if prefix:
            monkeypatch.setenv(f'{prefix}_NETWORK', 'mainnet')

    def offline(*args, **kwargs):
        raise AssertionError('network access during address validation')

    monkeypatch.setattr(socket, 'socket', offline)
    monkeypatch.setattr(socket, 'create_connection', offline)
    monkeypatch.setattr(socket, 'getaddrinfo', offline)
    return offline


@pytest.mark.parametrize('family', sorted(CASES))
def test_every_asset_of_a_family_validates_its_addresses_offline(monkeypatch, offline_mainnet, family):
    specs = [spec for spec in ASSET_REGISTRY if family_of(spec.chain_id) == family]
    assert specs
    for spec in specs:
        chain = spec.cls(**_constructor_kwargs(spec)).chain
        for entry_point in ('eth_rpc', 'btc_api_get'):
            monkeypatch.setattr(chain, entry_point, offline_mainnet, raising=False)
        for address, valid in CASES[family]:
            assert chain.is_valid_address(address) is valid, (spec.chain_id, address)
    if family == 'evm':
        # Every comparison goes through the case-folded form: EIP-55 is display-only.
        assert chain.normalize_address(EVM_ADDRESS) == EVM_ADDRESS.lower()


def test_gate_provider_builds_every_registered_asset():
    client = MagicMock()
    client.rpc.url = 'http://offline.invalid'
    client.keypair = MagicMock()
    for spec in ASSET_REGISTRY:
        assert swap.gate_provider(spec.chain_id, client, lambda: MagicMock()) is not None, spec.chain_id


def test_gate_provider_reports_constructor_failure(monkeypatch):
    class Broken:
        def __init__(self):
            raise RuntimeError('cannot build')

    monkeypatch.setattr('allways.assets.ASSET_REGISTRY', (SimpleNamespace(chain_id='bad', cls=Broken, kwarg_names=()),))
    console = MagicMock()
    monkeypatch.setattr(helpers, 'console', console)

    assert swap.gate_provider('bad', MagicMock(), lambda: MagicMock()) is None
    console.print.assert_called_once_with('  [yellow]could not check BAD address here[/yellow]')
