"""Avax unit tests — all offline (RPC layer mocked, signing is pure crypto).

The EVM behaviour itself is `EvmCoin`, proven by the Ether suite. What is AVAX-specific — and
what a wrong value here would cost — lives in the binding: which network the env selects, the
chain id every signed tx commits to, and the C-Chain's atomic P/X-chain imports, which credit a
balance with no EVM transaction and so must stay invisible to the deposit scanner.
"""

import pytest
from eth_account import Account

from allways.assets.asset import ProviderUnreachableError
from allways.assets.avax import Avax
from allways.assets.evm import AVALANCHE, EvmChain
from allways.assets.evm_coin import MAX_WALK_BLOCKS
from allways.chains import CHAIN_AVAX, get_chain_def
from allways.constants import LAUNCH_SPOKES

TEST_KEY = '0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80'  # hardhat account #0
RECIPIENT = '0x70997970C51812dc3A010C7d01b50e0d17dc79C8'
OTHER = '0x3C44CdDdB6a900fa2b585dd299e03d12FA4293BC'
TX = '0x' + 'ab' * 32
BLOCK_HASH = '0x' + 'cd' * 32

SEND_RESPONSES = {
    'eth_blockNumber': hex(100),
    'eth_getBlockByNumber': {'baseFeePerGas': hex(10**9)},
    'eth_maxPriorityFeePerGas': hex(10**9),
    'eth_getTransactionCount': '0x0',
    'eth_getBalance': hex(10**19),
    'eth_estimateGas': hex(21_000),
}


@pytest.fixture
def provider(monkeypatch):
    monkeypatch.setenv('AVAX_NETWORK', 'mainnet')
    monkeypatch.delenv('AVAX_RPC_URLS', raising=False)
    monkeypatch.delenv('AVAX_PRIVATE_KEY', raising=False)
    return Avax()


def rpc_stub(provider, responses: dict):
    """Replace eth_rpc with a method→response map. A callable value is invoked with params."""

    def fake_rpc(method, params, timeout=15, **kw):
        value = responses[method]
        return value(params) if callable(value) else value

    provider.chain.eth_rpc = fake_rpc
    return provider


def mined_responses(status='0x1', tip=1_000_002, value_hex=hex(10**18), to=RECIPIENT):
    return {
        'eth_getTransactionByHash': {
            'hash': TX,
            'from': '0x' + '11' * 20,
            'to': to,
            'value': value_hex,
            'blockNumber': '0xf4240',
            'blockHash': BLOCK_HASH,
        },
        'eth_getTransactionReceipt': {'status': status, 'blockNumber': '0xf4240', 'blockHash': BLOCK_HASH},
        'eth_blockNumber': hex(tip),
        'eth_getBlockByNumber': {'hash': BLOCK_HASH, 'timestamp': '0x64'},
    }


class TestRegistryRow:
    def test_is_a_launch_spoke_bound_to_its_registry_row(self, provider):
        assert provider.chain_def is CHAIN_AVAX is get_chain_def('avax')
        assert 'avax' in LAUNCH_SPOKES

    def test_native_coin_composes_its_chain(self, provider):
        # A native coin is an asset ON its network, not the network — same seam as a token,
        # so a second asset can land beside it without either claiming to be the chain.
        assert isinstance(provider.chain, EvmChain) and provider.chain is not provider
        assert provider.chain.env_prefix == CHAIN_AVAX.env_prefix

    def test_decimals_and_prefix(self):
        assert (CHAIN_AVAX.decimals, CHAIN_AVAX.env_prefix, CHAIN_AVAX.native_unit) == (18, 'AVAX', 'wei')

    def test_confirmations_stay_inside_the_program_fulfillment_grace(self):
        # An unlisted chain gets the program's 600s default grace; 2 × 1s blocks is far inside it.
        assert CHAIN_AVAX.min_confirmations * CHAIN_AVAX.seconds_per_block < 600


class TestNetworkSelection:
    def test_mainnet_chain_id(self, provider):
        assert provider.chain.chain_id == 43_114

    def test_fuji_chain_id(self, monkeypatch):
        monkeypatch.setenv('AVAX_NETWORK', 'fuji')
        assert Avax().chain.chain_id == 43_113

    def test_unknown_network_raises(self, monkeypatch):
        # A typo must never fall back to mainnet — that spends real AVAX against test swaps.
        # 'testnet' is the typo to beat here: Avalanche's testnet is named fuji.
        monkeypatch.setenv('AVAX_NETWORK', 'testnet')
        with pytest.raises(ValueError, match='AVAX_NETWORK'):
            Avax()

    def test_unset_network_defaults_to_mainnet(self, monkeypatch):
        monkeypatch.delenv('AVAX_NETWORK', raising=False)
        assert Avax().chain.network == 'mainnet'

    @pytest.mark.parametrize('network', tuple(AVALANCHE.chain_ids))
    def test_every_network_has_a_keyless_default_ladder(self, monkeypatch, network):
        monkeypatch.setenv('AVAX_NETWORK', network)
        assert Avax().chain.rpc_bases == list(AVALANCHE.rpc_urls[network])

    def test_rpc_urls_env_keeps_the_c_chain_path(self, monkeypatch):
        # Ava Labs' own gateways are path-bearing (/ext/bc/C/rpc) unlike every other EVM
        # endpoint in the registry; only a trailing slash may be trimmed.
        monkeypatch.setenv('AVAX_RPC_URLS', 'https://api.avax.network/ext/bc/C/rpc/,https://backup.example')
        assert Avax().chain.rpc_bases == ['https://api.avax.network/ext/bc/C/rpc', 'https://backup.example']

    def test_wrong_network_endpoint_fails_startup(self, monkeypatch):
        # Fuji configured, a mainnet endpoint answering: the ladder must be rejected outright,
        # never quietly used to verify legs against the wrong chain.
        monkeypatch.setenv('AVAX_NETWORK', 'fuji')
        p = rpc_stub(Avax(), {'eth_chainId': hex(43_114)})
        with pytest.raises(ConnectionError, match='expected 43113'):
            p.check_connection(require_send=False)


class TestVerification:
    def test_settled_transfer_matches(self, provider):
        rpc_stub(provider, mined_responses())
        info = provider.fetch_matching_tx(TX, RECIPIENT, 10**18)
        assert info is not None and info.confirmed and info.amount == 10**18
        assert info.block_time == 0x64

    def test_two_confirmations_are_enough(self, provider):
        rpc_stub(provider, mined_responses(tip=1_000_001))  # mined block + 1
        assert provider.fetch_matching_tx(TX, RECIPIENT, 10**18).confirmed

    def test_one_confirmation_is_not(self, provider):
        rpc_stub(provider, mined_responses(tip=1_000_000))
        assert provider.fetch_matching_tx(TX, RECIPIENT, 10**18).confirmed is False

    def test_mempool_tx_matches_but_is_unconfirmed(self, provider):
        responses = mined_responses()
        responses['eth_getTransactionByHash'] |= {'blockNumber': None, 'blockHash': None}
        info = rpc_stub(provider, responses).fetch_matching_tx(TX, RECIPIENT, 10**18)
        assert info is not None and info.confirmed is False and info.block_time is None

    def test_reverted_tx_rejected(self, provider):
        # Inclusion is not settlement: a reverted tx still carries intact to/value fields.
        rpc_stub(provider, mined_responses(status='0x0'))
        assert provider.fetch_matching_tx(TX, RECIPIENT, 10**18) is None

    def test_underpayment_rejected(self, provider):
        rpc_stub(provider, mined_responses(value_hex=hex(10**17)))
        assert provider.fetch_matching_tx(TX, RECIPIENT, 10**18) is None

    def test_wrong_recipient_rejected(self, provider):
        rpc_stub(provider, mined_responses(to=OTHER))
        assert provider.fetch_matching_tx(TX, RECIPIENT, 10**18) is None

    def test_absent_tx_is_absent(self, provider):
        rpc_stub(provider, {'eth_getTransactionByHash': None})
        assert provider.fetch_matching_tx(TX, RECIPIENT, 10**18) is None

    def test_unreadable_receipt_is_unknown_not_absent(self, provider):
        # 'unknown' must never read as 'no such payment' — that verdict slashes a paying miner.
        responses = mined_responses()
        responses['eth_getTransactionReceipt'] = None
        with pytest.raises(ProviderUnreachableError):
            rpc_stub(provider, responses).fetch_matching_tx(TX, RECIPIENT, 10**18)

    def test_unreadable_block_timestamp_is_unknown_not_absent(self, provider):
        # is_tx_fresh fails closed on a missing block_time, so a stale-looking dest leg
        # would ride to a TIMEOUT slash. Raise instead and let the caller retry.
        responses = mined_responses()
        responses['eth_getBlockByNumber'] = {'hash': BLOCK_HASH}
        with pytest.raises(ProviderUnreachableError):
            rpc_stub(provider, responses).fetch_matching_tx(TX, RECIPIENT, 10**18)


class TestSending:
    @pytest.fixture(autouse=True)
    def _key(self, provider, monkeypatch):
        monkeypatch.setenv('AVAX_PRIVATE_KEY', TEST_KEY)

    def test_broadcasts_a_c_chain_signed_transfer(self, provider):
        # Pins the whole signed payload: chain id 43114 (a tx signed for any other chain is simply
        # invalid here), EIP-1559 fees off the stubbed base fee, and the estimated gas + 20%.
        broadcast = {}

        def capture(params):
            broadcast['raw'] = params[0]
            return TX

        rpc_stub(provider, dict(SEND_RESPONSES, eth_sendRawTransaction=capture))
        assert provider.send_amount(RECIPIENT, 10**16) == (TX, 0)

        expected = Account.sign_transaction(
            {
                'chainId': 43_114,
                'nonce': 0,
                'to': RECIPIENT,
                'value': 10**16,
                'gas': 25_200,
                'maxFeePerGas': 3 * 10**9,
                'maxPriorityFeePerGas': 10**9,
            },
            TEST_KEY,
        )
        raw = getattr(expected, 'raw_transaction', None) or getattr(expected, 'rawTransaction')
        assert broadcast['raw'].removeprefix('0x') == raw.hex().removeprefix('0x')

    def test_no_key_refused(self, provider, monkeypatch):
        monkeypatch.delenv('AVAX_PRIVATE_KEY', raising=False)
        assert provider.send_amount(RECIPIENT, 10**16) is None
        assert 'AVAX_PRIVATE_KEY' in provider.last_send_error

    def test_key_mismatch_refused(self, provider):
        assert provider.send_amount(RECIPIENT, 10**16, from_address=RECIPIENT) is None
        assert 'key mismatch' in provider.last_send_error

    def test_insufficient_balance_refused(self, provider):
        rpc_stub(provider, dict(SEND_RESPONSES, eth_getBalance=hex(10**12)))
        assert provider.send_amount(RECIPIENT, 10**18) is None
        assert 'Insufficient AVAX' in provider.last_send_error


class TestDepositScanner:
    def test_walk_is_bounded_for_a_sub_second_chain(self, provider):
        # 5 min of ~1.1s blocks would be ~300 sequential eth_getBlockByNumber calls per first
        # scan. The walk bound is what keeps a public endpoint's rate budget intact.
        assert provider.SCAN_LOOKBACK_BLOCKS > MAX_WALK_BLOCKS

        scanned = []

        def block(params):
            scanned.append(int(params[0], 16))
            return {'transactions': []}

        rpc_stub(provider, {'eth_blockNumber': hex(10_000), 'eth_getBlockByNumber': block})
        assert provider.find_recent_outgoing(RECIPIENT, RECIPIENT, 1) is None
        assert len(scanned) == MAX_WALK_BLOCKS

    def test_cursor_parks_below_an_unreadable_block(self, provider):
        def boom(params):
            raise ConnectionError('down')

        rpc_stub(provider, {'eth_blockNumber': hex(10_000), 'eth_getBlockByNumber': boom})
        assert provider.find_recent_outgoing(RECIPIENT, RECIPIENT, 1) is None
        # Never leap a block the scan could not read — the deposit in it must stay reachable.
        assert provider.scan_cursors[(RECIPIENT.lower(), RECIPIENT.lower(), 1)] == 10_000 - MAX_WALK_BLOCKS

    def test_atomic_import_is_not_a_deposit(self, provider):
        # A P/X→C atomic import credits an EVM address from the block's extra data, producing no
        # EVM transaction: the payment is real but unverifiable, so the scanner must not claim it
        # (a claim needs a tx hash the confirm path can re-verify, and there is none).
        # The C-Chain's extra block fields must also not perturb the walk.
        atomic_block = {
            'transactions': [],
            'extDataGasUsed': '0x2bde',
            'extDataHash': '0x' + 'ef' * 32,
            'blockExtraData': '0x0000000000010000000000000001',
            'blockGasCost': '0x0',
            'timestampMilliseconds': '0x18f',
        }
        rpc_stub(provider, {'eth_blockNumber': hex(10_000), 'eth_getBlockByNumber': atomic_block})
        assert provider.find_recent_outgoing(OTHER, RECIPIENT, 1) is None
