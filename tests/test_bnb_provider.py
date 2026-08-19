"""Bnb unit tests — all offline (RPC layer mocked, signing is pure crypto).

The EVM behaviour itself is `EvmCoin`, proven by the Ether suite. What is BNB-specific — and
what a wrong value here would cost — lives in the binding: which network the env selects, the
chain id every signed tx commits to, the 15-block confirmation depth, and the sub-second block
time that drives the scanner. BSC's own hazard is a destination that stops accepting a native
transfer between reserve and payout (EIP-7702 delegation), so the delivery gates are pinned too.
"""

import pytest
from eth_account import Account

from allways.assets import evm_coin
from allways.assets.asset import ProviderUnreachableError
from allways.assets.bnb import Bnb
from allways.assets.evm import BSC, EvmChain, EvmRpcError
from allways.assets.evm_coin import MAX_WALK_BLOCKS
from allways.chains import CHAIN_BNB, get_chain_def
from allways.constants import LAUNCH_SPOKES

TEST_KEY = '0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80'  # hardhat account #0
RECIPIENT = '0x70997970C51812dc3A010C7d01b50e0d17dc79C8'
TX = '0x' + 'ab' * 32
BLOCK_HASH = '0x' + 'cd' * 32
DELEGATION = '0xef0100' + '11' * 20  # EIP-7702 delegation indicator
MINED_BLOCK = 0xF4240  # 1_000_000
CONFIRMED_TIP = MINED_BLOCK + CHAIN_BNB.min_confirmations - 1

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
    monkeypatch.setenv('BNB_NETWORK', 'mainnet')
    monkeypatch.delenv('BNB_RPC_URLS', raising=False)
    monkeypatch.delenv('BNB_PRIVATE_KEY', raising=False)
    return Bnb()


@pytest.fixture
def frozen_now(monkeypatch):
    """Pin the clock so delivery_refused's span arithmetic lands on exact block offsets."""
    now = 1_800_000_000
    monkeypatch.setattr(evm_coin.time, 'time', lambda: now)
    return now


def rpc_stub(provider, responses: dict):
    """Replace eth_rpc with a method→response map. A callable value is invoked with params."""

    def fake_rpc(method, params, timeout=15, **kw):
        value = responses[method]
        return value(params) if callable(value) else value

    provider.chain.eth_rpc = fake_rpc
    return provider


def mined_responses(status='0x1', tip=CONFIRMED_TIP, value_hex=hex(10**18)):
    return {
        'eth_getTransactionByHash': {
            'hash': TX,
            'from': '0x' + '11' * 20,
            # Lowercase on the wire against a checksummed expectation: EIP-55 is display-only,
            # and a case-sensitive compare would reject an honest miner's payout.
            'to': RECIPIENT.lower(),
            'value': value_hex,
            'blockNumber': hex(MINED_BLOCK),
            'blockHash': BLOCK_HASH,
        },
        'eth_getTransactionReceipt': {'status': status, 'blockNumber': hex(MINED_BLOCK), 'blockHash': BLOCK_HASH},
        'eth_blockNumber': hex(tip),
        'eth_getBlockByNumber': {'hash': BLOCK_HASH, 'timestamp': '0x64'},
    }


class TestRegistryRow:
    def test_is_a_launch_spoke_bound_to_its_registry_row(self, provider):
        assert provider.chain_def is CHAIN_BNB is get_chain_def('bnb')
        assert 'bnb' in LAUNCH_SPOKES

    def test_native_coin_composes_its_chain(self, provider):
        # A native coin is an asset ON its network, not the network — same seam as a token,
        # so a second BSC asset can land beside it without either claiming to be the chain.
        assert isinstance(provider.chain, EvmChain) and provider.chain is not provider
        assert provider.chain.env_prefix == CHAIN_BNB.env_prefix

    def test_decimals_and_prefix(self):
        assert (CHAIN_BNB.decimals, CHAIN_BNB.env_prefix, CHAIN_BNB.native_unit) == (18, 'BNB', 'wei')

    def test_confirmations_stay_inside_the_program_fulfillment_grace(self):
        # An unlisted chain gets the program's 600s default grace. 15 × the stored 1s is 15s;
        # real BSC blocks are 0.45s, so the true leg wait is ~6.8s — both far inside it.
        assert CHAIN_BNB.min_confirmations * CHAIN_BNB.seconds_per_block < 600


class TestNetworkSelection:
    def test_mainnet_chain_id(self, provider):
        assert provider.chain.chain_id == 56

    def test_testnet_chain_id(self, monkeypatch):
        monkeypatch.setenv('BNB_NETWORK', 'testnet')
        assert Bnb().chain.chain_id == 97

    def test_unknown_network_raises(self, monkeypatch):
        # A typo must never fall back to mainnet — that spends real BNB against test swaps.
        monkeypatch.setenv('BNB_NETWORK', 'chapel')
        with pytest.raises(ValueError, match='BNB_NETWORK'):
            Bnb()

    def test_unset_network_defaults_to_mainnet(self, monkeypatch):
        monkeypatch.delenv('BNB_NETWORK', raising=False)
        assert Bnb().chain.network == 'mainnet'

    @pytest.mark.parametrize('network', tuple(BSC.chain_ids))
    def test_every_network_has_a_keyless_default_ladder(self, monkeypatch, network):
        monkeypatch.setenv('BNB_NETWORK', network)
        assert Bnb().chain.rpc_bases == list(BSC.rpc_urls[network])

    def test_rpc_urls_env_overrides_the_public_ladder(self, monkeypatch):
        monkeypatch.setenv('BNB_RPC_URLS', 'https://paid.example/key/,https://backup.example')
        assert Bnb().chain.rpc_bases == ['https://paid.example/key', 'https://backup.example']

    def test_wrong_network_endpoint_fails_startup(self, monkeypatch):
        # Testnet configured, a mainnet endpoint answering: the ladder must be rejected outright,
        # never quietly used to verify legs against the wrong chain.
        monkeypatch.setenv('BNB_NETWORK', 'testnet')
        p = rpc_stub(Bnb(), {'eth_chainId': hex(56)})
        with pytest.raises(ConnectionError, match='expected 97'):
            p.check_connection(require_send=False)


class TestVerification:
    def test_settled_transfer_matches(self, provider):
        rpc_stub(provider, mined_responses())
        info = provider.fetch_matching_tx(TX, RECIPIENT, 10**18)
        assert info is not None and info.confirmed and info.amount == 10**18
        assert info.block_time == 0x64

    def test_fifteen_confirmations_are_enough(self, provider):
        rpc_stub(provider, mined_responses(tip=CONFIRMED_TIP))
        assert provider.fetch_matching_tx(TX, RECIPIENT, 10**18).confirmed

    def test_fourteen_confirmations_are_not(self, provider):
        rpc_stub(provider, mined_responses(tip=CONFIRMED_TIP - 1))
        assert provider.fetch_matching_tx(TX, RECIPIENT, 10**18).confirmed is False

    def test_reverted_tx_rejected(self, provider):
        # Inclusion is not settlement: a reverted tx still carries intact to/value fields.
        rpc_stub(provider, mined_responses(status='0x0'))
        assert provider.fetch_matching_tx(TX, RECIPIENT, 10**18) is None

    def test_underpayment_rejected(self, provider):
        rpc_stub(provider, mined_responses(value_hex=hex(10**17)))
        assert provider.fetch_matching_tx(TX, RECIPIENT, 10**18) is None

    def test_wrong_recipient_rejected(self, provider):
        rpc_stub(provider, mined_responses())
        assert provider.fetch_matching_tx(TX, '0x' + '22' * 20, 10**18) is None

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
        monkeypatch.setenv('BNB_PRIVATE_KEY', TEST_KEY)

    def test_broadcasts_a_bsc_signed_transfer(self, provider):
        # Pins the whole signed payload: chain id 56 (a tx signed for any other chain is simply
        # invalid here), EIP-1559 fees off the stubbed base fee, and the estimated gas + 20%.
        broadcast = {}

        def capture(params):
            broadcast['raw'] = params[0]
            return TX

        rpc_stub(provider, dict(SEND_RESPONSES, eth_sendRawTransaction=capture))
        assert provider.send_amount(RECIPIENT, 10**16) == (TX, 0)

        expected = Account.sign_transaction(
            {
                'chainId': 56,
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
        monkeypatch.delenv('BNB_PRIVATE_KEY', raising=False)
        assert provider.send_amount(RECIPIENT, 10**16) is None
        assert 'BNB_PRIVATE_KEY' in provider.last_send_error

    def test_key_mismatch_refused(self, provider):
        assert provider.send_amount(RECIPIENT, 10**16, from_address=RECIPIENT) is None
        assert 'key mismatch' in provider.last_send_error

    def test_insufficient_balance_refused(self, provider):
        rpc_stub(provider, dict(SEND_RESPONSES, eth_getBalance=hex(10**12)))
        assert provider.send_amount(RECIPIENT, 10**18) is None
        assert 'Insufficient BNB' in provider.last_send_error


class TestDeliveryGates:
    """A BSC dest can refuse a native transfer — a contract wallet, or an EOA that delegates to
    one via EIP-7702 *after* the reservation pinned it. The reserve gate keeps such a swap from
    starting; the slash gate keeps a miner who cannot pay from being punished for it."""

    def test_reserve_gate_admits_an_eoa_and_blocks_a_reverting_dest(self, provider):
        rpc_stub(provider, {'eth_getCode': '0x'})
        assert provider.can_deliver_to(RECIPIENT, 10**16) is True

        def refuse(params):
            raise EvmRpcError('execution reverted', {'code': 3, 'message': 'execution reverted'})

        rpc_stub(provider, {'eth_getCode': '0x60806040', 'eth_estimateGas': refuse})
        assert provider.can_deliver_to(RECIPIENT, 10**16) is False

    def test_reserve_gate_runs_the_send_paths_simulation(self, provider):
        # Sender-gated refusal (accepts from address(0), reverts for the committed miner) and an
        # estimate the +20% headroom pushes over MAX_TRANSFER_GAS must both bounce at reserve —
        # exactly what the miner's _transfer_gas would refuse to pay at delivery.
        miner = Account.from_key(TEST_KEY).address

        def gas(params):
            if params[0]['from'] == miner:
                raise EvmRpcError('execution reverted', {'code': 3, 'message': 'execution reverted'})
            return hex(90_000)  # 90k raw → 108k with headroom → over the 100k cap

        rpc_stub(provider, {'eth_getCode': '0x60806040', 'eth_estimateGas': gas})
        assert provider.can_deliver_to(RECIPIENT, 10**16, from_address=miner) is False
        assert provider.can_deliver_to(RECIPIENT, 10**16) is False

    def test_slash_gate_exempts_a_dest_that_gained_then_revoked_code(self, provider, frozen_now):
        # The case only temporal sampling can catch, and the one a miner gets slashed over: the
        # dest delegates via EIP-7702 after the reservation pinned it, the payout reverts, and the
        # delegation is revoked before the slash check. 'latest' is clean by then — only the
        # historical probes still see it. Code lives at the midpoint offset alone, so a passing
        # run proves all three probe offsets were actually read.
        probed = []

        def code_at(params):
            probed.append(params[1])
            return DELEGATION if params[1] == hex(9_933) else '0x'

        rpc_stub(provider, {'eth_blockNumber': hex(10_000), 'eth_getCode': code_at})
        assert provider.delivery_refused(RECIPIENT, frozen_now - 60) is True
        # The 60-second wall-time window is 134 blocks at BSC's 0.45-second rate.
        assert probed == ['latest', hex(9_866), hex(9_933)]

    def test_slash_gate_does_not_exempt_a_dest_that_never_had_code(self, provider, frozen_now):
        rpc_stub(provider, {'eth_blockNumber': hex(10_000), 'eth_getCode': '0x'})
        assert provider.delivery_refused(RECIPIENT, frozen_now - 60) is False

    def test_slash_gate_keeps_the_full_ten_minute_window(self, provider, frozen_now):
        probed = []
        rpc_stub(
            provider,
            {
                'eth_blockNumber': hex(10_000),
                'eth_getCode': lambda params: probed.append(params[1]) or '0x',
            },
        )
        assert provider.delivery_refused(RECIPIENT, frozen_now - 600) is False
        assert probed == ['latest', hex(8_666), hex(9_333)]


class TestDepositScanner:
    def test_walk_is_bounded_for_a_sub_second_chain(self, provider):
        # 5 min of BSC blocks is ~660 real blocks; the stored 1s block time already floors the
        # lookback at 300, and each one costs a sequential eth_getBlockByNumber. The walk bound
        # is what keeps a first scan inside a public endpoint's budget.
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
