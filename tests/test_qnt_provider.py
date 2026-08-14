"""Qnt unit tests — all offline (RPC layer mocked, signing is pure crypto).

Generic ERC-20 behaviour is proven by the arbusdc and ethusdc suites; this covers what is
qnt-specific. Two things: it is the third asset on Ethereum's env identity, and it declares
``refusal_checks=()`` — QNT implements neither isBlacklisted nor paused, and
probing them REVERTS, which the slash gate would read as "defer" forever.

Everything here builds on mainnet. On Sepolia the row pins Quant's official test QNT
(dispensed by its documented getTestQNT faucet) in TESTNET_TOKEN_CONTRACTS, with the
QNT_TOKEN_CONTRACT override still available for pointing elsewhere.
"""

import pytest

from allways.assets.asset import ProviderUnreachableError
from allways.assets.erc20 import SEL_TRANSFER, TESTNET_TOKEN_CONTRACTS, TRANSFER_TOPIC0
from allways.assets.eth import Ether
from allways.assets.evm import EvmRpcError
from allways.assets.qnt import Qnt
from allways.chains import CHAIN_ETH, CHAIN_QNT, get_chain_def
from allways.constants import LAUNCH_SPOKES

TEST_KEY = '0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80'  # hardhat account #0
SENDER = '0x' + '11' * 20
RECIPIENT = '0x70997970C51812dc3A010C7d01b50e0d17dc79C8'
CONTRACT = CHAIN_QNT.asset_locator
COUNTERFEIT = '0x' + '99' * 20
TX = '0x' + 'ab' * 32
BLOCK_HASH = '0x' + 'cd' * 32
AMOUNT = 5 * 10**18  # 5 QNT
MAINNET_ID = 1
SEPOLIA_ID = 11_155_111


@pytest.fixture
def provider(monkeypatch):
    monkeypatch.setenv('ETH_NETWORK', 'mainnet')
    for var in ('ETH_RPC_URLS', 'ETH_PRIVATE_KEY', 'QNT_TOKEN_CONTRACT'):
        monkeypatch.delenv(var, raising=False)
    return Qnt()


def rpc_stub(provider, responses: dict):
    """Replace eth_rpc with a method→response map. A callable value is invoked with params."""

    def fake_rpc(method, params, timeout=15, **kw):
        value = responses[method]
        return value(params) if callable(value) else value

    provider.chain.eth_rpc = fake_rpc  # the asset talks through its chain (composed seam)
    return provider


def no_rpc(provider):
    """Any RPC call fails the test: a declared 'none' surface must answer from the registry."""

    def boom(method, params, timeout=15, **kw):
        raise AssertionError(f'unexpected RPC {method} — QNT reverts on issuer probes')

    provider.chain.eth_rpc = boom
    return provider


def topic(address: str) -> str:
    return '0x' + '00' * 12 + address.lower().removeprefix('0x')


def transfer_log(contract=CONTRACT, sender=SENDER, recipient=RECIPIENT, amount=AMOUNT) -> dict:
    return {
        'address': contract,
        'topics': [TRANSFER_TOPIC0, topic(sender), topic(recipient)],
        'data': hex(amount),
        'transactionHash': TX,
    }


def mined(logs=None, status='0x1', tip=1_000_100, block=None) -> dict:
    return {
        'eth_getTransactionByHash': {'hash': TX, 'blockNumber': '0xf4240', 'blockHash': BLOCK_HASH},
        'eth_getTransactionReceipt': {
            'status': status,
            'blockNumber': '0xf4240',
            'blockHash': BLOCK_HASH,
            'logs': [transfer_log()] if logs is None else logs,
        },
        'eth_blockNumber': hex(tip),
        'eth_getBlockByNumber': {'hash': BLOCK_HASH, 'timestamp': '0x64'} if block is None else block,
    }


class TestSharedEnvIdentity:
    """qnt names the NETWORK, not the asset — the third asset on one ETH_NETWORK."""

    def test_row_is_a_launch_spoke_on_ethereums_identity(self, provider):
        assert provider.chain_def is CHAIN_QNT is get_chain_def('qnt')
        assert 'qnt' in LAUNCH_SPOKES
        assert (CHAIN_QNT.env_prefix, CHAIN_QNT.host_chain) == (CHAIN_ETH.env_prefix, CHAIN_ETH.host_chain)
        # CHAIN_ETH owns ETH_NETWORK; a second declaration renders a duplicate CLI row.
        assert CHAIN_QNT.networks == ()

    def test_one_eth_network_moves_both_assets(self, provider):
        assert provider.chain.chain_id == Ether().chain.chain_id == MAINNET_ID
        assert provider.token_contract == CHAIN_QNT.asset_locator

    def test_env_override_wins(self, monkeypatch):
        monkeypatch.setenv('QNT_TOKEN_CONTRACT', '0x' + '42' * 20)
        assert Qnt().token_contract == '0x' + '42' * 20

    def test_unknown_network_raises(self, monkeypatch):
        monkeypatch.setenv('ETH_NETWORK', 'seplia')
        with pytest.raises(ValueError, match='ETH_NETWORK'):
            Qnt()

    def test_finality_matches_the_chain_it_shares(self):
        # Two assets on one chain must not disagree about its reorg depth, and the implied
        # 384s leg stays inside the program's 600s default fulfillment grace.
        assert (CHAIN_QNT.min_confirmations, CHAIN_QNT.seconds_per_block, CHAIN_QNT.replay_grace_secs) == (
            CHAIN_ETH.min_confirmations,
            CHAIN_ETH.seconds_per_block,
            CHAIN_ETH.replay_grace_secs,
        )
        assert CHAIN_QNT.min_confirmations * CHAIN_QNT.seconds_per_block < 600

    def test_wrong_network_endpoint_fails_startup(self, provider):
        # Mainnet configured, a Sepolia endpoint answering: reject outright rather than
        # quietly verify legs against the wrong chain.
        rpc_stub(provider, {'eth_chainId': hex(SEPOLIA_ID)})
        with pytest.raises(ConnectionError, match=f'expected {MAINNET_ID}'):
            provider.check_connection(require_send=False)

    def test_codeless_contract_fails_startup(self, provider):
        rpc_stub(provider, {'eth_chainId': hex(MAINNET_ID), 'eth_blockNumber': '0x10', 'eth_getCode': '0x'})
        with pytest.raises(ConnectionError, match='no code'):
            provider.check_connection(require_send=False)


class TestTestnetDeployment:
    """The Sepolia pin is Quant's own test QNT — a verified plain ERC20 (immutable, no pause
    or blacklist; probing reverts like mainnet), so the row's declared surface stays true on
    testnet. A spoke without such a pin (e.g. paxg) degrades via MissingTestnetDeployment
    instead of failing the neuron's boot."""

    def test_sepolia_pin_builds_the_provider(self, monkeypatch):
        monkeypatch.setenv('ETH_NETWORK', 'sepolia')
        monkeypatch.delenv('QNT_TOKEN_CONTRACT', raising=False)
        assert Qnt().token_contract == TESTNET_TOKEN_CONTRACTS['qnt']['sepolia']

    def test_override_restores_the_shared_network_identity(self, monkeypatch):
        # What pinning a test token buys: the same one-ETH_NETWORK guarantee, on Sepolia.
        monkeypatch.setenv('ETH_NETWORK', 'sepolia')
        monkeypatch.setenv('QNT_TOKEN_CONTRACT', '0x' + '42' * 20)
        assert Qnt().chain.chain_id == Ether().chain.chain_id == SEPOLIA_ID


class TestDeclaredUnfreezable:
    """QNT reverts on isBlacklisted/paused. Probing them raises EvmRpcError, delivery_refused
    propagates it, and the caller defers the slash — forever. The row declares the surface
    instead, so both gates answer with ZERO RPC calls."""

    def test_reserve_gate_passes_without_touching_the_chain(self, provider):
        assert no_rpc(provider).can_deliver_to(RECIPIENT, AMOUNT) is True

    def test_slash_gate_never_refuses_and_never_probes(self, provider):
        assert no_rpc(provider).delivery_refused(RECIPIENT, 0) is False

    def test_boot_falsifier_accepts_the_real_declaration(self, provider):
        # Both probes reverting is the declaration holding — what QNT does live on mainnet.
        # An ANSWER from either fails boot; that direction is proven in the PR-0 suite.
        def reverts(_params):
            raise EvmRpcError('execution reverted', {'code': 3, 'message': 'execution reverted'})

        rpc_stub(
            provider,
            {'eth_chainId': hex(MAINNET_ID), 'eth_blockNumber': '0x10', 'eth_getCode': '0xbeef', 'eth_call': reverts},
        )
        provider.check_connection(require_send=False)


class TestVerification:
    def test_settled_transfer_matches_mixed_case_with_sender_pinned(self, provider):
        # Expecteds arrive in EIP-55 casing while the RPC answers lowercase — comparison
        # normalizes, and the on-chain sender is pinned to the reserved address.
        rpc_stub(provider, mined())
        info = provider.verify_transaction(TX, RECIPIENT, AMOUNT, expected_sender=SENDER.upper())
        assert info.confirmed and info.amount == AMOUNT and info.block_time == 0x64
        assert info.sender == SENDER.lower()

    def test_wrong_sender_rejected(self, provider):
        rpc_stub(provider, mined())
        assert provider.verify_transaction(TX, RECIPIENT, AMOUNT, expected_sender=COUNTERFEIT) is None

    def test_below_min_confs_unconfirmed(self, provider):
        rpc_stub(provider, mined(tip=1_000_030))  # 31 confs of the 32 required
        assert provider.fetch_matching_tx(TX, RECIPIENT.lower(), AMOUNT).confirmed is False

    def test_reverted_rejected(self, provider):
        # Inclusion is not settlement: a reverted tx moved no funds.
        rpc_stub(provider, mined(status='0x0'))
        assert provider.fetch_matching_tx(TX, RECIPIENT.lower(), AMOUNT) is None

    def test_underpay_rejected(self, provider):
        rpc_stub(provider, mined(logs=[transfer_log(amount=AMOUNT - 1)]))
        assert provider.fetch_matching_tx(TX, RECIPIENT.lower(), AMOUNT) is None

    def test_wrong_recipient_rejected(self, provider):
        rpc_stub(provider, mined(logs=[transfer_log(recipient=SENDER)]))
        assert provider.fetch_matching_tx(TX, RECIPIENT.lower(), AMOUNT) is None

    def test_pending_transfer_matches_off_calldata(self, provider):
        calldata = SEL_TRANSFER + topic(RECIPIENT).removeprefix('0x') + f'{AMOUNT:064x}'
        pending = {'hash': TX, 'from': SENDER, 'to': CONTRACT, 'input': calldata, 'blockNumber': None}
        rpc_stub(provider, {'eth_getTransactionByHash': pending})
        info = provider.fetch_matching_tx(TX, RECIPIENT.lower(), AMOUNT)
        assert info.confirmed is False and info.sender == SENDER.lower() and info.amount == AMOUNT

    def test_absent_tx_is_absent(self, provider):
        rpc_stub(provider, {'eth_getTransactionByHash': None})
        assert provider.fetch_matching_tx(TX, RECIPIENT.lower(), AMOUNT) is None

    @pytest.mark.parametrize('gap', ('eth_getTransactionReceipt', 'eth_getBlockByNumber'))
    def test_unreadable_settlement_is_unknown_not_absent(self, provider, gap):
        # 'unknown' must never read as 'no such payment' — that verdict slashes a paying miner.
        blind = dict(mined())
        blind[gap] = None if gap == 'eth_getTransactionReceipt' else {'hash': BLOCK_HASH}
        with pytest.raises(ProviderUnreachableError):
            rpc_stub(provider, blind).fetch_matching_tx(TX, RECIPIENT.lower(), AMOUNT)

    def test_counterfeit_token_log_never_matches_even_on_the_cache_path(self, provider):
        """A fake token emitting a well-formed Transfer must not settle the leg, and must not
        settle it on the re-verify either: the cache is only written from a pinned-contract match."""
        rpc_stub(provider, mined(logs=[transfer_log(contract=COUNTERFEIT)]))
        assert provider.fetch_matching_tx(TX, RECIPIENT.lower(), AMOUNT) is None
        assert provider._settled_cache == {}
        assert provider.fetch_matching_tx(TX, RECIPIENT.lower(), AMOUNT) is None


class TestSendGuards:
    SEND = {
        'eth_blockNumber': hex(100),
        'eth_getBlockByNumber': {'baseFeePerGas': hex(10**9)},
        'eth_maxPriorityFeePerGas': hex(10**9),
        'eth_getTransactionCount': '0x0',
        'eth_getBalance': hex(10**18),
        'eth_estimateGas': hex(65_000),
        'eth_call': f'0x{AMOUNT:064x}',  # balanceOf
    }

    def test_no_key_refused(self, provider):
        assert provider.send_amount(RECIPIENT, AMOUNT) is None
        assert 'ETH_PRIVATE_KEY' in provider.last_send_error

    def test_key_mismatch_refused(self, provider, monkeypatch):
        monkeypatch.setenv('ETH_PRIVATE_KEY', TEST_KEY)
        assert provider.send_amount(RECIPIENT, AMOUNT, from_address=RECIPIENT) is None
        assert 'key mismatch' in provider.last_send_error

    def test_insufficient_token_balance_refused(self, provider, monkeypatch):
        monkeypatch.setenv('ETH_PRIVATE_KEY', TEST_KEY)
        rpc_stub(provider, dict(self.SEND, eth_call=f'0x{AMOUNT - 1:064x}'))
        assert provider.send_amount(RECIPIENT, AMOUNT) is None
        assert 'insufficient balance' in provider.last_send_error

    def test_gas_poor_miner_refuses_before_broadcasting(self, provider, monkeypatch):
        # QNT-rich but ETH-poor is the ordinary state of a high-unit-value token holder:
        # refuse here rather than burn a revert on L1.
        monkeypatch.setenv('ETH_PRIVATE_KEY', TEST_KEY)
        rpc_stub(provider, dict(self.SEND, eth_getBalance=hex(10**9)))
        assert provider.send_amount(RECIPIENT, AMOUNT) is None
        assert 'Insufficient gas balance' in provider.last_send_error

    def test_broadcast_returns_a_hash(self, provider, monkeypatch):
        monkeypatch.setenv('ETH_PRIVATE_KEY', TEST_KEY)
        rpc_stub(provider, dict(self.SEND, eth_sendRawTransaction=TX))
        assert provider.send_amount(RECIPIENT, AMOUNT) == (TX, 0)


class TestDepositScanner:
    def test_getlogs_hit_is_bounded_to_the_lookback_window(self, provider):
        seen = {}

        def logs(params):
            seen.update(params[0])
            return [transfer_log()]

        rpc_stub(provider, {'eth_blockNumber': hex(10_000), 'eth_getLogs': logs})
        assert provider.find_recent_outgoing(SENDER, RECIPIENT, AMOUNT) == TX
        assert int(seen['fromBlock'], 16) == 10_000 - provider.SCAN_LOOKBACK_BLOCKS + 1
        assert seen['address'] == CONTRACT

    def test_failed_range_parks_the_cursor(self, provider):
        def boom(params):
            raise ConnectionError('down')

        rpc_stub(provider, {'eth_blockNumber': hex(10_000), 'eth_getLogs': boom})
        assert provider.find_recent_outgoing(SENDER, RECIPIENT, AMOUNT) is None
        # Never leap a range the scan could not read — the deposit in it must stay reachable.
        key = (SENDER.lower(), RECIPIENT.lower(), AMOUNT)
        assert provider.scan_cursors[key] == 10_000 - provider.SCAN_LOOKBACK_BLOCKS
