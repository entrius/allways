"""EthUsdc unit tests — all offline (RPC layer mocked, signing is pure crypto).

Generic ERC-20 behaviour is proven by the arbusdc suite; this covers what is ethusdc-specific.
Chiefly the shared env identity: ethusdc is the first asset to land on an already-configured
network, so ONE ETH_NETWORK moves both it and native ETH. A second prefix here would leave
ethusdc reading an unset var, silently defaulting to mainnet, and a testnet miner would pay
real USDC against test swaps.
"""

import pytest

from allways.assets.asset import ProviderUnreachableError
from allways.assets.erc20 import SEL_IS_BLACKLISTED, SEL_TRANSFER, TESTNET_TOKEN_CONTRACTS, TRANSFER_TOPIC0
from allways.assets.eth import Ether
from allways.assets.ethusdc import EthUsdc
from allways.chains import CHAIN_ETH, CHAIN_ETHUSDC, get_chain_def
from allways.constants import LAUNCH_SPOKES

TEST_KEY = '0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80'  # hardhat account #0
SENDER = '0x' + '11' * 20
RECIPIENT = '0x70997970C51812dc3A010C7d01b50e0d17dc79C8'
CONTRACT = TESTNET_TOKEN_CONTRACTS['ethusdc']['sepolia']
COUNTERFEIT = '0x' + '99' * 20
TX = '0x' + 'ab' * 32
BLOCK_HASH = '0x' + 'cd' * 32
AMOUNT = 150_000_000  # 150 USDC in µUSDC
SEPOLIA_ID = 11_155_111


@pytest.fixture
def provider(monkeypatch):
    monkeypatch.setenv('ETH_NETWORK', 'sepolia')
    for var in ('ETH_RPC_URLS', 'ETH_PRIVATE_KEY', 'ETHUSDC_TOKEN_CONTRACT'):
        monkeypatch.delenv(var, raising=False)
    return EthUsdc()


def rpc_stub(provider, responses: dict):
    """Replace eth_rpc with a method→response map. A callable value is invoked with params."""

    def fake_rpc(method, params, timeout=15, **kw):
        value = responses[method]
        return value(params) if callable(value) else value

    provider.chain.eth_rpc = fake_rpc  # the asset talks through its chain (composed seam)
    return provider


def topic(address: str) -> str:
    return '0x' + '00' * 12 + address.lower().removeprefix('0x')


def transfer_log(contract=CONTRACT, recipient=RECIPIENT, amount=AMOUNT) -> dict:
    return {
        'address': contract,
        'topics': [TRANSFER_TOPIC0, topic(SENDER), topic(recipient)],
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
    """The whole point of this row: ethusdc names the NETWORK, not the asset."""

    def test_row_is_a_launch_spoke_on_ethereums_identity(self, provider):
        assert provider.chain_def is CHAIN_ETHUSDC is get_chain_def('ethusdc')
        assert 'ethusdc' in LAUNCH_SPOKES
        assert (CHAIN_ETHUSDC.env_prefix, CHAIN_ETHUSDC.host_chain) == (CHAIN_ETH.env_prefix, CHAIN_ETH.host_chain)
        # CHAIN_ETH owns ETH_NETWORK; a second declaration renders a duplicate CLI row.
        assert CHAIN_ETHUSDC.networks == ()

    @pytest.mark.parametrize('network,chain_id', (('mainnet', 1), ('sepolia', SEPOLIA_ID)))
    def test_one_eth_network_moves_both_assets(self, monkeypatch, network, chain_id):
        monkeypatch.setenv('ETH_NETWORK', network)
        assert EthUsdc().chain.chain_id == Ether().chain.chain_id == chain_id

    def test_token_contract_follows_the_shared_network(self, provider, monkeypatch):
        assert provider.token_contract == CONTRACT  # sepolia deployment, off ETH_NETWORK
        monkeypatch.setenv('ETH_NETWORK', 'mainnet')
        assert EthUsdc().token_contract == CHAIN_ETHUSDC.asset_locator

    def test_env_override_wins(self, monkeypatch):
        monkeypatch.setenv('ETHUSDC_TOKEN_CONTRACT', '0x' + '42' * 20)
        assert EthUsdc().token_contract == '0x' + '42' * 20

    def test_unknown_network_raises(self, monkeypatch):
        monkeypatch.setenv('ETH_NETWORK', 'seplia')
        with pytest.raises(ValueError, match='ETH_NETWORK'):
            EthUsdc()

    def test_finality_matches_the_chain_it_shares(self):
        # Two assets on one chain must not disagree about its reorg depth, and the implied
        # 384s leg stays inside the program's 600s default fulfillment grace.
        assert (CHAIN_ETHUSDC.min_confirmations, CHAIN_ETHUSDC.seconds_per_block) == (
            CHAIN_ETH.min_confirmations,
            CHAIN_ETH.seconds_per_block,
        )
        assert CHAIN_ETHUSDC.replay_grace_secs == CHAIN_ETH.replay_grace_secs
        assert CHAIN_ETHUSDC.min_confirmations * CHAIN_ETHUSDC.seconds_per_block < 600

    def test_lookback_window_is_time_based(self, provider):
        assert provider.SCAN_LOOKBACK_BLOCKS == 25  # ≈5 min of 12s blocks

    def test_wrong_network_endpoint_fails_startup(self, provider):
        # Sepolia configured, a mainnet endpoint answering: reject outright rather than
        # quietly verify legs against the wrong chain.
        rpc_stub(provider, {'eth_chainId': '0x1'})
        with pytest.raises(ConnectionError, match=f'expected {SEPOLIA_ID}'):
            provider.check_connection(require_send=False)

    def test_codeless_contract_fails_startup(self, provider):
        rpc_stub(provider, {'eth_chainId': hex(SEPOLIA_ID), 'eth_blockNumber': '0x10', 'eth_getCode': '0x'})
        with pytest.raises(ConnectionError, match='no code'):
            provider.check_connection(require_send=False)


class TestVerification:
    def test_settled_transfer_matches_with_sender_pinned(self, provider):
        rpc_stub(provider, mined())
        info = provider.fetch_matching_tx(TX, RECIPIENT.lower(), AMOUNT)
        assert info.confirmed and info.amount == AMOUNT and info.block_time == 0x64
        assert info.sender == SENDER.lower()

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

    def test_pending_transfer_to_a_counterfeit_contract_rejected(self, provider):
        calldata = SEL_TRANSFER + topic(RECIPIENT).removeprefix('0x') + f'{AMOUNT:064x}'
        pending = {'hash': TX, 'from': SENDER, 'to': COUNTERFEIT, 'input': calldata, 'blockNumber': None}
        rpc_stub(provider, {'eth_getTransactionByHash': pending})
        assert provider.fetch_matching_tx(TX, RECIPIENT.lower(), AMOUNT) is None

    def test_absent_tx_is_absent(self, provider):
        rpc_stub(provider, {'eth_getTransactionByHash': None})
        assert provider.fetch_matching_tx(TX, RECIPIENT.lower(), AMOUNT) is None

    def test_receipt_unavailable_is_unknown_not_absent(self, provider):
        # 'unknown' must never read as 'no such payment' — that verdict slashes a paying miner.
        with pytest.raises(ProviderUnreachableError):
            rpc_stub(provider, dict(mined(), eth_getTransactionReceipt=None)).fetch_matching_tx(
                TX, RECIPIENT.lower(), AMOUNT
            )

    def test_missing_block_timestamp_is_unknown_not_absent(self, provider):
        with pytest.raises(ProviderUnreachableError):
            rpc_stub(provider, mined(block={'hash': BLOCK_HASH})).fetch_matching_tx(TX, RECIPIENT.lower(), AMOUNT)

    def test_counterfeit_token_log_never_matches_even_on_the_cache_path(self, provider):
        """A fake token emitting a well-formed Transfer must not settle the leg, and must not
        settle it on the re-verify either: the cache is only written from a pinned-contract match."""
        rpc_stub(provider, mined(logs=[transfer_log(contract=COUNTERFEIT)]))
        assert provider.fetch_matching_tx(TX, RECIPIENT.lower(), AMOUNT) is None
        assert provider._settled_cache == {}
        assert provider.fetch_matching_tx(TX, RECIPIENT.lower(), AMOUNT) is None


class TestIssuerGates:
    """USDC is Circle's FiatToken, so isBlacklisted/paused answer — verified live on mainnet."""

    def views(self, provider, blacklisted=False, paused=False):
        def call(params):
            hit = blacklisted if params[0]['data'].startswith(SEL_IS_BLACKLISTED) else paused
            return f'0x{int(hit):064x}'

        return rpc_stub(provider, {'eth_call': call, 'eth_blockNumber': hex(1_000_000)})

    def test_clean_dest_passes_reserve(self, provider):
        assert self.views(provider).can_deliver_to(RECIPIENT, AMOUNT) is True

    @pytest.mark.parametrize('gate', ('blacklisted', 'paused'))
    def test_issuer_freeze_bounces_reserve_and_defers_the_slash(self, provider, gate):
        # Freezing the payout address after reserve makes delivery impossible through no
        # fault of the miner — positive evidence, never a slash.
        p = self.views(provider, **{gate: True})
        assert p.can_deliver_to(RECIPIENT, AMOUNT) is False
        assert p.delivery_refused(RECIPIENT, 0) is True

    def test_slash_gate_raises_on_rpc_trouble(self, provider):
        def boom(params):
            raise ConnectionError('down')

        rpc_stub(provider, {'eth_call': boom})
        with pytest.raises(ConnectionError):  # a flaky RPC postpones a slash, never falsifies one
            provider.delivery_refused(RECIPIENT, 0)


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
        # Token-rich but ETH-poor: refuse here rather than burn a revert on L1.
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
