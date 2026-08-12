"""Uni unit tests — all offline (RPC layer mocked, signing is pure crypto).

Generic ERC-20 behaviour is proven by the arbusdc suite and the declared-refusal contract by
tests/test_erc20_refusal_checks.py; this covers what is uni-specific. Two things:

  * UNI is the first REAL row to declare ``refusal_checks=()``. Uni.sol has no
    isBlacklisted/paused, so probing them reverts — and a revert read as a verdict is what made
    such a pair permanently unslashable. The gates must answer without touching the RPC, and boot
    must falsify the claim against whatever contract actually resolved.
  * It is the THIRD asset on Ethereum's env identity (eth, ethusdc, uni). One ETH_NETWORK must
    move all three, or uni reads an unset var, silently defaults to mainnet, and a testnet miner
    pays real UNI against test swaps.
"""

import pytest

from allways.assets.asset import ProviderUnreachableError
from allways.assets.erc20 import (
    SEL_TRANSFER,
    TESTNET_TOKEN_CONTRACTS,
    TRANSFER_TOPIC0,
)
from allways.assets.eth import Ether
from allways.assets.ethusdc import EthUsdc
from allways.assets.uni import Uni
from allways.chains import CHAIN_ETH, CHAIN_UNI, get_chain_def
from allways.constants import LAUNCH_SPOKES

TEST_KEY = '0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80'  # hardhat account #0
SENDER = '0x' + '11' * 20
RECIPIENT = '0x70997970C51812dc3A010C7d01b50e0d17dc79C8'  # mixed case on purpose
CONTRACT = TESTNET_TOKEN_CONTRACTS['uni']['sepolia']
COUNTERFEIT = '0x' + '99' * 20
TX = '0x' + 'ab' * 32
BLOCK_HASH = '0x' + 'cd' * 32
AMOUNT = 12_500_000_000_000_000_000  # 12.5 UNI in wei — 18 decimals, not 6
SEPOLIA_ID = 11_155_111
# Startup reads: chain id, tip, then the token's code before the declaration is falsified.
BOOT = {'eth_chainId': hex(SEPOLIA_ID), 'eth_blockNumber': '0x10', 'eth_getCode': '0xdeadbeef'}


@pytest.fixture
def provider(monkeypatch):
    monkeypatch.setenv('ETH_NETWORK', 'sepolia')
    for var in ('ETH_RPC_URLS', 'ETH_PRIVATE_KEY', 'UNI_TOKEN_CONTRACT'):
        monkeypatch.delenv(var, raising=False)
    return Uni()


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
    """Ethereum is configured once, by CHAIN_ETH, for every asset that rides it."""

    def test_row_is_a_launch_spoke_on_ethereums_identity(self, provider):
        assert provider.chain_def is CHAIN_UNI is get_chain_def('uni')
        assert 'uni' in LAUNCH_SPOKES
        assert (CHAIN_UNI.env_prefix, CHAIN_UNI.host_chain) == (CHAIN_ETH.env_prefix, CHAIN_ETH.host_chain)
        # CHAIN_ETH owns ETH_NETWORK; a second declaration renders a duplicate CLI row.
        assert CHAIN_UNI.networks == ()

    @pytest.mark.parametrize('network,chain_id', (('mainnet', 1), ('sepolia', SEPOLIA_ID)))
    def test_one_eth_network_moves_all_three_assets(self, monkeypatch, network, chain_id):
        monkeypatch.setenv('ETH_NETWORK', network)
        assert Uni().chain.chain_id == EthUsdc().chain.chain_id == Ether().chain.chain_id == chain_id

    def test_token_contract_follows_the_shared_network(self, provider, monkeypatch):
        # UNI's Sepolia deployment sits at the mainnet address with byte-identical runtime code,
        # so the two resolve to the same string — assert the SOURCE, not just the value.
        assert provider.token_contract == CONTRACT == CHAIN_UNI.asset_locator
        monkeypatch.setenv('ETH_NETWORK', 'mainnet')
        assert Uni().token_contract == CHAIN_UNI.asset_locator

    def test_env_override_wins(self, monkeypatch):
        monkeypatch.setenv('UNI_TOKEN_CONTRACT', '0x' + '42' * 20)
        assert Uni().token_contract == '0x' + '42' * 20

    def test_unknown_network_raises(self, monkeypatch):
        monkeypatch.setenv('ETH_NETWORK', 'seplia')
        with pytest.raises(ValueError, match='ETH_NETWORK'):
            Uni()

    def test_finality_and_clock_match_the_chain_it_shares(self):
        # Two assets on one chain must not disagree about its reorg depth or its clock, and the
        # implied 384s leg stays inside the program's 600s default fulfillment grace.
        assert (CHAIN_UNI.min_confirmations, CHAIN_UNI.seconds_per_block, CHAIN_UNI.replay_grace_secs) == (
            CHAIN_ETH.min_confirmations,
            CHAIN_ETH.seconds_per_block,
            CHAIN_ETH.replay_grace_secs,
        )
        assert CHAIN_UNI.min_confirmations * CHAIN_UNI.seconds_per_block < 600

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


class TestUnfreezable:
    """Uni.sol has no freeze surface and cannot gain one. Probing for one reverts, and a revert
    propagating out of delivery_refused makes the caller defer every slash on the pair forever."""

    def test_row_declares_unfreezable(self):
        assert CHAIN_UNI.refusal_checks == ()

    def test_gates_answer_without_reaching_the_rpc(self, provider):
        def forbidden(*_a, **_kw):
            raise AssertionError('UNI has no issuer surface — the gates must not probe for one')

        provider.chain.eth_rpc = forbidden
        assert provider.delivery_refused(RECIPIENT, since_unix=0) is False
        assert provider.can_deliver_to(RECIPIENT, AMOUNT) is True

    def test_boot_probes_nothing(self, provider):
        # Nothing declared, nothing to read: boot must not reach for a surface UNI never had.
        def reverts(params):
            raise AssertionError('UNI declares no checks — boot must not probe for one')

        rpc_stub(provider, dict(BOOT, eth_call=reverts))
        provider.check_connection(require_send=False)


class TestVerification:
    def test_settled_transfer_matches_a_mixed_case_recipient_with_sender_pinned(self, provider):
        # Through verify_transaction, which is where casing canonicalizes: a checksummed
        # expected recipient against a lowercase on-chain topic must still settle the leg.
        rpc_stub(provider, mined())
        info = provider.verify_transaction(TX, RECIPIENT, AMOUNT, expected_sender=SENDER.upper())
        assert info.confirmed and info.amount == AMOUNT and info.block_time == 0x64
        assert info.sender == SENDER.lower()

    def test_wrong_sender_rejected(self, provider):
        rpc_stub(provider, mined())
        assert provider.verify_transaction(TX, RECIPIENT, AMOUNT, expected_sender=RECIPIENT) is None

    def test_below_min_confs_unconfirmed(self, provider):
        rpc_stub(provider, mined(tip=1_000_030))  # 31 confs of the 32 required
        assert provider.fetch_matching_tx(TX, RECIPIENT.lower(), AMOUNT).confirmed is False

    def test_reverted_rejected(self, provider):
        # Inclusion is not settlement: a reverted tx moved no funds.
        rpc_stub(provider, mined(status='0x0'))
        assert provider.fetch_matching_tx(TX, RECIPIENT.lower(), AMOUNT) is None

    def test_underpay_by_one_wei_rejected(self, provider):
        # 18 decimals: the shortfall a 6-decimal token could not even express.
        rpc_stub(provider, mined(logs=[transfer_log(amount=AMOUNT - 1)]))
        assert provider.fetch_matching_tx(TX, RECIPIENT.lower(), AMOUNT) is None

    def test_wrong_recipient_rejected(self, provider):
        rpc_stub(provider, mined(logs=[transfer_log(recipient=SENDER)]))
        assert provider.fetch_matching_tx(TX, RECIPIENT.lower(), AMOUNT) is None

    def test_delegation_logs_alongside_the_transfer_are_ignored(self, provider):
        # A UNI transfer also moves voting power, emitting DelegateVotesChanged from the SAME
        # contract. Only topic0 == Transfer may settle a leg.
        votes = dict(transfer_log(), topics=['0x' + 'de' * 32, topic(SENDER), topic(RECIPIENT)])
        rpc_stub(provider, mined(logs=[votes, transfer_log()]))
        assert provider.fetch_matching_tx(TX, RECIPIENT.lower(), AMOUNT).amount == AMOUNT

    def test_pending_transfer_matches_off_calldata(self, provider):
        calldata = SEL_TRANSFER + topic(RECIPIENT).removeprefix('0x') + f'{AMOUNT:064x}'
        pending = {'hash': TX, 'from': SENDER, 'to': CONTRACT, 'input': calldata, 'blockNumber': None}
        rpc_stub(provider, {'eth_getTransactionByHash': pending})
        info = provider.fetch_matching_tx(TX, RECIPIENT.lower(), AMOUNT)
        assert info.confirmed is False and info.sender == SENDER.lower() and info.amount == AMOUNT

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


class TestSendGuards:
    SEND = {
        'eth_blockNumber': hex(100),
        'eth_getBlockByNumber': {'baseFeePerGas': hex(10**9)},
        'eth_maxPriorityFeePerGas': hex(10**9),
        'eth_getTransactionCount': '0x0',
        'eth_getBalance': hex(10**18),
        'eth_estimateGas': hex(90_000),  # UNI's transfer also writes delegation checkpoints
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
