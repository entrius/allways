"""Aster unit tests — all offline (RPC layer mocked, signing is pure crypto).

Generic ERC-20 behaviour is proven by the arbusdc suite; this covers what is aster-specific.
Two things: the shared env identity (ONE BNB_NETWORK moves aster and native BNB, so a testnet
miner can never pay real ASTER against test swaps), and the 'unfreezable' declaration — ASTER
implements neither isBlacklisted nor paused, both of which REVERT, and a revert read as a
verdict is what leaves a pair's miners permanently unslashable.

ASTER has no deployment on Chapel, so this suite never constructs a testnet provider without
the ASTER_TOKEN_CONTRACT override — see TestTestnetGap.
"""

import pytest

from allways.assets.asset import ProviderUnreachableError
from allways.assets.aster import Aster
from allways.assets.bnb import Bnb
from allways.assets.erc20 import SEL_TRANSFER, TESTNET_TOKEN_CONTRACTS, TRANSFER_TOPIC0
from allways.assets.evm import EvmRpcError
from allways.chains import CHAIN_ASTER, CHAIN_BNB, get_chain_def
from allways.constants import LAUNCH_SPOKES

TEST_KEY = '0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80'  # hardhat account #0
SENDER = '0x' + '11' * 20
RECIPIENT = '0x70997970C51812dc3A010C7d01b50e0d17dc79C8'
CONTRACT = CHAIN_ASTER.asset_locator
COUNTERFEIT = '0x' + '99' * 20
TX = '0x' + 'ab' * 32
BLOCK_HASH = '0x' + 'cd' * 32
AMOUNT = 25 * 10**18  # 25 ASTER in wei
BSC_ID = 56


@pytest.fixture
def provider(monkeypatch):
    monkeypatch.setenv('BNB_NETWORK', 'mainnet')
    for var in ('BNB_RPC_URLS', 'BNB_PRIVATE_KEY', 'ASTER_TOKEN_CONTRACT'):
        monkeypatch.delenv(var, raising=False)
    return Aster()


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
    """The whole point of this row: aster names the NETWORK, not the asset."""

    def test_row_is_a_launch_spoke_on_bscs_identity(self, provider):
        assert provider.chain_def is CHAIN_ASTER is get_chain_def('aster')
        assert 'aster' in LAUNCH_SPOKES
        # decimals is pinned here because nothing else in this repo guards it: the drift gate that
        # would catch a typo lives in das, and allways merges first.
        assert (CHAIN_ASTER.decimals, CHAIN_ASTER.native_unit) == (18, 'wei')
        assert (CHAIN_ASTER.env_prefix, CHAIN_ASTER.host_chain) == (CHAIN_BNB.env_prefix, CHAIN_BNB.host_chain)
        # CHAIN_BNB owns BNB_NETWORK; a second declaration renders a duplicate CLI row.
        assert CHAIN_ASTER.networks == ()

    @pytest.mark.parametrize('network,chain_id', (('mainnet', BSC_ID), ('testnet', 97)))
    def test_one_bnb_network_moves_both_assets(self, monkeypatch, network, chain_id):
        monkeypatch.setenv('BNB_NETWORK', network)
        monkeypatch.setenv('ASTER_TOKEN_CONTRACT', CONTRACT)  # no Chapel deployment — see TestTestnetGap
        assert Aster().chain.chain_id == Bnb().chain.chain_id == chain_id

    def test_env_override_wins(self, provider, monkeypatch):
        assert provider.token_contract == CHAIN_ASTER.asset_locator
        monkeypatch.setenv('ASTER_TOKEN_CONTRACT', '0x' + '42' * 20)
        assert Aster().token_contract == '0x' + '42' * 20

    def test_unknown_network_raises(self, monkeypatch):
        monkeypatch.setenv('BNB_NETWORK', 'chapel')
        with pytest.raises(ValueError, match='BNB_NETWORK'):
            Aster()

    def test_finality_matches_the_chain_it_shares(self):
        # Two assets on one chain must not disagree about its reorg depth, and the implied
        # 15s leg stays inside the program's 600s default fulfillment grace.
        assert (CHAIN_ASTER.min_confirmations, CHAIN_ASTER.seconds_per_block) == (
            CHAIN_BNB.min_confirmations,
            CHAIN_BNB.seconds_per_block,
        )
        assert CHAIN_ASTER.replay_grace_secs == CHAIN_BNB.replay_grace_secs
        assert CHAIN_ASTER.min_confirmations * CHAIN_ASTER.seconds_per_block < 600

    def test_lookback_window_is_time_based(self, provider):
        assert provider.SCAN_LOOKBACK_BLOCKS == 300  # ≈5 min of BSC's sub-second blocks

    def test_wrong_network_endpoint_fails_startup(self, provider):
        # Mainnet configured, a Chapel endpoint answering: reject outright rather than
        # quietly verify legs against the wrong chain.
        rpc_stub(provider, {'eth_chainId': '0x61'})
        with pytest.raises(ConnectionError, match=f'expected {BSC_ID}'):
            provider.check_connection(require_send=False)

    def test_codeless_contract_fails_startup(self, provider):
        rpc_stub(provider, {'eth_chainId': hex(BSC_ID), 'eth_blockNumber': '0x10', 'eth_getCode': '0x'})
        with pytest.raises(ConnectionError, match='no code'):
            provider.check_connection(require_send=False)


class TestTestnetGap:
    """ASTER is deployed on BSC mainnet only. Until a project-deployed test token is pinned in
    TESTNET_TOKEN_CONTRACTS, a Chapel provider must fail LOUDLY at construction and name the
    override — silently defaulting to mainnet is how a testnet miner spends real ASTER."""

    def test_chapel_has_no_pinned_contract(self, monkeypatch):
        assert 'aster' not in TESTNET_TOKEN_CONTRACTS
        monkeypatch.setenv('BNB_NETWORK', 'testnet')
        monkeypatch.delenv('ASTER_TOKEN_CONTRACT', raising=False)
        with pytest.raises(ValueError, match='ASTER_TOKEN_CONTRACT'):
            Aster()


class TestVerification:
    def test_settled_transfer_matches_with_sender_pinned(self, provider):
        rpc_stub(provider, mined())
        info = provider.fetch_matching_tx(TX, RECIPIENT.lower(), AMOUNT)
        assert info.confirmed and info.amount == AMOUNT and info.block_time == 0x64
        assert info.sender == SENDER.lower()

    def test_below_min_confs_unconfirmed(self, provider):
        rpc_stub(provider, mined(tip=1_000_013))  # 14 confs of the 15 required
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


class TestUnfreezableDeclaration:
    """ASTER's verified source is stock OpenZeppelin ERC20 + ERC20Permit — no blacklist, no pause
    — on a contract that can never gain one (EIP-1967 slots empty, owner() reverts). Both halves
    are what 'unfreezable' asserts. On-chain, isBlacklisted and paused REVERT (probed live on BSC
    mainnet, 2026-08-12), so both gates answer from the registry with ZERO RPC calls; probing
    would raise, and a raise in delivery_refused defers every slash on the pair forever."""

    def dead_rpc(self, provider):
        def boom(method, params, timeout=15, **kw):
            raise AssertionError(f'no RPC may be made for an unfreezable token (got {method})')

        provider.chain.eth_rpc = boom
        return provider

    def test_row_declares_no_freeze_surface(self):
        assert CHAIN_ASTER.refusal_checks == ()

    def test_reserve_gate_passes_without_rpc(self, provider):
        assert self.dead_rpc(provider).can_deliver_to(RECIPIENT, AMOUNT) is True

    def test_slash_gate_answers_false_without_rpc(self, provider):
        # The regression guard: an RPC here would revert on ASTER, raise, and make every miner
        # on the pair permanently unslashable.
        assert self.dead_rpc(provider).delivery_refused(RECIPIENT, 0) is False

    def test_boot_falsifier_clears_against_asters_real_answers(self, provider):
        # Both freeze probes revert on the real contract, which is what the declaration predicts.
        def call(params):
            raise EvmRpcError('execution reverted', {'code': 3, 'message': 'execution reverted'})

        rpc_stub(
            provider,
            {'eth_chainId': hex(BSC_ID), 'eth_blockNumber': '0x10', 'eth_getCode': '0xfe', 'eth_call': call},
        )
        provider.check_connection(require_send=False)


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
        assert 'BNB_PRIVATE_KEY' in provider.last_send_error

    def test_key_mismatch_refused(self, provider, monkeypatch):
        monkeypatch.setenv('BNB_PRIVATE_KEY', TEST_KEY)
        assert provider.send_amount(RECIPIENT, AMOUNT, from_address=RECIPIENT) is None
        assert 'key mismatch' in provider.last_send_error

    def test_insufficient_token_balance_refused(self, provider, monkeypatch):
        monkeypatch.setenv('BNB_PRIVATE_KEY', TEST_KEY)
        rpc_stub(provider, dict(self.SEND, eth_call=f'0x{AMOUNT - 1:064x}'))
        assert provider.send_amount(RECIPIENT, AMOUNT) is None
        assert 'insufficient balance' in provider.last_send_error

    def test_gas_poor_miner_refuses_before_broadcasting(self, provider, monkeypatch):
        # Token-rich but BNB-poor: refuse here rather than burn a revert on-chain.
        monkeypatch.setenv('BNB_PRIVATE_KEY', TEST_KEY)
        rpc_stub(provider, dict(self.SEND, eth_getBalance=hex(10**9)))
        assert provider.send_amount(RECIPIENT, AMOUNT) is None
        assert 'Insufficient gas balance' in provider.last_send_error

    def test_broadcast_returns_a_hash(self, provider, monkeypatch):
        monkeypatch.setenv('BNB_PRIVATE_KEY', TEST_KEY)
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
