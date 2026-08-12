"""BaseUsdc unit tests — all offline (RPC layer mocked, signing is pure crypto).

The generic ERC-20 behaviour is `Erc20`, proven by the arbusdc suite. What is Base-specific
lives in the binding: which network the env selects, the chain id every signed tx commits to,
and — the hazard that makes this asset different from every other spoke — that the pinned
contract is Circle's native USDC and never the bridged USDbC deployed beside it.
"""

import pytest

from allways.assets.asset import ProviderUnreachableError
from allways.assets.baseusdc import BaseUsdc
from allways.assets.erc20 import (
    SEL_BALANCE_OF,
    SEL_IS_BLACKLISTED,
    SEL_PAUSED,
    SEL_TRANSFER,
    TRANSFER_TOPIC0,
)
from allways.assets.evm import BASE, EvmChain
from allways.chains import CHAIN_BASEUSDC, get_chain_def
from allways.constants import LAUNCH_SPOKES
from allways.utils.rate import is_executable_rate

TEST_KEY = '0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80'  # hardhat account #0
TEST_ADDR = '0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266'

CONTRACT = '0x036CbD53842c5426634e7929541eC2318f3dCF7e'  # Circle USDC, Base Sepolia
# Circle-verified native USDC on Base mainnet (developers.circle.com) — symbol 'USDC', name
# 'USD Coin'. Pinned as a literal: a transposed character yields a DIFFERENT REAL TOKEN, which
# has code and so passes every other gate, and miners would pay in an asset nobody asked for.
MAINNET_CONTRACT = '0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913'
# Bridged USD Base Coin — a different contract paying a well-formed Transfer nobody asked for.
USDBC = '0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA'
SENDER = '0x' + '11' * 20
RECIPIENT = '0x70997970C51812dc3A010C7d01b50e0d17dc79C8'
TX = '0x' + 'ab' * 32
BLOCK_HASH = '0x' + 'cd' * 32
AMOUNT = 150_000_000  # 150 USDC in µUSDC
MINED = 1_000_000
SETTLED_TIP = MINED + CHAIN_BASEUSDC.min_confirmations - 1


@pytest.fixture
def provider(monkeypatch):
    monkeypatch.setenv('BASE_NETWORK', 'sepolia')
    for var in ('BASE_RPC_URLS', 'BASE_PRIVATE_KEY', 'BASEUSDC_TOKEN_CONTRACT'):
        monkeypatch.delenv(var, raising=False)
    return BaseUsdc()


def rpc_stub(provider, responses: dict):
    """Replace eth_rpc with a method→response map. A callable value is invoked with params."""

    def fake_rpc(method, params, timeout=15, **kw):
        value = responses[method]
        return value(params) if callable(value) else value

    provider.chain.eth_rpc = fake_rpc  # the asset talks through its chain (composed seam)
    return provider


def topic(address: str) -> str:
    return '0x' + '00' * 12 + address.lower().removeprefix('0x')


def transfer_log(contract=CONTRACT, sender=SENDER, recipient=RECIPIENT, amount=AMOUNT) -> dict:
    return {'address': contract, 'topics': [TRANSFER_TOPIC0, topic(sender), topic(recipient)], 'data': hex(amount)}


def mined_tx_responses(logs=None, status='0x1', tip=SETTLED_TIP, receipt=..., timestamp='0x64'):
    if logs is None:
        logs = [transfer_log()]
    if receipt is ...:
        receipt = {'status': status, 'blockNumber': hex(MINED), 'blockHash': BLOCK_HASH, 'logs': logs}
    return {
        'eth_getTransactionByHash': {
            'hash': TX,
            'from': SENDER,
            'to': CONTRACT,
            'blockNumber': hex(MINED),
            'blockHash': BLOCK_HASH,
        },
        'eth_getTransactionReceipt': receipt,
        'eth_blockNumber': hex(tip),
        'eth_getBlockByNumber': {'hash': BLOCK_HASH, 'timestamp': timestamp},
    }


def eth_call_views(blacklisted=(), paused=False, balances=None):
    """eth_call dispatcher over the token's views, keyed by selector."""

    def call(params):
        data = params[0]['data']
        if data.startswith(SEL_PAUSED):
            return hex(int(paused))
        addr = '0x' + data[-40:]
        if data.startswith(SEL_IS_BLACKLISTED):
            return hex(int(addr in {a.lower() for a in blacklisted}))
        if data.startswith(SEL_BALANCE_OF):
            return hex((balances or {}).get(addr, 0))
        raise AssertionError(f'unexpected eth_call {data[:10]}')

    return call


class TestRegistryRow:
    def test_is_a_launch_spoke_bound_to_its_registry_row(self, provider):
        assert provider.chain_def is CHAIN_BASEUSDC is get_chain_def('baseusdc')
        assert 'baseusdc' in LAUNCH_SPOKES

    def test_token_composes_its_network(self, provider):
        assert isinstance(provider.chain, EvmChain) and provider.chain is not provider
        assert provider.chain.env_prefix == 'BASE'  # the NETWORK's identity, not the asset's

    def test_decimals_and_units(self):
        assert (CHAIN_BASEUSDC.decimals, CHAIN_BASEUSDC.native_unit) == (6, 'µUSDC')

    def test_confirmations_stay_inside_the_program_fulfillment_grace(self):
        # An unlisted chain gets the program's 600s default grace; 120 × 2s = 240s is inside it.
        assert CHAIN_BASEUSDC.min_confirmations * CHAIN_BASEUSDC.seconds_per_block == 240

    @pytest.mark.parametrize('pair', (('baseusdc', 'sol'), ('sol', 'baseusdc')))
    def test_sane_rates_execute_in_both_directions(self, pair):
        # 6 decimals against the hub's 9 plus a floor of 1 µUSDC: the gate must route a real
        # ~200 USDC/SOL market both ways, and still reject a nonsense rate.
        assert is_executable_rate(200.0, *pair, 10_000_000, 100_000_000_000) is True
        assert is_executable_rate(1e-20, *pair, 10_000_000, 100_000_000_000) is False


class TestNetworkAndContract:
    def test_mainnet_chain_id_and_circle_contract(self, monkeypatch):
        for var in ('BASE_NETWORK', 'BASE_RPC_URLS', 'BASEUSDC_TOKEN_CONTRACT'):
            monkeypatch.delenv(var, raising=False)
        p = BaseUsdc()
        assert (p.chain.network, p.chain.chain_id) == ('mainnet', 8_453)
        assert p.token_contract == CHAIN_BASEUSDC.asset_locator == MAINNET_CONTRACT
        # Native Circle USDC, not the bridged USD Base Coin deployed on the same network.
        assert p.token_contract.lower() != USDBC.lower()

    def test_sepolia_uses_the_testnet_deployment(self, provider):
        assert (provider.chain.chain_id, provider.token_contract) == (84_532, CONTRACT)

    def test_unknown_network_raises(self, monkeypatch):
        # A typo must never fall back to mainnet — that spends real USDC against test swaps.
        monkeypatch.setenv('BASE_NETWORK', 'seplia')
        with pytest.raises(ValueError, match='BASE_NETWORK'):
            BaseUsdc()

    @pytest.mark.parametrize('network', tuple(BASE.chain_ids))
    def test_every_network_has_a_two_deep_keyless_ladder(self, monkeypatch, network):
        monkeypatch.setenv('BASE_NETWORK', network)
        # Two endpoints or null quorum can never be reached, and every absent tx would raise.
        assert BaseUsdc().chain.rpc_bases == list(BASE.rpc_urls[network]) and len(BASE.rpc_urls[network]) == 2

    def test_env_override_wins(self, monkeypatch):
        monkeypatch.setenv('BASEUSDC_TOKEN_CONTRACT', '0x' + '42' * 20)
        assert BaseUsdc().token_contract == '0x' + '42' * 20

    def test_lookback_window_is_time_based(self, provider):
        assert provider.SCAN_LOOKBACK_BLOCKS == 150  # ≈5 min of 2s blocks


class TestCheckConnection:
    def test_wrong_network_endpoint_deeper_in_ladder_fails_boot(self, provider):
        # Every configured endpoint is probed — a mainnet URL at position 2 would otherwise
        # surface only mid-outage, quietly verifying legs against the wrong chain.
        def fake_rpc(method, params, timeout=15, bases=None, **kw):
            if method == 'eth_chainId':
                return hex(8_453) if bases and 'drpc' in bases[0] else hex(84_532)
            return '0x10'

        provider.chain.eth_rpc = fake_rpc
        with pytest.raises(ConnectionError, match='drpc serves chain id 8453'):
            provider.check_connection(require_send=False)

    def test_codeless_contract_fails(self, provider):
        rpc_stub(provider, {'eth_chainId': hex(84_532), 'eth_blockNumber': '0x10', 'eth_getCode': '0x'})
        with pytest.raises(ConnectionError, match='no code'):
            provider.check_connection(require_send=False)

    def test_missing_key_fails_when_send_required(self, provider):
        with pytest.raises(ConnectionError, match='BASE_PRIVATE_KEY'):
            provider.check_connection(require_send=True)


class TestVerification:
    def test_settled_transfer_matches_with_the_sender_pinned(self, provider):
        rpc_stub(provider, mined_tx_responses())
        info = provider.verify_transaction(TX, RECIPIENT.lower(), AMOUNT, expected_sender=SENDER.upper())
        assert info is not None and info.confirmed
        assert (info.sender, info.amount, info.block_time) == (SENDER, AMOUNT, 0x64)

    def test_one_short_of_the_confirmation_depth_is_unconfirmed(self, provider):
        rpc_stub(provider, mined_tx_responses(tip=SETTLED_TIP - 1))
        assert provider.verify_transaction(TX, RECIPIENT, AMOUNT).confirmed is False

    def test_pending_transfer_calldata_matches_unconfirmed(self, provider):
        pending = {
            'hash': TX,
            'from': SENDER,
            'to': CONTRACT,
            'input': SEL_TRANSFER + topic(RECIPIENT).removeprefix('0x') + f'{AMOUNT:064x}',
            'blockNumber': None,
        }
        rpc_stub(provider, {'eth_getTransactionByHash': pending})
        info = provider.verify_transaction(TX, RECIPIENT, AMOUNT)
        assert info is not None and not info.confirmed and info.sender == SENDER

    @pytest.mark.parametrize(
        'responses',
        (
            mined_tx_responses(status='0x0'),  # inclusion is not settlement
            mined_tx_responses(logs=[transfer_log(amount=AMOUNT - 1)]),  # underpaid
            mined_tx_responses(logs=[transfer_log(recipient='0x' + '22' * 20)]),  # wrong recipient
            mined_tx_responses(logs=[transfer_log(sender=RECIPIENT)]),  # self-transfer
            {'eth_getTransactionByHash': None},  # not found
        ),
    )
    def test_rejected(self, provider, responses):
        assert rpc_stub(provider, responses).verify_transaction(TX, RECIPIENT, AMOUNT) is None

    def test_sender_mismatch_rejected(self, provider):
        rpc_stub(provider, mined_tx_responses())
        assert provider.verify_transaction(TX, RECIPIENT, AMOUNT, expected_sender=TEST_ADDR) is None

    def test_receipt_unavailable_raises(self, provider):
        # 'unknown' must never read as 'no such payment' — that verdict slashes a paying miner.
        rpc_stub(provider, mined_tx_responses(receipt=None))
        with pytest.raises(ProviderUnreachableError):
            provider.verify_transaction(TX, RECIPIENT, AMOUNT)

    def test_timestamp_unavailable_raises(self, provider):
        # is_tx_fresh fails closed on a missing block_time, so a stale-looking dest leg would
        # ride to a TIMEOUT slash. Raise instead and let the caller retry.
        rpc_stub(provider, mined_tx_responses(timestamp='0x0'))
        with pytest.raises(ProviderUnreachableError):
            provider.verify_transaction(TX, RECIPIENT, AMOUNT)


class TestContractPin:
    """Base ships two USDCs. A leg is only paid if the log came from the pinned one."""

    def test_usdbc_transfer_never_satisfies_a_usdc_leg(self, provider):
        rpc_stub(provider, mined_tx_responses(logs=[transfer_log(contract=USDBC)]))
        assert provider.verify_transaction(TX, RECIPIENT, AMOUNT) is None

    def test_the_pinned_log_is_found_among_impostors(self, provider):
        noise = [transfer_log(contract=USDBC), transfer_log(contract='0x' + '99' * 20), transfer_log()]
        rpc_stub(provider, mined_tx_responses(logs=noise))
        assert provider.verify_transaction(TX, RECIPIENT, AMOUNT) is not None

    def test_a_rejected_tx_never_enters_the_settled_cache(self, provider):
        # The cache skips the log scan, so a rejected counterfeit must leave no entry to
        # later vouch for it — the pin has to hold on the cached path too.
        rpc_stub(provider, mined_tx_responses(logs=[transfer_log(contract=USDBC)]))
        assert provider.verify_transaction(TX, RECIPIENT, AMOUNT) is None
        assert TX not in provider._settled_cache
        provider.chain.clear_pass_tip()
        assert provider.verify_transaction(TX, RECIPIENT, AMOUNT) is None

    def test_the_cache_never_vouches_for_a_different_leg(self, provider):
        rpc_stub(provider, mined_tx_responses())
        assert provider.verify_transaction(TX, RECIPIENT, AMOUNT) is not None
        provider.chain.clear_pass_tip()
        assert provider.verify_transaction(TX, '0x' + '22' * 20, AMOUNT) is None
        assert provider.verify_transaction(TX, RECIPIENT, AMOUNT + 1) is None


class TestIssuerGates:
    """USDC on Base is Circle's FiatToken: isBlacklisted/paused answer (verified live)."""

    @pytest.mark.parametrize('views', (eth_call_views(blacklisted=[RECIPIENT]), eth_call_views(paused=True)))
    def test_issuer_refusal_bounces_reserve_and_defers_slash(self, provider, views):
        rpc_stub(provider, {'eth_blockNumber': hex(1000), 'eth_call': views})
        assert provider.can_deliver_to(RECIPIENT, AMOUNT) is False
        assert provider.delivery_refused(RECIPIENT, since_unix=0) is True

    def test_clean_dest_passes_both_gates(self, provider):
        rpc_stub(provider, {'eth_blockNumber': hex(1000), 'eth_call': eth_call_views()})
        assert provider.can_deliver_to(RECIPIENT, AMOUNT) is True
        assert provider.delivery_refused(RECIPIENT, since_unix=0) is False

    def test_slash_gate_raises_on_rpc_trouble(self, provider):
        # A missing/renamed view (Tether spells it isBlackListed) would revert here; the caller
        # must defer the slash rather than resolve it — a flaky RPC postpones, never falsifies.
        def boom(params):
            raise ConnectionError('down')

        rpc_stub(provider, {'eth_blockNumber': hex(1000), 'eth_call': boom})
        with pytest.raises(Exception):
            provider.delivery_refused(RECIPIENT, since_unix=0)


class TestSendGuards:
    def _responses(self, token_balance=10 * AMOUNT, gas_balance=10**18, est='0xfde8'):  # est 65k
        return {
            'eth_blockNumber': hex(1000),
            'eth_getBlockByNumber': {'baseFeePerGas': hex(10**8)},
            'eth_maxPriorityFeePerGas': hex(10**7),
            'eth_estimateGas': est,
            'eth_call': eth_call_views(balances={TEST_ADDR.lower(): token_balance}),
            'eth_getBalance': hex(gas_balance),
            'eth_getTransactionCount': '0x0',
            'eth_sendRawTransaction': '0x' + 'ee' * 32,
        }

    def test_no_key_refused(self, provider):
        assert provider.send_amount(RECIPIENT, AMOUNT) is None
        assert 'BASE_PRIVATE_KEY' in provider.last_send_error

    def test_key_mismatch_refused(self, provider, monkeypatch):
        monkeypatch.setenv('BASE_PRIVATE_KEY', TEST_KEY)
        assert provider.send_amount(RECIPIENT, AMOUNT, from_address=RECIPIENT) is None
        assert 'key mismatch' in provider.last_send_error

    def test_insufficient_token_balance_refused(self, provider, monkeypatch):
        monkeypatch.setenv('BASE_PRIVATE_KEY', TEST_KEY)
        rpc_stub(provider, self._responses(token_balance=AMOUNT - 1))
        assert provider.send_amount(RECIPIENT, AMOUNT) is None
        assert 'insufficient balance' in provider.last_send_error

    def test_token_rich_gas_poor_refuses_before_broadcasting(self, provider, monkeypatch):
        # Base gas is cheap but never free; a doomed transfer must be refused, not burned.
        monkeypatch.setenv('BASE_PRIVATE_KEY', TEST_KEY)
        responses = self._responses(gas_balance=10)
        responses['eth_sendRawTransaction'] = lambda params: pytest.fail('must not broadcast')
        rpc_stub(provider, responses)
        assert provider.send_amount(RECIPIENT, AMOUNT) is None
        assert 'Insufficient gas balance' in provider.last_send_error

    def test_happy_path_broadcasts_pinned_transfer_calldata(self, provider, monkeypatch):
        monkeypatch.setenv('BASE_PRIVATE_KEY', TEST_KEY)
        sent = {}
        responses = self._responses()

        def capture(params):
            sent['raw'] = params[0]
            return '0x' + 'ee' * 32

        responses['eth_sendRawTransaction'] = capture
        rpc_stub(provider, responses)
        assert provider.send_amount(RECIPIENT, AMOUNT, from_address=TEST_ADDR) == ('0x' + 'ee' * 32, 0)
        assert SEL_TRANSFER.removeprefix('0x') in sent['raw']
        assert CONTRACT.lower().removeprefix('0x') in sent['raw']  # addressed to the pinned token
        assert list(provider.broadcasted_txids.values()) == [(RECIPIENT.lower(), AMOUNT, 1000)]


class TestScanner:
    def test_getlogs_hit_is_bounded_and_pinned(self, provider):
        found = '0x' + 'bb' * 32

        def logs(params):
            f = params[0]
            assert f['address'] == CONTRACT  # a USDbC transfer is not even queried for
            assert int(f['toBlock'], 16) - int(f['fromBlock'], 16) < provider.SCAN_LOOKBACK_BLOCKS
            return [{'data': hex(AMOUNT), 'transactionHash': found}]

        rpc_stub(provider, {'eth_blockNumber': hex(1000), 'eth_getLogs': logs})
        assert provider.find_recent_outgoing(TEST_ADDR, RECIPIENT, AMOUNT) == found

    def test_failed_range_parks_cursor(self, provider):
        def boom(params):
            raise ConnectionError('range too large')

        rpc_stub(provider, {'eth_blockNumber': hex(1000), 'eth_getLogs': boom})
        assert provider.find_recent_outgoing(TEST_ADDR, RECIPIENT, AMOUNT) is None
        # Parked just below the failed range start — the same span is retried, never leapt.
        key = (TEST_ADDR.lower(), RECIPIENT.lower(), AMOUNT)
        assert provider.scan_cursors[key] == 1000 - provider.SCAN_LOOKBACK_BLOCKS
