"""AvaxUsdc / HypeUsdc unit tests — all offline (RPC layer mocked, signing is pure crypto).

Generic ERC-20 behaviour is proven by the arbusdc and ethusdc suites; this covers what each
row adds, parametrized over both since they differ only in the network they ride:

- **The shared env identity.** Each token rides its host coin's row (AVAX_ / HYPE_), so ONE
  {PREFIX}_NETWORK moves both. A second prefix would leave the token reading an unset var,
  silently defaulting to mainnet, and a testnet miner would pay real USDC against test swaps.
- **The pin.** Only the registry's asset_locator separates Circle's native deployment from a
  look-alike; the address is the safety property, so it is pinned here.
"""

from typing import NamedTuple

import pytest

from allways.assets.asset import ProviderUnreachableError
from allways.assets.avax import Avax
from allways.assets.avaxusdc import AvaxUsdc
from allways.assets.erc20 import SEL_IS_BLACKLISTED, SEL_TRANSFER, TESTNET_TOKEN_CONTRACTS, TRANSFER_TOPIC0
from allways.assets.hype import Hype
from allways.assets.hypeusdc import HypeUsdc
from allways.chains import CHAIN_AVAX, CHAIN_AVAXUSDC, CHAIN_HYPE, CHAIN_HYPEUSDC, ChainDefinition, get_chain_def
from allways.constants import LAUNCH_SPOKES


class Spoke(NamedTuple):
    cls: type
    host_cls: type
    row: ChainDefinition
    host_row: ChainDefinition
    testnet: str
    chain_ids: dict
    native: str  # Circle's mainnet deployment (developers.circle.com, 2026-10-14)
    impostor: str  # a live non-native contract the pin must reject


SPOKES = {
    'avaxusdc': Spoke(
        AvaxUsdc,
        Avax,
        CHAIN_AVAXUSDC,
        CHAIN_AVAX,
        'fuji',
        {'mainnet': 43_114, 'fuji': 43_113},
        '0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E',
        '0xA7D7079b0FEaD91F3e65f86E8915Cb59c1a4C664',  # bridged USDC.e
    ),
    'hypeusdc': Spoke(
        HypeUsdc,
        Hype,
        CHAIN_HYPEUSDC,
        CHAIN_HYPE,
        'testnet',
        {'mainnet': 999, 'testnet': 998},
        '0xb88339CB7199b77E23DB6E890353E22632Ba630f',
        '0x6b9e773128f453f5c2c60935ee2de2cbc5390a24',  # Circle's CoreDepositWallet, not an ERC-20
    ),
}

TEST_KEY = '0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80'  # hardhat account #0
SENDER = '0x' + '11' * 20
RECIPIENT = '0x70997970C51812dc3A010C7d01b50e0d17dc79C8'
TX = '0x' + 'ab' * 32
BLOCK_HASH = '0x' + 'cd' * 32
AMOUNT = 150_000_000  # 150 USDC in µUSDC
BLOCK = 1_000_000


@pytest.fixture(params=sorted(SPOKES))
def spoke(request) -> Spoke:
    return SPOKES[request.param]


@pytest.fixture
def contract(spoke) -> str:
    return TESTNET_TOKEN_CONTRACTS[spoke.row.id][spoke.testnet]


@pytest.fixture
def provider(monkeypatch, spoke):
    prefix = spoke.row.env_prefix
    monkeypatch.setenv(f'{prefix}_NETWORK', spoke.testnet)
    for var in (f'{prefix}_RPC_URLS', f'{prefix}_PRIVATE_KEY', f'{spoke.row.id.upper()}_TOKEN_CONTRACT'):
        monkeypatch.delenv(var, raising=False)
    return spoke.cls()


def rpc_stub(provider, responses: dict):
    """Replace eth_rpc with a method→response map. A callable value is invoked with params."""

    def fake_rpc(method, params, timeout=15, **kw):
        value = responses[method]
        return value(params) if callable(value) else value

    provider.chain.eth_rpc = fake_rpc  # the asset talks through its chain (composed seam)
    return provider


def topic(address: str) -> str:
    return '0x' + '00' * 12 + address.lower().removeprefix('0x')


def transfer_log(contract, recipient=RECIPIENT, amount=AMOUNT) -> dict:
    return {
        'address': contract,
        'topics': [TRANSFER_TOPIC0, topic(SENDER), topic(recipient)],
        'data': hex(amount),
        'transactionHash': TX,
    }


def mined(contract, logs=None, status='0x1', tip=BLOCK + 100, block=None) -> dict:
    return {
        'eth_getTransactionByHash': {'hash': TX, 'blockNumber': hex(BLOCK), 'blockHash': BLOCK_HASH},
        'eth_getTransactionReceipt': {
            'status': status,
            'blockNumber': hex(BLOCK),
            'blockHash': BLOCK_HASH,
            'logs': [transfer_log(contract)] if logs is None else logs,
        },
        'eth_blockNumber': hex(tip),
        'eth_getBlockByNumber': {'hash': BLOCK_HASH, 'timestamp': '0x64'} if block is None else block,
    }


class TestRegistryRow:
    def test_row_is_a_launch_spoke_on_its_hosts_identity(self, provider, spoke):
        assert provider.chain_def is spoke.row is get_chain_def(spoke.row.id)
        assert spoke.row.id in LAUNCH_SPOKES
        assert (spoke.row.env_prefix, spoke.row.host_chain) == (spoke.host_row.env_prefix, spoke.host_row.host_chain)
        # The host row owns {PREFIX}_NETWORK; a second declaration renders a duplicate CLI row.
        assert spoke.row.networks == ()

    def test_denomination_is_pinned(self, spoke):
        # das mirrors decimals, but its drift gate runs only after this merges.
        assert (spoke.row.decimals, spoke.row.native_unit) == (6, 'µUSDC')

    def test_the_pinned_contract_is_circles_native_deployment(self, spoke):
        assert spoke.row.asset_locator == spoke.native
        assert spoke.row.asset_locator.lower() != spoke.impostor.lower()
        assert spoke.row.refusal_checks == ('isBlacklisted(address)', 'paused()')

    def test_floor_matches_the_sibling_usdc_spokes(self, spoke):
        # The floor doubles as the crown-eligibility band edge; a cheap-gas floor would widen it.
        assert spoke.row.min_onchain_amount == get_chain_def('arbusdc').min_onchain_amount == 5_000_000

    def test_finality_and_clock_match_the_chain_it_shares(self, spoke):
        # Compared against the host row rather than literals, so restating a number cannot drift it.
        fields = ('min_confirmations', 'seconds_per_block', 'replay_grace_secs')
        assert [getattr(spoke.row, f) for f in fields] == [getattr(spoke.host_row, f) for f in fields]
        assert spoke.row.replay_grace_secs == 60  # set explicitly; an omitted field silently reads 0
        # Inside the program's 600s default fulfillment grace (neither chain has a table arm).
        assert spoke.row.min_confirmations * spoke.row.seconds_per_block < 600


class TestSharedEnvIdentity:
    @pytest.mark.parametrize('network', ('mainnet', 'testnet'))
    def test_one_network_var_moves_both_assets(self, monkeypatch, spoke, network):
        name = 'mainnet' if network == 'mainnet' else spoke.testnet
        monkeypatch.setenv(f'{spoke.row.env_prefix}_NETWORK', name)
        assert spoke.cls().chain.chain_id == spoke.host_cls().chain.chain_id == spoke.chain_ids[name]

    def test_token_contract_follows_the_shared_network(self, provider, spoke, contract, monkeypatch):
        assert provider.token_contract == contract
        monkeypatch.setenv(f'{spoke.row.env_prefix}_NETWORK', 'mainnet')
        assert spoke.cls().token_contract == spoke.row.asset_locator

    def test_env_override_wins(self, monkeypatch, spoke):
        monkeypatch.setenv(f'{spoke.row.id.upper()}_TOKEN_CONTRACT', '0x' + '42' * 20)
        assert spoke.cls().token_contract == '0x' + '42' * 20

    def test_unknown_network_raises(self, monkeypatch, spoke):
        var = f'{spoke.row.env_prefix}_NETWORK'
        monkeypatch.setenv(var, 'tesnet')
        with pytest.raises(ValueError, match=var):
            spoke.cls()

    def test_wrong_network_endpoint_fails_startup(self, provider, spoke):
        rpc_stub(provider, {'eth_chainId': hex(spoke.chain_ids['mainnet'])})
        with pytest.raises(ConnectionError, match=f'expected {spoke.chain_ids[spoke.testnet]}'):
            provider.check_connection(require_send=False)

    def test_codeless_contract_fails_startup(self, provider, spoke):
        chain_id = hex(spoke.chain_ids[spoke.testnet])
        rpc_stub(provider, {'eth_chainId': chain_id, 'eth_blockNumber': '0x10', 'eth_getCode': '0x'})
        with pytest.raises(ConnectionError, match='no code'):
            provider.check_connection(require_send=False)


class TestVerification:
    def test_settled_transfer_matches_with_sender_pinned(self, provider, contract):
        rpc_stub(provider, mined(contract))
        info = provider.fetch_matching_tx(TX, RECIPIENT.lower(), AMOUNT)
        assert info.confirmed and info.amount == AMOUNT and info.block_time == 0x64
        assert info.sender == SENDER.lower()

    def test_below_min_confs_unconfirmed(self, provider, contract):
        rpc_stub(provider, mined(contract, tip=BLOCK))  # 1 conf of the 2 required
        assert provider.fetch_matching_tx(TX, RECIPIENT.lower(), AMOUNT).confirmed is False

    def test_reverted_rejected(self, provider, contract):
        rpc_stub(provider, mined(contract, status='0x0'))
        assert provider.fetch_matching_tx(TX, RECIPIENT.lower(), AMOUNT) is None

    def test_underpay_rejected(self, provider, contract):
        rpc_stub(provider, mined(contract, logs=[transfer_log(contract, amount=AMOUNT - 1)]))
        assert provider.fetch_matching_tx(TX, RECIPIENT.lower(), AMOUNT) is None

    def test_wrong_recipient_rejected(self, provider, contract):
        rpc_stub(provider, mined(contract, logs=[transfer_log(contract, recipient=SENDER)]))
        assert provider.fetch_matching_tx(TX, RECIPIENT.lower(), AMOUNT) is None

    def test_absent_tx_is_absent(self, provider):
        rpc_stub(provider, {'eth_getTransactionByHash': None})
        assert provider.fetch_matching_tx(TX, RECIPIENT.lower(), AMOUNT) is None

    def test_receipt_unavailable_is_unknown_not_absent(self, provider, contract):
        # 'unknown' must never read as 'no such payment' — that verdict slashes a paying miner.
        with pytest.raises(ProviderUnreachableError):
            rpc_stub(provider, dict(mined(contract), eth_getTransactionReceipt=None)).fetch_matching_tx(
                TX, RECIPIENT.lower(), AMOUNT
            )

    def test_missing_block_timestamp_is_unknown_not_absent(self, provider, contract):
        with pytest.raises(ProviderUnreachableError):
            rpc_stub(provider, mined(contract, block={'hash': BLOCK_HASH})).fetch_matching_tx(
                TX, RECIPIENT.lower(), AMOUNT
            )

    def test_an_impostor_contract_transfer_never_settles_the_leg(self, provider, contract, spoke):
        """Right recipient, right amount, well-formed Transfer — from the wrong contract. Only the
        pinned address rejects it, and the reject must survive the re-verify."""
        rpc_stub(provider, mined(contract, logs=[transfer_log(spoke.impostor)]))
        assert provider.fetch_matching_tx(TX, RECIPIENT.lower(), AMOUNT) is None
        assert provider._settled_cache == {}
        assert provider.fetch_matching_tx(TX, RECIPIENT.lower(), AMOUNT) is None

    def test_pending_transfer_to_an_impostor_contract_rejected(self, provider, spoke):
        calldata = SEL_TRANSFER + topic(RECIPIENT).removeprefix('0x') + f'{AMOUNT:064x}'
        pending = {'hash': TX, 'from': SENDER, 'to': spoke.impostor, 'input': calldata, 'blockNumber': None}
        rpc_stub(provider, {'eth_getTransactionByHash': pending})
        assert provider.fetch_matching_tx(TX, RECIPIENT.lower(), AMOUNT) is None


class TestIssuerGates:
    """Circle's FiatToken v2: isBlacklisted/paused answer — probed live on all four deployments."""

    def views(self, provider, blacklisted=False, paused=False):
        def call(params):
            hit = blacklisted if params[0]['data'].startswith(SEL_IS_BLACKLISTED) else paused
            return f'0x{int(hit):064x}'

        return rpc_stub(provider, {'eth_call': call, 'eth_blockNumber': hex(BLOCK)})

    def test_clean_dest_passes_reserve(self, provider):
        assert self.views(provider).can_deliver_to(RECIPIENT, AMOUNT) is True

    @pytest.mark.parametrize('gate', ('blacklisted', 'paused'))
    def test_issuer_freeze_bounces_reserve_and_defers_the_slash(self, provider, gate):
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
        'eth_getBlockByNumber': {'baseFeePerGas': hex(25 * 10**9)},
        'eth_maxPriorityFeePerGas': hex(2 * 10**9),
        'eth_getTransactionCount': '0x0',
        'eth_getBalance': hex(10**18),
        'eth_estimateGas': hex(62_989),  # measured: cold-recipient FiatToken transfer, both mainnets
        'eth_call': f'0x{AMOUNT:064x}',  # balanceOf
    }

    def test_no_key_refused(self, provider, spoke):
        assert provider.send_amount(RECIPIENT, AMOUNT) is None
        assert f'{spoke.row.env_prefix}_PRIVATE_KEY' in provider.last_send_error

    def test_key_mismatch_refused(self, provider, spoke, monkeypatch):
        monkeypatch.setenv(f'{spoke.row.env_prefix}_PRIVATE_KEY', TEST_KEY)
        assert provider.send_amount(RECIPIENT, AMOUNT, from_address=RECIPIENT) is None
        assert 'key mismatch' in provider.last_send_error

    def test_insufficient_token_balance_refused(self, provider, spoke, monkeypatch):
        monkeypatch.setenv(f'{spoke.row.env_prefix}_PRIVATE_KEY', TEST_KEY)
        rpc_stub(provider, dict(self.SEND, eth_call=f'0x{AMOUNT - 1:064x}'))
        assert provider.send_amount(RECIPIENT, AMOUNT) is None
        assert 'insufficient balance' in provider.last_send_error

    def test_gas_poor_miner_refuses_before_broadcasting(self, provider, spoke, monkeypatch):
        # USDC-rich but holding no host coin for gas: refuse here rather than burn a revert.
        monkeypatch.setenv(f'{spoke.row.env_prefix}_PRIVATE_KEY', TEST_KEY)
        rpc_stub(provider, dict(self.SEND, eth_getBalance=hex(10**12)))
        assert provider.send_amount(RECIPIENT, AMOUNT) is None
        assert 'Insufficient gas balance' in provider.last_send_error

    def test_broadcast_returns_a_hash(self, provider, spoke, monkeypatch):
        monkeypatch.setenv(f'{spoke.row.env_prefix}_PRIVATE_KEY', TEST_KEY)
        rpc_stub(provider, dict(self.SEND, eth_sendRawTransaction=TX))
        assert provider.send_amount(RECIPIENT, AMOUNT) == (TX, 0)


class TestDepositScanner:
    def test_scan_window_covers_the_lookback(self, provider, contract):
        seen = {}

        def logs(params):
            seen.update(params[0])
            return [transfer_log(contract)]

        assert provider.SCAN_LOOKBACK_BLOCKS == 300  # 300s // 1s-per-block
        rpc_stub(provider, {'eth_blockNumber': hex(10_000), 'eth_getLogs': logs})
        assert provider.find_recent_outgoing(SENDER, RECIPIENT, AMOUNT) == TX
        assert int(seen['fromBlock'], 16) == 10_000 - 300 + 1
        assert seen['address'] == contract

    def test_failed_range_parks_the_cursor(self, provider):
        def boom(params):
            raise ConnectionError('down')

        rpc_stub(provider, {'eth_blockNumber': hex(10_000), 'eth_getLogs': boom})
        assert provider.find_recent_outgoing(SENDER, RECIPIENT, AMOUNT) is None
        # Never leap a range the scan could not read — the deposit in it must stay reachable.
        key = (SENDER.lower(), RECIPIENT.lower(), AMOUNT)
        assert provider.scan_cursors[key] == 10_000 - provider.SCAN_LOOKBACK_BLOCKS
