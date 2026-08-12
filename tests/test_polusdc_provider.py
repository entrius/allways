"""PolUsdc unit tests — all offline (RPC layer mocked, signing is pure crypto).

Generic ERC-20 behaviour is proven by the arbusdc and ethusdc suites; this covers what is
polusdc-specific. Two things, chiefly:

- **The shared env identity.** polusdc rides Polygon's row alongside native POL, so ONE
  POL_NETWORK moves both. A second prefix here would leave polusdc reading an unset var,
  silently defaulting to mainnet, and a testnet miner would pay real USDC against test swaps.
- **The pin.** Bridged USDC.e on Polygon answers ``symbol() == 'USDC'`` and the whole FiatToken
  freeze surface, so the boot probe cannot tell it from Circle's native deployment — only the
  registry's asset_locator does. The address is the safety property; it is pinned here.
"""

import pytest

from allways.assets.asset import ProviderUnreachableError
from allways.assets.erc20 import SEL_IS_BLACKLISTED, SEL_TRANSFER, TESTNET_TOKEN_CONTRACTS, TRANSFER_TOPIC0
from allways.assets.pol import Pol
from allways.assets.polusdc import PolUsdc
from allways.chains import CHAIN_POL, CHAIN_POLUSDC, get_chain_def
from allways.constants import LAUNCH_SPOKES

TEST_KEY = '0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80'  # hardhat account #0
SENDER = '0x' + '11' * 20
RECIPIENT = '0x70997970C51812dc3A010C7d01b50e0d17dc79C8'
CONTRACT = TESTNET_TOKEN_CONTRACTS['polusdc']['amoy']
# Bridged USDC.e — real, live, and symbol-identical to the native token. Pinning it would make
# miners pay in an asset nobody quoted, so it stands in for the counterfeit here.
BRIDGED = '0x2791Bca1f2de4661eD88A30C99A7a9449Aa84174'
TX = '0x' + 'ab' * 32
BLOCK_HASH = '0x' + 'cd' * 32
AMOUNT = 150_000_000  # 150 USDC in µUSDC
AMOY_ID = 80_002


@pytest.fixture
def provider(monkeypatch):
    monkeypatch.setenv('POL_NETWORK', 'amoy')
    for var in ('POL_RPC_URLS', 'POL_PRIVATE_KEY', 'POLUSDC_TOKEN_CONTRACT'):
        monkeypatch.delenv(var, raising=False)
    return PolUsdc()


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


class TestRegistryRow:
    def test_row_is_a_launch_spoke_on_polygons_identity(self, provider):
        assert provider.chain_def is CHAIN_POLUSDC is get_chain_def('polusdc')
        assert 'polusdc' in LAUNCH_SPOKES
        assert (CHAIN_POLUSDC.env_prefix, CHAIN_POLUSDC.host_chain) == (CHAIN_POL.env_prefix, CHAIN_POL.host_chain)
        # CHAIN_POL owns POL_NETWORK; a second declaration renders a duplicate CLI row.
        assert CHAIN_POLUSDC.networks == ()

    def test_denomination_is_pinned(self):
        # decimals is the only row field with no other allways-side guard: das mirrors it, but
        # das's drift gate runs only after this merges, so a typo would otherwise ship green.
        assert (CHAIN_POLUSDC.decimals, CHAIN_POLUSDC.native_unit) == (6, 'µUSDC')

    def test_the_pinned_contract_is_circles_native_deployment(self):
        # The bridged token is symbol-identical and answers the same freeze surface, so nothing
        # downstream can tell them apart — this pin is the whole defence.
        assert CHAIN_POLUSDC.asset_locator == '0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359'
        assert CHAIN_POLUSDC.asset_locator.lower() != BRIDGED.lower()
        assert CHAIN_POLUSDC.refusal_checks == ('isBlacklisted(address)', 'paused()')

    def test_finality_and_clock_match_the_chain_it_shares(self):
        # Two assets on one chain must not disagree about its reorg depth or its clock. Compared
        # against CHAIN_POL rather than against literals, so restating a number cannot drift it.
        assert (
            CHAIN_POLUSDC.min_confirmations,
            CHAIN_POLUSDC.seconds_per_block,
            CHAIN_POLUSDC.replay_grace_secs,
        ) == (CHAIN_POL.min_confirmations, CHAIN_POL.seconds_per_block, CHAIN_POL.replay_grace_secs)
        # 100 confs x 1.5s real = 150s, inside the program's 600s default fulfillment grace.
        assert CHAIN_POLUSDC.min_confirmations * 1.5 < 600


class TestSharedEnvIdentity:
    @pytest.mark.parametrize('network,chain_id', (('mainnet', 137), ('amoy', AMOY_ID)))
    def test_one_pol_network_moves_both_assets(self, monkeypatch, network, chain_id):
        monkeypatch.setenv('POL_NETWORK', network)
        assert PolUsdc().chain.chain_id == Pol().chain.chain_id == chain_id

    def test_token_contract_follows_the_shared_network(self, provider, monkeypatch):
        assert provider.token_contract == CONTRACT  # amoy deployment, off POL_NETWORK
        monkeypatch.setenv('POL_NETWORK', 'mainnet')
        assert PolUsdc().token_contract == CHAIN_POLUSDC.asset_locator

    def test_env_override_wins(self, monkeypatch):
        monkeypatch.setenv('POLUSDC_TOKEN_CONTRACT', '0x' + '42' * 20)
        assert PolUsdc().token_contract == '0x' + '42' * 20

    def test_unknown_network_raises(self, monkeypatch):
        monkeypatch.setenv('POL_NETWORK', 'amoi')
        with pytest.raises(ValueError, match='POL_NETWORK'):
            PolUsdc()

    def test_wrong_network_endpoint_fails_startup(self, provider):
        # Amoy configured, a mainnet endpoint answering: reject outright rather than quietly
        # verify legs against the wrong chain.
        rpc_stub(provider, {'eth_chainId': '0x89'})
        with pytest.raises(ConnectionError, match=f'expected {AMOY_ID}'):
            provider.check_connection(require_send=False)

    def test_codeless_contract_fails_startup(self, provider):
        rpc_stub(provider, {'eth_chainId': hex(AMOY_ID), 'eth_blockNumber': '0x10', 'eth_getCode': '0x'})
        with pytest.raises(ConnectionError, match='no code'):
            provider.check_connection(require_send=False)


class TestVerification:
    def test_settled_transfer_matches_with_sender_pinned(self, provider):
        rpc_stub(provider, mined())
        info = provider.fetch_matching_tx(TX, RECIPIENT.lower(), AMOUNT)
        assert info.confirmed and info.amount == AMOUNT and info.block_time == 0x64
        assert info.sender == SENDER.lower()

    def test_below_min_confs_unconfirmed(self, provider):
        rpc_stub(provider, mined(tip=1_000_098))  # 99 confs of the 100 required
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

    def test_a_bridged_usdc_transfer_never_settles_the_leg(self, provider):
        """USDC.e pays the right recipient the right amount and logs a well-formed Transfer.
        Only the pinned address rejects it — and the reject must survive the re-verify, since
        the settled cache is written from a pinned-contract match alone."""
        rpc_stub(provider, mined(logs=[transfer_log(contract=BRIDGED)]))
        assert provider.fetch_matching_tx(TX, RECIPIENT.lower(), AMOUNT) is None
        assert provider._settled_cache == {}
        assert provider.fetch_matching_tx(TX, RECIPIENT.lower(), AMOUNT) is None

    def test_pending_transfer_to_bridged_usdc_rejected(self, provider):
        calldata = SEL_TRANSFER + topic(RECIPIENT).removeprefix('0x') + f'{AMOUNT:064x}'
        pending = {'hash': TX, 'from': SENDER, 'to': BRIDGED, 'input': calldata, 'blockNumber': None}
        rpc_stub(provider, {'eth_getTransactionByHash': pending})
        assert provider.fetch_matching_tx(TX, RECIPIENT.lower(), AMOUNT) is None


class TestIssuerGates:
    """USDC is Circle's FiatToken, so isBlacklisted/paused answer — verified live on both networks."""

    def views(self, provider, blacklisted=False, paused=False):
        def call(params):
            hit = blacklisted if params[0]['data'].startswith(SEL_IS_BLACKLISTED) else paused
            return f'0x{int(hit):064x}'

        return rpc_stub(provider, {'eth_call': call, 'eth_blockNumber': hex(1_000_000)})

    def test_clean_dest_passes_reserve(self, provider):
        assert self.views(provider).can_deliver_to(RECIPIENT, AMOUNT) is True

    @pytest.mark.parametrize('gate', ('blacklisted', 'paused'))
    def test_issuer_freeze_bounces_reserve_and_defers_the_slash(self, provider, gate):
        # Freezing the payout address after reserve makes delivery impossible through no fault
        # of the miner — positive evidence, never a slash.
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
        'eth_getBlockByNumber': {'baseFeePerGas': hex(250 * 10**9)},  # Polygon's sustained base fee
        'eth_maxPriorityFeePerGas': hex(30 * 10**9),
        'eth_getTransactionCount': '0x0',
        'eth_getBalance': hex(10**18),
        'eth_estimateGas': hex(61_000),  # measured on a real mainnet USDC transfer
        'eth_call': f'0x{AMOUNT:064x}',  # balanceOf
    }

    def test_no_key_refused(self, provider):
        assert provider.send_amount(RECIPIENT, AMOUNT) is None
        assert 'POL_PRIVATE_KEY' in provider.last_send_error

    def test_key_mismatch_refused(self, provider, monkeypatch):
        monkeypatch.setenv('POL_PRIVATE_KEY', TEST_KEY)
        assert provider.send_amount(RECIPIENT, AMOUNT, from_address=RECIPIENT) is None
        assert 'key mismatch' in provider.last_send_error

    def test_insufficient_token_balance_refused(self, provider, monkeypatch):
        monkeypatch.setenv('POL_PRIVATE_KEY', TEST_KEY)
        rpc_stub(provider, dict(self.SEND, eth_call=f'0x{AMOUNT - 1:064x}'))
        assert provider.send_amount(RECIPIENT, AMOUNT) is None
        assert 'insufficient balance' in provider.last_send_error

    def test_gas_poor_miner_refuses_before_broadcasting(self, provider, monkeypatch):
        # USDC-rich but POL-poor: refuse here rather than burn a revert. Polygon's gas is cheap
        # in dollars and expensive in POL units, so this is a live failure mode, not a theory.
        monkeypatch.setenv('POL_PRIVATE_KEY', TEST_KEY)
        rpc_stub(provider, dict(self.SEND, eth_getBalance=hex(10**15)))
        assert provider.send_amount(RECIPIENT, AMOUNT) is None
        assert 'Insufficient gas balance' in provider.last_send_error

    def test_broadcast_returns_a_hash(self, provider, monkeypatch):
        monkeypatch.setenv('POL_PRIVATE_KEY', TEST_KEY)
        rpc_stub(provider, dict(self.SEND, eth_sendRawTransaction=TX))
        assert provider.send_amount(RECIPIENT, AMOUNT) == (TX, 0)


class TestDepositScanner:
    def test_scan_window_is_the_full_unbounded_lookback(self, provider):
        """C8: Erc20 has no MAX_WALK_BLOCKS, so this whole span goes out as ONE eth_getLogs.

        300 blocks is served by publicnode on both Polygon networks and refused by mainnet drpc
        past ~100, so the scanner fails over on every call and runs effectively single-rung.
        Pinned so C8's span cap has a test to change rather than a silent behaviour to break."""
        seen = {}

        def logs(params):
            seen.update(params[0])
            return [transfer_log()]

        assert provider.SCAN_LOOKBACK_BLOCKS == 300  # 300s // 1s-per-block
        rpc_stub(provider, {'eth_blockNumber': hex(10_000), 'eth_getLogs': logs})
        assert provider.find_recent_outgoing(SENDER, RECIPIENT, AMOUNT) == TX
        assert int(seen['fromBlock'], 16) == 10_000 - 300 + 1
        assert seen['address'] == CONTRACT

    def test_failed_range_parks_the_cursor(self, provider):
        def boom(params):
            raise ConnectionError('down')

        rpc_stub(provider, {'eth_blockNumber': hex(10_000), 'eth_getLogs': boom})
        assert provider.find_recent_outgoing(SENDER, RECIPIENT, AMOUNT) is None
        # Never leap a range the scan could not read — the deposit in it must stay reachable.
        key = (SENDER.lower(), RECIPIENT.lower(), AMOUNT)
        assert provider.scan_cursors[key] == 10_000 - provider.SCAN_LOOKBACK_BLOCKS
