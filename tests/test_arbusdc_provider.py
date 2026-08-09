"""Erc20/ArbUsdc unit tests — all offline (RPC layer mocked, signing is pure crypto).

Covers the token-specific hazards: settlement truth is the Transfer log of the PINNED
contract (never tx.value — F2), the provable payer is the log's `from` topic (F5),
dual-balance sends (F6), and the Circle-shaped delivery gates (blacklist + pause, no
getCode — F3). Shared EVM plumbing (failover, proofs, casing) is covered by the ETH suite.
"""

from typing import Optional

import pytest

from allways.assets.base import ProviderUnreachableError
from allways.assets.erc20 import (
    SEL_BALANCE_OF,
    SEL_IS_BLACKLISTED,
    SEL_PAUSED,
    SEL_TRANSFER,
    TRANSFER_TOPIC0,
    ArbUsdc,
)
from allways.chains import CHAIN_ARBUSDC

# Well-known dev key (hardhat account #0) — never funded on mainnet, deterministic address.
TEST_KEY = '0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80'
TEST_ADDR = '0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266'

CONTRACT = '0x75faf114eafb1BDbe2F0316DF893fd58CE46AA4d'  # Circle USDC, Arbitrum Sepolia
SENDER = '0x' + '11' * 20
RECIPIENT = '0x70997970C51812dc3A010C7d01b50e0d17dc79C8'
TX = '0x' + 'ab' * 32
BLOCK_HASH = '0x' + 'cd' * 32
AMOUNT = 150_000_000  # 150 USDC in µUSDC


@pytest.fixture
def provider(monkeypatch):
    monkeypatch.setenv('ARBUSDC_NETWORK', 'sepolia')
    for var in ('ARBUSDC_RPC_URLS', 'ARBUSDC_PRIVATE_KEY', 'ARBUSDC_TOKEN_CONTRACT'):
        monkeypatch.delenv(var, raising=False)
    return ArbUsdc()


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


def mined_tx_responses(logs=None, status='0x1', tip=1_000_100, receipt: Optional[dict] = ...):
    if logs is None:
        logs = [transfer_log()]
    if receipt is ...:
        receipt = {'status': status, 'blockNumber': '0xf4240', 'blockHash': BLOCK_HASH, 'logs': logs}
    return {
        'eth_getTransactionByHash': {
            'hash': TX,
            'from': SENDER,
            'to': CONTRACT,
            'blockNumber': '0xf4240',  # 1_000_000
            'blockHash': BLOCK_HASH,
        },
        'eth_getTransactionReceipt': receipt,
        'eth_blockNumber': hex(tip),
        'eth_getBlockByNumber': {'hash': BLOCK_HASH, 'timestamp': '0x64'},
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


class TestTransferLogVerification:
    def test_settled_transfer_matches(self, provider):
        rpc_stub(provider, mined_tx_responses())
        info = provider.verify_transaction(TX, RECIPIENT, AMOUNT)
        assert info is not None
        assert info.amount == AMOUNT
        assert info.sender == SENDER
        assert info.confirmed  # 101 confs >= 90
        assert info.block_time == 100

    def test_unconfirmed_below_min_confs(self, provider):
        rpc_stub(provider, mined_tx_responses(tip=1_000_010))
        info = provider.verify_transaction(TX, RECIPIENT, AMOUNT)
        assert info is not None and not info.confirmed

    def test_wrong_contract_log_rejected(self, provider):
        # F2: a Transfer of USDC.e / a fake token must never satisfy a native-USDC leg.
        fake = transfer_log(contract='0x' + '99' * 20)
        rpc_stub(provider, mined_tx_responses(logs=[fake]))
        assert provider.verify_transaction(TX, RECIPIENT, AMOUNT) is None

    def test_wrong_recipient_rejected(self, provider):
        rpc_stub(provider, mined_tx_responses(logs=[transfer_log(recipient='0x' + '22' * 20)]))
        assert provider.verify_transaction(TX, RECIPIENT, AMOUNT) is None

    def test_underpay_rejected_overpay_accepted(self, provider):
        rpc_stub(provider, mined_tx_responses(logs=[transfer_log(amount=AMOUNT - 1)]))
        assert provider.verify_transaction(TX, RECIPIENT, AMOUNT) is None
        rpc_stub(provider, mined_tx_responses(logs=[transfer_log(amount=AMOUNT + 5)]))
        info = provider.verify_transaction(TX, RECIPIENT, AMOUNT)
        assert info is not None and info.amount == AMOUNT + 5

    def test_multi_log_tx_scans_all_logs(self, provider):
        noise = [
            transfer_log(contract='0x' + '99' * 20),  # other token
            transfer_log(recipient='0x' + '22' * 20),  # other recipient
            transfer_log(),  # the real leg
        ]
        rpc_stub(provider, mined_tx_responses(logs=noise))
        assert provider.verify_transaction(TX, RECIPIENT, AMOUNT) is not None

    def test_reverted_rejected(self, provider):
        rpc_stub(provider, mined_tx_responses(status='0x0'))
        assert provider.verify_transaction(TX, RECIPIENT, AMOUNT) is None

    def test_receipt_unavailable_raises(self, provider):
        rpc_stub(provider, mined_tx_responses(receipt=None))
        with pytest.raises(ProviderUnreachableError):
            provider.verify_transaction(TX, RECIPIENT, AMOUNT)

    def test_rpc_unreachable_raises(self, provider):
        def boom(params):
            raise ConnectionError('down')

        rpc_stub(provider, {'eth_getTransactionByHash': boom})
        with pytest.raises(ProviderUnreachableError):
            provider.verify_transaction(TX, RECIPIENT, AMOUNT)

    def test_missing_block_timestamp_raises(self, provider):
        responses = mined_tx_responses()
        responses['eth_getBlockByNumber'] = {'hash': BLOCK_HASH, 'timestamp': '0x0'}
        rpc_stub(provider, responses)
        with pytest.raises(ProviderUnreachableError):
            provider.verify_transaction(TX, RECIPIENT, AMOUNT)

    def test_not_found_returns_none(self, provider):
        rpc_stub(provider, {'eth_getTransactionByHash': None})
        assert provider.verify_transaction(TX, RECIPIENT, AMOUNT) is None

    def test_checksummed_recipient_matches_lowercase_topic(self, provider):
        rpc_stub(provider, mined_tx_responses())
        assert provider.verify_transaction(TX, RECIPIENT.lower(), AMOUNT) is not None
        assert provider.verify_transaction(TX, RECIPIENT, AMOUNT) is not None  # EIP-55 form

    def test_settled_cache_skips_receipt_refetch(self, provider):
        calls: dict = {}
        responses = mined_tx_responses()

        def fake_rpc(method, params, timeout=15, **kw):
            calls[method] = calls.get(method, 0) + 1
            return responses[method]

        provider.chain.eth_rpc = fake_rpc
        assert provider.verify_transaction(TX, RECIPIENT, AMOUNT) is not None
        provider.chain.clear_pass_tip()
        assert provider.verify_transaction(TX, RECIPIENT, AMOUNT) is not None
        assert calls['eth_getTransactionReceipt'] == 1  # second pass served from the cache

    def test_settled_cache_never_vouches_for_a_different_leg(self, provider):
        # Caught live (2026-08-07): the match lives in the receipt logs the cache skips, so a
        # cached settled read must re-check recipient + amount or any claim on the tx passes.
        rpc_stub(provider, mined_tx_responses())
        assert provider.verify_transaction(TX, RECIPIENT, AMOUNT) is not None
        provider.chain.clear_pass_tip()
        assert provider.verify_transaction(TX, '0x' + '22' * 20, AMOUNT) is None
        assert provider.verify_transaction(TX, RECIPIENT, AMOUNT + 1) is None
        assert provider.verify_transaction(TX, RECIPIENT, AMOUNT - 5) is not None  # overpay still fine


class TestSenderPin:
    def test_sender_from_log_topic_any_casing(self, provider):
        rpc_stub(provider, mined_tx_responses())
        checksummed = '0x' + SENDER.removeprefix('0x').upper()
        assert provider.verify_transaction(TX, RECIPIENT, AMOUNT, expected_sender=checksummed) is not None

    def test_sender_mismatch_rejected(self, provider):
        rpc_stub(provider, mined_tx_responses())
        assert provider.verify_transaction(TX, RECIPIENT, AMOUNT, expected_sender=TEST_ADDR) is None

    def test_tx_from_is_not_the_pin(self, provider):
        # The tx envelope sender (router/sponsor) differs from the log party — the log wins.
        responses = mined_tx_responses()
        responses['eth_getTransactionByHash'] = dict(responses['eth_getTransactionByHash'], **{'from': TEST_ADDR})
        rpc_stub(provider, responses)
        info = provider.verify_transaction(TX, RECIPIENT, AMOUNT, expected_sender=SENDER)
        assert info is not None and info.sender == SENDER

    def test_malformed_sender_topic_fails_closed(self, provider):
        bad = transfer_log()
        bad['topics'][1] = '0x' + 'ff' * 32  # nonzero prefix — not an address topic
        rpc_stub(provider, mined_tx_responses(logs=[bad]))
        assert provider.verify_transaction(TX, RECIPIENT, AMOUNT) is None

    def test_self_transfer_rejected(self, provider):
        rpc_stub(provider, mined_tx_responses(logs=[transfer_log(sender=RECIPIENT)]))
        assert provider.verify_transaction(TX, RECIPIENT, AMOUNT) is None


class TestPendingCalldata:
    def _pending(self, to=CONTRACT, recipient=RECIPIENT, amount=AMOUNT, sender=SENDER):
        return {
            'hash': TX,
            'from': sender,
            'to': to,
            'input': SEL_TRANSFER + topic(recipient).removeprefix('0x') + f'{amount:064x}',
            'blockNumber': None,
        }

    def test_pending_transfer_matches(self, provider):
        rpc_stub(provider, {'eth_getTransactionByHash': self._pending()})
        info = provider.verify_transaction(TX, RECIPIENT, AMOUNT)
        assert info is not None and not info.confirmed
        assert info.sender == SENDER and info.amount == AMOUNT

    def test_pending_to_other_contract_rejected(self, provider):
        rpc_stub(provider, {'eth_getTransactionByHash': self._pending(to='0x' + '99' * 20)})
        assert provider.verify_transaction(TX, RECIPIENT, AMOUNT) is None

    def test_pending_underpay_rejected(self, provider):
        rpc_stub(provider, {'eth_getTransactionByHash': self._pending(amount=AMOUNT - 1)})
        assert provider.verify_transaction(TX, RECIPIENT, AMOUNT) is None

    def test_pending_wrong_recipient_rejected(self, provider):
        rpc_stub(provider, {'eth_getTransactionByHash': self._pending(recipient='0x' + '22' * 20)})
        assert provider.verify_transaction(TX, RECIPIENT, AMOUNT) is None


class TestDeliveryGates:
    def test_clean_dest_passes(self, provider):
        rpc_stub(provider, {'eth_call': eth_call_views()})
        assert provider.can_deliver_to(RECIPIENT, AMOUNT) is True

    def test_blacklisted_dest_blocks_reserve(self, provider):
        rpc_stub(provider, {'eth_call': eth_call_views(blacklisted=[RECIPIENT])})
        assert provider.can_deliver_to(RECIPIENT, AMOUNT) is False

    def test_paused_token_blocks_reserve(self, provider):
        rpc_stub(provider, {'eth_call': eth_call_views(paused=True)})
        assert provider.can_deliver_to(RECIPIENT, AMOUNT) is False

    def test_reserve_gate_fails_open_on_rpc_trouble(self, provider):
        def boom(params):
            raise ConnectionError('down')

        rpc_stub(provider, {'eth_call': boom})
        assert provider.can_deliver_to(RECIPIENT, AMOUNT) is True

    def test_slash_gate_blacklisted_now(self, provider):
        rpc_stub(provider, {'eth_blockNumber': hex(1000), 'eth_call': eth_call_views(blacklisted=[RECIPIENT])})
        assert provider.delivery_refused(RECIPIENT, since_unix=0) is True

    def test_slash_gate_paused_token(self, provider):
        # A pause makes delivery impossible for EVERYONE through no fault of the miner —
        # the slash gate must defer exactly like the reserve gate refuses.
        rpc_stub(provider, {'eth_blockNumber': hex(1000), 'eth_call': eth_call_views(paused=True)})
        assert provider.delivery_refused(RECIPIENT, since_unix=0) is True

    def test_slash_gate_samples_history(self, provider):
        # Clean at latest, blacklisted at a sampled historical block (freeze lifted mid-window).
        def call(params):
            data = params[0]['data']
            if data.startswith(SEL_PAUSED):
                return '0x0'
            if data.startswith(SEL_IS_BLACKLISTED):
                return hex(int(params[1] != 'latest'))
            raise AssertionError(params)

        rpc_stub(provider, {'eth_blockNumber': hex(1000), 'eth_call': call})
        assert provider.delivery_refused(RECIPIENT, since_unix=0) is True

    def test_slash_gate_clean_is_false(self, provider):
        rpc_stub(provider, {'eth_blockNumber': hex(1000), 'eth_call': eth_call_views()})
        assert provider.delivery_refused(RECIPIENT, since_unix=0) is False

    def test_slash_gate_raises_on_rpc_trouble(self, provider):
        # The caller defers the slash — a flaky RPC postpones, never falsifies.
        def boom(params):
            raise ConnectionError('down')

        rpc_stub(provider, {'eth_blockNumber': hex(1000), 'eth_call': boom})
        with pytest.raises(Exception):
            provider.delivery_refused(RECIPIENT, since_unix=0)

    def test_slash_gate_survives_historical_probe_failure(self, provider):
        # Public nodes serve historical eth_call only ~a minute deep (verified live) — a failing
        # sample is skipped so a shallow endpoint can't defer every slash on the pair forever.
        def call(params):
            if params[1] == 'latest':
                return '0x0'
            raise ConnectionError('pruned state')

        rpc_stub(provider, {'eth_blockNumber': hex(1000), 'eth_call': call})
        assert provider.delivery_refused(RECIPIENT, since_unix=0) is False


class TestSendGuards:
    def _send_responses(self, token_balance=10 * AMOUNT, gas_balance=10**18, est='0xfde8'):  # est 65k
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
        assert 'ARBUSDC_PRIVATE_KEY' in provider.last_send_error

    def test_key_mismatch_refused(self, provider, monkeypatch):
        monkeypatch.setenv('ARBUSDC_PRIVATE_KEY', TEST_KEY)
        assert provider.send_amount(RECIPIENT, AMOUNT, from_address=RECIPIENT) is None
        assert 'key mismatch' in provider.last_send_error

    def test_insufficient_token_balance_refused(self, provider, monkeypatch):
        monkeypatch.setenv('ARBUSDC_PRIVATE_KEY', TEST_KEY)
        rpc_stub(provider, self._send_responses(token_balance=AMOUNT - 1))
        assert provider.send_amount(RECIPIENT, AMOUNT) is None
        assert 'Insufficient ARBUSDC' in provider.last_send_error

    def test_insufficient_gas_balance_refused(self, provider, monkeypatch):
        # F6: token-rich but gas-poor must refuse BEFORE broadcasting a doomed transfer.
        monkeypatch.setenv('ARBUSDC_PRIVATE_KEY', TEST_KEY)
        rpc_stub(provider, self._send_responses(gas_balance=10))
        assert provider.send_amount(RECIPIENT, AMOUNT) is None
        assert 'Insufficient gas balance' in provider.last_send_error

    def test_insufficient_balance_diagnosed_before_estimator_revert(self, provider, monkeypatch):
        # An underfunded transfer reverts in the estimator too — the balance check runs first
        # so a token-poor miner reads "Insufficient", not "destination refuses".
        monkeypatch.setenv('ARBUSDC_PRIVATE_KEY', TEST_KEY)
        responses = self._send_responses(token_balance=AMOUNT - 1)

        def revert(params):
            raise RuntimeError('execution reverted: transfer amount exceeds balance')

        responses['eth_estimateGas'] = revert
        rpc_stub(provider, responses)
        assert provider.send_amount(RECIPIENT, AMOUNT) is None
        assert 'Insufficient ARBUSDC' in provider.last_send_error

    def test_reverting_transfer_refused(self, provider, monkeypatch):
        monkeypatch.setenv('ARBUSDC_PRIVATE_KEY', TEST_KEY)
        responses = self._send_responses()

        def revert(params):
            raise RuntimeError('execution reverted: Blacklistable: account is blacklisted')

        responses['eth_estimateGas'] = revert
        rpc_stub(provider, responses)
        assert provider.send_amount(RECIPIENT, AMOUNT) is None
        assert 'refuses' in provider.last_send_error

    def test_absurd_gas_estimate_refused(self, provider, monkeypatch):
        monkeypatch.setenv('ARBUSDC_PRIVATE_KEY', TEST_KEY)
        rpc_stub(provider, self._send_responses(est=hex(200_000)))  # ×1.2 > 150k cap
        assert provider.send_amount(RECIPIENT, AMOUNT) is None

    def test_happy_path_broadcasts_transfer_calldata(self, provider, monkeypatch):
        monkeypatch.setenv('ARBUSDC_PRIVATE_KEY', TEST_KEY)
        sent = {}
        responses = self._send_responses()

        def capture(params):
            sent['raw'] = params[0]
            return '0x' + 'ee' * 32

        responses['eth_sendRawTransaction'] = capture
        rpc_stub(provider, responses)
        result = provider.send_amount(RECIPIENT, AMOUNT, from_address=TEST_ADDR)
        assert result == ('0x' + 'ee' * 32, 0)
        # Dedup ledger keys by the PRE-broadcast precomputed txid, recorded before the send.
        assert list(provider.broadcasted_txids.values()) == [(RECIPIENT.lower(), AMOUNT, 1000)]
        # The signed payload carries transfer() calldata addressed to the token contract.
        assert SEL_TRANSFER.removeprefix('0x') in sent['raw']
        assert RECIPIENT.lower().removeprefix('0x') in sent['raw']

    def test_lowercase_contract_override_still_sends(self, provider, monkeypatch):
        # eth-account refuses a non-checksummed `to`; the sign path checksums it, so a
        # lowercase ARBUSDC_TOKEN_CONTRACT (env repoint, e2e fake) must not brick sends.
        monkeypatch.setenv('ARBUSDC_PRIVATE_KEY', TEST_KEY)
        provider.token_contract = provider.token_contract.lower()
        rpc_stub(provider, self._send_responses())
        assert provider.send_amount(RECIPIENT, AMOUNT, from_address=TEST_ADDR) is not None

    def test_prior_broadcast_reused_not_resent(self, provider, monkeypatch):
        monkeypatch.setenv('ARBUSDC_PRIVATE_KEY', TEST_KEY)
        prior_tx = '0x' + 'aa' * 32
        provider.broadcasted_txids[prior_tx] = (RECIPIENT.lower(), AMOUNT, 990)
        responses = self._send_responses()
        responses['eth_getTransactionByHash'] = {'hash': prior_tx, 'blockNumber': None}  # still in mempool

        def never(params):
            raise AssertionError('must not rebroadcast')

        responses['eth_sendRawTransaction'] = never
        rpc_stub(provider, responses)
        assert provider.send_amount(RECIPIENT, AMOUNT) == (prior_tx, 0)

    def test_stale_dedup_entry_expires(self, provider, monkeypatch):
        # An entry older than the lookback window is dropped, not reused (#461 class).
        monkeypatch.setenv('ARBUSDC_PRIVATE_KEY', TEST_KEY)
        prior_tx = '0x' + 'aa' * 32
        provider.broadcasted_txids[prior_tx] = (RECIPIENT.lower(), AMOUNT, 1000 - provider.SCAN_LOOKBACK_BLOCKS - 1)
        rpc_stub(provider, self._send_responses())
        result = provider.send_amount(RECIPIENT, AMOUNT)
        assert result is not None and result[0] != prior_tx
        assert prior_tx not in provider.broadcasted_txids


class TestScanner:
    def test_getlogs_hit_returns_hash(self, provider):
        found = '0x' + 'bb' * 32

        def logs(params):
            f = params[0]
            assert f['address'] == CONTRACT
            assert f['topics'] == [TRANSFER_TOPIC0, topic(TEST_ADDR), topic(RECIPIENT)]
            return [{'data': hex(AMOUNT), 'transactionHash': found}]

        rpc_stub(provider, {'eth_blockNumber': hex(1000), 'eth_getLogs': logs})
        assert provider.find_recent_outgoing(TEST_ADDR, RECIPIENT, AMOUNT) == found

    def test_underpaying_log_ignored_cursor_advances(self, provider):
        rpc_stub(provider, {'eth_blockNumber': hex(1000), 'eth_getLogs': [{'data': hex(1), 'transactionHash': TX}]})
        assert provider.find_recent_outgoing(TEST_ADDR, RECIPIENT, AMOUNT) is None
        key = (TEST_ADDR.lower(), RECIPIENT.lower(), AMOUNT)
        assert provider.scan_cursors[key] == 1000

    def test_failed_range_parks_cursor(self, provider):
        def boom(params):
            raise ConnectionError('range too large')

        rpc_stub(provider, {'eth_blockNumber': hex(1000), 'eth_getLogs': boom})
        assert provider.find_recent_outgoing(TEST_ADDR, RECIPIENT, AMOUNT) is None
        key = (TEST_ADDR.lower(), RECIPIENT.lower(), AMOUNT)
        # Parked just below the failed range start — the same span is retried next call.
        assert provider.scan_cursors[key] == 1000 - provider.SCAN_LOOKBACK_BLOCKS


class TestNetworkAndContract:
    def test_unknown_network_raises(self, monkeypatch):
        monkeypatch.setenv('ARBUSDC_NETWORK', 'seplia')
        with pytest.raises(ValueError, match='ARBUSDC_NETWORK'):
            ArbUsdc()

    def test_unset_defaults_to_mainnet_with_registry_contract(self, monkeypatch):
        for var in ('ARBUSDC_NETWORK', 'ARBUSDC_RPC_URLS', 'ARBUSDC_TOKEN_CONTRACT'):
            monkeypatch.delenv(var, raising=False)
        p = ArbUsdc()
        assert p.chain.network == 'mainnet'
        assert p.token_contract == CHAIN_ARBUSDC.asset_locator

    def test_sepolia_uses_testnet_deployment(self, provider):
        assert provider.token_contract == CONTRACT

    def test_env_override_wins(self, monkeypatch):
        monkeypatch.setenv('ARBUSDC_NETWORK', 'sepolia')
        monkeypatch.setenv('ARBUSDC_TOKEN_CONTRACT', '0x' + '42' * 20)
        assert ArbUsdc().token_contract == '0x' + '42' * 20

    def test_lookback_window_is_time_based(self, provider):
        # 1s blocks → the ≈5 min dedup/scan window is 300 blocks, not ETH's 25.
        assert provider.SCAN_LOOKBACK_BLOCKS == 300

    def test_chain_binding(self, provider):
        assert provider.chain_def is CHAIN_ARBUSDC
        assert provider.chain.chain_id == 421_614
        # Composed, not fused: the first non-fused asset — a second Arbitrum token shares the chain.
        assert provider.chain is not provider


class TestCheckConnection:
    def test_chain_id_mismatch_fails(self, provider):
        rpc_stub(provider, {'eth_chainId': '0x1', 'eth_blockNumber': '0x10'})
        with pytest.raises(ConnectionError, match='chain id'):
            provider.check_connection(require_send=False)

    def test_missing_key_fails_when_send_required(self, provider):
        with pytest.raises(ConnectionError, match='ARBUSDC_PRIVATE_KEY'):
            provider.check_connection(require_send=True)

    def test_wrong_network_endpoint_deeper_in_ladder_fails_boot(self, provider):
        # Every configured endpoint is probed at startup — a wrong-network URL at position 2
        # would otherwise surface only mid-outage, quietly serving wrong-chain verifications.
        def fake_rpc(method, params, timeout=15, bases=None, **kw):
            if method == 'eth_chainId':
                return '0x1' if bases and 'drpc' in bases[0] else hex(421_614)
            return '0x10'

        provider.chain.eth_rpc = fake_rpc
        with pytest.raises(ConnectionError, match='drpc serves chain id 1'):
            provider.check_connection(require_send=False)

    def test_codeless_contract_fails(self, provider):
        rpc_stub(provider, {'eth_chainId': hex(421_614), 'eth_blockNumber': '0x10', 'eth_getCode': '0x'})
        with pytest.raises(ConnectionError, match='no code'):
            provider.check_connection(require_send=False)

    def test_happy_path_paused_only_warns(self, provider):
        rpc_stub(
            provider,
            {
                'eth_chainId': hex(421_614),
                'eth_blockNumber': '0x10',
                'eth_getCode': '0x6080',
                'eth_call': eth_call_views(paused=True),
            },
        )
        provider.check_connection(require_send=False)  # paused is a warning, not a failure
