"""PAX Gold unit tests — all offline (RPC layer mocked, signing is pure crypto).

Generic ERC-20 behaviour is proven by the arbusdc suite and the declared-refusal contract by
tests/test_erc20_refusal_checks.py; this covers what is paxg-specific. Three things:

  * PAXG is the first row to declare a NON-Circle freeze surface. Paxos uses isFrozen(address),
    and isBlacklisted(address) reverts on its contract — declaring Circle's surface here would
    make every probe revert and defer every slash on the pair forever.
  * Its unit is a troy ounce of gold, ~1000x an ETH unit. A token COUNT copied from another row
    makes the minimum leg absurd, and the pair silently never routes.
  * It rides Ethereum's env identity alongside eth and ethusdc. One ETH_NETWORK must move all
    of them.
"""

import pytest

from allways.assets.erc20 import SEL_TRANSFER, TRANSFER_TOPIC0, _refusal_call
from allways.assets.eth import Ether
from allways.assets.ethusdc import EthUsdc
from allways.assets.paxg import Paxg
from allways.chains import CHAIN_ETH, CHAIN_PAXG, get_chain_def
from allways.constants import LAUNCH_SPOKES

SENDER = '0x' + '11' * 20
RECIPIENT = '0x70997970C51812dc3A010C7d01b50e0d17dc79C8'  # mixed case on purpose
CONTRACT = CHAIN_PAXG.asset_locator
COUNTERFEIT = '0x' + '99' * 20
TX = '0x' + 'ab' * 32
BLOCK_HASH = '0x' + 'cd' * 32
AMOUNT = 2_500_000_000_000_000_000  # 2.5 PAXG in wei — 18 decimals
MAINNET_ID = 1

SEL_IS_FROZEN, _ = _refusal_call('isFrozen(address)')
SEL_PAUSED_, _ = _refusal_call('paused()')


@pytest.fixture
def provider(monkeypatch):
    monkeypatch.setenv('ETH_NETWORK', 'mainnet')
    for var in ('ETH_RPC_URLS', 'ETH_PRIVATE_KEY', 'PAXG_TOKEN_CONTRACT'):
        monkeypatch.delenv(var, raising=False)
    return Paxg()


def rpc_stub(provider, responses: dict):
    def fake_rpc(method, params, timeout=15, **kw):
        value = responses[method]
        return value(params) if callable(value) else value

    provider.chain.eth_rpc = fake_rpc
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


def mined(logs=None, status='0x1', tip=1_000_100) -> dict:
    return {
        'eth_getTransactionByHash': {'hash': TX, 'blockNumber': '0xf4240', 'blockHash': BLOCK_HASH},
        'eth_getTransactionReceipt': {
            'status': status,
            'blockNumber': '0xf4240',
            'blockHash': BLOCK_HASH,
            'logs': [transfer_log()] if logs is None else logs,
        },
        'eth_blockNumber': hex(tip),
        'eth_getBlockByNumber': {'hash': BLOCK_HASH, 'timestamp': '0x64'},
    }


class TestSharedEnvIdentity:
    def test_row_is_a_launch_spoke_on_ethereums_identity(self, provider):
        assert provider.chain_def is CHAIN_PAXG is get_chain_def('paxg')
        assert 'paxg' in LAUNCH_SPOKES
        assert (CHAIN_PAXG.env_prefix, CHAIN_PAXG.host_chain) == (CHAIN_ETH.env_prefix, CHAIN_ETH.host_chain)
        # Pinned here because nothing else on the allways side guards them: a typo'd decimals
        # ships green through this CI and is only caught by the das drift gate, after merge.
        assert (CHAIN_PAXG.decimals, CHAIN_PAXG.native_unit) == (18, 'aPAXG')
        # CHAIN_ETH owns ETH_NETWORK; a second declaration renders a duplicate CLI row.
        assert CHAIN_PAXG.networks == ()

    def test_one_eth_network_moves_every_asset_on_it(self, monkeypatch):
        monkeypatch.setenv('ETH_NETWORK', 'mainnet')
        assert Paxg().chain.chain_id == EthUsdc().chain.chain_id == Ether().chain.chain_id == MAINNET_ID

    def test_finality_and_clock_match_the_chain_it_shares(self):
        assert (CHAIN_PAXG.min_confirmations, CHAIN_PAXG.seconds_per_block, CHAIN_PAXG.replay_grace_secs) == (
            CHAIN_ETH.min_confirmations,
            CHAIN_ETH.seconds_per_block,
            CHAIN_ETH.replay_grace_secs,
        )
        # The implied leg stays inside the program's 600s default fulfillment grace.
        assert CHAIN_PAXG.min_confirmations * CHAIN_PAXG.seconds_per_block < 600

    def test_no_testnet_deployment_without_an_override(self, monkeypatch):
        # Paxos publishes no Sepolia PAXG (verified 2026-08-13). Failing loudly is correct — the
        # alternative is a testnet miner paying real mainnet PAXG against test swaps.
        monkeypatch.setenv('ETH_NETWORK', 'sepolia')
        monkeypatch.delenv('PAXG_TOKEN_CONTRACT', raising=False)
        with pytest.raises(ValueError, match='PAXG_TOKEN_CONTRACT'):
            Paxg()

    def test_env_override_wins(self, monkeypatch):
        monkeypatch.setenv('PAXG_TOKEN_CONTRACT', '0x' + '42' * 20)
        assert Paxg().token_contract == '0x' + '42' * 20


class TestPaxosFreezeSurface:
    """Paxos freezes with isFrozen, not Circle's isBlacklisted. Declaring the wrong one would make
    every probe revert, `delivery_refused` raise, and the caller defer every slash forever."""

    def test_row_declares_the_paxos_surface(self):
        assert CHAIN_PAXG.refusal_checks == ('isFrozen(address)', 'paused()')

    @pytest.mark.parametrize('refusing', (SEL_IS_FROZEN, SEL_PAUSED_))
    def test_each_declared_check_is_positive_evidence(self, provider, refusing):
        def rpc(method, params, **kw):
            if method == 'eth_blockNumber':
                return hex(1000)
            return hex(int(params[0]['data'].startswith(refusing)))

        provider.chain.eth_rpc = rpc
        assert provider.delivery_refused(RECIPIENT, since_unix=0) is True

    def test_circles_selector_is_never_probed(self, provider):
        # The regression guard for this whole row: PAXG reverts on isBlacklisted, so a probe for
        # it would raise out of delivery_refused and defer every slash on the pair.
        seen = []

        def rpc(method, params, **kw):
            if method == 'eth_blockNumber':
                return hex(1000)
            seen.append(params[0]['data'][:10])
            return f'0x{0:064x}'

        provider.chain.eth_rpc = rpc
        provider.delivery_refused(RECIPIENT, since_unix=0)
        assert set(seen) == {SEL_IS_FROZEN, SEL_PAUSED_}


class TestGoldUnitEconomics:
    """One PAXG is a troy ounce. Every constant that is a token COUNT elsewhere is a trap here."""

    def test_the_minimum_leg_is_a_fraction_of_an_ounce(self):
        # UNI's floor is 3 tokens; the same figure here would be three ounces of gold (>$12,000)
        # and the pair would silently never route. The floor covers dest-leg gas, nothing more.
        assert CHAIN_PAXG.min_onchain_amount == 2_000_000_000_000_000  # 0.002 PAXG


class TestVerification:
    def test_settled_transfer_matches_a_mixed_case_recipient_with_sender_pinned(self, provider):
        rpc_stub(provider, mined())
        info = provider.verify_transaction(TX, RECIPIENT, AMOUNT, expected_sender=SENDER)
        assert info is not None and info.confirmed and info.amount == AMOUNT

    def test_a_log_from_another_contract_never_matches(self, provider):
        # Settlement truth is the PINNED contract's Transfer log — a counterfeit token emitting
        # the same event must not pay a PAXG leg.
        rpc_stub(provider, mined(logs=[transfer_log(contract=COUNTERFEIT)]))
        assert provider.verify_transaction(TX, RECIPIENT, AMOUNT, expected_sender=SENDER) is None

    def test_a_reverted_tx_moved_no_funds(self, provider):
        rpc_stub(provider, mined(status='0x0', logs=[]))
        assert provider.verify_transaction(TX, RECIPIENT, AMOUNT, expected_sender=SENDER) is None

    def test_an_underpaying_transfer_is_rejected(self, provider):
        rpc_stub(provider, mined(logs=[transfer_log(amount=AMOUNT - 1)]))
        assert provider.verify_transaction(TX, RECIPIENT, AMOUNT, expected_sender=SENDER) is None

    def test_a_transfer_to_someone_else_is_rejected(self, provider):
        rpc_stub(provider, mined(logs=[transfer_log(recipient=COUNTERFEIT)]))
        assert provider.verify_transaction(TX, RECIPIENT, AMOUNT, expected_sender=SENDER) is None

    def test_the_pinned_sender_must_match(self, provider):
        rpc_stub(provider, mined())
        assert provider.verify_transaction(TX, RECIPIENT, AMOUNT, expected_sender=COUNTERFEIT) is None


class TestSendGuards:
    def test_send_without_a_key_refuses(self, provider):
        assert provider.send_amount(RECIPIENT, AMOUNT) is None
        assert 'ETH_PRIVATE_KEY' in provider.last_send_error

    def test_calldata_targets_the_pinned_contract(self, provider):
        assert SEL_TRANSFER.startswith('0xa9059cbb')
        assert provider.token_contract == CHAIN_PAXG.asset_locator
