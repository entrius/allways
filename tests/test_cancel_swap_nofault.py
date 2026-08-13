"""Unit tests for the no-fault-cancel evidence seam (`Asset.cancel_evidence`).

This is the SOUND per-chain refusal verdict that terminates a swap with no slash (distinct from
`delivery_refused`, a mere deferral hint). Each arm must return a CANCEL_REASON_* only on evidence
strong enough that a wrong verdict can't hand a defaulting miner a free pass:
  - native EVM: the miner's OWN reverted delivery tx, and only with correct dest + value + gas-floor
    (the gas-limit floor is the anti-fake clause — under-gassing must NOT manufacture a refusal);
  - ERC-20: attested issuer blacklist/pause (dest-attributing, unlike an ambiguous reverted transfer);
  - Solana: reserved-account membership (offline, deterministic);
  - passive chains (BTC/TAO) and the base default: never — they can't refuse a payout.
"""

from allways.assets.eth import Ether
from allways.assets.ethusdc import EthUsdc
from allways.assets.sol import RESERVED_ACCOUNTS, Sol
from allways.constants import (
    CANCEL_REASON_ERC20_BLACKLIST,
    CANCEL_REASON_ERC20_PAUSED,
    CANCEL_REASON_EVM_REVERT,
    CANCEL_REASON_SOL_RESERVED,
)

DEST = '0x70997970C51812dc3A010C7d01b50e0d17dc79C8'
TX = '0x' + 'ab' * 32
AMOUNT = 256  # 0x100


def _evm(monkeypatch):
    monkeypatch.setenv('ETH_NETWORK', 'mainnet')
    monkeypatch.delenv('ETH_RPC_URLS', raising=False)
    monkeypatch.delenv('ETH_PRIVATE_KEY', raising=False)
    return Ether()


def _stub(provider, tx, receipt):
    def fake_rpc(method, params, timeout=15, **kw):
        return {'eth_getTransactionByHash': tx, 'eth_getTransactionReceipt': receipt}[method]

    provider.chain.eth_rpc = fake_rpc
    return provider


def _tx(to=DEST, value=AMOUNT, gas=100_000):
    return {'to': to, 'value': hex(value), 'gas': hex(gas)}


class TestEvmCancelEvidence:
    def test_reverted_with_correct_params_is_evidence(self, monkeypatch):
        p = _stub(_evm(monkeypatch), _tx(), {'status': '0x0'})
        assert p.cancel_evidence(DEST, AMOUNT, TX) == CANCEL_REASON_EVM_REVERT

    def test_succeeded_tx_is_not_a_refusal(self, monkeypatch):
        p = _stub(_evm(monkeypatch), _tx(), {'status': '0x1'})
        assert p.cancel_evidence(DEST, AMOUNT, TX) is None

    def test_wrong_destination_rejected(self, monkeypatch):
        p = _stub(_evm(monkeypatch), _tx(to='0x' + '11' * 20), {'status': '0x0'})
        assert p.cancel_evidence(DEST, AMOUNT, TX) is None

    def test_underpaid_value_rejected(self, monkeypatch):
        p = _stub(_evm(monkeypatch), _tx(value=AMOUNT - 1), {'status': '0x0'})
        assert p.cancel_evidence(DEST, AMOUNT, TX) is None

    def test_under_gassed_is_not_accepted_as_refusal(self, monkeypatch):
        # The load-bearing anti-fake clause: a miner must not under-gas to manufacture a "refused" verdict.
        p = _stub(_evm(monkeypatch), _tx(gas=90_000), {'status': '0x0'})
        assert p.cancel_evidence(DEST, AMOUNT, TX) is None

    def test_no_tx_hash_is_none(self, monkeypatch):
        assert _evm(monkeypatch).cancel_evidence(DEST, AMOUNT, None) is None

    def test_missing_receipt_is_none(self, monkeypatch):
        p = _stub(_evm(monkeypatch), _tx(), None)
        assert p.cancel_evidence(DEST, AMOUNT, TX) is None


class TestErc20CancelEvidence:
    def _provider(self, monkeypatch, blacklisted, paused):
        monkeypatch.setenv('ETH_NETWORK', 'sepolia')
        monkeypatch.delenv('ETH_RPC_URLS', raising=False)
        p = EthUsdc()
        from allways.assets.erc20 import SEL_IS_BLACKLISTED

        p._eth_call = lambda selector, address='', block='latest': (
            blacklisted if selector == SEL_IS_BLACKLISTED else paused
        )
        return p

    def test_blacklisted_dest(self, monkeypatch):
        p = self._provider(monkeypatch, blacklisted=1, paused=0)
        assert p.cancel_evidence(DEST, AMOUNT) == CANCEL_REASON_ERC20_BLACKLIST

    def test_paused_token(self, monkeypatch):
        p = self._provider(monkeypatch, blacklisted=0, paused=1)
        assert p.cancel_evidence(DEST, AMOUNT) == CANCEL_REASON_ERC20_PAUSED

    def test_clean_dest_is_none(self, monkeypatch):
        p = self._provider(monkeypatch, blacklisted=0, paused=0)
        assert p.cancel_evidence(DEST, AMOUNT) is None

    def test_blacklist_takes_precedence_over_pause(self, monkeypatch):
        p = self._provider(monkeypatch, blacklisted=1, paused=1)
        assert p.cancel_evidence(DEST, AMOUNT) == CANCEL_REASON_ERC20_BLACKLIST


class TestSolCancelEvidence:
    def test_reserved_account_is_evidence(self):
        p = Sol(solana_rpc_url='fake://rpc')
        assert p.cancel_evidence(next(iter(RESERVED_ACCOUNTS)), 0) == CANCEL_REASON_SOL_RESERVED

    def test_normal_wallet_is_none(self):
        p = Sol(solana_rpc_url='fake://rpc')
        assert p.cancel_evidence('So11111111111111111111111111111111111111112', 0) is None


def test_passive_chain_base_default_is_none():
    # A passive-destination chain never executes at delivery, so it can never refuse. The base default
    # returns None and reads nothing off self, so btc/tao (which do not override) inherit "never refuses".
    from allways.assets.asset import Asset

    assert Asset.cancel_evidence(None, DEST, AMOUNT, None) is None
