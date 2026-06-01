"""verify_transaction must reject self-transfers (sender == recipient).

A same-wallet A->A send is never a real swap leg — the paying and receiving
parties are always distinct addresses. An operator who lines up a miner's
committed address with the user's receive address could otherwise fulfill a
swap with a self-send and manufacture fake volume. A->B self-flow between two
operator-owned wallets is indistinguishable on-chain and is left to economic /
reward-side limits, so it still passes here.
"""

from typing import Optional

from allways.chain_providers.base import ChainProvider, TransactionInfo
from allways.chains import CHAIN_TAO, ChainDefinition


class FakeProvider(ChainProvider):
    """Returns a single canned tx; only the base post-checks are exercised."""

    def __init__(self, tx: TransactionInfo):
        self._tx = tx

    def get_chain(self) -> ChainDefinition:
        return CHAIN_TAO

    def check_connection(self, **kwargs) -> None: ...

    def fetch_matching_tx(
        self, tx_hash, expected_recipient, expected_amount, block_hint=0, max_scan_blocks=150
    ) -> Optional[TransactionInfo]:
        return self._tx

    def get_current_block_height(self) -> Optional[int]:
        return 100

    def get_balance(self, address: str) -> int:
        return 0

    def is_valid_address(self, address: str) -> bool:
        return True

    def sign_from_proof(self, address, message, key=None) -> str:
        return ''

    def verify_from_proof(self, address, message, signature) -> bool:
        return True

    def send_amount(self, to_address, amount, from_address=None):
        return None


def _tx(sender: str, recipient: str) -> TransactionInfo:
    return TransactionInfo(
        tx_hash='0xabc', confirmed=True, sender=sender, recipient=recipient, amount=100, confirmations=6
    )


def test_self_transfer_is_rejected():
    provider = FakeProvider(_tx(sender='5Aaa', recipient='5Aaa'))
    result = provider.verify_transaction(
        tx_hash='0xabc',
        expected_recipient='5Aaa',
        expected_amount=100,
        expected_sender='5Aaa',
    )
    assert result is None


def test_cross_party_transfer_passes():
    provider = FakeProvider(_tx(sender='5Miner', recipient='5User'))
    result = provider.verify_transaction(
        tx_hash='0xabc',
        expected_recipient='5User',
        expected_amount=100,
        expected_sender='5Miner',
    )
    assert result is not None
    assert result.sender == '5Miner'


def test_self_transfer_rejected_even_without_expected_sender():
    # The dest-confirm path pins expected_sender, but guard A->A regardless.
    provider = FakeProvider(_tx(sender='5Aaa', recipient='5Aaa'))
    result = provider.verify_transaction(
        tx_hash='0xabc',
        expected_recipient='5Aaa',
        expected_amount=100,
    )
    assert result is None
