"""Tests for ChainProvider.verify_transaction — shared post-fetch logic."""

from typing import Any, Optional, Tuple
from unittest.mock import MagicMock

from allways.chain_providers.base import ChainProvider, TransactionInfo
from allways.chains import ChainDefinition

_TEST_CHAIN = ChainDefinition(
    id='btc',
    name='Bitcoin',
    native_unit='sat',
    decimals=8,
    env_prefix='BTC',
    min_confirmations=3,
)


class _FakeProvider(ChainProvider):
    def __init__(self, tx: Optional[TransactionInfo] = None):
        self._tx = tx

    def get_chain(self) -> ChainDefinition:
        return _TEST_CHAIN

    def check_connection(self, **kwargs) -> None:
        return None

    def fetch_matching_tx(
        self, tx_hash: str, expected_recipient: str, expected_amount: int, block_hint: int = 0
    ) -> Optional[TransactionInfo]:
        return self._tx

    def get_balance(self, address: str) -> int:
        return 0

    def is_valid_address(self, address: str) -> bool:
        return True

    def sign_from_proof(self, address: str, message: str, key: Optional[Any] = None) -> str:
        return ''

    def verify_from_proof(self, address: str, message: str, signature: str) -> bool:
        return True

    def send_amount(
        self, to_address: str, amount: int, from_address: Optional[str] = None
    ) -> Optional[Tuple[str, int]]:
        return None


def _tx(**overrides) -> TransactionInfo:
    defaults = dict(
        tx_hash='deadbeef',
        confirmed=True,
        sender='bc1qsender',
        recipient='bc1qrec',
        amount=1000,
        confirmations=5,
    )
    defaults.update(overrides)
    return TransactionInfo(**defaults)


class TestVerifyTransaction:
    def test_none_fetch_returns_none(self):
        p = _FakeProvider(tx=None)
        assert p.verify_transaction('tx', 'bc1qrec', 1000) is None

    def test_confirmed_passes(self):
        p = _FakeProvider(tx=_tx())
        result = p.verify_transaction('tx', 'bc1qrec', 1000)
        assert result is not None
        assert result.confirmed

    def test_require_confirmed_rejects_unconfirmed(self):
        p = _FakeProvider(tx=_tx(confirmed=False, confirmations=1))
        assert p.verify_transaction('tx', 'bc1qrec', 1000, require_confirmed=True) is None

    def test_require_confirmed_accepts_confirmed(self):
        p = _FakeProvider(tx=_tx())
        result = p.verify_transaction('tx', 'bc1qrec', 1000, require_confirmed=True)
        assert result is not None

    def test_expected_sender_match(self):
        p = _FakeProvider(tx=_tx(sender='bc1qsender'))
        result = p.verify_transaction('tx', 'bc1qrec', 1000, expected_sender='bc1qsender')
        assert result is not None

    def test_expected_sender_mismatch_returns_none(self):
        p = _FakeProvider(tx=_tx(sender='bc1qother'))
        assert p.verify_transaction('tx', 'bc1qrec', 1000, expected_sender='bc1qsender') is None

    def test_expected_sender_empty_mismatch_rejected(self):
        p = _FakeProvider(tx=_tx(sender=''))
        assert p.verify_transaction('tx', 'bc1qrec', 1000, expected_sender='bc1qsender') is None

    def test_block_hint_passed_to_fetch(self):
        p = _FakeProvider(tx=_tx())
        p.fetch_matching_tx = MagicMock(return_value=_tx())
        p.verify_transaction('tx', 'bc1qrec', 1000, block_hint=42)
        _, kwargs = p.fetch_matching_tx.call_args
        assert kwargs['block_hint'] == 42

    def test_no_sender_check_accepts_empty_sender(self):
        p = _FakeProvider(tx=_tx(sender=''))
        result = p.verify_transaction('tx', 'bc1qrec', 1000)
        assert result is not None
