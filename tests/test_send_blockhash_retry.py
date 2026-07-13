"""Regression: a sustained BlockhashNotFound window must not abort a send mid-origination.

`client._send` retries ONLY the blockhash-staleness class — a BlockhashNotFound tx never entered the
ledger, so re-signing with a FRESH blockhash can't double-submit — now with exponential backoff and
more attempts (was 5×0.5s ≈ 2.5s, which a prolonged degraded-RPC window blew straight through,
orphaning a paid-for reservation seat). Any other send fault still raises immediately (a landed tx
might exist → a blind resend is the caller's call, not ours)."""

from unittest.mock import MagicMock

import pytest
from solders.instruction import Instruction
from solders.keypair import Keypair
from solders.pubkey import Pubkey

from allways.solana.client import AllwaysSolanaClient, SolanaClientError

_BH = '11111111111111111111111111111111'  # a valid 32-byte base58 value (parses as a Hash)


def _client(monkeypatch):
    monkeypatch.setattr('allways.solana.client.time.sleep', lambda *_: None)  # no real backoff waits
    c = AllwaysSolanaClient('http://rpc.test', program_id=Pubkey.default(), keypair=Keypair())
    c.rpc = MagicMock()
    c.rpc.get_latest_blockhash.return_value = _BH
    c.rpc.confirm.return_value = {}
    return c


def _ix():
    return Instruction(Pubkey.default(), b'', [])


def test_send_retries_blockhash_not_found_then_succeeds(monkeypatch):
    c = _client(monkeypatch)
    c.rpc.send_transaction.side_effect = [
        RuntimeError('Transaction simulation failed: Blockhash not found'),
        RuntimeError('blockhash not found'),
        'okSig',
    ]
    assert c._send([_ix()]) == 'okSig'
    assert c.rpc.send_transaction.call_count == 3
    assert c.rpc.get_latest_blockhash.call_count == 3  # a FRESH blockhash each attempt (why resend is safe)


def test_send_rides_out_a_long_degraded_window(monkeypatch):
    # 6 straight BlockhashNotFound then success — the old 5-attempt cap would have failed here.
    c = _client(monkeypatch)
    c.rpc.send_transaction.side_effect = [RuntimeError('Blockhash not found')] * 6 + ['okSig']
    assert c._send([_ix()]) == 'okSig'
    assert c.rpc.send_transaction.call_count == 7


def test_send_gives_up_after_retries_on_persistent_blockhash(monkeypatch):
    c = _client(monkeypatch)
    c.rpc.send_transaction.side_effect = RuntimeError('Blockhash not found')
    with pytest.raises(SolanaClientError):
        c._send([_ix()], retries=4)
    assert c.rpc.send_transaction.call_count == 4


def test_send_does_not_retry_a_non_blockhash_error(monkeypatch):
    c = _client(monkeypatch)
    c.rpc.send_transaction.side_effect = RuntimeError('custom program error: 0x1771')
    with pytest.raises(RuntimeError):
        c._send([_ix()])
    assert c.rpc.send_transaction.call_count == 1  # a deterministic reject surfaces at once — no resend
