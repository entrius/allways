"""Balance readers: a failed read holds the last good balance for a while, then reads unknown — never 0."""

import pytest

from allways.miner.optimizer.balances import HOLD_SECS, BtcBalanceReader, TaoBalanceReader


class FlakySubtensor:
    def __init__(self, results):
        self.results = list(results)

    def get_balance(self, address):
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


def test_a_failed_read_holds_the_last_good_balance_then_reads_unknown_not_zero():
    now = [1000.0]
    connects = []
    sub = FlakySubtensor(
        [
            834_000_000,
            RuntimeError('cannot call recv while another thread is already running recv'),
            RuntimeError('still down'),
        ]
    )

    def connect():
        connects.append(now[0])
        return sub

    reader = TaoBalanceReader(connect, clock=lambda: now[0])
    assert reader.get_balance('5FTuWM9k') == 834_000_000
    now[0] += 60
    assert reader.get_balance('5FTuWM9k') == 834_000_000  # held through a failed read
    now[0] += HOLD_SECS + 1
    assert reader.get_balance('5FTuWM9k') is None  # nothing fresh left: unknown, never 0
    assert connects == [1000.0, 1361.0]  # the failed read dropped the connection; the next read opened a new one


class EsploraResponse:
    def __init__(self, body=None, error=None):
        self.body, self.error = body, error

    def raise_for_status(self):
        if self.error is not None:
            raise self.error

    def json(self):
        return self.body


class FlakyEsplora:
    """The miner's BTC provider as the reader sees it: ``btc_api_get`` answering each call from ``results``."""

    def __init__(self, results):
        self.results = list(results)
        self.paths = []

    def btc_api_get(self, path, timeout=None):
        self.paths.append(path)
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


def address_stats(funded, spent, mempool_funded=0, mempool_spent=0):
    return {
        'chain_stats': {'funded_txo_sum': funded, 'spent_txo_sum': spent},
        'mempool_stats': {'funded_txo_sum': mempool_funded, 'spent_txo_sum': mempool_spent},
    }


def test_btc_balance_counts_confirmed_and_mempool_coins():
    # A payout may spend unconfirmed change, so an outgoing mempool spend counts against the balance and an incoming
    # one for it — the same coins the provider selects from.
    esplora = FlakyEsplora([EsploraResponse(address_stats(900_000, 100_000, mempool_funded=50_000, mempool_spent=0))])
    assert BtcBalanceReader(esplora).get_balance('bc1qminer') == 850_000
    assert esplora.paths == ['/address/bc1qminer']


@pytest.mark.parametrize(
    'failure',
    [
        ConnectionError('every Esplora endpoint down'),
        EsploraResponse(error=RuntimeError('429 Too Many Requests')),
        EsploraResponse({'chain_stats': {}}),  # a malformed body
    ],
)
def test_a_failed_btc_read_holds_the_last_good_balance_then_reads_unknown_not_zero(failure):
    now = [1000.0]
    esplora = FlakyEsplora([EsploraResponse(address_stats(500_000, 0)), failure, failure])
    reader = BtcBalanceReader(esplora, clock=lambda: now[0])
    assert reader.get_balance('bc1qminer') == 500_000
    now[0] += 60
    assert reader.get_balance('bc1qminer') == 500_000  # held through a failed read
    now[0] += HOLD_SECS + 1
    assert reader.get_balance('bc1qminer') is None  # the provider itself would have said 0
