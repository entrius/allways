"""Balance readers: a failed read holds the last good balance for a while, then reads unknown — never 0."""

import pytest

from allways.assets.btc import Bitcoin
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

    spendable_balance = Bitcoin.spendable_balance
    is_spendable_utxo = Bitcoin.is_spendable_utxo
    is_confirmed_utxo = staticmethod(Bitcoin.is_confirmed_utxo)

    def __init__(self, results, broadcasted=(), chain_limited=()):
        self.results = list(results)
        self.paths = []
        self.broadcasted_txids = {txid: None for txid in broadcasted}
        self.chain_limited_txids = set(chain_limited)

    def btc_api_get(self, path, timeout=None):
        self.paths.append(path)
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


def utxo(txid, value, confirmed=True):
    return {'txid': txid, 'vout': 0, 'value': value, 'status': {'confirmed': confirmed}}


def test_btc_balance_counts_confirmed_coins_and_our_own_change_only():
    # An outside unconfirmed coin can be replaced away before a payout built on it confirms.
    coins = [utxo('a', 800_000), utxo('ours', 50_000, confirmed=False), utxo('outside', 5_000_000, confirmed=False)]
    esplora = FlakyEsplora([EsploraResponse(coins)], broadcasted=['ours'])
    assert BtcBalanceReader(esplora).get_balance('bc1qminer') == 850_000
    assert esplora.paths == ['/address/bc1qminer/utxo']


def test_btc_balance_leaves_out_our_change_once_its_chain_is_limited():
    # Change a payout was refused for spending (descendant limits) is not spendable until it confirms.
    coins = [utxo('a', 800_000), utxo('ours', 50_000, confirmed=False)]
    esplora = FlakyEsplora([EsploraResponse(coins)], broadcasted=['ours'], chain_limited=['ours'])
    assert BtcBalanceReader(esplora).get_balance('bc1qminer') == 800_000


@pytest.mark.parametrize(
    'failure',
    [
        ConnectionError('every Esplora endpoint down'),
        EsploraResponse(error=RuntimeError('429 Too Many Requests')),
        EsploraResponse({'chain_stats': {}}),  # a malformed body (not a UTXO list)
    ],
)
def test_a_failed_btc_read_holds_the_last_good_balance_then_reads_unknown_not_zero(failure):
    now = [1000.0]
    esplora = FlakyEsplora([EsploraResponse([utxo('a', 500_000)]), failure, failure])
    reader = BtcBalanceReader(esplora, clock=lambda: now[0])
    assert reader.get_balance('bc1qminer') == 500_000
    now[0] += 60
    assert reader.get_balance('bc1qminer') == 500_000  # held through a failed read
    now[0] += HOLD_SECS + 1
    assert reader.get_balance('bc1qminer') is None  # the provider itself would have said 0
