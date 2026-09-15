"""TaoBalanceReader: a failed read holds the last good balance for a while, then reads unknown — never 0."""

from allways.miner.optimizer.balances import HOLD_SECS, TaoBalanceReader


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
    assert len(connects) == 3  # each failure drops the connection; the next read opens a new one
