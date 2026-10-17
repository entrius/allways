from types import SimpleNamespace

from allways.validator.app import chain_watchers
from allways.validator.chains.watcher import MEMPOOL_RECHECK_SECS, ChainWatcher, verify_probe


class _Chain:
    def __init__(self):
        self.tip = 100
        self.tip_reads = 0
        self._cached = None

    def clear_pass_tip(self):
        self._cached = None

    def cached_block_height(self):
        if self._cached is None:
            self.tip_reads += 1
            self._cached = self.tip
        return self._cached


class _Asset:
    def __init__(self, chain, host=None, spb=600):
        self.chain = chain
        self.chain_def = SimpleNamespace(host_chain=host, seconds_per_block=spb)
        self.verifies = 0
        self.confirmed_at_tip = None
        self.fail = False

    def verify_transaction(self, **kw):
        self.verifies += 1
        if self.fail:
            raise ConnectionError('endpoint down')
        confirmed = self.confirmed_at_tip is not None and self.chain.tip >= self.confirmed_at_tip
        return SimpleNamespace(confirmed=confirmed, tx_hash=kw['tx_hash'])


class _Clock:
    t = 1000.0

    def __call__(self):
        return self.t


def _watcher():
    clock = _Clock()
    chain = _Chain()
    btc = _Asset(chain)
    w = ChainWatcher('btc', chain, {'btc': btc}, seconds_per_block=600, clock=clock)
    return w, chain, btc, clock


REQ = verify_probe('btc', 'aa' * 32, 'bc1qdst', 1000)


def test_asking_never_blocks_and_the_check_runs_on_the_watcher():
    w, chain, btc, clock = _watcher()
    assert w.verdict(REQ) is None
    assert btc.verifies == 0
    w.tick(clock.t)
    assert btc.verifies == 1 and w.verdict(REQ).info.tx_hash == 'aa' * 32


def test_a_pending_leg_is_rechecked_when_the_tip_moves_not_every_tick():
    w, chain, btc, clock = _watcher()
    btc.confirmed_at_tip = 101
    w.verdict(REQ)
    w.tick(clock.t)
    w.tick(clock.t + 1)
    assert btc.verifies == 1  # same tip, recently checked: no new call
    chain.tip = 101
    w.tick(clock.t + 2)
    assert btc.verifies == 2 and w.verdict(REQ).settled


def test_a_pending_leg_is_rechecked_for_the_mempool_on_a_quiet_tip():
    w, _, btc, clock = _watcher()
    w.verdict(REQ)
    w.tick(clock.t)
    w.tick(clock.t + MEMPOOL_RECHECK_SECS)
    assert btc.verifies == 2


def test_a_settled_leg_is_never_checked_again():
    w, chain, btc, clock = _watcher()
    btc.confirmed_at_tip = 100
    w.verdict(REQ)
    w.tick(clock.t)
    chain.tip = 150
    w.tick(clock.t + 100)
    assert btc.verifies == 1 and w.verdict(REQ).settled


def test_an_idle_chain_and_a_settled_leg_cost_no_tip_read():
    w, chain, btc, clock = _watcher()
    w.tick(clock.t)
    assert chain.tip_reads == 0  # nothing asked
    btc.confirmed_at_tip = 100
    w.verdict(REQ)
    w.tick(clock.t)
    w.tick(clock.t + 100)
    assert chain.tip_reads == 1  # the one read that settled it


def test_every_check_in_a_tick_shares_one_tip_read():
    w, chain, btc, clock = _watcher()
    for n in range(5):
        w.verdict(verify_probe('btc', f'{n:064x}', 'bc1qdst', 1000))
    w.tick(clock.t)
    assert btc.verifies == 5 and chain.tip_reads == 1


def test_an_endpoint_failure_is_unknown_not_no_and_is_retried():
    w, _, btc, clock = _watcher()
    btc.fail = True
    w.verdict(REQ)
    w.tick(clock.t)
    v = w.verdict(REQ)
    assert v.error and v.info is None
    btc.fail = False
    w.tick(clock.t + 1)
    assert w.verdict(REQ).error is None


def test_a_leg_nobody_asks_about_is_forgotten():
    w, _, btc, clock = _watcher()
    w.verdict(REQ)
    w.tick(clock.t)
    clock.t += 3600
    w.tick(clock.t)
    assert btc.verifies == 1 and w._wanted == {}


def test_assets_on_one_chain_share_a_watcher():
    eth = _Chain()
    assets = {
        'eth': _Asset(eth, host=None, spb=12),
        'ethusdc': _Asset(eth, host='eth', spb=12),
        'btc': _Asset(_Chain()),
    }
    watchers = chain_watchers(assets)
    assert watchers['eth'] is watchers['ethusdc'] and watchers['btc'] is not watchers['eth']
    assert watchers['eth'].interval_secs == 6.0 and watchers['btc'].interval_secs == 30.0
