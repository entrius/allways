from types import SimpleNamespace

from allways.validator.app import chain_watchers
from allways.validator.chains.watcher import MEMPOOL_RECHECK_SECS, STALE_TIP_BLOCKS, ChainWatcher, verify_probe


class _Chain:
    def __init__(self):
        self.tip = 100
        self.tip_reads = 0
        self._cached = None

    def start_pass(self):
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


def test_assets_on_one_network_share_a_watcher():
    # create_assets binds every asset on a network to one chain object, and the watcher goes by that object.
    ethereum = _Chain()
    assets = {
        'eth': _Asset(ethereum, host='ethereum', spb=12),
        'ethusdc': _Asset(ethereum, host='ethereum', spb=12),
        'btc': _Asset(_Chain()),
    }
    watchers = chain_watchers(assets)
    assert watchers['eth'] is watchers['ethusdc'] and watchers['btc'] is not watchers['eth']
    assert {w.name for w in watchers.values()} == {'chain-ethereum', 'chain-btc'}
    assert watchers['eth'].interval_secs == 6.0 and watchers['btc'].interval_secs == 30.0


def test_a_tip_frozen_while_checks_wait_reconnects_the_chain():
    clock, chain, reconnects = _Clock(), _Chain(), []
    w = ChainWatcher('tao', chain, {'btc': _Asset(chain)}, 12, reconnect=lambda: reconnects.append(1), clock=clock)
    w.verdict(REQ)
    for _ in range(int(STALE_TIP_BLOCKS * 12 / w.interval_secs)):
        w.tick(clock.t)
        clock.t += w.interval_secs
    w.tick(clock.t)
    assert reconnects == [1]
    chain.tip += 1  # moving again: no further reconnects
    for _ in range(10):
        clock.t += w.interval_secs
        w.tick(clock.t)
    assert reconnects == [1]


def test_every_subnet_alpha_shares_the_tao_watcher():
    from allways.assets.tao import TaoChain

    subtensor = TaoChain.__new__(TaoChain)
    assets = {'tao': _Asset(subtensor, spb=12), 'sn3': _Asset(subtensor, spb=12), 'btc': _Asset(_Chain())}
    watchers = chain_watchers(assets)
    assert watchers['sn3'] is watchers['tao'] and watchers['tao'].name == 'chain-tao'
    assert len({id(w) for w in watchers.values()}) == 2


def test_a_reconnect_hands_every_tao_provider_the_new_subtensor_and_closes_the_old(monkeypatch):
    from allways.assets.tao import TaoChain
    from allways.validator.app import SubtensorSwap

    monkeypatch.setattr(SubtensorSwap, '_done_at', 0.0)
    old, new = SimpleNamespace(closed=0), SimpleNamespace()
    old.close = lambda: setattr(old, 'closed', old.closed + 1)
    tao, alpha = TaoChain.__new__(TaoChain), TaoChain.__new__(TaoChain)
    tao.subtensor = alpha.subtensor = old
    SubtensorSwap({'tao': _Asset(tao), 'sn3': _Asset(alpha), 'btc': _Asset(_Chain())}, lambda: new)()
    assert tao.subtensor is new and alpha.subtensor is new and old.closed == 1
