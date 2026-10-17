from allways.validator.feed.events import (
    EMPTY_TX_RETRIES,
    HISTORY_HORIZON_SECS,
    Backfill,
    EventFeed,
    HistoryGap,
    SlotClock,
    _Pending,
)
from allways.validator.runtime.bus import Topic

NOW = 2_000_000_000


class _SigRpc:
    """getSignaturesForAddress over a fixed newest-first history; counts pages and tx reads."""

    def __init__(self, history):
        self.history = history  # newest first
        self.pages = 0
        self.tx_reads = []

    def get_signatures_for_address(self, program_id, before=None, until=None, limit=1000):
        self.pages += 1
        start = 0
        if before is not None:
            start = next(i for i, e in enumerate(self.history) if e['signature'] == before) + 1
        out = []
        for e in self.history[start:]:
            if e['signature'] == until or len(out) == limit:
                break
            out.append(e)
        return out

    def get_transaction(self, sig):
        self.tx_reads.append(sig)
        return {'blockTime': int(sig.split('-')[1]), 'meta': {'logMessages': []}}

    def _call(self, method, params):
        return None


def _history(n, spacing):
    # newest first, one tx every `spacing` seconds ending at NOW
    return [
        {'signature': f's-{NOW - i * spacing}', 'slot': 10_000 - i, 'blockTime': NOW - i * spacing, 'err': None}
        for i in range(n)
    ]


def test_backfill_stops_at_the_horizon_and_marks_the_gap():
    rpc = _SigRpc(_history(5000, spacing=10))  # ~13.9h of history
    bf = Backfill(rpc, 'prog', until_sig=None, now=NOW)
    bf.page(max_pages=10)
    assert bf.paging_done and bf.truncated
    chunk = bf.peek(limit=100_000)
    assert len(chunk) == HISTORY_HORIZON_SECS // 10 + 1
    assert chunk[0]['blockTime'] >= NOW - HISTORY_HORIZON_SECS  # oldest first, inside the horizon
    assert chunk[-1]['blockTime'] == NOW


def test_backfill_from_a_recent_cursor_is_not_truncated():
    history = _history(500, spacing=10)
    bf = Backfill(_SigRpc(history), 'prog', until_sig=history[100]['signature'], now=NOW)
    bf.page()
    assert bf.paging_done and not bf.truncated
    assert len(bf.peek(limit=1000)) == 100


def test_backfill_hands_out_bounded_chunks():
    bf = Backfill(_SigRpc(_history(250, spacing=1)), 'prog', until_sig=None, now=NOW)
    bf.page()
    sizes = []
    while not bf.done:
        chunk = bf.peek(limit=100)
        sizes.append(len(chunk))
        for _ in chunk:
            bf.pop()
    assert sizes == [100, 100, 50]


def test_slot_clock_interpolates_between_anchors_and_extrapolates_past_them():
    times = {100: 1000, 200: 1040}
    clock = SlotClock(times.get, min_interval=0)
    assert clock.time_of(100) is None  # no anchor yet
    clock.anchor(100, now=0)
    assert clock.time_of(110) == 1004  # one anchor: the nominal 0.4s slot
    clock.anchor(200, now=1)
    assert clock.time_of(150) == 1020
    assert clock.time_of(300) == 1080  # past the newest anchor: the measured rate (0.4s here)
    assert clock.time_of(50) == 980


def test_slot_clock_reads_block_time_at_most_once_per_interval():
    reads = []
    clock = SlotClock(lambda slot: reads.append(slot) or 1000 + slot, min_interval=30)
    clock.anchor(100, now=0)
    clock.anchor(150, now=10)
    clock.anchor(200, now=31)
    assert reads == [100, 200]


class _FakeLive:
    def __init__(self):
        self.session = 1
        self.inbox = []

    def start(self):
        return self

    def take(self):
        out, self.inbox = self.inbox, []
        return out


def _feed(tmp_path, rpc, live, resume=None):
    out, gaps = Topic('events'), Topic('gaps')
    feed = EventFeed(lambda: rpc, live, lambda: resume, out, gaps, 'prog', clock=lambda: NOW)
    return feed, out.subscribe('t'), gaps.subscribe('t')


def test_fresh_start_backfills_within_horizon_then_follows_live(tmp_path):
    rpc = _SigRpc(_history(5000, spacing=10))
    live = _FakeLive()
    feed, events, gaps = _feed(tmp_path, rpc, live)
    feed.setup()
    for _ in range(20):
        feed.tick(NOW)
    assert feed._backfill is None
    assert gaps.drain() == [HistoryGap(NOW - HISTORY_HORIZON_SECS)]
    assert len(rpc.tx_reads) == HISTORY_HORIZON_SECS // 10 + 1  # never decoded past the horizon
    assert feed.status.get()[0].cursor[1] == f's-{NOW}'
    # A live tx already decoded by the backfill is not published twice.
    live.inbox.append(_Pending(10_000, f's-{NOW}', [('QuoteRemoved', {})]))
    feed._clock = SlotClock(lambda slot: NOW, min_interval=0)
    feed.tick(NOW)
    assert events.drain() == []


def test_live_events_need_no_transaction_read(tmp_path):
    rpc = _SigRpc([])
    live = _FakeLive()
    feed, events, _ = _feed(tmp_path, rpc, live)
    feed.setup()
    feed._clock = SlotClock(lambda slot: NOW + slot, min_interval=0)
    live.inbox.append(_Pending(7, 'live-1', [('QuoteRemoved', {'x': 1})]))
    feed.tick(NOW)
    feed.tick(NOW)  # the first tick also finishes the (empty) boot backfill
    (record,) = events.drain()
    assert (record.name, record.signature, record.block_time) == ('QuoteRemoved', 'live-1', NOW + 7)
    assert rpc.tx_reads == []
    assert feed.status.get()[0].cursor == (7, 'live-1')


class _FlakyRpc(_SigRpc):
    """Fails one signature's read once (an outage), or serves it empty for a while (not indexed yet)."""

    def __init__(self, history, sig, mode, times=1):
        super().__init__(history)
        self.sig, self.mode, self.left = sig, mode, times

    def get_transaction(self, sig):
        if sig == self.sig and self.left > 0:
            self.left -= 1
            self.tx_reads.append(sig)
            if self.mode == 'raise':
                raise RuntimeError('HTTP 429 after retries')
            return None
        return super().get_transaction(sig)


def _replay(rpc, ticks):
    feed, events, gaps = _feed(None, rpc, _FakeLive(), resume='before-history')
    feed.setup()
    feed._backfill.until_sig = None
    for _ in range(ticks):
        try:
            feed.tick(NOW)
        except RuntimeError:
            pass  # the component backs off and ticks again
    return feed, gaps


def test_a_failed_read_mid_chunk_resumes_at_that_entry():
    history = _history(10, spacing=10)
    oldest_first = [e['signature'] for e in reversed(history)]
    rpc = _FlakyRpc(history, oldest_first[3], 'raise')
    feed, _ = _replay(rpc, 5)
    assert [s for s in oldest_first if s not in rpc.tx_reads] == []  # nothing after the failure was skipped
    assert rpc.tx_reads.count(oldest_first[3]) == 2 and feed._backfill is None


def test_an_empty_transaction_holds_the_feed_then_becomes_a_gap():
    history = _history(10, spacing=10)
    stuck = [e['signature'] for e in reversed(history)][3]
    after = [e['signature'] for e in reversed(history)][4]

    feed, gaps = _replay(_FlakyRpc(history, stuck, 'empty', times=1), 3)
    assert feed._backfill is None and gaps.drain() == []  # served on the retry: no gap

    rpc = _FlakyRpc(history, stuck, 'empty', times=EMPTY_TX_RETRIES)
    _replay(rpc, 2)
    assert after not in rpc.tx_reads  # held at the empty entry: nothing after it is published first

    feed, gaps = _replay(_FlakyRpc(history, stuck, 'empty', times=EMPTY_TX_RETRIES), 10)
    assert feed._backfill is None and len(gaps.drain()) == 1  # given up as a gap, then the rest read
