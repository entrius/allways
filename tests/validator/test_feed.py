from allways.validator.feed import events as events_mod
from allways.validator.feed.events import (
    EMPTY_TX_RETRIES,
    HISTORY_HORIZON_SECS,
    SWEEP_SECS,
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


class _LoggedRpc(_SigRpc):
    """Every transaction carries one log line naming its signature, so each decodes to one event."""

    def get_transaction(self, sig):
        tx = super().get_transaction(sig)
        tx['meta']['logMessages'] = [sig]
        return tx


def test_a_notification_the_live_socket_dropped_is_recovered_by_the_next_sweep(tmp_path, monkeypatch):
    monkeypatch.setattr(
        events_mod, 'events_from_logs', lambda logs, pid: [('QuoteSet', {'tx': logs[0]})] if logs else []
    )
    history = _history(3, spacing=10)
    rpc, live = _LoggedRpc(history), _FakeLive()
    feed, events, gaps = _feed(tmp_path, rpc, live, resume=history[-1]['signature'])
    feed.setup()
    for _ in range(3):
        feed.tick(NOW)
    events.drain()
    rpc.tx_reads.clear()
    feed._clock = SlotClock(lambda slot: NOW, min_interval=0)

    # Three txs land on a busy socket; the second notification never arrives.
    sigs = [f's-{NOW + t}' for t in (10, 20, 30)]
    rpc.history[:0] = [{'signature': s, 'slot': 10_000 + i, 'blockTime': NOW, 'err': None} for i, s in enumerate(sigs)][
        ::-1
    ]
    live.inbox += [_Pending(10_000, sigs[0], [('QuoteSet', {})]), _Pending(10_002, sigs[2], [('QuoteSet', {})])]
    feed.tick(NOW + 30)
    assert [e.signature for e in events.drain()] == [sigs[0], sigs[2]]

    feed.tick(NOW + 30 + SWEEP_SECS)
    assert [(e.signature, e.fields) for e in events.drain()] == [(sigs[1], {'tx': sigs[1]})]
    assert rpc.tx_reads == [sigs[1]]  # only the one it never saw is decoded
    status = feed.status.get()[0]
    assert not status.catching_up and status.cursor[1] == sigs[2]  # nothing waits on a sweep
    assert gaps.drain() == []
    feed.tick(NOW + 30 + 2 * SWEEP_SECS)
    assert events.drain() == [] and rpc.tx_reads == [sigs[1]]  # the overlap re-lists, never re-decodes
