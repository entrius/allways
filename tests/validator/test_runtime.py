import threading
import time

from allways.validator.runtime.bus import Latest, Topic
from allways.validator.runtime.component import Component
from allways.validator.runtime.deadline_queue import DeadlineQueue, Intent, Urgency
from allways.validator.runtime.supervisor import Supervisor


def test_latest_versions_and_wait():
    cell = Latest()
    assert cell.get() == (None, 0)
    assert cell.set('a') == 1
    value, version = cell.get()
    assert (value, version) == ('a', 1)
    threading.Timer(0.05, cell.set, args=('b',)).start()
    assert cell.wait_newer(1, timeout=2) == ('b', 2)
    assert cell.wait_newer(2, timeout=0.01) == ('b', 2)  # times out, returns current


def test_topic_fans_out_and_drops_oldest_visibly():
    topic = Topic('t')
    fast = topic.subscribe('fast')
    slow = topic.subscribe('slow', maxsize=2)
    topic.publish_many([1, 2, 3])
    assert fast.drain() == [1, 2, 3]
    assert slow.drain() == [2, 3]
    assert slow.dropped == 1
    assert fast.drain() == []


def test_topic_rejects_duplicate_subscriber():
    topic = Topic('t')
    topic.subscribe('a')
    try:
        topic.subscribe('a')
    except ValueError:
        return
    raise AssertionError('duplicate subscriber accepted')


def _i(key, urgency, deadline, not_before=0.0):
    return Intent((key,), urgency, deadline, not_before=not_before)


def test_deadline_queue_orders_by_urgency_then_deadline():
    q = DeadlineQueue()
    q.push(_i('timeout', Urgency.SOFT, 10))
    q.push(_i('confirm', Urgency.VOTE, 50))
    q.push(_i('extend-late', Urgency.EXTEND, 300))
    q.push(_i('extend-soon', Urgency.EXTEND, 100))
    assert [i.key[0] for i in q.pop_due(now=0)] == ['extend-soon', 'extend-late', 'confirm', 'timeout']
    assert len(q) == 0


def test_deadline_queue_dedupes_by_key_and_holds_not_before():
    q = DeadlineQueue()
    q.push(_i('a', Urgency.VOTE, 100))
    q.push(_i('a', Urgency.EXTEND, 90))  # re-plan replaces
    q.push(_i('later', Urgency.EXTEND, 50, not_before=20))
    assert len(q) == 2
    due = q.pop_due(now=10)
    assert [(i.key[0], i.urgency) for i in due] == [('a', Urgency.EXTEND)]
    assert [i.key[0] for i in q.pop_due(now=25)] == ['later']


def test_deadline_queue_discard_and_limit():
    q = DeadlineQueue()
    for n in range(5):
        q.push(_i(n, Urgency.SOFT, n))
    q.discard((0,))
    assert [i.key[0] for i in q.pop_due(now=0, limit=2)] == [1, 2]
    assert len(q) == 2


class _Ticker(Component):
    interval_secs = 0.01

    def __init__(self, name, critical=False, stall_secs=5.0, fail=False, hang=None):
        super().__init__()
        self.name = name
        self.critical = critical
        self.stall_secs = stall_secs
        self.fail = fail
        self.hang = hang

    def tick(self, now):
        if self.hang is not None:
            self.hang.wait()
        if self.fail:
            raise RuntimeError('boom')


def test_failing_tick_never_kills_the_thread():
    stop = threading.Event()
    c = _Ticker('flaky', fail=True)
    c.start(stop)
    time.sleep(0.1)
    assert c.alive and c.errors >= 1
    stop.set()


def test_only_a_critical_stall_is_fatal():
    hang = threading.Event()
    fatal = []
    critical = _Ticker('actuator', critical=True, stall_secs=0.05, hang=hang)
    background = _Ticker('scoring', stall_secs=0.05, hang=hang)
    healthy = _Ticker('feed')
    sup = Supervisor([critical, background, healthy], on_fatal=fatal.append)
    sup.start()
    time.sleep(0.2)
    bad = {h.name for h in sup.check()}
    assert bad == {'actuator', 'scoring'}
    assert len(fatal) == 1 and fatal[0].startswith('actuator stalled')
    hang.set()
    sup.stop.set()


def test_a_requeued_intent_never_replaces_a_newer_one_with_its_key():
    q = DeadlineQueue()
    q.push(Intent(('k',), Urgency.VOTE, 5.0, payload='old'))
    (taken,) = q.pop_due(now=0)
    q.push(Intent(('k',), Urgency.VOTE, 5.0, payload='new'))
    q.requeue(taken)  # its send failed meanwhile
    assert [i.payload for i in q.pop_due(now=0)] == ['new']
    q.requeue(taken)
    assert [i.payload for i in q.pop_due(now=0)] == ['old']
