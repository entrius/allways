"""SubscriptionFeed: acks route by request id and notifications by subscription id, it is live only while every
subscription is acknowledged, its renewal is make-before-break, and stop/start opens a new generation."""

import asyncio
import contextlib
import json
import time

import pytest

from allways.miner.optimizer.subscription_feed import FeedSession, Subscription, SubscriptionFeed

SUB_A = Subscription('a', 'accountSubscribe', ['A', {'encoding': 'base64'}])
SUB_B = Subscription('b', 'programSubscribe', ['B', {'encoding': 'base64'}])


def _ack(request, sub_id):
    return {'jsonrpc': '2.0', 'result': sub_id, 'id': request['id']}


def _note(sub_id, value):
    return {'jsonrpc': '2.0', 'method': 'accountNotification', 'params': {'subscription': sub_id, 'result': value}}


def test_session_routes_acks_by_request_id_and_notifications_by_subscription_id():
    session = FeedSession()
    req_a, req_b = session.request(SUB_A), session.request(SUB_B)
    assert session.handle_frame(_ack(req_b, 900)) is None  # acks may arrive out of order
    assert session.handle_frame(_ack(req_a, 800)) is None
    assert session.handle_frame(_note(900, {'value': 'b'})) == ('b', {'value': 'b'})
    assert session.handle_frame(_note(800, {'value': 'a'})) == ('a', {'value': 'a'})
    assert session.handle_frame(_note(7, {'value': '?'})) is None


def test_a_rejected_subscription_raises_so_the_session_is_replaced():
    session = FeedSession()
    request = session.request(SUB_A)
    with pytest.raises(ConnectionError):
        session.handle_frame({'jsonrpc': '2.0', 'error': {'code': -32602}, 'id': request['id']})


def test_live_only_while_every_subscription_is_acknowledged():
    feed = SubscriptionFeed('ws://x', lambda key, result: None)
    assert feed.add(SUB_A) and feed.add(SUB_B) and not feed.add(SUB_A)
    session = feed._current = FeedSession()
    req_a, req_b = session.request(SUB_A), session.request(SUB_B)
    session.handle_frame(_ack(req_a, 1))
    assert not feed.live
    session.handle_frame(_ack(req_b, 2))
    assert feed.live
    feed.add(Subscription('c', 'accountSubscribe', ['C']))
    assert not feed.live  # until the new subscription is acknowledged too


class ScriptedSocket:
    """Acknowledges each subscribe shortly after it is sent and logs what happens, in order."""

    def __init__(self, name, log):
        self.name, self.log = name, log
        self.inbox = asyncio.Queue()

    async def send(self, data):
        msg = json.loads(data)
        self.log.append((self.name, 'subscribe', msg['params'][0]))
        asyncio.get_running_loop().call_later(
            0.01, self.inbox.put_nowait, {'result': 1000 + msg['id'], 'id': msg['id']}
        )
        asyncio.get_running_loop().call_later(0.02, self.inbox.put_nowait, _note(1000 + msg['id'], {'from': self.name}))

    def __aiter__(self):
        return self

    async def __anext__(self):
        msg = await self.inbox.get()
        if msg is None:
            raise StopAsyncIteration
        if 'id' in msg:
            self.log.append((self.name, 'ack', msg['id']))
        return json.dumps(msg)

    async def close(self):
        self.log.append((self.name, 'close'))
        self.inbox.put_nowait(None)


def test_scheduled_resubscribe_is_make_before_break():
    log, sockets, pushes, live = [], [], [], []

    async def connect(url, **kwargs):
        sock = ScriptedSocket(f'ws{len(sockets) + 1}', log)
        sockets.append(sock)
        log.append((sock.name, 'open'))
        return sock

    feed = SubscriptionFeed(
        'ws://x', lambda key, result: pushes.append((key, result)), max_session_secs=0.15, connect=connect
    )
    feed.add(SUB_A)
    feed.add(SUB_B)

    async def scenario():
        task = asyncio.ensure_future(feed._supervise())
        for _ in range(40):
            await asyncio.sleep(0.01)
            live.append(feed.live)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    asyncio.run(scenario())

    assert len(sockets) >= 2 and feed.connections_opened == len(sockets)
    new_acks = [i for i, entry in enumerate(log) if entry[:2] == ('ws2', 'ack')]
    assert len(new_acks) == 2
    assert log.index(('ws1', 'close')) > max(new_acks)  # the old session closes only after the new one is acked
    assert live[live.index(True) :] == [True] * (len(live) - live.index(True))  # never down across the swap
    assert feed.generation == 1 and feed.bytes_received > 0
    assert {key for key, _ in pushes} == {'a', 'b'}


def _wait_until(condition, timeout=2.0):
    deadline = time.monotonic() + timeout
    while not condition() and time.monotonic() < deadline:
        time.sleep(0.01)
    return condition()


def test_stop_closes_the_connection_and_start_opens_a_new_generation():
    log, sockets = [], []

    async def connect(url, **kwargs):
        sock = ScriptedSocket(f'ws{len(sockets) + 1}', log)
        sockets.append(sock)
        return sock

    feed = SubscriptionFeed('ws://x', lambda key, result: None, connect=connect)
    feed.add(SUB_A)
    feed.start()
    assert _wait_until(lambda: feed.live)
    feed.stop()
    assert not feed.live and ('ws1', 'close') in log
    assert feed._thread is None and feed.generation == 1
    feed.start()
    assert _wait_until(lambda: feed.live)
    assert feed.generation == 2 and len(sockets) == 2
    feed.stop()
