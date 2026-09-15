import asyncio
import base64
import contextlib
import json
import os
import sys
import time
import types

import pytest
from solders.pubkey import Pubkey

from allways.solana.layouts import EVENT_DISCRIMINATORS, EVENT_LAYOUTS
from allways.solana.program_feed import (
    FeedSession,
    ProgramEventFeed,
    Subscription,
    SubscriptionFeed,
    events_from_logs,
)
from allways.solana.rpc import resolve_ws_url

MINER = Pubkey.from_bytes(b'\x01' * 32)
WINNER = Pubkey.from_bytes(b'\x02' * 32)


def _program_data(name: str, **fields) -> str:
    raw = EVENT_DISCRIMINATORS[name] + EVENT_LAYOUTS[name].build(fields)
    return 'Program data: ' + base64.b64encode(raw).decode()


def _frame(logs, err=None) -> dict:
    return {
        'method': 'logsNotification',
        'params': {'result': {'value': {'signature': 'sig', 'err': err, 'logs': logs}}},
    }


def test_events_from_logs_decodes_program_events_and_skips_the_rest():
    logs = [
        'Program 6JVB invoke [1]',
        _program_data('PoolResolved', miner=bytes(MINER), winner=bytes(WINNER), requests=2, collateral_chain='sol'),
        'Program data: AAAA',  # too short / foreign
        'Program log: Instruction: ResolvePool',
    ]
    events = events_from_logs(logs)
    assert [n for n, _ in events] == ['PoolResolved']
    ev = events[0][1]
    assert ev.miner == MINER and ev.winner == WINNER and ev.requests == 2 and ev.collateral_chain == 'sol'


def test_feed_dispatches_to_handlers_by_name_and_drops_failed_txs():
    feed = ProgramEventFeed('ws://x', 'prog')
    seen = []
    feed.on('PoolDrawArmed', lambda name, ev: seen.append((name, int(ev.seed_slot))))
    feed.on('PoolResolved', lambda name, ev: (_ for _ in ()).throw(RuntimeError('boom')))  # never fatal
    logs = [
        _program_data('PoolDrawArmed', miner=bytes(MINER), seed_slot=4242, collateral_chain='tao'),
        _program_data('PoolResolved', miner=bytes(MINER), winner=bytes(WINNER), requests=1, collateral_chain='sol'),
    ]
    assert feed.handle_notification(_frame(logs)) == 2
    assert feed.handle_notification(_frame(logs, err={'InstructionError': [0, 'Custom']})) == 0
    assert seen == [('PoolDrawArmed', 4242)]


def test_resolve_ws_url_swaps_scheme_and_keeps_the_key(monkeypatch):
    monkeypatch.delenv('SOLANA_WS_URL', raising=False)
    assert resolve_ws_url('https://devnet.helius-rpc.com/?api-key=k') == 'wss://devnet.helius-rpc.com/?api-key=k'
    assert resolve_ws_url('http://127.0.0.1:8899') == 'ws://127.0.0.1:8899'
    monkeypatch.setenv('SOLANA_WS_URL', 'wss://override')
    assert resolve_ws_url('https://x') == 'wss://override'
    assert os.environ['SOLANA_WS_URL'] == 'wss://override'


def test_mentions_defaults_to_the_program_and_narrows_to_a_miner():
    assert ProgramEventFeed('ws://x', 'prog').mentions == 'prog'
    assert ProgramEventFeed('ws://x', 'prog', mentions=MINER).mentions == str(MINER)


def test_connected_only_after_the_subscribe_ack_and_session_counts_acks():
    feed = ProgramEventFeed('ws://x', 'prog')
    assert not feed.connected and feed.session == 0
    feed.handle_frame({'jsonrpc': '2.0', 'result': 7, 'id': 1})
    assert feed.connected and feed.session == 1
    feed.handle_frame({'jsonrpc': '2.0', 'result': 8, 'id': 1})
    assert feed.session == 2


def test_rejected_subscribe_raises_so_the_session_reconnects():
    feed = ProgramEventFeed('ws://x', 'prog')
    with pytest.raises(ConnectionError):
        feed.handle_frame({'jsonrpc': '2.0', 'error': {'code': -32601}, 'id': 1})
    assert not feed.connected


class FakeSocket:
    def __init__(self, frames):
        self.frames = list(frames)
        self.sent = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def send(self, data):
        self.sent.append(json.loads(data))

    async def recv(self):
        if self.frames:
            return json.dumps(self.frames.pop(0))
        await asyncio.sleep(3600)  # a quiet socket


def test_session_subscribes_to_the_mentioned_account_and_ends_on_schedule(monkeypatch):
    sock = FakeSocket([{'jsonrpc': '2.0', 'result': 1, 'id': 1}])
    monkeypatch.setitem(sys.modules, 'websockets', types.SimpleNamespace(connect=lambda *a, **k: sock))
    feed = ProgramEventFeed('ws://x', 'prog', mentions=MINER, max_session_secs=0.2)

    asyncio.run(feed._session())  # returns on its own once the session is due for a resubscribe

    assert sock.sent[0]['method'] == 'logsSubscribe'
    assert sock.sent[0]['params'][0] == {'mentions': [str(MINER)]}
    assert feed.session == 1


# ─── SubscriptionFeed ───

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
