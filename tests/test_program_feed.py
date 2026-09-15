import asyncio
import base64
import json
import os
import sys
import types

import pytest
from solders.pubkey import Pubkey

from allways.solana.layouts import EVENT_DISCRIMINATORS, EVENT_LAYOUTS
from allways.solana.program_feed import ProgramEventFeed, events_from_logs
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
