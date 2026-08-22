import base64
import os

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
