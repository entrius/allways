"""Program events are read only from lines the allways program itself logged."""

import base64
from types import SimpleNamespace

from solders.keypair import Keypair

from allways.solana.events import decode_event, program_event_payloads
from allways.solana.layouts import EVENT_DISCRIMINATORS, EVENT_LAYOUTS
from allways.solana.program_feed import events_from_logs
from allways.validator.feed.events import EventFeed, LiveFeed
from allways.validator.runtime.bus import Topic

PROGRAM = str(Keypair().pubkey())
OTHER = str(Keypair().pubkey())
MINER = Keypair().pubkey()
DELIVERY = 'dsttx'
SWAP_KEY, UNRELATED_KEY = b'\x01' * 32, b'\x02' * 32


def _data(name: str, **fields) -> str:
    raw = EVENT_DISCRIMINATORS[name] + EVENT_LAYOUTS[name].build(fields)
    return 'Program data: ' + base64.b64encode(raw).decode()


def _fulfilled(key=SWAP_KEY) -> str:
    return _data('SwapFulfilled', swap_key=key, miner=bytes(MINER), to_tx_hash=DELIVERY, to_amount=1000)


def _completed(key=SWAP_KEY) -> str:
    return _data(
        'SwapCompleted',
        swap_key=key,
        miner=bytes(MINER),
        collateral_amount=0,
        fee=0,
        from_chain='btc',
        to_chain='sol',
        from_amount=500,
        to_amount=1000,
        rate=0,
        collateral_chain='sol',
    )


def _frame(program: str, *lines: str) -> list:
    return [f'Program {program} invoke [1]', *lines, f'Program {program} success']


def _names(logs):
    return [decode_event(raw)[0] for raw in program_event_payloads(logs, PROGRAM)]


def test_the_programs_own_events_are_read():
    logs = _frame(PROGRAM, 'Program log: Instruction: MarkFulfilled', _fulfilled())
    assert _names(logs) == ['SwapFulfilled']


def test_lines_logged_by_another_program_are_dropped():
    logs = _frame(OTHER, _fulfilled(), _completed())
    assert program_event_payloads(logs, PROGRAM) == []
    assert events_from_logs(logs, PROGRAM) == []


def test_only_the_lines_inside_our_frame_count_when_another_program_calls_ours():
    logs = [
        f'Program {OTHER} invoke [1]',
        _fulfilled(),
        f'Program {PROGRAM} invoke [2]',
        _fulfilled(),
        f'Program {PROGRAM} success',
        _fulfilled(),
        f'Program {OTHER} success',
    ]
    assert _names(logs) == ['SwapFulfilled']


def test_program_output_never_parses_as_a_frame():
    # A program's own output is always prefixed `Program log:`, so it cannot open or close a frame.
    logs = _frame(OTHER, f'Program log: Program {PROGRAM} invoke [2]', 'Program log: success', _fulfilled())
    assert program_event_payloads(logs, PROGRAM) == []


def test_frames_that_do_not_nest_void_the_tx():
    skipped_depth = [f'Program {PROGRAM} invoke [2]', _fulfilled(), f'Program {PROGRAM} success']
    wrong_exit = [f'Program {PROGRAM} invoke [1]', f'Program {OTHER} success', _fulfilled()]
    assert program_event_payloads(skipped_depth, PROGRAM) == []
    assert program_event_payloads(wrong_exit, PROGRAM) == []


def test_lines_outside_any_frame_are_dropped():
    assert program_event_payloads([_fulfilled()], PROGRAM) == []


def _mixed_logs() -> list:
    # A forged frame naming an unrelated swap, then our program's genuine frame.
    return [*_frame(OTHER, _fulfilled(UNRELATED_KEY), _completed(UNRELATED_KEY)), *_frame(PROGRAM, _fulfilled())]


def _keys(events):
    return [(name, bytes(fields['swap_key'])) for name, fields in events]


def test_the_live_feed_publishes_only_our_frames_events():
    feed = LiveFeed('ws://x', PROGRAM)
    notification = {
        'params': {'result': {'context': {'slot': 7}, 'value': {'signature': 's', 'err': None, 'logs': _mixed_logs()}}}
    }
    assert feed.handle_notification(notification) == 1
    (pending,) = feed.take()
    assert _keys(pending.events) == [('SwapFulfilled', SWAP_KEY)]


def test_the_backfill_publishes_only_our_frames_events():
    class Rpc:
        def get_signatures_for_address(self, program_id, before=None, until=None, limit=1000):
            return [] if before else [{'signature': 's', 'slot': 7, 'blockTime': 1400, 'err': None}]

        def get_transaction(self, sig):
            return {'blockTime': 1400, 'meta': {'logMessages': _mixed_logs()}}

        def _call(self, method, params):
            return None

    live = SimpleNamespace(session=1, start=lambda: live, take=lambda: [])
    out, gaps = Topic('events'), Topic('gaps')
    feed = EventFeed(lambda: Rpc(), live, lambda: 'before-history', out, gaps, PROGRAM, clock=lambda: 1400)
    events = out.subscribe('t')
    feed.setup()
    feed._backfill.until_sig = None
    for _ in range(3):
        feed.tick(1400)
    assert _keys((r.name, r.fields) for r in events.drain()) == [('SwapFulfilled', SWAP_KEY)]
