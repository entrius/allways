"""Program events are read only from lines the allways program itself logged."""

import base64
import tempfile
from pathlib import Path
from types import SimpleNamespace

from solders.keypair import Keypair

from allways.solana.client import AllwaysSolanaClient
from allways.solana.events import EventRecord, decode_event, program_event_payloads
from allways.solana.layouts import EVENT_DISCRIMINATORS, EVENT_LAYOUTS
from allways.solana.program_feed import events_from_logs
from allways.validator.event_index import SolanaEventIndex
from allways.validator.solana_swap_loop import SwapDecision
from allways.validator.state_store import ValidatorStateStore
from tests.test_solana_swap_loop import loop_with, make_swap

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


def test_events_outside_our_frame_never_reach_the_delivery_ledger():
    # Another program's lines naming this swap's delivery for an unrelated swap leave the swap confirmable.
    txs = {'a': _frame(OTHER, _fulfilled(UNRELATED_KEY)), 'b': _frame(OTHER, _completed(UNRELATED_KEY))}
    client = SimpleNamespace(
        program_id=PROGRAM,
        rpc=SimpleNamespace(get_transaction=lambda sig: {'meta': {'logMessages': txs[sig]}}),
    )
    records = [
        EventRecord(name=name, fields=fields, slot=1, block_time=1400, signature=sig)
        for sig in txs
        for name, fields in map(decode_event, AllwaysSolanaClient.get_event_logs(client, sig))
    ]
    assert records == []

    store = ValidatorStateStore(Path(tempfile.mkdtemp()) / 'state.db')
    SolanaEventIndex(store).ingest(records, {})
    loop, _ = loop_with(state_store=store)
    swap = make_swap(key=SWAP_KEY)
    swap.miner = str(MINER)
    assert loop.decide(swap, now=1500).decision == SwapDecision.CONFIRM
