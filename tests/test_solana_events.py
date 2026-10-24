"""Unit tests for Solana event decode + cursor ingest.

Decoder: discriminators recomputed independently via Anchor's sha256("event:<Name>")[:8] formula, plus the
borsh round-trip of the slash event. Ingest: cursor paging (oldest-first, until-cursor) + skips failed txs, driven
by a fake client.
"""

import hashlib
import re
from pathlib import Path

import pytest
from solders.keypair import Keypair

from allways.solana import events
from allways.solana.events import SolanaEventIngest, decode_event


@pytest.fixture(autouse=True)
def _no_history_horizon(monkeypatch):
    # These fixtures stamp entries with fixed historic blockTimes; the lookback cut has its own tests.
    monkeypatch.setattr(events, 'HISTORY_HORIZON_SECS', 10**12)


def _event_disc(name: str) -> bytes:
    return hashlib.sha256(f'event:{name}'.encode()).digest()[:8]


def test_event_discriminators_match_anchor_formula():
    for name, disc in events.EVENT_DISCRIMINATORS.items():
        assert disc == _event_disc(name), f'{name} event discriminator mismatch'
    # every discriminator has a layout + pubkey-field entry
    assert set(events.EVENT_DISCRIMINATORS) == set(events.EVENT_LAYOUTS)
    assert set(events.EVENT_DISCRIMINATORS) == set(events.EVENT_PUBKEY_FIELDS)


def _encode(name: str, value: dict) -> bytes:
    return events.EVENT_DISCRIMINATORS[name] + events.EVENT_LAYOUTS[name].build(value)


def test_decode_swap_timed_out_roundtrip():
    # The slash relay's whole input: absolute figures plus the payee they are owed to. `payee` is the
    # last field — appended later, so a stale decoder truncates rather than mis-reads the figures.
    miner = Keypair().pubkey()
    raw = _encode(
        'SwapTimedOut',
        {
            'swap_key': bytes(range(32)),
            'miner': bytes(miner),
            'collateral_amount': 3_000_000_000,
            'slash': 0,
            'collateral_chain': 'tao',
            'penalty': 3_300_000_000,
            'reimbursement': 3_300_000_000,
            'payee': '5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty',
            'hotkey': bytes(range(32)),
        },
    )
    name, f = decode_event(raw)
    assert name == 'SwapTimedOut'
    assert f.miner == miner and bytes(f.swap_key) == bytes(range(32))
    assert f.collateral_chain == 'tao' and f.slash == 0
    assert f.penalty == 3_300_000_000 and f.reimbursement == 3_300_000_000
    assert f.payee == '5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty'
    assert bytes(f.hotkey) == bytes(range(32))  # raw bytes, like swap_key


def test_decode_swap_timed_out_carries_no_payee_when_it_settled_locally():
    # A "sol" verdict already paid the user on Solana; the empty string is the on-the-wire shape of
    # "nothing is owed elsewhere", and it must decode as a value, not a missing field.
    raw = _encode(
        'SwapTimedOut',
        {
            'swap_key': bytes(range(32)),
            'miner': bytes(Keypair().pubkey()),
            'collateral_amount': 2_000_000_000,
            'slash': 2_200_000_000,
            'collateral_chain': 'sol',
            'penalty': 2_200_000_000,
            'reimbursement': 2_200_000_000,
            'payee': '',
            'hotkey': bytes(32),
        },
    )
    _, f = decode_event(raw)
    assert f.payee == '' and f.slash == 2_200_000_000
    assert bytes(f.hotkey) == bytes(32)  # never-bound miner ⇒ zeroed pin decodes as a value


PRIOR_VALUES = {
    'QuoteSet': {
        'miner': bytes(32),
        'from_chain': 'btc',
        'to_chain': 'tao',
        'collateral_chain': 'tao',
        'rate': 1,
        'liquidity': 2,
        'updated_at': 3,
        'update_fee': 4,
    },
    'ReservationRequested': {'miner': bytes(32), 'router': bytes(32), 'requests': 1, 'fee_paid': 5},
    'PoolResolved': {'miner': bytes(32), 'winner': bytes(32), 'requests': 1, 'collateral_chain': 'sol'},
}


APPENDED_VALUES = {
    'QuoteSet': {'miner_from_addr': 'bc1qsrc', 'miner_to_addr': '5Cdst'},
    'ReservationRequested': {'collateral_chain': 'tao'},
    'PoolResolved': {'finalize_by': 77},
}


@pytest.mark.parametrize('name', sorted(events.EVENT_PRIOR_LAYOUTS))
def test_events_emitted_before_the_upgrade_still_decode(name):
    # History from before an upgrade stays in the logs; a backfill must not drop it.
    (prior,) = events.EVENT_PRIOR_LAYOUTS[name]
    decoded = decode_event(events.EVENT_DISCRIMINATORS[name] + prior.build(PRIOR_VALUES[name]))
    assert decoded is not None and decoded[0] == name
    assert all(getattr(decoded[1], k) is not None for k in PRIOR_VALUES[name])


@pytest.mark.parametrize('name', sorted(events.EVENT_PRIOR_LAYOUTS))
def test_a_decoder_from_before_the_upgrade_reads_the_new_shape(name):
    # Validators, miners and the indexer on the previous package see the appended fields as trailing
    # bytes; the older layout must parse the new body to the same leading values.
    (prior,) = events.EVENT_PRIOR_LAYOUTS[name]
    current = events.EVENT_LAYOUTS[name]
    extra = APPENDED_VALUES[name]
    parsed = prior.parse(current.build({**PRIOR_VALUES[name], **extra}))
    assert {k: parsed[k] for k in PRIOR_VALUES[name]} == PRIOR_VALUES[name]


def test_decode_unknown_discriminator_returns_none():
    assert decode_event(b'\x00' * 8 + b'junk') is None
    assert decode_event(b'\x01\x02') is None  # too short


def test_decode_foreign_version_payload_returns_none():
    # A pre-v3 SwapTimedOut body (no collateral_chain/penalty/reimbursement/payee): a genesis rescan
    # walks the same program id's retained history and WILL meet these — they must drop, not raise.
    from borsh_construct import U64, CStruct
    from construct import Bytes as _Raw

    old = CStruct('swap_key' / _Raw(32), 'miner' / _Raw(32), 'collateral_amount' / U64, 'slash' / U64)
    body = old.build({'swap_key': bytes(32), 'miner': bytes(32), 'collateral_amount': 5, 'slash': 3})
    assert decode_event(events.EVENT_DISCRIMINATORS['SwapTimedOut'] + body) is None
    # Truncated garbage under a known discriminator drops the same way.
    assert decode_event(events.EVENT_DISCRIMINATORS['QuoteSet'] + b'\x01\x02\x03') is None


def test_every_contract_event_is_registered():
    # Every #[event] struct in the program source must decode, or the indexer/validator
    # silently drop it (decode_event returns None for unknown discriminators).
    events_rs = (
        Path(__file__).resolve().parents[1] / 'smart-contracts/solana/programs/allways_swap_manager/src/events.rs'
    )
    declared = set(re.findall(r'#\[event\]\s*pub struct (\w+)', events_rs.read_text()))
    assert declared, 'no #[event] structs parsed from events.rs — pattern drifted?'
    missing = declared - set(events.EVENT_DISCRIMINATORS)
    assert not missing, f'contract events missing from the Python decoder registry: {sorted(missing)}'


# ---- ingest cursor ----


class FakeRpc:
    def __init__(self, pages, transactions=None):
        # pages: list of batches as the RPC would return them (newest-first within a call)
        self._pages = pages
        self.transactions = transactions or {}
        self.calls = []

    def get_transaction(self, signature, commitment='confirmed'):
        # What the listing omitted, the transaction may still carry.
        return self.transactions.get(signature)

    def get_signatures_for_address(self, program_id, before=None, until=None, limit=1000):
        self.calls.append({'before': before, 'until': until})
        # Simple model: first call returns the single page; subsequent return empty (no paging needed here).
        if before is None:
            return self._pages
        return []


class FakeClient:
    def __init__(self, pages, logs_by_sig, transactions=None):
        self.program_id = 'PROG'
        self.rpc = FakeRpc(pages, transactions)
        self._logs = logs_by_sig

    def get_event_logs(self, sig):
        return self._logs.get(sig, [])


def test_ingest_returns_oldest_first_and_advances_cursor():
    miner = Keypair().pubkey()
    ev = _encode('MinerActivated', {'miner': bytes(miner), 'at': 1})
    # RPC returns newest-first: sigB (newer) then sigA (older).
    pages = [
        {'signature': 'sigB', 'slot': 20, 'blockTime': 1_700_000_020, 'err': None},
        {'signature': 'sigA', 'slot': 10, 'blockTime': 1_700_000_010, 'err': None},
    ]
    client = FakeClient(pages, {'sigA': [ev], 'sigB': [ev]})
    ingest = SolanaEventIngest(client)
    records, cursor = ingest.poll(until_sig=None)
    # Oldest-first: sigA before sigB.
    assert [r.signature for r in records] == ['sigA', 'sigB']
    assert records[0].slot == 10 and records[1].slot == 20
    assert cursor == 'sigB'  # newest seen


def test_ingest_skips_failed_tx_and_empty_is_noop():
    miner = Keypair().pubkey()
    ev = _encode('MinerActivated', {'miner': bytes(miner), 'at': 1})
    pages = [
        {'signature': 'good', 'slot': 5, 'blockTime': 1, 'err': None},
        {'signature': 'bad', 'slot': 4, 'blockTime': 1, 'err': {'InstructionError': []}},
    ]
    client = FakeClient(pages, {'good': [ev], 'bad': [ev]})
    records, cursor = SolanaEventIngest(client).poll(until_sig=None)
    assert [r.signature for r in records] == ['good']  # failed tx skipped

    # Nothing new → cursor unchanged, no records.
    empty = FakeClient([], {})
    recs, cur = SolanaEventIngest(empty).poll(until_sig='good')
    assert recs == [] and cur == 'good'


def test_poll_holds_cursor_at_unstamped_tip():
    miner = Keypair().pubkey()
    ev = _encode('MinerActivated', {'miner': bytes(miner), 'at': 1})
    # Newest-first: stamped tip, unstamped middle (fresh), stamped oldest.
    pages = [
        {'signature': 'sigC', 'slot': 21, 'blockTime': 1_700_000_021, 'err': None},
        {'signature': 'sigB', 'slot': 20, 'blockTime': None, 'err': None},
        {'signature': 'sigA', 'slot': 10, 'blockTime': 1_700_000_010, 'err': None},
    ]
    client = FakeClient(pages, {s: [ev] for s in ('sigA', 'sigB', 'sigC')})
    records, cursor = SolanaEventIngest(client).poll(until_sig=None)
    # The cursor holds before sigB so its events are re-read once stamped; sigC
    # (newer than the hold point) is deliberately not consumed either.
    assert [r.signature for r in records] == ['sigA']
    assert cursor == 'sigA'


def test_poll_reingests_previously_unstamped_once_stamped():
    miner = Keypair().pubkey()
    ev = _encode('MinerActivated', {'miner': bytes(miner), 'at': 1})
    pages = [
        {'signature': 'sigC', 'slot': 21, 'blockTime': 1_700_000_021, 'err': None},
        {'signature': 'sigB', 'slot': 20, 'blockTime': 1_700_000_020, 'err': None},
    ]
    client = FakeClient(pages, {'sigB': [ev], 'sigC': [ev]})
    records, cursor = SolanaEventIngest(client).poll(until_sig='sigA')
    assert [r.signature for r in records] == ['sigB', 'sigC']
    assert cursor == 'sigC'


class PagingRpc:
    """A faithful `get_signatures_for_address`: newest-first, `before`-paged, stopping at `until`."""

    def __init__(self, sigs):
        self.sigs = sigs  # newest-first

    def get_transaction(self, signature, commitment='confirmed'):
        # What the listing omitted, the transaction may still carry.
        return self.transactions.get(signature)

    def get_signatures_for_address(self, program_id, before=None, until=None, limit=1000):
        start = 0
        if before is not None:
            start = next(i for i, s in enumerate(self.sigs) if s['signature'] == before) + 1
        out = []
        for s in self.sigs[start:]:
            if until is not None and s['signature'] == until:
                break
            out.append(s)
            if len(out) == limit:
                break
        return out


class PagingClient:
    def __init__(self, sigs, ev):
        self.program_id = 'PROG'
        self.rpc = PagingRpc(sigs)
        self._ev = ev

    def get_event_logs(self, sig):
        return [self._ev]


def test_pagination_drains_a_backlog_across_ticks_without_dropping_the_gap():
    # A backlog deeper than max_pages*page_size must not silently drop the older gap. With
    # max_pages=2, page_size=2 the poller can reach only 4 of the 5 new signatures in one pass; it
    # buffers and resumes deeper next tick, holding its cursor until the whole window is assembled.
    ev = _encode('MinerActivated', {'miner': bytes(Keypair().pubkey()), 'at': 1})
    # newest-first s5..s1; all stamped so nothing holds at an unstamped tip.
    sigs = [{'signature': f's{i}', 'slot': i, 'blockTime': 1_700_000_000 + i, 'err': None} for i in range(5, 0, -1)]
    ingest = SolanaEventIngest(PagingClient(sigs, ev), max_pages=2, page_size=2)

    records, cursor = ingest.poll(until_sig=None)
    assert records == [] and cursor is None, 'gap still open — cursor holds, nothing consumed yet'

    records, cursor = ingest.poll(until_sig=None)
    assert [r.signature for r in records] == ['s1', 's2', 's3', 's4', 's5'], 'full window, oldest-first'
    assert cursor == 's5'


def test_a_fresh_cursor_reads_back_only_to_the_horizon():
    # A fresh database (or long outage) must not page back to the program's first tx.
    ev = _encode('MinerActivated', {'miner': bytes(Keypair().pubkey()), 'at': 1})
    now = 1_700_010_000
    sigs = [{'signature': f's{i}', 'slot': i, 'blockTime': now - 1000 * (5 - i), 'err': None} for i in range(5, 0, -1)]
    ingest = SolanaEventIngest(PagingClient(sigs, ev), page_size=2, horizon_secs=2500, clock=lambda: now)
    records, cursor = ingest.poll(until_sig=None)
    assert [r.signature for r in records] == ['s3', 's4', 's5'] and cursor == 's5'


def test_a_backlog_is_decoded_a_budget_per_pass_and_resumes_from_the_cursor():
    # Each pass decodes at most max_decode entries and stops the cursor there; the next pass carries on.
    ev = _encode('MinerActivated', {'miner': bytes(Keypair().pubkey()), 'at': 1})
    sigs = [{'signature': f's{i}', 'slot': i, 'blockTime': 1_700_000_000 + i, 'err': None} for i in range(5, 0, -1)]
    ingest = SolanaEventIngest(PagingClient(sigs, ev), max_decode=2)
    seen, cursor = [], None
    for _ in range(3):
        records, cursor = ingest.poll(until_sig=cursor)
        seen.append([r.signature for r in records])
    assert seen == [['s1', 's2'], ['s3', 's4'], ['s5']] and cursor == 's5'


class FlakyPagingRpc(PagingRpc):
    """A faithful pager that raises exactly once, on the page requested with `before == fail_before`
    (the second page of the first pass here), then behaves normally on every later call."""

    def __init__(self, sigs, fail_before):
        super().__init__(sigs)
        self._fail_before = fail_before

    def get_transaction(self, signature, commitment='confirmed'):
        # What the listing omitted, the transaction may still carry.
        return self.transactions.get(signature)

    def get_signatures_for_address(self, program_id, before=None, until=None, limit=1000):
        if self._fail_before is not None and before == self._fail_before:
            self._fail_before = None  # blip once, then recover
            raise RuntimeError('rpc paging blip')
        return super().get_signatures_for_address(program_id, before=before, until=until, limit=limit)


def test_pagination_rolls_back_a_partial_page_on_a_mid_pagination_rpc_failure():
    # If paging fails partway, the buffered partial page must be rolled back. The caller
    # holds its cursor and re-pages from the same resume point next tick, so a retained partial would be
    # re-fetched — duplicating and reordering records in a later drain.
    ev = _encode('MinerActivated', {'miner': bytes(Keypair().pubkey()), 'at': 1})
    sigs = [{'signature': f's{i}', 'slot': i, 'blockTime': 1_700_000_000 + i, 'err': None} for i in range(5, 0, -1)]
    client = PagingClient(sigs, ev)
    client.rpc = FlakyPagingRpc(sigs, fail_before='s4')  # page 1 buffers [s5,s4]; page 2 (before=s4) blips
    ingest = SolanaEventIngest(client, max_pages=2, page_size=2)

    with pytest.raises(RuntimeError):
        ingest.poll(until_sig=None)  # partial page must be rolled back, not left buffered

    # Cursor was held; re-page cleanly. Without the rollback the drain would carry duplicated s4/s5.
    records, cursor = ingest.poll(until_sig=None)
    assert records == [] and cursor is None, 'gap still open — resumes deeper next tick'
    records, cursor = ingest.poll(until_sig=None)
    assert [r.signature for r in records] == ['s1', 's2', 's3', 's4', 's5'], 'full window, no dupes, oldest-first'
    assert cursor == 's5'


def test_poll_abandons_an_entry_nothing_can_stamp():
    miner = Keypair().pubkey()
    ev = _encode('MinerActivated', {'miner': bytes(miner), 'at': 1})
    # sigOld is unstamped past UNSTAMPED_GIVE_UP_SLOTS and the tx has no blockTime either:
    # nothing can stamp it, so the cursor moves past (its events are written off).
    pages = [
        {'signature': 'sigTip', 'slot': 500, 'blockTime': 1_700_000_500, 'err': None},
        {'signature': 'sigOld', 'slot': 100, 'blockTime': None, 'err': None},
    ]
    client = FakeClient(pages, {'sigTip': [ev], 'sigOld': [ev]}, {'sigOld': {'blockTime': None}})
    records, cursor = SolanaEventIngest(client).poll(until_sig=None)
    assert [r.signature for r in records] == ['sigTip']
    assert cursor == 'sigTip'


def test_an_entry_the_listing_left_unstamped_is_stamped_from_its_transaction():
    """A listing without a blockTime is not a chain without one: the events are ingested, not written off."""
    miner = Keypair().pubkey()
    ev = _encode('MinerActivated', {'miner': bytes(miner), 'at': 1})
    pages = [
        {'signature': 'sigTip', 'slot': 500, 'blockTime': 1_700_000_500, 'err': None},
        {'signature': 'sigOld', 'slot': 100, 'blockTime': None, 'err': None},
    ]
    client = FakeClient(pages, {'sigTip': [ev], 'sigOld': [ev]}, {'sigOld': {'blockTime': 1_700_000_100}})

    records, cursor = SolanaEventIngest(client).poll(until_sig=None)

    assert [r.signature for r in records] == ['sigOld', 'sigTip']
    assert [r.block_time for r in records] == [1_700_000_100, 1_700_000_500]
    assert cursor == 'sigTip'


def test_an_unreadable_transaction_leaves_the_entry_abandoned_not_crashing(caplog):
    miner = Keypair().pubkey()
    ev = _encode('MinerActivated', {'miner': bytes(miner), 'at': 1})
    pages = [
        {'signature': 'sigTip', 'slot': 500, 'blockTime': 1_700_000_500, 'err': None},
        {'signature': 'sigOld', 'slot': 100, 'blockTime': None, 'err': None},
    ]
    client = FakeClient(pages, {'sigTip': [ev], 'sigOld': [ev]})
    client.rpc.get_transaction = lambda *a, **k: (_ for _ in ()).throw(RuntimeError('rpc down'))

    records, cursor = SolanaEventIngest(client).poll(until_sig=None)

    assert [r.signature for r in records] == ['sigTip'] and cursor == 'sigTip'
