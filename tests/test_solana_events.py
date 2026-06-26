"""B3.1 — unit tests for Solana event decode + cursor ingest.

Decoder: discriminators recomputed independently via Anchor's sha256("event:<Name>")[:8] formula, plus a
borsh round-trip per event. Ingest: cursor paging (oldest-first, until-cursor) + skips failed txs, driven
by a fake client.
"""

import hashlib

from solders.keypair import Keypair

from allways.solana import events
from allways.solana.events import SolanaEventIngest, decode_event


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


def test_decode_quote_set_roundtrip():
    miner = Keypair().pubkey()
    raw = _encode(
        'QuoteSet',
        {
            'miner': bytes(miner),
            'from_chain': 'btc',
            'to_chain': 'tao',
            'rate': 345 * 10**18,
            'liquidity': 1_000,
            'updated_at': 1_700_000_000,
            'update_fee': 50_000,
        },
    )
    name, f = decode_event(raw)
    assert name == 'QuoteSet'
    assert f.miner == miner  # converted to Pubkey
    assert f.from_chain == 'btc' and f.to_chain == 'tao'
    assert f.rate == 345 * 10**18 and f.updated_at == 1_700_000_000


def test_decode_swap_completed_roundtrip():
    miner = Keypair().pubkey()
    raw = _encode(
        'SwapCompleted',
        {
            'swap_key': bytes(range(32)),
            'miner': bytes(miner),
            'sol_amount': 2_000_000_000,
            'fee': 20_000_000,
            'from_chain': 'btc',
            'to_chain': 'tao',
            'from_amount': 100_000,
            'to_amount': 345_000_000,
            'rate': 345 * 10**18,
        },
    )
    name, f = decode_event(raw)
    assert name == 'SwapCompleted'
    assert f.miner == miner
    assert f.to_amount == 345_000_000 and f.from_amount == 100_000


def test_decode_miner_activated_and_collateral():
    miner = Keypair().pubkey()
    name, f = decode_event(_encode('MinerActivated', {'miner': bytes(miner), 'at': 1_700_000_111}))
    assert name == 'MinerActivated' and f.miner == miner and f.at == 1_700_000_111

    name, f = decode_event(_encode('CollateralPosted', {'miner': bytes(miner), 'amount': 5, 'total': 9}))
    assert name == 'CollateralPosted' and f.total == 9


def test_decode_unknown_discriminator_returns_none():
    assert decode_event(b'\x00' * 8 + b'junk') is None
    assert decode_event(b'\x01\x02') is None  # too short


# ---- ingest cursor ----


class FakeRpc:
    def __init__(self, pages):
        # pages: list of batches as the RPC would return them (newest-first within a call)
        self._pages = pages
        self.calls = []

    def get_signatures_for_address(self, program_id, before=None, until=None, limit=1000):
        self.calls.append({'before': before, 'until': until})
        # Simple model: first call returns the single page; subsequent return empty (no paging needed here).
        if before is None:
            return self._pages
        return []


class FakeClient:
    def __init__(self, pages, logs_by_sig):
        self.program_id = 'PROG'
        self.rpc = FakeRpc(pages)
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
