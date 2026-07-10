"""Phase 9 — unit tests for the swap-intake instruction builders (open_or_request / resolve_pool).

No chain: `_send` is stubbed to capture the assembled Instruction. Asserts the 8-byte discriminator
(independently recomputed via Anchor's sha256("global:<name>") formula), the Borsh arg body, and the
exact account-meta order + signer/writable flags — these must match the Rust account structs
(open_or_request.rs OpenOrRequest / resolve_pool.rs ResolvePool).
"""

import hashlib

import pytest
from solders.keypair import Keypair

from allways.solana import layouts, pdas
from allways.solana.client import SLOT_HASHES, SYSTEM_PROGRAM, AllwaysSolanaClient
from allways.solana.program import resolve_program_id

PID = resolve_program_id()


def _global_disc(name: str) -> bytes:
    return hashlib.sha256(f'global:{name}'.encode()).digest()[:8]


@pytest.fixture
def client():
    c = AllwaysSolanaClient('http://localhost:9', keypair=Keypair())
    cap = {}

    def fake_send(ixs, **kw):
        cap['ixs'] = ixs
        return 'SIG'

    c._send = fake_send
    c._cap = cap
    return c


def _ix(client):
    ixs = client._cap['ixs']
    assert len(ixs) == 1
    return ixs[0]


def _metas(ix):
    return [(m.pubkey, m.is_signer, m.is_writable) for m in ix.accounts]


def test_ix_discriminators_match_anchor_global_formula():
    for name in ('open_or_request', 'resolve_pool'):
        assert layouts.IX_DISCRIMINATORS[name] == _global_disc(name), f'{name} ix discriminator mismatch'


def test_open_or_request_ix(client):
    miner = Keypair().pubkey()
    client.open_or_request(miner, 'sol', 'btc')
    ix = _ix(client)
    assert ix.data[:8] == layouts.IX_DISCRIMINATORS['open_or_request']
    assert ix.data[8:] == layouts.IX_OPEN_OR_REQUEST_ARGS.build({'from_chain': 'sol', 'to_chain': 'btc'})
    assert _metas(ix) == [
        (client.keypair.pubkey(), True, True),
        (pdas.config_pda(PID), False, False),
        (miner, False, False),
        (pdas.miner_state_pda(miner, PID), False, True),
        (pdas.quote_pda(miner, 'sol', 'btc', PID), False, False),
        (pdas.pool_pda(miner, PID), False, True),
        (pdas.treasury_pda(PID), False, True),
        (pdas.reservation_pda(miner, PID), False, True),
        (SYSTEM_PROGRAM, False, False),
    ]


def test_resolve_pool_ix(client):
    miner = Keypair().pubkey()
    client.resolve_pool(miner)
    ix = _ix(client)
    assert ix.data[:8] == layouts.IX_DISCRIMINATORS['resolve_pool']
    assert ix.data[8:] == b''  # no args
    assert _metas(ix) == [
        (client.keypair.pubkey(), True, True),
        (pdas.config_pda(PID), False, False),
        (miner, False, False),
        (pdas.miner_state_pda(miner, PID), False, True),
        (pdas.pool_pda(miner, PID), False, True),
        (pdas.reservation_pda(miner, PID), False, True),
        (SLOT_HASHES, False, False),
        (SYSTEM_PROGRAM, False, False),
    ]
