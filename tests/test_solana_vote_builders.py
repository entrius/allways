"""B2.0 — unit tests for the validator vote/claim instruction builders.

No chain: builders are exercised with `_send` stubbed to capture the assembled Instruction. Asserts the
8-byte discriminator (independently recomputed via Anchor's sha256("global:<name>") formula), the Borsh
arg body, and the exact account-meta order + signer/writable flags — these must match the Rust test
helpers (tests/test_swap.rs initiate_ix/confirm_ix/timeout_ix, test_consensus.rs vote_activate_ix).
"""

import hashlib

import pytest
from Crypto.Hash import keccak as keccak_lib
from solders.keypair import Keypair

from allways.solana import layouts, pdas
from allways.solana.client import SYSTEM_PROGRAM, AllwaysSolanaClient, swap_key_from_tx_hash, weights_round_key
from allways.solana.program import resolve_program_id

PID = resolve_program_id()
SK = bytes(range(32))  # a stand-in 32-byte swap_key


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
    for name in (
        'submit_swap_claim',
        'vote_initiate',
        'confirm_swap',
        'timeout_swap',
        'close_stale_claim',
        'vote_activate',
        'vote_set_weights',
        'mark_fulfilled',
        'extend_timeout',
        'extend_reservation',
    ):
        assert layouts.IX_DISCRIMINATORS[name] == _global_disc(name), f'{name} ix discriminator mismatch'


def test_swap_key_from_tx_hash_is_keccak256():
    # keccak256("") known vector — guards against a sha256/keccak mix-up.
    assert swap_key_from_tx_hash('').hex() == 'c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470'
    assert len(swap_key_from_tx_hash('deadbeef')) == 32


def test_submit_swap_claim_ix(client):
    miner = Keypair().pubkey()
    client.submit_swap_claim(miner, SK, 'txhash123', 800_001)
    ix = _ix(client)
    assert ix.data[:8] == layouts.IX_DISCRIMINATORS['submit_swap_claim']
    assert ix.data[8:] == layouts.IX_SUBMIT_CLAIM_ARGS.build(
        {'swap_key': SK, 'from_tx_hash': 'txhash123', 'from_tx_block': 800_001}
    )
    assert _metas(ix) == [
        (client.keypair.pubkey(), True, True),
        (pdas.config_pda(PID), False, False),
        (miner, False, False),
        (pdas.reservation_pda(miner, PID), False, True),
        (pdas.swap_pda(SK, PID), False, True),
        (SYSTEM_PROGRAM, False, False),
    ]


def test_vote_initiate_ix(client):
    miner = Keypair().pubkey()
    client.vote_initiate(SK, miner)
    ix = _ix(client)
    assert ix.data[:8] == layouts.IX_DISCRIMINATORS['vote_initiate']
    assert ix.data[8:] == SK
    assert _metas(ix) == [
        (client.keypair.pubkey(), True, True),
        (pdas.config_pda(PID), False, False),
        (miner, False, False),
        (pdas.miner_state_pda(miner, PID), False, True),
        (pdas.reservation_pda(miner, PID), False, True),
        (pdas.vote_round_pda(pdas.REQ_INITIATE, miner, PID), False, True),
        (pdas.swap_pda(SK, PID), False, True),
        (SYSTEM_PROGRAM, False, False),
    ]


def test_vote_set_weights_ix(client):
    weights = [3, 0, 7]
    keys = [bytes(Keypair().pubkey()) for _ in range(3)]
    client.vote_set_weights(weights, keys)
    ix = _ix(client)
    # round_key mirrors consensus::weights_hash: keccak(REQ_SET_WEIGHTS || keys || weights LE).
    kec = keccak_lib.new(digest_bits=256)
    kec.update(bytes([pdas.REQ_SET_WEIGHTS]))
    for k in keys:
        kec.update(k)
    for w in weights:
        kec.update(w.to_bytes(8, 'little'))
    round_key = kec.digest()
    assert weights_round_key(keys, weights) == round_key
    assert ix.data[:8] == layouts.IX_DISCRIMINATORS['vote_set_weights']
    # Borsh: Vec<u64> (u32 LE length prefix + elements) then the raw 32-byte round_key.
    assert ix.data[8:] == layouts.IX_SET_WEIGHTS_ARGS.build({'weights': weights, 'round_key': round_key})
    assert _metas(ix) == [
        (client.keypair.pubkey(), True, True),
        (pdas.config_pda(PID), False, True),
        (pdas.vote_round_pda(pdas.REQ_SET_WEIGHTS, round_key, PID), False, True),
        (SYSTEM_PROGRAM, False, False),
    ]


def test_confirm_swap_ix(client):
    miner = Keypair().pubkey()
    client.confirm_swap(SK, miner, 'BTC', 'tao')
    ix = _ix(client)
    assert ix.data[:8] == layouts.IX_DISCRIMINATORS['confirm_swap']
    assert ix.data[8:] == layouts.IX_CONFIRM_SWAP_ARGS.build({'swap_key': SK, 'from_chain': 'BTC', 'to_chain': 'tao'})
    assert _metas(ix) == [
        (client.keypair.pubkey(), True, True),
        (pdas.config_pda(PID), False, False),
        (miner, False, False),
        (pdas.miner_state_pda(miner, PID), False, True),
        (pdas.collateral_vault_pda(miner, PID), False, True),
        (pdas.treasury_pda(PID), False, True),
        (pdas.swap_pda(SK, PID), False, True),
        (pdas.stats_pda(miner, 'BTC', 'tao', PID), False, True),
        (pdas.vote_round_pda(pdas.REQ_CONFIRM, SK, PID), False, True),
        (SYSTEM_PROGRAM, False, False),
    ]


def test_close_stale_claim_ix(client):
    miner = Keypair().pubkey()
    client.close_stale_claim(miner, SK)
    ix = _ix(client)
    assert ix.data[:8] == layouts.IX_DISCRIMINATORS['close_stale_claim']
    assert ix.data[8:] == SK
    # Accounts match CloseStaleClaim<'info>: caller(signer,writable), miner, reservation(mut), swap(mut).
    assert _metas(ix) == [
        (client.keypair.pubkey(), True, True),
        (miner, False, False),
        (pdas.reservation_pda(miner, PID), False, True),
        (pdas.swap_pda(SK, PID), False, True),
    ]


def test_timeout_swap_ix(client):
    miner = Keypair().pubkey()
    user = Keypair().pubkey()
    client.timeout_swap(SK, miner, user)
    ix = _ix(client)
    assert ix.data[:8] == layouts.IX_DISCRIMINATORS['timeout_swap']
    assert ix.data[8:] == SK
    assert _metas(ix) == [
        (client.keypair.pubkey(), True, True),
        (pdas.config_pda(PID), False, False),
        (miner, False, False),
        (pdas.miner_state_pda(miner, PID), False, True),
        (pdas.collateral_vault_pda(miner, PID), False, True),
        (user, False, True),
        (pdas.swap_pda(SK, PID), False, True),
        (pdas.vote_round_pda(pdas.REQ_TIMEOUT, SK, PID), False, True),
        (SYSTEM_PROGRAM, False, False),
    ]


def test_vote_activate_ix(client):
    miner = Keypair().pubkey()
    client.vote_activate(miner)
    ix = _ix(client)
    assert ix.data[:8] == layouts.IX_DISCRIMINATORS['vote_activate']
    assert ix.data[8:] == b''  # no args
    assert _metas(ix) == [
        (client.keypair.pubkey(), True, True),
        (pdas.config_pda(PID), False, False),
        (miner, False, False),
        (pdas.miner_state_pda(miner, PID), False, True),
        (pdas.vote_round_pda(pdas.REQ_ACTIVATE, miner, PID), False, True),
        (SYSTEM_PROGRAM, False, False),
    ]


def test_mark_fulfilled_ix(client):
    # miner-only: signer is this client's keypair, not writable.
    client.mark_fulfilled(SK, 'destTx', 200)
    ix = _ix(client)
    assert ix.data[:8] == layouts.IX_DISCRIMINATORS['mark_fulfilled']
    assert ix.data[8:] == layouts.IX_MARK_FULFILLED_ARGS.build(
        {'swap_key': SK, 'to_tx_hash': 'destTx', 'to_tx_block': 200}
    )
    assert _metas(ix) == [
        (client.keypair.pubkey(), True, False),
        (pdas.miner_state_pda(client.keypair.pubkey(), PID), False, True),
        (pdas.swap_pda(SK, PID), False, True),
    ]


def test_extend_timeout_ix(client):
    miner = Keypair().pubkey()
    client.extend_timeout(SK, miner, 1_700_009_999)
    ix = _ix(client)
    assert ix.data[:8] == layouts.IX_DISCRIMINATORS['extend_timeout']
    assert ix.data[8:] == layouts.IX_EXTEND_TIMEOUT_ARGS.build({'swap_key': SK, 'target_at': 1_700_009_999})
    # validator is a signer but NOT writable (no init/payer here).
    assert _metas(ix) == [
        (client.keypair.pubkey(), True, False),
        (pdas.config_pda(PID), False, False),
        (miner, False, False),
        (pdas.miner_state_pda(miner, PID), False, True),
        (pdas.swap_pda(SK, PID), False, True),
    ]


def test_extend_reservation_ix(client):
    miner = Keypair().pubkey()
    client.extend_reservation(miner, 1_700_009_999)
    ix = _ix(client)
    assert ix.data[:8] == layouts.IX_DISCRIMINATORS['extend_reservation']
    assert ix.data[8:] == layouts.IX_EXTEND_RESERVATION_ARGS.build({'target_at': 1_700_009_999})
    assert _metas(ix) == [
        (client.keypair.pubkey(), True, False),
        (pdas.config_pda(PID), False, False),
        (miner, False, False),
        (pdas.miner_state_pda(miner, PID), False, True),
        (pdas.reservation_pda(miner, PID), False, True),
    ]
