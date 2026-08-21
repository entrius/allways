"""W2b — instruction builders for quote-level backing, plus the W1/W2 setters and attestation writers
that had no client builder until now.

Same approach as the other builder suites: no chain — `_send` is stubbed to capture the assembled
Instruction, and each discriminator is independently recomputed via Anchor's sha256("global:<name>")
formula, so a mis-copied IDL is caught here rather than on localnet. Account-meta order + flags must
match the Rust contexts (set_quote.rs, remove_quote.rs, open_or_request.rs, deactivate.rs,
close_legacy_quote.rs, vote_set_attestation.rs, vote_attest_heartbeat.rs, admin.rs).
"""

import hashlib

import pytest
from solders.keypair import Keypair

from allways.solana import layouts, pdas
from allways.solana.client import SYSTEM_PROGRAM, AllwaysSolanaClient
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


def _ix(c):
    return c._cap['ixs'][0]


def _body(ix):
    return ix.data[8:]


def _metas(ix):
    return [(m.pubkey, m.is_signer, m.is_writable) for m in ix.accounts]


# --- discriminators independently recomputed --------------------------------------------------


@pytest.mark.parametrize(
    'name',
    [
        'set_quote',
        'remove_quote',
        'open_or_request',
        'deactivate',
        'close_legacy_quote',
        'close_legacy_pool',
        'close_legacy_reservation',
        'vote_set_attestation',
        'vote_attest_heartbeat',
        'set_tao_min_swap_amount',
        'set_tao_max_swap_amount',
        'set_tao_min_collateral',
        'set_settlement_grace',
        'set_attest_max_age',
        'migrate_config',
        'migrate_miner_state',
    ],
)
def test_discriminators_match_anchors_formula(name):
    assert layouts.IX_DISCRIMINATORS[name] == _global_disc(name)


# --- the backing rides in the seeds ------------------------------------------------------------


def test_quote_pda_separates_two_backings_on_one_direction():
    miner = Keypair().pubkey()
    sol_backed = pdas.quote_pda(miner, 'sol', 'tao', 'sol', PID)
    tao_backed = pdas.quote_pda(miner, 'sol', 'tao', 'tao', PID)
    assert sol_backed != tao_backed, 'D2: one miner, one direction, two live offers'
    # And neither collides with the pre-W2b address the reaper targets.
    assert pdas.legacy_quote_pda(miner, 'sol', 'tao', PID) not in (sol_backed, tao_backed)


def test_set_quote_names_the_miner_state_and_the_backed_quote(client):
    client.set_quote('sol', 'tao', 'So1src', '5dst', 10**18, 0, backing='tao')
    ix = _ix(client)
    miner = client.keypair.pubkey()
    assert ix.data[:8] == layouts.IX_DISCRIMINATORS['set_quote']
    assert _body(ix) == layouts.IX_SET_QUOTE_ARGS.build(
        {
            'from_chain': 'sol',
            'to_chain': 'tao',
            'collateral_chain': 'tao',
            'miner_from_addr': 'So1src',
            'miner_to_addr': '5dst',
            'rate': 10**18,
            'liquidity': 0,
        }
    )
    assert _metas(ix) == [
        (miner, True, True),
        # miner_state is read-only: set_quote checks the activation bit, it never writes one.
        (pdas.miner_state_pda(miner, PID), False, False),
        (pdas.quote_pda(miner, 'sol', 'tao', 'tao', PID), False, True),
        (pdas.treasury_pda(PID), False, True),
        (SYSTEM_PROGRAM, False, False),
    ]


def test_open_or_request_carries_the_attestation_for_a_non_sol_backing(client):
    miner = Keypair().pubkey()
    client.open_or_request(miner, 'sol', 'tao', backing='tao')
    ix = _ix(client)
    assert _body(ix) == layouts.IX_OPEN_OR_REQUEST_ARGS.build(
        {'from_chain': 'sol', 'to_chain': 'tao', 'collateral_chain': 'tao'}
    )
    metas = _metas(ix)
    assert metas[4] == (pdas.quote_pda(miner, 'sol', 'tao', 'tao', PID), False, False)
    assert metas[5] == (pdas.bond_attestation_pda(miner, 'tao', PID), False, False)


def test_open_or_request_passes_the_program_id_for_sol(client):
    # Anchor encodes an absent optional account as the program id — "sol" reads the local vault.
    miner = Keypair().pubkey()
    client.open_or_request(miner, 'btc', 'sol')
    assert _metas(_ix(client))[5] == (PID, False, False)


def test_remove_quote_targets_one_backing(client):
    client.remove_quote('sol', 'tao', backing='sol')
    ix = _ix(client)
    miner = client.keypair.pubkey()
    assert _metas(ix)[1] == (pdas.quote_pda(miner, 'sol', 'tao', 'sol', PID), False, True)


# --- partial exit -------------------------------------------------------------------------------


def test_deactivate_tags_the_optional_backing(client):
    client.deactivate()
    assert _body(_ix(client)) == b'\x00', 'None is a single borsh tag byte'
    client.deactivate(backing='tao')
    assert _body(_ix(client)) == layouts.IX_OPT_BACKING_ARGS.build('tao')


# --- the legacy reaper --------------------------------------------------------------------------


def test_close_legacy_quote_targets_the_old_derivation_and_pays_the_miner(client):
    miner = Keypair().pubkey()
    client.close_legacy_quote(miner, 'btc', 'sol')
    ix = _ix(client)
    assert _body(ix) == b'', 'no args — the account proves its own orphanhood'
    assert _metas(ix) == [
        (client.keypair.pubkey(), True, False),  # caller only pays the tx
        (miner, False, True),  # rent goes back to the miner that paid it
        (pdas.legacy_quote_pda(miner, 'btc', 'sol', PID), False, True),
    ]


def test_close_legacy_slots_target_the_retired_addresses_and_pay_the_miner(client):
    # v3.1 moved the live slots to backing-qualified seeds, so the closers name the RETIRED
    # [seed, miner] addresses — the derivation itself is the legacy proof now, and a live slot is
    # structurally out of reach at a different address entirely.
    miner = Keypair().pubkey()
    client.close_legacy_pool(miner)
    ix = _ix(client)
    assert _body(ix) == b'', 'no args — the retired address proves its own vintage'
    assert _metas(ix) == [
        (client.keypair.pubkey(), True, False),
        (miner, False, True),
        (pdas.legacy_pool_pda(miner, PID), False, True),
    ]
    assert pdas.legacy_pool_pda(miner, PID) != pdas.pool_pda(miner, 'sol', PID)

    client.close_legacy_reservation(miner)
    ix = _ix(client)
    assert _body(ix) == b''
    assert _metas(ix) == [
        (client.keypair.pubkey(), True, False),
        (miner, False, True),
        (pdas.legacy_reservation_pda(miner, PID), False, True),
    ]

    # The retired per-miner initiate round: rent refunds the CALLING validator (it funded rounds).
    client.close_legacy_initiate_round(miner)
    ix = _ix(client)
    assert _body(ix) == b''
    assert _metas(ix) == [
        (client.keypair.pubkey(), True, True),
        (miner, False, False),
        (pdas.legacy_initiate_round_pda(miner, PID), False, True),
    ]


# --- attestation writers (W3 will drive these; tests + ops use them now) -------------------------


def test_vote_set_attestation_builds_the_round_and_the_pda(client):
    miner = Keypair().pubkey()
    client.vote_set_attestation(miner, 'tao', 5 * 10**9, True, 7)
    ix = _ix(client)
    assert _body(ix) == layouts.IX_SET_ATTESTATION_ARGS.build(
        {'chain': 'tao', 'effective_balance': 5 * 10**9, 'locked': True, 'epoch': 7}
    )
    assert _metas(ix) == [
        (client.keypair.pubkey(), True, True),
        (pdas.config_pda(PID), False, False),
        (miner, False, False),
        (pdas.miner_state_pda(miner, PID), False, True),  # writable: under-floor bond drops the hub bit
        (pdas.reservation_pda(miner, 'tao', PID), False, False),  # F5: filled-reservation obligation floor
        (pdas.bond_attestation_pda(miner, 'tao', PID), False, True),
        (pdas.attestation_round_pda(miner, 'tao', PID), False, True),
        (SYSTEM_PROGRAM, False, False),
    ]


def test_vote_attest_heartbeat_is_a_global_round_keyed_by_config(client):
    client.vote_attest_heartbeat()
    ix = _ix(client)
    assert _body(ix) == b''
    assert _metas(ix) == [
        (client.keypair.pubkey(), True, True),
        (pdas.config_pda(PID), False, True),  # writable: quorum advances last_attest_heartbeat
        (pdas.vote_round_pda(pdas.REQ_ATTEST_HEARTBEAT, pdas.config_pda(PID), PID), False, True),
        (SYSTEM_PROGRAM, False, False),
    ]


# --- split-collateral setters -------------------------------------------------------------------


@pytest.mark.parametrize(
    'method, name, arg, layout',
    [
        ('set_tao_min_swap_amount', 'set_tao_min_swap_amount', 500_000_000, 'IX_AMOUNT_ARGS'),
        ('set_tao_max_swap_amount', 'set_tao_max_swap_amount', 0, 'IX_AMOUNT_ARGS'),
        ('set_tao_min_collateral', 'set_tao_min_collateral', 5_000_000_000, 'IX_AMOUNT_ARGS'),
        ('set_settlement_grace', 'set_settlement_grace', 900, 'IX_I64_ARGS'),
        ('set_attest_max_age', 'set_attest_max_age', 86_400, 'IX_I64_ARGS'),
    ],
)
def test_split_collateral_setters(client, method, name, arg, layout):
    getattr(client, method)(arg)
    ix = _ix(client)
    admin = client.keypair.pubkey()
    assert ix.data[:8] == layouts.IX_DISCRIMINATORS[name]
    key = 'amount' if layout == 'IX_AMOUNT_ARGS' else 'value'
    assert _body(ix) == getattr(layouts, layout).build({key: arg})
    assert _metas(ix) == [(admin, True, False), (pdas.config_pda(PID), False, True)]
