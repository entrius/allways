"""B4.0 — unit tests for the miner/admin instruction builders added in B4 + the `SolanaSwap` adapter.

Same approach as test_solana_vote_builders: no chain — `_send` is stubbed to capture the assembled
Instruction, and each discriminator is independently recomputed via Anchor's sha256("global:<name>")
formula (so a wrong IDL copy is caught here, not on localnet). Account-meta order + signer/writable flags
must match the Rust contexts (instructions/admin.rs, remove_quote.rs, deactivate.rs, withdraw_treasury.rs).
"""

import hashlib
import types

import pytest
from solders.keypair import Keypair

from allways.solana import layouts, pdas
from allways.solana.client import SYSTEM_PROGRAM, AllwaysSolanaClient, swap_from_solana, swap_key_from_tx_hash
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


def _body(ix):
    return bytes(ix.data[8:])


def test_b4_discriminators_match_anchor_global_formula():
    for name in (
        'deactivate',
        'remove_quote',
        'remove_validator',
        'set_consensus_threshold',
        'set_fulfillment_timeout',
        'set_halted',
        'set_min_collateral',
        'set_max_collateral',
        'set_min_swap_amount',
        'set_max_swap_amount',
        'set_reservation_ttl',
        'withdraw_treasury',
    ):
        assert layouts.IX_DISCRIMINATORS[name] == _global_disc(name), f'{name} ix discriminator mismatch'


def test_remove_quote(client):
    client.remove_quote('btc', 'tao')
    ix = _ix(client)
    miner = client.keypair.pubkey()
    assert ix.data[:8] == layouts.IX_DISCRIMINATORS['remove_quote']
    assert _body(ix) == layouts.IX_REMOVE_QUOTE_ARGS.build({'from_chain': 'btc', 'to_chain': 'tao'})
    assert _metas(ix) == [
        (miner, True, True),
        (pdas.quote_pda(miner, 'btc', 'tao', PID), False, True),
        (pdas.treasury_pda(PID), False, True),
        (SYSTEM_PROGRAM, False, False),
    ]


def test_deactivate(client):
    client.deactivate()
    ix = _ix(client)
    miner = client.keypair.pubkey()
    assert ix.data[:8] == layouts.IX_DISCRIMINATORS['deactivate']
    assert _body(ix) == b''  # no args
    assert _metas(ix) == [
        (miner, True, False),
        (pdas.miner_state_pda(miner, PID), False, True),
    ]


def test_withdraw_treasury(client):
    recipient = Keypair().pubkey()
    client.withdraw_treasury(recipient, 5_000)
    ix = _ix(client)
    admin = client.keypair.pubkey()
    assert ix.data[:8] == layouts.IX_DISCRIMINATORS['withdraw_treasury']
    assert _body(ix) == layouts.IX_AMOUNT_ARGS.build({'amount': 5_000})
    assert _metas(ix) == [
        (admin, True, False),
        (pdas.config_pda(PID), False, False),
        (pdas.treasury_pda(PID), False, True),
        (recipient, False, True),
    ]


@pytest.mark.parametrize(
    'call, ix_name, body',
    [
        (lambda c: c.set_halted(True), 'set_halted', layouts.IX_BOOL_ARGS.build({'value': True})),
        (lambda c: c.set_consensus_threshold(67), 'set_consensus_threshold', layouts.IX_U8_ARGS.build({'value': 67})),
        (
            lambda c: c.set_fulfillment_timeout(3600),
            'set_fulfillment_timeout',
            layouts.IX_I64_ARGS.build({'value': 3600}),
        ),
        (lambda c: c.set_min_collateral(1_000), 'set_min_collateral', layouts.IX_AMOUNT_ARGS.build({'amount': 1_000})),
        (lambda c: c.set_max_collateral(9_000), 'set_max_collateral', layouts.IX_AMOUNT_ARGS.build({'amount': 9_000})),
        (lambda c: c.set_min_swap_amount(10), 'set_min_swap_amount', layouts.IX_AMOUNT_ARGS.build({'amount': 10})),
        (lambda c: c.set_max_swap_amount(99), 'set_max_swap_amount', layouts.IX_AMOUNT_ARGS.build({'amount': 99})),
        (lambda c: c.set_reservation_ttl(600), 'set_reservation_ttl', layouts.IX_I64_ARGS.build({'value': 600})),
    ],
)
def test_admin_config_setters(client, call, ix_name, body):
    call(client)
    ix = _ix(client)
    admin = client.keypair.pubkey()
    assert ix.data[:8] == layouts.IX_DISCRIMINATORS[ix_name]
    assert _body(ix) == body
    assert _metas(ix) == [
        (admin, True, False),
        (pdas.config_pda(PID), False, True),
    ]


def test_remove_validator(client):
    v = Keypair().pubkey()
    client.remove_validator(v)
    ix = _ix(client)
    admin = client.keypair.pubkey()
    assert ix.data[:8] == layouts.IX_DISCRIMINATORS['remove_validator']
    assert _body(ix) == layouts.IX_PUBKEY_ARGS.build({'value': bytes(v)})
    assert _metas(ix) == [
        (admin, True, False),
        (pdas.config_pda(PID), False, True),
    ]


def _fake_swap_account(**over):
    """A stand-in for a decoded `Swap` account (attribute access, like borsh_construct's Container)."""
    status = over.pop('status', types.new_class('Active')())
    fields = dict(
        user=bytes(range(32)),
        miner=bytes(range(32, 64)),
        from_chain='btc',
        to_chain='tao',
        user_from_addr='bc1quser',
        user_to_addr='5Fuser',
        miner_from_addr='bc1qminer',
        miner_to_addr='5Fminer',
        rate=12345,
        sol_amount=1_000_000,
        from_amount=500,
        to_amount=600,
        from_tx_hash='deadbeef',
        from_tx_block=111,
        to_tx_hash='',
        to_tx_block=0,
        initiated_at=1000,
        timeout_at=4600,
        max_extend_at=8000,
        fulfilled_at=0,
    )
    fields.update(over)
    fields['status'] = status
    return types.SimpleNamespace(**fields)


def test_swap_from_solana_maps_fields_and_derives_key():
    acct = _fake_swap_account()
    sw = swap_from_solana(acct)
    assert sw.swap_key == swap_key_from_tx_hash('deadbeef')
    assert sw.key_hex == swap_key_from_tx_hash('deadbeef').hex()
    assert sw.from_chain == 'btc' and sw.to_chain == 'tao'
    assert sw.to_amount == 600 and sw.from_amount == 500 and sw.rate == 12345
    assert sw.timeout_at == 4600  # unix axis, not a block
    assert str(sw.miner) == str(pdas.Pubkey.from_bytes(bytes(range(32, 64))))
    assert sw.status == 'Active'


def test_swap_from_solana_status_name_and_explicit_key():
    acct = _fake_swap_account(status=types.new_class('Fulfilled')())
    explicit = bytes([9] * 32)
    sw = swap_from_solana(acct, swap_key=explicit)
    assert sw.swap_key == explicit  # not derived when supplied
    assert sw.status == 'Fulfilled'
