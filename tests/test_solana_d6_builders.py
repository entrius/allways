"""D6 — unit tests for the remaining admin-lever builders (set_reservation_fee/pool_window/
weights_update_min_interval/max_total_extension). Same approach as test_solana_b4_builders: no chain —
`_send` is stubbed, each discriminator is independently recomputed via sha256("global:<name>")[:8], and the
Context<AdminConfig> meta order (admin signer + config mut) must match instructions/admin.rs.
"""

import hashlib

import pytest
from solders.keypair import Keypair

from allways.solana import layouts, pdas
from allways.solana.client import AllwaysSolanaClient
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


def test_d6_discriminators_match_anchor_global_formula():
    for name in (
        'set_reservation_fee',
        'set_pool_window',
        'set_weights_update_min_interval',
        'set_max_total_extension',
    ):
        assert layouts.IX_DISCRIMINATORS[name] == _global_disc(name), f'{name} ix discriminator mismatch'


@pytest.mark.parametrize(
    'call, ix_name, body',
    [
        (
            lambda c: c.set_reservation_fee(5_000),
            'set_reservation_fee',
            layouts.IX_AMOUNT_ARGS.build({'amount': 5_000}),
        ),
        (lambda c: c.set_pool_window(60), 'set_pool_window', layouts.IX_I64_ARGS.build({'value': 60})),
        (
            lambda c: c.set_weights_update_min_interval(1_200),
            'set_weights_update_min_interval',
            layouts.IX_I64_ARGS.build({'value': 1_200}),
        ),
        (
            lambda c: c.set_max_total_extension(3_600),
            'set_max_total_extension',
            layouts.IX_I64_ARGS.build({'value': 3_600}),
        ),
    ],
)
def test_d6_admin_setters(client, call, ix_name, body):
    call(client)
    ixs = client._cap['ixs']
    assert len(ixs) == 1
    ix = ixs[0]
    admin = client.keypair.pubkey()
    assert ix.data[:8] == layouts.IX_DISCRIMINATORS[ix_name]
    assert bytes(ix.data[8:]) == body
    assert [(m.pubkey, m.is_signer, m.is_writable) for m in ix.accounts] == [
        (admin, True, False),
        (pdas.config_pda(PID), False, True),
    ]
