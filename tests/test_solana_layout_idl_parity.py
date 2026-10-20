"""The hand-written borsh layouts must match the packaged IDL field for field — a drifted order or a
scalar left where the program now writes an array silently mis-decodes every account after it."""

import json

import pytest
from borsh_construct import CStruct

from allways.metadata import METADATA_DIR
from allways.solana import layouts

IDL = json.loads((METADATA_DIR / 'allways_swap_manager.json').read_text())
TYPES = {t['name']: t['type'] for t in IDL['types']}


def _fields(cstruct):
    return [sc.name for sc in cstruct.subcons]


def _idl_fields(name):
    return [f['name'] for f in TYPES[name]['fields']]


@pytest.mark.parametrize('name', sorted(layouts.ACCOUNT_LAYOUTS))
def test_account_layout_matches_the_idl(name):
    assert _fields(layouts.ACCOUNT_LAYOUTS[name]) == _idl_fields(name)
    disc = next(a['discriminator'] for a in IDL['accounts'] if a['name'] == name)
    assert layouts.DISCRIMINATORS[name] == bytes(disc)


@pytest.mark.parametrize('name', sorted(layouts.EVENT_LAYOUTS))
def test_event_layout_matches_the_idl(name):
    assert _fields(layouts.EVENT_LAYOUTS[name]) == _idl_fields(name)
    disc = next(e['discriminator'] for e in IDL['events'] if e['name'] == name)
    assert layouts.EVENT_DISCRIMINATORS[name] == bytes(disc)


@pytest.mark.parametrize('name', sorted(layouts.IX_DISCRIMINATORS))
def test_instruction_discriminator_matches_the_idl(name):
    disc = next(i['discriminator'] for i in IDL['instructions'] if i['name'] == name)
    assert layouts.IX_DISCRIMINATORS[name] == bytes(disc)


# Config as the v16 program wrote it — what every un-upgraded client still decodes with.
_CONFIG_V16 = CStruct(
    *layouts.Config.subcons[: [sc.name for sc in layouts.Config.subcons].index('vault_generation') + 1]
)
V16_FIELDS = [
    'admin', 'version', 'min_collateral', 'max_collateral', 'fulfillment_timeout_secs', 'min_swap_amount',
    'max_swap_amount', 'tao_min_swap_amount', 'tao_max_swap_amount', 'tao_min_collateral',
    'settlement_grace_secs', 'last_attest_heartbeat', 'attest_max_age_secs', 'reservation_ttl_secs',
    'consensus_threshold_percent', 'validators', 'last_weights_update', 'halted', 'reservation_fee_lamports',
    'pool_window_secs', 'finalize_window_secs', 'weights_update_min_interval_secs', 'max_total_extension_secs',
    'bump', 'vault_generation',
]  # fmt: skip


def test_config_v17_only_appends_after_the_v16_fields():
    assert _idl_fields('Config')[: len(V16_FIELDS)] == V16_FIELDS
    assert _fields(_CONFIG_V16) == V16_FIELDS


def test_an_un_upgraded_client_still_decodes_every_old_field_of_a_v17_config():
    v17 = {
        'admin': bytes(range(32)),
        'version': 17,
        'min_collateral': 1_000_000_000,
        'max_collateral': 0,
        'fulfillment_timeout_secs': 600,
        'min_swap_amount': 100_000_000,
        'max_swap_amount': 5_000_000_000,
        'tao_min_swap_amount': 100_000_000,
        'tao_max_swap_amount': 1_000_000_000,
        'tao_min_collateral': 250_000_000,
        'settlement_grace_secs': 900,
        'last_attest_heartbeat': 1_760_000_000,
        'attest_max_age_secs': 86_400,
        'reservation_ttl_secs': 600,
        'consensus_threshold_percent': 66,
        'validators': [{'key': bytes([i] * 32), 'weight': i} for i in range(1, 4)],
        'last_weights_update': 1_759_000_000,
        'halted': True,
        'reservation_fee_lamports': 20_000_000,
        'pool_window_secs': 5,
        'finalize_window_secs': 60,
        'weights_update_min_interval_secs': 1_200,
        'max_total_extension_secs': 8_400,
        'bump': 254,
        'vault_generation': 3,
        'usdc_min_swap_amount': 10_000_000,
        'usdc_max_swap_amount': 2_500_000_000,
        'usdc_min_collateral': 25_000_000,
        'usdc_last_attest_heartbeat': 1_760_000_100,
        'usdc_attest_max_age_secs': 43_200,
        'usdc_vault_generation': 1,
    }
    # Zero-padded like the real allocation; construct's parse ignores the trailing bytes it doesn't name.
    raw = layouts.Config.build(v17) + bytes(512)
    old = _CONFIG_V16.parse(raw)
    for name in V16_FIELDS:
        got = getattr(old, name)
        if name == 'validators':
            got = [{'key': bytes(v.key), 'weight': v.weight} for v in got]
        elif name == 'admin':
            got = bytes(got)
        assert got == v17[name], name


@pytest.mark.parametrize(
    'ix, args',
    [
        ('vote_attest_heartbeat', ['hub']),
        ('bump_vault_generation', ['hub']),
        ('set_attest_max_age', ['hub', 'secs']),
        ('bind_evm_address', ['evm_address', 'evm_signature']),
    ],
)
def test_per_hub_instruction_args_match_the_idl(ix, args):
    assert [a['name'] for a in next(i for i in IDL['instructions'] if i['name'] == ix)['args']] == args
