"""The hand-written borsh layouts must match the packaged IDL field for field — a drifted order or a
scalar left where the program now writes an array silently mis-decodes every account after it."""

import json

import pytest

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


def test_the_per_hub_config_arrays_are_as_wide_as_the_program_writes():
    widths = {f['name']: f['type'] for f in TYPES['Config']['fields']}
    for field in ('last_attest_heartbeat', 'attest_max_age_secs', 'vault_generation'):
        assert widths[field]['array'][1] == layouts.MAX_BACKING_SLOTS


@pytest.mark.parametrize(
    'ix, args',
    [
        ('vote_attest_heartbeat', ['hub']),
        ('bump_vault_generation', ['hub']),
        ('set_attest_max_age', ['hub', 'secs']),
        ('bind_evm_address', ['evm_address']),
    ],
)
def test_per_hub_instruction_args_match_the_idl(ix, args):
    assert [a['name'] for a in next(i for i in IDL['instructions'] if i['name'] == ix)['args']] == args
