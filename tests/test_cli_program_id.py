"""B5 — the CLI honors a configured Solana program-id / RPC instead of ignoring it.

`get_solana_cli_context` used to hardcode `pdas.PROGRAM_ID` and read the RPC from the env only, so
`alw config set program-id <addr>` (and the legacy `contract` key) silently had no effect. These lock the
config → client wiring.
"""

import os
from unittest.mock import patch

from solders.keypair import Keypair

from allways.cli.swap_commands import helpers
from allways.solana import pdas


def _ctx(config):
    """Call get_solana_cli_context with a fixed config and no real keypair/RPC."""
    with (
        patch.object(helpers, 'get_effective_config', return_value=config),
        patch('allways.solana.keys.load_or_create', return_value=Keypair()),
    ):
        return helpers.get_solana_cli_context()


def test_program_id_config_is_honored():
    custom = str(Keypair().pubkey())
    _, client = _ctx({'program-id': custom})
    assert str(client.program_id) == custom


def test_contract_alias_is_honored():
    custom = str(Keypair().pubkey())
    _, client = _ctx({'contract': custom})
    assert str(client.program_id) == custom


def test_program_id_key_wins_over_contract_alias():
    primary = str(Keypair().pubkey())
    _, client = _ctx({'program-id': primary, 'contract': str(Keypair().pubkey())})
    assert str(client.program_id) == primary


def test_defaults_to_pdas_program_id_when_absent():
    _, client = _ctx({})
    assert client.program_id == pdas.PROGRAM_ID


def test_invalid_program_id_falls_back_to_default():
    _, client = _ctx({'program-id': 'not-a-valid-pubkey'})
    assert client.program_id == pdas.PROGRAM_ID


def test_solana_rpc_config_used_when_env_unset():
    with patch.dict(os.environ, {}, clear=True):
        _, client = _ctx({'solana-rpc': 'http://example.test:1234'})
    assert client.rpc.url == 'http://example.test:1234'


def test_env_rpc_overrides_config():
    with patch.dict(os.environ, {'SOLANA_RPC_URL': 'http://env.test:9999'}, clear=True):
        _, client = _ctx({'solana-rpc': 'http://config.test:1111'})
    assert client.rpc.url == 'http://env.test:9999'
