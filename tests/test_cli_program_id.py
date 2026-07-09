"""The program address resolves from one place, with env > CLI config > committed default.

`get_solana_cli_context` used to hardcode `pdas.PROGRAM_ID` and read the RPC from the env only, so
`alw config set program-id <addr>` (and the legacy `contract` key) silently had no effect. Later it
let config override the env, inverting the precedence its sibling resolvers use. These lock the
config → client wiring and the precedence order.
"""

import os
from unittest.mock import patch

import pytest
from solders.keypair import Keypair
from solders.pubkey import Pubkey

from allways.cli.swap_commands import helpers
from allways.constants import PROGRAM_ID as DEFAULT_PROGRAM_ID
from allways.solana.program import ENV_VAR, resolve_program_id


def _ctx(config, env=None):
    """Call get_solana_cli_context with a fixed config and no real keypair/RPC."""
    with (
        patch.dict(os.environ, env or {}, clear=True),
        patch.object(helpers, 'get_effective_config', return_value=config),
        patch('allways.solana.keys.load_or_create', return_value=Keypair()),
    ):
        return helpers.get_solana_cli_context()


# ---------- resolver precedence ----------


def test_env_overrides_config():
    custom, ignored = str(Keypair().pubkey()), str(Keypair().pubkey())
    with patch.dict(os.environ, {ENV_VAR: custom}, clear=True):
        assert str(resolve_program_id({'program-id': ignored})) == custom


def test_config_used_when_env_unset():
    custom = str(Keypair().pubkey())
    with patch.dict(os.environ, {}, clear=True):
        assert str(resolve_program_id({'program-id': custom})) == custom


def test_defaults_when_env_and_config_unset():
    with patch.dict(os.environ, {}, clear=True):
        assert resolve_program_id({}) == Pubkey.from_string(DEFAULT_PROGRAM_ID)
        assert resolve_program_id(None) == Pubkey.from_string(DEFAULT_PROGRAM_ID)


def test_invalid_env_raises():
    """Silently falling back would point a mainnet node at the devnet program."""
    with patch.dict(os.environ, {ENV_VAR: 'not-a-valid-pubkey'}, clear=True):
        with pytest.raises(ValueError, match=ENV_VAR):
            resolve_program_id({})


def test_invalid_config_falls_back_to_default():
    with patch.dict(os.environ, {}, clear=True):
        assert resolve_program_id({'program-id': 'not-a-valid-pubkey'}) == Pubkey.from_string(DEFAULT_PROGRAM_ID)


def test_resolution_is_lazy_not_import_time():
    """pdas/client must not freeze the address at import — that is what broke .env in the neurons."""
    custom = str(Keypair().pubkey())
    with patch.dict(os.environ, {ENV_VAR: custom}, clear=True):
        from allways.solana import pdas

        assert pdas.config_pda() == Pubkey.find_program_address([b'config'], Pubkey.from_string(custom))[0]


# ---------- CLI wiring ----------


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


def test_cli_env_overrides_config():
    custom = str(Keypair().pubkey())
    _, client = _ctx({'program-id': str(Keypair().pubkey())}, env={ENV_VAR: custom})
    assert str(client.program_id) == custom


def test_defaults_to_committed_program_id_when_absent():
    _, client = _ctx({})
    assert client.program_id == Pubkey.from_string(DEFAULT_PROGRAM_ID)


def test_invalid_program_id_falls_back_to_default():
    _, client = _ctx({'program-id': 'not-a-valid-pubkey'})
    assert client.program_id == Pubkey.from_string(DEFAULT_PROGRAM_ID)


def test_solana_rpc_config_used_when_env_unset():
    _, client = _ctx({'solana-rpc': 'http://example.test:1234'})
    assert client.rpc.url == 'http://example.test:1234'


def test_env_rpc_overrides_config():
    _, client = _ctx({'solana-rpc': 'http://config.test:1111'}, env={'SOLANA_RPC_URL': 'http://env.test:9999'})
    assert client.rpc.url == 'http://env.test:9999'
