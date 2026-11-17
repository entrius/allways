"""The program address resolves from one place, with env > CLI config > committed default.

These lock the config → client wiring for the program id and RPC, and the env-over-config
precedence its sibling resolvers use.
"""

import os
from unittest.mock import patch

import pytest
from solders.keypair import Keypair
from solders.pubkey import Pubkey

from allways.cli.swap_commands import helpers
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


def test_invalid_env_raises():
    """Silently falling back would point a mainnet node at the devnet program."""
    with patch.dict(os.environ, {ENV_VAR: 'not-a-valid-pubkey'}, clear=True):
        with pytest.raises(ValueError, match=ENV_VAR):
            resolve_program_id({})


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


def test_cli_env_overrides_config():
    custom = str(Keypair().pubkey())
    _, client = _ctx({'program-id': str(Keypair().pubkey())}, env={ENV_VAR: custom})
    assert str(client.program_id) == custom


def test_env_rpc_overrides_config():
    _, client = _ctx({'solana-rpc': 'http://config.test:1111'}, env={'SOLANA_RPC_URL': 'http://env.test:9999'})
    assert client.rpc.url == 'http://env.test:9999'
