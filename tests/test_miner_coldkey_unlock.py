"""Miner.unlock_coldkey: headless startup across encrypted and plaintext keyfiles.

A miner created without a coldkey password has nothing to prompt for; prompting
anyway kills a detached neuron with EOFError before it ever polls a swap.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from neurons.miner import Miner


def _wallet(encrypted: bool) -> SimpleNamespace:
    return SimpleNamespace(
        name='miner',
        coldkey_file=SimpleNamespace(is_encrypted=lambda: encrypted, save_password_to_env=MagicMock()),
        unlock_coldkey=MagicMock(),
    )


def _no_prompt(monkeypatch):
    """Make any getpass() call an error — a headless miner has no stdin to read."""
    import getpass as getpass_module

    def _boom(_prompt=''):
        raise EOFError('getpass called with no tty')

    monkeypatch.setattr(getpass_module, 'getpass', _boom)


def test_plaintext_keyfile_unlocks_without_prompting(monkeypatch):
    monkeypatch.delenv('MINER_BITTENSOR_COLDKEY_PASSWORD', raising=False)
    _no_prompt(monkeypatch)
    wallet = _wallet(encrypted=False)

    Miner.unlock_coldkey(SimpleNamespace(wallet=wallet))

    wallet.unlock_coldkey.assert_called_once()
    # Nothing to cache: a password env var written for a plaintext keyfile is noise.
    wallet.coldkey_file.save_password_to_env.assert_not_called()


def test_encrypted_keyfile_caches_password_from_env(monkeypatch):
    monkeypatch.setenv('MINER_BITTENSOR_COLDKEY_PASSWORD', 'hunter2')
    _no_prompt(monkeypatch)
    wallet = _wallet(encrypted=True)

    Miner.unlock_coldkey(SimpleNamespace(wallet=wallet))

    wallet.coldkey_file.save_password_to_env.assert_called_once_with('hunter2')
    wallet.unlock_coldkey.assert_called_once()


def test_encrypted_keyfile_without_env_still_prompts(monkeypatch):
    """The guard must not swallow the real prompt path — an encrypted keyfile
    with no env var is exactly when a human is expected to type one in."""
    monkeypatch.delenv('MINER_BITTENSOR_COLDKEY_PASSWORD', raising=False)
    _no_prompt(monkeypatch)

    with pytest.raises(EOFError):
        Miner.unlock_coldkey(SimpleNamespace(wallet=_wallet(encrypted=True)))
