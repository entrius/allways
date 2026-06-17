from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from allways.classes import Swap, SwapStatus
from allways.cli.swap_commands import claim as claim_module
from allways.cli.swap_commands.claim import claim_command


def _make_swap(user_hotkey: str) -> Swap:
    return Swap(
        id=42,
        user_hotkey=user_hotkey,
        miner_hotkey='miner-hotkey',
        from_chain='tao',
        to_chain='btc',
        from_amount=1,
        to_amount=1,
        tao_amount=1,
        user_from_address='user-from',
        user_to_address='user-to',
        status=SwapStatus.TIMED_OUT,
    )


def _invoke(wallet, client):
    config = {'network': 'test'}
    with (
        patch.object(claim_module, 'get_cli_context', return_value=(config, wallet, None, client)),
        patch.object(claim_module, 'loading', return_value=nullcontext()),
    ):
        return CliRunner().invoke(claim_command, ['42', '--yes'], catch_exceptions=False)


class TestClaimCommandSignerSelection:
    def test_claim_uses_coldkey_when_swap_user_matches_coldkey(self):
        coldkey_signer = object()
        wallet = SimpleNamespace(
            hotkey=SimpleNamespace(ss58_address='hot-addr'),
            coldkeypub=SimpleNamespace(ss58_address='cold-addr'),
            coldkey=coldkey_signer,
        )
        client = MagicMock()
        client.get_pending_slash.return_value = 123_000_000
        client.get_swap.return_value = _make_swap(user_hotkey='cold-addr')

        result = _invoke(wallet, client)

        assert result.exit_code == 0, result.output
        client.claim_slash.assert_called_once_with(wallet=wallet, swap_id=42, keypair=coldkey_signer)
        assert 'Claiming:   cold-addr' in result.output

    def test_claim_refuses_when_wallet_cannot_sign_for_swap_user(self):
        wallet = SimpleNamespace(
            hotkey=SimpleNamespace(ss58_address='hot-addr'),
            coldkeypub=SimpleNamespace(ss58_address='cold-addr'),
            coldkey=object(),
        )
        client = MagicMock()
        client.get_pending_slash.return_value = 123_000_000
        client.get_swap.return_value = _make_swap(user_hotkey='external-user')

        result = _invoke(wallet, client)

        assert result.exit_code == 0, result.output
        client.claim_slash.assert_not_called()
        assert 'This wallet cannot claim this slash.' in result.output
