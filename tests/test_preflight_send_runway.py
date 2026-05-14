"""Tests for ``preflight_send_runway`` — the CLI gate that refuses to broadcast
source funds into a reservation that's already too short for confirmations to
land (with or without the validator auto-extension flow).

The classifier itself is unit-tested in test_chains.py; these tests cover the
wiring: re-reading current_block at send time, branch routing
(too_short / extension_required / ok), and the skip_confirm contract.
"""

from unittest.mock import MagicMock, patch

from allways.cli.preflight import preflight_send_runway


def make_subtensor(current_block: int) -> MagicMock:
    """Mock subtensor whose ``get_current_block`` returns the given height."""
    sub = MagicMock()
    sub.get_current_block.return_value = current_block
    return sub


class TestPreflightSendRunway:
    def test_ok_runway_passes_silently(self):
        # TAO needs ~6 subtensor blocks for confirmation; 200 blocks left is
        # comfortably in the OK band — should return True without prompting.
        sub = make_subtensor(current_block=1000)
        with patch('allways.cli.preflight.click.confirm') as confirm:
            result = preflight_send_runway(
                subtensor=sub,
                from_chain='tao',
                reserved_until_block=1200,
                skip_confirm=False,
            )
        assert result is True
        confirm.assert_not_called()

    def test_too_short_hard_refuses_without_prompt(self):
        # Reservation only 5 blocks out — well below the extension floor.
        # Must return False AND must not have asked the user anything.
        sub = make_subtensor(current_block=1000)
        with patch('allways.cli.preflight.click.confirm') as confirm:
            result = preflight_send_runway(
                subtensor=sub,
                from_chain='btc',
                reserved_until_block=1005,
                skip_confirm=False,
            )
        assert result is False
        confirm.assert_not_called()

    def test_too_short_refuses_in_skip_confirm_mode_too(self):
        # --yes must not silently override a doomed broadcast.
        sub = make_subtensor(current_block=1000)
        result = preflight_send_runway(
            subtensor=sub,
            from_chain='btc',
            reserved_until_block=1005,
            skip_confirm=True,
        )
        assert result is False

    def test_extension_required_prompts_user_in_interactive_mode(self):
        # BTC needs ~100 subtensor blocks for confirmation. 40 blocks left is
        # above the extension floor but below the confirmation window — should
        # warn and ask. Confirming yes returns True.
        sub = make_subtensor(current_block=1000)
        with patch(
            'allways.cli.preflight.click.confirm', return_value=True
        ) as confirm:
            result = preflight_send_runway(
                subtensor=sub,
                from_chain='btc',
                reserved_until_block=1040,
                skip_confirm=False,
            )
        assert result is True
        confirm.assert_called_once()

    def test_extension_required_user_declines(self):
        # Same band, user says no — must abort.
        sub = make_subtensor(current_block=1000)
        with patch(
            'allways.cli.preflight.click.confirm', return_value=False
        ) as confirm:
            result = preflight_send_runway(
                subtensor=sub,
                from_chain='btc',
                reserved_until_block=1040,
                skip_confirm=False,
            )
        assert result is False
        confirm.assert_called_once()

    def test_extension_required_skip_confirm_passes_through(self):
        # In --yes mode the extension-required band must not block — the
        # validator auto-extension can still rescue this and scripted callers
        # need a deterministic exit, not a hung prompt.
        sub = make_subtensor(current_block=1000)
        with patch('allways.cli.preflight.click.confirm') as confirm:
            result = preflight_send_runway(
                subtensor=sub,
                from_chain='btc',
                reserved_until_block=1040,
                skip_confirm=True,
            )
        assert result is True
        confirm.assert_not_called()

    def test_reads_current_block_at_send_time(self):
        # The user may idle at the summary panel — the gate must re-read
        # current_block instead of trusting whatever was captured at reserve
        # time. Verifies subtensor is actually queried.
        sub = make_subtensor(current_block=1000)
        preflight_send_runway(
            subtensor=sub,
            from_chain='tao',
            reserved_until_block=1200,
            skip_confirm=True,
        )
        sub.get_current_block.assert_called_once()
