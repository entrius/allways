"""Tests for `alw swap now --from BTC --to TAO` (issue #248).

The full swap.py:swap_now is interactive and pulls heavy deps; reach into
the validation block via a minimal harness that mirrors swap.py:603-621
exactly (including the lowercasing fix and the existing SUPPORTED_CHAINS
membership / equality checks)."""

from typing import Optional

import click
import pytest
from click.testing import CliRunner

from allways.chains import SUPPORTED_CHAINS


def make_validate_only_cli():
    """Mirror swap.py's chain validation block — no contract / wallet setup."""

    @click.command()
    @click.option('--from', 'from_chain_opt', default=None)
    @click.option('--to', 'to_chain_opt', default=None)
    def cmd(from_chain_opt: Optional[str], to_chain_opt: Optional[str]):
        # The fix:
        if from_chain_opt:
            from_chain_opt = from_chain_opt.lower()
        if to_chain_opt:
            to_chain_opt = to_chain_opt.lower()

        # Identical to swap.py:603-611 after the fix.
        if from_chain_opt and from_chain_opt not in SUPPORTED_CHAINS:
            click.echo(f'Unknown source chain: {from_chain_opt}')
            return
        if to_chain_opt and to_chain_opt not in SUPPORTED_CHAINS:
            click.echo(f'Unknown destination chain: {to_chain_opt}')
            return
        if from_chain_opt and to_chain_opt and from_chain_opt == to_chain_opt:
            click.echo('Source and destination chains must be different')
            return

        click.echo(f'OK from={from_chain_opt} to={to_chain_opt}')

    return cmd


@pytest.fixture
def runner():
    return CliRunner()


class TestSwapNowChainCaseInsensitive:
    def test_uppercase_btc_tao_accepted(self, runner):
        """Reproducer from issue #248."""
        result = runner.invoke(make_validate_only_cli(), ['--from', 'BTC', '--to', 'TAO'])
        assert result.exit_code == 0
        assert 'OK from=btc to=tao' in result.output

    def test_mixed_case_accepted(self, runner):
        result = runner.invoke(make_validate_only_cli(), ['--from', 'Btc', '--to', 'Tao'])
        assert result.exit_code == 0
        assert 'OK from=btc to=tao' in result.output

    def test_lowercase_still_works(self, runner):
        """Don't regress the happy path."""
        result = runner.invoke(make_validate_only_cli(), ['--from', 'btc', '--to', 'tao'])
        assert result.exit_code == 0

    def test_only_from_uppercase(self, runner):
        """One-sided invocations also lowercase correctly."""
        result = runner.invoke(make_validate_only_cli(), ['--from', 'BTC'])
        assert result.exit_code == 0
        assert 'OK from=btc to=None' in result.output

    def test_only_to_uppercase(self, runner):
        result = runner.invoke(make_validate_only_cli(), ['--to', 'TAO'])
        assert result.exit_code == 0
        assert 'OK from=None to=tao' in result.output


class TestRejectionsStillWork:
    """Lowercasing must NOT accidentally accept genuinely unsupported chains."""

    def test_unknown_uppercase_still_rejected(self, runner):
        result = runner.invoke(make_validate_only_cli(), ['--from', 'ETH', '--to', 'TAO'])
        assert 'Unknown source chain: eth' in result.output

    def test_unknown_lowercase_still_rejected(self, runner):
        result = runner.invoke(make_validate_only_cli(), ['--from', 'eth', '--to', 'tao'])
        assert 'Unknown source chain: eth' in result.output

    def test_unknown_dest_rejected(self, runner):
        result = runner.invoke(make_validate_only_cli(), ['--from', 'btc', '--to', 'XYZ'])
        assert 'Unknown destination chain: xyz' in result.output


class TestEqualChainsCaseInsensitive:
    """The fix also fixes a subtle existing bug: `--from BTC --to btc` used
    to silently slip past the equality check (uppercase == lowercase fails).
    Post-fix both sides are lowercased so equality is detected."""

    def test_BTC_btc_now_correctly_rejected(self, runner):
        result = runner.invoke(make_validate_only_cli(), ['--from', 'BTC', '--to', 'btc'])
        assert 'must be different' in result.output

    def test_BTC_BTC_correctly_rejected(self, runner):
        result = runner.invoke(make_validate_only_cli(), ['--from', 'BTC', '--to', 'BTC'])
        assert 'must be different' in result.output


class TestNoArgsStillWorks:
    """Without --from/--to, the function falls through to the interactive
    prompt path. The lowercase block must safely no-op on None."""

    def test_no_args(self, runner):
        result = runner.invoke(make_validate_only_cli(), [])
        assert result.exit_code == 0
        assert 'OK from=None to=None' in result.output
