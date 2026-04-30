"""Tests for `alw view active-swaps --status completed|timed_out` (issue #246).

Verifies the dashboard hint short-circuits before touching the contract,
and that the queryable filters (active/fulfilled) still go through the
normal get_active_swaps() path."""

import pytest
from click.testing import CliRunner

from allways.cli.swap_commands.view import DEFAULT_DASHBOARD_URL, view_active_swaps


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture(autouse=True)
def isolate_dashboard_env(monkeypatch):
    monkeypatch.delenv('ALLWAYS_DASHBOARD_URL', raising=False)


class TestResolvedHintShortCircuit:
    def test_status_completed_prints_dashboard_hint_no_contract_call(self, runner, monkeypatch):
        """The bug: get_active_swaps was called only to filter to []. The
        fix: short-circuit to the dashboard hint before any RPC."""
        called = {'get_cli_context': False}

        def fail_if_called(*a, **kw):
            called['get_cli_context'] = True
            raise AssertionError('get_cli_context must NOT be called for completed/timed_out')

        monkeypatch.setattr('allways.cli.swap_commands.view.get_cli_context', fail_if_called)

        result = runner.invoke(view_active_swaps, ['--status', 'completed'])
        assert result.exit_code == 0, result.output
        assert called['get_cli_context'] is False
        assert 'pruned from on-chain storage' in result.output
        assert DEFAULT_DASHBOARD_URL in result.output
        # The misleading "No swaps found" must NOT appear.
        assert 'No swaps found' not in result.output

    def test_status_timed_out_prints_dashboard_hint(self, runner, monkeypatch):
        def fail_if_called(*a, **kw):
            raise AssertionError('get_cli_context must NOT be called')

        monkeypatch.setattr('allways.cli.swap_commands.view.get_cli_context', fail_if_called)

        result = runner.invoke(view_active_swaps, ['--status', 'timed_out'])
        assert result.exit_code == 0
        assert 'pruned from on-chain storage' in result.output
        assert 'No swaps found' not in result.output

    def test_uppercase_status_still_routes_to_dashboard(self, runner, monkeypatch):
        """case_sensitive=False on the Click choice — uppercase still hits."""

        def fail_if_called(*a, **kw):
            raise AssertionError('contract call leaked')

        monkeypatch.setattr('allways.cli.swap_commands.view.get_cli_context', fail_if_called)

        result = runner.invoke(view_active_swaps, ['--status', 'COMPLETED'])
        assert result.exit_code == 0
        assert 'pruned from on-chain storage' in result.output

    def test_dashboard_url_env_override_honored(self, runner, monkeypatch):
        monkeypatch.setattr('allways.cli.swap_commands.view.get_cli_context', lambda *a, **kw: None)
        monkeypatch.setenv('ALLWAYS_DASHBOARD_URL', 'https://my.custom.dash/')

        result = runner.invoke(view_active_swaps, ['--status', 'completed'])
        assert 'https://my.custom.dash' in result.output
        # Trailing slash should be stripped (matches view_swap behavior).
        assert 'https://my.custom.dash/' not in result.output.replace(
            'https://my.custom.dash/\n', 'https://my.custom.dash\n'
        )


class TestQueryableStatusesUnchanged:
    """The active/fulfilled paths must still hit the contract. Only the
    misleading completed/timed_out filters short-circuit."""

    def test_status_active_still_calls_contract(self, runner, monkeypatch):
        called = {'get_cli_context': False}

        class FakeClient:
            def get_active_swaps(self):
                called['get_cli_context'] = True
                return []  # empty is fine for this test

        def fake_ctx(*a, **kw):
            return None, None, None, FakeClient()

        monkeypatch.setattr('allways.cli.swap_commands.view.get_cli_context', fake_ctx)

        result = runner.invoke(view_active_swaps, ['--status', 'active'])
        assert result.exit_code == 0
        assert called['get_cli_context'] is True
        # No dashboard hint for queryable statuses — that text is reserved
        # for the resolved branch.
        assert 'pruned from on-chain storage' not in result.output

    def test_no_status_still_calls_contract(self, runner, monkeypatch):
        called = {'get_cli_context': False}

        class FakeClient:
            def get_active_swaps(self):
                called['get_cli_context'] = True
                return []

        def fake_ctx(*a, **kw):
            return None, None, None, FakeClient()

        monkeypatch.setattr('allways.cli.swap_commands.view.get_cli_context', fake_ctx)

        result = runner.invoke(view_active_swaps, [])
        assert result.exit_code == 0
        assert called['get_cli_context'] is True
        assert 'pruned from on-chain storage' not in result.output


class TestChoiceListPreserved:
    """The fix routes completed/timed_out via dashboard rather than dropping
    them — keeps the user-facing surface area, just makes it honest."""

    def test_invalid_status_rejected_by_click(self, runner):
        result = runner.invoke(view_active_swaps, ['--status', 'bogus'])
        assert result.exit_code != 0
        assert 'Invalid value' in result.output or 'bogus' in result.output

    def test_all_four_statuses_accepted_by_click(self, runner, monkeypatch):
        """Click should accept all four; behavior diverges inside the function."""

        class FakeClient:
            def get_active_swaps(self):
                return []

        monkeypatch.setattr(
            'allways.cli.swap_commands.view.get_cli_context',
            lambda *a, **kw: (None, None, None, FakeClient()),
        )

        for status in ('active', 'fulfilled', 'completed', 'timed_out'):
            result = runner.invoke(view_active_swaps, ['--status', status])
            assert result.exit_code == 0, f'{status}: {result.output}'
