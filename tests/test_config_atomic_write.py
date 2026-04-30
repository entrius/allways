"""Tests for atomic config write + loud read warning (issue #244)."""

import json
import os

import pytest
from click.testing import CliRunner

from allways.cli.main import config_set
from allways.cli.swap_commands import helpers as helpers_module
from allways.cli.swap_commands.helpers import load_cli_config


@pytest.fixture
def fake_allways_dir(tmp_path, monkeypatch):
    """Redirect ALLWAYS_DIR / CONFIG_FILE to a temp location so tests don't
    touch the real ~/.allways/config.json."""
    fake_dir = tmp_path / '.allways'
    fake_dir.mkdir()
    fake_config = fake_dir / 'config.json'
    monkeypatch.setattr(helpers_module, 'ALLWAYS_DIR', fake_dir)
    monkeypatch.setattr(helpers_module, 'CONFIG_FILE', fake_config)
    # main.py imports ALLWAYS_DIR / CONFIG_FILE by name — patch its module too.
    from allways.cli import main as main_module

    monkeypatch.setattr(main_module, 'ALLWAYS_DIR', fake_dir)
    monkeypatch.setattr(main_module, 'CONFIG_FILE', fake_config)
    return fake_config


class TestAtomicConfigWrite:
    def test_write_creates_complete_json(self, fake_allways_dir):
        """Happy path: a normal `config set` lands a fully-formed file."""
        runner = CliRunner()
        result = runner.invoke(config_set, ['network', 'local'])
        assert result.exit_code == 0, result.output
        assert json.loads(fake_allways_dir.read_text()) == {'network': 'local'}

    def test_no_residual_tmp_file_on_success(self, fake_allways_dir):
        runner = CliRunner()
        runner.invoke(config_set, ['wallet', 'alice'])
        leftovers = list(fake_allways_dir.parent.glob('*.tmp'))
        assert leftovers == []

    def test_existing_config_preserved_when_write_fails(self, fake_allways_dir, monkeypatch):
        """The crucial property: simulate an interrupt mid-write; the
        old config must survive intact (issue #244 reproducer fixed)."""
        fake_allways_dir.write_text(json.dumps({'network': 'local', 'netuid': 7}))

        original_replace = os.replace

        def boom(*a, **kw):
            raise KeyboardInterrupt('Ctrl+C mid-replace')

        monkeypatch.setattr('os.replace', boom)

        runner = CliRunner()
        result = runner.invoke(config_set, ['wallet', 'bob'], catch_exceptions=True)
        # The interrupt propagates; what matters is the file state.
        assert result.exit_code != 0
        # Restore os.replace so any cleanup the test runner does still works.
        monkeypatch.setattr('os.replace', original_replace)
        # Original config is untouched — user did NOT silently revert to mainnet.
        assert json.loads(fake_allways_dir.read_text()) == {'network': 'local', 'netuid': 7}

    def test_tmp_file_cleaned_up_on_write_failure(self, fake_allways_dir, monkeypatch):
        """If os.replace fails, the temp file should be unlinked, not leaked."""

        def boom(*a, **kw):
            raise OSError('disk full')

        monkeypatch.setattr('os.replace', boom)

        runner = CliRunner()
        runner.invoke(config_set, ['wallet', 'alice'], catch_exceptions=True)
        leftovers = list(fake_allways_dir.parent.glob('*.tmp'))
        assert leftovers == []


class TestLoudLoadCliConfig:
    def test_missing_file_returns_empty_silently(self, fake_allways_dir, capsys):
        # File doesn't exist yet — silent {} is correct.
        assert load_cli_config() == {}
        captured = capsys.readouterr()
        assert 'Warning' not in captured.out

    def test_valid_file_returns_parsed(self, fake_allways_dir):
        fake_allways_dir.write_text(json.dumps({'network': 'local'}))
        assert load_cli_config() == {'network': 'local'}

    def test_truncated_file_warns_and_returns_empty(self, fake_allways_dir, capsys):
        """Reproducer from issue #244: partial-write truncation."""
        fake_allways_dir.write_text('{"network": "lo')
        result = load_cli_config()
        assert result == {}  # still {} so callers don't crash
        captured = capsys.readouterr()
        # Rich line-wraps to terminal width, so the path can split across
        # lines — collapse whitespace before substring matching.
        output = ' '.join((captured.out + captured.err).split())
        assert 'Warning' in output
        assert 'unreadable' in output
        assert 'config.json' in output

    def test_empty_file_warns(self, fake_allways_dir, capsys):
        fake_allways_dir.write_text('')
        result = load_cli_config()
        assert result == {}
        captured = capsys.readouterr()
        assert 'Warning' in captured.out + captured.err
