"""Tests for allways.utils.config — argparse wiring + check_config."""

import argparse
import os
import tempfile
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from allways.utils.config import (
    add_args,
    add_miner_args,
    add_validator_args,
    check_config,
    config,
)


def _parse(add_fn, argv=None):
    parser = argparse.ArgumentParser()
    add_fn(None, parser)
    return parser.parse_args(argv or [])


class TestAddArgs:
    def test_override_netuid(self):
        args = _parse(add_args, ['--netuid', '42'])
        assert args.netuid == 42

    def test_dont_save_events_flag(self):
        args = _parse(add_args, ['--neuron.dont_save_events'])
        assert getattr(args, 'neuron.dont_save_events') is True


class TestAddMinerArgs:
    def test_override_miner_name(self):
        args = _parse(add_miner_args, ['--neuron.name', 'myminer'])
        assert getattr(args, 'neuron.name') == 'myminer'

    def test_poll_interval_override(self):
        args = _parse(add_miner_args, ['--miner.poll_interval', '5'])
        assert getattr(args, 'miner.poll_interval') == 5


class TestAddValidatorArgs:
    def test_disable_set_weights_flag(self):
        args = _parse(add_validator_args, ['--neuron.disable_set_weights'])
        assert getattr(args, 'neuron.disable_set_weights') is True

    def test_axon_off_flag(self):
        args = _parse(add_validator_args, ['--axon_off'])
        assert getattr(args, 'neuron.axon_off') is True

    def test_moving_average_alpha_override(self):
        args = _parse(add_validator_args, ['--neuron.moving_average_alpha', '0.1'])
        assert getattr(args, 'neuron.moving_average_alpha') == 0.1


class TestCheckConfig:
    def _make_config(self, tmp_dir: str, dont_save_events=True):
        return SimpleNamespace(
            logging=SimpleNamespace(logging_dir=tmp_dir),
            wallet=SimpleNamespace(name='default', hotkey='default'),
            netuid=7,
            neuron=SimpleNamespace(
                name='validator',
                full_path='',
                dont_save_events=dont_save_events,
                events_retention_size=1024,
            ),
        )

    def test_creates_full_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = self._make_config(tmp)
            with patch('bittensor.logging.check_config'):
                check_config(None, cfg)
            assert os.path.exists(cfg.neuron.full_path)

    def test_dont_save_events_skips_logger(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = self._make_config(tmp, dont_save_events=True)
            with patch('bittensor.logging.check_config'), patch('allways.utils.config.setup_events_logger') as setup:
                check_config(None, cfg)
            setup.assert_not_called()

    def test_save_events_registers_logger(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = self._make_config(tmp, dont_save_events=False)
            logger = MagicMock()
            logger.name = 'events'
            with (
                patch('bittensor.logging.check_config'),
                patch('allways.utils.config.setup_events_logger', return_value=logger),
                patch('bittensor.logging.register_primary_logger') as reg,
            ):
                check_config(None, cfg)
            reg.assert_called_once_with('events')


class TestConfig:
    def test_builds_config_with_cls_add_args(self):
        cls = MagicMock()
        with (
            patch('bittensor.Wallet.add_args'),
            patch('bittensor.Subtensor.add_args'),
            patch('bittensor.logging.add_args'),
            patch('bittensor.Axon.add_args'),
            patch('bittensor.Config', return_value='cfg') as bt_config,
        ):
            result = config(cls)
        assert result == 'cfg'
        cls.add_args.assert_called_once()
        bt_config.assert_called_once()
