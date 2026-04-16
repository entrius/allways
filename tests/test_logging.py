"""Tests for allways.utils.logging."""

import logging
import os
import tempfile
from unittest.mock import patch

from allways.utils import logging as events_logging
from allways.utils.logging import EVENTS_LEVEL_NUM, log_on_change, setup_events_logger


class TestSetupEventsLogger:
    def test_returns_logger_at_events_level(self):
        with tempfile.TemporaryDirectory() as tmp:
            logger = setup_events_logger(tmp, events_retention_size=1024)
            assert isinstance(logger, logging.Logger)
            assert logger.level == EVENTS_LEVEL_NUM

    def test_creates_events_log_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            logger = setup_events_logger(tmp, events_retention_size=1024)
            logger.log(EVENTS_LEVEL_NUM, 'hello')
            for h in logger.handlers:
                h.flush()
            assert os.path.exists(os.path.join(tmp, 'events.log'))

    def test_registers_event_level_name(self):
        with tempfile.TemporaryDirectory() as tmp:
            setup_events_logger(tmp, events_retention_size=1024)
            assert logging.getLevelName(EVENTS_LEVEL_NUM) == 'EVENT'

    def test_adds_handler(self):
        with tempfile.TemporaryDirectory() as tmp:
            logger = setup_events_logger(tmp, events_retention_size=1024)
            assert any(
                h.level == EVENTS_LEVEL_NUM for h in logger.handlers
            ), 'expected at least one handler at EVENT level'


class TestLogOnChange:
    def setup_method(self):
        events_logging._last_seen.clear()

    def test_logs_first_time(self):
        with patch('allways.utils.logging.bt.logging.info') as info:
            log_on_change('k', 'v1', 'first')
        info.assert_called_once_with('first')

    def test_suppresses_unchanged_value(self):
        with patch('allways.utils.logging.bt.logging.info') as info:
            log_on_change('k', 'v1', 'first')
            log_on_change('k', 'v1', 'second')
        assert info.call_count == 1

    def test_logs_when_value_changes(self):
        with patch('allways.utils.logging.bt.logging.info') as info:
            log_on_change('k', 'v1', 'first')
            log_on_change('k', 'v2', 'second')
        assert info.call_count == 2

    def test_independent_keys(self):
        with patch('allways.utils.logging.bt.logging.info') as info:
            log_on_change('a', 1, 'a-msg')
            log_on_change('b', 1, 'b-msg')
        assert info.call_count == 2
