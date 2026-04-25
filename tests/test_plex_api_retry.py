"""Tests for _retry_plextv_call helper.

Verifies that transient plex.tv network errors (timeouts, connection errors)
are retried with backoff, while permanent errors raise immediately.
"""

import os
import sys
from unittest.mock import MagicMock, patch, call

import pytest

sys.modules['fcntl'] = MagicMock()
for _mod in [
    'apscheduler', 'apscheduler.schedulers',
    'apscheduler.schedulers.background', 'apscheduler.triggers',
    'apscheduler.triggers.cron', 'apscheduler.triggers.interval',
    'plexapi', 'plexapi.server', 'plexapi.video', 'plexapi.myplex',
    'plexapi.library', 'plexapi.exceptions',
]:
    sys.modules.setdefault(_mod, MagicMock())

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests

from core.plex_api import _retry_plextv_call, PLEXTV_MAX_RETRIES


class TestRetryPlexTvCall:
    """Verify retry semantics for the plex.tv retry helper."""

    def test_returns_immediately_on_success(self):
        func = MagicMock(return_value="ok")
        result = _retry_plextv_call(func, label="test")
        assert result == "ok"
        assert func.call_count == 1

    def test_retries_on_read_timeout_then_succeeds(self):
        func = MagicMock(side_effect=[requests.Timeout("read timeout"), "ok"])
        with patch('core.plex_api.time.sleep') as mock_sleep:
            result = _retry_plextv_call(func, label="test")
        assert result == "ok"
        assert func.call_count == 2
        mock_sleep.assert_called_once()

    def test_retries_on_connection_error_then_succeeds(self):
        func = MagicMock(side_effect=[requests.ConnectionError("dns fail"), "ok"])
        with patch('core.plex_api.time.sleep'):
            result = _retry_plextv_call(func, label="test")
        assert result == "ok"
        assert func.call_count == 2

    def test_gives_up_after_max_attempts_and_raises(self):
        err = requests.Timeout("read timeout")
        func = MagicMock(side_effect=err)
        with patch('core.plex_api.time.sleep'), pytest.raises(requests.Timeout):
            _retry_plextv_call(func, label="test")
        assert func.call_count == PLEXTV_MAX_RETRIES

    def test_non_retriable_exception_raised_immediately(self):
        """Auth errors, logic bugs, etc. should not be retried."""
        func = MagicMock(side_effect=ValueError("bad token"))
        with pytest.raises(ValueError):
            _retry_plextv_call(func, label="test")
        assert func.call_count == 1

    def test_backoff_is_exponential(self):
        """Wait times should be 2s, 4s (PLEXTV_RETRY_BASE_WAIT ** attempt)."""
        func = MagicMock(side_effect=[
            requests.Timeout("t1"), requests.Timeout("t2"), "ok"
        ])
        with patch('core.plex_api.time.sleep') as mock_sleep:
            _retry_plextv_call(func, label="test")
        wait_times = [c.args[0] for c in mock_sleep.call_args_list]
        assert wait_times == [2, 4]

    def test_respects_custom_max_attempts(self):
        func = MagicMock(side_effect=requests.Timeout("t"))
        with patch('core.plex_api.time.sleep'), pytest.raises(requests.Timeout):
            _retry_plextv_call(func, label="test", max_attempts=2)
        assert func.call_count == 2

    def test_logs_warning_with_label_on_retry(self, caplog):
        import logging
        func = MagicMock(side_effect=[requests.Timeout("oops"), "ok"])
        with patch('core.plex_api.time.sleep'), caplog.at_level(logging.WARNING):
            _retry_plextv_call(func, label="watchlist for Brandon")
        assert any("watchlist for Brandon" in rec.message for rec in caplog.records)
        assert any("1/3" in rec.message for rec in caplog.records)
