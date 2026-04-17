"""Tests for the _log_api_error helper in core.plex_api.

Verifies that HTML (e.g. nginx 5xx error pages) is stripped from log output
and that transient 5xx responses are logged at WARNING rather than ERROR.
"""

import logging
import os
import sys
from unittest.mock import MagicMock

import pytest

sys.modules['fcntl'] = MagicMock()

for _mod in [
    'apscheduler', 'apscheduler.schedulers',
    'apscheduler.schedulers.background', 'apscheduler.triggers',
    'apscheduler.triggers.cron', 'apscheduler.triggers.interval',
]:
    sys.modules.setdefault(_mod, MagicMock())

for _mod in [
    'plexapi', 'plexapi.server', 'plexapi.video', 'plexapi.myplex',
    'plexapi.library', 'plexapi.exceptions', 'requests',
]:
    sys.modules.setdefault(_mod, MagicMock())

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.plex_api import _log_api_error


NGINX_503_HTML = (
    "(503) Service Temporarily Unavailable; "
    "<html>\r\n<head><title>503 Service Temporarily Unavailable</title></head>\r\n"
    "<body>\r\n<center><h1>503 Service Temporarily Unavailable</h1></center>\r\n"
    "<hr><center>nginx</center>\r\n</body>\r\n</html>\r\n"
)


class TestLogApiErrorHtmlStripping:
    """HTML tags from upstream error bodies must not reach logs."""

    def test_503_html_body_is_stripped(self, caplog):
        with caplog.at_level(logging.DEBUG, logger="root"):
            _log_api_error("load user tokens", Exception(NGINX_503_HTML))

        combined = "\n".join(r.getMessage() for r in caplog.records)
        assert "<" not in combined
        assert ">" not in combined
        assert "503 Service Temporarily Unavailable" in combined
        assert "nginx" in combined

    def test_whitespace_is_collapsed(self, caplog):
        with caplog.at_level(logging.DEBUG, logger="root"):
            _log_api_error("ctx", Exception("line1\r\n\r\n\tline2   line3"))

        combined = "\n".join(r.getMessage() for r in caplog.records)
        assert "line1 line2 line3" in combined

    def test_plain_error_message_preserved(self, caplog):
        with caplog.at_level(logging.DEBUG, logger="root"):
            _log_api_error("fetch watchlist", Exception("Something simple went wrong"))

        combined = "\n".join(r.getMessage() for r in caplog.records)
        assert "Something simple went wrong" in combined


class TestLogApiErrorSeverity:
    """Transient plex.tv outages should not trip error-level notification handlers."""

    @pytest.mark.parametrize("status", ["500", "502", "503"])
    def test_5xx_logged_as_warning(self, caplog, status):
        with caplog.at_level(logging.DEBUG, logger="root"):
            _log_api_error("get Plex account for main", Exception(f"({status}) Bad Gateway"))

        levels = {r.levelno for r in caplog.records}
        assert logging.ERROR not in levels
        assert logging.WARNING in levels

    def test_401_still_logged_as_error(self, caplog):
        with caplog.at_level(logging.DEBUG, logger="root"):
            _log_api_error("load user tokens", Exception("(401) Unauthorized"))

        levels = {r.levelno for r in caplog.records}
        assert logging.ERROR in levels

    def test_403_still_logged_as_error(self, caplog):
        with caplog.at_level(logging.DEBUG, logger="root"):
            _log_api_error("switchHomeUser", Exception("(403) Forbidden"))

        levels = {r.levelno for r in caplog.records}
        assert logging.ERROR in levels

    def test_429_logged_as_warning(self, caplog):
        with caplog.at_level(logging.DEBUG, logger="root"):
            _log_api_error("fetch OnDeck", Exception("(429) Too Many Requests"))

        levels = {r.levelno for r in caplog.records}
        assert logging.WARNING in levels
        assert logging.ERROR not in levels

    def test_404_logged_as_warning(self, caplog):
        with caplog.at_level(logging.DEBUG, logger="root"):
            _log_api_error("resolve UUID", Exception("(404) Not Found"))

        levels = {r.levelno for r in caplog.records}
        assert logging.WARNING in levels
        assert logging.ERROR not in levels

    def test_unrecognized_error_logged_as_error(self, caplog):
        with caplog.at_level(logging.DEBUG, logger="root"):
            _log_api_error("ctx", Exception("Some unrecognized failure"))

        levels = {r.levelno for r in caplog.records}
        assert logging.ERROR in levels
