"""Tests for switchHomeUser retry wiring.

Verifies that `get_plex_instance` and `get_watchlist_media` wrap their
plex.tv `MyPlexAccount(...)` + `switchHomeUser(...)` pair in
`_retry_plextv_call` so transient connection resets / timeouts don't
fail the user's run on the first hiccup. The helper's own retry
semantics live in `test_plex_api_retry.py`; this module focuses on
integration at the two call sites.
"""

import os
import sys
from unittest.mock import MagicMock, patch

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

from core.plex_api import PlexManager, PLEXTV_MAX_RETRIES


def _bare_api():
    """Construct a PlexManager without running __init__ (avoids network auth)."""
    api = PlexManager.__new__(PlexManager)
    api.plex_url = "http://localhost:32400"
    api.plex_token = "ADMIN_TOKEN"
    api._user_tokens = {}
    api._user_is_home = {}
    api._ondeck_data_complete = True
    api._watchlist_data_complete = True
    # _token_lock + _rate_limited_api_call are touched by both call sites
    import threading
    api._token_lock = threading.Lock()
    api._rate_limited_api_call = MagicMock()
    api._token_cache = MagicMock()
    return api


class TestGetPlexInstanceSwitchHomeUserRetry:
    """get_plex_instance() home-user fallback path."""

    def test_transient_then_success_returns_plex_server(self):
        """ConnectionResetError on first attempt, success on second → user gets a PlexServer."""
        api = _bare_api()
        api._user_is_home["Paige"] = True

        switched = MagicMock()
        switched.authenticationToken = "PAIGE_TOKEN"
        admin = MagicMock()
        # First switchHomeUser raises a wrapped ConnectionResetError; second succeeds.
        admin.switchHomeUser.side_effect = [
            requests.ConnectionError("('Connection aborted.', ConnectionResetError(104, 'Connection reset by peer'))"),
            switched,
        ]

        user = MagicMock()
        user.title = "Paige"

        with patch('plexapi.myplex.MyPlexAccount', return_value=admin) as mock_acct, \
             patch('core.plex_api.PlexServer') as mock_server, \
             patch('core.plex_api.time.sleep'):
            username, server = api.get_plex_instance(user=user)

        assert username == "Paige"
        assert server is mock_server.return_value
        # Two construction attempts (one per retry attempt) and two switch attempts.
        assert mock_acct.call_count == 2
        assert admin.switchHomeUser.call_count == 2

    def test_all_attempts_fail_returns_none(self):
        """Persistent transient error → returns (None, None) after exhausting retries."""
        api = _bare_api()
        api._user_is_home["Paige"] = True

        admin = MagicMock()
        admin.switchHomeUser.side_effect = requests.ConnectionError("connection reset")

        user = MagicMock()
        user.title = "Paige"

        with patch('plexapi.myplex.MyPlexAccount', return_value=admin), \
             patch('core.plex_api.PlexServer'), \
             patch('core.plex_api.time.sleep'):
            username, server = api.get_plex_instance(user=user)

        assert username is None
        assert server is None
        assert admin.switchHomeUser.call_count == PLEXTV_MAX_RETRIES

    def test_non_retriable_error_not_retried(self):
        """Auth errors (e.g. 401) raise immediately; no retry, no sleep."""
        api = _bare_api()
        api._user_is_home["Paige"] = True

        admin = MagicMock()
        admin.switchHomeUser.side_effect = ValueError("(401) Unauthorized")

        user = MagicMock()
        user.title = "Paige"

        with patch('plexapi.myplex.MyPlexAccount', return_value=admin), \
             patch('core.plex_api.PlexServer'), \
             patch('core.plex_api.time.sleep') as mock_sleep:
            username, server = api.get_plex_instance(user=user)

        assert username is None
        assert server is None
        assert admin.switchHomeUser.call_count == 1
        mock_sleep.assert_not_called()


class TestFetchUserWatchlistSwitchHomeUserRetry:
    """_fetch_user_watchlist() home-user account-construction path."""

    def _setup_for_watchlist(self, api):
        """Wire up the minimum state _fetch_user_watchlist() touches before the switch."""
        api._watchlist_data_complete = True
        api.mark_watchlist_incomplete = MagicMock(
            side_effect=lambda: setattr(api, '_watchlist_data_complete', False)
        )

    def test_transient_then_success_proceeds_to_watchlist(self):
        """First attempt resets, second succeeds → watchlist is fetched (no incomplete flag)."""
        api = _bare_api()
        self._setup_for_watchlist(api)

        switched_account = MagicMock()
        switched_account.watchlist.return_value = []
        admin = MagicMock()
        admin.switchHomeUser.side_effect = [
            requests.ConnectionError("Connection aborted."),
            switched_account,
        ]

        user = MagicMock()
        user.title = "Paige"

        with patch('core.plex_api.MyPlexAccount', return_value=admin), \
             patch('core.plex_api.requests.Session'), \
             patch('core.plex_api.time.sleep'):
            list(api._fetch_user_watchlist(
                user=user,
                valid_sections=[1],
                watchlist_episodes=3,
                skip_watchlist=[],
                rss_url=None,
                filtered_sections=[1],
            ))

        # Retry succeeded → no incomplete flag set, watchlist fetch reached.
        assert api.mark_watchlist_incomplete.call_count == 0
        assert admin.switchHomeUser.call_count == 2

    def test_all_attempts_fail_marks_watchlist_incomplete(self):
        """Persistent transient error → mark_watchlist_incomplete + early return."""
        api = _bare_api()
        self._setup_for_watchlist(api)

        admin = MagicMock()
        admin.switchHomeUser.side_effect = requests.ConnectionError("connection reset")

        user = MagicMock()
        user.title = "Paige"

        with patch('core.plex_api.MyPlexAccount', return_value=admin), \
             patch('core.plex_api.requests.Session'), \
             patch('core.plex_api.time.sleep'):
            list(api._fetch_user_watchlist(
                user=user,
                valid_sections=[1],
                watchlist_episodes=3,
                skip_watchlist=[],
                rss_url=None,
                filtered_sections=[1],
            ))

        api.mark_watchlist_incomplete.assert_called_once()
        assert admin.switchHomeUser.call_count == PLEXTV_MAX_RETRIES

    def test_non_retriable_error_not_retried(self):
        """403 from switchHomeUser → no retry, mark incomplete, return."""
        api = _bare_api()
        self._setup_for_watchlist(api)

        admin = MagicMock()
        admin.switchHomeUser.side_effect = ValueError("(403) Forbidden")

        user = MagicMock()
        user.title = "Paige"

        with patch('core.plex_api.MyPlexAccount', return_value=admin), \
             patch('core.plex_api.requests.Session'), \
             patch('core.plex_api.time.sleep') as mock_sleep:
            list(api._fetch_user_watchlist(
                user=user,
                valid_sections=[1],
                watchlist_episodes=3,
                skip_watchlist=[],
                rss_url=None,
                filtered_sections=[1],
            ))

        api.mark_watchlist_incomplete.assert_called_once()
        assert admin.switchHomeUser.call_count == 1
        mock_sleep.assert_not_called()
