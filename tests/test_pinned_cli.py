"""Tests for core/pinned_cli.py — CLI handlers for pinned media management."""

import json
import os
import sys
from unittest.mock import MagicMock, patch

import pytest

from core.pinned_media import PinnedMediaTracker


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_config_manager(tmp_path):
    """Build a mock ConfigManager pointing at tmp_path for data files."""
    cm = MagicMock()
    pinned_file = tmp_path / "pinned_media.json"
    cm.get_pinned_media_file.return_value = pinned_file
    cm.plex.plex_url = "http://localhost:32400"
    cm.plex.plex_token = "test-token"
    cm.plex.pinned_preferred_resolution = "highest"
    return cm


def _tracker_from_cm(cm):
    """Create a PinnedMediaTracker using the same path that _get_tracker would."""
    return PinnedMediaTracker(str(cm.get_pinned_media_file()))


def _make_plex_item(rating_key, title, item_type="movie", year=2020):
    item = MagicMock()
    item.ratingKey = int(rating_key)
    item.title = title
    item.type = item_type
    item.year = year
    return item


# ---------------------------------------------------------------------------
# extract_flag_value
# ---------------------------------------------------------------------------

class TestExtractFlagValue:
    def test_extracts_value(self):
        from core.pinned_cli import extract_flag_value
        with patch.object(sys, "argv", ["prog", "--pin", "12345"]):
            assert extract_flag_value("--pin") == "12345"

    def test_returns_none_when_missing(self):
        from core.pinned_cli import extract_flag_value
        with patch.object(sys, "argv", ["prog", "--verbose"]):
            assert extract_flag_value("--pin") is None

    def test_returns_none_when_no_value(self):
        from core.pinned_cli import extract_flag_value
        with patch.object(sys, "argv", ["prog", "--pin"]):
            assert extract_flag_value("--pin") is None

    def test_extracts_quoted_title(self):
        from core.pinned_cli import extract_flag_value
        with patch.object(sys, "argv", ["prog", "--pin-by-title", "Breaking Bad"]):
            assert extract_flag_value("--pin-by-title") == "Breaking Bad"


# ---------------------------------------------------------------------------
# handle_list_pins
# ---------------------------------------------------------------------------

class TestHandleListPins:
    def test_empty_list(self, tmp_path, capsys):
        from core.pinned_cli import handle_list_pins
        cm = _make_config_manager(tmp_path)
        handle_list_pins(cm)
        assert "No pinned media" in capsys.readouterr().out

    def test_lists_pins(self, tmp_path, capsys):
        from core.pinned_cli import handle_list_pins
        cm = _make_config_manager(tmp_path)
        tracker = _tracker_from_cm(cm)
        tracker.add_pin("100", "movie", "Test Movie", added_by="cli")
        tracker.add_pin("200", "show", "Test Show", added_by="web")

        with patch("core.pinned_cli._connect_plex", return_value=None):
            handle_list_pins(cm)

        out = capsys.readouterr().out
        assert "2 items" in out
        assert "Test Movie" in out
        assert "Test Show" in out
        assert "rating_key=100" in out

    def test_shows_resolution_count(self, tmp_path, capsys):
        from core.pinned_cli import handle_list_pins
        cm = _make_config_manager(tmp_path)
        tracker = _tracker_from_cm(cm)
        tracker.add_pin("100", "movie", "Test Movie")

        mock_plex = MagicMock()
        resolved = [("/data/Movies/Test.mkv", "100", "movie")]

        with patch("core.pinned_cli._connect_plex", return_value=mock_plex), \
             patch("core.pinned_media.resolve_pins_to_paths", return_value=(resolved, [])) as mock_resolve:
            handle_list_pins(cm)

        out = capsys.readouterr().out
        assert "1 file(s)" in out


# ---------------------------------------------------------------------------
# handle_pin
# ---------------------------------------------------------------------------

class TestHandlePin:
    def test_pin_new_item(self, tmp_path, capsys):
        from core.pinned_cli import handle_pin
        cm = _make_config_manager(tmp_path)

        mock_plex = MagicMock()
        item = _make_plex_item("555", "Inception")
        mock_plex.fetchItem.return_value = item

        with patch("core.pinned_cli._connect_plex", return_value=mock_plex):
            handle_pin(cm, "555")

        out = capsys.readouterr().out
        assert "Pinned" in out
        assert "Inception" in out
        # Re-read from the same file to confirm persistence
        tracker = _tracker_from_cm(cm)
        assert tracker.is_pinned("555")

    def test_pin_already_pinned(self, tmp_path, capsys):
        from core.pinned_cli import handle_pin
        cm = _make_config_manager(tmp_path)
        tracker = _tracker_from_cm(cm)
        tracker.add_pin("555", "movie", "Inception")

        handle_pin(cm, "555")
        assert "Already pinned" in capsys.readouterr().out

    def test_pin_fetch_fails(self, tmp_path, capsys):
        from core.pinned_cli import handle_pin
        cm = _make_config_manager(tmp_path)

        mock_plex = MagicMock()
        mock_plex.fetchItem.side_effect = Exception("Not found")

        with patch("core.pinned_cli._connect_plex", return_value=mock_plex):
            handle_pin(cm, "999")

        assert "Could not fetch" in capsys.readouterr().out

    def test_pin_no_plex(self, tmp_path, capsys):
        from core.pinned_cli import handle_pin
        cm = _make_config_manager(tmp_path)

        with patch("core.pinned_cli._connect_plex", return_value=None):
            handle_pin(cm, "555")

        tracker = _tracker_from_cm(cm)
        assert not tracker.is_pinned("555")

    def test_pin_derives_type_from_show(self, tmp_path, capsys):
        from core.pinned_cli import handle_pin
        cm = _make_config_manager(tmp_path)

        mock_plex = MagicMock()
        item = _make_plex_item("300", "Breaking Bad", item_type="show")
        mock_plex.fetchItem.return_value = item

        with patch("core.pinned_cli._connect_plex", return_value=mock_plex):
            handle_pin(cm, "300")

        tracker = _tracker_from_cm(cm)
        pin = tracker.get_pin("300")
        assert pin["type"] == "show"


# ---------------------------------------------------------------------------
# handle_unpin
# ---------------------------------------------------------------------------

class TestHandleUnpin:
    def test_unpin_existing(self, tmp_path, capsys):
        from core.pinned_cli import handle_unpin
        cm = _make_config_manager(tmp_path)
        tracker = _tracker_from_cm(cm)
        tracker.add_pin("100", "movie", "Test Movie")

        handle_unpin(cm, "100")
        out = capsys.readouterr().out
        assert "Unpinned" in out
        assert "Test Movie" in out
        # Re-read from disk to confirm removal
        tracker2 = _tracker_from_cm(cm)
        assert not tracker2.is_pinned("100")

    def test_unpin_not_pinned(self, tmp_path, capsys):
        from core.pinned_cli import handle_unpin
        cm = _make_config_manager(tmp_path)
        handle_unpin(cm, "999")
        assert "Not pinned" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# handle_pin_by_title
# ---------------------------------------------------------------------------

class TestHandlePinByTitle:
    def test_no_results(self, tmp_path, capsys):
        from core.pinned_cli import handle_pin_by_title
        cm = _make_config_manager(tmp_path)

        mock_plex = MagicMock()
        mock_plex.search.return_value = []

        with patch("core.pinned_cli._connect_plex", return_value=mock_plex):
            handle_pin_by_title(cm, "nonexistent")

        assert "No results" in capsys.readouterr().out

    def test_results_displayed_and_pin(self, tmp_path, capsys, monkeypatch):
        from core.pinned_cli import handle_pin_by_title
        cm = _make_config_manager(tmp_path)

        item = _make_plex_item("400", "Breaking Bad", item_type="show", year=2008)
        mock_plex = MagicMock()
        mock_plex.search.side_effect = lambda q, mediatype=None, limit=None: (
            [item] if mediatype == "show" else []
        )

        monkeypatch.setattr("builtins.input", lambda: "1")

        with patch("core.pinned_cli._connect_plex", return_value=mock_plex):
            handle_pin_by_title(cm, "Breaking")

        out = capsys.readouterr().out
        assert "Breaking Bad" in out
        assert "(2008)" in out
        assert "Pinned" in out

        tracker = _tracker_from_cm(cm)
        assert tracker.is_pinned("400")

    def test_cancel_with_q(self, tmp_path, capsys, monkeypatch):
        from core.pinned_cli import handle_pin_by_title
        cm = _make_config_manager(tmp_path)

        item = _make_plex_item("400", "Breaking Bad", item_type="show")
        mock_plex = MagicMock()
        mock_plex.search.side_effect = lambda q, mediatype=None, limit=None: (
            [item] if mediatype == "show" else []
        )

        monkeypatch.setattr("builtins.input", lambda: "q")

        with patch("core.pinned_cli._connect_plex", return_value=mock_plex):
            handle_pin_by_title(cm, "Breaking")

        assert "Cancelled" in capsys.readouterr().out

    def test_invalid_choice(self, tmp_path, capsys, monkeypatch):
        from core.pinned_cli import handle_pin_by_title
        cm = _make_config_manager(tmp_path)

        item = _make_plex_item("400", "Breaking Bad", item_type="show")
        mock_plex = MagicMock()
        mock_plex.search.side_effect = lambda q, mediatype=None, limit=None: (
            [item] if mediatype == "show" else []
        )

        monkeypatch.setattr("builtins.input", lambda: "99")

        with patch("core.pinned_cli._connect_plex", return_value=mock_plex):
            handle_pin_by_title(cm, "Breaking")

        assert "Invalid choice" in capsys.readouterr().out

    def test_already_pinned_skipped(self, tmp_path, capsys, monkeypatch):
        from core.pinned_cli import handle_pin_by_title
        cm = _make_config_manager(tmp_path)
        tracker = _tracker_from_cm(cm)
        tracker.add_pin("400", "show", "Breaking Bad")

        item = _make_plex_item("400", "Breaking Bad", item_type="show")
        mock_plex = MagicMock()
        mock_plex.search.side_effect = lambda q, mediatype=None, limit=None: (
            [item] if mediatype == "show" else []
        )

        monkeypatch.setattr("builtins.input", lambda: "1")

        with patch("core.pinned_cli._connect_plex", return_value=mock_plex):
            handle_pin_by_title(cm, "Breaking")

        out = capsys.readouterr().out
        assert "[PINNED]" in out
        assert "Already pinned" in out

    def test_no_plex_connection(self, tmp_path, capsys):
        from core.pinned_cli import handle_pin_by_title
        cm = _make_config_manager(tmp_path)

        with patch("core.pinned_cli._connect_plex", return_value=None):
            handle_pin_by_title(cm, "anything")

    def test_eof_cancels(self, tmp_path, capsys, monkeypatch):
        from core.pinned_cli import handle_pin_by_title
        cm = _make_config_manager(tmp_path)

        item = _make_plex_item("400", "Test", item_type="movie")
        mock_plex = MagicMock()
        mock_plex.search.side_effect = lambda q, mediatype=None, limit=None: (
            [item] if mediatype == "movie" else []
        )

        def raise_eof():
            raise EOFError()
        monkeypatch.setattr("builtins.input", raise_eof)

        with patch("core.pinned_cli._connect_plex", return_value=mock_plex):
            handle_pin_by_title(cm, "Test")

        assert "Cancelled" in capsys.readouterr().out

    def test_pin_movie_type(self, tmp_path, capsys, monkeypatch):
        from core.pinned_cli import handle_pin_by_title
        cm = _make_config_manager(tmp_path)

        item = _make_plex_item("500", "Inception", item_type="movie", year=2010)
        mock_plex = MagicMock()
        mock_plex.search.side_effect = lambda q, mediatype=None, limit=None: (
            [item] if mediatype == "movie" else []
        )

        monkeypatch.setattr("builtins.input", lambda: "1")

        with patch("core.pinned_cli._connect_plex", return_value=mock_plex):
            handle_pin_by_title(cm, "Inception")

        tracker = _tracker_from_cm(cm)
        pin = tracker.get_pin("500")
        assert pin["type"] == "movie"


# ---------------------------------------------------------------------------
# _connect_plex
# ---------------------------------------------------------------------------

class TestConnectPlex:
    def test_missing_url(self, capsys):
        from core.pinned_cli import _connect_plex
        cm = MagicMock()
        cm.plex.plex_url = ""
        cm.plex.plex_token = "tok"
        assert _connect_plex(cm) is None
        assert "must be configured" in capsys.readouterr().out

    def test_missing_token(self, capsys):
        from core.pinned_cli import _connect_plex
        cm = MagicMock()
        cm.plex.plex_url = "http://localhost:32400"
        cm.plex.plex_token = ""
        assert _connect_plex(cm) is None

    def test_connection_error(self, capsys):
        from core.pinned_cli import _connect_plex
        cm = MagicMock()
        cm.plex.plex_url = "http://localhost:32400"
        cm.plex.plex_token = "tok"

        mock_server_mod = MagicMock()
        mock_server_mod.PlexServer.side_effect = Exception("Connection refused")
        with patch.dict("sys.modules", {"plexapi.server": mock_server_mod, "plexapi": MagicMock()}):
            result = _connect_plex(cm)

        assert result is None
        assert "Could not connect" in capsys.readouterr().out

    def test_success(self):
        from core.pinned_cli import _connect_plex
        cm = MagicMock()
        cm.plex.plex_url = "http://localhost:32400"
        cm.plex.plex_token = "tok"

        mock_server_mod = MagicMock()
        mock_plex = MagicMock()
        mock_server_mod.PlexServer.return_value = mock_plex
        with patch.dict("sys.modules", {"plexapi.server": mock_server_mod, "plexapi": MagicMock()}):
            result = _connect_plex(cm)

        assert result is mock_plex


# ---------------------------------------------------------------------------
# _derive_pin_type
# ---------------------------------------------------------------------------

class TestDerivePinType:
    @pytest.mark.parametrize("item_type,expected", [
        ("movie", "movie"),
        ("show", "show"),
        ("season", "season"),
        ("episode", "episode"),
        ("unknown", "movie"),
        ("", "movie"),
    ])
    def test_derives_correctly(self, item_type, expected):
        from core.pinned_cli import _derive_pin_type
        item = MagicMock()
        item.type = item_type
        assert _derive_pin_type(item) == expected


# ---------------------------------------------------------------------------
# Integration: _run_pinned_command dispatch
# ---------------------------------------------------------------------------

class TestRunPinnedCommand:
    """Test _run_pinned_command dispatch from core/app.py.

    core.app has heavy imports (plexapi, etc.) so we mock those out
    and test the dispatch function directly.
    """

    @pytest.fixture(autouse=True)
    def _mock_heavy_imports(self):
        """Ensure plexapi and requests are mocked before importing core.app."""
        mocks = {}
        for mod_name in [
            "plexapi", "plexapi.server", "plexapi.myplex", "plexapi.exceptions",
            "plexapi.library", "plexapi.video", "plexapi.media",
            "requests",
        ]:
            if mod_name not in sys.modules:
                mocks[mod_name] = MagicMock()
        with patch.dict("sys.modules", mocks):
            yield

    def _make_settings_file(self, tmp_path):
        config_file = str(tmp_path / "settings.json")
        settings = {
            "plex_url": "http://localhost:32400",
            "plex_token": "test",
            "pinned_preferred_resolution": "highest",
        }
        with open(config_file, "w") as f:
            json.dump(settings, f, indent=2)
        return config_file

    def _run_with_mock_config(self, config_file):
        """Import and run _run_pinned_command with mocked ConfigManager."""
        from core.app import _run_pinned_command
        with patch("core.app.ConfigManager") as MockCM:
            mock_cm = MagicMock()
            MockCM.return_value = mock_cm
            _run_pinned_command(config_file)
            return mock_cm

    def test_list_pins_dispatches(self, tmp_path):
        config_file = self._make_settings_file(tmp_path)
        with patch("core.pinned_cli.handle_list_pins") as mock_handler, \
             patch.object(sys, "argv", ["prog", "--list-pins"]):
            self._run_with_mock_config(config_file)
            mock_handler.assert_called_once()

    def test_pin_dispatches(self, tmp_path):
        config_file = self._make_settings_file(tmp_path)
        with patch("core.pinned_cli.handle_pin") as mock_handler, \
             patch.object(sys, "argv", ["prog", "--pin", "12345"]):
            self._run_with_mock_config(config_file)
            mock_handler.assert_called_once()
            args = mock_handler.call_args
            assert args[0][1] == "12345"

    def test_unpin_dispatches(self, tmp_path):
        config_file = self._make_settings_file(tmp_path)
        with patch("core.pinned_cli.handle_unpin") as mock_handler, \
             patch.object(sys, "argv", ["prog", "--unpin", "12345"]):
            self._run_with_mock_config(config_file)
            mock_handler.assert_called_once()
            args = mock_handler.call_args
            assert args[0][1] == "12345"

    def test_pin_by_title_dispatches(self, tmp_path):
        config_file = self._make_settings_file(tmp_path)
        with patch("core.pinned_cli.handle_pin_by_title") as mock_handler, \
             patch.object(sys, "argv", ["prog", "--pin-by-title", "Breaking Bad"]):
            self._run_with_mock_config(config_file)
            mock_handler.assert_called_once()
            args = mock_handler.call_args
            assert args[0][1] == "Breaking Bad"

    def test_pin_missing_value_prints_error(self, tmp_path, capsys):
        config_file = self._make_settings_file(tmp_path)
        with patch.object(sys, "argv", ["prog", "--pin"]):
            self._run_with_mock_config(config_file)
        assert "requires a rating_key" in capsys.readouterr().out

    def test_unpin_missing_value_prints_error(self, tmp_path, capsys):
        config_file = self._make_settings_file(tmp_path)
        with patch.object(sys, "argv", ["prog", "--unpin"]):
            self._run_with_mock_config(config_file)
        assert "requires a rating_key" in capsys.readouterr().out

    def test_pin_by_title_missing_value_prints_error(self, tmp_path, capsys):
        config_file = self._make_settings_file(tmp_path)
        with patch.object(sys, "argv", ["prog", "--pin-by-title"]):
            self._run_with_mock_config(config_file)
        assert "requires a search query" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# Budget enforcement (enhancement #11)
# ---------------------------------------------------------------------------


def _make_sized_movie(rating_key, title, size_bytes):
    """Build a plex-item mock whose single Media reports size_bytes."""
    item = _make_plex_item(rating_key, title, item_type="movie")
    part = MagicMock()
    part.size = size_bytes
    media = MagicMock()
    media.parts = [part]
    # videoResolution drives select_media_version; 1080 matches "highest"
    # with a single media (sort is stable, one element).
    media.videoResolution = "1080"
    item.media = [media]
    return item


class TestCliBudgetEnforcement:
    """The CLI must apply the same cache-budget guard as the web UI.

    Before #11 the CLI called ``tracker.add_pin`` without a preflight, so
    ``--pin`` or ``--pin-by-title`` could silently push pinned bytes past
    ``cache_limit``. These tests lock in the new behavior: over-budget pins
    error out with a clear message and a non-zero exit code, while
    under-budget and unconfigured-budget pins continue to succeed.
    """

    def _make_cm_with_budget(self, tmp_path, cache_limit):
        cm = _make_config_manager(tmp_path)
        cm.settings_data = {
            "cache_limit": cache_limit,
            "min_free_space": "",
            "plexcache_quota": "",
            "path_mappings": [],
        }
        return cm

    def test_pin_rejected_when_item_would_exceed_budget(self, tmp_path, capsys):
        from core.pinned_cli import handle_pin
        cm = self._make_cm_with_budget(tmp_path, "500MB")

        mock_plex = MagicMock()
        # 1GB movie against a 500MB cache_limit → hard-block.
        mock_plex.fetchItem.return_value = _make_sized_movie(
            "777", "Oppenheimer", size_bytes=1024 * 1024 * 1024
        )

        with patch("core.pinned_cli._connect_plex", return_value=mock_plex):
            with pytest.raises(SystemExit) as exc:
                handle_pin(cm, "777")

        assert exc.value.code == 1
        out = capsys.readouterr().out
        assert "exceed the cache budget" in out
        # Tracker must not have been written.
        tracker = _tracker_from_cm(cm)
        assert not tracker.is_pinned("777")

    def test_pin_succeeds_when_item_fits_budget(self, tmp_path, capsys):
        from core.pinned_cli import handle_pin
        cm = self._make_cm_with_budget(tmp_path, "10GB")

        mock_plex = MagicMock()
        mock_plex.fetchItem.return_value = _make_sized_movie(
            "888", "Arrival", size_bytes=2 * 1024 * 1024 * 1024
        )

        with patch("core.pinned_cli._connect_plex", return_value=mock_plex):
            handle_pin(cm, "888")

        out = capsys.readouterr().out
        assert "Pinned" in out
        tracker = _tracker_from_cm(cm)
        assert tracker.is_pinned("888")

    def test_pin_allowed_when_budget_unconfigured(self, tmp_path, capsys):
        """cache_limit empty / "0" / "none" → budget guard opt-out, matches the
        web behavior. Even a huge item slides through."""
        from core.pinned_cli import handle_pin
        cm = self._make_cm_with_budget(tmp_path, "")  # budget disabled

        mock_plex = MagicMock()
        mock_plex.fetchItem.return_value = _make_sized_movie(
            "999", "Dune Part Two", size_bytes=100 * 1024 * 1024 * 1024  # 100 GB
        )

        with patch("core.pinned_cli._connect_plex", return_value=mock_plex):
            handle_pin(cm, "999")

        tracker = _tracker_from_cm(cm)
        assert tracker.is_pinned("999")

    def test_pin_by_title_rejected_when_over_budget(self, tmp_path, capsys):
        from core.pinned_cli import handle_pin_by_title
        cm = self._make_cm_with_budget(tmp_path, "500MB")

        mock_plex = MagicMock()
        # search() is what handle_pin_by_title calls first for result listing.
        search_item = _make_sized_movie("777", "Oppenheimer", size_bytes=1024 * 1024 * 1024)
        mock_plex.search.return_value = [search_item]
        # fetchItem is what estimate_item_bytes calls during preflight.
        mock_plex.fetchItem.return_value = search_item

        with patch("core.pinned_cli._connect_plex", return_value=mock_plex), \
             patch("builtins.input", return_value="1"):
            with pytest.raises(SystemExit) as exc:
                handle_pin_by_title(cm, "Oppenheimer")

        assert exc.value.code == 1
        out = capsys.readouterr().out
        assert "exceed the cache budget" in out
        tracker = _tracker_from_cm(cm)
        assert not tracker.is_pinned("777")

    def test_percent_based_cache_limit_resolves_against_disk_total(self, tmp_path, capsys):
        """A "50%" cache_limit must be resolved against the cache drive's
        disk.total so CLI matches the web UI's percent handling."""
        from core.pinned_cli import handle_pin

        cm = _make_config_manager(tmp_path)
        cm.settings_data = {
            "cache_limit": "50%",
            "min_free_space": "",
            "plexcache_quota": "",
            "path_mappings": [
                {"enabled": True, "plex_path": "/data", "cache_path": str(tmp_path)},
            ],
        }

        mock_plex = MagicMock()
        # 2GB movie. 50% of a 1GB disk = 500MB → reject.
        mock_plex.fetchItem.return_value = _make_sized_movie(
            "123", "Interstellar", size_bytes=2 * 1024 * 1024 * 1024
        )

        # Stub get_disk_usage so "50%" resolves to 500MB.
        fake_disk = MagicMock()
        fake_disk.total = 1024 * 1024 * 1024  # 1 GB
        fake_disk.used = 0

        with patch("core.pinned_cli._connect_plex", return_value=mock_plex), \
             patch("core.system_utils.get_disk_usage", return_value=fake_disk):
            with pytest.raises(SystemExit) as exc:
                handle_pin(cm, "123")

        assert exc.value.code == 1
        assert "exceed the cache budget" in capsys.readouterr().out
