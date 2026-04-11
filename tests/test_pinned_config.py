"""Tests for pinned-media config wiring.

Covers:
- PlexConfig.pinned_preferred_resolution default + dataclass field
- ConfigManager.get_pinned_media_file() path resolution
- SettingsService get/save round-trip through the Cache settings endpoint
"""

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Mock optional deps before importing core modules
sys.modules.setdefault('fcntl', MagicMock())
sys.modules.setdefault('apscheduler', MagicMock())
sys.modules.setdefault('apscheduler.schedulers', MagicMock())
sys.modules.setdefault('apscheduler.schedulers.background', MagicMock())
sys.modules.setdefault('apscheduler.triggers', MagicMock())
sys.modules.setdefault('apscheduler.triggers.cron', MagicMock())
sys.modules.setdefault('apscheduler.triggers.interval', MagicMock())
sys.modules.setdefault('plexapi', MagicMock())
sys.modules.setdefault('plexapi.server', MagicMock())


class TestPlexConfigDataclass:
    def test_default_is_highest(self):
        from core.config import PlexConfig
        config = PlexConfig()
        assert config.pinned_preferred_resolution == "highest"

    def test_field_is_writable(self):
        from core.config import PlexConfig
        config = PlexConfig()
        config.pinned_preferred_resolution = "4k"
        assert config.pinned_preferred_resolution == "4k"

    def test_field_included_in_dataclass(self):
        from dataclasses import fields
        from core.config import PlexConfig
        field_names = {f.name for f in fields(PlexConfig)}
        assert "pinned_preferred_resolution" in field_names


class TestGetPinnedMediaFile:
    def test_returns_data_folder_path(self, tmp_path):
        """get_pinned_media_file() returns data_folder / pinned_media.json."""
        from core.config import ConfigManager
        cm = object.__new__(ConfigManager)
        cm.paths = MagicMock()
        cm.paths.data_folder = str(tmp_path)
        result = cm.get_pinned_media_file()
        assert result == tmp_path / "pinned_media.json"

    def test_same_directory_as_other_trackers(self, tmp_path):
        """Pinned media file lives alongside ondeck/watchlist trackers."""
        from core.config import ConfigManager
        cm = object.__new__(ConfigManager)
        cm.paths = MagicMock()
        cm.paths.data_folder = str(tmp_path)
        pinned = cm.get_pinned_media_file()
        ondeck = cm.get_ondeck_tracker_file()
        watchlist = cm.get_watchlist_tracker_file()
        assert pinned.parent == ondeck.parent == watchlist.parent


@pytest.fixture
def tmp_settings(tmp_path):
    settings_file = tmp_path / "plexcache_settings.json"
    settings_file.write_text("{}", encoding="utf-8")
    return settings_file


@pytest.fixture
def settings_service(tmp_settings):
    with patch("web.services.settings_service.SETTINGS_FILE", tmp_settings), \
         patch("web.services.settings_service.DATA_DIR", tmp_settings.parent):
        from web.services.settings_service import SettingsService
        return SettingsService()


def _write_settings(service, data):
    with open(service.settings_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)
    service._cached_settings = None


class TestSettingsServiceRoundTrip:
    def test_get_default_is_highest(self, settings_service):
        _write_settings(settings_service, {})
        result = settings_service.get_cache_settings()
        assert result["pinned_preferred_resolution"] == "highest"

    def test_get_existing_value(self, settings_service):
        _write_settings(settings_service, {"pinned_preferred_resolution": "4k"})
        result = settings_service.get_cache_settings()
        assert result["pinned_preferred_resolution"] == "4k"

    def test_save_persists_value(self, settings_service):
        _write_settings(settings_service, {})
        settings_service.save_cache_settings({"pinned_preferred_resolution": "1080p"})

        with open(settings_service.settings_file, "r") as f:
            raw = json.load(f)
        assert raw["pinned_preferred_resolution"] == "1080p"

    def test_round_trip(self, settings_service):
        _write_settings(settings_service, {})
        settings_service.save_cache_settings({"pinned_preferred_resolution": "lowest"})
        result = settings_service.get_cache_settings()
        assert result["pinned_preferred_resolution"] == "lowest"

    def test_save_does_not_clobber_other_fields(self, settings_service):
        _write_settings(settings_service, {
            "cache_retention_hours": 24,
            "pinned_preferred_resolution": "first",
        })
        settings_service.save_cache_settings({"pinned_preferred_resolution": "highest"})
        with open(settings_service.settings_file, "r") as f:
            raw = json.load(f)
        assert raw["pinned_preferred_resolution"] == "highest"
        assert raw["cache_retention_hours"] == 24
