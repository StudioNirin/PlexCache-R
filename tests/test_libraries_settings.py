"""Tests for Libraries settings logic (migration, rebuild, auto-fill, toggle)."""

import json
import os
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

# Mock fcntl/apscheduler for Windows test compat
sys.modules.setdefault('fcntl', MagicMock())
sys.modules.setdefault('apscheduler', MagicMock())
sys.modules.setdefault('apscheduler.schedulers', MagicMock())
sys.modules.setdefault('apscheduler.schedulers.background', MagicMock())
sys.modules.setdefault('apscheduler.triggers', MagicMock())
sys.modules.setdefault('apscheduler.triggers.cron', MagicMock())
sys.modules.setdefault('apscheduler.triggers.interval', MagicMock())
sys.modules.setdefault('plexapi', MagicMock())
sys.modules.setdefault('plexapi.server', MagicMock())


@pytest.fixture
def tmp_settings(tmp_path):
    """Create a temporary settings file and return its path."""
    settings_file = tmp_path / "plexcache_settings.json"
    settings_file.write_text("{}", encoding="utf-8")
    return settings_file


@pytest.fixture
def settings_service(tmp_settings):
    """Create a SettingsService with a temporary settings file."""
    with patch("web.services.settings_service.SETTINGS_FILE", tmp_settings), \
         patch("web.services.settings_service.DATA_DIR", tmp_settings.parent), \
         patch("web.services.settings_service.PROJECT_ROOT", tmp_settings.parent):
        from web.services.settings_service import SettingsService
        service = SettingsService()
        return service


def _write_settings(settings_service, data):
    """Helper to write settings dict to the temp file."""
    with open(settings_service.settings_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)
    settings_service._cached_settings = None


class TestMigrateLinkPathMappings:
    """Tests for migrate_link_path_mappings_to_libraries()."""

    def test_migrate_links_mappings_to_sections(self, settings_service):
        """Verify plex_path matching sets section_id."""
        _write_settings(settings_service, {
            "PLEX_URL": "http://localhost:32400",
            "PLEX_TOKEN": "abc",
            "valid_sections": [],
            "path_mappings": [
                {"name": "TV Shows", "plex_path": "/data/tv/", "real_path": "/mnt/user/tv/",
                 "cache_path": "/mnt/cache/tv/", "cacheable": True, "enabled": True},
                {"name": "Movies", "plex_path": "/data/movies/", "real_path": "/mnt/user/movies/",
                 "cache_path": "/mnt/cache/movies/", "cacheable": True, "enabled": True},
            ]
        })

        mock_libraries = [
            {"id": 1, "title": "Movies", "type": "movie", "type_label": "Movies",
             "locations": ["/data/movies/"]},
            {"id": 2, "title": "TV Shows", "type": "show", "type_label": "TV Shows",
             "locations": ["/data/tv/"]},
        ]

        with patch.object(settings_service, 'get_plex_libraries', return_value=mock_libraries):
            result = settings_service.migrate_link_path_mappings_to_libraries()

        assert result is True

        raw = settings_service._load_raw()
        mappings = raw["path_mappings"]
        assert mappings[0]["section_id"] == 2  # TV Shows -> section 2
        assert mappings[1]["section_id"] == 1  # Movies -> section 1
        assert sorted(raw["valid_sections"]) == [1, 2]

    def test_migrate_idempotent(self, settings_service):
        """Skips if any mapping already has section_id."""
        _write_settings(settings_service, {
            "PLEX_URL": "http://localhost:32400",
            "PLEX_TOKEN": "abc",
            "valid_sections": [1],
            "path_mappings": [
                {"name": "Movies", "plex_path": "/data/movies/", "real_path": "/mnt/user/movies/",
                 "cache_path": "/mnt/cache/movies/", "cacheable": True, "enabled": True,
                 "section_id": 1},
            ]
        })

        result = settings_service.migrate_link_path_mappings_to_libraries()
        assert result is False


class TestRebuildValidSections:
    """Tests for _rebuild_valid_sections()."""

    def test_rebuild_valid_sections(self, settings_service):
        """Enabled mappings with section_id produce correct list."""
        raw = {
            "path_mappings": [
                {"section_id": 3, "enabled": True},
                {"section_id": 1, "enabled": True},
                {"section_id": 3, "enabled": True},  # duplicate
            ]
        }
        settings_service._rebuild_valid_sections(raw)
        assert raw["valid_sections"] == [1, 3]

    def test_rebuild_ignores_disabled(self, settings_service):
        """Disabled mappings are excluded from valid_sections."""
        raw = {
            "path_mappings": [
                {"section_id": 1, "enabled": True},
                {"section_id": 2, "enabled": False},
                {"section_id": 3, "enabled": True},
            ]
        }
        settings_service._rebuild_valid_sections(raw)
        assert raw["valid_sections"] == [1, 3]

    def test_rebuild_handles_no_section_id(self, settings_service):
        """Mappings without section_id are ignored."""
        raw = {
            "path_mappings": [
                {"name": "Custom", "enabled": True},
                {"section_id": 5, "enabled": True},
            ]
        }
        settings_service._rebuild_valid_sections(raw)
        assert raw["valid_sections"] == [5]


class TestAutoFillMapping:
    """Tests for auto_fill_mapping()."""

    def test_auto_fill_mapping_docker_pattern(self, settings_service):
        """/data/tv/ → /mnt/user/tv/ real path translation."""
        library = {"id": 2, "title": "TV Shows", "type": "show", "type_label": "TV Shows",
                   "locations": ["/data/tv/"]}
        settings = {"cache_dir": "/mnt/cache"}

        result = settings_service.auto_fill_mapping(library, "/data/tv/", settings)

        assert result["plex_path"] == "/data/tv/"
        assert result["real_path"] == "/mnt/user/tv/"
        assert result["section_id"] == 2
        assert result["enabled"] is True
        assert result["cacheable"] is True

    def test_auto_fill_mapping_cache_path(self, settings_service):
        """Cache path is generated from folder name + cache_dir."""
        library = {"id": 1, "title": "Movies", "type": "movie", "type_label": "Movies",
                   "locations": ["/data/movies/"]}
        settings = {"cache_dir": "/mnt/cache"}

        result = settings_service.auto_fill_mapping(library, "/data/movies/", settings)

        assert result["cache_path"] == "/mnt/cache/movies/"
        assert result["name"] == "Movies"

    def test_auto_fill_mapping_media_prefix(self, settings_service):
        """/media/ prefix also maps to /mnt/user/."""
        library = {"id": 3, "title": "Music", "type": "artist", "type_label": "Music",
                   "locations": ["/media/music/"]}
        settings = {"cache_dir": "/mnt/cache"}

        result = settings_service.auto_fill_mapping(library, "/media/music/", settings)

        assert result["real_path"] == "/mnt/user/music/"

    def test_auto_fill_mapping_no_trailing_slash(self, settings_service):
        """Plex paths without trailing slash get one added."""
        library = {"id": 1, "title": "Movies", "type": "movie", "type_label": "Movies",
                   "locations": ["/data/movies"]}
        settings = {"cache_dir": "/mnt/cache"}

        result = settings_service.auto_fill_mapping(library, "/data/movies", settings)

        assert result["plex_path"] == "/data/movies/"


class TestToggleLibrary:
    """Tests for library toggle behavior via settings service methods."""

    def test_toggle_on_creates_mappings(self, settings_service):
        """Toggle with no existing mappings should auto-create them."""
        _write_settings(settings_service, {
            "PLEX_URL": "http://localhost:32400",
            "PLEX_TOKEN": "abc",
            "valid_sections": [],
            "path_mappings": [],
            "cache_dir": "/mnt/cache",
        })

        mock_libraries = [
            {"id": 1, "title": "Movies", "type": "movie", "type_label": "Movies",
             "locations": ["/data/movies/"]},
        ]

        with patch.object(settings_service, 'get_plex_libraries', return_value=mock_libraries):
            # Simulate toggle ON: auto-create mapping
            raw = settings_service._load_raw()
            mappings = raw.get("path_mappings", [])
            library = mock_libraries[0]

            for loc in library.get("locations", []):
                new_mapping = settings_service.auto_fill_mapping(library, loc, raw)
                mappings.append(new_mapping)

            raw["path_mappings"] = mappings
            settings_service._rebuild_valid_sections(raw)
            settings_service._save_raw(raw)

        raw = settings_service._load_raw()
        assert len(raw["path_mappings"]) == 1
        assert raw["path_mappings"][0]["section_id"] == 1
        assert raw["path_mappings"][0]["enabled"] is True
        assert raw["valid_sections"] == [1]

    def test_toggle_off_disables_mappings(self, settings_service):
        """Toggle OFF sets enabled=false, doesn't delete."""
        _write_settings(settings_service, {
            "PLEX_URL": "http://localhost:32400",
            "PLEX_TOKEN": "abc",
            "valid_sections": [1],
            "path_mappings": [
                {"name": "Movies", "plex_path": "/data/movies/", "real_path": "/mnt/user/movies/",
                 "cache_path": "/mnt/cache/movies/", "cacheable": True, "enabled": True,
                 "section_id": 1},
            ],
            "cache_dir": "/mnt/cache",
        })

        raw = settings_service._load_raw()
        mappings = raw["path_mappings"]

        # Toggle OFF: disable all mappings with section_id 1
        for m in mappings:
            if m.get("section_id") == 1:
                m["enabled"] = False

        raw["path_mappings"] = mappings
        settings_service._rebuild_valid_sections(raw)
        settings_service._save_raw(raw)

        raw = settings_service._load_raw()
        assert len(raw["path_mappings"]) == 1  # Not deleted
        assert raw["path_mappings"][0]["enabled"] is False
        assert raw["valid_sections"] == []  # Removed from valid_sections
