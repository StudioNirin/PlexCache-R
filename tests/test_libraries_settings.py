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
         patch("web.services.settings_service.DATA_DIR", tmp_settings.parent):
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
        """/data/tv/ → /mnt/user0/tv/ real path translation (array-direct default)."""
        library = {"id": 2, "title": "TV Shows", "type": "show", "type_label": "TV Shows",
                   "locations": ["/data/tv/"]}
        settings = {"cache_dir": "/mnt/cache"}

        result = settings_service.auto_fill_mapping(library, "/data/tv/", settings)

        assert result["plex_path"] == "/data/tv/"
        assert result["real_path"] == "/mnt/user0/tv/"
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
        """/media/ prefix also maps to /mnt/user0/ (array-direct default)."""
        library = {"id": 3, "title": "Music", "type": "artist", "type_label": "Music",
                   "locations": ["/media/music/"]}
        settings = {"cache_dir": "/mnt/cache"}

        result = settings_service.auto_fill_mapping(library, "/media/music/", settings)

        assert result["real_path"] == "/mnt/user0/music/"

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


class TestDetectPathMappingHealthIssues:
    """Tests for detect_path_mapping_health_issues() (issue #136 regression)."""

    def test_healthy_config_returns_empty(self, settings_service):
        _write_settings(settings_service, {
            "path_mappings": [
                {"name": "Movies", "plex_path": "/data/Movies/",
                 "real_path": "/mnt/user/Media/Movies/",
                 "cache_path": "/mnt/cache/Media/Movies/",
                 "cacheable": True, "enabled": True, "section_id": 1},
            ],
        })
        assert settings_service.detect_path_mapping_health_issues() == []

    def test_detects_bare_cache_root(self, settings_service):
        """The 'Default (migrated)' case from issue #136."""
        _write_settings(settings_service, {
            "path_mappings": [
                {"name": "Default (migrated)", "plex_path": "/data/",
                 "real_path": "/mnt/user/Media/",
                 "cache_path": "/mnt/cache/",
                 "cacheable": True, "enabled": True, "section_id": None},
            ],
        })
        issues = settings_service.detect_path_mapping_health_issues()
        assert len(issues) == 1
        assert issues[0]["issue_type"] == "cache_root"
        assert issues[0]["mapping_name"] == "Default (migrated)"
        assert "/mnt/cache/" in issues[0]["message"]

    def test_detects_fuse_cache_path(self, settings_service):
        """The 'Movies → /mnt/user/...' case from issue #136."""
        _write_settings(settings_service, {
            "path_mappings": [
                {"name": "Movies", "plex_path": "/data/Movies/",
                 "real_path": "/mnt/user/Media/Movies/",
                 "cache_path": "/mnt/user/Media/Movies/",
                 "cacheable": True, "enabled": True, "section_id": 4},
            ],
        })
        issues = settings_service.detect_path_mapping_health_issues()
        assert len(issues) == 1
        assert issues[0]["issue_type"] == "fuse_cache_path"
        assert issues[0]["mapping_name"] == "Movies"

    def test_mnt_user0_not_flagged_as_fuse(self, settings_service):
        """`/mnt/user0/` is the array-direct path, not FUSE — don't flag it."""
        _write_settings(settings_service, {
            "path_mappings": [
                {"name": "Edge", "plex_path": "/data/X/",
                 "real_path": "/mnt/user0/X/",
                 "cache_path": "/mnt/user0/X/",
                 "cacheable": True, "enabled": True, "section_id": 99},
            ],
        })
        assert settings_service.detect_path_mapping_health_issues() == []

    def test_disabled_mappings_skipped(self, settings_service):
        _write_settings(settings_service, {
            "path_mappings": [
                {"name": "Default (migrated)", "plex_path": "/data/",
                 "real_path": "/mnt/user/Media/",
                 "cache_path": "/mnt/cache/",
                 "cacheable": True, "enabled": False, "section_id": None},
            ],
        })
        assert settings_service.detect_path_mapping_health_issues() == []

    def test_multiple_issues_reported(self, settings_service):
        """Exact replica of the issue #136 reporter's config."""
        _write_settings(settings_service, {
            "path_mappings": [
                {"name": "Default (migrated)", "plex_path": "/data/",
                 "real_path": "/mnt/user/Media/",
                 "cache_path": "/mnt/cache/",
                 "cacheable": True, "enabled": True, "section_id": None},
                {"name": "Novelas", "plex_path": "/data/Novelas/",
                 "real_path": "/mnt/user/Media/Novelas/",
                 "cache_path": "/mnt/cache/Media/Novelas/",
                 "cacheable": True, "enabled": True, "section_id": 13},
                {"name": "TV Shows", "plex_path": "/data/TV Shows/",
                 "real_path": "/mnt/user/Media/TV Shows/",
                 "cache_path": "/mnt/cache/Media/TV Shows/",
                 "cacheable": True, "enabled": True, "section_id": 3},
                {"name": "Movies", "plex_path": "/data/Movies/",
                 "real_path": "/mnt/user/Media/Movies/",
                 "cache_path": "/mnt/user/Media/Movies/",
                 "cacheable": True, "enabled": True, "section_id": 4},
            ],
        })
        issues = settings_service.detect_path_mapping_health_issues()
        assert len(issues) == 2
        issue_types = {i["issue_type"] for i in issues}
        assert issue_types == {"cache_root", "fuse_cache_path"}

    def test_empty_cache_path_ignored(self, settings_service):
        _write_settings(settings_service, {
            "path_mappings": [
                {"name": "Passthrough", "plex_path": "/data/X/",
                 "real_path": "/mnt/user/X/",
                 "cache_path": "",
                 "cacheable": True, "enabled": True, "section_id": 1},
            ],
        })
        assert settings_service.detect_path_mapping_health_issues() == []


class TestWarnCachePath:
    """Tests for SettingsService.warn_cache_path() (issue #136).

    warn_cache_path is advisory only — it returns a human-readable warning
    string for risky values but never blocks. Callers log the warning and
    continue. Some valid configs (dedicated cache drive with flat layout,
    containers without /mnt/cache mounted) legitimately use these values.
    """

    def test_valid_cache_subdir_returns_none(self, settings_service):
        assert settings_service.warn_cache_path("/mnt/cache/Media/Movies/") is None

    def test_valid_cache_subdir_no_trailing_slash(self, settings_service):
        assert settings_service.warn_cache_path("/mnt/cache/Movies") is None

    def test_empty_returns_none(self, settings_service):
        assert settings_service.warn_cache_path("") is None
        assert settings_service.warn_cache_path(None) is None

    def test_warns_bare_cache_root(self, settings_service):
        warning = settings_service.warn_cache_path("/mnt/cache/")
        assert warning is not None
        assert "bare cache drive root" in warning
        # Warning should explain the risk AND note the legitimate use case
        assert "can be ignored" in warning or "If you really" in warning or "that's not what you want" in warning

    def test_warns_bare_cache_root_no_slash(self, settings_service):
        warning = settings_service.warn_cache_path("/mnt/cache")
        assert warning is not None
        assert "bare cache drive root" in warning

    def test_warns_fuse_path_with_suggestion(self, settings_service):
        warning = settings_service.warn_cache_path("/mnt/user/Media/Movies/")
        assert warning is not None
        assert "FUSE" in warning
        assert "/mnt/cache/Media/Movies/" in warning  # suggestion

    def test_allows_mnt_user0(self, settings_service):
        """/mnt/user0/ is array-direct, not FUSE — no warning."""
        assert settings_service.warn_cache_path("/mnt/user0/Media/Movies/") is None

    def test_allows_non_mnt_paths(self, settings_service):
        """Custom mount points (e.g. non-Unraid) aren't our concern."""
        assert settings_service.warn_cache_path("/custom/mount/media/") is None


class TestAutoFillMappingCachePath:
    """Tests for the auto_fill_mapping cache_path tightening (issue #136)."""

    def test_auto_fill_ignores_fuse_cache_dir(self, settings_service):
        """If settings.cache_dir is a FUSE path, fall back to /mnt/cache."""
        library = {"id": 1, "title": "Movies", "type": "movie", "locations": ["/data/Movies"]}
        settings = {"cache_dir": "/mnt/user/Media"}
        result = settings_service.auto_fill_mapping(library, "/data/Movies/", settings)
        assert result["cache_path"] == "/mnt/cache/Movies/"
        assert not result["cache_path"].startswith("/mnt/user/")

    def test_auto_fill_ignores_mnt_user_cache_dir(self, settings_service):
        library = {"id": 1, "title": "Movies", "type": "movie", "locations": ["/data/Movies"]}
        settings = {"cache_dir": "/mnt/user"}
        result = settings_service.auto_fill_mapping(library, "/data/Movies/", settings)
        assert result["cache_path"].startswith("/mnt/cache/")
