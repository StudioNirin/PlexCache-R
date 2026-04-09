"""Tests for legacy→path_mappings migration (core/config.py).

Regression coverage for issue #136: the migration previously copied the
legacy ``cache_dir`` verbatim into ``path_mappings[0].cache_path``, which
for users with ``cache_dir="/mnt/cache/"`` caused the audit to walk the
entire cache drive. The migration now mirrors the ``real_source`` subpath.
"""

from core.config import _derive_migrated_cache_path, migrate_path_settings


class TestDeriveMigratedCachePath:
    """Unit tests for the subdir-mirroring helper."""

    def test_bare_cache_root_with_media_subdir(self):
        """The #136 case: real_source deeper than cache_dir."""
        result = _derive_migrated_cache_path(
            real_source="/mnt/user/Media/",
            cache_dir="/mnt/cache/",
        )
        assert result == "/mnt/cache/Media/"

    def test_bare_cache_root_no_trailing_slash(self):
        result = _derive_migrated_cache_path(
            real_source="/mnt/user/Media",
            cache_dir="/mnt/cache",
        )
        assert result == "/mnt/cache/Media"

    def test_multi_level_subdir(self):
        result = _derive_migrated_cache_path(
            real_source="/mnt/user/Videos/Movies/",
            cache_dir="/mnt/cache/",
        )
        assert result == "/mnt/cache/Videos/Movies/"

    def test_user_already_specific_cache_path_unchanged(self):
        """If cache_dir already names a specific subdir, leave it alone."""
        result = _derive_migrated_cache_path(
            real_source="/mnt/user/Movies/",
            cache_dir="/mnt/cache/Movies/",
        )
        assert result == "/mnt/cache/Movies/"

    def test_non_mnt_user_real_source_unchanged(self):
        """Don't touch non-/mnt/user/ paths — migration has nothing to mirror."""
        result = _derive_migrated_cache_path(
            real_source="/some/custom/path/",
            cache_dir="/other/cache/",
        )
        assert result == "/other/cache/"

    def test_empty_real_source_returns_cache_dir(self):
        assert _derive_migrated_cache_path("", "/mnt/cache/") == "/mnt/cache/"

    def test_empty_cache_dir_returns_cache_dir(self):
        assert _derive_migrated_cache_path("/mnt/user/Media/", "") == ""


class TestMigratePathSettings:
    """End-to-end tests for migrate_path_settings()."""

    def test_issue_136_scenario(self):
        """Real-world config that caused the dashboard stall."""
        settings = {
            "plex_source": "/data/",
            "real_source": "/mnt/user/Media/",
            "cache_dir": "/mnt/cache/",
        }
        migrated, was_migrated = migrate_path_settings(settings)
        assert was_migrated is True
        assert len(migrated["path_mappings"]) == 1
        mapping = migrated["path_mappings"][0]
        assert mapping["name"] == "Default (migrated)"
        assert mapping["plex_path"] == "/data/"
        assert mapping["real_path"] == "/mnt/user/Media/"
        assert mapping["cache_path"] == "/mnt/cache/Media/"

    def test_already_migrated_is_noop(self):
        settings = {
            "path_mappings": [{"name": "existing", "plex_path": "/a", "real_path": "/b"}],
            "plex_source": "/data/",
            "real_source": "/mnt/user/Media/",
            "cache_dir": "/mnt/cache/",
        }
        migrated, was_migrated = migrate_path_settings(settings)
        assert was_migrated is False
        assert len(migrated["path_mappings"]) == 1
        assert migrated["path_mappings"][0]["name"] == "existing"

    def test_missing_legacy_settings_is_noop(self):
        settings = {"plex_url": "http://localhost:32400"}
        migrated, was_migrated = migrate_path_settings(settings)
        assert was_migrated is False
        assert "path_mappings" not in migrated

    def test_preserves_other_keys(self):
        settings = {
            "plex_source": "/data/",
            "real_source": "/mnt/user/Media/",
            "cache_dir": "/mnt/cache/",
            "plex_token": "secret",
            "watchlist_toggle": True,
        }
        migrated, _ = migrate_path_settings(settings)
        assert migrated["plex_token"] == "secret"
        assert migrated["watchlist_toggle"] is True

    def test_specific_cache_dir_preserved(self):
        """A user who already pointed cache_dir at a subdir should see it preserved."""
        settings = {
            "plex_source": "/data/Movies/",
            "real_source": "/mnt/user/Movies/",
            "cache_dir": "/mnt/cache/Movies/",
        }
        _, was_migrated = migrate_path_settings(settings)
        assert was_migrated is True
        assert settings["path_mappings"][0]["cache_path"] == "/mnt/cache/Movies/"
