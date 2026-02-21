"""
Tests for web/services/maintenance_service.py

Covers:
- _get_paths() conversion of /mnt/user/ to /mnt/user0/ for array paths
- sync_to_array() logic (backup exists vs no backup)
- protect_with_backup() skipping copy when backup already exists
- _check_array_duplicate() correctness
- _cache_to_array_path() translation
- Path translation between host and container paths
"""

import json
import os
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock, PropertyMock

import pytest

# conftest.py handles fcntl mock and sys.path setup


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

MOCK_SETTINGS = {
    "path_mappings": [
        {
            "name": "Movies",
            "cache_path": "/mnt/cache/media/Movies",
            "real_path": "/mnt/user/media/Movies",
            "host_cache_path": "/mnt/cache_downloads/media/Movies",
            "cacheable": True,
            "enabled": True,
        },
        {
            "name": "TV",
            "cache_path": "/mnt/cache/media/TV",
            "real_path": "/mnt/user/media/TV",
            "host_cache_path": "/mnt/cache_downloads/media/TV",
            "cacheable": True,
            "enabled": True,
        },
    ]
}


def _make_service(tmp_path, settings=None):
    """Create a MaintenanceService wired to tmp_path directories.

    Patches web.config constants so the service points at test directories,
    then returns the service instance.
    """
    if settings is None:
        settings = MOCK_SETTINGS

    settings_file = tmp_path / "plexcache_settings.json"
    settings_file.write_text(json.dumps(settings), encoding="utf-8")

    exclude_file = tmp_path / "plexcache_cached_files.txt"
    timestamps_file = tmp_path / "data" / "timestamps.json"
    timestamps_file.parent.mkdir(parents=True, exist_ok=True)

    with patch("web.services.maintenance_service.SETTINGS_FILE", settings_file), \
         patch("web.services.maintenance_service.CONFIG_DIR", tmp_path), \
         patch("web.services.maintenance_service.DATA_DIR", tmp_path / "data"):
        from web.services.maintenance_service import MaintenanceService
        svc = MaintenanceService()

    # Override the paths that __init__ resolved from the (now un-patched) constants
    svc.settings_file = settings_file
    svc.exclude_file = exclude_file
    svc.timestamps_file = timestamps_file

    return svc


# ============================================================================
# _get_paths() tests
# ============================================================================

class TestGetPaths:
    """Verify _get_paths converts /mnt/user/ -> /mnt/user0/ for array dirs."""

    def test_converts_user_to_user0(self, tmp_path):
        svc = _make_service(tmp_path)
        cache_dirs, array_dirs = svc._get_paths()

        assert cache_dirs == [
            "/mnt/cache/media/Movies",
            "/mnt/cache/media/TV",
        ]
        assert array_dirs == [
            "/mnt/user0/media/Movies",
            "/mnt/user0/media/TV",
        ]

    def test_disabled_mapping_excluded(self, tmp_path):
        settings = {
            "path_mappings": [
                {
                    "name": "Movies",
                    "cache_path": "/mnt/cache/media/Movies",
                    "real_path": "/mnt/user/media/Movies",
                    "cacheable": True,
                    "enabled": False,
                },
                {
                    "name": "TV",
                    "cache_path": "/mnt/cache/media/TV",
                    "real_path": "/mnt/user/media/TV",
                    "cacheable": True,
                    "enabled": True,
                },
            ]
        }
        svc = _make_service(tmp_path, settings)
        cache_dirs, array_dirs = svc._get_paths()

        assert len(cache_dirs) == 1
        assert cache_dirs[0] == "/mnt/cache/media/TV"
        assert array_dirs[0] == "/mnt/user0/media/TV"

    def test_non_cacheable_mapping_excluded(self, tmp_path):
        settings = {
            "path_mappings": [
                {
                    "name": "Music",
                    "cache_path": "/mnt/cache/media/Music",
                    "real_path": "/mnt/user/media/Music",
                    "cacheable": False,
                    "enabled": True,
                },
            ]
        }
        svc = _make_service(tmp_path, settings)
        cache_dirs, array_dirs = svc._get_paths()

        assert cache_dirs == []
        assert array_dirs == []

    def test_legacy_single_path_mode(self, tmp_path):
        settings = {
            "cache_dir": "/mnt/cache/downloads",
            "real_source": "/mnt/user/downloads",
            "nas_library_folders": ["Movies", "TV"],
        }
        svc = _make_service(tmp_path, settings)
        cache_dirs, array_dirs = svc._get_paths()

        assert len(cache_dirs) == 2
        assert os.path.join("/mnt/cache/downloads", "Movies") in cache_dirs
        assert os.path.join("/mnt/user0/downloads", "TV") in array_dirs

    def test_caches_results(self, tmp_path):
        svc = _make_service(tmp_path)
        cache1, array1 = svc._get_paths()
        cache2, array2 = svc._get_paths()
        # Internal lists should be the same objects (cached)
        assert cache1 is cache2
        assert array1 is array2

    def test_strips_trailing_slashes(self, tmp_path):
        settings = {
            "path_mappings": [
                {
                    "name": "Movies",
                    "cache_path": "/mnt/cache/media/Movies/",
                    "real_path": "/mnt/user/media/Movies/",
                    "cacheable": True,
                    "enabled": True,
                },
            ]
        }
        svc = _make_service(tmp_path, settings)
        cache_dirs, array_dirs = svc._get_paths()

        assert not cache_dirs[0].endswith("/")
        assert not array_dirs[0].endswith("/")


# ============================================================================
# _cache_to_array_path() tests
# ============================================================================

class TestCacheToArrayPath:

    def test_translates_cache_to_array(self, tmp_path):
        svc = _make_service(tmp_path)
        result = svc._cache_to_array_path("/mnt/cache/media/Movies/Film.mkv")
        assert result == "/mnt/user0/media/Movies/Film.mkv"

    def test_returns_none_for_unknown_path(self, tmp_path):
        svc = _make_service(tmp_path)
        result = svc._cache_to_array_path("/totally/unknown/path/file.mkv")
        assert result is None

    def test_nested_subdirectory(self, tmp_path):
        svc = _make_service(tmp_path)
        result = svc._cache_to_array_path(
            "/mnt/cache/media/TV/Breaking Bad/Season 1/S01E01.mkv"
        )
        assert result == "/mnt/user0/media/TV/Breaking Bad/Season 1/S01E01.mkv"


# ============================================================================
# _check_array_duplicate() tests
# ============================================================================

class TestCheckArrayDuplicate:

    def test_duplicate_exists(self, tmp_path):
        svc = _make_service(tmp_path)

        array_file = "/mnt/user0/media/Movies/Film.mkv"
        with patch("os.path.exists", return_value=True):
            has_dup, path = svc._check_array_duplicate(
                "/mnt/cache/media/Movies/Film.mkv"
            )
        assert has_dup is True
        assert path == array_file

    def test_no_duplicate(self, tmp_path):
        svc = _make_service(tmp_path)
        with patch("os.path.exists", return_value=False):
            has_dup, path = svc._check_array_duplicate(
                "/mnt/cache/media/Movies/Film.mkv"
            )
        assert has_dup is False

    def test_unknown_mapping_returns_none(self, tmp_path):
        svc = _make_service(tmp_path)
        has_dup, path = svc._check_array_duplicate("/unknown/path/file.mkv")
        assert has_dup is False
        assert path is None


# ============================================================================
# _check_plexcached_backup() tests
# ============================================================================

class TestCheckPlexcachedBackup:

    def test_backup_exists(self, tmp_path):
        svc = _make_service(tmp_path)
        with patch("os.path.exists", return_value=True):
            has_backup, path = svc._check_plexcached_backup(
                "/mnt/cache/media/Movies/Film.mkv"
            )
        assert has_backup is True
        assert path == "/mnt/user0/media/Movies/Film.mkv.plexcached"

    def test_no_backup(self, tmp_path):
        svc = _make_service(tmp_path)
        with patch("os.path.exists", return_value=False):
            has_backup, path = svc._check_plexcached_backup(
                "/mnt/cache/media/Movies/Film.mkv"
            )
        assert has_backup is False


# ============================================================================
# sync_to_array() tests
# ============================================================================

class TestSyncToArray:
    """Verify sync_to_array behaviour for the three scenarios:
    1. .plexcached backup exists -> restore it, delete cache copy
    2. Array duplicate exists -> just delete cache copy
    3. No backup/duplicate -> copy to array, verify, then delete cache
    """

    def test_dry_run_counts_all(self, tmp_path):
        svc = _make_service(tmp_path)
        paths = [
            "/mnt/cache/media/Movies/A.mkv",
            "/mnt/cache/media/Movies/B.mkv",
        ]
        result = svc.sync_to_array(paths, dry_run=True)
        assert result.success is True
        assert result.affected_count == 2
        assert "Would move" in result.message

    def test_empty_paths_returns_failure(self, tmp_path):
        svc = _make_service(tmp_path)
        result = svc.sync_to_array([], dry_run=False)
        assert result.success is False

    def test_unknown_mapping_returns_error(self, tmp_path):
        svc = _make_service(tmp_path)
        result = svc.sync_to_array(["/unknown/file.mkv"], dry_run=False)
        assert result.success is False
        assert len(result.errors) == 1
        assert "Unknown path mapping" in result.errors[0]

    @patch("os.path.exists")
    @patch("os.rename")
    @patch("os.remove")
    def test_restores_plexcached_and_deletes_cache(
        self, mock_remove, mock_rename, mock_exists, tmp_path
    ):
        """When .plexcached backup exists, rename it back and delete cache copy."""
        svc = _make_service(tmp_path)

        # os.path.exists calls:
        #   _check_plexcached_backup -> plexcached_path exists? YES
        #   _check_array_duplicate -> array_path exists? NO
        #   original_array_path exists (redundant check)? NO
        #   cache_path exists (before remove)? YES
        def exists_side_effect(path):
            if path.endswith(".plexcached"):
                return True
            if path == "/mnt/cache/media/Movies/Film.mkv":
                return True
            return False

        mock_exists.side_effect = exists_side_effect

        result = svc.sync_to_array(
            ["/mnt/cache/media/Movies/Film.mkv"], dry_run=False
        )

        assert result.success is True
        assert result.affected_count == 1
        # .plexcached renamed to original
        mock_rename.assert_called_once_with(
            "/mnt/user0/media/Movies/Film.mkv.plexcached",
            "/mnt/user0/media/Movies/Film.mkv",
        )
        # cache copy deleted
        mock_remove.assert_called_once_with("/mnt/cache/media/Movies/Film.mkv")

    @patch("os.path.exists")
    @patch("os.remove")
    def test_duplicate_on_array_just_deletes_cache(
        self, mock_remove, mock_exists, tmp_path
    ):
        """When array duplicate exists (no .plexcached), just delete cache."""
        svc = _make_service(tmp_path)

        def exists_side_effect(path):
            if path.endswith(".plexcached"):
                return False
            # Array duplicate exists, cache exists
            return True

        mock_exists.side_effect = exists_side_effect

        result = svc.sync_to_array(
            ["/mnt/cache/media/Movies/Film.mkv"], dry_run=False
        )

        assert result.success is True
        assert result.affected_count == 1

    @patch("os.path.getsize")
    @patch("os.path.exists")
    @patch("os.remove")
    @patch("os.makedirs")
    def test_no_backup_copies_to_array_and_verifies(
        self, mock_makedirs, mock_remove, mock_exists, mock_getsize, tmp_path
    ):
        """No backup/duplicate: copy to array, verify size, then delete cache."""
        svc = _make_service(tmp_path)
        mock_copy = MagicMock()

        cache_path = "/mnt/cache/media/Movies/Film.mkv"
        array_path = "/mnt/user0/media/Movies/Film.mkv"

        def exists_side_effect(path):
            if path.endswith(".plexcached"):
                return False
            if path == array_path:
                return mock_copy.called
            if path == cache_path:
                return True
            return False

        mock_exists.side_effect = exists_side_effect
        mock_getsize.return_value = 5000  # same size for both

        with patch.object(svc, "_copy_with_progress", mock_copy):
            result = svc.sync_to_array([cache_path], dry_run=False)

        assert result.success is True
        assert result.affected_count == 1
        mock_copy.assert_called_once()
        mock_remove.assert_called_once_with(cache_path)

    @patch("os.path.getsize")
    @patch("os.path.exists")
    @patch("os.makedirs")
    def test_size_mismatch_does_not_delete_cache(
        self, mock_makedirs, mock_exists, mock_getsize, tmp_path
    ):
        """If copy produces a size mismatch, cache must NOT be deleted."""
        svc = _make_service(tmp_path)
        mock_copy = MagicMock()

        cache_path = "/mnt/cache/media/Movies/Film.mkv"
        array_path = "/mnt/user0/media/Movies/Film.mkv"

        def exists_side_effect(path):
            if path.endswith(".plexcached"):
                return False
            if path == array_path:
                return mock_copy.called
            if path == cache_path:
                return True
            return False

        mock_exists.side_effect = exists_side_effect
        # Different sizes!
        mock_getsize.side_effect = lambda p: 5000 if p == cache_path else 3000

        with patch.object(svc, "_copy_with_progress", mock_copy):
            result = svc.sync_to_array([cache_path], dry_run=False)

        # Should report the mismatch error, file not counted as affected
        assert result.affected_count == 0
        assert any("Size mismatch" in e for e in result.errors)


# ============================================================================
# protect_with_backup() tests
# ============================================================================

class TestProtectWithBackup:

    def test_dry_run_counts(self, tmp_path):
        svc = _make_service(tmp_path)
        result = svc.protect_with_backup(
            ["/mnt/cache/media/Movies/Film.mkv"], dry_run=True
        )
        assert result.success is True
        assert result.affected_count == 1

    def test_empty_paths(self, tmp_path):
        svc = _make_service(tmp_path)
        result = svc.protect_with_backup([], dry_run=False)
        assert result.success is False

    @patch("os.path.exists")
    def test_skips_copy_when_backup_already_exists(self, mock_exists, tmp_path):
        """When .plexcached backup already exists, skip the copy entirely."""
        svc = _make_service(tmp_path)
        # Create exclude file so appending works
        svc.exclude_file.touch()

        cache_path = "/mnt/cache/media/Movies/Film.mkv"
        plexcached_path = "/mnt/user0/media/Movies/Film.mkv.plexcached"

        mock_exists.side_effect = lambda p: p == plexcached_path or p == cache_path

        with patch("shutil.copy2") as mock_copy, \
             patch.object(svc, "_add_to_timestamps"):
            result = svc.protect_with_backup([cache_path], dry_run=False)

        # shutil.copy2 should NOT be called because backup already exists
        mock_copy.assert_not_called()
        assert result.success is True
        assert result.affected_count == 1

    @patch("os.path.exists")
    @patch("os.path.getsize")
    @patch("os.makedirs")
    def test_copies_when_no_backup(
        self, mock_makedirs, mock_getsize, mock_exists, tmp_path
    ):
        """When no .plexcached exists, copy to array and verify."""
        svc = _make_service(tmp_path)
        svc.exclude_file.touch()
        mock_copy = MagicMock()

        cache_path = "/mnt/cache/media/Movies/Film.mkv"
        plexcached_path = "/mnt/user0/media/Movies/Film.mkv.plexcached"

        def exists_side_effect(path):
            if path == plexcached_path:
                return mock_copy.called  # after copy, exists
            if path == cache_path:
                return True
            return False

        mock_exists.side_effect = exists_side_effect
        mock_getsize.return_value = 4096

        with patch.object(svc, "_copy_with_progress", mock_copy), \
             patch.object(svc, "_add_to_timestamps"):
            result = svc.protect_with_backup([cache_path], dry_run=False)

        mock_copy.assert_called_once()
        assert result.success is True

    @patch("os.path.exists")
    @patch("os.path.getsize")
    @patch("os.makedirs")
    def test_size_mismatch_removes_failed_backup(
        self, mock_makedirs, mock_getsize, mock_exists, tmp_path
    ):
        """Size mismatch after copy should remove the bad backup."""
        svc = _make_service(tmp_path)
        svc.exclude_file.touch()
        mock_copy = MagicMock()

        cache_path = "/mnt/cache/media/Movies/Film.mkv"
        plexcached_path = "/mnt/user0/media/Movies/Film.mkv.plexcached"

        def exists_side_effect(path):
            if path == plexcached_path:
                return mock_copy.called
            if path == cache_path:
                return True
            return False

        mock_exists.side_effect = exists_side_effect
        mock_getsize.side_effect = lambda p: 4096 if p == cache_path else 2048

        with patch("os.remove") as mock_remove, \
             patch.object(svc, "_copy_with_progress", mock_copy), \
             patch.object(svc, "_add_to_timestamps"):
            result = svc.protect_with_backup([cache_path], dry_run=False)

        # Should remove the bad backup
        mock_remove.assert_called_once_with(plexcached_path)
        assert result.affected_count == 0
        assert any("Copy verification failed" in e for e in result.errors)

    def test_unknown_mapping_returns_error(self, tmp_path):
        svc = _make_service(tmp_path)
        result = svc.protect_with_backup(
            ["/unknown/path/file.mkv"], dry_run=False
        )
        assert result.affected_count == 0
        assert any("Unknown path mapping" in e for e in result.errors)


# ============================================================================
# Path translation tests
# ============================================================================

class TestPathTranslation:

    def test_host_to_container(self, tmp_path):
        svc = _make_service(tmp_path)
        result = svc._translate_host_to_container_path(
            "/mnt/cache_downloads/media/Movies/Film.mkv"
        )
        assert result == "/mnt/cache/media/Movies/Film.mkv"

    def test_container_to_host(self, tmp_path):
        svc = _make_service(tmp_path)
        result = svc._translate_container_to_host_path(
            "/mnt/cache/media/Movies/Film.mkv"
        )
        assert result == "/mnt/cache_downloads/media/Movies/Film.mkv"

    def test_no_translation_needed(self, tmp_path):
        """When host_cache_path == cache_path, path is returned unchanged."""
        settings = {
            "path_mappings": [
                {
                    "name": "Movies",
                    "cache_path": "/mnt/cache/media/Movies",
                    "real_path": "/mnt/user/media/Movies",
                    "host_cache_path": "/mnt/cache/media/Movies",
                    "cacheable": True,
                    "enabled": True,
                },
            ]
        }
        svc = _make_service(tmp_path, settings)
        result = svc._translate_container_to_host_path(
            "/mnt/cache/media/Movies/Film.mkv"
        )
        # Same path returned because host == container
        assert result == "/mnt/cache/media/Movies/Film.mkv"

    def test_unrecognized_path_returned_unchanged(self, tmp_path):
        svc = _make_service(tmp_path)
        result = svc._translate_host_to_container_path("/totally/unknown/path")
        assert result == "/totally/unknown/path"


# ============================================================================
# format_bytes() tests (was _format_size, now consolidated in system_utils)
# ============================================================================

class TestFormatSize:

    def test_zero_bytes(self):
        from core.system_utils import format_bytes
        assert format_bytes(0) == "0 B"

    def test_bytes(self):
        from core.system_utils import format_bytes
        assert format_bytes(512) == "512 B"

    def test_kilobytes(self):
        from core.system_utils import format_bytes
        result = format_bytes(2048)
        assert "KB" in result

    def test_gigabytes(self):
        from core.system_utils import format_bytes
        result = format_bytes(5 * 1024 ** 3)
        assert "GB" in result

    def test_terabytes(self):
        from core.system_utils import format_bytes
        result = format_bytes(2 * 1024 ** 4)
        assert "TB" in result


# ============================================================================
# _should_skip_directory() tests
# ============================================================================

class TestShouldSkipDirectory:

    def test_hidden_directories_skipped(self, tmp_path):
        svc = _make_service(tmp_path)
        assert svc._should_skip_directory(".Trash") is True
        assert svc._should_skip_directory(".Recycle.Bin") is True

    def test_normal_directory_not_skipped(self, tmp_path):
        svc = _make_service(tmp_path)
        assert svc._should_skip_directory("Movies") is False

    def test_excluded_folder_from_settings(self, tmp_path):
        settings = {
            **MOCK_SETTINGS,
            "excluded_folders": ["@eaDir", "tmp"],
        }
        svc = _make_service(tmp_path, settings)
        assert svc._should_skip_directory("@eaDir") is True
        assert svc._should_skip_directory("tmp") is True
        assert svc._should_skip_directory("Movies") is False


# ============================================================================
# add_to_exclude() tests
# ============================================================================

class TestAddToExclude:

    def test_dry_run(self, tmp_path):
        svc = _make_service(tmp_path)
        result = svc.add_to_exclude(
            ["/mnt/cache/media/Movies/Film.mkv"], dry_run=True
        )
        assert result.success is True
        assert result.affected_count == 1
        assert "Would add" in result.message

    def test_writes_host_path_to_exclude(self, tmp_path):
        svc = _make_service(tmp_path)
        svc.exclude_file.touch()
        result = svc.add_to_exclude(
            ["/mnt/cache/media/Movies/Film.mkv"], dry_run=False
        )
        assert result.success is True
        content = svc.exclude_file.read_text(encoding="utf-8")
        # Should be translated to host path
        assert "/mnt/cache_downloads/media/Movies/Film.mkv" in content

    def test_empty_paths(self, tmp_path):
        svc = _make_service(tmp_path)
        result = svc.add_to_exclude([], dry_run=False)
        assert result.success is False


# ============================================================================
# AuditResults health calculation tests
# ============================================================================

class TestAuditResultsHealth:

    def test_healthy_status(self):
        from web.services.maintenance_service import AuditResults
        results = AuditResults(
            cache_file_count=10, exclude_entry_count=10, timestamp_entry_count=10
        )
        results.calculate_health_status()
        assert results.health_status == "healthy"

    def test_untracked_files_do_not_affect_health(self):
        """Untracked files are informational and should not trigger critical status"""
        from web.services.maintenance_service import AuditResults, UnprotectedFile
        results = AuditResults(
            cache_file_count=10, exclude_entry_count=5, timestamp_entry_count=10
        )
        results.unprotected_files.append(
            UnprotectedFile(
                cache_path="/path/file.mkv", filename="file.mkv",
                size=1000, size_display="1 KB",
                has_plexcached_backup=False, backup_path=None,
                has_array_duplicate=False, array_path=None,
                recommended_action="sync_to_array",
            )
        )
        results.calculate_health_status()
        assert results.health_status == "healthy"

    def test_critical_with_orphaned_backups(self):
        from web.services.maintenance_service import AuditResults, OrphanedBackup
        results = AuditResults(
            cache_file_count=10, exclude_entry_count=10, timestamp_entry_count=10
        )
        results.orphaned_plexcached.append(
            OrphanedBackup(
                plexcached_path="/mnt/user0/media/file.mkv.plexcached",
                original_filename="file.mkv",
                size=1000, size_display="1 KB",
                restore_path="/mnt/user0/media/file.mkv",
                backup_type="orphaned",
            )
        )
        results.calculate_health_status()
        assert results.health_status == "critical"

    def test_warnings_with_stale_entries(self):
        from web.services.maintenance_service import AuditResults
        results = AuditResults(
            cache_file_count=10, exclude_entry_count=12, timestamp_entry_count=10
        )
        results.stale_exclude_entries = ["/stale/path"]
        results.calculate_health_status()
        assert results.health_status == "warnings"
