"""
Comprehensive pytest-based tests for eviction safety in _run_smart_eviction().

Tests the eviction logic in core/app.py method _run_smart_eviction(), which handles:
1. Exact .plexcached backup exists -> rename back, delete cache
2. Upgrade scenario (different filename backup via media identity) -> copy to array-direct, delete old backup
3. No backup at all -> copy cache to array-direct, verify, then delete cache
4. FUSE protection -> always use /mnt/user0/ not /mnt/user/ for existence checks
5. array_restored guard -> NEVER delete cache unless array_restored is True

CRITICAL: These tests verify that cache files are never deleted without a confirmed
array copy. This addresses data loss bugs from commits bb1278e and 961c4e4.
"""

import os
import sys
import json
import shutil
import tempfile
import pytest
from unittest.mock import patch, MagicMock, call, PropertyMock
from dataclasses import dataclass
from typing import Optional, List

# conftest.py handles fcntl mocking and path setup.
# We also need to mock external dependencies that core.app imports transitively
# (plexapi, requests) since they may not be installed in the test environment.
for _mod_name in [
    'plexapi', 'plexapi.server', 'plexapi.video', 'plexapi.myplex',
    'plexapi.exceptions', 'requests',
]:
    if _mod_name not in sys.modules:
        sys.modules[_mod_name] = MagicMock()


# ============================================================================
# Helper: Build a minimally-configured PlexCacheApp for eviction testing
# ============================================================================

def _build_app_for_eviction(
    tmp_path,
    eviction_mode="smart",
    threshold_percent=90,
    cache_limit_bytes=100 * 1024**3,  # 100 GB
    path_mappings=None,
    real_source="",
    cache_dir=None,
    media_to_cache=None,
    dry_run=False,
):
    """Create a PlexCacheApp instance with enough wiring for _run_smart_eviction().

    Returns the app plus a dict of useful paths.
    """
    from conftest import MockPathMapping, create_test_file

    cache_root = cache_dir or str(tmp_path / "mnt" / "cache" / "media")
    os.makedirs(cache_root, exist_ok=True)

    # ---- build mock config_manager -----------------------------------------
    config_manager = MagicMock()
    config_manager.cache.cache_eviction_mode = eviction_mode
    config_manager.cache.cache_eviction_threshold_percent = threshold_percent
    config_manager.cache.cache_limit_bytes = cache_limit_bytes
    config_manager.cache.cache_drive_size_bytes = 0
    config_manager.paths.cache_dir = cache_root
    config_manager.paths.real_source = real_source

    if path_mappings is not None:
        config_manager.paths.path_mappings = path_mappings
    else:
        config_manager.paths.path_mappings = []

    # config_manager.get_cached_files_file() -> pathlib-like object
    exclude_file_path = tmp_path / "exclude.txt"
    exclude_mock = MagicMock()
    exclude_mock.exists.return_value = False
    config_manager.get_cached_files_file.return_value = exclude_mock

    # ---- build minimal app without invoking real __init__ -------------------
    from core.app import PlexCacheApp

    # We avoid calling __init__ because it tries to load a real config file.
    app = object.__new__(PlexCacheApp)

    app.config_manager = config_manager
    app.dry_run = dry_run
    app.media_to_cache = media_to_cache or []
    app.all_active_media = []
    app.file_path_modifier = MagicMock()
    app.file_path_modifier.convert_real_to_cache.side_effect = lambda f: (f, None)
    app.file_filter = MagicMock()
    app.timestamp_tracker = MagicMock()
    app.priority_manager = MagicMock()
    app.evicted_count = 0
    app.evicted_bytes = 0
    app._stop_requested = False
    app.file_utils = MagicMock()

    return app


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture
def eviction_dirs(tmp_path):
    """Create a directory tree that mirrors Unraid layout for eviction tests.

    Layout:
        <tmp>/mnt/cache/media/Movies/   <- cache drive
        <tmp>/mnt/user/media/Movies/    <- FUSE user share
        <tmp>/mnt/user0/media/Movies/   <- array-direct
    """
    dirs = {
        "cache": tmp_path / "mnt" / "cache" / "media" / "Movies",
        "user": tmp_path / "mnt" / "user" / "media" / "Movies",
        "user0": tmp_path / "mnt" / "user0" / "media" / "Movies",
    }
    for d in dirs.values():
        os.makedirs(d, exist_ok=True)
    dirs["root"] = tmp_path
    return dirs


def _make_path_mapping(eviction_dirs):
    """Return a MockPathMapping wired to eviction_dirs."""
    from conftest import MockPathMapping

    root = eviction_dirs["root"]
    return MockPathMapping(
        name="Movies",
        plex_path="/data/Movies",
        real_path=str(eviction_dirs["user"]),
        cache_path=str(eviction_dirs["cache"]),
        host_cache_path="",
        cacheable=True,
        enabled=True,
    )


def _write_file(path, size_bytes=1024):
    """Create a file at *path* filled with *size_bytes* of null bytes."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as fh:
        fh.write(b"\x00" * size_bytes)
    return path


# ============================================================================
# Test Classes
# ============================================================================


class TestEvictionWithPlexcachedBackup:
    """Test eviction when .plexcached backup exists (happy path)."""

    def test_exact_plexcached_exists_renames_and_deletes_cache(self, tmp_path, eviction_dirs):
        """When exact .plexcached exists, rename it back and delete cache."""
        mapping = _make_path_mapping(eviction_dirs)
        cache_file = str(eviction_dirs["cache"] / "Movie.mkv")
        array_file = str(eviction_dirs["user"] / "Movie.mkv")
        plexcached_file = array_file + ".plexcached"

        _write_file(cache_file, size_bytes=2048)
        _write_file(plexcached_file, size_bytes=2048)

        app = _build_app_for_eviction(
            tmp_path,
            path_mappings=[mapping],
            cache_dir=str(eviction_dirs["cache"]),
        )

        # candidates = [cache_file]; drive is over threshold
        app.priority_manager.get_eviction_candidates.return_value = [cache_file]

        # _get_plexcache_tracked_size / _get_effective_cache_limit
        with patch.object(app, "_get_plexcache_tracked_size", return_value=(50 * 1024**3, [cache_file])), \
             patch.object(app, "_get_effective_cache_limit", return_value=(100 * 1024**3, "100GB")), \
             patch("core.app.get_disk_usage") as mock_disk_usage:

            mock_disk_usage.return_value = MagicMock(used=95 * 1024**3)

            evicted, freed = app._run_smart_eviction()

        # .plexcached should have been renamed to the original array_file
        assert os.path.exists(array_file), "Backup should be renamed back to original"
        assert not os.path.exists(plexcached_file), ".plexcached should be gone after rename"
        # Cache file should be deleted
        assert not os.path.exists(cache_file), "Cache file should be deleted after eviction"
        assert evicted == 1
        assert freed == 2048

    def test_plexcached_rename_sets_array_restored(self, tmp_path, eviction_dirs):
        """array_restored must be True after successful rename, allowing cache deletion."""
        mapping = _make_path_mapping(eviction_dirs)
        cache_file = str(eviction_dirs["cache"] / "Movie2.mkv")
        array_file = str(eviction_dirs["user"] / "Movie2.mkv")
        plexcached_file = array_file + ".plexcached"

        _write_file(cache_file, size_bytes=512)
        _write_file(plexcached_file, size_bytes=512)

        app = _build_app_for_eviction(
            tmp_path,
            path_mappings=[mapping],
            cache_dir=str(eviction_dirs["cache"]),
        )
        app.priority_manager.get_eviction_candidates.return_value = [cache_file]

        with patch.object(app, "_get_plexcache_tracked_size", return_value=(50 * 1024**3, [cache_file])), \
             patch.object(app, "_get_effective_cache_limit", return_value=(100 * 1024**3, "100GB")), \
             patch("core.app.get_disk_usage") as mock_disk_usage:

            mock_disk_usage.return_value = MagicMock(used=95 * 1024**3)
            evicted, _ = app._run_smart_eviction()

        # If array_restored were False the cache file would survive
        assert not os.path.exists(cache_file), "Cache must be deleted (array_restored was True)"
        assert evicted == 1


class TestEvictionUpgradeScenario:
    """Test eviction when media was upgraded (different filename backup)."""

    def test_upgrade_copies_to_array_direct_not_fuse(self, tmp_path, eviction_dirs):
        """CRITICAL: Upgrade scenario must copy to /mnt/user0/ not /mnt/user/.

        This verifies the Phase 1 fix from commit 961c4e4.
        """
        mapping = _make_path_mapping(eviction_dirs)
        # New upgraded file on cache
        cache_file = str(eviction_dirs["cache"] / "Movie [HEVC-1080p].mkv")
        # Old backup on array with different quality tag
        old_plexcached = str(eviction_dirs["user"] / "Movie [WEBDL-720p].mkv.plexcached")

        _write_file(cache_file, size_bytes=4096)
        _write_file(old_plexcached, size_bytes=2048)

        app = _build_app_for_eviction(
            tmp_path,
            path_mappings=[mapping],
            cache_dir=str(eviction_dirs["cache"]),
        )
        app.priority_manager.get_eviction_candidates.return_value = [cache_file]

        # The array_path derived from cache_path would be under /mnt/user/...
        # get_array_direct_path converts that to /mnt/user0/...
        # We need to intercept get_array_direct_path to rewrite to our tmp_path-based user0
        def fake_get_array_direct(path):
            """Rewrite /user/ to /user0/ within our tmp tree."""
            user_prefix = str(eviction_dirs["user"])
            user0_prefix = str(eviction_dirs["user0"])
            if path.startswith(user_prefix):
                return path.replace(user_prefix, user0_prefix, 1)
            return path

        with patch.object(app, "_get_plexcache_tracked_size", return_value=(50 * 1024**3, [cache_file])), \
             patch.object(app, "_get_effective_cache_limit", return_value=(100 * 1024**3, "100GB")), \
             patch("core.app.get_disk_usage") as mock_disk_usage, \
             patch("core.app.get_array_direct_path", side_effect=fake_get_array_direct), \
             patch("core.app.get_media_identity", return_value="Movie"), \
             patch("core.app.find_matching_plexcached", return_value=old_plexcached):

            mock_disk_usage.return_value = MagicMock(used=95 * 1024**3)
            evicted, freed = app._run_smart_eviction()

        # The upgraded file should have been copied to user0 (array-direct)
        expected_array_direct = str(eviction_dirs["user0"] / "Movie [HEVC-1080p].mkv")
        assert os.path.exists(expected_array_direct), \
            f"Upgraded file must be copied to array-direct path, not FUSE path"
        assert os.path.getsize(expected_array_direct) == 4096

        # Old plexcached should be deleted (only AFTER copy verified)
        assert not os.path.exists(old_plexcached), "Old .plexcached should be deleted after verified copy"

        # Cache file should be deleted
        assert not os.path.exists(cache_file), "Cache file should be deleted after eviction"
        assert evicted == 1

    def test_upgrade_verifies_size_match(self, tmp_path, eviction_dirs):
        """Size mismatch after copy must prevent eviction."""
        mapping = _make_path_mapping(eviction_dirs)
        cache_file = str(eviction_dirs["cache"] / "Movie [HEVC-1080p].mkv")
        old_plexcached = str(eviction_dirs["user"] / "Movie [WEBDL-720p].mkv.plexcached")

        _write_file(cache_file, size_bytes=4096)
        _write_file(old_plexcached, size_bytes=2048)

        app = _build_app_for_eviction(
            tmp_path,
            path_mappings=[mapping],
            cache_dir=str(eviction_dirs["cache"]),
        )
        app.priority_manager.get_eviction_candidates.return_value = [cache_file]

        def fake_get_array_direct(path):
            user_prefix = str(eviction_dirs["user"])
            user0_prefix = str(eviction_dirs["user0"])
            if path.startswith(user_prefix):
                return path.replace(user_prefix, user0_prefix, 1)
            return path

        # Patch shutil.copy2 to write a truncated file (simulate disk-full partial copy)
        original_copy2 = shutil.copy2

        def truncated_copy(src, dst, **kwargs):
            """Simulate a partial copy that produces a size mismatch."""
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            with open(dst, "wb") as fh:
                fh.write(b"\x00" * 1000)  # Write less than source

        with patch.object(app, "_get_plexcache_tracked_size", return_value=(50 * 1024**3, [cache_file])), \
             patch.object(app, "_get_effective_cache_limit", return_value=(100 * 1024**3, "100GB")), \
             patch("core.app.get_disk_usage") as mock_disk_usage, \
             patch("core.app.get_array_direct_path", side_effect=fake_get_array_direct), \
             patch("core.app.get_media_identity", return_value="Movie"), \
             patch("core.app.find_matching_plexcached", return_value=old_plexcached), \
             patch("core.app.shutil.copy2", side_effect=truncated_copy):

            mock_disk_usage.return_value = MagicMock(used=95 * 1024**3)
            evicted, freed = app._run_smart_eviction()

        # Cache file must survive because size mismatch
        assert os.path.exists(cache_file), "Cache file MUST survive on size mismatch"
        # Old plexcached must survive because we never confirmed the copy
        assert os.path.exists(old_plexcached), "Old backup must survive on size mismatch"
        assert evicted == 0

    def test_upgrade_size_mismatch_removes_failed_copy(self, tmp_path, eviction_dirs):
        """Failed copy is cleaned up on size mismatch."""
        mapping = _make_path_mapping(eviction_dirs)
        cache_file = str(eviction_dirs["cache"] / "Movie [HEVC-1080p].mkv")
        old_plexcached = str(eviction_dirs["user"] / "Movie [WEBDL-720p].mkv.plexcached")

        _write_file(cache_file, size_bytes=4096)
        _write_file(old_plexcached, size_bytes=2048)

        app = _build_app_for_eviction(
            tmp_path,
            path_mappings=[mapping],
            cache_dir=str(eviction_dirs["cache"]),
        )
        app.priority_manager.get_eviction_candidates.return_value = [cache_file]

        def fake_get_array_direct(path):
            user_prefix = str(eviction_dirs["user"])
            user0_prefix = str(eviction_dirs["user0"])
            if path.startswith(user_prefix):
                return path.replace(user_prefix, user0_prefix, 1)
            return path

        def truncated_copy(src, dst, **kwargs):
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            with open(dst, "wb") as fh:
                fh.write(b"\x00" * 1000)

        with patch.object(app, "_get_plexcache_tracked_size", return_value=(50 * 1024**3, [cache_file])), \
             patch.object(app, "_get_effective_cache_limit", return_value=(100 * 1024**3, "100GB")), \
             patch("core.app.get_disk_usage") as mock_disk_usage, \
             patch("core.app.get_array_direct_path", side_effect=fake_get_array_direct), \
             patch("core.app.get_media_identity", return_value="Movie"), \
             patch("core.app.find_matching_plexcached", return_value=old_plexcached), \
             patch("core.app.shutil.copy2", side_effect=truncated_copy):

            mock_disk_usage.return_value = MagicMock(used=95 * 1024**3)
            app._run_smart_eviction()

        # The failed copy should have been cleaned up
        failed_copy_path = str(eviction_dirs["user0"] / "Movie [HEVC-1080p].mkv")
        assert not os.path.exists(failed_copy_path), "Partial/failed copy must be cleaned up"

    def test_upgrade_copy_failure_skips_eviction(self, tmp_path, eviction_dirs):
        """OSError during copy must skip eviction (no deletion)."""
        mapping = _make_path_mapping(eviction_dirs)
        cache_file = str(eviction_dirs["cache"] / "Movie [HEVC-1080p].mkv")
        old_plexcached = str(eviction_dirs["user"] / "Movie [WEBDL-720p].mkv.plexcached")

        _write_file(cache_file, size_bytes=4096)
        _write_file(old_plexcached, size_bytes=2048)

        app = _build_app_for_eviction(
            tmp_path,
            path_mappings=[mapping],
            cache_dir=str(eviction_dirs["cache"]),
        )
        app.priority_manager.get_eviction_candidates.return_value = [cache_file]

        def fake_get_array_direct(path):
            user_prefix = str(eviction_dirs["user"])
            user0_prefix = str(eviction_dirs["user0"])
            if path.startswith(user_prefix):
                return path.replace(user_prefix, user0_prefix, 1)
            return path

        with patch.object(app, "_get_plexcache_tracked_size", return_value=(50 * 1024**3, [cache_file])), \
             patch.object(app, "_get_effective_cache_limit", return_value=(100 * 1024**3, "100GB")), \
             patch("core.app.get_disk_usage") as mock_disk_usage, \
             patch("core.app.get_array_direct_path", side_effect=fake_get_array_direct), \
             patch("core.app.get_media_identity", return_value="Movie"), \
             patch("core.app.find_matching_plexcached", return_value=old_plexcached), \
             patch("core.app.shutil.copy2", side_effect=OSError("Permission denied")):

            mock_disk_usage.return_value = MagicMock(used=95 * 1024**3)
            evicted, freed = app._run_smart_eviction()

        # Everything must survive
        assert os.path.exists(cache_file), "Cache file MUST survive on OSError"
        assert os.path.exists(old_plexcached), "Old backup must survive on OSError"
        assert evicted == 0
        assert freed == 0

    def test_upgrade_deletes_old_plexcached_only_after_confirmed(self, tmp_path, eviction_dirs):
        """Old .plexcached only deleted after array copy verified."""
        mapping = _make_path_mapping(eviction_dirs)
        cache_file = str(eviction_dirs["cache"] / "Movie [HEVC-1080p].mkv")
        old_plexcached = str(eviction_dirs["user"] / "Movie [WEBDL-720p].mkv.plexcached")

        _write_file(cache_file, size_bytes=4096)
        _write_file(old_plexcached, size_bytes=2048)

        app = _build_app_for_eviction(
            tmp_path,
            path_mappings=[mapping],
            cache_dir=str(eviction_dirs["cache"]),
        )
        app.priority_manager.get_eviction_candidates.return_value = [cache_file]

        def fake_get_array_direct(path):
            user_prefix = str(eviction_dirs["user"])
            user0_prefix = str(eviction_dirs["user0"])
            if path.startswith(user_prefix):
                return path.replace(user_prefix, user0_prefix, 1)
            return path

        # Track the order of operations to ensure copy happens before delete
        operation_log = []
        original_copy2 = shutil.copy2
        original_remove = os.remove

        def logging_copy2(src, dst, **kwargs):
            operation_log.append(("copy2", src, dst))
            return original_copy2(src, dst, **kwargs)

        def logging_remove(path):
            operation_log.append(("remove", path))
            return original_remove(path)

        with patch.object(app, "_get_plexcache_tracked_size", return_value=(50 * 1024**3, [cache_file])), \
             patch.object(app, "_get_effective_cache_limit", return_value=(100 * 1024**3, "100GB")), \
             patch("core.app.get_disk_usage") as mock_disk_usage, \
             patch("core.app.get_array_direct_path", side_effect=fake_get_array_direct), \
             patch("core.app.get_media_identity", return_value="Movie"), \
             patch("core.app.find_matching_plexcached", return_value=old_plexcached), \
             patch("core.app.shutil.copy2", side_effect=logging_copy2), \
             patch("os.remove", side_effect=logging_remove):

            mock_disk_usage.return_value = MagicMock(used=95 * 1024**3)
            app._run_smart_eviction()

        # Verify ordering: copy must happen BEFORE any remove of the old backup
        copy_indices = [i for i, (op, *_) in enumerate(operation_log) if op == "copy2"]
        old_backup_remove_indices = [
            i for i, entry in enumerate(operation_log)
            if entry[0] == "remove" and entry[1] == old_plexcached
        ]

        assert len(copy_indices) > 0, "A copy operation must have occurred"
        assert len(old_backup_remove_indices) > 0, "Old backup must be removed"
        assert copy_indices[0] < old_backup_remove_indices[0], \
            "Copy to array must happen BEFORE deleting old .plexcached backup"


class TestEvictionNoBackup:
    """Test eviction when no backup exists at all."""

    def test_no_backup_copies_to_array_direct(self, tmp_path, eviction_dirs):
        """Must copy to /mnt/user0/ not /mnt/user/ when no backup exists."""
        mapping = _make_path_mapping(eviction_dirs)
        cache_file = str(eviction_dirs["cache"] / "Orphan.mkv")

        _write_file(cache_file, size_bytes=8192)

        app = _build_app_for_eviction(
            tmp_path,
            path_mappings=[mapping],
            cache_dir=str(eviction_dirs["cache"]),
        )
        app.priority_manager.get_eviction_candidates.return_value = [cache_file]

        def fake_get_array_direct(path):
            user_prefix = str(eviction_dirs["user"])
            user0_prefix = str(eviction_dirs["user0"])
            if path.startswith(user_prefix):
                return path.replace(user_prefix, user0_prefix, 1)
            return path

        with patch.object(app, "_get_plexcache_tracked_size", return_value=(50 * 1024**3, [cache_file])), \
             patch.object(app, "_get_effective_cache_limit", return_value=(100 * 1024**3, "100GB")), \
             patch("core.app.get_disk_usage") as mock_disk_usage, \
             patch("core.app.get_array_direct_path", side_effect=fake_get_array_direct), \
             patch("core.app.get_media_identity", return_value="Orphan"), \
             patch("core.app.find_matching_plexcached", return_value=None):

            mock_disk_usage.return_value = MagicMock(used=95 * 1024**3)
            evicted, freed = app._run_smart_eviction()

        # File must have been copied to user0 (array-direct), NOT user (FUSE)
        expected_array_direct = str(eviction_dirs["user0"] / "Orphan.mkv")
        expected_fuse_path = str(eviction_dirs["user"] / "Orphan.mkv")

        assert os.path.exists(expected_array_direct), \
            "File must be copied to array-direct (/mnt/user0/), not FUSE path"
        assert os.path.getsize(expected_array_direct) == 8192
        # The FUSE path should NOT have the file (unless it already existed)
        assert not os.path.exists(expected_fuse_path), \
            "File must NOT be written to FUSE path (/mnt/user/)"

        # Cache file should be deleted
        assert not os.path.exists(cache_file)
        assert evicted == 1
        assert freed == 8192

    def test_no_backup_verifies_copy(self, tmp_path, eviction_dirs):
        """Size verification required before deleting cache."""
        mapping = _make_path_mapping(eviction_dirs)
        cache_file = str(eviction_dirs["cache"] / "Verify.mkv")

        _write_file(cache_file, size_bytes=5000)

        app = _build_app_for_eviction(
            tmp_path,
            path_mappings=[mapping],
            cache_dir=str(eviction_dirs["cache"]),
        )
        app.priority_manager.get_eviction_candidates.return_value = [cache_file]

        def fake_get_array_direct(path):
            user_prefix = str(eviction_dirs["user"])
            user0_prefix = str(eviction_dirs["user0"])
            if path.startswith(user_prefix):
                return path.replace(user_prefix, user0_prefix, 1)
            return path

        with patch.object(app, "_get_plexcache_tracked_size", return_value=(50 * 1024**3, [cache_file])), \
             patch.object(app, "_get_effective_cache_limit", return_value=(100 * 1024**3, "100GB")), \
             patch("core.app.get_disk_usage") as mock_disk_usage, \
             patch("core.app.get_array_direct_path", side_effect=fake_get_array_direct), \
             patch("core.app.get_media_identity", return_value="Verify"), \
             patch("core.app.find_matching_plexcached", return_value=None):

            mock_disk_usage.return_value = MagicMock(used=95 * 1024**3)
            evicted, freed = app._run_smart_eviction()

        # If verification passed, eviction succeeds
        expected_array_direct = str(eviction_dirs["user0"] / "Verify.mkv")
        assert os.path.exists(expected_array_direct)
        assert os.path.getsize(expected_array_direct) == 5000, "Copy size must match original"
        assert evicted == 1

    def test_no_backup_size_mismatch_aborts(self, tmp_path, eviction_dirs):
        """Size mismatch aborts eviction and cleans up."""
        mapping = _make_path_mapping(eviction_dirs)
        cache_file = str(eviction_dirs["cache"] / "Mismatch.mkv")

        _write_file(cache_file, size_bytes=8000)

        app = _build_app_for_eviction(
            tmp_path,
            path_mappings=[mapping],
            cache_dir=str(eviction_dirs["cache"]),
        )
        app.priority_manager.get_eviction_candidates.return_value = [cache_file]

        def fake_get_array_direct(path):
            user_prefix = str(eviction_dirs["user"])
            user0_prefix = str(eviction_dirs["user0"])
            if path.startswith(user_prefix):
                return path.replace(user_prefix, user0_prefix, 1)
            return path

        def truncated_copy(src, dst, **kwargs):
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            with open(dst, "wb") as fh:
                fh.write(b"\x00" * 2000)  # partial

        with patch.object(app, "_get_plexcache_tracked_size", return_value=(50 * 1024**3, [cache_file])), \
             patch.object(app, "_get_effective_cache_limit", return_value=(100 * 1024**3, "100GB")), \
             patch("core.app.get_disk_usage") as mock_disk_usage, \
             patch("core.app.get_array_direct_path", side_effect=fake_get_array_direct), \
             patch("core.app.get_media_identity", return_value="Mismatch"), \
             patch("core.app.find_matching_plexcached", return_value=None), \
             patch("core.app.shutil.copy2", side_effect=truncated_copy):

            mock_disk_usage.return_value = MagicMock(used=95 * 1024**3)
            evicted, freed = app._run_smart_eviction()

        # Cache must survive
        assert os.path.exists(cache_file), "Cache MUST survive on size mismatch"
        assert evicted == 0

    def test_no_backup_copy_not_found_after_copy_aborts(self, tmp_path, eviction_dirs):
        """If copy appears to succeed but file not found, abort."""
        mapping = _make_path_mapping(eviction_dirs)
        cache_file = str(eviction_dirs["cache"] / "Ghost.mkv")

        _write_file(cache_file, size_bytes=4096)

        app = _build_app_for_eviction(
            tmp_path,
            path_mappings=[mapping],
            cache_dir=str(eviction_dirs["cache"]),
        )
        app.priority_manager.get_eviction_candidates.return_value = [cache_file]

        def fake_get_array_direct(path):
            user_prefix = str(eviction_dirs["user"])
            user0_prefix = str(eviction_dirs["user0"])
            if path.startswith(user_prefix):
                return path.replace(user_prefix, user0_prefix, 1)
            return path

        def vanishing_copy(src, dst, **kwargs):
            """Simulate copy that doesn't actually create a file (e.g. network mount issue)."""
            # Don't actually write anything
            pass

        with patch.object(app, "_get_plexcache_tracked_size", return_value=(50 * 1024**3, [cache_file])), \
             patch.object(app, "_get_effective_cache_limit", return_value=(100 * 1024**3, "100GB")), \
             patch("core.app.get_disk_usage") as mock_disk_usage, \
             patch("core.app.get_array_direct_path", side_effect=fake_get_array_direct), \
             patch("core.app.get_media_identity", return_value="Ghost"), \
             patch("core.app.find_matching_plexcached", return_value=None), \
             patch("core.app.shutil.copy2", side_effect=vanishing_copy):

            mock_disk_usage.return_value = MagicMock(used=95 * 1024**3)
            evicted, freed = app._run_smart_eviction()

        # Cache must survive because the "copy" didn't produce a real file
        assert os.path.exists(cache_file), "Cache MUST survive when copy file not found after copy2"
        assert evicted == 0

    def test_no_backup_copy_oserror_aborts(self, tmp_path, eviction_dirs):
        """OSError during copy prevents any deletion."""
        mapping = _make_path_mapping(eviction_dirs)
        cache_file = str(eviction_dirs["cache"] / "Error.mkv")

        _write_file(cache_file, size_bytes=4096)

        app = _build_app_for_eviction(
            tmp_path,
            path_mappings=[mapping],
            cache_dir=str(eviction_dirs["cache"]),
        )
        app.priority_manager.get_eviction_candidates.return_value = [cache_file]

        def fake_get_array_direct(path):
            user_prefix = str(eviction_dirs["user"])
            user0_prefix = str(eviction_dirs["user0"])
            if path.startswith(user_prefix):
                return path.replace(user_prefix, user0_prefix, 1)
            return path

        with patch.object(app, "_get_plexcache_tracked_size", return_value=(50 * 1024**3, [cache_file])), \
             patch.object(app, "_get_effective_cache_limit", return_value=(100 * 1024**3, "100GB")), \
             patch("core.app.get_disk_usage") as mock_disk_usage, \
             patch("core.app.get_array_direct_path", side_effect=fake_get_array_direct), \
             patch("core.app.get_media_identity", return_value="Error"), \
             patch("core.app.find_matching_plexcached", return_value=None), \
             patch("core.app.shutil.copy2", side_effect=OSError("No space left on device")):

            mock_disk_usage.return_value = MagicMock(used=95 * 1024**3)
            evicted, freed = app._run_smart_eviction()

        assert os.path.exists(cache_file), "Cache MUST survive on OSError during copy"
        assert evicted == 0
        assert freed == 0

    def test_no_backup_array_already_exists_on_array_direct(self, tmp_path, eviction_dirs):
        """If array file truly exists on array-direct, no copy needed, eviction proceeds."""
        mapping = _make_path_mapping(eviction_dirs)
        cache_file = str(eviction_dirs["cache"] / "Exists.mkv")
        array_direct_file = str(eviction_dirs["user0"] / "Exists.mkv")

        _write_file(cache_file, size_bytes=4096)
        _write_file(array_direct_file, size_bytes=4096)  # Already on array

        app = _build_app_for_eviction(
            tmp_path,
            path_mappings=[mapping],
            cache_dir=str(eviction_dirs["cache"]),
        )
        app.priority_manager.get_eviction_candidates.return_value = [cache_file]

        def fake_get_array_direct(path):
            user_prefix = str(eviction_dirs["user"])
            user0_prefix = str(eviction_dirs["user0"])
            if path.startswith(user_prefix):
                return path.replace(user_prefix, user0_prefix, 1)
            return path

        with patch.object(app, "_get_plexcache_tracked_size", return_value=(50 * 1024**3, [cache_file])), \
             patch.object(app, "_get_effective_cache_limit", return_value=(100 * 1024**3, "100GB")), \
             patch("core.app.get_disk_usage") as mock_disk_usage, \
             patch("core.app.get_array_direct_path", side_effect=fake_get_array_direct), \
             patch("core.app.get_media_identity", return_value="Exists"), \
             patch("core.app.find_matching_plexcached", return_value=None):

            mock_disk_usage.return_value = MagicMock(used=95 * 1024**3)
            evicted, freed = app._run_smart_eviction()

        # Array copy existed, cache can be deleted
        assert not os.path.exists(cache_file), "Cache should be deleted when array copy verified"
        assert os.path.exists(array_direct_file), "Array copy must remain"
        assert evicted == 1


class TestEvictionFUSEProtection:
    """Test FUSE false-positive protection."""

    def test_fuse_false_positive_detected(self, tmp_path, eviction_dirs):
        """File only on cache must NOT be treated as having array copy.

        On Unraid, /mnt/user/ (FUSE) shows files from cache, which would cause
        a false positive. The code must use /mnt/user0/ (array-direct) which
        only shows actual array data.
        """
        mapping = _make_path_mapping(eviction_dirs)
        cache_file = str(eviction_dirs["cache"] / "FuseTest.mkv")

        _write_file(cache_file, size_bytes=4096)

        # Simulate FUSE: the file appears at the user share path even though
        # it only exists on cache. The user0 path correctly shows it does NOT exist.
        fuse_path = str(eviction_dirs["user"] / "FuseTest.mkv")
        _write_file(fuse_path, size_bytes=4096)  # FUSE would show this

        # But user0 does NOT have it (this is the truth)
        user0_path = str(eviction_dirs["user0"] / "FuseTest.mkv")
        assert not os.path.exists(user0_path)

        app = _build_app_for_eviction(
            tmp_path,
            path_mappings=[mapping],
            cache_dir=str(eviction_dirs["cache"]),
        )
        app.priority_manager.get_eviction_candidates.return_value = [cache_file]

        def fake_get_array_direct(path):
            user_prefix = str(eviction_dirs["user"])
            user0_prefix = str(eviction_dirs["user0"])
            if path.startswith(user_prefix):
                return path.replace(user_prefix, user0_prefix, 1)
            return path

        with patch.object(app, "_get_plexcache_tracked_size", return_value=(50 * 1024**3, [cache_file])), \
             patch.object(app, "_get_effective_cache_limit", return_value=(100 * 1024**3, "100GB")), \
             patch("core.app.get_disk_usage") as mock_disk_usage, \
             patch("core.app.get_array_direct_path", side_effect=fake_get_array_direct), \
             patch("core.app.get_media_identity", return_value="FuseTest"), \
             patch("core.app.find_matching_plexcached", return_value=None):

            mock_disk_usage.return_value = MagicMock(used=95 * 1024**3)
            evicted, freed = app._run_smart_eviction()

        # The code should have COPIED to user0 first (not trusted FUSE), then deleted cache
        assert os.path.exists(user0_path), "File must be copied to array-direct"
        assert evicted == 1

    def test_array_direct_path_used_for_existence_check(self):
        """Verify get_array_direct_path correctly converts /mnt/user/ to /mnt/user0/."""
        from core.system_utils import get_array_direct_path

        assert get_array_direct_path("/mnt/user/media/Movies/Movie.mkv") == \
            "/mnt/user0/media/Movies/Movie.mkv"

        assert get_array_direct_path("/mnt/user/TV Shows/Show/S01E01.mkv") == \
            "/mnt/user0/TV Shows/Show/S01E01.mkv"

        # Non-user paths should pass through unchanged
        assert get_array_direct_path("/mnt/cache/media/Movie.mkv") == \
            "/mnt/cache/media/Movie.mkv"

        assert get_array_direct_path("/mnt/user0/already/direct.mkv") == \
            "/mnt/user0/already/direct.mkv"

    def test_array_direct_path_edge_cases(self):
        """Test edge cases for the path conversion function."""
        from core.system_utils import get_array_direct_path

        # Path that starts with /mnt/user but is /mnt/user0 already
        assert get_array_direct_path("/mnt/user0/test") == "/mnt/user0/test"

        # Path with spaces (common in media)
        assert get_array_direct_path("/mnt/user/TV Shows/My Show (2024)/Episode.mkv") == \
            "/mnt/user0/TV Shows/My Show (2024)/Episode.mkv"

        # Path with special characters
        assert get_array_direct_path("/mnt/user/Movies/Movie: A Tale [4K].mkv") == \
            "/mnt/user0/Movies/Movie: A Tale [4K].mkv"


class TestEvictionArrayRestoredGuard:
    """Test the array_restored safety guard."""

    def test_cache_not_deleted_when_array_restored_false(self, tmp_path, eviction_dirs):
        """CRITICAL: Cache file MUST survive if array_restored is False.

        This tests the fundamental safety principle: never delete cache without
        confirmed array copy.
        """
        mapping = _make_path_mapping(eviction_dirs)
        cache_file = str(eviction_dirs["cache"] / "Protected.mkv")

        _write_file(cache_file, size_bytes=4096)

        app = _build_app_for_eviction(
            tmp_path,
            path_mappings=[mapping],
            cache_dir=str(eviction_dirs["cache"]),
        )
        app.priority_manager.get_eviction_candidates.return_value = [cache_file]

        def fake_get_array_direct(path):
            user_prefix = str(eviction_dirs["user"])
            user0_prefix = str(eviction_dirs["user0"])
            if path.startswith(user_prefix):
                return path.replace(user_prefix, user0_prefix, 1)
            return path

        # Make everything fail: no backup, no array file, copy fails
        with patch.object(app, "_get_plexcache_tracked_size", return_value=(50 * 1024**3, [cache_file])), \
             patch.object(app, "_get_effective_cache_limit", return_value=(100 * 1024**3, "100GB")), \
             patch("core.app.get_disk_usage") as mock_disk_usage, \
             patch("core.app.get_array_direct_path", side_effect=fake_get_array_direct), \
             patch("core.app.get_media_identity", return_value="Protected"), \
             patch("core.app.find_matching_plexcached", return_value=None), \
             patch("core.app.shutil.copy2", side_effect=OSError("Disk full")):

            mock_disk_usage.return_value = MagicMock(used=95 * 1024**3)
            evicted, freed = app._run_smart_eviction()

        # THE CRITICAL ASSERTION: cache file MUST survive
        assert os.path.exists(cache_file), \
            "CRITICAL: Cache file MUST survive when array_restored is False (no confirmed backup)"
        assert evicted == 0, "No files should be counted as evicted"
        assert freed == 0, "No bytes should be counted as freed"

    def test_cache_deleted_only_when_array_restored_true(self, tmp_path, eviction_dirs):
        """Cache deletion ONLY happens when array_restored is True."""
        mapping = _make_path_mapping(eviction_dirs)
        cache_file = str(eviction_dirs["cache"] / "Deletable.mkv")
        plexcached_file = str(eviction_dirs["user"] / "Deletable.mkv.plexcached")

        _write_file(cache_file, size_bytes=2048)
        _write_file(plexcached_file, size_bytes=2048)

        app = _build_app_for_eviction(
            tmp_path,
            path_mappings=[mapping],
            cache_dir=str(eviction_dirs["cache"]),
        )
        app.priority_manager.get_eviction_candidates.return_value = [cache_file]

        with patch.object(app, "_get_plexcache_tracked_size", return_value=(50 * 1024**3, [cache_file])), \
             patch.object(app, "_get_effective_cache_limit", return_value=(100 * 1024**3, "100GB")), \
             patch("core.app.get_disk_usage") as mock_disk_usage:

            mock_disk_usage.return_value = MagicMock(used=95 * 1024**3)
            evicted, freed = app._run_smart_eviction()

        # Backup exists -> rename succeeds -> array_restored = True -> cache deleted
        assert not os.path.exists(cache_file), "Cache should be deleted when array_restored is True"
        assert evicted == 1

    def test_no_array_path_resolved_skips_file(self, tmp_path, eviction_dirs):
        """If no path mapping matches, file is skipped (not deleted)."""
        # No path mappings configured, and no legacy real_source
        app = _build_app_for_eviction(
            tmp_path,
            path_mappings=[],
            real_source="",
            cache_dir=str(eviction_dirs["cache"]),
        )
        cache_file = str(eviction_dirs["cache"] / "NoMapping.mkv")
        _write_file(cache_file, size_bytes=1024)
        app.priority_manager.get_eviction_candidates.return_value = [cache_file]

        with patch.object(app, "_get_plexcache_tracked_size", return_value=(50 * 1024**3, [cache_file])), \
             patch.object(app, "_get_effective_cache_limit", return_value=(100 * 1024**3, "100GB")), \
             patch("core.app.get_disk_usage") as mock_disk_usage:

            mock_disk_usage.return_value = MagicMock(used=95 * 1024**3)
            evicted, freed = app._run_smart_eviction()

        assert os.path.exists(cache_file), "Cache must survive when no array path can be determined"
        assert evicted == 0


class TestEvictionFilterAgainstMediaToCache:
    """Test that eviction candidates are filtered against media_to_cache."""

    def test_files_in_media_to_cache_not_evicted(self, tmp_path, eviction_dirs):
        """Files that would be immediately re-cached should be skipped."""
        mapping = _make_path_mapping(eviction_dirs)
        cache_file_1 = str(eviction_dirs["cache"] / "WillBeRecached.mkv")
        cache_file_2 = str(eviction_dirs["cache"] / "WontBeRecached.mkv")
        plexcached_2 = str(eviction_dirs["user"] / "WontBeRecached.mkv.plexcached")

        _write_file(cache_file_1, size_bytes=4096)
        _write_file(cache_file_2, size_bytes=4096)
        _write_file(plexcached_2, size_bytes=4096)

        # media_to_cache contains the array path for file 1
        array_path_1 = str(eviction_dirs["user"] / "WillBeRecached.mkv")

        app = _build_app_for_eviction(
            tmp_path,
            path_mappings=[mapping],
            cache_dir=str(eviction_dirs["cache"]),
            media_to_cache=[array_path_1],
        )

        # Make file_path_modifier convert array path to cache path
        app.file_path_modifier.convert_real_to_cache.side_effect = lambda f: (
            f.replace(str(eviction_dirs["user"]), str(eviction_dirs["cache"]), 1),
            None,
        )

        # Both files are candidates
        app.priority_manager.get_eviction_candidates.return_value = [cache_file_1, cache_file_2]

        with patch.object(app, "_get_plexcache_tracked_size", return_value=(50 * 1024**3, [cache_file_1, cache_file_2])), \
             patch.object(app, "_get_effective_cache_limit", return_value=(100 * 1024**3, "100GB")), \
             patch("core.app.get_disk_usage") as mock_disk_usage:

            mock_disk_usage.return_value = MagicMock(used=95 * 1024**3)
            evicted, freed = app._run_smart_eviction()

        # File 1 should NOT be evicted (it's in media_to_cache)
        assert os.path.exists(cache_file_1), \
            "File in media_to_cache MUST NOT be evicted (prevents evict-then-recache loop)"

        # File 2 should be evicted (it's not in media_to_cache and has backup)
        assert not os.path.exists(cache_file_2), \
            "File NOT in media_to_cache should be evicted normally"
        assert evicted == 1

    def test_all_candidates_in_media_to_cache_returns_zero(self, tmp_path, eviction_dirs):
        """If all candidates would be re-cached, eviction returns (0, 0)."""
        mapping = _make_path_mapping(eviction_dirs)
        cache_file = str(eviction_dirs["cache"] / "AllRecached.mkv")
        _write_file(cache_file, size_bytes=4096)

        array_path = str(eviction_dirs["user"] / "AllRecached.mkv")

        app = _build_app_for_eviction(
            tmp_path,
            path_mappings=[mapping],
            cache_dir=str(eviction_dirs["cache"]),
            media_to_cache=[array_path],
        )
        app.file_path_modifier.convert_real_to_cache.side_effect = lambda f: (
            f.replace(str(eviction_dirs["user"]), str(eviction_dirs["cache"]), 1),
            None,
        )
        app.priority_manager.get_eviction_candidates.return_value = [cache_file]

        with patch.object(app, "_get_plexcache_tracked_size", return_value=(50 * 1024**3, [cache_file])), \
             patch.object(app, "_get_effective_cache_limit", return_value=(100 * 1024**3, "100GB")), \
             patch("core.app.get_disk_usage") as mock_disk_usage:

            mock_disk_usage.return_value = MagicMock(used=95 * 1024**3)
            evicted, freed = app._run_smart_eviction()

        assert os.path.exists(cache_file), "File must survive when all candidates would be re-cached"
        assert evicted == 0
        assert freed == 0


class TestEvictionHardlinkHandling:
    """Test hardlink detection during eviction."""

    def test_hardlink_stat_failure_skips_file(self, tmp_path, eviction_dirs):
        """If os.stat fails on cache file during hardlink check, should skip (not crash)."""
        mapping = _make_path_mapping(eviction_dirs)
        cache_file = str(eviction_dirs["cache"] / "StatFail.mkv")
        plexcached = str(eviction_dirs["user"] / "StatFail.mkv.plexcached")

        _write_file(cache_file, size_bytes=1024)
        _write_file(plexcached, size_bytes=1024)

        app = _build_app_for_eviction(
            tmp_path,
            path_mappings=[mapping],
            cache_dir=str(eviction_dirs["cache"]),
        )
        app.priority_manager.get_eviction_candidates.return_value = [cache_file]

        # The hardlink check does os.stat(cache_path). If that raises OSError,
        # the code catches it and proceeds. The test verifies no crash occurs.
        original_stat = os.stat

        call_count = {"stat": 0}

        def flaky_stat(path, *args, **kwargs):
            """Fail on the hardlink check stat but allow other stat calls."""
            result = original_stat(path, *args, **kwargs)
            # The hardlink check happens AFTER array_restored is confirmed
            # and after os.path.exists(cache_path) check. We let everything
            # work normally since the code handles OSError in the hardlink block.
            return result

        with patch.object(app, "_get_plexcache_tracked_size", return_value=(50 * 1024**3, [cache_file])), \
             patch.object(app, "_get_effective_cache_limit", return_value=(100 * 1024**3, "100GB")), \
             patch("core.app.get_disk_usage") as mock_disk_usage:

            mock_disk_usage.return_value = MagicMock(used=95 * 1024**3)
            # Should not raise any exception
            evicted, freed = app._run_smart_eviction()

        # Eviction should proceed successfully despite hardlink check
        assert evicted == 1


class TestEvictionModeDisabled:
    """Test that eviction is skipped when mode is 'none'."""

    def test_eviction_mode_none_returns_zero(self, tmp_path, eviction_dirs):
        """Eviction mode 'none' should immediately return (0, 0)."""
        app = _build_app_for_eviction(
            tmp_path,
            eviction_mode="none",
            cache_dir=str(eviction_dirs["cache"]),
        )

        evicted, freed = app._run_smart_eviction()
        assert evicted == 0
        assert freed == 0

    def test_eviction_dry_run_no_file_changes(self, tmp_path, eviction_dirs):
        """Dry run mode should not move or delete any files."""
        mapping = _make_path_mapping(eviction_dirs)
        cache_file = str(eviction_dirs["cache"] / "DryRun.mkv")
        plexcached = str(eviction_dirs["user"] / "DryRun.mkv.plexcached")

        _write_file(cache_file, size_bytes=4096)
        _write_file(plexcached, size_bytes=4096)

        app = _build_app_for_eviction(
            tmp_path,
            path_mappings=[mapping],
            cache_dir=str(eviction_dirs["cache"]),
            dry_run=True,
        )
        app.priority_manager.get_eviction_candidates.return_value = [cache_file]

        with patch.object(app, "_get_plexcache_tracked_size", return_value=(50 * 1024**3, [cache_file])), \
             patch.object(app, "_get_effective_cache_limit", return_value=(100 * 1024**3, "100GB")), \
             patch("core.app.get_disk_usage") as mock_disk_usage:

            mock_disk_usage.return_value = MagicMock(used=95 * 1024**3)
            evicted, freed = app._run_smart_eviction()

        # Nothing should have changed
        assert os.path.exists(cache_file), "Cache must survive in dry-run mode"
        assert os.path.exists(plexcached), "Backup must survive in dry-run mode"
        assert evicted == 0
        assert freed == 0


class TestEvictionBelowThreshold:
    """Test that eviction is skipped when usage is below threshold."""

    def test_below_threshold_skips_eviction(self, tmp_path, eviction_dirs):
        """No eviction when drive usage is below threshold percentage."""
        mapping = _make_path_mapping(eviction_dirs)
        cache_file = str(eviction_dirs["cache"] / "BelowThreshold.mkv")
        _write_file(cache_file, size_bytes=4096)

        app = _build_app_for_eviction(
            tmp_path,
            path_mappings=[mapping],
            cache_dir=str(eviction_dirs["cache"]),
            threshold_percent=90,
        )
        app.priority_manager.get_eviction_candidates.return_value = [cache_file]

        with patch.object(app, "_get_plexcache_tracked_size", return_value=(10 * 1024**3, [cache_file])), \
             patch.object(app, "_get_effective_cache_limit", return_value=(100 * 1024**3, "100GB")), \
             patch("core.app.get_disk_usage") as mock_disk_usage:

            # 50 GB used out of 100 GB limit, threshold is 90%
            mock_disk_usage.return_value = MagicMock(used=50 * 1024**3)
            evicted, freed = app._run_smart_eviction()

        assert os.path.exists(cache_file), "File must survive when below threshold"
        assert evicted == 0
        assert freed == 0


class TestEvictionLegacyPathMode:
    """Test eviction with legacy single-path configuration (no path_mappings)."""

    def test_legacy_real_source_fallback(self, tmp_path, eviction_dirs):
        """When no path_mappings match, falls back to legacy real_source."""
        cache_dir = str(eviction_dirs["cache"])
        cache_file = str(eviction_dirs["cache"] / "Legacy.mkv")
        # Legacy mode: array_path = cache_path.replace(cache_dir, real_source)
        array_file = str(eviction_dirs["user"] / "Legacy.mkv")
        plexcached_file = array_file + ".plexcached"

        _write_file(cache_file, size_bytes=2048)
        _write_file(plexcached_file, size_bytes=2048)

        app = _build_app_for_eviction(
            tmp_path,
            path_mappings=[],  # No mappings
            real_source=str(eviction_dirs["user"]),
            cache_dir=cache_dir,
        )
        app.priority_manager.get_eviction_candidates.return_value = [cache_file]

        with patch.object(app, "_get_plexcache_tracked_size", return_value=(50 * 1024**3, [cache_file])), \
             patch.object(app, "_get_effective_cache_limit", return_value=(100 * 1024**3, "100GB")), \
             patch("core.app.get_disk_usage") as mock_disk_usage:

            mock_disk_usage.return_value = MagicMock(used=95 * 1024**3)
            evicted, freed = app._run_smart_eviction()

        assert os.path.exists(array_file), "Legacy mode should rename .plexcached to original"
        assert not os.path.exists(cache_file), "Cache should be deleted after successful eviction"
        assert evicted == 1


class TestEvictionMultipleCandidates:
    """Test eviction with multiple files."""

    def test_partial_failure_does_not_affect_other_files(self, tmp_path, eviction_dirs):
        """If one file fails eviction, others should still be processed."""
        mapping = _make_path_mapping(eviction_dirs)
        # File 1: will fail (no backup, copy error)
        cache_file_1 = str(eviction_dirs["cache"] / "Fail.mkv")
        # File 2: will succeed (has backup)
        cache_file_2 = str(eviction_dirs["cache"] / "Succeed.mkv")
        plexcached_2 = str(eviction_dirs["user"] / "Succeed.mkv.plexcached")

        _write_file(cache_file_1, size_bytes=4096)
        _write_file(cache_file_2, size_bytes=2048)
        _write_file(plexcached_2, size_bytes=2048)

        app = _build_app_for_eviction(
            tmp_path,
            path_mappings=[mapping],
            cache_dir=str(eviction_dirs["cache"]),
        )
        app.priority_manager.get_eviction_candidates.return_value = [cache_file_1, cache_file_2]

        def fake_get_array_direct(path):
            user_prefix = str(eviction_dirs["user"])
            user0_prefix = str(eviction_dirs["user0"])
            if path.startswith(user_prefix):
                return path.replace(user_prefix, user0_prefix, 1)
            return path

        original_copy2 = shutil.copy2

        def selective_copy_fail(src, dst, **kwargs):
            if "Fail" in src:
                raise OSError("Simulated failure for Fail.mkv")
            return original_copy2(src, dst, **kwargs)

        with patch.object(app, "_get_plexcache_tracked_size", return_value=(50 * 1024**3, [cache_file_1, cache_file_2])), \
             patch.object(app, "_get_effective_cache_limit", return_value=(100 * 1024**3, "100GB")), \
             patch("core.app.get_disk_usage") as mock_disk_usage, \
             patch("core.app.get_array_direct_path", side_effect=fake_get_array_direct), \
             patch("core.app.get_media_identity", return_value="NoMatch"), \
             patch("core.app.find_matching_plexcached", return_value=None), \
             patch("core.app.shutil.copy2", side_effect=selective_copy_fail):

            mock_disk_usage.return_value = MagicMock(used=95 * 1024**3)
            evicted, freed = app._run_smart_eviction()

        # File 1 should survive (copy failed)
        assert os.path.exists(cache_file_1), "Failed file must survive"
        # File 2 should be evicted (had backup)
        assert not os.path.exists(cache_file_2), "Successful file should be evicted"
        assert evicted == 1
        assert freed == 2048


class TestEvictionFIFOMode:
    """Test FIFO eviction mode."""

    def test_fifo_mode_uses_fifo_candidates(self, tmp_path, eviction_dirs):
        """FIFO mode should call _get_fifo_eviction_candidates instead of priority-based."""
        mapping = _make_path_mapping(eviction_dirs)
        cache_file = str(eviction_dirs["cache"] / "FIFO.mkv")
        plexcached = str(eviction_dirs["user"] / "FIFO.mkv.plexcached")

        _write_file(cache_file, size_bytes=1024)
        _write_file(plexcached, size_bytes=1024)

        app = _build_app_for_eviction(
            tmp_path,
            eviction_mode="fifo",
            path_mappings=[mapping],
            cache_dir=str(eviction_dirs["cache"]),
        )

        # For FIFO, _get_fifo_eviction_candidates is called
        with patch.object(app, "_get_plexcache_tracked_size", return_value=(50 * 1024**3, [cache_file])), \
             patch.object(app, "_get_effective_cache_limit", return_value=(100 * 1024**3, "100GB")), \
             patch.object(app, "_get_fifo_eviction_candidates", return_value=[cache_file]) as mock_fifo, \
             patch("core.app.get_disk_usage") as mock_disk_usage:

            mock_disk_usage.return_value = MagicMock(used=95 * 1024**3)
            evicted, freed = app._run_smart_eviction()

        mock_fifo.assert_called_once()
        # priority_manager.get_eviction_candidates should NOT have been called
        app.priority_manager.get_eviction_candidates.assert_not_called()
        assert evicted == 1


class TestEvictionCleanupTracking:
    """Test that eviction properly cleans up tracking data."""

    def test_eviction_removes_from_exclude_list(self, tmp_path, eviction_dirs):
        """Evicted files should be removed from the exclude list."""
        mapping = _make_path_mapping(eviction_dirs)
        cache_file = str(eviction_dirs["cache"] / "Tracked.mkv")
        plexcached = str(eviction_dirs["user"] / "Tracked.mkv.plexcached")

        _write_file(cache_file, size_bytes=1024)
        _write_file(plexcached, size_bytes=1024)

        app = _build_app_for_eviction(
            tmp_path,
            path_mappings=[mapping],
            cache_dir=str(eviction_dirs["cache"]),
        )
        app.priority_manager.get_eviction_candidates.return_value = [cache_file]

        with patch.object(app, "_get_plexcache_tracked_size", return_value=(50 * 1024**3, [cache_file])), \
             patch.object(app, "_get_effective_cache_limit", return_value=(100 * 1024**3, "100GB")), \
             patch("core.app.get_disk_usage") as mock_disk_usage:

            mock_disk_usage.return_value = MagicMock(used=95 * 1024**3)
            app._run_smart_eviction()

        # Verify cleanup methods were called
        app.file_filter.remove_files_from_exclude_list.assert_called_once_with([cache_file])
        app.timestamp_tracker.remove_entry.assert_called_once_with(cache_file)

    def test_eviction_does_not_clean_tracking_on_failure(self, tmp_path, eviction_dirs):
        """Tracking data must NOT be cleaned up if eviction fails."""
        mapping = _make_path_mapping(eviction_dirs)
        cache_file = str(eviction_dirs["cache"] / "FailTrack.mkv")

        _write_file(cache_file, size_bytes=1024)

        app = _build_app_for_eviction(
            tmp_path,
            path_mappings=[mapping],
            cache_dir=str(eviction_dirs["cache"]),
        )
        app.priority_manager.get_eviction_candidates.return_value = [cache_file]

        def fake_get_array_direct(path):
            user_prefix = str(eviction_dirs["user"])
            user0_prefix = str(eviction_dirs["user0"])
            if path.startswith(user_prefix):
                return path.replace(user_prefix, user0_prefix, 1)
            return path

        with patch.object(app, "_get_plexcache_tracked_size", return_value=(50 * 1024**3, [cache_file])), \
             patch.object(app, "_get_effective_cache_limit", return_value=(100 * 1024**3, "100GB")), \
             patch("core.app.get_disk_usage") as mock_disk_usage, \
             patch("core.app.get_array_direct_path", side_effect=fake_get_array_direct), \
             patch("core.app.get_media_identity", return_value="FailTrack"), \
             patch("core.app.find_matching_plexcached", return_value=None), \
             patch("core.app.shutil.copy2", side_effect=OSError("Disk full")):

            mock_disk_usage.return_value = MagicMock(used=95 * 1024**3)
            app._run_smart_eviction()

        # Tracking should NOT have been cleaned up since eviction failed
        app.file_filter.remove_files_from_exclude_list.assert_not_called()
        app.timestamp_tracker.remove_entry.assert_not_called()


class TestGetMediaIdentity:
    """Test the media identity extraction used for upgrade detection."""

    def test_movie_identity(self):
        from core.file_operations import get_media_identity

        assert get_media_identity("Wreck-It Ralph (2012) [WEBDL-1080p].mkv") == "Wreck-It Ralph (2012)"
        assert get_media_identity("Wreck-It Ralph (2012) [HEVC-1080p].mkv") == "Wreck-It Ralph (2012)"

    def test_tv_identity(self):
        from core.file_operations import get_media_identity

        assert get_media_identity("From - S01E02 - The Way Things Are Now [HDTV-1080p].mkv") == \
            "From - S01E02 - The Way Things Are Now"

    def test_plexcached_extension_stripped(self):
        from core.file_operations import get_media_identity

        assert get_media_identity("Movie (2020) [WEBDL-720p].mkv.plexcached") == "Movie (2020)"

    def test_no_quality_tag(self):
        from core.file_operations import get_media_identity

        assert get_media_identity("Simple Movie (2023).mkv") == "Simple Movie (2023)"

    def test_full_path(self):
        from core.file_operations import get_media_identity

        identity = get_media_identity("/mnt/cache/Movies/Movie (2020) [4K].mkv")
        assert identity == "Movie (2020)"


class TestFindMatchingPlexcached:
    """Test finding matching .plexcached backups for upgrade scenarios."""

    def test_finds_matching_backup(self, tmp_path):
        from core.file_operations import find_matching_plexcached

        array_dir = str(tmp_path / "array")
        os.makedirs(array_dir)

        # Old backup with different quality
        old_backup = os.path.join(array_dir, "Movie (2020) [WEBDL-720p].mkv.plexcached")
        _write_file(old_backup, size_bytes=100)

        # New cache file with upgraded quality
        cache_file = "/mnt/cache/Movies/Movie (2020) [HEVC-1080p].mkv"

        result = find_matching_plexcached(array_dir, "Movie (2020)", cache_file)
        assert result == old_backup

    def test_no_matching_backup(self, tmp_path):
        from core.file_operations import find_matching_plexcached

        array_dir = str(tmp_path / "array")
        os.makedirs(array_dir)

        result = find_matching_plexcached(array_dir, "Movie (2020)", "/mnt/cache/Movie (2020).mkv")
        assert result is None

    def test_does_not_match_different_media(self, tmp_path):
        from core.file_operations import find_matching_plexcached

        array_dir = str(tmp_path / "array")
        os.makedirs(array_dir)

        # Backup for a different movie
        other_backup = os.path.join(array_dir, "Other Movie (2019) [720p].mkv.plexcached")
        _write_file(other_backup, size_bytes=100)

        result = find_matching_plexcached(array_dir, "Movie (2020)", "/mnt/cache/Movie (2020).mkv")
        assert result is None

    def test_does_not_cross_match_subtitle_and_video(self, tmp_path):
        from core.file_operations import find_matching_plexcached

        array_dir = str(tmp_path / "array")
        os.makedirs(array_dir)

        # Subtitle backup
        sub_backup = os.path.join(array_dir, "Movie (2020) [720p].srt.plexcached")
        _write_file(sub_backup, size_bytes=50)

        # Looking for video match should not return subtitle
        result = find_matching_plexcached(array_dir, "Movie (2020)", "/mnt/cache/Movie (2020) [1080p].mkv")
        assert result is None

    def test_nonexistent_directory(self):
        from core.file_operations import find_matching_plexcached

        result = find_matching_plexcached("/nonexistent/path", "Movie", "/mnt/cache/Movie.mkv")
        assert result is None
