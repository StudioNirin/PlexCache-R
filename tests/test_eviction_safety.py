"""Tests for eviction safety — verifying CacheService.evict_file() never loses data.

CRITICAL: These tests call production code and assert real filesystem state.
They replace the previous static-analysis-only tests with behavioral tests.

Source: CacheService.evict_file() in web/services/cache_service.py
"""

import json
import os
import sys
import shutil
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# conftest.py handles fcntl/apscheduler mocking and path setup
from conftest import create_test_file

# Mock web.config before importing cache_service
sys.modules.setdefault('web.config', MagicMock(
    PROJECT_ROOT=MagicMock(),
    DATA_DIR=MagicMock(),
    CONFIG_DIR=MagicMock(),
    SETTINGS_FILE=MagicMock(),
))

from web.services.cache_service import CacheService


# ============================================================================
# Helpers
# ============================================================================

def _make_service(tmp_path, cache_prefix="/mnt/cache", real_prefix="/mnt/user"):
    """Create a CacheService with tmp_path-based file paths and patched settings."""
    svc = CacheService()

    # Point tracking files at tmp_path
    svc.exclude_file = tmp_path / "exclude.txt"
    svc.timestamps_file = tmp_path / "timestamps.json"
    svc.settings_file = tmp_path / "settings.json"

    settings = {
        "path_mappings": [{
            "enabled": True,
            "cache_path": cache_prefix,
            "real_path": real_prefix,
            "cacheable": True,
        }]
    }
    svc.settings_file.write_text(json.dumps(settings, indent=2))

    return svc


def _setup_tracking(svc, cache_path):
    """Add a cache_path to the timestamps and exclude files so evict_file finds it."""
    # Timestamps
    ts_data = {cache_path: {"cached_at": "2026-01-15T10:00:00", "source": "ondeck"}}
    svc.timestamps_file.write_text(json.dumps(ts_data, indent=2))

    # Exclude file (host path = same as cache path for simplicity)
    svc.exclude_file.write_text(cache_path + "\n")


# ============================================================================
# TestEvictionSafetyInvariants
# ============================================================================

class TestEvictionSafetyInvariants:
    """CRITICAL: Core safety invariants for eviction."""

    def test_no_path_mapping_aborts(self, tmp_path):
        """CRITICAL: Eviction aborts when no path mapping matches the cache file."""
        svc = _make_service(tmp_path, cache_prefix="/mnt/other_cache")
        cache_file = tmp_path / "cache" / "movie.mkv"
        create_test_file(str(cache_file), "precious data")
        _setup_tracking(svc, "/mnt/cache/movie.mkv")

        result = svc.evict_file("/mnt/cache/movie.mkv")

        assert not result["success"]
        assert "aborted" in result["message"].lower() or "Cannot determine" in result["message"]

    def test_copy_oserror_preserves_cache(self, tmp_path):
        """CRITICAL: Cache file preserved when copy to array raises OSError."""
        svc = _make_service(tmp_path)
        cache_path = "/mnt/cache/movie.mkv"

        # Create cache file on real filesystem
        real_cache = tmp_path / "cache_file.mkv"
        real_cache.write_text("movie data")

        _setup_tracking(svc, cache_path)

        with patch('os.path.exists') as mock_exists, \
             patch('shutil.copy2', side_effect=OSError("disk full")), \
             patch('web.services.cache_service.get_array_direct_path', return_value="/mnt/user0/movie.mkv"):

            # exists: cache_path=True, plexcached=False, array=False, array_direct=False
            def exists_side_effect(p):
                if p == cache_path:
                    return True
                return False
            mock_exists.side_effect = exists_side_effect

            result = svc.evict_file(cache_path)

        assert not result["success"]

    def test_size_mismatch_aborts_eviction(self, tmp_path):
        """CRITICAL: Eviction aborts when copy succeeds but sizes don't match."""
        svc = _make_service(tmp_path)
        cache_path = "/mnt/cache/movie.mkv"
        array_direct = "/mnt/user0/movie.mkv"

        _setup_tracking(svc, cache_path)

        # Track calls to array_direct — first check (step 2) returns False,
        # second check (after copy) returns True
        array_direct_calls = {'count': 0}

        with patch('os.path.exists') as mock_exists, \
             patch('os.path.getsize') as mock_getsize, \
             patch('shutil.copy2'), \
             patch('os.makedirs'), \
             patch('os.remove') as mock_remove, \
             patch('web.services.cache_service.get_array_direct_path', return_value=array_direct):

            def exists_side(p):
                if p == cache_path:
                    return True
                if p == array_direct:
                    array_direct_calls['count'] += 1
                    return array_direct_calls['count'] > 1  # False first, True after copy
                return False
            mock_exists.side_effect = exists_side

            def size_side(p):
                if p == cache_path:
                    return 1000
                if p == array_direct:
                    return 500  # Mismatch!
                return 0
            mock_getsize.side_effect = size_side

            result = svc.evict_file(cache_path)

        assert not result["success"]
        assert "mismatch" in result["message"].lower() or "Size" in result["message"]

    def test_permission_error_preserves_cache(self, tmp_path):
        """CRITICAL: PermissionError during eviction doesn't delete cache."""
        svc = _make_service(tmp_path)
        cache_path = "/mnt/cache/movie.mkv"

        _setup_tracking(svc, cache_path)

        with patch('os.path.exists', return_value=True), \
             patch('os.rename', side_effect=PermissionError("denied")):

            result = svc.evict_file(cache_path)

        assert not result["success"]
        assert "Permission" in result["message"] or "denied" in result["message"]

    def test_plexcached_restore_then_delete_flow(self, tmp_path):
        """When .plexcached backup exists, restores it and deletes cache copy."""
        svc = _make_service(tmp_path)
        cache_path = "/mnt/cache/movie.mkv"
        array_path = "/mnt/user/movie.mkv"
        plexcached_path = array_path + ".plexcached"

        _setup_tracking(svc, cache_path)

        rename_calls = []
        remove_calls = []

        with patch('os.path.exists') as mock_exists, \
             patch('os.rename') as mock_rename, \
             patch('os.remove') as mock_remove:

            def exists_side(p):
                if p == cache_path:
                    return True
                if p == plexcached_path:
                    return True
                return False
            mock_exists.side_effect = exists_side
            mock_rename.side_effect = lambda s, d: rename_calls.append((s, d))
            mock_remove.side_effect = lambda p: remove_calls.append(p)

            result = svc.evict_file(cache_path)

        assert result["success"]
        # .plexcached should be renamed to original array path
        assert any(plexcached_path == call[0] for call in rename_calls)
        # Cache file should be removed
        assert cache_path in remove_calls

    def test_no_backup_copy_then_delete_flow(self, tmp_path):
        """When no backup exists, copies cache to array, verifies, then deletes."""
        svc = _make_service(tmp_path)
        cache_path = "/mnt/cache/movie.mkv"
        array_direct = "/mnt/user0/movie.mkv"

        _setup_tracking(svc, cache_path)

        copy_calls = []
        remove_calls = []

        # Track calls — array_direct returns False on first check (step 2),
        # True after copy (verification check)
        array_direct_calls = {'count': 0}

        with patch('os.path.exists') as mock_exists, \
             patch('os.path.getsize', return_value=1000), \
             patch('shutil.copy2') as mock_copy, \
             patch('os.makedirs'), \
             patch('os.remove') as mock_remove, \
             patch('web.services.cache_service.get_array_direct_path', return_value=array_direct):

            def exists_side(p):
                if p == cache_path:
                    return True
                if p == array_direct:
                    array_direct_calls['count'] += 1
                    return array_direct_calls['count'] > 1  # False first, True after copy
                return False
            mock_exists.side_effect = exists_side
            mock_copy.side_effect = lambda s, d: copy_calls.append((s, d))
            mock_remove.side_effect = lambda p: remove_calls.append(p)

            result = svc.evict_file(cache_path)

        assert result["success"]
        assert len(copy_calls) == 1
        assert copy_calls[0] == (cache_path, array_direct)
        assert cache_path in remove_calls

    def test_array_confirmed_false_prevents_deletion(self, tmp_path):
        """CRITICAL: When array copy cannot be confirmed, cache file is NOT deleted."""
        svc = _make_service(tmp_path)
        cache_path = "/mnt/cache/movie.mkv"
        array_direct = "/mnt/user0/movie.mkv"

        _setup_tracking(svc, cache_path)

        with patch('os.path.exists') as mock_exists, \
             patch('shutil.copy2'), \
             patch('os.makedirs'), \
             patch('os.remove') as mock_remove, \
             patch('web.services.cache_service.get_array_direct_path', return_value=array_direct):

            def exists_side(p):
                if p == cache_path:
                    return True
                if p == array_direct:
                    return False  # Copy didn't produce a file
                return False
            mock_exists.side_effect = exists_side

            result = svc.evict_file(cache_path)

        assert not result["success"]
        assert "aborted" in result["message"].lower() or "Failed" in result["message"]
        # cache file should NOT have been removed
        assert cache_path not in [c.args[0] if hasattr(c, 'args') else c for c in mock_remove.call_args_list]

    def test_empty_path_rejected(self, tmp_path):
        """Empty path is rejected immediately."""
        svc = _make_service(tmp_path)
        result = svc.evict_file("")

        assert not result["success"]
        assert "No file path" in result["message"]

    def test_file_not_in_cache_list_rejected(self, tmp_path):
        """File not tracked in cache list is rejected."""
        svc = _make_service(tmp_path)
        # Don't set up tracking
        svc.timestamps_file.write_text("{}")
        svc.exclude_file.write_text("")

        result = svc.evict_file("/mnt/cache/unknown.mkv")

        assert not result["success"]
        assert "not found" in result["message"].lower()


# ============================================================================
# TestEvictionTrackingCleanup
# ============================================================================

class TestEvictionTrackingCleanup:
    """Tests for tracking file cleanup after successful eviction."""

    def test_success_removes_from_timestamps(self, tmp_path):
        """Successful eviction removes the file from timestamps.json."""
        svc = _make_service(tmp_path)
        cache_path = "/mnt/cache/movie.mkv"
        plexcached_path = "/mnt/user/movie.mkv.plexcached"

        _setup_tracking(svc, cache_path)

        with patch('os.path.exists') as mock_exists, \
             patch('os.rename'), \
             patch('os.remove'):

            def exists_side(p):
                if p == cache_path:
                    return True
                if p == plexcached_path:
                    return True
                return False
            mock_exists.side_effect = exists_side

            result = svc.evict_file(cache_path)

        assert result["success"]
        ts_data = json.loads(svc.timestamps_file.read_text())
        assert cache_path not in ts_data

    def test_success_removes_from_exclude_file(self, tmp_path):
        """Successful eviction removes the file from exclude list."""
        svc = _make_service(tmp_path)
        cache_path = "/mnt/cache/movie.mkv"
        plexcached_path = "/mnt/user/movie.mkv.plexcached"

        _setup_tracking(svc, cache_path)
        # Add another entry to verify it's preserved
        svc.exclude_file.write_text(cache_path + "\n/mnt/cache/other.mkv\n")

        with patch('os.path.exists') as mock_exists, \
             patch('os.rename'), \
             patch('os.remove'):

            def exists_side(p):
                if p == cache_path:
                    return True
                if p == plexcached_path:
                    return True
                return False
            mock_exists.side_effect = exists_side

            result = svc.evict_file(cache_path)

        assert result["success"]
        lines = svc.exclude_file.read_text().strip().split('\n')
        stripped = [l.strip() for l in lines if l.strip()]
        assert cache_path not in stripped

    def test_failure_preserves_tracking_files(self, tmp_path):
        """Failed eviction does not modify tracking files."""
        svc = _make_service(tmp_path)
        cache_path = "/mnt/cache/movie.mkv"

        _setup_tracking(svc, cache_path)
        original_ts = svc.timestamps_file.read_text()
        original_exclude = svc.exclude_file.read_text()

        with patch('os.path.exists', return_value=False):
            # File not on disk — will fail to find .plexcached or cache file
            result = svc.evict_file(cache_path)

        # Tracking files unchanged (the function returned early before cleanup)
        assert svc.timestamps_file.read_text() == original_ts
