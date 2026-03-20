"""
Tests for PlexCache Quota enforcement in _apply_cache_limit().

Tests the plexcache_quota constraint which limits the total size of
PlexCache-managed files (from the exclude list), independent of total drive usage.
"""

import os
import sys
import tempfile
import pytest
from unittest.mock import patch, MagicMock, PropertyMock
from collections import namedtuple

# conftest.py handles fcntl mocking and path setup.
for _mod_name in [
    'plexapi', 'plexapi.server', 'plexapi.video', 'plexapi.myplex',
    'plexapi.exceptions', 'requests',
]:
    if _mod_name not in sys.modules:
        sys.modules[_mod_name] = MagicMock()


DiskUsage = namedtuple('DiskUsage', ['total', 'used', 'free'])

GB = 1024 ** 3


def _build_app(tmp_path, cache_limit_bytes=0, min_free_bytes=0, quota_bytes=0,
               drive_total=1000*GB, drive_used=400*GB, tracked_size=200*GB):
    """Build a minimal PlexCacheApp for _apply_cache_limit() testing."""
    from conftest import create_test_file
    from core.app import PlexCacheApp

    cache_dir = str(tmp_path / "cache")
    os.makedirs(cache_dir, exist_ok=True)

    config_manager = MagicMock()
    config_manager.cache.cache_limit_bytes = cache_limit_bytes
    config_manager.cache.min_free_space_bytes = min_free_bytes
    config_manager.cache.plexcache_quota_bytes = quota_bytes
    config_manager.cache.cache_drive_size_bytes = 0

    exclude_file = tmp_path / "exclude.txt"
    exclude_mock = MagicMock()
    exclude_mock.exists.return_value = False
    config_manager.get_cached_files_file.return_value = exclude_mock

    app = object.__new__(PlexCacheApp)
    app.config_manager = config_manager
    app.dry_run = False
    app.file_filter = None
    app._stop_requested = False

    # Create test media files (each 10GB)
    files = []
    for i in range(5):
        f = os.path.join(cache_dir, f"movie_{i}.mkv")
        create_test_file(f, size_bytes=1024)  # Small files, we mock getsize
        files.append(f)

    # Mock disk_usage and file sizes
    disk = DiskUsage(total=drive_total, used=drive_used, free=drive_total - drive_used)

    return app, cache_dir, files, disk, tracked_size


class TestPlexcacheQuotaEnforcement:
    """Tests for plexcache_quota as a constraint in _apply_cache_limit()."""

    def test_quota_disabled_no_effect(self, tmp_path):
        """When plexcache_quota is empty/0, it has no effect."""
        app, cache_dir, files, disk, _ = _build_app(
            tmp_path, quota_bytes=0, cache_limit_bytes=0, min_free_bytes=0
        )

        # No constraints at all -> all files pass through
        result = app._apply_cache_limit(files, cache_dir)
        assert result == files

    def test_quota_only_constraint(self, tmp_path):
        """When only quota is set, it limits based on tracked size."""
        app, cache_dir, files, disk, tracked_size = _build_app(
            tmp_path, quota_bytes=250*GB, tracked_size=200*GB
        )

        # Mock disk usage and tracked size
        with patch('core.app.get_disk_usage', return_value=disk), \
             patch.object(app, '_get_plexcache_tracked_size', return_value=(200*GB, [])), \
             patch('os.path.getsize', return_value=10*GB):
            result = app._apply_cache_limit(files, cache_dir)

        # 250GB quota - 200GB tracked = 50GB available
        # Each file is 10GB, so 5 files fit
        assert len(result) == 5

    def test_quota_limits_files(self, tmp_path):
        """When quota has limited space, only fitting files are cached."""
        app, cache_dir, files, disk, _ = _build_app(
            tmp_path, quota_bytes=220*GB
        )

        with patch('core.app.get_disk_usage', return_value=disk), \
             patch.object(app, '_get_plexcache_tracked_size', return_value=(200*GB, [])), \
             patch('os.path.getsize', return_value=10*GB):
            result = app._apply_cache_limit(files, cache_dir)

        # 220GB quota - 200GB tracked = 20GB available
        # Each file is 10GB, so only 2 files fit
        assert len(result) == 2

    def test_quota_exceeded_returns_empty(self, tmp_path):
        """When tracked size already exceeds quota, no files are cached."""
        app, cache_dir, files, disk, _ = _build_app(
            tmp_path, quota_bytes=150*GB
        )

        with patch('core.app.get_disk_usage', return_value=disk), \
             patch.object(app, '_get_plexcache_tracked_size', return_value=(200*GB, [])), \
             patch('os.path.getsize', return_value=10*GB):
            result = app._apply_cache_limit(files, cache_dir)

        # 150GB quota - 200GB tracked = -50GB available -> empty
        assert result == []

    def test_quota_more_restrictive_than_cache_limit(self, tmp_path):
        """When quota is more restrictive than cache_limit, quota wins."""
        app, cache_dir, files, disk, _ = _build_app(
            tmp_path,
            cache_limit_bytes=800*GB,  # 800GB limit, 400GB used = 400GB available
            quota_bytes=220*GB,  # 220GB quota, 200GB tracked = 20GB available
        )

        with patch('core.app.get_disk_usage', return_value=disk), \
             patch.object(app, '_get_plexcache_tracked_size', return_value=(200*GB, [])), \
             patch('os.path.getsize', return_value=10*GB):
            result = app._apply_cache_limit(files, cache_dir)

        # quota is more restrictive: 20GB vs 400GB
        # Only 2 files fit (20GB / 10GB each)
        assert len(result) == 2

    def test_cache_limit_more_restrictive_than_quota(self, tmp_path):
        """When cache_limit is more restrictive than quota, cache_limit wins."""
        app, cache_dir, files, disk, _ = _build_app(
            tmp_path,
            cache_limit_bytes=410*GB,  # 410GB limit, 400GB used = 10GB available
            quota_bytes=500*GB,  # 500GB quota, 200GB tracked = 300GB available
        )

        with patch('core.app.get_disk_usage', return_value=disk), \
             patch.object(app, '_get_plexcache_tracked_size', return_value=(200*GB, [])), \
             patch('os.path.getsize', return_value=10*GB):
            result = app._apply_cache_limit(files, cache_dir)

        # cache_limit is more restrictive: 10GB vs 300GB
        # Only 1 file fits (10GB / 10GB each)
        assert len(result) == 1

    def test_min_free_space_more_restrictive_than_quota(self, tmp_path):
        """When min_free_space is more restrictive than quota, min_free wins."""
        drive_free = 15 * GB
        drive_used = 985 * GB
        disk = DiskUsage(total=1000*GB, used=drive_used, free=drive_free)

        app, cache_dir, files, _, _ = _build_app(
            tmp_path,
            min_free_bytes=10*GB,  # 15GB free - 10GB floor = 5GB available
            quota_bytes=500*GB,    # 500GB quota - 200GB tracked = 300GB available
        )

        with patch('core.app.get_disk_usage', return_value=disk), \
             patch.object(app, '_get_plexcache_tracked_size', return_value=(200*GB, [])), \
             patch('os.path.getsize', return_value=10*GB):
            result = app._apply_cache_limit(files, cache_dir)

        # min_free is most restrictive: 5GB available, file is 10GB -> 0 fit
        assert len(result) == 0

    def test_percentage_quota(self, tmp_path):
        """Percentage-based quota is resolved against drive total."""
        app, cache_dir, files, disk, _ = _build_app(
            tmp_path,
            quota_bytes=-25,  # -25 means 25% of drive
        )

        with patch('core.app.get_disk_usage', return_value=disk), \
             patch.object(app, '_get_plexcache_tracked_size', return_value=(200*GB, [])), \
             patch('os.path.getsize', return_value=10*GB):
            result = app._apply_cache_limit(files, cache_dir)

        # 25% of 1000GB = 250GB quota, 200GB tracked = 50GB available
        # 5 files * 10GB = 50GB -> all fit
        assert len(result) == 5

    def test_all_three_constraints_quota_wins(self, tmp_path):
        """When all three constraints are active, the most restrictive wins."""
        app, cache_dir, files, disk, _ = _build_app(
            tmp_path,
            cache_limit_bytes=800*GB,   # 400GB available (800 - 400 used)
            min_free_bytes=100*GB,      # 500GB available (600 free - 100 floor)
            quota_bytes=215*GB,         # 15GB available (215 - 200 tracked)
        )

        with patch('core.app.get_disk_usage', return_value=disk), \
             patch.object(app, '_get_plexcache_tracked_size', return_value=(200*GB, [])), \
             patch('os.path.getsize', return_value=10*GB):
            result = app._apply_cache_limit(files, cache_dir)

        # quota is most restrictive: 15GB available
        # Only 1 file fits (10GB <= 15GB, then 5GB left < 10GB)
        assert len(result) == 1


class TestPlexcacheQuotaConfig:
    """Tests for plexcache_quota config parsing."""

    def test_config_default_empty(self):
        """Default plexcache_quota is empty string."""
        from core.config import CacheConfig
        config = CacheConfig()
        assert config.plexcache_quota == ""
        assert config.plexcache_quota_bytes == 0

    def test_parse_quota_gb(self):
        """Parse absolute GB value for quota."""
        from core.config import ConfigManager
        cm = object.__new__(ConfigManager)
        assert cm._parse_cache_limit("500GB") == 500 * GB

    def test_parse_quota_percentage(self):
        """Parse percentage value for quota (returns negative)."""
        from core.config import ConfigManager
        cm = object.__new__(ConfigManager)
        assert cm._parse_cache_limit("50%") == -50

    def test_parse_quota_empty(self):
        """Parse empty string returns 0 (disabled)."""
        from core.config import ConfigManager
        cm = object.__new__(ConfigManager)
        assert cm._parse_cache_limit("") == 0

    def test_parse_quota_zero(self):
        """Parse "0" returns 0 (disabled)."""
        from core.config import ConfigManager
        cm = object.__new__(ConfigManager)
        assert cm._parse_cache_limit("0") == 0


class TestGetEffectivePlexcacheQuota:
    """Tests for _get_effective_plexcache_quota() method."""

    def test_disabled_returns_zero(self, tmp_path):
        """When quota is 0, returns (0, None)."""
        app, cache_dir, _, _, _ = _build_app(tmp_path, quota_bytes=0)
        result = app._get_effective_plexcache_quota(cache_dir)
        assert result == (0, None)

    def test_absolute_value(self, tmp_path):
        """Absolute byte value is returned directly."""
        app, cache_dir, _, disk, _ = _build_app(tmp_path, quota_bytes=500*GB)
        result = app._get_effective_plexcache_quota(cache_dir)
        assert result[0] == 500 * GB
        assert "500.0GB" in result[1]

    def test_percentage_resolved(self, tmp_path):
        """Percentage is resolved against drive total."""
        app, cache_dir, _, disk, _ = _build_app(tmp_path, quota_bytes=-25)

        with patch('core.app.get_disk_usage', return_value=disk):
            result = app._get_effective_plexcache_quota(cache_dir)

        assert result[0] == 250 * GB  # 25% of 1000GB
        assert "25%" in result[1]
