"""
Tests for web/services/cache_service.py

Covers:
- evict_file() array path resolution, error handling, and safety checks
- _parse_size_bytes() parsing various size formats
- Storage/disk usage error handling
- Path translation helpers
- _format_size() output
- _is_subtitle_file() detection
"""

import json
import os
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock, call

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
    ],
    "cache_eviction_mode": "smart",
    "cache_drive_size": "",
}


def _make_service(tmp_path, settings=None):
    """Create a CacheService with test settings on disk."""
    if settings is None:
        settings = MOCK_SETTINGS

    settings_file = tmp_path / "plexcache_settings.json"
    settings_file.write_text(json.dumps(settings), encoding="utf-8")

    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    exclude_file = tmp_path / "plexcache_cached_files.txt"
    timestamps_file = data_dir / "timestamps.json"
    ondeck_file = data_dir / "ondeck_tracker.json"
    watchlist_file = data_dir / "watchlist_tracker.json"

    with patch("web.services.cache_service.SETTINGS_FILE", settings_file), \
         patch("web.services.cache_service.CONFIG_DIR", tmp_path), \
         patch("web.services.cache_service.DATA_DIR", data_dir):
        from web.services.cache_service import CacheService
        svc = CacheService()

    # Override paths that __init__ resolved from constants
    svc.settings_file = settings_file
    svc.exclude_file = exclude_file
    svc.timestamps_file = timestamps_file
    svc.ondeck_file = ondeck_file
    svc.watchlist_file = watchlist_file

    return svc


# ============================================================================
# _parse_size_bytes() tests
# ============================================================================

class TestParseSizeBytes:
    """Test the module-level _parse_size_bytes() helper."""

    @staticmethod
    def _parse(value):
        from core.system_utils import parse_size_bytes
        return parse_size_bytes(value)

    # --- TB ---

    def test_parse_tb_suffix(self):
        result = self._parse("1TB")
        assert result == 1 * 1024 ** 4

    def test_parse_t_suffix(self):
        result = self._parse("2T")
        assert result == 2 * 1024 ** 4

    def test_parse_fractional_tb(self):
        result = self._parse("1.5TB")
        assert result == int(1.5 * 1024 ** 4)

    # --- GB ---

    def test_parse_gb_suffix(self):
        result = self._parse("500GB")
        assert result == 500 * 1024 ** 3

    def test_parse_g_suffix(self):
        result = self._parse("500G")
        assert result == 500 * 1024 ** 3

    # --- MB ---

    def test_parse_mb_suffix(self):
        result = self._parse("100MB")
        assert result == 100 * 1024 ** 2

    def test_parse_m_suffix(self):
        result = self._parse("100M")
        assert result == 100 * 1024 ** 2

    # --- Bare numbers default to GB ---

    def test_bare_number_defaults_to_gb(self):
        result = self._parse("250")
        assert result == int(250 * 1024 ** 3)

    # --- Empty / zero / invalid ---

    def test_empty_string(self):
        assert self._parse("") == 0

    def test_zero_string(self):
        assert self._parse("0") == 0

    def test_none_value(self):
        assert self._parse(None) == 0

    def test_invalid_text(self):
        assert self._parse("notanumber") == 0

    def test_whitespace_stripped(self):
        result = self._parse("  500GB  ")
        assert result == 500 * 1024 ** 3

    # --- Case insensitive ---

    def test_lowercase_gb(self):
        result = self._parse("500gb")
        assert result == 500 * 1024 ** 3

    def test_mixed_case(self):
        result = self._parse("1Tb")
        assert result == 1 * 1024 ** 4


# ============================================================================
# evict_file() tests
# ============================================================================

class TestEvictFile:

    def _setup_timestamps(self, svc, paths):
        """Write a timestamps.json file listing the given paths."""
        data = {p: {"cached_at": "2026-01-20T10:00:00", "source": "ondeck"} for p in paths}
        svc.timestamps_file.write_text(json.dumps(data), encoding="utf-8")

    def _setup_exclude(self, svc, paths):
        """Write an exclude file listing the given paths (as host paths)."""
        lines = [svc._translate_container_to_host_path(p) + "\n" for p in paths]
        svc.exclude_file.write_text("".join(lines), encoding="utf-8")

    # --- Error cases ---

    def test_empty_path_returns_error(self, tmp_path):
        svc = _make_service(tmp_path)
        result = svc.evict_file("")
        assert result["success"] is False
        assert "No file path" in result["message"]

    def test_file_not_in_cache_list_returns_error(self, tmp_path):
        svc = _make_service(tmp_path)
        # No timestamps file, so no cached files
        result = svc.evict_file("/mnt/cache/media/Movies/Ghost.mkv")
        assert result["success"] is False
        assert "not found in cache list" in result["message"]

    @patch("os.path.exists", return_value=False)
    def test_no_array_path_determinable_returns_error(self, mock_exists, tmp_path):
        """If cache_path doesn't match any mapping, should abort safely."""
        settings = {"path_mappings": []}
        svc = _make_service(tmp_path, settings)

        cache_path = "/unknown/Movies/Film.mkv"
        self._setup_timestamps(svc, [cache_path])

        result = svc.evict_file(cache_path)
        assert result["success"] is False
        # When no mapping is found, array_path is None and the code safely
        # aborts because array_confirmed stays False
        assert "aborted" in result["message"].lower() or "not confirmed" in result["message"].lower()

    # --- Uses get_array_direct_path ---

    @patch("os.remove")
    @patch("os.path.exists")
    def test_uses_get_array_direct_path_for_checks(
        self, mock_exists, mock_remove, tmp_path
    ):
        """evict_file must use get_array_direct_path for array existence checks."""
        svc = _make_service(tmp_path)
        cache_path = "/mnt/cache/media/Movies/Film.mkv"
        array_path = "/mnt/user/media/Movies/Film.mkv"
        plexcached_path = array_path + ".plexcached"

        self._setup_timestamps(svc, [cache_path])
        self._setup_exclude(svc, [cache_path])

        # .plexcached exists -> rename, set array_confirmed
        def exists_side_effect(path):
            if path == plexcached_path:
                return True
            if path == cache_path:
                return True
            return False

        mock_exists.side_effect = exists_side_effect

        with patch("os.rename") as mock_rename:
            result = svc.evict_file(cache_path)

        assert result["success"] is True
        # Should have renamed .plexcached back to original
        mock_rename.assert_called_once_with(plexcached_path, array_path)

    @patch("os.remove")
    @patch("os.path.exists")
    def test_array_confirmed_false_prevents_deletion(
        self, mock_exists, mock_remove, tmp_path
    ):
        """When array_confirmed stays False, cache file must NOT be deleted."""
        svc = _make_service(tmp_path)
        cache_path = "/mnt/cache/media/Movies/Film.mkv"

        self._setup_timestamps(svc, [cache_path])

        # No .plexcached, no array file, no cache file to copy from
        mock_exists.return_value = False

        result = svc.evict_file(cache_path)

        assert result["success"] is False
        assert "Array copy not confirmed" in result["message"]
        # os.remove should NOT have been called for cache_path
        for c in mock_remove.call_args_list:
            assert c[0][0] != cache_path

    # --- Permission error ---

    @patch("os.path.exists")
    def test_permission_error_returns_error(self, mock_exists, tmp_path):
        svc = _make_service(tmp_path)
        cache_path = "/mnt/cache/media/Movies/Film.mkv"
        plexcached_path = "/mnt/user/media/Movies/Film.mkv.plexcached"

        self._setup_timestamps(svc, [cache_path])

        mock_exists.side_effect = lambda p: p == plexcached_path

        with patch("os.rename", side_effect=PermissionError("denied")):
            result = svc.evict_file(cache_path)

        assert result["success"] is False
        assert "Permission denied" in result["message"]

    # --- Size mismatch during copy ---

    @patch("os.path.getsize")
    @patch("os.makedirs")
    @patch("shutil.copy2")
    @patch("os.path.exists")
    @patch("os.remove")
    def test_size_mismatch_aborts_eviction(
        self, mock_remove, mock_exists, mock_copy2, mock_makedirs,
        mock_getsize, tmp_path
    ):
        """Size mismatch after copy to array -> abort and remove failed copy."""
        svc = _make_service(tmp_path)
        cache_path = "/mnt/cache/media/Movies/Film.mkv"
        array_path = "/mnt/user/media/Movies/Film.mkv"
        # get_array_direct_path converts /mnt/user/ to /mnt/user0/
        array_direct_path = "/mnt/user0/media/Movies/Film.mkv"

        self._setup_timestamps(svc, [cache_path])

        def exists_side_effect(path):
            if path.endswith(".plexcached"):
                return False
            if path == array_direct_path:
                # After copy, file exists
                return mock_copy2.called
            if path == cache_path:
                return True
            return False

        mock_exists.side_effect = exists_side_effect
        # Size mismatch: cache=5000, array copy=3000
        mock_getsize.side_effect = lambda p: 5000 if p == cache_path else 3000

        with patch("web.services.cache_service.get_array_direct_path",
                    side_effect=lambda p: p.replace("/mnt/user/", "/mnt/user0/")):
            result = svc.evict_file(cache_path)

        assert result["success"] is False
        assert "Size mismatch" in result["message"]
        # Should remove the bad copy from array
        mock_remove.assert_called_once_with(array_direct_path)

    # --- Successful eviction with no backup ---

    @patch("os.path.getsize")
    @patch("os.makedirs")
    @patch("shutil.copy2")
    @patch("os.path.exists")
    @patch("os.remove")
    def test_successful_eviction_copies_to_array_direct(
        self, mock_remove, mock_exists, mock_copy2, mock_makedirs,
        mock_getsize, tmp_path
    ):
        """When no backup exists, copy to /mnt/user0/ (not /mnt/user/)."""
        svc = _make_service(tmp_path)
        cache_path = "/mnt/cache/media/Movies/Film.mkv"
        array_direct_path = "/mnt/user0/media/Movies/Film.mkv"

        self._setup_timestamps(svc, [cache_path])
        self._setup_exclude(svc, [cache_path])

        def exists_side_effect(path):
            if path.endswith(".plexcached"):
                return False
            if path == array_direct_path:
                return mock_copy2.called
            if path == cache_path:
                return True
            return False

        mock_exists.side_effect = exists_side_effect
        mock_getsize.return_value = 5000  # same size

        with patch("web.services.cache_service.get_array_direct_path",
                    side_effect=lambda p: p.replace("/mnt/user/", "/mnt/user0/")):
            result = svc.evict_file(cache_path)

        assert result["success"] is True
        assert "Evicted" in result["message"]
        # Copy destination must be /mnt/user0/ not /mnt/user/
        mock_copy2.assert_called_once_with(cache_path, array_direct_path)

    # --- Success with .plexcached backup ---

    @patch("os.rename")
    @patch("os.remove")
    @patch("os.path.exists")
    def test_eviction_with_plexcached_backup(
        self, mock_exists, mock_remove, mock_rename, tmp_path
    ):
        svc = _make_service(tmp_path)
        cache_path = "/mnt/cache/media/Movies/Film.mkv"
        array_path = "/mnt/user/media/Movies/Film.mkv"
        plexcached = array_path + ".plexcached"

        self._setup_timestamps(svc, [cache_path])
        self._setup_exclude(svc, [cache_path])

        def exists_side_effect(path):
            if path == plexcached:
                return True
            if path == cache_path:
                return True
            return False

        mock_exists.side_effect = exists_side_effect

        result = svc.evict_file(cache_path)

        assert result["success"] is True
        mock_rename.assert_called_once_with(plexcached, array_path)
        mock_remove.assert_called_once_with(cache_path)


# ============================================================================
# Storage / disk usage error handling
# ============================================================================

class TestStorageDiskUsageErrorHandling:

    @patch("os.path.exists", return_value=True)
    def test_get_cache_stats_handles_os_error(self, mock_exists, tmp_path):
        """When get_disk_usage raises OSError, stats should not crash."""
        svc = _make_service(tmp_path)

        # Write minimal data files so the service can load them
        svc.timestamps_file.write_text("{}", encoding="utf-8")
        svc.ondeck_file.write_text("{}", encoding="utf-8")
        svc.watchlist_file.write_text("{}", encoding="utf-8")

        with patch("web.services.cache_service.get_disk_usage",
                    side_effect=OSError("disk error")):
            stats = svc.get_cache_stats()

        assert stats["cache_size_bytes"] == 0
        assert stats["cache_limit_bytes"] == 0
        assert stats["usage_percent"] == 0

    @patch("os.path.exists", return_value=False)
    def test_get_cache_stats_handles_missing_cache_dir(
        self, mock_exists, tmp_path
    ):
        """If cache_dir does not exist, should return zero stats."""
        svc = _make_service(tmp_path)
        svc.timestamps_file.write_text("{}", encoding="utf-8")
        svc.ondeck_file.write_text("{}", encoding="utf-8")
        svc.watchlist_file.write_text("{}", encoding="utf-8")

        stats = svc.get_cache_stats()
        assert stats["cache_size_bytes"] == 0


# ============================================================================
# _format_size() tests
# ============================================================================

class TestFormatSize:

    def test_zero(self):
        from core.system_utils import format_bytes
        assert format_bytes(0) == "0 B"

    def test_bytes(self):
        from core.system_utils import format_bytes
        assert format_bytes(512) == "512 B"

    def test_megabytes(self):
        from core.system_utils import format_bytes
        result = format_bytes(5 * 1024 ** 2)
        assert "MB" in result

    def test_terabytes(self):
        from core.system_utils import format_bytes
        result = format_bytes(3 * 1024 ** 4)
        assert "TB" in result


# ============================================================================
# _is_subtitle_file() tests
# ============================================================================

class TestIsSubtitleFile:

    def test_srt_detected(self, tmp_path):
        svc = _make_service(tmp_path)
        assert svc._is_subtitle_file("movie.srt") is True

    def test_ass_detected(self, tmp_path):
        svc = _make_service(tmp_path)
        assert svc._is_subtitle_file("movie.ass") is True

    def test_language_suffix_detected(self, tmp_path):
        svc = _make_service(tmp_path)
        assert svc._is_subtitle_file("movie.en.srt") is True

    def test_mkv_not_subtitle(self, tmp_path):
        svc = _make_service(tmp_path)
        assert svc._is_subtitle_file("movie.mkv") is False


# ============================================================================
# _get_video_base_name() tests
# ============================================================================

class TestGetVideoBaseName:

    def test_simple_srt(self, tmp_path):
        svc = _make_service(tmp_path)
        assert svc._get_video_base_name("/path/Movie.srt") == "Movie"

    def test_language_code_stripped(self, tmp_path):
        svc = _make_service(tmp_path)
        assert svc._get_video_base_name("/path/Movie.en.srt") == "Movie"

    def test_region_code_stripped(self, tmp_path):
        svc = _make_service(tmp_path)
        assert svc._get_video_base_name("/path/Movie.pt-br.srt") == "Movie"


# ============================================================================
# Path translation tests
# ============================================================================

class TestCacheServicePathTranslation:

    def test_container_to_host(self, tmp_path):
        svc = _make_service(tmp_path)
        result = svc._translate_container_to_host_path(
            "/mnt/cache/media/Movies/Film.mkv"
        )
        assert result == "/mnt/cache_downloads/media/Movies/Film.mkv"

    def test_host_to_container(self, tmp_path):
        svc = _make_service(tmp_path)
        result = svc._translate_host_to_container_path(
            "/mnt/cache_downloads/media/Movies/Film.mkv"
        )
        assert result == "/mnt/cache/media/Movies/Film.mkv"

    def test_unknown_path_unchanged(self, tmp_path):
        svc = _make_service(tmp_path)
        result = svc._translate_container_to_host_path("/some/other/path")
        assert result == "/some/other/path"


# ============================================================================
# _get_cache_dir() and _get_cache_dir_for_display() tests
# ============================================================================

class TestGetCacheDir:

    def test_uses_first_cacheable_mapping(self, tmp_path):
        svc = _make_service(tmp_path)
        result = svc._get_cache_dir()
        assert result == "/mnt/cache/media/Movies"

    def test_falls_back_to_cache_dir_setting(self, tmp_path):
        settings = {"cache_dir": "/mnt/cache/downloads"}
        svc = _make_service(tmp_path, settings)
        result = svc._get_cache_dir()
        assert result == "/mnt/cache/downloads"

    def test_display_prefers_host_cache_path(self, tmp_path):
        svc = _make_service(tmp_path)
        result = svc._get_cache_dir_for_display()
        # Should use host_cache_path and find common parent
        assert "cache_downloads" in result


# ============================================================================
# evict_files() batch tests
# ============================================================================

class TestEvictFiles:

    def test_returns_counts(self, tmp_path):
        svc = _make_service(tmp_path)

        with patch.object(svc, "evict_file") as mock_evict:
            mock_evict.side_effect = [
                {"success": True, "message": "ok"},
                {"success": False, "message": "failed"},
                {"success": True, "message": "ok"},
            ]
            result = svc.evict_files(["a", "b", "c"])

        assert result["success"] is True
        assert result["evicted_count"] == 2
        assert result["total_count"] == 3
        assert len(result["errors"]) == 1


# ============================================================================
# _remove_from_exclude_file / _remove_from_timestamps tests
# ============================================================================

class TestRemoveFromTrackingFiles:

    def test_remove_from_exclude(self, tmp_path):
        svc = _make_service(tmp_path)
        host_path = "/mnt/cache_downloads/media/Movies/Film.mkv"
        svc.exclude_file.write_text(
            host_path + "\n/mnt/cache_downloads/media/TV/Show.mkv\n",
            encoding="utf-8",
        )
        svc._remove_from_exclude_file("/mnt/cache/media/Movies/Film.mkv")

        remaining = svc.exclude_file.read_text(encoding="utf-8")
        assert host_path not in remaining
        assert "Show.mkv" in remaining

    def test_remove_from_timestamps(self, tmp_path):
        svc = _make_service(tmp_path)
        ts_data = {
            "/mnt/cache/media/Movies/Film.mkv": "2026-01-20T10:00:00",
            "/mnt/cache/media/TV/Show.mkv": "2026-01-21T10:00:00",
        }
        svc.timestamps_file.write_text(json.dumps(ts_data), encoding="utf-8")

        svc._remove_from_timestamps("/mnt/cache/media/Movies/Film.mkv")

        result = json.loads(svc.timestamps_file.read_text(encoding="utf-8"))
        assert "/mnt/cache/media/Movies/Film.mkv" not in result
        assert "/mnt/cache/media/TV/Show.mkv" in result

    def test_remove_nonexistent_path_no_error(self, tmp_path):
        svc = _make_service(tmp_path)
        ts_data = {"/mnt/cache/media/Movies/Film.mkv": "2026-01-20T10:00:00"}
        svc.timestamps_file.write_text(json.dumps(ts_data), encoding="utf-8")

        # Should not raise
        svc._remove_from_timestamps("/mnt/cache/nonexistent.mkv")

    def test_remove_from_missing_exclude_file_no_error(self, tmp_path):
        svc = _make_service(tmp_path)
        # exclude_file does not exist on disk
        svc._remove_from_exclude_file("/mnt/cache/media/Movies/Film.mkv")
        # Should complete without error


# ============================================================================
# Atomic tracker writes in _transfer_tracking_data()
# ============================================================================

class TestTransferTrackingAtomicWrites:
    """Verify _transfer_upgrade_tracking() uses save_json_atomically() for all tracker writes."""

    def _call_transfer(self, svc, **overrides):
        """Helper to call _transfer_upgrade_tracking with standard args."""
        defaults = dict(
            old_cache_path="/mnt/cache/media/Movies/Old.mkv",
            old_real_path="/mnt/user/media/Movies/Old.mkv",
            old_plex_path="/data/media/Movies/Old.mkv",
            new_cache_path="/mnt/cache/media/Movies/New.mkv",
            new_real_path="/mnt/user/media/Movies/New.mkv",
            new_plex_path="/data/media/Movies/New.mkv",
            rating_key="12345",
            settings={"create_plexcached_backups": False},
            path_mappings=[],
        )
        defaults.update(overrides)
        return svc._transfer_upgrade_tracking(**defaults)

    def test_timestamps_written_atomically(self, tmp_path):
        """Timestamps file must be written via save_json_atomically()."""
        svc = _make_service(tmp_path)

        old_cache = "/mnt/cache/media/Movies/Old.mkv"
        svc.timestamps_file.write_text(json.dumps({
            old_cache: {"cached_at": "2026-01-01T00:00:00", "source": "ondeck"}
        }), encoding="utf-8")

        # Get the actual module reference for patching
        import web.services.cache_service as cs_mod
        mock_atomic = MagicMock()
        original = cs_mod.save_json_atomically

        with patch.object(cs_mod, "save_json_atomically", mock_atomic), \
             patch.object(cs_mod, "remove_from_exclude_file"), \
             patch.object(cs_mod, "remove_from_timestamps_file"), \
             patch.object(cs_mod, "get_array_direct_path", side_effect=lambda p: p.replace("/mnt/user/", "/mnt/user0/")), \
             patch.object(cs_mod, "get_media_identity", return_value="Old"), \
             patch.object(cs_mod, "find_matching_plexcached", return_value=None):
            self._call_transfer(svc)

        ts_calls = [c for c in mock_atomic.call_args_list
                    if c[1].get("label") == "timestamps" or (len(c[0]) >= 3 and c[0][2] == "timestamps")]
        assert len(ts_calls) == 1, f"Expected 1 timestamps atomic write, got {len(ts_calls)}: {mock_atomic.call_args_list}"

    def test_ondeck_tracker_written_atomically(self, tmp_path):
        """OnDeck tracker must be written via save_json_atomically()."""
        svc = _make_service(tmp_path)

        old_real = "/mnt/user/media/Movies/Old.mkv"
        svc.ondeck_file.write_text(json.dumps({
            old_real: {"users": ["alice"], "added": "2026-01-01T00:00:00"}
        }), encoding="utf-8")
        svc.timestamps_file.write_text("{}", encoding="utf-8")

        import web.services.cache_service as cs_mod
        mock_atomic = MagicMock()

        with patch.object(cs_mod, "save_json_atomically", mock_atomic), \
             patch.object(cs_mod, "remove_from_exclude_file"), \
             patch.object(cs_mod, "remove_from_timestamps_file"), \
             patch.object(cs_mod, "get_array_direct_path", side_effect=lambda p: p.replace("/mnt/user/", "/mnt/user0/")), \
             patch.object(cs_mod, "get_media_identity", return_value="Old"), \
             patch.object(cs_mod, "find_matching_plexcached", return_value=None):
            self._call_transfer(svc, old_real_path=old_real)

        od_calls = [c for c in mock_atomic.call_args_list
                    if c[1].get("label") == "ondeck tracker" or (len(c[0]) >= 3 and c[0][2] == "ondeck tracker")]
        assert len(od_calls) == 1, f"Expected 1 ondeck atomic write, got {len(od_calls)}: {mock_atomic.call_args_list}"

    def test_watchlist_tracker_written_atomically(self, tmp_path):
        """Watchlist tracker must be written via save_json_atomically()."""
        svc = _make_service(tmp_path)

        old_plex = "/data/media/Movies/Old.mkv"
        svc.watchlist_file.write_text(json.dumps({
            old_plex: {"users": ["bob"], "added": "2026-01-01T00:00:00"}
        }), encoding="utf-8")
        svc.timestamps_file.write_text("{}", encoding="utf-8")

        import web.services.cache_service as cs_mod
        mock_atomic = MagicMock()

        with patch.object(cs_mod, "save_json_atomically", mock_atomic), \
             patch.object(cs_mod, "remove_from_exclude_file"), \
             patch.object(cs_mod, "remove_from_timestamps_file"), \
             patch.object(cs_mod, "get_array_direct_path", side_effect=lambda p: p.replace("/mnt/user/", "/mnt/user0/")), \
             patch.object(cs_mod, "get_media_identity", return_value="Old"), \
             patch.object(cs_mod, "find_matching_plexcached", return_value=None):
            self._call_transfer(svc, old_plex_path=old_plex)

        wl_calls = [c for c in mock_atomic.call_args_list
                    if c[1].get("label") == "watchlist tracker" or (len(c[0]) >= 3 and c[0][2] == "watchlist tracker")]
        assert len(wl_calls) == 1, f"Expected 1 watchlist atomic write, got {len(wl_calls)}: {mock_atomic.call_args_list}"


# ============================================================================
# Upgrade backup ordering — _handle_upgrade_plexcached()
# ============================================================================

class TestUpgradeBackupOrdering:
    """Verify _handle_upgrade_plexcached() creates new backup before deleting old."""

    def test_old_backup_preserved_when_new_copy_fails(self, tmp_path):
        """If new backup copy fails, old .plexcached must NOT be deleted."""
        import shutil as shutil_mod
        import web.services.cache_service as cs_mod

        svc = _make_service(tmp_path)

        # Create fake old .plexcached backup
        array_dir = tmp_path / "array" / "Movies"
        array_dir.mkdir(parents=True)
        old_plexcached = array_dir / "OldMovie.mkv.plexcached"
        old_plexcached.write_bytes(b"old backup content")

        # Create fake new cache file
        cache_dir = tmp_path / "cache" / "Movies"
        cache_dir.mkdir(parents=True)
        new_cache_file = cache_dir / "NewMovie.mkv"
        new_cache_file.write_bytes(b"new movie content")

        new_array_path = str(array_dir / "NewMovie.mkv")

        with patch.object(cs_mod, "get_array_direct_path", side_effect=lambda p: p), \
             patch.object(cs_mod, "get_media_identity", return_value="OldMovie"), \
             patch.object(cs_mod, "find_matching_plexcached", return_value=str(old_plexcached)), \
             patch.object(shutil_mod, "copy2", side_effect=OSError("Disk full")):
            svc._handle_upgrade_plexcached(
                old_real_path=str(array_dir / "OldMovie.mkv"),
                new_real_path=new_array_path,
                new_cache_path=str(new_cache_file),
                rating_key="999",
                settings={"create_plexcached_backups": True, "backup_upgraded_files": True},
            )

        # Old backup must still exist
        assert old_plexcached.exists(), "Old .plexcached was deleted even though new backup failed"

    def test_old_backup_deleted_after_new_succeeds(self, tmp_path):
        """Old .plexcached deleted only after new backup is verified."""
        import shutil as shutil_mod
        import web.services.cache_service as cs_mod

        svc = _make_service(tmp_path)

        array_dir = tmp_path / "array" / "Movies"
        array_dir.mkdir(parents=True)
        old_plexcached = array_dir / "OldMovie.mkv.plexcached"
        old_plexcached.write_bytes(b"old backup")

        cache_dir = tmp_path / "cache" / "Movies"
        cache_dir.mkdir(parents=True)
        new_cache_file = cache_dir / "NewMovie.mkv"
        new_cache_file.write_bytes(b"new movie data")

        new_array_path = str(array_dir / "NewMovie.mkv")

        call_order = []
        original_copy2 = shutil_mod.copy2

        def tracking_copy2(src, dst):
            call_order.append(("copy2", str(dst)))
            original_copy2(src, dst)

        original_remove = os.remove
        def tracking_remove(path):
            call_order.append(("remove", str(path)))
            original_remove(path)

        # os.path.isfile must check real files for the plexcached check, but
        # return True for new_plexcached existence check (returns False so copy runs)
        def selective_isfile(path):
            path_str = str(path)
            # new_plexcached doesn't exist yet — must return False so copy runs
            if path_str.endswith("NewMovie.mkv.plexcached"):
                return False
            return True  # old_plexcached and new_cache_path exist

        with patch.object(cs_mod, "get_array_direct_path", side_effect=lambda p: p), \
             patch.object(cs_mod, "get_media_identity", return_value="OldMovie"), \
             patch.object(cs_mod, "find_matching_plexcached", return_value=str(old_plexcached)), \
             patch.object(shutil_mod, "copy2", side_effect=tracking_copy2), \
             patch("os.remove", side_effect=tracking_remove), \
             patch("os.path.isfile", side_effect=selective_isfile), \
             patch("os.path.getsize", return_value=14), \
             patch("os.makedirs"):
            svc._handle_upgrade_plexcached(
                old_real_path=str(array_dir / "OldMovie.mkv"),
                new_real_path=new_array_path,
                new_cache_path=str(new_cache_file),
                rating_key="999",
                settings={"create_plexcached_backups": True, "backup_upgraded_files": True},
            )

        copy_idx = next((i for i, (op, _) in enumerate(call_order) if op == "copy2"), None)
        remove_idx = next((i for i, (op, p) in enumerate(call_order) if op == "remove" and "OldMovie" in p), None)
        assert copy_idx is not None, f"copy2 was not called. call_order: {call_order}"
        assert remove_idx is not None, f"os.remove of old backup was not called. call_order: {call_order}"
        assert copy_idx < remove_idx, "New backup must be created BEFORE old backup is deleted"
