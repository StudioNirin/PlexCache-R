"""
Tests for safety-critical operations in core/file_operations.py.

Covers:
- _should_add_to_cache: array file must be RENAMED to .plexcached, never deleted
- _move_to_array: uses get_array_direct_path, creates correct backups, handles errors
- FileFilter exclude list operations with Docker path translation
- Atomic save pattern for tracker classes (JSONTracker, CacheTimestampTracker)
"""

import os
import sys
import json
import threading
import time
import pytest
from unittest.mock import patch, MagicMock, PropertyMock

# Mock fcntl before any project imports (Windows compatibility)
sys.modules['fcntl'] = MagicMock()

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from conftest import MockPathMapping, MockMultiPathModifier, create_test_file
from core.file_operations import (
    FileFilter,
    FileMover,
    CacheTimestampTracker,
    JSONTracker,
    WatchlistTracker,
    PLEXCACHED_EXTENSION,
)


# ============================================================================
# Helpers
# ============================================================================

def _make_file_filter(tmp_path, *, is_unraid=True, is_docker=False,
                      path_modifier=None, timestamp_tracker=None,
                      ondeck_tracker=None, watchlist_tracker=None):
    """Build a FileFilter wired to tmp_path directories."""
    exclude_file = os.path.join(str(tmp_path), "exclude.txt")
    # Create the exclude file so it exists for reads
    with open(exclude_file, "w") as f:
        pass

    return FileFilter(
        real_source="/mnt/user/media",
        cache_dir=os.path.join(str(tmp_path), "cache"),
        is_unraid=is_unraid,
        mover_cache_exclude_file=exclude_file,
        timestamp_tracker=timestamp_tracker,
        cache_retention_hours=12,
        ondeck_tracker=ondeck_tracker,
        watchlist_tracker=watchlist_tracker,
        path_modifier=path_modifier,
        is_docker=is_docker,
    )


def _make_file_mover(tmp_path, *, is_unraid=True, path_modifier=None,
                     timestamp_tracker=None, create_backups=True):
    """Build a FileMover wired to tmp_path directories."""
    exclude_file = os.path.join(str(tmp_path), "exclude.txt")
    with open(exclude_file, "w") as f:
        pass

    file_utils = MagicMock()
    file_utils.is_docker = False
    file_utils.is_linux = True
    file_utils.copy_file_with_permissions = MagicMock(return_value=0)
    file_utils.create_directory_with_permissions = MagicMock()

    return FileMover(
        real_source="/mnt/user/media",
        cache_dir=os.path.join(str(tmp_path), "cache"),
        is_unraid=is_unraid,
        file_utils=file_utils,
        debug=False,
        mover_cache_exclude_file=exclude_file,
        timestamp_tracker=timestamp_tracker,
        path_modifier=path_modifier,
        create_plexcached_backups=create_backups,
    )


# ============================================================================
# _should_add_to_cache tests
# ============================================================================

class TestShouldAddToCache:
    """Test FileFilter._should_add_to_cache array file handling."""

    def test_array_file_renamed_to_plexcached(self, tmp_path):
        """Array file must be RENAMED to .plexcached, not deleted.

        This is the critical safety invariant: when a file already exists on
        cache AND on the array, the array copy is renamed to .plexcached as a
        backup.  os.rename must be used, not os.remove.
        """
        cache_dir = os.path.join(str(tmp_path), "cache", "media", "Movies")
        array_dir = os.path.join(str(tmp_path), "user0", "media", "Movies")

        cache_file = create_test_file(os.path.join(cache_dir, "Movie.mkv"), "cache data")
        array_file = create_test_file(os.path.join(array_dir, "Movie.mkv"), "array data")

        filt = _make_file_filter(tmp_path, is_unraid=False)
        filt.cache_dir = os.path.join(str(tmp_path), "cache")

        # Patch so _should_add_to_cache derives the array path we control.
        # The method does: array_file = file.replace("/mnt/user/", "/mnt/user0/", 1)
        # For non-Unraid, array_file == file, so we make "file" point to array_dir.
        file_arg = array_file  # the "Plex" path
        cache_file_name = cache_file  # the cache copy

        result = filt._should_add_to_cache(file_arg, cache_file_name)

        # Should return False (file already on cache, no need to add)
        assert result is False

        # The array file must have been renamed to .plexcached
        plexcached = array_file + PLEXCACHED_EXTENSION
        assert os.path.isfile(plexcached), (
            "Array file was NOT renamed to .plexcached -- possible data loss!"
        )
        assert not os.path.isfile(array_file), (
            "Original array file still exists; rename did not happen"
        )

        # .plexcached content must match original array content
        with open(plexcached, "r") as f:
            assert f.read() == "array data"

    def test_array_file_not_deleted_when_no_plexcached_exists(self, tmp_path):
        """When .plexcached does NOT already exist, os.rename is used (not os.remove)."""
        cache_dir = os.path.join(str(tmp_path), "cache", "media", "Movies")
        array_dir = os.path.join(str(tmp_path), "array", "media", "Movies")

        cache_file = create_test_file(os.path.join(cache_dir, "Movie.mkv"), "cache")
        array_file = create_test_file(os.path.join(array_dir, "Movie.mkv"), "array")

        filt = _make_file_filter(tmp_path, is_unraid=False)
        filt.cache_dir = os.path.join(str(tmp_path), "cache")

        with patch("core.file_operations.os.rename", wraps=os.rename) as mock_rename:
            filt._should_add_to_cache(array_file, cache_file)

        # os.rename must have been called to create the .plexcached backup
        mock_rename.assert_called_once_with(
            array_file, array_file + PLEXCACHED_EXTENSION
        )

    def test_plexcached_already_exists_removes_redundant_array(self, tmp_path):
        """When .plexcached already exists, the redundant array file is removed."""
        cache_dir = os.path.join(str(tmp_path), "cache", "media", "Movies")
        array_dir = os.path.join(str(tmp_path), "array", "media", "Movies")

        cache_file = create_test_file(os.path.join(cache_dir, "Movie.mkv"), "cache")
        array_file = create_test_file(os.path.join(array_dir, "Movie.mkv"), "array")
        plexcached = create_test_file(
            os.path.join(array_dir, "Movie.mkv" + PLEXCACHED_EXTENSION), "backup"
        )

        filt = _make_file_filter(tmp_path, is_unraid=False)
        filt.cache_dir = os.path.join(str(tmp_path), "cache")

        result = filt._should_add_to_cache(array_file, cache_file)

        assert result is False
        # The original array file should be removed (backup already exists)
        assert not os.path.isfile(array_file)
        # The existing .plexcached backup is untouched
        assert os.path.isfile(plexcached)

    def test_no_array_file_returns_true(self, tmp_path):
        """When no array file exists, should proceed with caching."""
        cache_dir = os.path.join(str(tmp_path), "cache", "media", "Movies")
        array_dir = os.path.join(str(tmp_path), "array", "media", "Movies")
        os.makedirs(array_dir, exist_ok=True)

        # No cache file on disk, no array file on disk
        cache_file_name = os.path.join(cache_dir, "Movie.mkv")
        file_arg = os.path.join(array_dir, "Movie.mkv")

        filt = _make_file_filter(tmp_path, is_unraid=False)
        filt.cache_dir = os.path.join(str(tmp_path), "cache")

        result = filt._should_add_to_cache(file_arg, cache_file_name)
        assert result is True

    def test_cache_exists_no_array_returns_false(self, tmp_path):
        """When file is already on cache but not on array, returns False (skip)."""
        cache_dir = os.path.join(str(tmp_path), "cache", "media", "Movies")
        array_dir = os.path.join(str(tmp_path), "array", "media", "Movies")
        os.makedirs(array_dir, exist_ok=True)

        cache_file = create_test_file(os.path.join(cache_dir, "Movie.mkv"), "cache")
        file_arg = os.path.join(array_dir, "Movie.mkv")  # does not exist on disk

        filt = _make_file_filter(tmp_path, is_unraid=False)
        filt.cache_dir = os.path.join(str(tmp_path), "cache")

        result = filt._should_add_to_cache(file_arg, cache_file)
        assert result is False

    def test_timestamp_recorded_for_preexisting(self, tmp_path):
        """When file already on cache, timestamp is recorded as 'pre-existing'."""
        cache_dir = os.path.join(str(tmp_path), "cache", "media", "Movies")
        array_dir = os.path.join(str(tmp_path), "array", "media", "Movies")
        os.makedirs(array_dir, exist_ok=True)

        cache_file = create_test_file(os.path.join(cache_dir, "Movie.mkv"), "cache")
        file_arg = os.path.join(array_dir, "Movie.mkv")

        ts_file = os.path.join(str(tmp_path), "timestamps.json")
        tracker = CacheTimestampTracker(ts_file)

        filt = _make_file_filter(tmp_path, is_unraid=False, timestamp_tracker=tracker)
        filt.cache_dir = os.path.join(str(tmp_path), "cache")

        filt._should_add_to_cache(file_arg, cache_file)

        # Tracker should have an entry for the cache file with source "pre-existing"
        assert tracker.get_source(cache_file) == "pre-existing"


# ============================================================================
# _move_to_array tests
# ============================================================================

class TestMoveToArray:
    """Test FileMover._move_to_array scenarios."""

    def test_plexcached_rename_restores_file(self, tmp_path):
        """When .plexcached exists and sizes match, it is renamed back (fast restore)."""
        cache_dir = os.path.join(str(tmp_path), "cache", "media", "Movies")
        array_dir = os.path.join(str(tmp_path), "user0", "media", "Movies")

        content = "A" * 500
        cache_file = create_test_file(os.path.join(cache_dir, "Movie.mkv"), content)
        plexcached = create_test_file(
            os.path.join(array_dir, "Movie.mkv" + PLEXCACHED_EXTENSION), content
        )
        array_file = os.path.join(array_dir, "Movie.mkv")

        mover = _make_file_mover(tmp_path, is_unraid=False)
        ts_file = os.path.join(str(tmp_path), "timestamps.json")
        mover.timestamp_tracker = CacheTimestampTracker(ts_file)

        # Patch get_array_direct_path to be identity for non-Unraid
        with patch("core.file_operations.get_array_direct_path", side_effect=lambda p: p):
            # Patch tqdm.write to avoid import error
            with patch("core.file_operations.get_console_lock"):
                with patch("tqdm.tqdm.write"):
                    result = mover._move_to_array(cache_file, array_dir, cache_file)

        assert result == 0
        # .plexcached should be renamed to original
        assert os.path.isfile(array_file)
        assert not os.path.isfile(plexcached)
        # Cache file should be deleted
        assert not os.path.isfile(cache_file)

    def test_uses_get_array_direct_path(self, tmp_path):
        """_move_to_array must use get_array_direct_path for array checks."""
        cache_dir = os.path.join(str(tmp_path), "cache", "media", "Movies")
        array_dir = os.path.join(str(tmp_path), "user0", "media", "Movies")

        content = "video data"
        cache_file = create_test_file(os.path.join(cache_dir, "Movie.mkv"), content)
        plexcached = create_test_file(
            os.path.join(array_dir, "Movie.mkv" + PLEXCACHED_EXTENSION), content
        )

        mover = _make_file_mover(tmp_path, is_unraid=False)

        with patch("core.file_operations.get_array_direct_path",
                   wraps=lambda p: p) as mock_gadp:
            with patch("core.file_operations.get_console_lock"):
                with patch("tqdm.tqdm.write"):
                    mover._move_to_array(cache_file, array_dir, cache_file)

        # get_array_direct_path must have been called at least once
        assert mock_gadp.call_count >= 1

    def test_plexcached_extension_appended_correctly(self, tmp_path):
        """The .plexcached extension must be appended to the FULL filename (including original ext)."""
        # e.g. "Movie.mkv" -> "Movie.mkv.plexcached", NOT "Movie.plexcached"
        cache_dir = os.path.join(str(tmp_path), "cache", "media", "Movies")
        array_dir = os.path.join(str(tmp_path), "user0", "media", "Movies")

        content = "video"
        cache_file = create_test_file(os.path.join(cache_dir, "Movie (2024).mkv"), content)
        # Create the backup with correct naming
        expected_plexcached_name = "Movie (2024).mkv" + PLEXCACHED_EXTENSION
        plexcached = create_test_file(
            os.path.join(array_dir, expected_plexcached_name), content
        )

        mover = _make_file_mover(tmp_path, is_unraid=False)

        with patch("core.file_operations.get_array_direct_path", side_effect=lambda p: p):
            with patch("core.file_operations.get_console_lock"):
                with patch("tqdm.tqdm.write"):
                    result = mover._move_to_array(cache_file, array_dir, cache_file)

        assert result == 0
        restored = os.path.join(array_dir, "Movie (2024).mkv")
        assert os.path.isfile(restored)

    def test_no_plexcached_copies_to_array_direct(self, tmp_path):
        """When no .plexcached exists, file is copied to array-direct path."""
        cache_dir = os.path.join(str(tmp_path), "cache", "media", "Movies")
        array_dir = os.path.join(str(tmp_path), "user0", "media", "Movies")
        os.makedirs(array_dir, exist_ok=True)

        content = "video data"
        cache_file = create_test_file(os.path.join(cache_dir, "Movie.mkv"), content)
        array_file_path = os.path.join(array_dir, "Movie.mkv")

        mover = _make_file_mover(tmp_path, is_unraid=False)

        # Simulate the copy by having copy_file_with_permissions actually create the dest
        def fake_copy(src, dest, **kwargs):
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            with open(dest, "w") as f:
                f.write(content)
            return 0

        mover.file_utils.copy_file_with_permissions = fake_copy

        with patch("core.file_operations.get_array_direct_path", side_effect=lambda p: p):
            with patch("core.file_operations.get_console_lock"):
                with patch("tqdm.tqdm.write"):
                    result = mover._move_to_array(cache_file, array_dir, cache_file)

        assert result == 0
        assert os.path.isfile(array_file_path)
        assert not os.path.isfile(cache_file)

    def test_permission_error_returns_error_code(self, tmp_path):
        """Permission errors during restore must return error code, not crash."""
        cache_dir = os.path.join(str(tmp_path), "cache", "media", "Movies")
        array_dir = os.path.join(str(tmp_path), "user0", "media", "Movies")
        os.makedirs(array_dir, exist_ok=True)

        cache_file = create_test_file(os.path.join(cache_dir, "Movie.mkv"), "data")

        mover = _make_file_mover(tmp_path, is_unraid=False)

        # Simulate copy raising PermissionError
        mover.file_utils.copy_file_with_permissions = MagicMock(
            side_effect=RuntimeError("Permission denied")
        )

        with patch("core.file_operations.get_array_direct_path", side_effect=lambda p: p):
            result = mover._move_to_array(cache_file, array_dir, cache_file)

        assert result == 1  # Error code
        # Cache file must still exist (data preserved)
        assert os.path.isfile(cache_file)

    def test_size_mismatch_aborts(self, tmp_path):
        """If copy produces a size mismatch, cache file is NOT deleted."""
        cache_dir = os.path.join(str(tmp_path), "cache", "media", "Movies")
        array_dir = os.path.join(str(tmp_path), "user0", "media", "Movies")
        os.makedirs(array_dir, exist_ok=True)

        cache_file = create_test_file(
            os.path.join(cache_dir, "Movie.mkv"),
            size_bytes=1000
        )
        array_file_path = os.path.join(array_dir, "Movie.mkv")

        mover = _make_file_mover(tmp_path, is_unraid=False)

        # Simulate partial copy (different size)
        def fake_partial_copy(src, dest, **kwargs):
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            with open(dest, "wb") as f:
                f.write(b"\x00" * 500)  # Only 500 of 1000 bytes
            return 0

        mover.file_utils.copy_file_with_permissions = fake_partial_copy

        with patch("core.file_operations.get_array_direct_path", side_effect=lambda p: p):
            result = mover._move_to_array(cache_file, array_dir, cache_file)

        assert result == 1  # Error (size mismatch)
        # Cache file must still exist
        assert os.path.isfile(cache_file)
        # Mismatched array file should have been cleaned up
        assert not os.path.isfile(array_file_path)


# ============================================================================
# FileFilter exclude list tests
# ============================================================================

class TestExcludeListOperations:
    """Test FileFilter exclude list management."""

    def test_add_files_translates_to_host_paths_in_docker(self, tmp_path):
        """In Docker, exclude list entries must use host paths, not container paths."""
        mappings = [
            MockPathMapping(
                name="TV",
                plex_path="/data/TV",
                real_path="/mnt/user/media/TV",
                cache_path="/mnt/cache/media/TV",
                host_cache_path="/mnt/cache_downloads/media/TV",
            ),
        ]
        modifier = MockMultiPathModifier(mappings)

        filt = _make_file_filter(tmp_path, is_docker=True, path_modifier=modifier)

        # The container sees this path:
        container_path = "/mnt/cache/media/TV/Show/S01E01.mkv"
        filt._add_to_exclude_file(container_path)

        # Read back exclude file
        with open(filt.mover_cache_exclude_file, "r") as f:
            entries = [line.strip() for line in f if line.strip()]

        # Should have host path, not container path
        expected = "/mnt/cache_downloads/media/TV/Show/S01E01.mkv"
        assert expected in entries, (
            f"Exclude file has {entries}, expected host path {expected}"
        )
        assert container_path not in entries, (
            "Container path must NOT appear in exclude file (Unraid mover can't see it)"
        )

    def test_add_files_no_translation_when_not_docker(self, tmp_path):
        """When not in Docker, paths are written as-is to exclude file."""
        filt = _make_file_filter(tmp_path, is_docker=False)

        path = "/mnt/cache/media/TV/Show/S01E01.mkv"
        filt._add_to_exclude_file(path)

        with open(filt.mover_cache_exclude_file, "r") as f:
            entries = [line.strip() for line in f if line.strip()]

        assert path in entries

    def test_add_files_no_duplicates(self, tmp_path):
        """Adding the same file twice should not create duplicate entries."""
        filt = _make_file_filter(tmp_path, is_docker=False)

        path = "/mnt/cache/media/Movies/Movie.mkv"
        filt._add_to_exclude_file(path)
        filt._add_to_exclude_file(path)

        with open(filt.mover_cache_exclude_file, "r") as f:
            entries = [line.strip() for line in f if line.strip()]

        assert entries.count(path) == 1

    def test_remove_files_works(self, tmp_path):
        """Files can be removed from exclude list."""
        filt = _make_file_filter(tmp_path, is_docker=False)

        # Pre-populate exclude file
        files = [
            "/mnt/cache/media/Movies/Movie1.mkv",
            "/mnt/cache/media/Movies/Movie2.mkv",
            "/mnt/cache/media/TV/Show/S01E01.mkv",
        ]
        with open(filt.mover_cache_exclude_file, "w") as f:
            for p in files:
                f.write(p + "\n")

        # Remove one file
        filt.remove_files_from_exclude_list(["/mnt/cache/media/Movies/Movie2.mkv"])

        with open(filt.mover_cache_exclude_file, "r") as f:
            remaining = [line.strip() for line in f if line.strip()]

        assert "/mnt/cache/media/Movies/Movie1.mkv" in remaining
        assert "/mnt/cache/media/TV/Show/S01E01.mkv" in remaining
        assert "/mnt/cache/media/Movies/Movie2.mkv" not in remaining

    def test_remove_files_translates_container_paths_in_docker(self, tmp_path):
        """In Docker, container paths must be translated to host paths for removal."""
        mappings = [
            MockPathMapping(
                name="Movies",
                plex_path="/data/Movies",
                real_path="/mnt/user/media/Movies",
                cache_path="/mnt/cache/media/Movies",
                host_cache_path="/mnt/cache_downloads/media/Movies",
            ),
        ]
        modifier = MockMultiPathModifier(mappings)

        filt = _make_file_filter(tmp_path, is_docker=True, path_modifier=modifier)

        # Exclude file stores HOST paths
        host_path = "/mnt/cache_downloads/media/Movies/Movie.mkv"
        with open(filt.mover_cache_exclude_file, "w") as f:
            f.write(host_path + "\n")

        # Remove is called with CONTAINER path
        container_path = "/mnt/cache/media/Movies/Movie.mkv"
        filt.remove_files_from_exclude_list([container_path])

        with open(filt.mover_cache_exclude_file, "r") as f:
            remaining = [line.strip() for line in f if line.strip()]

        assert host_path not in remaining

    def test_clean_stale_entries_translates_from_host(self, tmp_path):
        """Stale entry cleanup must translate host paths to container paths before checking existence."""
        mappings = [
            MockPathMapping(
                name="TV",
                plex_path="/data/TV",
                real_path="/mnt/user/media/TV",
                cache_path="/mnt/cache/media/TV",
                host_cache_path="/mnt/cache_downloads/media/TV",
            ),
        ]
        modifier = MockMultiPathModifier(mappings)

        filt = _make_file_filter(tmp_path, is_docker=True, path_modifier=modifier)

        # Create a real file at the CONTAINER path
        container_file = os.path.join(str(tmp_path), "existing_file.mkv")
        create_test_file(container_file, "video")

        # Exclude file has HOST paths: one valid, one stale
        valid_host_path = "/mnt/cache_downloads/media/TV/Show/existing_file.mkv"
        stale_host_path = "/mnt/cache_downloads/media/TV/Show/deleted_file.mkv"

        with open(filt.mover_cache_exclude_file, "w") as f:
            f.write(valid_host_path + "\n")
            f.write(stale_host_path + "\n")

        # Patch _translate_from_host_path to map valid_host_path to our real file
        # and stale_host_path to a non-existent file
        original_translate = filt._translate_from_host_path

        def patched_translate(host_path):
            if host_path == valid_host_path:
                return container_file  # exists on disk
            elif host_path == stale_host_path:
                return os.path.join(str(tmp_path), "nonexistent.mkv")  # doesn't exist
            return original_translate(host_path)

        filt._translate_from_host_path = patched_translate

        removed = filt.clean_stale_exclude_entries()

        assert removed == 1

        with open(filt.mover_cache_exclude_file, "r") as f:
            remaining = [line.strip() for line in f if line.strip()]

        assert valid_host_path in remaining
        assert stale_host_path not in remaining

    def test_clean_stale_entries_no_changes_when_all_valid(self, tmp_path):
        """When all entries are valid, file is not rewritten."""
        filt = _make_file_filter(tmp_path, is_docker=False)

        # Create real files
        f1 = create_test_file(os.path.join(str(tmp_path), "movie1.mkv"), "data")
        f2 = create_test_file(os.path.join(str(tmp_path), "movie2.mkv"), "data")

        with open(filt.mover_cache_exclude_file, "w") as f:
            f.write(f1 + "\n")
            f.write(f2 + "\n")

        mtime_before = os.path.getmtime(filt.mover_cache_exclude_file)
        # Small sleep so mtime would differ if file is rewritten
        time.sleep(0.05)

        removed = filt.clean_stale_exclude_entries()

        assert removed == 0
        mtime_after = os.path.getmtime(filt.mover_cache_exclude_file)
        assert mtime_before == mtime_after, "File should not be rewritten when no stale entries"

    def test_clean_stale_returns_zero_when_file_missing(self, tmp_path):
        """clean_stale_exclude_entries returns 0 if exclude file doesn't exist."""
        filt = _make_file_filter(tmp_path, is_docker=False)
        # Point to nonexistent file
        filt.mover_cache_exclude_file = os.path.join(str(tmp_path), "nonexistent.txt")
        assert filt.clean_stale_exclude_entries() == 0


# ============================================================================
# Tracker atomic save tests
# ============================================================================

class TestTrackerAtomicSave:
    """Test that tracker saves produce valid JSON and are thread-safe."""

    def test_save_writes_valid_json(self, tmp_path):
        """Save must produce valid, parseable JSON."""
        tracker_file = os.path.join(str(tmp_path), "tracker.json")
        tracker = JSONTracker(tracker_file, "test")

        # Add some data
        with tracker._lock:
            tracker._data["file1.mkv"] = {"first_seen": "2026-01-01T00:00:00", "users": ["user1"]}
            tracker._data["file2.mkv"] = {"first_seen": "2026-01-02T00:00:00", "users": ["user2"]}
            tracker._save()

        # Must be valid JSON
        with open(tracker_file, "r", encoding="utf-8") as f:
            loaded = json.load(f)

        assert "file1.mkv" in loaded
        assert "file2.mkv" in loaded
        assert loaded["file1.mkv"]["users"] == ["user1"]

    def test_save_overwrites_completely(self, tmp_path):
        """Save replaces the entire file, not appending."""
        tracker_file = os.path.join(str(tmp_path), "tracker.json")
        tracker = JSONTracker(tracker_file, "test")

        # Write initial data
        with tracker._lock:
            tracker._data["file1.mkv"] = {"key": "value1"}
            tracker._save()

        # Replace with different data
        with tracker._lock:
            tracker._data = {"file2.mkv": {"key": "value2"}}
            tracker._save()

        with open(tracker_file, "r", encoding="utf-8") as f:
            loaded = json.load(f)

        # Only new data should exist
        assert "file1.mkv" not in loaded
        assert "file2.mkv" in loaded

    def test_save_survives_concurrent_reads(self, tmp_path):
        """Save must not corrupt file during concurrent access."""
        tracker_file = os.path.join(str(tmp_path), "tracker.json")
        tracker = JSONTracker(tracker_file, "test")

        errors = []

        def writer():
            for i in range(50):
                with tracker._lock:
                    tracker._data[f"file_{i}.mkv"] = {"index": i}
                    tracker._save()

        def reader():
            for _ in range(50):
                try:
                    if os.path.exists(tracker_file):
                        with open(tracker_file, "r", encoding="utf-8") as f:
                            content = f.read()
                            if content.strip():
                                json.loads(content)
                except json.JSONDecodeError as e:
                    errors.append(str(e))
                except IOError:
                    pass  # File may be mid-write on Windows

        t_write = threading.Thread(target=writer)
        t_read = threading.Thread(target=reader)
        t_write.start()
        t_read.start()
        t_write.join()
        t_read.join()

        # Final state must be valid JSON
        with open(tracker_file, "r", encoding="utf-8") as f:
            final = json.load(f)
        assert len(final) == 50

    def test_load_handles_corrupt_file(self, tmp_path):
        """Loading a corrupt JSON file should not crash, should reset to empty."""
        tracker_file = os.path.join(str(tmp_path), "tracker.json")
        with open(tracker_file, "w") as f:
            f.write("{invalid json content")

        tracker = JSONTracker(tracker_file, "test")
        assert tracker._data == {}

    def test_load_handles_missing_file(self, tmp_path):
        """Loading from nonexistent file should start empty."""
        tracker_file = os.path.join(str(tmp_path), "nonexistent.json")
        tracker = JSONTracker(tracker_file, "test")
        assert tracker._data == {}

    def test_remove_entry_persists(self, tmp_path):
        """Removing an entry must save to disk immediately."""
        tracker_file = os.path.join(str(tmp_path), "tracker.json")

        # Pre-populate
        initial_data = {
            "file1.mkv": {"first_seen": "2026-01-01T00:00:00", "last_seen": "2026-01-02T00:00:00"},
            "file2.mkv": {"first_seen": "2026-01-01T00:00:00", "last_seen": "2026-01-02T00:00:00"},
        }
        with open(tracker_file, "w") as f:
            json.dump(initial_data, f)

        tracker = JSONTracker(tracker_file, "test")
        tracker.remove_entry("file1.mkv")

        # Read back from disk (fresh load)
        with open(tracker_file, "r") as f:
            on_disk = json.load(f)

        assert "file1.mkv" not in on_disk
        assert "file2.mkv" in on_disk


# ============================================================================
# CacheTimestampTracker tests
# ============================================================================

class TestCacheTimestampTracker:
    """Test CacheTimestampTracker operations."""

    def test_record_cache_time_does_not_overwrite(self, tmp_path):
        """Existing timestamps are never overwritten (preserves original cache time)."""
        ts_file = os.path.join(str(tmp_path), "timestamps.json")
        tracker = CacheTimestampTracker(ts_file)

        # Record initial timestamp
        tracker.record_cache_time("/mnt/cache/Movie.mkv", source="ondeck")

        # Read it back
        with open(ts_file, "r") as f:
            first_data = json.load(f)
        first_timestamp = first_data["/mnt/cache/Movie.mkv"]["cached_at"]

        time.sleep(0.05)

        # Try to overwrite with different source
        tracker.record_cache_time("/mnt/cache/Movie.mkv", source="watchlist")

        # Read it back
        with open(ts_file, "r") as f:
            second_data = json.load(f)
        second_timestamp = second_data["/mnt/cache/Movie.mkv"]["cached_at"]

        # Timestamp must NOT have changed
        assert first_timestamp == second_timestamp
        # Source must still be "ondeck" (first recorded)
        assert second_data["/mnt/cache/Movie.mkv"]["source"] == "ondeck"

    def test_record_cache_time_new_entry(self, tmp_path):
        """New entries are recorded with correct source and timestamp."""
        ts_file = os.path.join(str(tmp_path), "timestamps.json")
        tracker = CacheTimestampTracker(ts_file)

        tracker.record_cache_time("/mnt/cache/Movie.mkv", source="watchlist")

        with open(ts_file, "r") as f:
            data = json.load(f)

        assert "/mnt/cache/Movie.mkv" in data
        assert data["/mnt/cache/Movie.mkv"]["source"] == "watchlist"
        assert "cached_at" in data["/mnt/cache/Movie.mkv"]

    def test_remove_entry_saves(self, tmp_path):
        """Removing entry persists to disk."""
        ts_file = os.path.join(str(tmp_path), "timestamps.json")
        tracker = CacheTimestampTracker(ts_file)

        # Add two entries
        tracker.record_cache_time("/mnt/cache/Movie1.mkv", source="ondeck")
        tracker.record_cache_time("/mnt/cache/Movie2.mkv", source="watchlist")

        # Remove one
        tracker.remove_entry("/mnt/cache/Movie1.mkv")

        # Read back from disk
        with open(ts_file, "r") as f:
            data = json.load(f)

        assert "/mnt/cache/Movie1.mkv" not in data
        assert "/mnt/cache/Movie2.mkv" in data

    def test_remove_nonexistent_entry_is_noop(self, tmp_path):
        """Removing a nonexistent entry does not error or corrupt data."""
        ts_file = os.path.join(str(tmp_path), "timestamps.json")
        tracker = CacheTimestampTracker(ts_file)

        tracker.record_cache_time("/mnt/cache/Movie.mkv", source="ondeck")
        tracker.remove_entry("/mnt/cache/Nonexistent.mkv")

        with open(ts_file, "r") as f:
            data = json.load(f)
        assert "/mnt/cache/Movie.mkv" in data

    def test_get_source_returns_correct_value(self, tmp_path):
        """get_source returns the recorded source type."""
        ts_file = os.path.join(str(tmp_path), "timestamps.json")
        tracker = CacheTimestampTracker(ts_file)

        tracker.record_cache_time("/mnt/cache/A.mkv", source="ondeck")
        tracker.record_cache_time("/mnt/cache/B.mkv", source="watchlist")

        assert tracker.get_source("/mnt/cache/A.mkv") == "ondeck"
        assert tracker.get_source("/mnt/cache/B.mkv") == "watchlist"
        assert tracker.get_source("/mnt/cache/C.mkv") == "unknown"

    def test_migrates_old_format(self, tmp_path):
        """Old format (plain timestamp string) is migrated to new dict format on load."""
        ts_file = os.path.join(str(tmp_path), "timestamps.json")

        old_data = {
            "/mnt/cache/OldMovie.mkv": "2025-12-01T10:00:00"
        }
        with open(ts_file, "w") as f:
            json.dump(old_data, f)

        tracker = CacheTimestampTracker(ts_file)

        # Should have been migrated to dict format
        with open(ts_file, "r") as f:
            data = json.load(f)

        entry = data["/mnt/cache/OldMovie.mkv"]
        assert isinstance(entry, dict)
        assert entry["cached_at"] == "2025-12-01T10:00:00"
        assert entry["source"] == "unknown"

    def test_cleanup_missing_files(self, tmp_path):
        """cleanup_missing_files removes entries for non-existent files."""
        ts_file = os.path.join(str(tmp_path), "timestamps.json")
        tracker = CacheTimestampTracker(ts_file)

        # Create one real file, leave the other missing
        real_file = create_test_file(os.path.join(str(tmp_path), "existing.mkv"), "data")
        missing_file = os.path.join(str(tmp_path), "missing.mkv")

        tracker.record_cache_time(real_file, source="ondeck")
        tracker.record_cache_time(missing_file, source="watchlist")

        removed = tracker.cleanup_missing_files()
        assert removed == 1

        with open(ts_file, "r") as f:
            data = json.load(f)

        assert real_file in data
        assert missing_file not in data

    def test_concurrent_record_and_remove(self, tmp_path):
        """Concurrent record and remove operations must not corrupt the tracker."""
        ts_file = os.path.join(str(tmp_path), "timestamps.json")
        tracker = CacheTimestampTracker(ts_file)

        errors = []

        def add_entries():
            for i in range(50):
                try:
                    tracker.record_cache_time(f"/mnt/cache/file_{i}.mkv", source="ondeck")
                except Exception as e:
                    errors.append(str(e))

        def remove_entries():
            for i in range(50):
                try:
                    tracker.remove_entry(f"/mnt/cache/file_{i}.mkv")
                except Exception as e:
                    errors.append(str(e))

        t1 = threading.Thread(target=add_entries)
        t2 = threading.Thread(target=remove_entries)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert len(errors) == 0, f"Errors during concurrent access: {errors}"

        # Final file must be valid JSON
        with open(ts_file, "r") as f:
            final_data = json.load(f)
        assert isinstance(final_data, dict)


# ============================================================================
# WatchlistTracker (inherits JSONTracker) - regression tests
# ============================================================================

class TestWatchlistTrackerSafety:
    """Verify WatchlistTracker correctly uses JSONTracker base class."""

    def test_save_after_update(self, tmp_path):
        """update_entry must persist to disk."""
        tracker_file = os.path.join(str(tmp_path), "watchlist.json")
        tracker = WatchlistTracker(tracker_file)

        from datetime import datetime
        tracker.update_entry("/mnt/cache/Movie.mkv", "user1", datetime(2026, 1, 15))

        # Verify on disk
        with open(tracker_file, "r") as f:
            data = json.load(f)

        assert "/mnt/cache/Movie.mkv" in data
        assert "user1" in data["/mnt/cache/Movie.mkv"]["users"]

    def test_remove_entry_persists(self, tmp_path):
        """remove_entry must persist to disk."""
        tracker_file = os.path.join(str(tmp_path), "watchlist.json")
        tracker = WatchlistTracker(tracker_file)

        from datetime import datetime
        tracker.update_entry("/mnt/cache/Movie.mkv", "user1", datetime(2026, 1, 15))
        tracker.remove_entry("/mnt/cache/Movie.mkv")

        with open(tracker_file, "r") as f:
            data = json.load(f)

        assert "/mnt/cache/Movie.mkv" not in data
