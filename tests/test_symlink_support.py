"""Tests for symlink support feature (non-Unraid systems).

Symlinks allow Plex to find files at their original locations after
PlexCache moves them to cache and renames/deletes the originals.
"""
import os
import sys
import logging
from unittest.mock import MagicMock, patch

import pytest

# Mock fcntl for Windows compatibility
if 'fcntl' not in sys.modules:
    sys.modules['fcntl'] = MagicMock()

from core.file_operations import FileMover, FileFilter, PlexcachedRestorer, PLEXCACHED_EXTENSION

# Skip symlink tests on Windows (requires developer mode or admin privileges)
needs_symlink = pytest.mark.skipif(
    os.name == 'nt', reason="Symlink tests require Unix or Windows developer mode"
)


def _make_symlink_mover(tmp_path, *, use_symlinks=True, create_backups=True,
                        is_unraid=False):
    """Build a FileMover configured for symlink testing."""
    exclude_file = str(tmp_path / "exclude.txt")
    with open(exclude_file, "w"):
        pass

    file_utils = MagicMock()
    file_utils.is_docker = False
    file_utils.is_linux = True
    file_utils.copy_file_with_permissions = MagicMock(return_value=0)
    file_utils.create_directory_with_permissions = MagicMock()

    return FileMover(
        real_source=str(tmp_path / "storage"),
        cache_dir=str(tmp_path / "cache"),
        is_unraid=is_unraid,
        file_utils=file_utils,
        debug=False,
        mover_cache_exclude_file=exclude_file,
        timestamp_tracker=None,
        create_plexcached_backups=create_backups,
        use_symlinks=use_symlinks,
    )


# ============================================================================
# _create_symlink / _remove_symlink helper tests
# ============================================================================

class TestSymlinkHelpers:
    """Test the low-level symlink helper methods."""

    @needs_symlink
    def test_create_symlink_basic(self, tmp_path):
        """Symlink is created pointing to target."""
        mover = _make_symlink_mover(tmp_path)
        target = tmp_path / "cache" / "movie.mkv"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"cached content")

        symlink = tmp_path / "storage" / "movie.mkv"
        symlink.parent.mkdir(parents=True, exist_ok=True)

        result = mover._create_symlink(str(symlink), str(target))

        assert result is True
        assert os.path.islink(str(symlink))
        assert os.readlink(str(symlink)) == str(target)
        # Symlink should resolve to the target content
        assert symlink.read_bytes() == b"cached content"

    @needs_symlink
    def test_create_symlink_overwrites_existing(self, tmp_path):
        """Existing symlink is replaced when re-creating."""
        mover = _make_symlink_mover(tmp_path)

        old_target = tmp_path / "old_target.mkv"
        old_target.write_bytes(b"old")
        new_target = tmp_path / "new_target.mkv"
        new_target.write_bytes(b"new")

        symlink = tmp_path / "link.mkv"
        os.symlink(str(old_target), str(symlink))
        assert os.readlink(str(symlink)) == str(old_target)

        result = mover._create_symlink(str(symlink), str(new_target))

        assert result is True
        assert os.readlink(str(symlink)) == str(new_target)

    @needs_symlink
    def test_create_symlink_creates_parent_dirs(self, tmp_path):
        """Parent directories are created if they don't exist."""
        mover = _make_symlink_mover(tmp_path)
        target = tmp_path / "target.mkv"
        target.write_bytes(b"data")

        symlink = tmp_path / "deep" / "nested" / "dir" / "link.mkv"

        result = mover._create_symlink(str(symlink), str(target))

        assert result is True
        assert os.path.islink(str(symlink))

    @needs_symlink
    def test_create_symlink_failure_nonfatal(self, tmp_path):
        """os.symlink failure logs warning but returns False (non-fatal)."""
        mover = _make_symlink_mover(tmp_path)

        with patch('os.symlink', side_effect=OSError("Permission denied")):
            result = mover._create_symlink("/fake/link", "/fake/target")

        assert result is False

    @needs_symlink
    def test_remove_symlink_basic(self, tmp_path):
        """Symlink is removed when it exists."""
        mover = _make_symlink_mover(tmp_path)
        target = tmp_path / "target.mkv"
        target.write_bytes(b"data")
        symlink = tmp_path / "link.mkv"
        os.symlink(str(target), str(symlink))

        result = mover._remove_symlink(str(symlink))

        assert result is True
        assert not os.path.islink(str(symlink))
        # Target should still exist
        assert target.exists()

    @needs_symlink
    def test_remove_symlink_not_a_symlink(self, tmp_path):
        """Returns False for regular files (not symlinks)."""
        mover = _make_symlink_mover(tmp_path)
        regular_file = tmp_path / "regular.mkv"
        regular_file.write_bytes(b"data")

        result = mover._remove_symlink(str(regular_file))

        assert result is False
        # File should NOT be removed
        assert regular_file.exists()

    @needs_symlink
    def test_remove_symlink_nonexistent(self, tmp_path):
        """Returns False for paths that don't exist."""
        mover = _make_symlink_mover(tmp_path)

        result = mover._remove_symlink(str(tmp_path / "nonexistent"))

        assert result is False


# ============================================================================
# _move_to_cache symlink creation tests
# ============================================================================

class TestMoveToCache:
    """Test symlink creation during _move_to_cache."""

    @needs_symlink
    def test_symlink_created_after_cache(self, tmp_path):
        """_move_to_cache creates symlink at original_path when use_symlinks is enabled."""
        mover = _make_symlink_mover(tmp_path, use_symlinks=True, create_backups=True)

        # Set up array file
        storage_dir = tmp_path / "storage" / "Movies" / "Test (2024)"
        storage_dir.mkdir(parents=True)
        array_file = storage_dir / "movie.mkv"
        array_file.write_bytes(b"x" * 1000)

        # Set up cache destination
        cache_dir = tmp_path / "cache" / "Movies" / "Test (2024)"
        cache_dir.mkdir(parents=True)
        cache_file = cache_dir / "movie.mkv"

        # Mock copy to actually create the cache file
        def fake_copy(src, dst, *a, **kw):
            import shutil
            shutil.copy2(src, dst)
            return 0
        mover.file_utils.copy_file_with_permissions.side_effect = fake_copy

        original_path = str(array_file)
        result = mover._move_to_cache(
            array_file=str(array_file),
            cache_path=str(cache_dir),
            cache_file_name=str(cache_file),
            original_path=original_path,
        )

        assert result == 0
        # Symlink should exist at original location
        assert os.path.islink(original_path)
        # Symlink should point to cache file
        assert os.readlink(original_path) == str(cache_file)
        # .plexcached backup should exist
        assert os.path.isfile(str(array_file) + PLEXCACHED_EXTENSION)

    @needs_symlink
    def test_no_symlink_when_disabled(self, tmp_path):
        """Default use_symlinks=False creates no symlink."""
        mover = _make_symlink_mover(tmp_path, use_symlinks=False, create_backups=True)

        storage_dir = tmp_path / "storage" / "Movies"
        storage_dir.mkdir(parents=True)
        array_file = storage_dir / "movie.mkv"
        array_file.write_bytes(b"x" * 1000)

        cache_dir = tmp_path / "cache" / "Movies"
        cache_dir.mkdir(parents=True)
        cache_file = cache_dir / "movie.mkv"

        def fake_copy(src, dst, *a, **kw):
            import shutil
            shutil.copy2(src, dst)
            return 0
        mover.file_utils.copy_file_with_permissions.side_effect = fake_copy

        original_path = str(array_file)
        result = mover._move_to_cache(
            array_file=str(array_file),
            cache_path=str(cache_dir),
            cache_file_name=str(cache_file),
            original_path=original_path,
        )

        assert result == 0
        # No symlink should be created
        assert not os.path.islink(original_path)

    @needs_symlink
    def test_symlink_without_backups(self, tmp_path):
        """Symlinks work when .plexcached backups are disabled (delete mode)."""
        mover = _make_symlink_mover(tmp_path, use_symlinks=True, create_backups=False)

        storage_dir = tmp_path / "storage" / "Movies"
        storage_dir.mkdir(parents=True)
        array_file = storage_dir / "movie.mkv"
        array_file.write_bytes(b"x" * 1000)

        cache_dir = tmp_path / "cache" / "Movies"
        cache_dir.mkdir(parents=True)
        cache_file = cache_dir / "movie.mkv"

        def fake_copy(src, dst, *a, **kw):
            import shutil
            shutil.copy2(src, dst)
            return 0
        mover.file_utils.copy_file_with_permissions.side_effect = fake_copy

        original_path = str(array_file)
        result = mover._move_to_cache(
            array_file=str(array_file),
            cache_path=str(cache_dir),
            cache_file_name=str(cache_file),
            original_path=original_path,
        )

        assert result == 0
        # Symlink should exist at original location
        assert os.path.islink(original_path)
        # Original should be deleted (not renamed to .plexcached)
        assert not os.path.isfile(str(array_file) + PLEXCACHED_EXTENSION)

    @needs_symlink
    def test_symlink_overwrite_on_recache(self, tmp_path):
        """Existing symlink is replaced when re-caching same file."""
        mover = _make_symlink_mover(tmp_path, use_symlinks=True, create_backups=True)

        storage_dir = tmp_path / "storage" / "Movies"
        storage_dir.mkdir(parents=True)
        array_file = storage_dir / "movie.mkv"

        cache_dir = tmp_path / "cache" / "Movies"
        cache_dir.mkdir(parents=True)
        cache_file = cache_dir / "movie.mkv"

        # Create an old symlink pointing to wrong place
        old_target = tmp_path / "old_cache" / "movie.mkv"
        old_target.parent.mkdir(parents=True)
        old_target.write_bytes(b"old cached")
        os.symlink(str(old_target), str(array_file))

        # Now write the real array file via .plexcached (simulating a restore then re-cache)
        plexcached = storage_dir / ("movie.mkv" + PLEXCACHED_EXTENSION)
        plexcached.write_bytes(b"x" * 1000)

        def fake_copy(src, dst, *a, **kw):
            import shutil
            shutil.copy2(src, dst)
            return 0
        mover.file_utils.copy_file_with_permissions.side_effect = fake_copy

        # The symlink at array_file location will prevent normal rename, so we
        # test _create_symlink directly for overwrite
        result = mover._create_symlink(str(array_file), str(cache_file))
        assert result is True
        assert os.readlink(str(array_file)) == str(cache_file)


# ============================================================================
# _move_to_array symlink removal tests
# ============================================================================

class TestMoveToArray:
    """Test symlink removal during _move_to_array."""

    @needs_symlink
    def test_symlink_removed_before_array_restore(self, tmp_path):
        """_move_to_array removes symlink at array location before restoring."""
        mover = _make_symlink_mover(tmp_path, use_symlinks=True, create_backups=True)

        storage_dir = tmp_path / "storage" / "Movies"
        storage_dir.mkdir(parents=True)

        cache_dir = tmp_path / "cache" / "Movies"
        cache_dir.mkdir(parents=True)
        cache_file = cache_dir / "movie.mkv"
        cache_file.write_bytes(b"original content")

        # .plexcached backup on array (same size as cache so rename path is taken)
        plexcached = storage_dir / ("movie.mkv" + PLEXCACHED_EXTENSION)
        plexcached.write_bytes(b"original content")

        # Symlink at original location pointing to cache
        symlink = storage_dir / "movie.mkv"
        os.symlink(str(cache_file), str(symlink))

        result = mover._move_to_array(
            cache_file=str(cache_file),
            array_path=str(storage_dir),
            cache_file_name=str(cache_file),
        )

        assert result == 0
        # Symlink should be gone
        assert not os.path.islink(str(symlink))
        # Original file should be restored from .plexcached
        assert os.path.isfile(str(symlink))
        assert not os.path.islink(str(symlink))
        assert symlink.read_bytes() == b"original content"


# ============================================================================
# _get_move_command guard tests
# ============================================================================

class TestGetMoveCommandSymlinkGuard:
    """Test that _get_move_command doesn't treat symlinks as real files."""

    @needs_symlink
    def test_get_move_command_ignores_symlink(self, tmp_path):
        """Symlink at user_file_name doesn't cause false 'already exists' skip."""
        mover = _make_symlink_mover(tmp_path, use_symlinks=True, create_backups=True)

        storage_dir = tmp_path / "storage" / "Movies"
        storage_dir.mkdir(parents=True)

        cache_dir = tmp_path / "cache" / "Movies"
        cache_dir.mkdir(parents=True)
        cache_file = cache_dir / "movie.mkv"
        cache_file.write_bytes(b"cached")

        # Symlink at the user file location pointing to cache
        user_file = storage_dir / "movie.mkv"
        os.symlink(str(cache_file), str(user_file))

        # Also create .plexcached backup
        plexcached = storage_dir / ("movie.mkv" + PLEXCACHED_EXTENSION)
        plexcached.write_bytes(b"backup")

        result = mover._get_move_command(
            destination='array',
            cache_file_name=str(cache_file),
            user_path=str(storage_dir),
            user_file_name=str(user_file),
            cache_path=str(cache_dir),
        )

        # Should NOT return None (should not skip as "already exists")
        assert result is not None


# ============================================================================
# _should_add_to_cache / _should_add_to_array symlink guard tests
# ============================================================================


def _make_symlink_filter(tmp_path, *, use_symlinks=True):
    """Build a FileFilter configured for symlink testing."""
    exclude_file = str(tmp_path / "exclude.txt")
    with open(exclude_file, "w"):
        pass

    return FileFilter(
        real_source=str(tmp_path / "storage"),
        cache_dir=str(tmp_path / "cache"),
        is_unraid=False,
        mover_cache_exclude_file=exclude_file,
        use_symlinks=use_symlinks,
    )


class TestShouldAddSymlinkGuards:
    """Test that _should_add_to_cache and _should_add_to_array don't treat symlinks as real files."""

    @needs_symlink
    def test_should_add_to_cache_does_not_delete_symlink(self, tmp_path):
        """Second run: file already on cache with symlink at original — symlink must survive."""
        filt = _make_symlink_filter(tmp_path, use_symlinks=True)
        filt.last_already_cached_count = 0

        storage_dir = tmp_path / "storage" / "Movies"
        storage_dir.mkdir(parents=True)

        cache_dir = tmp_path / "cache" / "Movies"
        cache_dir.mkdir(parents=True)
        cache_file = cache_dir / "movie.mkv"
        cache_file.write_bytes(b"cached content")

        # .plexcached backup exists on array
        plexcached = storage_dir / ("movie.mkv" + PLEXCACHED_EXTENSION)
        plexcached.write_bytes(b"original content")

        # Symlink at original location (created by first run)
        original = storage_dir / "movie.mkv"
        os.symlink(str(cache_file), str(original))

        result = filt._should_add_to_cache(str(original), str(cache_file))

        # Should return False (already cached)
        assert result is False
        # Symlink must NOT be deleted
        assert os.path.islink(str(original))
        # .plexcached backup must NOT be deleted
        assert plexcached.exists()

    @needs_symlink
    def test_should_add_to_cache_recreates_missing_symlink(self, tmp_path):
        """If symlink was deleted (e.g., by Plex scan), it should be re-created."""
        filt = _make_symlink_filter(tmp_path, use_symlinks=True)
        filt.last_already_cached_count = 0

        storage_dir = tmp_path / "storage" / "Movies"
        storage_dir.mkdir(parents=True)

        cache_dir = tmp_path / "cache" / "Movies"
        cache_dir.mkdir(parents=True)
        cache_file = cache_dir / "movie.mkv"
        cache_file.write_bytes(b"cached content")

        # .plexcached backup exists but NO symlink (deleted by Plex or manually)
        plexcached = storage_dir / ("movie.mkv" + PLEXCACHED_EXTENSION)
        plexcached.write_bytes(b"original content")

        original = storage_dir / "movie.mkv"
        # No symlink and no real file at original location

        result = filt._should_add_to_cache(str(original), str(cache_file))

        assert result is False
        # Symlink should be re-created
        assert os.path.islink(str(original))
        assert os.readlink(str(original)) == str(cache_file)

    @needs_symlink
    def test_should_add_to_array_does_not_delete_cache_for_symlink(self, tmp_path):
        """Eviction: symlink at array path must not cause cache file deletion."""
        filt = _make_symlink_filter(tmp_path, use_symlinks=True)

        storage_dir = tmp_path / "storage" / "Movies"
        storage_dir.mkdir(parents=True)

        cache_dir = tmp_path / "cache" / "Movies"
        cache_dir.mkdir(parents=True)
        cache_file = cache_dir / "movie.mkv"
        cache_file.write_bytes(b"cached content")

        # Symlink at array location
        original = storage_dir / "movie.mkv"
        os.symlink(str(cache_file), str(original))

        should_add, cache_removed = filt._should_add_to_array(
            str(original), str(cache_file), media_to_cache=[]
        )

        # Should add to array (symlink doesn't count as real file)
        assert should_add is True
        assert cache_removed is False
        # Cache file must still exist
        assert cache_file.exists()
        # Symlink should still exist (removal happens in _move_to_array)
        assert os.path.islink(str(original))


# ============================================================================
# _cleanup_failed_cache_copy tests
# ============================================================================

class TestCleanupSymlink:
    """Test that cleanup removes symlinks on failure."""

    @needs_symlink
    def test_cleanup_removes_symlink_on_failure(self, tmp_path):
        """_cleanup_failed_cache_copy removes symlink and restores .plexcached."""
        mover = _make_symlink_mover(tmp_path, use_symlinks=True, create_backups=True)

        storage_dir = tmp_path / "storage" / "Movies"
        storage_dir.mkdir(parents=True)

        # .plexcached backup exists
        array_file = storage_dir / "movie.mkv"
        plexcached = storage_dir / ("movie.mkv" + PLEXCACHED_EXTENSION)
        plexcached.write_bytes(b"backup data")

        # Symlink was created at original path before failure
        cache_file = tmp_path / "cache" / "Movies" / "movie.mkv"
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        cache_file.write_bytes(b"partial")
        os.symlink(str(cache_file), str(array_file))

        mover._cleanup_failed_cache_copy(
            array_file=str(array_file),
            cache_file_name=str(cache_file),
            original_path=str(array_file),
        )

        # Symlink should be removed
        assert not os.path.islink(str(array_file))
        # .plexcached should be restored to original name
        assert os.path.isfile(str(array_file))
        assert array_file.read_bytes() == b"backup data"
        # Partial cache file should be removed
        assert not os.path.isfile(str(cache_file))


# ============================================================================
# PlexcachedRestorer symlink tests
# ============================================================================

class TestRestorerSymlink:
    """Test that PlexcachedRestorer handles symlinks at original locations."""

    @needs_symlink
    def test_restorer_handles_symlink(self, tmp_path):
        """restore_all removes symlink before renaming .plexcached back."""
        storage_dir = tmp_path / "storage" / "Movies"
        storage_dir.mkdir(parents=True)

        # .plexcached backup
        plexcached = storage_dir / ("movie.mkv" + PLEXCACHED_EXTENSION)
        plexcached.write_bytes(b"original content")

        # Symlink at original location
        cache_file = tmp_path / "cache" / "movie.mkv"
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        cache_file.write_bytes(b"cached")
        original = storage_dir / "movie.mkv"
        os.symlink(str(cache_file), str(original))

        restorer = PlexcachedRestorer([str(storage_dir)])
        success, errors = restorer.restore_all(dry_run=False)

        assert success == 1
        assert errors == 0
        # Symlink should be gone, original file restored
        assert not os.path.islink(str(original))
        assert os.path.isfile(str(original))
        assert original.read_bytes() == b"original content"


# ============================================================================
# Integration: symlink with plexcached backups end-to-end
# ============================================================================

class TestSymlinkEndToEnd:
    """End-to-end tests combining symlinks with .plexcached backup system."""

    @needs_symlink
    def test_symlink_with_plexcached_full_cycle(self, tmp_path):
        """Full cycle: cache (copy+rename+symlink) then restore (remove symlink+rename back)."""
        mover = _make_symlink_mover(tmp_path, use_symlinks=True, create_backups=True)

        # Setup
        storage_dir = tmp_path / "storage" / "Movies"
        storage_dir.mkdir(parents=True)
        array_file = storage_dir / "movie.mkv"
        array_file.write_bytes(b"original movie data" * 100)

        cache_dir = tmp_path / "cache" / "Movies"
        cache_dir.mkdir(parents=True)
        cache_file = cache_dir / "movie.mkv"

        def fake_copy(src, dst, *a, **kw):
            import shutil
            shutil.copy2(src, dst)
            return 0
        mover.file_utils.copy_file_with_permissions.side_effect = fake_copy

        # --- Phase 1: Cache the file ---
        original_path = str(array_file)
        result = mover._move_to_cache(
            array_file=str(array_file),
            cache_path=str(cache_dir),
            cache_file_name=str(cache_file),
            original_path=original_path,
        )
        assert result == 0

        # Verify state after caching:
        # - Cache file exists
        assert os.path.isfile(str(cache_file))
        # - Symlink at original location
        assert os.path.islink(original_path)
        assert os.readlink(original_path) == str(cache_file)
        # - .plexcached backup exists
        assert os.path.isfile(original_path + PLEXCACHED_EXTENSION)
        # - Plex can read file through symlink
        assert os.path.getsize(original_path) == os.path.getsize(str(cache_file))

        # --- Phase 2: Restore to array ---
        result = mover._move_to_array(
            cache_file=str(cache_file),
            array_path=str(storage_dir),
            cache_file_name=str(cache_file),
        )
        assert result == 0

        # Verify state after restore:
        # - Symlink removed
        assert not os.path.islink(original_path)
        # - Original file restored from .plexcached
        assert os.path.isfile(original_path)
        # - .plexcached backup gone
        assert not os.path.isfile(original_path + PLEXCACHED_EXTENSION)
        # - Cache copy deleted
        assert not os.path.isfile(str(cache_file))

    @needs_symlink
    def test_symlink_without_backups_full_cycle(self, tmp_path):
        """Full cycle without .plexcached backups: cache (copy+delete+symlink) then restore (copy back)."""
        mover = _make_symlink_mover(tmp_path, use_symlinks=True, create_backups=False)

        storage_dir = tmp_path / "storage" / "Movies"
        storage_dir.mkdir(parents=True)
        array_file = storage_dir / "movie.mkv"
        array_file.write_bytes(b"original movie data" * 100)

        cache_dir = tmp_path / "cache" / "Movies"
        cache_dir.mkdir(parents=True)
        cache_file = cache_dir / "movie.mkv"

        def fake_copy(src, dst, *a, **kw):
            import shutil
            shutil.copy2(src, dst)
            return 0
        mover.file_utils.copy_file_with_permissions.side_effect = fake_copy

        # --- Phase 1: Cache the file ---
        original_path = str(array_file)
        result = mover._move_to_cache(
            array_file=str(array_file),
            cache_path=str(cache_dir),
            cache_file_name=str(cache_file),
            original_path=original_path,
        )
        assert result == 0

        # Verify: symlink at original, no .plexcached, cache file exists
        assert os.path.islink(original_path)
        assert not os.path.isfile(original_path + PLEXCACHED_EXTENSION)
        assert os.path.isfile(str(cache_file))

        # --- Phase 2: Restore to array ---
        # Without .plexcached, this copies from cache to array
        result = mover._move_to_array(
            cache_file=str(cache_file),
            array_path=str(storage_dir),
            cache_file_name=str(cache_file),
        )
        assert result == 0

        # Verify: symlink removed, real file at original location
        assert not os.path.islink(original_path)
        assert os.path.isfile(original_path)
