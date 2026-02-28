"""Tests for PlexcachedRestorer - emergency recovery of .plexcached files.

CRITICAL: This is the last-resort recovery tool (--restore-plexcached).
If broken during an emergency, data loss is irreversible.
"""

import os
import sys
from unittest.mock import MagicMock

# conftest.py handles fcntl/apscheduler mocking and path setup
from core.file_operations import PlexcachedRestorer, PLEXCACHED_EXTENSION


# ============================================================================
# TestFindPlexcachedFiles
# ============================================================================

class TestFindPlexcachedFiles:
    """Tests for PlexcachedRestorer.find_plexcached_files()."""

    def test_finds_plexcached_in_flat_dir(self, tmp_path):
        """Finds .plexcached files in a flat directory."""
        (tmp_path / "movie.mkv.plexcached").write_text("data")
        (tmp_path / "movie.mkv").write_text("data")  # not .plexcached

        restorer = PlexcachedRestorer([str(tmp_path)])
        found = restorer.find_plexcached_files()

        assert len(found) == 1
        assert found[0].endswith("movie.mkv.plexcached")

    def test_finds_plexcached_in_nested_dirs(self, tmp_path):
        """Finds .plexcached files recursively in nested directories."""
        show_dir = tmp_path / "TV" / "Show" / "Season 1"
        show_dir.mkdir(parents=True)
        (show_dir / "ep01.mkv.plexcached").write_text("data")
        (show_dir / "ep02.mkv.plexcached").write_text("data")

        restorer = PlexcachedRestorer([str(tmp_path)])
        found = restorer.find_plexcached_files()

        assert len(found) == 2

    def test_skips_hidden_dirs(self, tmp_path):
        """Skips hidden directories (e.g., .Trash, .Recycle.Bin)."""
        hidden = tmp_path / ".Trash"
        hidden.mkdir()
        (hidden / "deleted.mkv.plexcached").write_text("data")

        visible = tmp_path / "Movies"
        visible.mkdir()
        (visible / "movie.mkv.plexcached").write_text("data")

        restorer = PlexcachedRestorer([str(tmp_path)])
        found = restorer.find_plexcached_files()

        assert len(found) == 1
        assert "Movies" in found[0]

    def test_skips_nonexistent_paths(self, tmp_path):
        """Logs warning and skips search paths that don't exist."""
        restorer = PlexcachedRestorer([str(tmp_path / "nonexistent")])
        found = restorer.find_plexcached_files()

        assert found == []

    def test_ignores_non_plexcached_files(self, tmp_path):
        """Only finds files ending with .plexcached, not other extensions."""
        (tmp_path / "movie.mkv").write_text("data")
        (tmp_path / "movie.srt").write_text("data")
        (tmp_path / "notes.txt").write_text("data")

        restorer = PlexcachedRestorer([str(tmp_path)])
        found = restorer.find_plexcached_files()

        assert found == []

    def test_handles_multiple_search_paths(self, tmp_path):
        """Searches across multiple search paths."""
        path_a = tmp_path / "share_a"
        path_b = tmp_path / "share_b"
        path_a.mkdir()
        path_b.mkdir()
        (path_a / "file1.mkv.plexcached").write_text("data")
        (path_b / "file2.mkv.plexcached").write_text("data")

        restorer = PlexcachedRestorer([str(path_a), str(path_b)])
        found = restorer.find_plexcached_files()

        assert len(found) == 2

    def test_mixed_existing_and_nonexistent_paths(self, tmp_path):
        """Works when some search paths exist and others don't."""
        real = tmp_path / "real"
        real.mkdir()
        (real / "movie.mkv.plexcached").write_text("data")

        restorer = PlexcachedRestorer([str(real), str(tmp_path / "fake")])
        found = restorer.find_plexcached_files()

        assert len(found) == 1


# ============================================================================
# TestRestoreAll
# ============================================================================

class TestRestoreAll:
    """Tests for PlexcachedRestorer.restore_all()."""

    def test_basic_rename(self, tmp_path):
        """Renames .mkv.plexcached back to .mkv."""
        pc = tmp_path / "movie.mkv.plexcached"
        pc.write_text("movie data")

        restorer = PlexcachedRestorer([str(tmp_path)])
        success, errors = restorer.restore_all()

        assert success == 1
        assert errors == 0
        assert not pc.exists()
        assert (tmp_path / "movie.mkv").exists()
        assert (tmp_path / "movie.mkv").read_text() == "movie data"

    def test_multiple_files_restored(self, tmp_path):
        """Restores multiple .plexcached files in one pass."""
        for i in range(3):
            (tmp_path / f"movie{i}.mkv.plexcached").write_text(f"data{i}")

        restorer = PlexcachedRestorer([str(tmp_path)])
        success, errors = restorer.restore_all()

        assert success == 3
        assert errors == 0
        for i in range(3):
            assert (tmp_path / f"movie{i}.mkv").exists()

    def test_dry_run_no_rename(self, tmp_path):
        """Dry run counts files but does not rename them."""
        pc = tmp_path / "movie.mkv.plexcached"
        pc.write_text("data")

        restorer = PlexcachedRestorer([str(tmp_path)])
        success, errors = restorer.restore_all(dry_run=True)

        assert success == 1
        assert errors == 0
        assert pc.exists()  # Still there
        assert not (tmp_path / "movie.mkv").exists()  # Not created

    def test_original_already_exists_skips(self, tmp_path):
        """CRITICAL: Skips restore if original file already exists (avoids overwrite)."""
        pc = tmp_path / "movie.mkv.plexcached"
        pc.write_text("backup data")
        original = tmp_path / "movie.mkv"
        original.write_text("current data")

        restorer = PlexcachedRestorer([str(tmp_path)])
        success, errors = restorer.restore_all()

        assert success == 0
        assert errors == 1
        # Both files preserved
        assert pc.exists()
        assert original.read_text() == "current data"

    def test_rename_error_handling(self, tmp_path, monkeypatch):
        """Handles rename errors gracefully and counts as error."""
        pc = tmp_path / "movie.mkv.plexcached"
        pc.write_text("data")

        def failing_rename(src, dst):
            raise OSError("Permission denied")

        monkeypatch.setattr(os, 'rename', failing_rename)

        restorer = PlexcachedRestorer([str(tmp_path)])
        success, errors = restorer.restore_all()

        assert success == 0
        assert errors == 1

    def test_empty_search_returns_zero(self, tmp_path):
        """Returns (0, 0) when no .plexcached files found."""
        restorer = PlexcachedRestorer([str(tmp_path)])
        success, errors = restorer.restore_all()

        assert success == 0
        assert errors == 0

    def test_correct_return_counts(self, tmp_path):
        """Returns accurate (success_count, error_count) tuple."""
        (tmp_path / "ok1.mkv.plexcached").write_text("data")
        (tmp_path / "ok2.mkv.plexcached").write_text("data")
        # This one will fail: original already exists
        (tmp_path / "conflict.mkv.plexcached").write_text("backup")
        (tmp_path / "conflict.mkv").write_text("original")

        restorer = PlexcachedRestorer([str(tmp_path)])
        success, errors = restorer.restore_all()

        assert success == 2
        assert errors == 1

    def test_preserves_directory_structure(self, tmp_path):
        """Restores files in-place within their directory structure."""
        deep_dir = tmp_path / "TV" / "Show" / "Season 1"
        deep_dir.mkdir(parents=True)
        pc = deep_dir / "ep01.mkv.plexcached"
        pc.write_text("episode data")

        restorer = PlexcachedRestorer([str(tmp_path)])
        success, errors = restorer.restore_all()

        assert success == 1
        restored = deep_dir / "ep01.mkv"
        assert restored.exists()
        assert restored.read_text() == "episode data"

    def test_restores_various_extensions(self, tmp_path):
        """Correctly restores files with different original extensions."""
        (tmp_path / "movie.mp4.plexcached").write_text("mp4")
        (tmp_path / "show.avi.plexcached").write_text("avi")
        (tmp_path / "sub.en.srt.plexcached").write_text("srt")

        restorer = PlexcachedRestorer([str(tmp_path)])
        success, _ = restorer.restore_all()

        assert success == 3
        assert (tmp_path / "movie.mp4").exists()
        assert (tmp_path / "show.avi").exists()
        assert (tmp_path / "sub.en.srt").exists()
