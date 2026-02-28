"""Tests for cache status tracking on JSONTracker entries.

Verifies mark_cached(), mark_uncached(), and get_cached_entries() on
OnDeckTracker and WatchlistTracker (inherited from JSONTracker base).
"""

import os
import sys
import json
import tempfile
import threading
from datetime import datetime
from unittest.mock import MagicMock

# Mock fcntl for Windows compatibility
sys.modules['fcntl'] = MagicMock()

# Mock apscheduler
for _mod in [
    'apscheduler', 'apscheduler.schedulers',
    'apscheduler.schedulers.background', 'apscheduler.triggers',
    'apscheduler.triggers.cron', 'apscheduler.triggers.interval',
]:
    sys.modules.setdefault(_mod, MagicMock())

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from core.file_operations import OnDeckTracker, WatchlistTracker


@pytest.fixture
def ondeck_tracker(tmp_path):
    """Create a fresh OnDeckTracker with a temp file."""
    return OnDeckTracker(str(tmp_path / "ondeck_tracker.json"))


@pytest.fixture
def watchlist_tracker(tmp_path):
    """Create a fresh WatchlistTracker with a temp file."""
    return WatchlistTracker(str(tmp_path / "watchlist_tracker.json"))


class TestMarkCached:
    """Tests for mark_cached()."""

    def test_sets_fields_on_exact_path(self, ondeck_tracker):
        """mark_cached sets is_cached, cache_source, cached_at on exact path match."""
        ondeck_tracker.update_entry("/mnt/user/media/movie.mkv", "Alice")
        ondeck_tracker.mark_cached("/mnt/user/media/movie.mkv", "ondeck")

        entry = ondeck_tracker.get_entry("/mnt/user/media/movie.mkv")
        assert entry['is_cached'] is True
        assert entry['cache_source'] == "ondeck"
        assert 'cached_at' in entry

    def test_uses_filename_fallback(self, ondeck_tracker):
        """mark_cached finds entry by filename when full path doesn't match."""
        ondeck_tracker.update_entry("/mnt/user/media/movie.mkv", "Alice")
        ondeck_tracker.mark_cached("/mnt/cache/media/movie.mkv", "pre-existing")

        entry = ondeck_tracker.get_entry("/mnt/user/media/movie.mkv")
        assert entry['is_cached'] is True
        assert entry['cache_source'] == "pre-existing"

    def test_noop_when_entry_missing(self, ondeck_tracker):
        """mark_cached is a no-op when the entry doesn't exist."""
        ondeck_tracker.mark_cached("/mnt/user/media/nonexistent.mkv", "ondeck")
        assert ondeck_tracker.get_entry("/mnt/user/media/nonexistent.mkv") is None

    def test_custom_cached_at(self, ondeck_tracker):
        """mark_cached uses provided cached_at timestamp."""
        ondeck_tracker.update_entry("/mnt/user/media/movie.mkv", "Alice")
        ts = "2026-01-15T10:30:00"
        ondeck_tracker.mark_cached("/mnt/user/media/movie.mkv", "ondeck", cached_at=ts)

        entry = ondeck_tracker.get_entry("/mnt/user/media/movie.mkv")
        assert entry['cached_at'] == ts

    def test_idempotent(self, ondeck_tracker):
        """Calling mark_cached twice with same source is safe."""
        ondeck_tracker.update_entry("/mnt/user/media/movie.mkv", "Alice")
        ondeck_tracker.mark_cached("/mnt/user/media/movie.mkv", "ondeck")
        first_cached_at = ondeck_tracker.get_entry("/mnt/user/media/movie.mkv")['cached_at']

        ondeck_tracker.mark_cached("/mnt/user/media/movie.mkv", "ondeck")
        entry = ondeck_tracker.get_entry("/mnt/user/media/movie.mkv")
        assert entry['is_cached'] is True
        # Second call overwrites cached_at (expected — timestamp updates)
        assert entry['cache_source'] == "ondeck"


class TestMarkUncached:
    """Tests for mark_uncached()."""

    def test_clears_fields(self, ondeck_tracker):
        """mark_uncached sets is_cached=False and removes source/timestamp."""
        ondeck_tracker.update_entry("/mnt/user/media/movie.mkv", "Alice")
        ondeck_tracker.mark_cached("/mnt/user/media/movie.mkv", "ondeck")
        ondeck_tracker.mark_uncached("/mnt/user/media/movie.mkv")

        entry = ondeck_tracker.get_entry("/mnt/user/media/movie.mkv")
        assert entry['is_cached'] is False
        assert 'cache_source' not in entry
        assert 'cached_at' not in entry

    def test_noop_when_entry_missing(self, ondeck_tracker):
        """mark_uncached is a no-op when the entry doesn't exist."""
        ondeck_tracker.mark_uncached("/mnt/user/media/nonexistent.mkv")
        # Should not raise


class TestGetCachedEntries:
    """Tests for get_cached_entries()."""

    def test_returns_only_cached(self, ondeck_tracker):
        """get_cached_entries returns only entries where is_cached is True."""
        ondeck_tracker.update_entry("/mnt/user/media/movie1.mkv", "Alice")
        ondeck_tracker.update_entry("/mnt/user/media/movie2.mkv", "Alice")
        ondeck_tracker.update_entry("/mnt/user/media/movie3.mkv", "Alice")

        ondeck_tracker.mark_cached("/mnt/user/media/movie1.mkv", "ondeck")
        ondeck_tracker.mark_cached("/mnt/user/media/movie3.mkv", "watchlist")

        cached = ondeck_tracker.get_cached_entries()
        assert len(cached) == 2
        assert "/mnt/user/media/movie1.mkv" in cached
        assert "/mnt/user/media/movie3.mkv" in cached
        assert "/mnt/user/media/movie2.mkv" not in cached

    def test_returns_empty_when_none_cached(self, ondeck_tracker):
        """get_cached_entries returns empty dict when nothing is cached."""
        ondeck_tracker.update_entry("/mnt/user/media/movie.mkv", "Alice")
        assert ondeck_tracker.get_cached_entries() == {}

    def test_backward_compat_missing_is_cached(self, tmp_path):
        """Entries without is_cached field default to False (backward compat)."""
        tracker_file = str(tmp_path / "ondeck_tracker.json")
        # Write a tracker file without is_cached fields (pre-enhancement format)
        data = {
            "/mnt/user/media/movie.mkv": {
                "users": ["Alice"],
                "first_seen": "2026-01-01T00:00:00",
                "last_seen": "2026-01-01T00:00:00",
                "user_first_seen": {"Alice": "2026-01-01T00:00:00"},
                "ondeck_users": ["Alice"]
            }
        }
        with open(tracker_file, 'w') as f:
            json.dump(data, f, indent=2)

        tracker = OnDeckTracker(tracker_file)
        assert tracker.get_cached_entries() == {}


class TestPersistence:
    """Tests for disk persistence of cache status."""

    def test_mark_cached_persists_to_disk(self, tmp_path):
        """mark_cached changes survive tracker reload."""
        tracker_file = str(tmp_path / "ondeck_tracker.json")
        tracker = OnDeckTracker(tracker_file)
        tracker.update_entry("/mnt/user/media/movie.mkv", "Alice")
        tracker.mark_cached("/mnt/user/media/movie.mkv", "ondeck")

        # Reload from disk
        tracker2 = OnDeckTracker(tracker_file)
        entry = tracker2.get_entry("/mnt/user/media/movie.mkv")
        assert entry['is_cached'] is True
        assert entry['cache_source'] == "ondeck"
        assert 'cached_at' in entry


class TestInteractionWithExistingMethods:
    """Tests for interaction with prepare_for_run and cleanup_unseen."""

    def test_prepare_for_run_preserves_cache_status(self, ondeck_tracker):
        """prepare_for_run does not clear is_cached/cache_source/cached_at."""
        ondeck_tracker.update_entry("/mnt/user/media/movie.mkv", "Alice")
        ondeck_tracker.mark_cached("/mnt/user/media/movie.mkv", "ondeck")

        ondeck_tracker.prepare_for_run()

        entry = ondeck_tracker.get_entry("/mnt/user/media/movie.mkv")
        assert entry['is_cached'] is True
        assert entry['cache_source'] == "ondeck"
        assert 'cached_at' in entry

    def test_cleanup_unseen_removes_cached_entry(self, ondeck_tracker):
        """cleanup_unseen removes the whole entry including cache status."""
        ondeck_tracker.update_entry("/mnt/user/media/movie.mkv", "Alice")
        ondeck_tracker.mark_cached("/mnt/user/media/movie.mkv", "ondeck")

        ondeck_tracker.prepare_for_run()
        # Don't call update_entry — entry is unseen this run
        removed = ondeck_tracker.cleanup_unseen()
        assert removed == 1
        assert ondeck_tracker.get_entry("/mnt/user/media/movie.mkv") is None


class TestWatchlistTracker:
    """Tests for mark_cached on WatchlistTracker (Plex paths)."""

    def test_mark_cached_with_plex_paths(self, watchlist_tracker):
        """mark_cached works on WatchlistTracker with Plex-style paths."""
        watchlist_tracker.update_entry(
            "/mnt/user/media/movie.mkv", "Alice",
            watchlisted_at=datetime(2026, 1, 1)
        )
        watchlist_tracker.mark_cached("/mnt/user/media/movie.mkv", "watchlist")

        entry = watchlist_tracker.get_entry("/mnt/user/media/movie.mkv")
        assert entry['is_cached'] is True
        assert entry['cache_source'] == "watchlist"

    def test_get_cached_entries_on_watchlist(self, watchlist_tracker):
        """get_cached_entries works on WatchlistTracker."""
        watchlist_tracker.update_entry(
            "/mnt/user/media/movie1.mkv", "Alice",
            watchlisted_at=datetime(2026, 1, 1)
        )
        watchlist_tracker.update_entry(
            "/mnt/user/media/movie2.mkv", "Alice",
            watchlisted_at=datetime(2026, 1, 1)
        )
        watchlist_tracker.mark_cached("/mnt/user/media/movie1.mkv", "watchlist")

        cached = watchlist_tracker.get_cached_entries()
        assert len(cached) == 1
        assert "/mnt/user/media/movie1.mkv" in cached


class TestThreadSafety:
    """Tests for concurrent access to cache status methods."""

    def test_concurrent_mark_cached(self, tmp_path):
        """Concurrent mark_cached calls don't corrupt data."""
        tracker_file = str(tmp_path / "ondeck_tracker.json")
        tracker = OnDeckTracker(tracker_file)

        # Create many entries
        for i in range(20):
            tracker.update_entry(f"/mnt/user/media/movie{i}.mkv", "Alice")

        errors = []

        def mark_batch(start, end):
            try:
                for i in range(start, end):
                    tracker.mark_cached(f"/mnt/user/media/movie{i}.mkv", "ondeck")
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=mark_batch, args=(0, 10)),
            threading.Thread(target=mark_batch, args=(10, 20)),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"Errors during concurrent access: {errors}"
        cached = tracker.get_cached_entries()
        assert len(cached) == 20
