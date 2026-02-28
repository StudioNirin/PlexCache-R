"""Tests for rating key tracking and media upgrade detection.

Tests the rating_key field on OnDeckItem, reverse index on OnDeckTracker,
rating_key storage in trackers, upgrade detection logic, and settings.
"""

import os
import sys
import json
import shutil
import tempfile
from unittest.mock import MagicMock, patch, PropertyMock
from datetime import datetime, timedelta

import pytest

# Mock fcntl for Windows compatibility before any imports
sys.modules['fcntl'] = MagicMock()

# Mock apscheduler
for _mod in [
    'apscheduler', 'apscheduler.schedulers',
    'apscheduler.schedulers.background', 'apscheduler.triggers',
    'apscheduler.triggers.cron', 'apscheduler.triggers.interval',
]:
    sys.modules.setdefault(_mod, MagicMock())

# Mock plexapi and requests (not installed in test environment)
for _mod in [
    'plexapi', 'plexapi.server', 'plexapi.video', 'plexapi.myplex',
    'plexapi.library', 'plexapi.exceptions', 'requests',
]:
    sys.modules.setdefault(_mod, MagicMock())

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.plex_api import OnDeckItem
from core.file_operations import OnDeckTracker, WatchlistTracker, CacheTimestampTracker
from core.config import CacheConfig


# ============================================================================
# OnDeckItem rating_key tests
# ============================================================================

class TestOnDeckItemRatingKey:
    """Test that OnDeckItem dataclass has the rating_key field."""

    def test_rating_key_defaults_to_none(self):
        item = OnDeckItem(file_path="/media/movie.mkv", username="Alice")
        assert item.rating_key is None

    def test_rating_key_stores_string(self):
        item = OnDeckItem(
            file_path="/media/movie.mkv",
            username="Alice",
            rating_key="12345"
        )
        assert item.rating_key == "12345"

    def test_rating_key_with_all_fields(self):
        item = OnDeckItem(
            file_path="/media/tv/show/S01E01.mkv",
            username="Bob",
            episode_info={"show": "Test", "season": 1, "episode": 1},
            is_current_ondeck=True,
            rating_key="207619"
        )
        assert item.rating_key == "207619"
        assert item.is_current_ondeck is True
        assert item.episode_info["show"] == "Test"


# ============================================================================
# OnDeckTracker rating_key tests
# ============================================================================

class TestOnDeckTrackerRatingKey:
    """Test rating_key storage and reverse index in OnDeckTracker."""

    @pytest.fixture
    def tracker(self, tmp_path):
        return OnDeckTracker(str(tmp_path / "ondeck_tracker.json"))

    def test_stores_rating_key(self, tracker):
        tracker.update_entry("/media/movie.mkv", "Alice", rating_key="100")
        entry = tracker.get_entry("/media/movie.mkv")
        assert entry is not None
        assert entry["rating_key"] == "100"

    def test_rating_key_none_not_stored(self, tracker):
        tracker.update_entry("/media/movie.mkv", "Alice")
        entry = tracker.get_entry("/media/movie.mkv")
        assert entry is not None
        assert "rating_key" not in entry

    def test_does_not_overwrite_with_none(self, tracker):
        tracker.update_entry("/media/movie.mkv", "Alice", rating_key="100")
        tracker.update_entry("/media/movie.mkv", "Bob", rating_key=None)
        entry = tracker.get_entry("/media/movie.mkv")
        assert entry["rating_key"] == "100"

    def test_find_by_rating_key(self, tracker):
        tracker.update_entry("/media/movie.mkv", "Alice", rating_key="100")
        assert tracker.find_by_rating_key("100") == "/media/movie.mkv"

    def test_find_by_rating_key_not_found(self, tracker):
        tracker.update_entry("/media/movie.mkv", "Alice", rating_key="100")
        assert tracker.find_by_rating_key("999") is None

    def test_find_by_rating_key_no_entries(self, tracker):
        assert tracker.find_by_rating_key("100") is None

    def test_index_rebuilt_on_load(self, tmp_path):
        tracker_file = str(tmp_path / "ondeck_tracker.json")

        # Create tracker and add entry with rating_key
        tracker1 = OnDeckTracker(tracker_file)
        tracker1.update_entry("/media/movie.mkv", "Alice", rating_key="100")

        # Create new tracker from same file (simulates restart)
        tracker2 = OnDeckTracker(tracker_file)
        assert tracker2.find_by_rating_key("100") == "/media/movie.mkv"

    def test_index_cleaned_on_remove(self, tracker):
        tracker.update_entry("/media/movie.mkv", "Alice", rating_key="100")
        assert tracker.find_by_rating_key("100") == "/media/movie.mkv"

        tracker.remove_entry("/media/movie.mkv")
        assert tracker.find_by_rating_key("100") is None

    def test_index_updated_on_new_path(self, tracker):
        """When same rating_key appears with new path, index points to new path."""
        tracker.update_entry("/media/old.mkv", "Alice", rating_key="100")
        tracker.update_entry("/media/new.mkv", "Bob", rating_key="100")
        assert tracker.find_by_rating_key("100") == "/media/new.mkv"

    def test_index_cleaned_on_cleanup_unseen(self, tracker):
        tracker.update_entry("/media/movie1.mkv", "Alice", rating_key="100")
        tracker.update_entry("/media/movie2.mkv", "Bob", rating_key="200")

        tracker.prepare_for_run()
        # Only refresh movie2
        tracker.update_entry("/media/movie2.mkv", "Bob", rating_key="200")
        tracker.cleanup_unseen()

        assert tracker.find_by_rating_key("100") is None
        assert tracker.find_by_rating_key("200") == "/media/movie2.mkv"

    def test_index_cleaned_on_cleanup_stale(self, tracker):
        tracker.update_entry("/media/movie.mkv", "Alice", rating_key="100")

        # Make entry stale
        entry = tracker._data["/media/movie.mkv"]
        old_ts = (datetime.now() - timedelta(days=10)).isoformat()
        entry['last_seen'] = old_ts
        tracker._save()

        tracker.cleanup_stale_entries(max_days_since_seen=1)
        assert tracker.find_by_rating_key("100") is None

    def test_multiple_rating_keys(self, tracker):
        tracker.update_entry("/media/movie1.mkv", "Alice", rating_key="100")
        tracker.update_entry("/media/movie2.mkv", "Bob", rating_key="200")
        tracker.update_entry("/media/tv/ep1.mkv", "Alice", rating_key="300")

        assert tracker.find_by_rating_key("100") == "/media/movie1.mkv"
        assert tracker.find_by_rating_key("200") == "/media/movie2.mkv"
        assert tracker.find_by_rating_key("300") == "/media/tv/ep1.mkv"


# ============================================================================
# WatchlistTracker rating_key tests
# ============================================================================

class TestWatchlistTrackerRatingKey:
    """Test basic rating_key storage in WatchlistTracker."""

    @pytest.fixture
    def tracker(self, tmp_path):
        return WatchlistTracker(str(tmp_path / "watchlist_tracker.json"))

    def test_stores_rating_key(self, tracker):
        tracker.update_entry("/media/movie.mkv", "Alice", None, rating_key="100")
        entry = tracker.get_entry("/media/movie.mkv")
        assert entry is not None
        assert entry["rating_key"] == "100"

    def test_rating_key_none_not_stored_new(self, tracker):
        tracker.update_entry("/media/movie.mkv", "Alice", None)
        entry = tracker.get_entry("/media/movie.mkv")
        assert entry is not None
        assert "rating_key" not in entry

    def test_does_not_overwrite_with_none(self, tracker):
        tracker.update_entry("/media/movie.mkv", "Alice", None, rating_key="100")
        tracker.update_entry("/media/movie.mkv", "Bob", None, rating_key=None)
        entry = tracker.get_entry("/media/movie.mkv")
        assert entry["rating_key"] == "100"

    def test_updates_rating_key(self, tracker):
        tracker.update_entry("/media/movie.mkv", "Alice", None, rating_key="100")
        tracker.update_entry("/media/movie.mkv", "Alice", None, rating_key="200")
        entry = tracker.get_entry("/media/movie.mkv")
        assert entry["rating_key"] == "200"


# ============================================================================
# CacheTimestampTracker rating_key and get_entry tests
# ============================================================================

class TestTimestampTrackerRatingKey:
    """Test rating_key storage and get_entry in CacheTimestampTracker."""

    @pytest.fixture
    def tracker(self, tmp_path):
        return CacheTimestampTracker(str(tmp_path / "timestamps.json"))

    def test_stores_rating_key(self, tracker):
        tracker.record_cache_time("/cache/movie.mkv", source="ondeck", rating_key="100")
        entry = tracker.get_entry("/cache/movie.mkv")
        assert entry is not None
        assert entry["rating_key"] == "100"

    def test_rating_key_none_not_stored(self, tracker):
        tracker.record_cache_time("/cache/movie.mkv", source="ondeck")
        entry = tracker.get_entry("/cache/movie.mkv")
        assert entry is not None
        assert "rating_key" not in entry

    def test_get_entry_returns_none_for_missing(self, tracker):
        assert tracker.get_entry("/nonexistent.mkv") is None

    def test_get_entry_returns_full_dict(self, tracker):
        tracker.record_cache_time(
            "/cache/movie.mkv", source="ondeck",
            media_type="movie", rating_key="100"
        )
        entry = tracker.get_entry("/cache/movie.mkv")
        assert "cached_at" in entry
        assert entry["source"] == "ondeck"
        assert entry["media_type"] == "movie"
        assert entry["rating_key"] == "100"

    def test_record_does_not_overwrite_existing(self, tracker):
        """Existing entries (including rating_key) are preserved."""
        tracker.record_cache_time("/cache/movie.mkv", source="ondeck", rating_key="100")
        tracker.record_cache_time("/cache/movie.mkv", source="watchlist", rating_key="200")
        entry = tracker.get_entry("/cache/movie.mkv")
        assert entry["source"] == "ondeck"
        assert entry["rating_key"] == "100"


# ============================================================================
# CacheConfig settings tests
# ============================================================================

class TestCacheConfigSettings:
    """Test auto_transfer_upgrades and backup_upgraded_files settings."""

    def test_defaults_are_true(self):
        config = CacheConfig()
        assert config.auto_transfer_upgrades is True
        assert config.backup_upgraded_files is True

    def test_can_set_to_false(self):
        config = CacheConfig(auto_transfer_upgrades=False, backup_upgraded_files=False)
        assert config.auto_transfer_upgrades is False
        assert config.backup_upgraded_files is False


# ============================================================================
# Upgrade detection tests
# ============================================================================

class TestUpgradeDetection:
    """Test the upgrade detection logic in PlexCacheApp."""

    @pytest.fixture
    def mock_app(self, tmp_path):
        """Create a minimal mock PlexCacheApp with trackers."""
        app = MagicMock()
        app.dry_run = False

        # Real trackers
        app.ondeck_tracker = OnDeckTracker(str(tmp_path / "ondeck.json"))
        app.watchlist_tracker = WatchlistTracker(str(tmp_path / "watchlist.json"))
        app.timestamp_tracker = CacheTimestampTracker(str(tmp_path / "timestamps.json"))

        # Mock file_filter
        app.file_filter = MagicMock()
        app.file_filter.remove_files_from_exclude_list = MagicMock(return_value=True)
        app.file_filter._add_to_exclude_file = MagicMock()

        # Mock file_path_modifier
        app.file_path_modifier = MagicMock()
        app.file_path_modifier.convert_real_to_cache = MagicMock(
            side_effect=lambda p: (p.replace("/mnt/user/", "/mnt/cache/"), None)
        )

        # Mock config_manager
        app.config_manager = MagicMock()
        app.config_manager.cache.auto_transfer_upgrades = True
        app.config_manager.cache.backup_upgraded_files = True
        app.config_manager.cache.create_plexcached_backups = True

        return app

    def test_same_rk_different_path_is_upgrade(self, mock_app):
        """Same rating_key with different path triggers upgrade detection."""
        from core.app import PlexCacheApp

        # Set up pre-run state: rating_key "100" -> old path
        pre_run_rk_index = {"100": "/mnt/user/media/old.mkv"}

        # Current ondeck items: rating_key "100" -> new path
        ondeck_items = [
            OnDeckItem(
                file_path="/plex/media/new.mkv",
                username="Alice",
                rating_key="100"
            )
        ]
        plex_to_real = {"/plex/media/new.mkv": "/mnt/user/media/new.mkv"}

        # Call the detect method using the unbound method
        with patch.object(PlexCacheApp, '__init__', lambda self, *a, **kw: None):
            app = PlexCacheApp.__new__(PlexCacheApp)
            app.dry_run = False
            app.ondeck_tracker = mock_app.ondeck_tracker
            app.watchlist_tracker = mock_app.watchlist_tracker
            app.timestamp_tracker = mock_app.timestamp_tracker
            app.file_filter = mock_app.file_filter
            app.file_path_modifier = mock_app.file_path_modifier
            app.config_manager = mock_app.config_manager

            # Add the old entry to ondeck tracker so remove works
            app.ondeck_tracker.update_entry("/mnt/user/media/old.mkv", "Alice", rating_key="100")
            # Add new entry (as would happen in the update loop)
            app.ondeck_tracker.update_entry("/mnt/user/media/new.mkv", "Alice", rating_key="100")

            app._detect_and_transfer_upgrades(ondeck_items, plex_to_real, pre_run_rk_index)

        # Old entry should be removed
        assert app.ondeck_tracker.get_entry("/mnt/user/media/old.mkv") is None
        # New entry should exist
        assert app.ondeck_tracker.get_entry("/mnt/user/media/new.mkv") is not None
        # Exclude list should have been updated
        mock_app.file_filter.remove_files_from_exclude_list.assert_called()
        mock_app.file_filter._add_to_exclude_file.assert_called()

    def test_same_path_no_upgrade(self, mock_app):
        """Same rating_key with same path is not an upgrade."""
        from core.app import PlexCacheApp

        pre_run_rk_index = {"100": "/mnt/user/media/movie.mkv"}

        ondeck_items = [
            OnDeckItem(
                file_path="/plex/media/movie.mkv",
                username="Alice",
                rating_key="100"
            )
        ]
        plex_to_real = {"/plex/media/movie.mkv": "/mnt/user/media/movie.mkv"}

        with patch.object(PlexCacheApp, '__init__', lambda self, *a, **kw: None):
            app = PlexCacheApp.__new__(PlexCacheApp)
            app.dry_run = False
            app.ondeck_tracker = mock_app.ondeck_tracker
            app.file_filter = mock_app.file_filter
            app.file_path_modifier = mock_app.file_path_modifier
            app.config_manager = mock_app.config_manager

            app._detect_and_transfer_upgrades(ondeck_items, plex_to_real, pre_run_rk_index)

        # No exclude list changes
        mock_app.file_filter.remove_files_from_exclude_list.assert_not_called()

    def test_missing_rating_key_skipped(self, mock_app):
        """Items without rating_key are skipped."""
        from core.app import PlexCacheApp

        pre_run_rk_index = {"100": "/mnt/user/media/old.mkv"}

        ondeck_items = [
            OnDeckItem(
                file_path="/plex/media/new.mkv",
                username="Alice",
                rating_key=None  # No rating key
            )
        ]
        plex_to_real = {"/plex/media/new.mkv": "/mnt/user/media/new.mkv"}

        with patch.object(PlexCacheApp, '__init__', lambda self, *a, **kw: None):
            app = PlexCacheApp.__new__(PlexCacheApp)
            app.dry_run = False
            app.ondeck_tracker = mock_app.ondeck_tracker
            app.file_filter = mock_app.file_filter
            app.file_path_modifier = mock_app.file_path_modifier
            app.config_manager = mock_app.config_manager

            app._detect_and_transfer_upgrades(ondeck_items, plex_to_real, pre_run_rk_index)

        mock_app.file_filter.remove_files_from_exclude_list.assert_not_called()

    def test_empty_pre_run_index_skipped(self, mock_app):
        """Empty pre-run index means no upgrades detected (first run)."""
        from core.app import PlexCacheApp

        ondeck_items = [
            OnDeckItem(
                file_path="/plex/media/new.mkv",
                username="Alice",
                rating_key="100"
            )
        ]
        plex_to_real = {"/plex/media/new.mkv": "/mnt/user/media/new.mkv"}

        with patch.object(PlexCacheApp, '__init__', lambda self, *a, **kw: None):
            app = PlexCacheApp.__new__(PlexCacheApp)
            app.dry_run = False
            app.ondeck_tracker = mock_app.ondeck_tracker
            app.file_filter = mock_app.file_filter
            app.file_path_modifier = mock_app.file_path_modifier
            app.config_manager = mock_app.config_manager

            app._detect_and_transfer_upgrades(ondeck_items, plex_to_real, {})

        mock_app.file_filter.remove_files_from_exclude_list.assert_not_called()

    def test_dry_run_no_changes(self, mock_app):
        """In dry-run mode, upgrades are logged but no changes made."""
        from core.app import PlexCacheApp

        pre_run_rk_index = {"100": "/mnt/user/media/old.mkv"}

        ondeck_items = [
            OnDeckItem(
                file_path="/plex/media/new.mkv",
                username="Alice",
                rating_key="100"
            )
        ]
        plex_to_real = {"/plex/media/new.mkv": "/mnt/user/media/new.mkv"}

        with patch.object(PlexCacheApp, '__init__', lambda self, *a, **kw: None):
            app = PlexCacheApp.__new__(PlexCacheApp)
            app.dry_run = True  # Dry run
            app.ondeck_tracker = mock_app.ondeck_tracker
            app.watchlist_tracker = mock_app.watchlist_tracker
            app.timestamp_tracker = mock_app.timestamp_tracker
            app.file_filter = mock_app.file_filter
            app.file_path_modifier = mock_app.file_path_modifier
            app.config_manager = mock_app.config_manager

            app.ondeck_tracker.update_entry("/mnt/user/media/old.mkv", "Alice", rating_key="100")

            app._detect_and_transfer_upgrades(ondeck_items, plex_to_real, pre_run_rk_index)

        # Old entry should NOT be removed in dry-run
        assert app.ondeck_tracker.get_entry("/mnt/user/media/old.mkv") is not None
        # No exclude list changes
        mock_app.file_filter.remove_files_from_exclude_list.assert_not_called()

    def test_timestamp_tracker_transferred(self, mock_app):
        """Timestamp tracker entry is transferred from old to new path."""
        from core.app import PlexCacheApp

        pre_run_rk_index = {"100": "/mnt/user/media/old.mkv"}

        ondeck_items = [
            OnDeckItem(
                file_path="/plex/media/new.mkv",
                username="Alice",
                rating_key="100",
                episode_info={"show": "Test", "season": 1, "episode": 3}
            )
        ]
        plex_to_real = {"/plex/media/new.mkv": "/mnt/user/media/new.mkv"}

        with patch.object(PlexCacheApp, '__init__', lambda self, *a, **kw: None):
            app = PlexCacheApp.__new__(PlexCacheApp)
            app.dry_run = False
            app.ondeck_tracker = mock_app.ondeck_tracker
            app.watchlist_tracker = mock_app.watchlist_tracker
            app.timestamp_tracker = mock_app.timestamp_tracker
            app.file_filter = mock_app.file_filter
            app.file_path_modifier = mock_app.file_path_modifier
            app.config_manager = mock_app.config_manager

            # Set up old entries
            app.ondeck_tracker.update_entry("/mnt/user/media/old.mkv", "Alice", rating_key="100")
            app.ondeck_tracker.update_entry("/mnt/user/media/new.mkv", "Alice", rating_key="100")
            old_cache_path = "/mnt/cache/media/old.mkv"
            app.timestamp_tracker.record_cache_time(old_cache_path, source="ondeck")

            app._detect_and_transfer_upgrades(ondeck_items, plex_to_real, pre_run_rk_index)

        # Old timestamp entry removed
        assert app.timestamp_tracker.get_entry("/mnt/cache/media/old.mkv") is None
        # New timestamp entry created with preserved source
        new_entry = app.timestamp_tracker.get_entry("/mnt/cache/media/new.mkv")
        assert new_entry is not None
        assert new_entry["source"] == "ondeck"
        assert new_entry["rating_key"] == "100"

    def test_watchlist_entry_transferred(self, mock_app):
        """Watchlist tracker entry is transferred when it exists for old path."""
        from core.app import PlexCacheApp

        pre_run_rk_index = {"100": "/mnt/user/media/old.mkv"}

        ondeck_items = [
            OnDeckItem(
                file_path="/plex/media/new.mkv",
                username="Alice",
                rating_key="100"
            )
        ]
        plex_to_real = {"/plex/media/new.mkv": "/mnt/user/media/new.mkv"}

        with patch.object(PlexCacheApp, '__init__', lambda self, *a, **kw: None):
            app = PlexCacheApp.__new__(PlexCacheApp)
            app.dry_run = False
            app.ondeck_tracker = mock_app.ondeck_tracker
            app.watchlist_tracker = mock_app.watchlist_tracker
            app.timestamp_tracker = mock_app.timestamp_tracker
            app.file_filter = mock_app.file_filter
            app.file_path_modifier = mock_app.file_path_modifier
            app.config_manager = mock_app.config_manager

            # Set up old entries
            app.ondeck_tracker.update_entry("/mnt/user/media/old.mkv", "Alice", rating_key="100")
            app.ondeck_tracker.update_entry("/mnt/user/media/new.mkv", "Alice", rating_key="100")
            app.watchlist_tracker.update_entry(
                "/mnt/user/media/old.mkv", "Alice", datetime(2026, 1, 15)
            )
            app.watchlist_tracker.update_entry(
                "/mnt/user/media/old.mkv", "Bob", datetime(2026, 1, 16)
            )

            app._detect_and_transfer_upgrades(ondeck_items, plex_to_real, pre_run_rk_index)

        # Old watchlist entry removed
        assert app.watchlist_tracker.get_entry("/mnt/user/media/old.mkv") is None
        # New watchlist entry created with both users
        new_entry = app.watchlist_tracker.get_entry("/mnt/user/media/new.mkv")
        assert new_entry is not None
        assert "Alice" in new_entry["users"]
        assert "Bob" in new_entry["users"]
        assert new_entry["rating_key"] == "100"
