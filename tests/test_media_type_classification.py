"""Tests for Plex API media type classification (issue #13 enhancement).

Tests metadata storage, retrieval, enrichment, and classification fallback logic.
"""

import os
import sys
import json
import tempfile
import shutil
from unittest.mock import MagicMock, patch
from datetime import datetime

import pytest

# Ensure conftest mocks are applied
from conftest import create_test_file

from core.file_operations import (
    CacheTimestampTracker,
    OnDeckTracker,
    WatchlistTracker,
    FileFilter,
    CachePriorityManager,
)


# ============================================================================
# CacheTimestampTracker: record_cache_time with media metadata
# ============================================================================

class TestCacheTimestampTrackerMediaFields:
    """Test CacheTimestampTracker media_type and episode_info storage."""

    @pytest.fixture
    def tracker(self, tmp_path):
        return CacheTimestampTracker(str(tmp_path / "timestamps.json"))

    def test_record_with_episode_metadata(self, tracker):
        """New fields are stored when provided."""
        tracker.record_cache_time(
            "/mnt/cache/TV/Show/S01E05.mkv",
            source="ondeck",
            media_type="episode",
            episode_info={"show": "Foundation", "season": 1, "episode": 5}
        )
        assert tracker.get_media_type("/mnt/cache/TV/Show/S01E05.mkv") == "episode"
        ep = tracker.get_episode_info("/mnt/cache/TV/Show/S01E05.mkv")
        assert ep == {"show": "Foundation", "season": 1, "episode": 5}

    def test_record_with_movie_metadata(self, tracker):
        """Movie type stored, episode_info is None."""
        tracker.record_cache_time(
            "/mnt/cache/Movies/Film.mkv",
            source="watchlist",
            media_type="movie"
        )
        assert tracker.get_media_type("/mnt/cache/Movies/Film.mkv") == "movie"
        assert tracker.get_episode_info("/mnt/cache/Movies/Film.mkv") is None

    def test_record_without_metadata_fields(self, tracker):
        """Legacy behavior: no media_type stored, getters return None."""
        tracker.record_cache_time("/mnt/cache/Movies/Old.mkv", source="ondeck")
        assert tracker.get_media_type("/mnt/cache/Movies/Old.mkv") is None
        assert tracker.get_episode_info("/mnt/cache/Movies/Old.mkv") is None

    def test_record_does_not_overwrite_existing(self, tracker):
        """First recording wins — subsequent calls don't update."""
        tracker.record_cache_time(
            "/mnt/cache/TV/S01E01.mkv",
            source="ondeck",
            media_type="episode",
            episode_info={"show": "A", "season": 1, "episode": 1}
        )
        # Second call with different metadata should be ignored
        tracker.record_cache_time(
            "/mnt/cache/TV/S01E01.mkv",
            source="watchlist",
            media_type="movie"
        )
        assert tracker.get_media_type("/mnt/cache/TV/S01E01.mkv") == "episode"

    def test_metadata_persists_across_reload(self, tmp_path):
        """Metadata survives save/load cycle."""
        path = str(tmp_path / "ts.json")
        tracker1 = CacheTimestampTracker(path)
        tracker1.record_cache_time(
            "/mnt/cache/TV/S02E03.mkv",
            source="ondeck",
            media_type="episode",
            episode_info={"show": "Lost", "season": 2, "episode": 3}
        )
        # Reload from disk
        tracker2 = CacheTimestampTracker(path)
        assert tracker2.get_media_type("/mnt/cache/TV/S02E03.mkv") == "episode"
        ep = tracker2.get_episode_info("/mnt/cache/TV/S02E03.mkv")
        assert ep["show"] == "Lost"
        assert ep["season"] == 2

    def test_backward_compat_old_entries(self, tmp_path):
        """Old entries without media_type return None from getters."""
        path = str(tmp_path / "ts.json")
        old_data = {
            "/mnt/cache/TV/Old.mkv": {
                "cached_at": "2026-01-01T00:00:00",
                "source": "ondeck"
            }
        }
        with open(path, 'w') as f:
            json.dump(old_data, f)

        tracker = CacheTimestampTracker(path)
        assert tracker.get_media_type("/mnt/cache/TV/Old.mkv") is None
        assert tracker.get_episode_info("/mnt/cache/TV/Old.mkv") is None

    def test_get_media_type_nonexistent(self, tracker):
        """Nonexistent path returns None."""
        assert tracker.get_media_type("/nonexistent") is None

    def test_get_episode_info_nonexistent(self, tracker):
        """Nonexistent path returns None."""
        assert tracker.get_episode_info("/nonexistent") is None


# ============================================================================
# CacheTimestampTracker: enrich_media_info
# ============================================================================

class TestEnrichMediaInfo:
    """Test the enrich_media_info backfill method."""

    @pytest.fixture
    def tracker(self, tmp_path):
        t = CacheTimestampTracker(str(tmp_path / "timestamps.json"))
        # Record an entry without media metadata (simulates pre-existing)
        t.record_cache_time("/mnt/cache/TV/Show/S01E01.mkv", source="pre-existing")
        return t

    def test_enriches_entry_without_metadata(self, tracker):
        """Enrich sets media_type on entries that lack it."""
        tracker.enrich_media_info(
            "/mnt/cache/TV/Show/S01E01.mkv",
            media_type="episode",
            episode_info={"show": "Show", "season": 1, "episode": 1}
        )
        assert tracker.get_media_type("/mnt/cache/TV/Show/S01E01.mkv") == "episode"
        ep = tracker.get_episode_info("/mnt/cache/TV/Show/S01E01.mkv")
        assert ep["show"] == "Show"

    def test_does_not_overwrite_existing_metadata(self, tracker):
        """Enrich does nothing if media_type already set."""
        # First set it
        tracker.enrich_media_info(
            "/mnt/cache/TV/Show/S01E01.mkv",
            media_type="episode",
            episode_info={"show": "Show", "season": 1, "episode": 1}
        )
        # Try to overwrite with movie — should be ignored
        tracker.enrich_media_info(
            "/mnt/cache/TV/Show/S01E01.mkv",
            media_type="movie"
        )
        assert tracker.get_media_type("/mnt/cache/TV/Show/S01E01.mkv") == "episode"

    def test_does_nothing_for_nonexistent_entry(self, tracker):
        """Enrich is a no-op for entries that don't exist."""
        tracker.enrich_media_info(
            "/mnt/cache/Nonexistent.mkv",
            media_type="movie"
        )
        assert tracker.get_media_type("/mnt/cache/Nonexistent.mkv") is None

    def test_does_nothing_when_media_type_is_none(self, tracker):
        """Enrich with media_type=None is a no-op."""
        tracker.enrich_media_info(
            "/mnt/cache/TV/Show/S01E01.mkv",
            media_type=None
        )
        assert tracker.get_media_type("/mnt/cache/TV/Show/S01E01.mkv") is None


# ============================================================================
# FileFilter._lookup_media_info
# ============================================================================

class TestLookupMediaInfo:
    """Test the _lookup_media_info helper for metadata-first classification."""

    @pytest.fixture
    def file_filter(self, tmp_path):
        ts_tracker = CacheTimestampTracker(str(tmp_path / "ts.json"))
        ondeck_tracker = OnDeckTracker(str(tmp_path / "ondeck.json"))
        ff = FileFilter(
            real_source="/mnt/user/",
            cache_dir="/mnt/cache/",
            is_unraid=False,
            mover_cache_exclude_file="",
            timestamp_tracker=ts_tracker,
            ondeck_tracker=ondeck_tracker,
        )
        return ff

    def test_returns_episode_from_ondeck_tracker(self, file_filter):
        """OnDeck tracker is checked first and returns episode info."""
        file_filter.ondeck_tracker.update_entry(
            "/mnt/user/TV/Show/S01E05.mkv",
            "user1",
            episode_info={"show": "Show", "season": 1, "episode": 5},
            is_current_ondeck=True
        )
        result = file_filter._lookup_media_info("/mnt/user/TV/Show/S01E05.mkv")
        assert result is not None
        media_type, ep_info = result
        assert media_type == "episode"
        assert ep_info["show"] == "Show"

    def test_returns_movie_from_ondeck_tracker(self, file_filter):
        """OnDeck tracker entry without episode_info → movie."""
        # Manually add an entry without episode_info
        file_filter.ondeck_tracker._data["/mnt/user/Movies/Film.mkv"] = {
            "users": ["user1"],
            "first_seen": datetime.now().isoformat(),
            "last_seen": datetime.now().isoformat(),
        }
        result = file_filter._lookup_media_info("/mnt/user/Movies/Film.mkv")
        assert result is not None
        media_type, ep_info = result
        assert media_type == "movie"
        assert ep_info is None

    def test_returns_from_media_info_map(self, file_filter):
        """media_info_map is checked after OnDeck tracker."""
        file_filter.set_media_info_map({
            "/mnt/user/TV/Anime/ep01.mkv": {
                "media_type": "episode",
                "episode_info": {"show": "Anime", "season": 1, "episode": 1}
            }
        })
        result = file_filter._lookup_media_info("/mnt/user/TV/Anime/ep01.mkv")
        assert result is not None
        media_type, ep_info = result
        assert media_type == "episode"
        assert ep_info["show"] == "Anime"

    def test_returns_from_timestamp_tracker(self, file_filter):
        """CacheTimestampTracker (persistent) is checked last."""
        file_filter.timestamp_tracker.record_cache_time(
            "/mnt/cache/TV/Show/S02E01.mkv",
            source="ondeck",
            media_type="episode",
            episode_info={"show": "Show", "season": 2, "episode": 1}
        )
        result = file_filter._lookup_media_info("/mnt/cache/TV/Show/S02E01.mkv")
        assert result is not None
        media_type, ep_info = result
        assert media_type == "episode"
        assert ep_info["season"] == 2

    def test_returns_none_for_unknown_file(self, file_filter):
        """No metadata anywhere → None (caller falls back to regex)."""
        result = file_filter._lookup_media_info("/mnt/user/Unknown/file.mkv")
        assert result is None

    def test_ondeck_takes_priority_over_timestamp(self, file_filter):
        """OnDeck tracker is preferred over timestamp tracker."""
        # Set both sources with different info
        path = "/mnt/cache/TV/Show/S01E01.mkv"
        file_filter.timestamp_tracker.record_cache_time(
            path, source="watchlist", media_type="movie"
        )
        file_filter.ondeck_tracker.update_entry(
            path, "user1",
            episode_info={"show": "Show", "season": 1, "episode": 1},
            is_current_ondeck=True
        )
        result = file_filter._lookup_media_info(path)
        assert result is not None
        media_type, _ = result
        assert media_type == "episode"  # OnDeck wins


# ============================================================================
# FileFilter._build_needed_media_sets with metadata
# ============================================================================

class TestBuildNeededMediaSetsWithMetadata:
    """Test _build_needed_media_sets uses metadata-first classification."""

    @pytest.fixture
    def file_filter(self, tmp_path):
        ts_tracker = CacheTimestampTracker(str(tmp_path / "ts.json"))
        ondeck_tracker = OnDeckTracker(str(tmp_path / "ondeck.json"))
        ff = FileFilter(
            real_source="/mnt/user/",
            cache_dir="/mnt/cache/",
            is_unraid=False,
            mover_cache_exclude_file="",
            timestamp_tracker=ts_tracker,
            ondeck_tracker=ondeck_tracker,
        )
        return ff

    def test_classifies_via_metadata(self, file_filter):
        """Items with metadata are classified without regex."""
        # This path has no S01E01 pattern — regex would fail
        anime_path = "/mnt/user/TV/One Piece/Water 7 Arc/ep305.mkv"
        file_filter.ondeck_tracker.update_entry(
            anime_path, "user1",
            episode_info={"show": "One Piece", "season": 1, "episode": 305},
            is_current_ondeck=True
        )
        ondeck = {anime_path}
        tv_eps, movies = file_filter._build_needed_media_sets(ondeck, set())
        assert "One Piece" in tv_eps

    def test_falls_back_to_regex(self, file_filter):
        """Items without metadata fall back to regex parsing."""
        standard_path = "/mnt/user/TV/Breaking Bad/Season 01/S01E01.mkv"
        ondeck = {standard_path}
        tv_eps, movies = file_filter._build_needed_media_sets(ondeck, set())
        assert "Breaking Bad" in tv_eps

    def test_movie_via_metadata(self, file_filter):
        """Movie identified via metadata, not just regex."""
        movie_path = "/mnt/user/Movies/Inception (2010)/Inception.mkv"
        file_filter.set_media_info_map({
            movie_path: {"media_type": "movie", "episode_info": None}
        })
        watchlist = {movie_path}
        tv_eps, movies = file_filter._build_needed_media_sets(set(), watchlist)
        assert len(movies) > 0


# ============================================================================
# FileFilter.get_files_to_move_back_to_array with metadata
# ============================================================================

class TestMoveBackWithMetadata:
    """Test get_files_to_move_back_to_array uses stored metadata."""

    @pytest.fixture
    def setup(self, tmp_path):
        """Create a file filter with exclude file and cache files."""
        cache_dir = str(tmp_path / "cache")
        os.makedirs(cache_dir, exist_ok=True)

        # Create cache files
        tv_file = os.path.join(cache_dir, "TV", "Anime", "Arc Name", "ep01.mkv")
        create_test_file(tv_file)

        # Create exclude file
        exclude_path = str(tmp_path / "exclude.txt")
        with open(exclude_path, 'w') as f:
            f.write(tv_file + "\n")

        ts_tracker = CacheTimestampTracker(str(tmp_path / "ts.json"))
        ondeck_tracker = OnDeckTracker(str(tmp_path / "ondeck.json"))

        # Store metadata in timestamp tracker (simulating prior run)
        ts_tracker.record_cache_time(
            tv_file, source="ondeck",
            media_type="episode",
            episode_info={"show": "Anime", "season": 1, "episode": 1}
        )

        ff = FileFilter(
            real_source="/mnt/user/",
            cache_dir=cache_dir,
            is_unraid=False,
            mover_cache_exclude_file=exclude_path,
            timestamp_tracker=ts_tracker,
            cache_retention_hours=0,
            ondeck_tracker=ondeck_tracker,
        )
        return ff, tv_file

    def test_uses_stored_metadata_for_cached_files(self, setup):
        """Cached files classified via timestamp tracker metadata."""
        ff, tv_file = setup
        # File is episode 1, OnDeck is at episode 1 → keep it
        ondeck_items = set()
        ff.ondeck_tracker.update_entry(
            "/mnt/user/TV/Anime/Arc Name/ep02.mkv", "user1",
            episode_info={"show": "Anime", "season": 1, "episode": 2},
            is_current_ondeck=True
        )
        # Simulate that the ondeck item is in our ondeck set
        ondeck_items.add("/mnt/user/TV/Anime/Arc Name/ep02.mkv")

        files_back, stale, exclude = ff.get_files_to_move_back_to_array(
            ondeck_items, set()
        )
        # Episode 1 is before OnDeck (ep 2), so it should be moved back
        assert len(files_back) > 0


# ============================================================================
# CachePriorityManager._is_tv_episode with persistent fallback
# ============================================================================

class TestIsTvEpisodeWithFallback:
    """Test _is_tv_episode uses CacheTimestampTracker as fallback."""

    @pytest.fixture
    def manager(self, tmp_path):
        ts_tracker = CacheTimestampTracker(str(tmp_path / "ts.json"))
        wl_tracker = WatchlistTracker(str(tmp_path / "wl.json"))
        od_tracker = OnDeckTracker(str(tmp_path / "od.json"))
        return CachePriorityManager(ts_tracker, wl_tracker, od_tracker)

    def test_finds_episode_via_ondeck(self, manager):
        """OnDeck tracker identifies episodes."""
        manager.ondeck_tracker.update_entry(
            "/mnt/cache/TV/S01E01.mkv", "user1",
            episode_info={"show": "Show", "season": 1, "episode": 1}
        )
        assert manager._is_tv_episode("/mnt/cache/TV/S01E01.mkv") is True

    def test_finds_episode_via_timestamp_tracker(self, manager):
        """CacheTimestampTracker identifies episodes when OnDeck is empty."""
        manager.timestamp_tracker.record_cache_time(
            "/mnt/cache/TV/S01E01.mkv",
            source="ondeck",
            media_type="episode",
            episode_info={"show": "Show", "season": 1, "episode": 1}
        )
        assert manager._is_tv_episode("/mnt/cache/TV/S01E01.mkv") is True

    def test_movie_via_timestamp_tracker(self, manager):
        """CacheTimestampTracker identifies movies."""
        manager.timestamp_tracker.record_cache_time(
            "/mnt/cache/Movies/Film.mkv",
            source="watchlist",
            media_type="movie"
        )
        assert manager._is_tv_episode("/mnt/cache/Movies/Film.mkv") is False

    def test_unknown_returns_false(self, manager):
        """No metadata anywhere → False."""
        assert manager._is_tv_episode("/mnt/cache/Unknown.mkv") is False


# ============================================================================
# CachePriorityManager._get_episodes_ahead_of_ondeck with fallback
# ============================================================================

class TestGetEpisodesAheadWithFallback:
    """Test _get_episodes_ahead_of_ondeck uses timestamp tracker as fallback."""

    @pytest.fixture
    def manager(self, tmp_path):
        ts_tracker = CacheTimestampTracker(str(tmp_path / "ts.json"))
        wl_tracker = WatchlistTracker(str(tmp_path / "wl.json"))
        od_tracker = OnDeckTracker(str(tmp_path / "od.json"))
        return CachePriorityManager(ts_tracker, wl_tracker, od_tracker)

    def test_uses_timestamp_tracker_episode_info(self, manager):
        """Falls back to timestamp tracker for episode info."""
        # OnDeck tracker has the current ondeck position
        manager.ondeck_tracker.update_entry(
            "/mnt/cache/TV/S01E03.mkv", "user1",
            episode_info={"show": "Show", "season": 1, "episode": 3},
            is_current_ondeck=True
        )
        # Timestamp tracker has info for a different episode (not in OnDeck)
        manager.timestamp_tracker.record_cache_time(
            "/mnt/cache/TV/S01E05.mkv",
            source="watchlist",
            media_type="episode",
            episode_info={"show": "Show", "season": 1, "episode": 5}
        )
        # S01E05 is 2 episodes ahead of OnDeck S01E03
        result = manager._get_episodes_ahead_of_ondeck("/mnt/cache/TV/S01E05.mkv")
        assert result == 2

    def test_returns_minus_one_for_no_info(self, manager):
        """No episode info anywhere → -1."""
        result = manager._get_episodes_ahead_of_ondeck("/mnt/cache/Unknown.mkv")
        assert result == -1


# ============================================================================
# Watchlist 4-tuple integration test
# ============================================================================

class TestWatchlist4Tuple:
    """Test that watchlist functions yield 4-tuples with episode_info."""

    @pytest.fixture(autouse=True)
    def _mock_plexapi(self):
        """Mock plexapi modules so plex_api can be imported without plexapi installed."""
        mocks = {}
        plexapi_mods = [
            'plexapi', 'plexapi.server', 'plexapi.myplex',
            'plexapi.video', 'plexapi.library', 'plexapi.exceptions',
        ]
        for mod in plexapi_mods:
            if mod not in sys.modules:
                mocks[mod] = MagicMock()
                sys.modules[mod] = mocks[mod]
        # Also mock requests if not available
        if 'requests' not in sys.modules:
            mocks['requests'] = MagicMock()
            sys.modules['requests'] = mocks['requests']
        yield
        for mod in mocks:
            sys.modules.pop(mod, None)

    def test_process_watchlist_show_yields_episode_info(self):
        """_process_watchlist_show yields 4-tuple with episode_info for TV."""
        from core.plex_api import PlexManager

        # Create a mock episode
        mock_episode = MagicMock()
        mock_episode.isPlayed = False
        mock_episode.parentIndex = 2
        mock_episode.index = 7
        mock_part = MagicMock()
        mock_part.file = "/data/TV/Show/Season 02/S02E07.mkv"
        mock_media = MagicMock()
        mock_media.parts = [mock_part]
        mock_episode.media = [mock_media]

        # Create a mock show
        mock_show = MagicMock()
        mock_show.title = "Foundation"
        mock_show.episodes.return_value = [mock_episode]

        api = PlexManager.__new__(PlexManager)
        results = list(api._process_watchlist_show(mock_show, 5, "user1", None))

        assert len(results) == 1
        file_path, username, watchlisted_at, episode_info = results[0]
        assert file_path == "/data/TV/Show/Season 02/S02E07.mkv"
        assert username == "user1"
        assert episode_info is not None
        assert episode_info["show"] == "Foundation"
        assert episode_info["season"] == 2
        assert episode_info["episode"] == 7

    def test_process_watchlist_movie_yields_none_episode_info(self):
        """_process_watchlist_movie yields 4-tuple with None episode_info."""
        from core.plex_api import PlexManager

        mock_part = MagicMock()
        mock_part.file = "/data/Movies/Inception.mkv"
        mock_media = MagicMock()
        mock_media.parts = [mock_part]
        mock_movie = MagicMock()
        mock_movie.media = [mock_media]

        api = PlexManager.__new__(PlexManager)
        results = list(api._process_watchlist_movie(mock_movie, "user1", None))

        assert len(results) == 1
        file_path, username, watchlisted_at, episode_info = results[0]
        assert file_path == "/data/Movies/Inception.mkv"
        assert episode_info is None

    def test_process_watchlist_show_missing_indices(self):
        """Episodes with missing parentIndex/index yield None episode_info."""
        from core.plex_api import PlexManager

        mock_episode = MagicMock()
        mock_episode.isPlayed = False
        mock_episode.parentIndex = None
        mock_episode.index = None
        mock_part = MagicMock()
        mock_part.file = "/data/TV/Show/ep.mkv"
        mock_media = MagicMock()
        mock_media.parts = [mock_part]
        mock_episode.media = [mock_media]

        mock_show = MagicMock()
        mock_show.title = "Show"
        mock_show.episodes.return_value = [mock_episode]

        api = PlexManager.__new__(PlexManager)
        results = list(api._process_watchlist_show(mock_show, 5, "user1", None))

        assert len(results) == 1
        _, _, _, episode_info = results[0]
        assert episode_info is None
