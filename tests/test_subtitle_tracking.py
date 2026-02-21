"""Tests for subtitle tracking as media file attribute (#14).

Tests the CacheTimestampTracker subtitle association, SubtitleFinder grouped output,
FileFilter subtitle delegation, and CachePriorityManager subtitle delegation.
"""

import os
import json
import tempfile
import shutil
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from core.file_operations import (
    CacheTimestampTracker,
    SubtitleFinder,
    FileFilter,
    CachePriorityManager,
    OnDeckTracker,
    WatchlistTracker,
    is_subtitle_file,
)
from conftest import create_test_file


@pytest.fixture
def temp_dir():
    d = tempfile.mkdtemp(prefix="plexcache_subtest_")
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def tracker(temp_dir):
    """Provide a CacheTimestampTracker with a temp file."""
    ts_file = os.path.join(temp_dir, "timestamps.json")
    return CacheTimestampTracker(ts_file)


@pytest.fixture
def tracker_with_data(temp_dir):
    """Provide a tracker pre-populated with a video and subtitle entries."""
    ts_file = os.path.join(temp_dir, "timestamps.json")
    data = {
        "/mnt/cache/media/Movies/Movie.mkv": {
            "cached_at": datetime.now().isoformat(),
            "source": "ondeck",
            "media_type": "movie",
        },
        "/mnt/cache/media/Movies/Movie.en.srt": {
            "cached_at": datetime.now().isoformat(),
            "source": "ondeck",
        },
        "/mnt/cache/media/Movies/Movie.es.srt": {
            "cached_at": datetime.now().isoformat(),
            "source": "ondeck",
        },
    }
    with open(ts_file, 'w') as f:
        json.dump(data, f)
    return CacheTimestampTracker(ts_file)


# ============================================================================
# CacheTimestampTracker association tests
# ============================================================================

class TestAssociateSubtitles:
    def test_associate_subtitles_links_to_parent(self, tracker):
        """Bulk association works — subtitles linked to parent, reverse index built."""
        tracker.record_cache_time("/mnt/cache/Movies/Movie.mkv", source="ondeck", media_type="movie")
        tracker.record_cache_time("/mnt/cache/Movies/Movie.en.srt", source="ondeck")
        tracker.record_cache_time("/mnt/cache/Movies/Movie.es.srt", source="ondeck")

        subtitle_map = {
            "/mnt/cache/Movies/Movie.mkv": [
                "/mnt/cache/Movies/Movie.en.srt",
                "/mnt/cache/Movies/Movie.es.srt",
            ]
        }
        tracker.associate_subtitles(subtitle_map)

        # Verify association
        subs = tracker.get_subtitles("/mnt/cache/Movies/Movie.mkv")
        assert "/mnt/cache/Movies/Movie.en.srt" in subs
        assert "/mnt/cache/Movies/Movie.es.srt" in subs
        assert len(subs) == 2

    def test_associate_subtitles_removes_standalone_entries(self, tracker):
        """Standalone subtitle entries removed from top-level timestamps after association."""
        tracker.record_cache_time("/mnt/cache/Movies/Movie.mkv", source="ondeck", media_type="movie")
        tracker.record_cache_time("/mnt/cache/Movies/Movie.en.srt", source="ondeck")

        subtitle_map = {"/mnt/cache/Movies/Movie.mkv": ["/mnt/cache/Movies/Movie.en.srt"]}
        tracker.associate_subtitles(subtitle_map)

        # Subtitle should not be a top-level entry anymore
        assert "/mnt/cache/Movies/Movie.en.srt" not in tracker._timestamps

    def test_associate_subtitles_handles_missing_parent(self, tracker):
        """Subtitles for untracked parent videos left as standalone."""
        tracker.record_cache_time("/mnt/cache/Movies/Movie.en.srt", source="ondeck")

        subtitle_map = {
            "/mnt/cache/Movies/Nonexistent.mkv": ["/mnt/cache/Movies/Movie.en.srt"]
        }
        tracker.associate_subtitles(subtitle_map)

        # Subtitle stays standalone — parent doesn't exist in tracker
        assert "/mnt/cache/Movies/Movie.en.srt" in tracker._timestamps


class TestSubtitleInheritance:
    def test_subtitle_inherits_media_type(self, tracker):
        """get_media_type() for subtitle returns parent's type."""
        tracker.record_cache_time("/mnt/cache/Movies/Movie.mkv", source="ondeck", media_type="movie")
        tracker.associate_subtitles({
            "/mnt/cache/Movies/Movie.mkv": ["/mnt/cache/Movies/Movie.en.srt"]
        })

        assert tracker.get_media_type("/mnt/cache/Movies/Movie.en.srt") == "movie"

    def test_subtitle_inherits_episode_info(self, tracker):
        """get_episode_info() for subtitle returns parent's info."""
        ep_info = {"show": "Breaking Bad", "season": 1, "episode": 1}
        tracker.record_cache_time(
            "/mnt/cache/TV/BB/S01E01.mkv",
            source="ondeck", media_type="episode", episode_info=ep_info
        )
        tracker.associate_subtitles({
            "/mnt/cache/TV/BB/S01E01.mkv": ["/mnt/cache/TV/BB/S01E01.en.srt"]
        })

        result = tracker.get_episode_info("/mnt/cache/TV/BB/S01E01.en.srt")
        assert result is not None
        assert result["show"] == "Breaking Bad"
        assert result["season"] == 1

    def test_subtitle_inherits_source(self, tracker):
        """get_source() for subtitle returns parent's source."""
        tracker.record_cache_time("/mnt/cache/Movies/Movie.mkv", source="watchlist")
        tracker.associate_subtitles({
            "/mnt/cache/Movies/Movie.mkv": ["/mnt/cache/Movies/Movie.en.srt"]
        })

        assert tracker.get_source("/mnt/cache/Movies/Movie.en.srt") == "watchlist"

    def test_subtitle_inherits_retention(self, tracker):
        """is_within_retention_period() delegates to parent."""
        # Record with a recent timestamp (now)
        tracker.record_cache_time("/mnt/cache/Movies/Movie.mkv", source="ondeck")
        tracker.associate_subtitles({
            "/mnt/cache/Movies/Movie.mkv": ["/mnt/cache/Movies/Movie.en.srt"]
        })

        # Parent was cached just now, so 24h retention should be active
        assert tracker.is_within_retention_period("/mnt/cache/Movies/Movie.en.srt", 24) is True

    def test_subtitle_retention_remaining_delegates(self, tracker):
        """get_retention_remaining() for subtitle delegates to parent."""
        tracker.record_cache_time("/mnt/cache/Movies/Movie.mkv", source="ondeck")
        tracker.associate_subtitles({
            "/mnt/cache/Movies/Movie.mkv": ["/mnt/cache/Movies/Movie.en.srt"]
        })

        remaining = tracker.get_retention_remaining("/mnt/cache/Movies/Movie.en.srt", 24)
        assert remaining > 23  # Just cached, should have ~24h remaining


class TestRemoveEntry:
    def test_remove_parent_cleans_reverse_index(self, tracker):
        """Removing parent clears _subtitle_to_parent for its subtitles."""
        tracker.record_cache_time("/mnt/cache/Movies/Movie.mkv", source="ondeck")
        tracker.associate_subtitles({
            "/mnt/cache/Movies/Movie.mkv": [
                "/mnt/cache/Movies/Movie.en.srt",
                "/mnt/cache/Movies/Movie.es.srt",
            ]
        })

        tracker.remove_entry("/mnt/cache/Movies/Movie.mkv")

        assert tracker.find_parent_video("/mnt/cache/Movies/Movie.en.srt") is None
        assert tracker.find_parent_video("/mnt/cache/Movies/Movie.es.srt") is None

    def test_remove_subtitle_cleans_parent_list(self, tracker):
        """Removing a subtitle updates the parent's subtitles list."""
        tracker.record_cache_time("/mnt/cache/Movies/Movie.mkv", source="ondeck")
        tracker.associate_subtitles({
            "/mnt/cache/Movies/Movie.mkv": [
                "/mnt/cache/Movies/Movie.en.srt",
                "/mnt/cache/Movies/Movie.es.srt",
            ]
        })

        tracker.remove_entry("/mnt/cache/Movies/Movie.en.srt")

        subs = tracker.get_subtitles("/mnt/cache/Movies/Movie.mkv")
        assert "/mnt/cache/Movies/Movie.en.srt" not in subs
        assert "/mnt/cache/Movies/Movie.es.srt" in subs
        assert tracker.find_parent_video("/mnt/cache/Movies/Movie.en.srt") is None


class TestGetAndFind:
    def test_get_subtitles_returns_list(self, tracker):
        """get_subtitles() returns the correct list."""
        tracker.record_cache_time("/mnt/cache/Movies/Movie.mkv", source="ondeck")
        tracker.associate_subtitles({
            "/mnt/cache/Movies/Movie.mkv": ["/mnt/cache/Movies/Movie.en.srt"]
        })

        subs = tracker.get_subtitles("/mnt/cache/Movies/Movie.mkv")
        assert subs == ["/mnt/cache/Movies/Movie.en.srt"]

    def test_get_subtitles_no_subs(self, tracker):
        """get_subtitles() returns empty list for videos without subtitles."""
        tracker.record_cache_time("/mnt/cache/Movies/Movie.mkv", source="ondeck")
        assert tracker.get_subtitles("/mnt/cache/Movies/Movie.mkv") == []

    def test_find_parent_video_returns_correct_parent(self, tracker):
        """Reverse lookup works correctly."""
        tracker.record_cache_time("/mnt/cache/Movies/Movie.mkv", source="ondeck")
        tracker.associate_subtitles({
            "/mnt/cache/Movies/Movie.mkv": ["/mnt/cache/Movies/Movie.en.srt"]
        })

        assert tracker.find_parent_video("/mnt/cache/Movies/Movie.en.srt") == "/mnt/cache/Movies/Movie.mkv"

    def test_find_parent_video_unknown_subtitle(self, tracker):
        """find_parent_video() returns None for unknown paths."""
        assert tracker.find_parent_video("/mnt/cache/Movies/Unknown.srt") is None


class TestCleanupMissingFiles:
    def test_cleanup_removes_missing_subtitles(self, temp_dir):
        """cleanup_missing_files() prunes non-existent subs from parent lists."""
        # Create a real video file
        video_path = os.path.join(temp_dir, "Movie.mkv")
        create_test_file(video_path)

        # Create one real subtitle and one that doesn't exist
        sub_real = os.path.join(temp_dir, "Movie.en.srt")
        create_test_file(sub_real)
        sub_missing = os.path.join(temp_dir, "Movie.es.srt")
        # Don't create sub_missing on disk

        ts_file = os.path.join(temp_dir, "timestamps.json")
        tracker = CacheTimestampTracker(ts_file)
        tracker.record_cache_time(video_path, source="ondeck")
        tracker.associate_subtitles({
            video_path: [sub_real, sub_missing]
        })

        removed = tracker.cleanup_missing_files()
        assert removed == 1  # sub_missing was pruned
        subs = tracker.get_subtitles(video_path)
        assert sub_real in subs
        assert sub_missing not in subs
        assert tracker.find_parent_video(sub_missing) is None


class TestBackwardCompat:
    def test_backward_compat_no_subtitles_key(self, temp_dir):
        """Old entries without 'subtitles' key work fine."""
        ts_file = os.path.join(temp_dir, "timestamps.json")
        data = {
            "/mnt/cache/Movies/Movie.mkv": {
                "cached_at": "2026-01-15T10:00:00",
                "source": "ondeck",
                "media_type": "movie",
            }
        }
        with open(ts_file, 'w') as f:
            json.dump(data, f)

        tracker = CacheTimestampTracker(ts_file)
        assert tracker.get_subtitles("/mnt/cache/Movies/Movie.mkv") == []
        assert tracker.get_media_type("/mnt/cache/Movies/Movie.mkv") == "movie"


class TestMigration:
    def test_migration_on_load(self, temp_dir):
        """Standalone subtitle entries migrated to parent on load."""
        video_path = os.path.join(temp_dir, "Movie.mkv")
        sub_path = os.path.join(temp_dir, "Movie.en.srt")
        create_test_file(video_path)
        create_test_file(sub_path)

        ts_file = os.path.join(temp_dir, "timestamps.json")
        data = {
            video_path: {
                "cached_at": "2026-01-15T10:00:00",
                "source": "ondeck",
                "media_type": "movie",
            },
            sub_path: {
                "cached_at": "2026-01-15T10:00:00",
                "source": "ondeck",
            },
        }
        with open(ts_file, 'w') as f:
            json.dump(data, f)

        tracker = CacheTimestampTracker(ts_file)

        # Subtitle should have been migrated to parent
        assert sub_path not in tracker._timestamps
        assert sub_path in tracker.get_subtitles(video_path)
        assert tracker.find_parent_video(sub_path) == video_path

    def test_migration_orphan_left_standalone(self, temp_dir):
        """Standalone subtitle without matching parent stays standalone."""
        sub_path = os.path.join(temp_dir, "Orphan.en.srt")
        create_test_file(sub_path)

        ts_file = os.path.join(temp_dir, "timestamps.json")
        data = {
            sub_path: {
                "cached_at": "2026-01-15T10:00:00",
                "source": "ondeck",
            },
        }
        with open(ts_file, 'w') as f:
            json.dump(data, f)

        tracker = CacheTimestampTracker(ts_file)
        # No parent video on disk, so subtitle stays standalone
        assert sub_path in tracker._timestamps


class TestPersistence:
    def test_persistence_across_reload(self, temp_dir):
        """Associations survive save/load cycle."""
        ts_file = os.path.join(temp_dir, "timestamps.json")

        tracker1 = CacheTimestampTracker(ts_file)
        tracker1.record_cache_time("/mnt/cache/Movies/Movie.mkv", source="ondeck", media_type="movie")
        tracker1.associate_subtitles({
            "/mnt/cache/Movies/Movie.mkv": ["/mnt/cache/Movies/Movie.en.srt"]
        })

        # Load fresh from disk
        tracker2 = CacheTimestampTracker(ts_file)
        subs = tracker2.get_subtitles("/mnt/cache/Movies/Movie.mkv")
        assert "/mnt/cache/Movies/Movie.en.srt" in subs
        assert tracker2.find_parent_video("/mnt/cache/Movies/Movie.en.srt") == "/mnt/cache/Movies/Movie.mkv"
        assert tracker2.get_media_type("/mnt/cache/Movies/Movie.en.srt") == "movie"


# ============================================================================
# SubtitleFinder tests
# ============================================================================

class TestSubtitleFinderGrouped:
    def test_get_media_subtitles_grouped_returns_mapping(self, temp_dir):
        """Returns {video: [subs]} mapping."""
        video_dir = os.path.join(temp_dir, "Movies")
        video = os.path.join(video_dir, "Movie.mkv")
        sub1 = os.path.join(video_dir, "Movie.en.srt")
        sub2 = os.path.join(video_dir, "Movie.es.vtt")
        create_test_file(video)
        create_test_file(sub1)
        create_test_file(sub2)

        finder = SubtitleFinder()
        result = finder.get_media_subtitles_grouped([video])

        assert video in result
        assert sub1 in result[video]
        assert sub2 in result[video]

    def test_get_media_subtitles_grouped_no_subtitles(self, temp_dir):
        """Empty list for videos without subtitles."""
        video_dir = os.path.join(temp_dir, "Movies")
        video = os.path.join(video_dir, "Movie.mkv")
        create_test_file(video)

        finder = SubtitleFinder()
        result = finder.get_media_subtitles_grouped([video])

        assert video in result
        assert result[video] == []

    def test_get_media_subtitles_backward_compat(self, temp_dir):
        """Flat list method still works after refactor."""
        video_dir = os.path.join(temp_dir, "Movies")
        video = os.path.join(video_dir, "Movie.mkv")
        sub = os.path.join(video_dir, "Movie.en.srt")
        create_test_file(video)
        create_test_file(sub)

        finder = SubtitleFinder()
        result = finder.get_media_subtitles([video])

        assert video in result
        assert sub in result


# ============================================================================
# FileFilter + CachePriorityManager tests
# ============================================================================

class TestFileFilterSubtitleDelegation:
    def test_lookup_media_info_delegates_for_subtitle(self, temp_dir):
        """Subtitle gets parent's metadata via _lookup_media_info."""
        ts_file = os.path.join(temp_dir, "timestamps.json")
        tracker = CacheTimestampTracker(ts_file)
        tracker.record_cache_time("/mnt/cache/Movies/Movie.mkv", source="ondeck", media_type="movie")
        tracker.associate_subtitles({
            "/mnt/cache/Movies/Movie.mkv": ["/mnt/cache/Movies/Movie.en.srt"]
        })

        ondeck_file = os.path.join(temp_dir, "ondeck.json")
        watchlist_file = os.path.join(temp_dir, "watchlist.json")
        ondeck_tracker = OnDeckTracker(ondeck_file)
        watchlist_tracker = WatchlistTracker(watchlist_file)

        file_filter = FileFilter(
            real_source="/mnt/user/media",
            cache_dir="/mnt/cache/media",
            is_unraid=False,
            mover_cache_exclude_file="",
            timestamp_tracker=tracker,
            ondeck_tracker=ondeck_tracker,
            watchlist_tracker=watchlist_tracker,
        )

        result = file_filter._lookup_media_info("/mnt/cache/Movies/Movie.en.srt")
        assert result is not None
        assert result[0] == "movie"


class TestCachePriorityManagerSubtitleDelegation:
    def test_subtitle_priority_equals_parent(self, temp_dir):
        """Subtitle gets same priority score as parent."""
        ts_file = os.path.join(temp_dir, "timestamps.json")
        tracker = CacheTimestampTracker(ts_file)
        tracker.record_cache_time("/mnt/cache/Movies/Movie.mkv", source="ondeck", media_type="movie")
        tracker.associate_subtitles({
            "/mnt/cache/Movies/Movie.mkv": ["/mnt/cache/Movies/Movie.en.srt"]
        })

        ondeck_file = os.path.join(temp_dir, "ondeck.json")
        watchlist_file = os.path.join(temp_dir, "watchlist.json")
        ondeck_tracker = OnDeckTracker(ondeck_file)
        watchlist_tracker = WatchlistTracker(watchlist_file)

        priority_mgr = CachePriorityManager(
            timestamp_tracker=tracker,
            watchlist_tracker=watchlist_tracker,
            ondeck_tracker=ondeck_tracker,
        )

        parent_score = priority_mgr.calculate_priority("/mnt/cache/Movies/Movie.mkv")
        sub_score = priority_mgr.calculate_priority("/mnt/cache/Movies/Movie.en.srt")
        assert sub_score == parent_score
        assert sub_score > 0  # Sanity check
