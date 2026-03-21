"""Tests for extension-free caching (#6).

Tests SiblingFileFinder sibling discovery, CacheTimestampTracker generalization
(associate_files, migration, reference counting), priority delegation for
non-video files, and .plexcached three-way category matching.
"""

import os
import json
import tempfile
import shutil
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from core.file_operations import (
    SiblingFileFinder,
    SubtitleFinder,
    CacheTimestampTracker,
    CachePriorityManager,
    OnDeckTracker,
    WatchlistTracker,
    is_video_file,
    is_directory_level_file,
    is_season_like_folder,
    _get_file_category,
    find_matching_plexcached,
    PLEXCACHED_EXTENSION,
)
from conftest import create_test_file


@pytest.fixture
def temp_dir():
    d = tempfile.mkdtemp(prefix="plexcache_efctest_")
    yield d
    shutil.rmtree(d, ignore_errors=True)


# ============================================================
# is_video_file tests
# ============================================================

class TestIsVideoFile:
    def test_video_extensions(self):
        assert is_video_file("movie.mkv") is True
        assert is_video_file("movie.mp4") is True
        assert is_video_file("movie.avi") is True

    def test_non_video_extensions(self):
        assert is_video_file("movie.srt") is False
        assert is_video_file("poster.jpg") is False
        assert is_video_file("movie.nfo") is False

    def test_case_insensitive(self):
        assert is_video_file("movie.MKV") is True
        assert is_video_file("movie.Mp4") is True


# ============================================================
# is_directory_level_file tests
# ============================================================

class TestIsDirectoryLevelFile:
    def test_name_prefixed_file(self):
        """Files starting with video's base name are NOT directory-level."""
        assert is_directory_level_file(
            "/media/Movie (2020).nfo",
            "/media/Movie (2020).mkv"
        ) is False

    def test_name_prefixed_subtitle(self):
        assert is_directory_level_file(
            "/media/Movie (2020).en.srt",
            "/media/Movie (2020).mkv"
        ) is False

    def test_directory_level_poster(self):
        """poster.jpg is not prefixed with the video name."""
        assert is_directory_level_file(
            "/media/poster.jpg",
            "/media/Movie (2020).mkv"
        ) is True

    def test_directory_level_fanart(self):
        assert is_directory_level_file(
            "/media/fanart.jpg",
            "/media/Movie (2020).mkv"
        ) is True


# ============================================================
# _get_file_category tests
# ============================================================

class TestGetFileCategory:
    def test_video(self):
        assert _get_file_category("movie.mkv") == "video"
        assert _get_file_category("movie.mp4") == "video"

    def test_subtitle(self):
        assert _get_file_category("movie.srt") == "subtitle"
        assert _get_file_category("movie.ass") == "subtitle"

    def test_sidecar(self):
        assert _get_file_category("poster.jpg") == "sidecar"
        assert _get_file_category("movie.nfo") == "sidecar"
        assert _get_file_category("fanart.png") == "sidecar"


# ============================================================
# SiblingFileFinder tests
# ============================================================

class TestSiblingFileFinder:
    def test_finds_subtitles(self, temp_dir):
        """Sibling finder discovers subtitle files."""
        video = create_test_file(os.path.join(temp_dir, "Movie.mkv"), "video")
        sub = create_test_file(os.path.join(temp_dir, "Movie.en.srt"), "sub")
        finder = SiblingFileFinder()
        result = finder.get_media_siblings_grouped([video])
        assert sub in result[video]

    def test_finds_artwork(self, temp_dir):
        """Sibling finder discovers artwork files."""
        video = create_test_file(os.path.join(temp_dir, "Movie.mkv"), "video")
        poster = create_test_file(os.path.join(temp_dir, "poster.jpg"), "img")
        fanart = create_test_file(os.path.join(temp_dir, "fanart.jpg"), "img")
        finder = SiblingFileFinder()
        result = finder.get_media_siblings_grouped([video])
        assert poster in result[video]
        assert fanart in result[video]

    def test_finds_nfo(self, temp_dir):
        """Sibling finder discovers NFO files."""
        video = create_test_file(os.path.join(temp_dir, "Movie.mkv"), "video")
        nfo = create_test_file(os.path.join(temp_dir, "Movie.nfo"), "nfo")
        finder = SiblingFileFinder()
        result = finder.get_media_siblings_grouped([video])
        assert nfo in result[video]

    def test_skips_other_videos(self, temp_dir):
        """Sibling finder does NOT include other video files."""
        video1 = create_test_file(os.path.join(temp_dir, "Movie1.mkv"), "video1")
        create_test_file(os.path.join(temp_dir, "Movie2.mkv"), "video2")
        finder = SiblingFileFinder()
        result = finder.get_media_siblings_grouped([video1])
        # Movie2.mkv should not be in Movie1's siblings
        siblings = result[video1]
        for s in siblings:
            assert not s.endswith(".mkv")

    def test_skips_hidden_files(self, temp_dir):
        """Sibling finder skips hidden files (dotfiles)."""
        video = create_test_file(os.path.join(temp_dir, "Movie.mkv"), "video")
        create_test_file(os.path.join(temp_dir, ".hidden"), "hidden")
        finder = SiblingFileFinder()
        result = finder.get_media_siblings_grouped([video])
        for s in result[video]:
            assert not os.path.basename(s).startswith(".")

    def test_skips_plexcached_files(self, temp_dir):
        """Sibling finder skips .plexcached backup files."""
        video = create_test_file(os.path.join(temp_dir, "Movie.mkv"), "video")
        create_test_file(os.path.join(temp_dir, "Movie.mkv.plexcached"), "backup")
        create_test_file(os.path.join(temp_dir, "OtherMovie.mkv.plexcached"), "backup2")
        create_test_file(os.path.join(temp_dir, "poster.jpg"), "img")
        finder = SiblingFileFinder()
        result = finder.get_media_siblings_grouped([video])
        names = [os.path.basename(f) for f in result[video]]
        assert "poster.jpg" in names
        assert not any(n.endswith(".plexcached") for n in names)

    def test_backward_compat_alias(self):
        """SubtitleFinder is an alias for SiblingFileFinder."""
        assert SubtitleFinder is SiblingFileFinder

    def test_get_media_subtitles_grouped_filters_subtitles_only(self, temp_dir):
        """Backward-compat method only returns subtitle files."""
        video = create_test_file(os.path.join(temp_dir, "Movie.mkv"), "video")
        sub = create_test_file(os.path.join(temp_dir, "Movie.en.srt"), "sub")
        create_test_file(os.path.join(temp_dir, "poster.jpg"), "img")
        finder = SiblingFileFinder()
        result = finder.get_media_subtitles_grouped([video])
        assert sub in result[video]
        # poster.jpg should NOT be in subtitle-only results
        for s in result[video]:
            assert not s.endswith(".jpg")

    def test_empty_directory(self, temp_dir):
        """Video with no siblings returns empty list."""
        video = create_test_file(os.path.join(temp_dir, "Movie.mkv"), "video")
        finder = SiblingFileFinder()
        result = finder.get_media_siblings_grouped([video])
        assert result[video] == []

    def test_multi_version_name_prefix_disambiguation(self, temp_dir):
        """When two videos share a directory, siblings are assigned by name prefix."""
        # Simulate 4K + 1080p versions with name-prefixed artwork
        video_1080 = create_test_file(os.path.join(temp_dir, "Movie - [1080P]-FGT.mkv"), "v1")
        video_4k = create_test_file(os.path.join(temp_dir, "Movie - [2160P]-REMUX.mkv"), "v2")
        fanart = create_test_file(os.path.join(temp_dir, "Movie - [1080P]-FGT-fanart.jpg"), "img")
        poster = create_test_file(os.path.join(temp_dir, "Movie - [1080P]-FGT-poster.jpg"), "img")
        nfo = create_test_file(os.path.join(temp_dir, "Movie - [1080P]-FGT.nfo"), "nfo")

        finder = SiblingFileFinder()
        result = finder.get_media_siblings_grouped([video_1080, video_4k])

        # Artwork belongs to 1080p (name prefix match), not 4K
        assert fanart in result[video_1080]
        assert poster in result[video_1080]
        assert nfo in result[video_1080]
        assert result[video_4k] == []

    def test_multi_version_generic_siblings_assigned_to_first(self, temp_dir):
        """Generic siblings (no name prefix match) go to the first video."""
        video_1080 = create_test_file(os.path.join(temp_dir, "Movie.1080p.mkv"), "v1")
        video_4k = create_test_file(os.path.join(temp_dir, "Movie.2160p.mkv"), "v2")
        # Generic poster doesn't match either video's stem
        poster = create_test_file(os.path.join(temp_dir, "poster.jpg"), "img")

        finder = SiblingFileFinder()
        result = finder.get_media_siblings_grouped([video_1080, video_4k])

        assert poster in result[video_1080]
        assert poster not in result[video_4k]

    def test_multi_version_each_has_own_siblings(self, temp_dir):
        """Each version can have its own name-prefixed siblings."""
        video_1080 = create_test_file(os.path.join(temp_dir, "Movie.1080p.mkv"), "v1")
        video_4k = create_test_file(os.path.join(temp_dir, "Movie.2160p.mkv"), "v2")
        sub_1080 = create_test_file(os.path.join(temp_dir, "Movie.1080p.en.srt"), "sub1")
        sub_4k = create_test_file(os.path.join(temp_dir, "Movie.2160p.en.srt"), "sub2")

        finder = SiblingFileFinder()
        result = finder.get_media_siblings_grouped([video_1080, video_4k])

        assert sub_1080 in result[video_1080]
        assert sub_4k in result[video_4k]
        assert sub_4k not in result[video_1080]
        assert sub_1080 not in result[video_4k]


# ============================================================
# CacheTimestampTracker migration tests
# ============================================================

class TestTrackerMigration:
    def test_subtitles_key_migrated_to_associated_files(self, temp_dir):
        """Old 'subtitles' key is migrated to 'associated_files' on load."""
        ts_file = os.path.join(temp_dir, "timestamps.json")
        data = {
            "/cache/Movie.mkv": {
                "cached_at": "2025-12-01T10:00:00",
                "source": "ondeck",
                "subtitles": ["/cache/Movie.en.srt"]
            }
        }
        with open(ts_file, 'w') as f:
            json.dump(data, f, indent=2)

        tracker = CacheTimestampTracker(ts_file)

        # Should have migrated
        entry = tracker.get_entry("/cache/Movie.mkv")
        assert "associated_files" in entry
        assert "subtitles" not in entry
        assert "/cache/Movie.en.srt" in entry["associated_files"]

    def test_reverse_index_built_after_migration(self, temp_dir):
        """Reverse index works after subtitles→associated_files migration."""
        ts_file = os.path.join(temp_dir, "timestamps.json")
        data = {
            "/cache/Movie.mkv": {
                "cached_at": "2025-12-01T10:00:00",
                "source": "ondeck",
                "subtitles": ["/cache/Movie.en.srt"]
            }
        }
        with open(ts_file, 'w') as f:
            json.dump(data, f, indent=2)

        tracker = CacheTimestampTracker(ts_file)
        assert tracker.find_parent_video("/cache/Movie.en.srt") == "/cache/Movie.mkv"


# ============================================================
# CacheTimestampTracker associate_files tests
# ============================================================

class TestAssociateFiles:
    def test_associate_mixed_file_types(self, temp_dir):
        """associate_files handles subtitles, artwork, and NFOs."""
        ts_file = os.path.join(temp_dir, "timestamps.json")
        data = {
            "/cache/Movie.mkv": {
                "cached_at": "2025-12-01T10:00:00",
                "source": "ondeck"
            }
        }
        with open(ts_file, 'w') as f:
            json.dump(data, f, indent=2)

        tracker = CacheTimestampTracker(ts_file)
        tracker.associate_files({
            "/cache/Movie.mkv": [
                "/cache/Movie.en.srt",
                "/cache/poster.jpg",
                "/cache/Movie.nfo"
            ]
        })

        files = tracker.get_associated_files("/cache/Movie.mkv")
        assert "/cache/Movie.en.srt" in files
        assert "/cache/poster.jpg" in files
        assert "/cache/Movie.nfo" in files

    def test_backward_compat_associate_subtitles(self, temp_dir):
        """associate_subtitles still works (alias)."""
        ts_file = os.path.join(temp_dir, "timestamps.json")
        data = {
            "/cache/Movie.mkv": {
                "cached_at": "2025-12-01T10:00:00",
                "source": "ondeck"
            }
        }
        with open(ts_file, 'w') as f:
            json.dump(data, f, indent=2)

        tracker = CacheTimestampTracker(ts_file)
        tracker.associate_subtitles({
            "/cache/Movie.mkv": ["/cache/Movie.en.srt"]
        })
        assert "/cache/Movie.en.srt" in tracker.get_associated_files("/cache/Movie.mkv")

    def test_backward_compat_get_subtitles(self, temp_dir):
        """get_subtitles returns associated_files (alias)."""
        ts_file = os.path.join(temp_dir, "timestamps.json")
        data = {
            "/cache/Movie.mkv": {
                "cached_at": "2025-12-01T10:00:00",
                "source": "ondeck",
                "associated_files": ["/cache/Movie.en.srt", "/cache/poster.jpg"]
            }
        }
        with open(ts_file, 'w') as f:
            json.dump(data, f, indent=2)

        tracker = CacheTimestampTracker(ts_file)
        subs = tracker.get_subtitles("/cache/Movie.mkv")
        assert "/cache/Movie.en.srt" in subs
        assert "/cache/poster.jpg" in subs


# ============================================================
# Reference counting tests
# ============================================================

class TestReferenceCount:
    def test_get_other_videos_in_directory(self, temp_dir):
        """get_other_videos_in_directory finds sibling videos."""
        ts_file = os.path.join(temp_dir, "timestamps.json")
        data = {
            "/cache/Show/Season 1/S01E01.mkv": {
                "cached_at": "2025-12-01T10:00:00",
                "source": "ondeck"
            },
            "/cache/Show/Season 1/S01E02.mkv": {
                "cached_at": "2025-12-01T10:00:00",
                "source": "ondeck"
            }
        }
        with open(ts_file, 'w') as f:
            json.dump(data, f, indent=2)

        tracker = CacheTimestampTracker(ts_file)
        others = tracker.get_other_videos_in_directory(
            "/cache/Show/Season 1",
            excluding="/cache/Show/Season 1/S01E01.mkv"
        )
        assert "/cache/Show/Season 1/S01E02.mkv" in others
        assert "/cache/Show/Season 1/S01E01.mkv" not in others

    def test_get_other_videos_empty_when_last(self, temp_dir):
        """No other videos when only one exists."""
        ts_file = os.path.join(temp_dir, "timestamps.json")
        data = {
            "/cache/Movie/Movie.mkv": {
                "cached_at": "2025-12-01T10:00:00",
                "source": "ondeck"
            }
        }
        with open(ts_file, 'w') as f:
            json.dump(data, f, indent=2)

        tracker = CacheTimestampTracker(ts_file)
        others = tracker.get_other_videos_in_directory(
            "/cache/Movie",
            excluding="/cache/Movie/Movie.mkv"
        )
        assert others == []

    def test_reassociate_file(self, temp_dir):
        """reassociate_file moves a file from one parent to another."""
        ts_file = os.path.join(temp_dir, "timestamps.json")
        data = {
            "/cache/Show/Season 1/S01E01.mkv": {
                "cached_at": "2025-12-01T10:00:00",
                "source": "ondeck",
                "associated_files": ["/cache/Show/Season 1/poster.jpg"]
            },
            "/cache/Show/Season 1/S01E02.mkv": {
                "cached_at": "2025-12-01T10:00:00",
                "source": "ondeck"
            }
        }
        with open(ts_file, 'w') as f:
            json.dump(data, f, indent=2)

        tracker = CacheTimestampTracker(ts_file)
        tracker.reassociate_file(
            "/cache/Show/Season 1/poster.jpg",
            from_parent="/cache/Show/Season 1/S01E01.mkv",
            to_parent="/cache/Show/Season 1/S01E02.mkv"
        )

        # poster.jpg should be moved
        assert "/cache/Show/Season 1/poster.jpg" not in tracker.get_associated_files("/cache/Show/Season 1/S01E01.mkv")
        assert "/cache/Show/Season 1/poster.jpg" in tracker.get_associated_files("/cache/Show/Season 1/S01E02.mkv")
        # Reverse index should be updated
        assert tracker.find_parent_video("/cache/Show/Season 1/poster.jpg") == "/cache/Show/Season 1/S01E02.mkv"


# ============================================================
# .plexcached three-way category matching
# ============================================================

class TestPlexcachedCategoryMatching:
    def test_video_matches_video(self, temp_dir):
        """Video .plexcached matches video source."""
        # Create array directory with .plexcached file
        array_dir = os.path.join(temp_dir, "array", "Movies", "Movie (2020)")
        os.makedirs(array_dir, exist_ok=True)
        create_test_file(
            os.path.join(array_dir, "Movie (2020) [WEBDL-1080p].mkv" + PLEXCACHED_EXTENSION),
            "backup"
        )
        result = find_matching_plexcached(
            array_dir,
            "Movie (2020)",
            "Movie (2020) [HEVC-1080p].mkv"
        )
        assert result is not None

    def test_sidecar_does_not_match_video(self, temp_dir):
        """A sidecar .plexcached should NOT match a video source."""
        array_dir = os.path.join(temp_dir, "array", "Movies", "Movie (2020)")
        os.makedirs(array_dir, exist_ok=True)
        create_test_file(
            os.path.join(array_dir, "Movie (2020).nfo" + PLEXCACHED_EXTENSION),
            "backup"
        )
        result = find_matching_plexcached(
            array_dir,
            "Movie (2020)",
            "Movie (2020) [HEVC-1080p].mkv"
        )
        assert result is None

    def test_sidecar_matches_sidecar(self, temp_dir):
        """A sidecar .plexcached matches a sidecar source."""
        array_dir = os.path.join(temp_dir, "array", "Movies", "Movie (2020)")
        os.makedirs(array_dir, exist_ok=True)
        create_test_file(
            os.path.join(array_dir, "Movie (2020).nfo" + PLEXCACHED_EXTENSION),
            "backup"
        )
        result = find_matching_plexcached(
            array_dir,
            "Movie (2020)",
            "Movie (2020).nfo"
        )
        assert result is not None


# ============================================================
# Priority delegation for non-video files
# ============================================================

class TestPriorityDelegation:
    def test_artwork_delegates_to_parent(self, temp_dir):
        """Non-video files (artwork) delegate priority to parent video."""
        ts_file = os.path.join(temp_dir, "timestamps.json")
        data = {
            "/cache/Movie.mkv": {
                "cached_at": datetime.now().isoformat(),
                "source": "ondeck",
                "associated_files": ["/cache/poster.jpg"]
            }
        }
        with open(ts_file, 'w') as f:
            json.dump(data, f, indent=2)

        tracker = CacheTimestampTracker(ts_file)
        ondeck = OnDeckTracker(os.path.join(temp_dir, "ondeck.json"))
        watchlist = WatchlistTracker(os.path.join(temp_dir, "watchlist.json"))

        priority_mgr = CachePriorityManager(tracker, watchlist, ondeck)

        video_priority = priority_mgr.calculate_priority("/cache/Movie.mkv")
        artwork_priority = priority_mgr.calculate_priority("/cache/poster.jpg")
        assert artwork_priority == video_priority

    def test_nfo_delegates_to_parent(self, temp_dir):
        """NFO files delegate priority to parent video."""
        ts_file = os.path.join(temp_dir, "timestamps.json")
        data = {
            "/cache/Movie.mkv": {
                "cached_at": datetime.now().isoformat(),
                "source": "ondeck",
                "associated_files": ["/cache/Movie.nfo"]
            }
        }
        with open(ts_file, 'w') as f:
            json.dump(data, f, indent=2)

        tracker = CacheTimestampTracker(ts_file)
        ondeck = OnDeckTracker(os.path.join(temp_dir, "ondeck.json"))
        watchlist = WatchlistTracker(os.path.join(temp_dir, "watchlist.json"))

        priority_mgr = CachePriorityManager(tracker, watchlist, ondeck)

        video_priority = priority_mgr.calculate_priority("/cache/Movie.mkv")
        nfo_priority = priority_mgr.calculate_priority("/cache/Movie.nfo")
        assert nfo_priority == video_priority


# ============================================================
# Tracker cleanup with associated_files
# ============================================================

class TestTrackerCleanup:
    def test_remove_parent_clears_associated_files(self, temp_dir):
        """Removing a parent video clears associated files from reverse index."""
        ts_file = os.path.join(temp_dir, "timestamps.json")
        data = {
            "/cache/Movie.mkv": {
                "cached_at": "2025-12-01T10:00:00",
                "source": "ondeck",
                "associated_files": ["/cache/Movie.en.srt", "/cache/poster.jpg"]
            }
        }
        with open(ts_file, 'w') as f:
            json.dump(data, f, indent=2)

        tracker = CacheTimestampTracker(ts_file)
        assert tracker.find_parent_video("/cache/Movie.en.srt") == "/cache/Movie.mkv"

        tracker.remove_entry("/cache/Movie.mkv")
        assert tracker.find_parent_video("/cache/Movie.en.srt") is None

    def test_remove_associated_file(self, temp_dir):
        """Removing an associated file removes it from parent's list."""
        ts_file = os.path.join(temp_dir, "timestamps.json")
        data = {
            "/cache/Movie.mkv": {
                "cached_at": "2025-12-01T10:00:00",
                "source": "ondeck",
                "associated_files": ["/cache/Movie.en.srt", "/cache/poster.jpg"]
            }
        }
        with open(ts_file, 'w') as f:
            json.dump(data, f, indent=2)

        tracker = CacheTimestampTracker(ts_file)
        tracker.remove_entry("/cache/poster.jpg")

        files = tracker.get_associated_files("/cache/Movie.mkv")
        assert "/cache/poster.jpg" not in files
        assert "/cache/Movie.en.srt" in files


# ============================================================
# Retention delegation for non-video files
# ============================================================

class TestRetentionDelegation:
    def test_artwork_inherits_parent_retention(self, temp_dir):
        """Non-video associated files inherit parent's retention period."""
        ts_file = os.path.join(temp_dir, "timestamps.json")
        data = {
            "/cache/Movie.mkv": {
                "cached_at": datetime.now().isoformat(),
                "source": "ondeck",
                "associated_files": ["/cache/poster.jpg"]
            }
        }
        with open(ts_file, 'w') as f:
            json.dump(data, f, indent=2)

        tracker = CacheTimestampTracker(ts_file)
        # Parent is within retention
        assert tracker.is_within_retention_period("/cache/Movie.mkv", 24)
        # Artwork should also be within retention via delegation
        assert tracker.is_within_retention_period("/cache/poster.jpg", 24)


# ============================================================
# is_season_like_folder tests
# ============================================================

class TestIsSeasonLikeFolder:
    def test_season_numbered(self):
        assert is_season_like_folder("Season 01") is True
        assert is_season_like_folder("Season 1") is True
        assert is_season_like_folder("Season 10") is True

    def test_series_numbered(self):
        assert is_season_like_folder("Series 1") is True
        assert is_season_like_folder("Series 02") is True

    def test_specials(self):
        assert is_season_like_folder("Specials") is True
        assert is_season_like_folder("specials") is True
        assert is_season_like_folder("SPECIALS") is True

    def test_bare_numeric(self):
        assert is_season_like_folder("01") is True
        assert is_season_like_folder("1") is True
        assert is_season_like_folder("12") is True

    def test_case_insensitive(self):
        assert is_season_like_folder("season 01") is True
        assert is_season_like_folder("SEASON 01") is True

    def test_movie_folder_not_matched(self):
        assert is_season_like_folder("Movie (2020)") is False

    def test_show_name_not_matched(self):
        assert is_season_like_folder("Breaking Bad") is False

    def test_extras_not_matched(self):
        assert is_season_like_folder("Extras") is False
        assert is_season_like_folder("Behind the Scenes") is False


# ============================================================
# Show root directory discovery tests
# ============================================================

class TestShowRootDiscovery:
    def test_discovers_show_root_files(self, temp_dir):
        """Show-root assets are discovered when video is in a Season folder."""
        show_dir = os.path.join(temp_dir, "Show Name")
        season_dir = os.path.join(show_dir, "Season 01")
        os.makedirs(season_dir)

        video = create_test_file(os.path.join(season_dir, "S01E01.mkv"), "video")
        poster = create_test_file(os.path.join(show_dir, "poster.jpg"), "img")
        fanart = create_test_file(os.path.join(show_dir, "fanart.jpg"), "img")
        theme = create_test_file(os.path.join(show_dir, "theme.mp3"), "audio")

        finder = SiblingFileFinder()
        result = finder.get_media_siblings_grouped([video])
        siblings = result[video]
        assert poster in siblings
        assert fanart in siblings
        assert theme in siblings

    def test_no_parent_scan_for_movie(self, temp_dir):
        """Movie folders do NOT trigger parent directory scan."""
        library_dir = os.path.join(temp_dir, "Movies")
        movie_dir = os.path.join(library_dir, "Movie (2020)")
        os.makedirs(movie_dir)

        # Put a file in the library root — should NOT be discovered
        create_test_file(os.path.join(library_dir, "library_poster.jpg"), "img")
        video = create_test_file(os.path.join(movie_dir, "Movie (2020).mkv"), "video")
        poster = create_test_file(os.path.join(movie_dir, "poster.jpg"), "img")

        finder = SiblingFileFinder()
        result = finder.get_media_siblings_grouped([video])
        siblings = result[video]
        # Same-dir poster is found
        assert poster in siblings
        # Library-level file is NOT found
        library_poster = os.path.join(library_dir, "library_poster.jpg")
        assert library_poster not in siblings

    def test_deduplication_across_episodes(self, temp_dir):
        """Show-root files are only assigned to the first episode processed."""
        show_dir = os.path.join(temp_dir, "Show Name")
        season_dir = os.path.join(show_dir, "Season 01")
        os.makedirs(season_dir)

        ep1 = create_test_file(os.path.join(season_dir, "S01E01.mkv"), "video")
        ep2 = create_test_file(os.path.join(season_dir, "S01E02.mkv"), "video")
        poster = create_test_file(os.path.join(show_dir, "poster.jpg"), "img")

        finder = SiblingFileFinder()
        result = finder.get_media_siblings_grouped([ep1, ep2])

        # First episode gets the show-root poster
        assert poster in result[ep1]
        # Second episode does NOT (parent dir already scanned)
        assert poster not in result[ep2]

    def test_deduplication_across_seasons(self, temp_dir):
        """Show-root files are not re-discovered for episodes in different seasons."""
        show_dir = os.path.join(temp_dir, "Show Name")
        s1_dir = os.path.join(show_dir, "Season 01")
        s2_dir = os.path.join(show_dir, "Season 02")
        os.makedirs(s1_dir)
        os.makedirs(s2_dir)

        ep1 = create_test_file(os.path.join(s1_dir, "S01E01.mkv"), "video")
        ep2 = create_test_file(os.path.join(s2_dir, "S02E01.mkv"), "video")
        poster = create_test_file(os.path.join(show_dir, "poster.jpg"), "img")

        finder = SiblingFileFinder()
        result = finder.get_media_siblings_grouped([ep1, ep2])

        # First episode gets it
        assert poster in result[ep1]
        # Season 02 episode does NOT re-discover
        assert poster not in result[ep2]

    def test_skips_hidden_and_plexcached_in_show_root(self, temp_dir):
        """_find_sibling_files filtering applies to show root too."""
        show_dir = os.path.join(temp_dir, "Show Name")
        season_dir = os.path.join(show_dir, "Season 01")
        os.makedirs(season_dir)

        video = create_test_file(os.path.join(season_dir, "S01E01.mkv"), "video")
        poster = create_test_file(os.path.join(show_dir, "poster.jpg"), "img")
        create_test_file(os.path.join(show_dir, ".hidden"), "hidden")
        create_test_file(os.path.join(show_dir, "poster.jpg.plexcached"), "backup")

        finder = SiblingFileFinder()
        result = finder.get_media_siblings_grouped([video])
        siblings = result[video]
        assert poster in siblings
        names = [os.path.basename(f) for f in siblings]
        assert ".hidden" not in names
        assert not any(n.endswith(".plexcached") for n in names)

    def test_skips_video_files_in_show_root(self, temp_dir):
        """Video files in the show root are NOT included as siblings."""
        show_dir = os.path.join(temp_dir, "Show Name")
        season_dir = os.path.join(show_dir, "Season 01")
        os.makedirs(season_dir)

        video = create_test_file(os.path.join(season_dir, "S01E01.mkv"), "video")
        create_test_file(os.path.join(show_dir, "trailer.mkv"), "trailer")
        poster = create_test_file(os.path.join(show_dir, "poster.jpg"), "img")

        finder = SiblingFileFinder()
        result = finder.get_media_siblings_grouped([video])
        siblings = result[video]
        assert poster in siblings
        # Video files in show root are excluded by _find_sibling_files
        assert not any(is_video_file(s) for s in siblings)

    def test_skips_subdirectories_in_show_root(self, temp_dir):
        """Subdirectories (Season folders) are not included as siblings."""
        show_dir = os.path.join(temp_dir, "Show Name")
        season_dir = os.path.join(show_dir, "Season 01")
        os.makedirs(os.path.join(show_dir, "Season 02"))  # Another season dir
        os.makedirs(season_dir)

        video = create_test_file(os.path.join(season_dir, "S01E01.mkv"), "video")
        poster = create_test_file(os.path.join(show_dir, "poster.jpg"), "img")

        finder = SiblingFileFinder()
        result = finder.get_media_siblings_grouped([video])
        siblings = result[video]
        # Only files, no directories
        assert poster in siblings
        for s in siblings:
            assert os.path.isfile(s)

    def test_specials_folder_triggers_parent_scan(self, temp_dir):
        """Specials/ is a season-like folder and triggers show root scan."""
        show_dir = os.path.join(temp_dir, "Show Name")
        specials_dir = os.path.join(show_dir, "Specials")
        os.makedirs(specials_dir)

        video = create_test_file(os.path.join(specials_dir, "Special01.mkv"), "video")
        poster = create_test_file(os.path.join(show_dir, "poster.jpg"), "img")

        finder = SiblingFileFinder()
        result = finder.get_media_siblings_grouped([video])
        assert poster in result[video]

    def test_bare_numeric_folder_triggers_parent_scan(self, temp_dir):
        """Bare numeric folder (e.g., '01') triggers show root scan."""
        show_dir = os.path.join(temp_dir, "Show Name")
        numeric_dir = os.path.join(show_dir, "01")
        os.makedirs(numeric_dir)

        video = create_test_file(os.path.join(numeric_dir, "S01E01.mkv"), "video")
        poster = create_test_file(os.path.join(show_dir, "poster.jpg"), "img")

        finder = SiblingFileFinder()
        result = finder.get_media_siblings_grouped([video])
        assert poster in result[video]


# ============================================================
# Show root eviction reference counting tests
# ============================================================

class TestShowRootEviction:
    def test_get_other_videos_in_subdirectories(self, temp_dir):
        """Finds videos across multiple season subdirectories."""
        ts_file = os.path.join(temp_dir, "timestamps.json")
        data = {
            "/cache/Show/Season 1/S01E01.mkv": {
                "cached_at": "2025-12-01T10:00:00",
                "source": "ondeck"
            },
            "/cache/Show/Season 1/S01E02.mkv": {
                "cached_at": "2025-12-01T10:00:00",
                "source": "ondeck"
            },
            "/cache/Show/Season 2/S02E01.mkv": {
                "cached_at": "2025-12-01T10:00:00",
                "source": "ondeck"
            }
        }
        with open(ts_file, 'w') as f:
            json.dump(data, f, indent=2)

        tracker = CacheTimestampTracker(ts_file)
        others = tracker.get_other_videos_in_subdirectories(
            "/cache/Show",
            excluding="/cache/Show/Season 1/S01E01.mkv"
        )
        assert "/cache/Show/Season 1/S01E02.mkv" in others
        assert "/cache/Show/Season 2/S02E01.mkv" in others
        assert "/cache/Show/Season 1/S01E01.mkv" not in others

    def test_empty_when_last_episode(self, temp_dir):
        """Returns empty when the excluded video is the only one."""
        ts_file = os.path.join(temp_dir, "timestamps.json")
        data = {
            "/cache/Show/Season 1/S01E01.mkv": {
                "cached_at": "2025-12-01T10:00:00",
                "source": "ondeck"
            }
        }
        with open(ts_file, 'w') as f:
            json.dump(data, f, indent=2)

        tracker = CacheTimestampTracker(ts_file)
        others = tracker.get_other_videos_in_subdirectories(
            "/cache/Show",
            excluding="/cache/Show/Season 1/S01E01.mkv"
        )
        assert others == []

    def test_does_not_match_sibling_shows(self, temp_dir):
        """Videos from a different show under the same library root are NOT matched."""
        ts_file = os.path.join(temp_dir, "timestamps.json")
        data = {
            "/cache/TV/Show A/Season 1/S01E01.mkv": {
                "cached_at": "2025-12-01T10:00:00",
                "source": "ondeck"
            },
            "/cache/TV/Show B/Season 1/S01E01.mkv": {
                "cached_at": "2025-12-01T10:00:00",
                "source": "ondeck"
            }
        }
        with open(ts_file, 'w') as f:
            json.dump(data, f, indent=2)

        tracker = CacheTimestampTracker(ts_file)
        others = tracker.get_other_videos_in_subdirectories(
            "/cache/TV/Show A",
            excluding="/cache/TV/Show A/Season 1/S01E01.mkv"
        )
        # Show B's episode should NOT appear
        assert others == []

    def test_show_root_poster_survives_single_eviction(self, temp_dir):
        """Show-root poster is reassociated when one episode is evicted but others remain."""
        ts_file = os.path.join(temp_dir, "timestamps.json")
        data = {
            "/cache/Show/Season 1/S01E01.mkv": {
                "cached_at": "2025-12-01T10:00:00",
                "source": "ondeck",
                "associated_files": ["/cache/Show/poster.jpg"]
            },
            "/cache/Show/Season 1/S01E02.mkv": {
                "cached_at": "2025-12-01T10:00:00",
                "source": "ondeck"
            }
        }
        with open(ts_file, 'w') as f:
            json.dump(data, f, indent=2)

        tracker = CacheTimestampTracker(ts_file)

        # Simulate eviction reference counting: poster.jpg is in show root,
        # video is in Season 1. Directories differ → use subdirectory check.
        assoc_file = "/cache/Show/poster.jpg"
        check_path = "/cache/Show/Season 1/S01E01.mkv"
        directory = os.path.dirname(assoc_file)  # /cache/Show
        video_dir = os.path.dirname(check_path)  # /cache/Show/Season 1

        assert directory != video_dir
        others = tracker.get_other_videos_in_subdirectories(directory, excluding=check_path)
        assert len(others) == 1
        assert "/cache/Show/Season 1/S01E02.mkv" in others

        # Reassociate poster to remaining episode
        tracker.reassociate_file(assoc_file, from_parent=check_path, to_parent=others[0])
        assert tracker.find_parent_video(assoc_file) == "/cache/Show/Season 1/S01E02.mkv"

    def test_show_root_poster_evicted_with_last_episode(self, temp_dir):
        """Show-root poster is evicted when the last episode is evicted."""
        ts_file = os.path.join(temp_dir, "timestamps.json")
        data = {
            "/cache/Show/Season 1/S01E01.mkv": {
                "cached_at": "2025-12-01T10:00:00",
                "source": "ondeck",
                "associated_files": ["/cache/Show/poster.jpg"]
            }
        }
        with open(ts_file, 'w') as f:
            json.dump(data, f, indent=2)

        tracker = CacheTimestampTracker(ts_file)

        assoc_file = "/cache/Show/poster.jpg"
        check_path = "/cache/Show/Season 1/S01E01.mkv"
        directory = os.path.dirname(assoc_file)

        others = tracker.get_other_videos_in_subdirectories(directory, excluding=check_path)
        # No other episodes — poster should be evicted
        assert others == []
