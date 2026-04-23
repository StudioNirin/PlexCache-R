"""Tests for FileFilter._extract_media_name().

Regression tests for the associated-file thrash bug: main video files and
their artwork/NFO/subtitle siblings in the same movie folder must produce
the same identifier so the "still needed in OnDeck" lookup matches for all
of them, not just the main .mkv.
"""

import os
import sys
from unittest.mock import MagicMock

import pytest

sys.modules['fcntl'] = MagicMock()
for _mod in [
    'apscheduler', 'apscheduler.schedulers',
    'apscheduler.schedulers.background', 'apscheduler.triggers',
    'apscheduler.triggers.cron', 'apscheduler.triggers.interval',
    'plexapi', 'plexapi.server', 'plexapi.video', 'plexapi.myplex',
    'plexapi.library', 'plexapi.exceptions', 'requests',
]:
    sys.modules.setdefault(_mod, MagicMock())

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.file_operations import FileFilter


@pytest.fixture
def file_filter():
    """Bare FileFilter instance (skips __init__ — we only exercise _extract_media_name)."""
    return FileFilter.__new__(FileFilter)


class TestMovieFolderConsistency:
    """Regression: main video and associated siblings must share an identifier."""

    def test_mkv_and_png_in_same_movie_folder_return_same_name(self, file_filter):
        folder = "/mnt/cache/Movies/A Minecraft Movie (2025)"
        mkv = f"{folder}/A Minecraft Movie (2025) - [WEBDL-1080P][EAC3 ATMOS 5.1][X264][8Bit]-TECHNOBLADENEVERDIES.mkv"
        png = f"{folder}/A Minecraft Movie (2025) - [WEBDL-1080P][EAC3 ATMOS 5.1][X264][8Bit]-TECHNOBLADENEVERDIES-clearlogo.png"

        assert file_filter._extract_media_name(mkv) == file_filter._extract_media_name(png)

    def test_mkv_and_all_artwork_variants_match(self, file_filter):
        folder = "/mnt/cache/Movies/The Running Man (2025)"
        mkv = f"{folder}/The Running Man (2025) - [WEBDL-1080P][EAC3 ATMOS 5.1][H264][8Bit]-BYNDR.mkv"
        poster = f"{folder}/The Running Man (2025) - [WEBDL-1080P][EAC3 ATMOS 5.1][H264][8Bit]-BYNDR-poster.jpg"
        fanart = f"{folder}/The Running Man (2025) - [WEBDL-1080P][EAC3 ATMOS 5.1][H264][8Bit]-BYNDR-fanart.jpg"
        clearlogo = f"{folder}/The Running Man (2025) - [WEBDL-1080P][EAC3 ATMOS 5.1][H264][8Bit]-BYNDR-clearlogo.png"
        nfo = f"{folder}/The Running Man (2025) - [WEBDL-1080P][EAC3 ATMOS 5.1][H264][8Bit]-BYNDR.nfo"

        mkv_name = file_filter._extract_media_name(mkv)
        assert mkv_name == file_filter._extract_media_name(poster)
        assert mkv_name == file_filter._extract_media_name(fanart)
        assert mkv_name == file_filter._extract_media_name(clearlogo)
        assert mkv_name == file_filter._extract_media_name(nfo)

    def test_movie_in_yearless_folder_is_consistent(self, file_filter):
        """Users who don't include (YYYY) in folder names still get siblings matched."""
        folder = "/mnt/cache/Movies/A Minecraft Movie"
        mkv = f"{folder}/A Minecraft Movie - [WEBDL-1080P][EAC3 ATMOS 5.1][X264][8Bit]-TECHNOBLADENEVERDIES.mkv"
        poster = f"{folder}/A Minecraft Movie - [WEBDL-1080P][EAC3 ATMOS 5.1][X264][8Bit]-TECHNOBLADENEVERDIES-poster.jpg"

        assert file_filter._extract_media_name(mkv) == file_filter._extract_media_name(poster)
        assert file_filter._extract_media_name(mkv) == "A Minecraft Movie"

    def test_simple_mkv_equals_folder_name(self, file_filter):
        """'/MOVIE/MOVIE.mkv' layout without any release-group or quality tags."""
        path = "/mnt/cache/Movies/The Matrix/The Matrix.mkv"
        assert file_filter._extract_media_name(path) == "The Matrix"


class TestTvShowPathsUnchanged:
    """TV-show-in-Season-folder behavior must keep working unchanged."""

    def test_tv_mkv_returns_show_name(self, file_filter):
        path = "/mnt/cache/TV Shows/Scrubs (2026)/Season 01/Scrubs (2026) - S01E02 - My 2nd First Day [WEBDL-1080p][5.1][EAC3][8Bit][h264]-Sonarr.mkv"
        assert file_filter._extract_media_name(path) == "Scrubs (2026)"

    def test_tv_episode_artwork_returns_show_name(self, file_filter):
        path = "/mnt/cache/TV Shows/Scrubs (2026)/Season 01/Scrubs (2026) - S01E02-thumb.jpg"
        assert file_filter._extract_media_name(path) == "Scrubs (2026)"

    def test_tv_specials_folder_returns_show_name(self, file_filter):
        path = "/mnt/cache/TV Shows/Some Show/Specials/Some Show - S00E01.mkv"
        assert file_filter._extract_media_name(path) == "Some Show"

    def test_tv_numeric_season_folder_returns_show_name(self, file_filter):
        path = "/mnt/cache/TV Shows/Some Show/01/Some Show - S01E01.mkv"
        assert file_filter._extract_media_name(path) == "Some Show"


class TestMovieAtLibraryRoot:
    """Movies without per-movie subfolders (filename directly under library root)."""

    def test_flat_movie_file_falls_back_to_cleaned_filename(self, file_filter):
        # No per-movie folder → parent_dir is "Movies" (no year), can't use it.
        # Must fall back to filename cleanup.
        path = "/mnt/cache/Movies/Some Old Movie (1999).mkv"
        result = file_filter._extract_media_name(path)
        assert result is not None
        # Should extract something meaningful — the movie title.
        assert "Some Old Movie" in result


class TestAssociatedFilesFallback:
    """Non-video sibling files behave the same as the main video."""

    def test_subtitle_file_delegates_to_parent_folder_in_movie(self, file_filter):
        folder = "/mnt/cache/Movies/A Minecraft Movie (2025)"
        mkv = f"{folder}/A Minecraft Movie (2025) - [WEBDL-1080P]-TECHNOBLADENEVERDIES.mkv"
        sub = f"{folder}/A Minecraft Movie (2025) - [WEBDL-1080P]-TECHNOBLADENEVERDIES.en.srt"

        assert file_filter._extract_media_name(mkv) == file_filter._extract_media_name(sub)

    def test_subtitle_in_tv_season_returns_show_name(self, file_filter):
        path = "/mnt/cache/TV Shows/Scrubs (2026)/Season 01/Scrubs (2026) - S01E02.en.srt"
        assert file_filter._extract_media_name(path) == "Scrubs (2026)"
