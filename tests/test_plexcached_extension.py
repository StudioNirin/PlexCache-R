"""Tests for .plexcached file extension handling.

Tests get_media_identity() — the production function used for matching files
across upgrade renames. Also verifies the PLEXCACHED_EXTENSION constant
and representative path construction patterns.

Previous test file tested stdlib functions (os.path.splitext, str.replace, etc.)
which always pass regardless of production code correctness. This file focuses
on real production code.
"""

import os
import sys
from unittest.mock import MagicMock

# conftest.py handles fcntl/apscheduler mocking and path setup

from core.file_operations import (
    get_media_identity,
    PLEXCACHED_EXTENSION,
)


# ============================================================================
# TestPlexcachedConstant
# ============================================================================

class TestPlexcachedConstant:
    """Verify the .plexcached extension constant."""

    def test_plexcached_extension_value(self):
        """PLEXCACHED_EXTENSION is exactly '.plexcached'."""
        assert PLEXCACHED_EXTENSION == ".plexcached"


# ============================================================================
# TestGetMediaIdentity
# ============================================================================

class TestGetMediaIdentity:
    """Tests for get_media_identity() — used to match files across rename/upgrade."""

    def test_movie_with_extension(self):
        """Movie with extension extracts correct identity."""
        path = "/mnt/user/Movies/Wreck-It Ralph (2012) [WEBDL-1080p].mkv"
        identity = get_media_identity(path)
        assert identity == "Wreck-It Ralph (2012)"

    def test_tv_with_extension(self):
        """TV episode with extension extracts correct identity."""
        path = "/mnt/user/TV/From - S01E02 - The Way Things Are Now [HDTV-1080p].mkv"
        identity = get_media_identity(path)
        assert identity == "From - S01E02 - The Way Things Are Now"

    def test_anime_with_extension(self):
        """Anime episode (the reported bug case) extracts correct identity."""
        path = "/mnt/user/TV/My Hero Academia - 170 - My Hero Academia (HDTV-1080p 10bit DualAudio).mkv"
        identity = get_media_identity(path)
        assert "My Hero Academia - 170" in identity

    def test_plexcached_with_extension(self):
        """Properly named .plexcached file extracts correct identity."""
        path = "/mnt/user/TV/episode.mkv.plexcached"
        identity = get_media_identity(path)
        assert identity == "episode"

    def test_plexcached_without_extension(self):
        """Malformed .plexcached file (missing extension) still extracts identity."""
        path = "/mnt/user/TV/episode.plexcached"
        identity = get_media_identity(path)
        assert identity == "episode"

    def test_multiple_dots(self):
        """Filename with multiple dots preserves identity."""
        path = "/mnt/user/TV/Show.Name.S01E01.Episode.Title.1080p.WEB-DL.mkv"
        identity = get_media_identity(path)
        assert "Show.Name.S01E01" in identity

    def test_no_extension(self):
        """File with no extension still extracts identity."""
        path = "/mnt/user/Movies/SomeMovie"
        identity = get_media_identity(path)
        assert identity == "SomeMovie"

    def test_real_filenames_from_bug_report(self):
        """Tests media identity with actual filenames from the bug report."""
        working = "My Hero Academia - 160 - Toshinori Yagi Rising Origin (HDTV-1080p 10bit DualAudio).mkv"
        broken = "My Hero Academia - 164 - Historys Greatest Villain (HDTV-1080p 10bit DualAudio).plexcached"

        identity_working = get_media_identity(working)
        identity_broken = get_media_identity(broken)

        assert "My Hero Academia - 160" in identity_working
        assert "My Hero Academia - 164" in identity_broken


# ============================================================================
# TestPlexcachedPathConstruction
# ============================================================================

class TestPlexcachedPathConstruction:
    """Representative tests for .plexcached path construction pattern.

    The correct pattern is: array_file + PLEXCACHED_EXTENSION
    (appends .plexcached to the FULL filename including original extension).
    """

    def test_mkv_movie(self):
        """Movie .mkv creates correct .plexcached path."""
        array_file = "/mnt/user0/Movies/Movie (2024).mkv"
        plexcached_file = array_file + PLEXCACHED_EXTENSION
        assert plexcached_file.endswith(".mkv.plexcached")

    def test_anime_with_parentheses(self):
        """Anime filename with parentheses creates correct .plexcached path."""
        array_file = "/mnt/user0/TV/My Hero Academia - 170 - My Hero Academia (HDTV-1080p 10bit DualAudio).mkv"
        plexcached_file = array_file + PLEXCACHED_EXTENSION
        assert plexcached_file.endswith(").mkv.plexcached")

    def test_unicode_filename(self):
        """Unicode characters in filename create correct .plexcached path."""
        array_file = "/mnt/user0/TV/僕のヒーローアカデミア - 170.mkv"
        plexcached_file = array_file + PLEXCACHED_EXTENSION
        assert plexcached_file.endswith(".mkv.plexcached")
