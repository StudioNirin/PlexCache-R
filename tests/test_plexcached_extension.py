"""
Unit tests for .plexcached file extension handling.

These tests verify that file extensions are preserved correctly when creating
.plexcached backup files. This addresses a reported bug where TV show backups
were created without their original extension (e.g., 'episode.plexcached'
instead of 'episode.mkv.plexcached').
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Mock fcntl for Windows compatibility
sys.modules['fcntl'] = MagicMock()

from core.file_operations import (
    get_media_identity,
    PLEXCACHED_EXTENSION,
)


class TestPlexcachedExtension(unittest.TestCase):
    """Test that .plexcached files preserve the original extension."""

    def test_plexcached_extension_constant(self):
        """Verify the constant is correct."""
        self.assertEqual(PLEXCACHED_EXTENSION, ".plexcached")

    # =========================================================================
    # Test get_media_identity - used for matching files
    # =========================================================================

    def test_media_identity_movie_with_extension(self):
        """Movie with extension extracts correct identity."""
        path = "/mnt/user/Movies/Wreck-It Ralph (2012) [WEBDL-1080p].mkv"
        identity = get_media_identity(path)
        self.assertEqual(identity, "Wreck-It Ralph (2012)")

    def test_media_identity_tv_with_extension(self):
        """TV episode with extension extracts correct identity."""
        path = "/mnt/user/TV/From - S01E02 - The Way Things Are Now [HDTV-1080p].mkv"
        identity = get_media_identity(path)
        self.assertEqual(identity, "From - S01E02 - The Way Things Are Now")

    def test_media_identity_anime_with_extension(self):
        """Anime episode (the reported bug case) extracts correct identity."""
        path = "/mnt/user/TV/My Hero Academia - 170 - My Hero Academia (HDTV-1080p 10bit DualAudio).mkv"
        identity = get_media_identity(path)
        # Should preserve the episode title
        self.assertIn("My Hero Academia - 170", identity)

    def test_media_identity_plexcached_with_extension(self):
        """Properly named .plexcached file extracts correct identity."""
        path = "/mnt/user/TV/episode.mkv.plexcached"
        identity = get_media_identity(path)
        self.assertEqual(identity, "episode")

    def test_media_identity_plexcached_without_extension(self):
        """Malformed .plexcached file (missing extension) extracts identity."""
        # This is the bug case - file named episode.plexcached instead of episode.mkv.plexcached
        path = "/mnt/user/TV/episode.plexcached"
        identity = get_media_identity(path)
        self.assertEqual(identity, "episode")

    def test_media_identity_multiple_dots(self):
        """Filename with multiple dots preserves identity."""
        path = "/mnt/user/TV/Show.Name.S01E01.Episode.Title.1080p.WEB-DL.mkv"
        identity = get_media_identity(path)
        # Should remove quality info in brackets but preserve dots in name
        self.assertIn("Show.Name.S01E01", identity)

    def test_media_identity_no_extension(self):
        """File with no extension still extracts identity."""
        path = "/mnt/user/Movies/SomeMovie"
        identity = get_media_identity(path)
        self.assertEqual(identity, "SomeMovie")

    # =========================================================================
    # Test plexcached path construction patterns
    # =========================================================================

    def test_plexcached_path_construction_movie(self):
        """Movie path correctly creates .plexcached path."""
        array_file = "/mnt/user0/Movies/Movie (2024).mkv"
        plexcached_file = array_file + PLEXCACHED_EXTENSION

        self.assertEqual(plexcached_file, "/mnt/user0/Movies/Movie (2024).mkv.plexcached")
        # Verify extension is preserved
        self.assertTrue(plexcached_file.endswith(".mkv.plexcached"))

    def test_plexcached_path_construction_tv_episode(self):
        """TV episode path correctly creates .plexcached path."""
        array_file = "/mnt/user0/TV Shows/Show Name/Season 1/Show - S01E01 - Pilot.mkv"
        plexcached_file = array_file + PLEXCACHED_EXTENSION

        self.assertEqual(
            plexcached_file,
            "/mnt/user0/TV Shows/Show Name/Season 1/Show - S01E01 - Pilot.mkv.plexcached"
        )
        self.assertTrue(plexcached_file.endswith(".mkv.plexcached"))

    def test_plexcached_path_construction_anime(self):
        """Anime episode (reported bug case) correctly creates .plexcached path."""
        array_file = "/mnt/user0/TV/My Hero Academia - 170 - My Hero Academia (HDTV-1080p 10bit DualAudio).mkv"
        plexcached_file = array_file + PLEXCACHED_EXTENSION

        expected = "/mnt/user0/TV/My Hero Academia - 170 - My Hero Academia (HDTV-1080p 10bit DualAudio).mkv.plexcached"
        self.assertEqual(plexcached_file, expected)
        self.assertTrue(plexcached_file.endswith(".mkv.plexcached"))

    def test_plexcached_path_construction_mp4(self):
        """MP4 file correctly creates .plexcached path."""
        array_file = "/mnt/user0/Movies/Movie.mp4"
        plexcached_file = array_file + PLEXCACHED_EXTENSION

        self.assertTrue(plexcached_file.endswith(".mp4.plexcached"))

    def test_plexcached_path_construction_subtitle(self):
        """Subtitle file correctly creates .plexcached path."""
        array_file = "/mnt/user0/Movies/Movie.en.srt"
        plexcached_file = array_file + PLEXCACHED_EXTENSION

        self.assertTrue(plexcached_file.endswith(".srt.plexcached"))

    # =========================================================================
    # Test path conversion edge cases
    # =========================================================================

    def test_path_replace_preserves_extension(self):
        """String replace for path conversion preserves extension."""
        # This is how we convert /mnt/user/ to /mnt/user0/
        file_path = "/mnt/user/TV Shows/Show/Season 1/Episode.mkv"
        array_file = file_path.replace("/mnt/user/", "/mnt/user0/", 1)

        self.assertEqual(array_file, "/mnt/user0/TV Shows/Show/Season 1/Episode.mkv")
        self.assertTrue(array_file.endswith(".mkv"))

    def test_path_replace_cache_to_array(self):
        """Cache to array path conversion preserves extension."""
        cache_path = "/mnt/cache/TV Shows/Show/Season 1/Episode.mkv"
        # Simulate path mapping conversion
        array_path = cache_path.replace("/mnt/cache/", "/mnt/user/", 1)

        self.assertEqual(array_path, "/mnt/user/TV Shows/Show/Season 1/Episode.mkv")
        self.assertTrue(array_path.endswith(".mkv"))

    def test_basename_preserves_extension(self):
        """os.path.basename preserves extension."""
        path = "/mnt/user/TV/Show - S01E01.mkv"
        basename = os.path.basename(path)

        self.assertEqual(basename, "Show - S01E01.mkv")
        self.assertTrue(basename.endswith(".mkv"))

    def test_join_preserves_extension(self):
        """os.path.join with basename preserves extension."""
        original = "/mnt/user/TV/Show - S01E01.mkv"
        new_dir = "/mnt/cache/TV"
        new_path = os.path.join(new_dir, os.path.basename(original))

        self.assertEqual(new_path, "/mnt/cache/TV/Show - S01E01.mkv")
        self.assertTrue(new_path.endswith(".mkv"))

    # =========================================================================
    # Test edge cases that might cause bugs
    # =========================================================================

    def test_filename_ending_with_dot(self):
        """Filename ending with dot handles correctly."""
        # Edge case: what if filename ends with a dot?
        array_file = "/mnt/user0/TV/Strange.File."
        plexcached_file = array_file + PLEXCACHED_EXTENSION

        # Should just append .plexcached
        self.assertEqual(plexcached_file, "/mnt/user0/TV/Strange.File..plexcached")

    def test_filename_with_plexcached_in_name(self):
        """Filename containing 'plexcached' string handles correctly."""
        # Edge case: what if the show is literally called "plexcached"?
        array_file = "/mnt/user0/TV/plexcached - S01E01.mkv"
        plexcached_file = array_file + PLEXCACHED_EXTENSION

        self.assertEqual(plexcached_file, "/mnt/user0/TV/plexcached - S01E01.mkv.plexcached")
        self.assertTrue(plexcached_file.endswith(".mkv.plexcached"))

    def test_very_long_filename(self):
        """Very long filename handles correctly."""
        # Some anime have extremely long episode titles
        long_name = "A" * 200 + ".mkv"
        array_file = f"/mnt/user0/TV/{long_name}"
        plexcached_file = array_file + PLEXCACHED_EXTENSION

        self.assertTrue(plexcached_file.endswith(".mkv.plexcached"))
        self.assertEqual(len(os.path.basename(plexcached_file)), 200 + 4 + 11)  # name + .mkv + .plexcached

    def test_unicode_filename(self):
        """Unicode characters in filename handle correctly."""
        # Japanese characters common in anime
        array_file = "/mnt/user0/TV/僕のヒーローアカデミア - 170.mkv"
        plexcached_file = array_file + PLEXCACHED_EXTENSION

        self.assertTrue(plexcached_file.endswith(".mkv.plexcached"))

    def test_special_characters_in_filename(self):
        """Special characters in filename handle correctly."""
        array_file = "/mnt/user0/TV/Show's Name! (2024) [1080p].mkv"
        plexcached_file = array_file + PLEXCACHED_EXTENSION

        self.assertTrue(plexcached_file.endswith(".mkv.plexcached"))

    def test_parentheses_in_filename(self):
        """Parentheses in filename (like the anime case) handle correctly."""
        # This matches the reported bug filename pattern
        array_file = "/mnt/user0/TV/My Hero Academia - 170 - My Hero Academia (HDTV-1080p 10bit DualAudio).mkv"
        plexcached_file = array_file + PLEXCACHED_EXTENSION

        self.assertTrue(plexcached_file.endswith(").mkv.plexcached"))

    # =========================================================================
    # Test splitext behavior (used in get_media_identity)
    # =========================================================================

    def test_splitext_normal_file(self):
        """splitext correctly handles normal filename."""
        name, ext = os.path.splitext("movie.mkv")
        self.assertEqual(name, "movie")
        self.assertEqual(ext, ".mkv")

    def test_splitext_plexcached_file(self):
        """splitext removes only the last extension."""
        name, ext = os.path.splitext("movie.mkv.plexcached")
        self.assertEqual(name, "movie.mkv")
        self.assertEqual(ext, ".plexcached")

    def test_splitext_no_extension(self):
        """splitext handles file with no extension."""
        name, ext = os.path.splitext("filename")
        self.assertEqual(name, "filename")
        self.assertEqual(ext, "")

    def test_splitext_multiple_dots(self):
        """splitext only removes last extension with multiple dots."""
        name, ext = os.path.splitext("Show.Name.S01E01.1080p.mkv")
        self.assertEqual(name, "Show.Name.S01E01.1080p")
        self.assertEqual(ext, ".mkv")

    def test_splitext_hidden_file(self):
        """splitext handles hidden files (starting with dot)."""
        name, ext = os.path.splitext(".hidden")
        self.assertEqual(name, ".hidden")
        self.assertEqual(ext, "")

    def test_splitext_hidden_file_with_extension(self):
        """splitext handles hidden files with extension."""
        name, ext = os.path.splitext(".hidden.txt")
        self.assertEqual(name, ".hidden")
        self.assertEqual(ext, ".txt")


class TestPlexcachedRestoration(unittest.TestCase):
    """Test that .plexcached files are restored correctly."""

    def test_restore_removes_plexcached_suffix(self):
        """Restoring .plexcached removes only the .plexcached suffix."""
        plexcached_file = "/mnt/user0/TV/episode.mkv.plexcached"
        # This is how restoration works
        original_file = plexcached_file[:-len(PLEXCACHED_EXTENSION)]

        self.assertEqual(original_file, "/mnt/user0/TV/episode.mkv")
        self.assertTrue(original_file.endswith(".mkv"))

    def test_restore_malformed_plexcached(self):
        """Restoring malformed .plexcached (missing extension) exposes the bug."""
        # This is the bug case
        plexcached_file = "/mnt/user0/TV/episode.plexcached"
        original_file = plexcached_file[:-len(PLEXCACHED_EXTENSION)]

        # After restoration, the file has NO extension!
        self.assertEqual(original_file, "/mnt/user0/TV/episode")
        self.assertFalse(original_file.endswith(".mkv"))
        # This is the problem - the restored file has no extension

    def test_restore_length_calculation(self):
        """PLEXCACHED_EXTENSION length is correct for slicing."""
        self.assertEqual(len(PLEXCACHED_EXTENSION), 11)
        self.assertEqual(len(".plexcached"), 11)


class TestHypotheticalBugScenarios(unittest.TestCase):
    """
    Test hypothetical scenarios that could cause the bug.

    These tests explore potential root causes for the reported issue where
    .plexcached files are created without the original file extension.
    """

    def test_scenario_splitext_used_before_append(self):
        """
        HYPOTHESIS: Someone accidentally used splitext before appending .plexcached

        This would cause: movie.mkv -> movie.plexcached (wrong!)
        Instead of: movie.mkv -> movie.mkv.plexcached (correct)
        """
        array_file = "/mnt/user0/TV/episode.mkv"

        # WRONG way (bug):
        wrong_plexcached = os.path.splitext(array_file)[0] + PLEXCACHED_EXTENSION
        self.assertEqual(wrong_plexcached, "/mnt/user0/TV/episode.plexcached")
        self.assertFalse(".mkv" in wrong_plexcached)  # Extension lost!

        # CORRECT way:
        correct_plexcached = array_file + PLEXCACHED_EXTENSION
        self.assertEqual(correct_plexcached, "/mnt/user0/TV/episode.mkv.plexcached")
        self.assertTrue(".mkv" in correct_plexcached)  # Extension preserved!

    def test_scenario_basename_without_extension(self):
        """
        HYPOTHESIS: basename is extracted without extension then path rebuilt
        """
        array_file = "/mnt/user0/TV/episode.mkv"
        directory = os.path.dirname(array_file)

        # WRONG way (bug):
        name_without_ext = os.path.splitext(os.path.basename(array_file))[0]
        wrong_path = os.path.join(directory, name_without_ext + PLEXCACHED_EXTENSION)
        self.assertEqual(wrong_path, "/mnt/user0/TV/episode.plexcached")

        # CORRECT way:
        correct_path = os.path.join(directory, os.path.basename(array_file) + PLEXCACHED_EXTENSION)
        self.assertEqual(correct_path, "/mnt/user0/TV/episode.mkv.plexcached")

    def test_scenario_media_identity_used_for_path(self):
        """
        HYPOTHESIS: get_media_identity is accidentally used to construct the path

        get_media_identity strips the extension (by design, for matching).
        If used for path construction, this would cause the bug.
        """
        array_file = "/mnt/user0/TV/My Hero Academia - 170 - My Hero Academia (HDTV-1080p 10bit DualAudio).mkv"

        # get_media_identity is for MATCHING, not path construction
        identity = get_media_identity(array_file)

        # WRONG: using identity for path (bug)
        directory = os.path.dirname(array_file)
        wrong_plexcached = os.path.join(directory, identity + PLEXCACHED_EXTENSION)
        self.assertFalse(wrong_plexcached.endswith(".mkv.plexcached"))

        # CORRECT: using full path
        correct_plexcached = array_file + PLEXCACHED_EXTENSION
        self.assertTrue(correct_plexcached.endswith(".mkv.plexcached"))

    def test_scenario_plex_returns_path_without_extension(self):
        """
        HYPOTHESIS: Plex API returns path without extension for some files

        If Plex returns '/mnt/plex/TV/episode' instead of '/mnt/plex/TV/episode.mkv',
        then the .plexcached would be created without extension.
        """
        # Simulate Plex returning path without extension
        plex_path_without_ext = "/mnt/plex/TV Shows/Show/Season 1/episode"

        # Path conversion preserves what it gets
        array_path = plex_path_without_ext.replace("/mnt/plex/", "/mnt/user0/", 1)
        plexcached = array_path + PLEXCACHED_EXTENSION

        # The bug would manifest here
        self.assertEqual(plexcached, "/mnt/user0/TV Shows/Show/Season 1/episode.plexcached")
        self.assertFalse(".mkv" in plexcached)

    def test_scenario_tracker_stores_path_without_extension(self):
        """
        HYPOTHESIS: A tracker file stores paths without extensions

        If timestamps.json or ondeck_tracker.json stores paths without extensions,
        and those paths are used to construct .plexcached paths, bug would occur.
        """
        # Simulate reading a path from tracker that's missing extension
        tracker_path = "/mnt/cache/TV/episode"  # Bug: no extension

        # Convert to array path
        array_path = tracker_path.replace("/mnt/cache/", "/mnt/user0/", 1)
        plexcached = array_path + PLEXCACHED_EXTENSION

        # Bug manifests
        self.assertEqual(plexcached, "/mnt/user0/TV/episode.plexcached")


class TestRealBugCases(unittest.TestCase):
    """
    Test cases using actual filenames from the bug report.

    From the screenshot, we can see:
    - Episodes 160-163 have CORRECT naming: filename.mkv
    - Episodes 164-170 have BROKEN naming: filename.plexcached (missing .mkv)

    This class tests these specific filenames to understand the bug.
    """

    # These are the CORRECT files (have .mkv extension)
    CORRECT_FILES = [
        "My Hero Academia - 160 - Toshinori Yagi Rising Origin (HDTV-1080p 10bit DualAudio).mkv",
        "My Hero Academia - 161 - The End of an Era and the Beginning (HDTV-1080p 10bit DualAudio).mkv",
        "My Hero Academia - 162 - The Final Boss!! (WEBDL-1080p DualAudio).mkv",
        "My Hero Academia - 163 - Quirk Explosion!! (HDTV-1080p 10bit DualAudio).mkv",
    ]

    # These are the BROKEN .plexcached files (missing .mkv before .plexcached)
    BROKEN_PLEXCACHED = [
        "My Hero Academia - 164 - Historys Greatest Villain (HDTV-1080p 10bit DualAudio).plexcached",
        "My Hero Academia - 165 - Wreck It Open Izuku Midoriya!! (HDTV-1080p 10bit DualAudio).plexcached",
        "My Hero Academia - 166 - From Aizawa (HDTV-1080p 10bit DualAudio).plexcached",
        "My Hero Academia - 167 - Izuku Midoriya Rising (HDTV-1080p 10bit DualAudio).plexcached",
        "My Hero Academia - 168 - Epilogue The Hellish Todoroki Family Final (HDTV-1080p 10bit DualAudio).plexcached",
        "My Hero Academia - 169 - The Girl Who Loves Smiles (HDTV-1080p 10bit DualAudio).plexcached",
        "My Hero Academia - 170 - My Hero Academia (HDTV-1080p 10bit DualAudio).plexcached",
    ]

    # What the broken files SHOULD have been named
    EXPECTED_PLEXCACHED = [
        "My Hero Academia - 164 - Historys Greatest Villain (HDTV-1080p 10bit DualAudio).mkv.plexcached",
        "My Hero Academia - 165 - Wreck It Open Izuku Midoriya!! (HDTV-1080p 10bit DualAudio).mkv.plexcached",
        "My Hero Academia - 166 - From Aizawa (HDTV-1080p 10bit DualAudio).mkv.plexcached",
        "My Hero Academia - 167 - Izuku Midoriya Rising (HDTV-1080p 10bit DualAudio).mkv.plexcached",
        "My Hero Academia - 168 - Epilogue The Hellish Todoroki Family Final (HDTV-1080p 10bit DualAudio).mkv.plexcached",
        "My Hero Academia - 169 - The Girl Who Loves Smiles (HDTV-1080p 10bit DualAudio).mkv.plexcached",
        "My Hero Academia - 170 - My Hero Academia (HDTV-1080p 10bit DualAudio).mkv.plexcached",
    ]

    def test_correct_files_have_mkv_extension(self):
        """Verify correct files end with .mkv"""
        for filename in self.CORRECT_FILES:
            self.assertTrue(filename.endswith(".mkv"), f"Expected .mkv extension: {filename}")

    def test_broken_files_missing_mkv_extension(self):
        """Document that broken files are missing .mkv before .plexcached"""
        for filename in self.BROKEN_PLEXCACHED:
            # These end with ).plexcached instead of ).mkv.plexcached
            self.assertTrue(filename.endswith(").plexcached"), f"Broken pattern: {filename}")
            self.assertFalse(".mkv" in filename, f"Should be missing .mkv: {filename}")

    def test_expected_plexcached_format(self):
        """Verify what the correct .plexcached names should be"""
        for filename in self.EXPECTED_PLEXCACHED:
            self.assertTrue(filename.endswith(".mkv.plexcached"), f"Expected format: {filename}")

    def test_plexcached_creation_with_real_filename(self):
        """Test .plexcached creation with actual problematic filename"""
        # This is what the array file should look like
        array_file = "/mnt/user0/media/tv/series-anime/My Hero Academia (2016)/Season 8/My Hero Academia - 170 - My Hero Academia (HDTV-1080p 10bit DualAudio).mkv"

        # Correct .plexcached creation
        plexcached = array_file + PLEXCACHED_EXTENSION

        self.assertTrue(plexcached.endswith(".mkv.plexcached"))
        self.assertEqual(
            os.path.basename(plexcached),
            "My Hero Academia - 170 - My Hero Academia (HDTV-1080p 10bit DualAudio).mkv.plexcached"
        )

    def test_filename_ending_with_parenthesis(self):
        """
        HYPOTHESIS: The closing parenthesis ) before extension might cause issues.

        All affected files end with pattern: ...DualAudio).mkv
        Could there be regex or parsing that incorrectly handles the ) character?
        """
        # Filename pattern from bug report
        filename = "My Hero Academia - 170 - My Hero Academia (HDTV-1080p 10bit DualAudio).mkv"

        # Test splitext behavior with parenthesis before extension
        name, ext = os.path.splitext(filename)
        self.assertEqual(ext, ".mkv")
        self.assertEqual(name, "My Hero Academia - 170 - My Hero Academia (HDTV-1080p 10bit DualAudio)")

        # Test that .plexcached is correctly appended
        plexcached = filename + PLEXCACHED_EXTENSION
        self.assertTrue(plexcached.endswith(").mkv.plexcached"))

    def test_get_media_identity_with_real_filenames(self):
        """Test media identity extraction with the real problematic filenames"""
        # Working file
        working = "My Hero Academia - 160 - Toshinori Yagi Rising Origin (HDTV-1080p 10bit DualAudio).mkv"
        identity_working = get_media_identity(working)

        # Broken .plexcached (what was created)
        broken = "My Hero Academia - 164 - Historys Greatest Villain (HDTV-1080p 10bit DualAudio).plexcached"
        identity_broken = get_media_identity(broken)

        # Both should extract valid identities (for matching purposes)
        self.assertIn("My Hero Academia - 160", identity_working)
        self.assertIn("My Hero Academia - 164", identity_broken)

    def test_path_with_parentheses_in_folder(self):
        """
        Test paths where parent folder also has parentheses.

        Full path from screenshot: /mnt/user0/media/tv/series-anime/My Hero Academia (2016)/Season 8/
        """
        full_path = "/mnt/user0/media/tv/series-anime/My Hero Academia (2016)/Season 8/My Hero Academia - 170 - My Hero Academia (HDTV-1080p 10bit DualAudio).mkv"

        # Path conversion should preserve everything
        cache_path = full_path.replace("/mnt/user0/", "/mnt/cache/", 1)
        self.assertTrue(cache_path.endswith(".mkv"))

        # .plexcached creation should work
        plexcached = full_path + PLEXCACHED_EXTENSION
        self.assertTrue(plexcached.endswith(".mkv.plexcached"))

    def test_reconstruct_what_bug_would_look_like(self):
        """
        Reconstruct what code path would cause this bug.

        The bug creates: episode.plexcached
        Instead of: episode.mkv.plexcached

        This means somewhere, the .mkv extension was stripped BEFORE
        appending .plexcached.
        """
        original_file = "My Hero Academia - 170 - My Hero Academia (HDTV-1080p 10bit DualAudio).mkv"

        # BUGGY CODE would do something like this:
        # Option 1: Using splitext before appending
        buggy_result_1 = os.path.splitext(original_file)[0] + PLEXCACHED_EXTENSION
        self.assertEqual(
            buggy_result_1,
            "My Hero Academia - 170 - My Hero Academia (HDTV-1080p 10bit DualAudio).plexcached"
        )
        # This matches the broken files!

        # CORRECT CODE:
        correct_result = original_file + PLEXCACHED_EXTENSION
        self.assertEqual(
            correct_result,
            "My Hero Academia - 170 - My Hero Academia (HDTV-1080p 10bit DualAudio).mkv.plexcached"
        )

    def test_episode_number_pattern(self):
        """
        The bug affects episodes 164-170 but not 160-163.

        Could there be something about episode numbers > 163 that triggers a bug?
        (Unlikely, but worth testing)
        """
        for ep_num in range(160, 171):
            filename = f"My Hero Academia - {ep_num} - Episode Title (HDTV-1080p 10bit DualAudio).mkv"
            plexcached = filename + PLEXCACHED_EXTENSION

            # All should work correctly
            self.assertTrue(
                plexcached.endswith(".mkv.plexcached"),
                f"Episode {ep_num} should have correct extension"
            )


class TestCodePathAnalysis(unittest.TestCase):
    """
    Analyze specific code paths that could cause the bug.

    Based on the test cases, the bug is caused by using splitext() or similar
    before appending .plexcached. Let's find where this might happen.
    """

    def test_hypothesis_get_media_identity_used_for_path(self):
        """
        HYPOTHESIS: Code accidentally uses get_media_identity() result for path construction.

        get_media_identity() strips extension by design (for matching).
        If mistakenly used to build the .plexcached path, this would cause the bug.
        """
        array_file = "/mnt/user0/TV/My Hero Academia - 170 - My Hero Academia (HDTV-1080p 10bit DualAudio).mkv"

        # get_media_identity strips extension
        identity = get_media_identity(array_file)
        self.assertFalse(identity.endswith(".mkv"))

        # If someone did this (BUG):
        directory = os.path.dirname(array_file)
        buggy_plexcached = os.path.join(directory, identity + PLEXCACHED_EXTENSION)

        # Result would match the broken files!
        self.assertTrue(buggy_plexcached.endswith(").plexcached"))
        self.assertFalse(".mkv.plexcached" in buggy_plexcached)

    def test_hypothesis_wrong_variable_used(self):
        """
        HYPOTHESIS: Code uses wrong variable (one that had extension stripped).

        For example, if there's a display_name variable without extension
        that gets accidentally used for the rename operation.
        """
        array_file = "/mnt/user0/TV/episode.mkv"

        # Some code might create a display name without extension
        display_name = os.path.splitext(os.path.basename(array_file))[0]
        self.assertEqual(display_name, "episode")

        # If this display_name is accidentally used:
        directory = os.path.dirname(array_file)
        buggy_path = os.path.join(directory, display_name + PLEXCACHED_EXTENSION)
        self.assertEqual(buggy_path, "/mnt/user0/TV/episode.plexcached")  # BUG!

        # Correct would be:
        correct_path = array_file + PLEXCACHED_EXTENSION
        self.assertEqual(correct_path, "/mnt/user0/TV/episode.mkv.plexcached")


if __name__ == "__main__":
    unittest.main(verbosity=2)
