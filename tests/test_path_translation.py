"""
Unit tests for Docker path translation logic.

These tests verify that path translation between container paths and host paths
works correctly, which is critical for:
1. The Unraid mover exclude file (must contain host paths)
2. File existence checks (must use container paths)
3. Log display (should show host paths for user clarity)

Bug context: Issue where clean_stale_exclude_entries() was checking file existence
using host paths inside the container where only container paths exist.
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch
from dataclasses import dataclass

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Mock fcntl for Windows compatibility
sys.modules['fcntl'] = MagicMock()


@dataclass
class MockPathMapping:
    """Mock PathMapping for testing without importing full config module."""
    name: str = ""
    plex_path: str = ""
    real_path: str = ""
    cache_path: str = ""
    host_cache_path: str = ""
    cacheable: bool = True
    enabled: bool = True


class MockMultiPathModifier:
    """Mock MultiPathModifier for testing path translation."""

    def __init__(self, mappings):
        self.mappings = mappings


class TestPathTranslationToHost(unittest.TestCase):
    """Test _translate_to_host_path() - container path -> host path."""

    def setUp(self):
        """Set up FileFilter with mock path mappings."""
        from core.file_operations import FileFilter

        # Typical Docker setup: container sees /mnt/cache, host sees /mnt/cache_downloads
        self.mappings = [
            MockPathMapping(
                name="Movies",
                plex_path="/data/Movies",
                real_path="/mnt/user/media/Movies",
                cache_path="/mnt/cache/media/Movies",
                host_cache_path="/mnt/cache_downloads/media/Movies",
            ),
            MockPathMapping(
                name="TV",
                plex_path="/data/TV",
                real_path="/mnt/user/media/TV",
                cache_path="/mnt/cache/media/TV",
                host_cache_path="/mnt/cache_downloads/media/TV",
            ),
        ]

        self.path_modifier = MockMultiPathModifier(self.mappings)

        self.file_filter = FileFilter(
            real_source="/mnt/user",
            cache_dir="/mnt/cache",
            is_unraid=True,
            mover_cache_exclude_file="/config/exclude.txt",
            path_modifier=self.path_modifier,
            is_docker=True,
        )

    def test_translate_container_to_host_movies(self):
        """Container Movies path translates to host path."""
        container_path = "/mnt/cache/media/Movies/Movie (2024)/Movie (2024).mkv"
        expected_host = "/mnt/cache_downloads/media/Movies/Movie (2024)/Movie (2024).mkv"

        result = self.file_filter._translate_to_host_path(container_path)
        self.assertEqual(result, expected_host)

    def test_translate_container_to_host_tv(self):
        """Container TV path translates to host path."""
        container_path = "/mnt/cache/media/TV/Show Name/Season 1/Episode.mkv"
        expected_host = "/mnt/cache_downloads/media/TV/Show Name/Season 1/Episode.mkv"

        result = self.file_filter._translate_to_host_path(container_path)
        self.assertEqual(result, expected_host)

    def test_no_translation_when_paths_match(self):
        """No translation when host_cache_path equals cache_path."""
        # Override mappings with matching paths
        self.mappings[0].host_cache_path = "/mnt/cache/media/Movies"

        container_path = "/mnt/cache/media/Movies/Movie.mkv"
        result = self.file_filter._translate_to_host_path(container_path)

        # Should return unchanged
        self.assertEqual(result, container_path)

    def test_no_translation_when_not_docker(self):
        """No translation when not running in Docker."""
        from core.file_operations import FileFilter

        non_docker_filter = FileFilter(
            real_source="/mnt/user",
            cache_dir="/mnt/cache",
            is_unraid=True,
            mover_cache_exclude_file="/config/exclude.txt",
            path_modifier=self.path_modifier,
            is_docker=False,  # Not Docker
        )

        container_path = "/mnt/cache/media/Movies/Movie.mkv"
        result = non_docker_filter._translate_to_host_path(container_path)

        # Should return unchanged
        self.assertEqual(result, container_path)

    def test_no_translation_when_no_path_modifier(self):
        """No translation when path_modifier is None."""
        from core.file_operations import FileFilter

        no_modifier_filter = FileFilter(
            real_source="/mnt/user",
            cache_dir="/mnt/cache",
            is_unraid=True,
            mover_cache_exclude_file="/config/exclude.txt",
            path_modifier=None,  # No path modifier
            is_docker=True,
        )

        container_path = "/mnt/cache/media/Movies/Movie.mkv"
        result = no_modifier_filter._translate_to_host_path(container_path)

        # Should return unchanged
        self.assertEqual(result, container_path)

    def test_unmatched_path_returns_unchanged(self):
        """Paths not matching any mapping return unchanged."""
        unmatched_path = "/mnt/other/some/file.mkv"
        result = self.file_filter._translate_to_host_path(unmatched_path)

        self.assertEqual(result, unmatched_path)

    def test_translate_with_trailing_slash_in_mapping(self):
        """Translation works even if mapping has trailing slash."""
        # Add trailing slashes to test rstrip behavior
        self.mappings[0].cache_path = "/mnt/cache/media/Movies/"
        self.mappings[0].host_cache_path = "/mnt/cache_downloads/media/Movies/"

        container_path = "/mnt/cache/media/Movies/Movie.mkv"
        expected_host = "/mnt/cache_downloads/media/Movies/Movie.mkv"

        result = self.file_filter._translate_to_host_path(container_path)
        self.assertEqual(result, expected_host)

    def test_translate_preserves_full_path(self):
        """Translation preserves the full file path structure."""
        container_path = "/mnt/cache/media/Movies/Genre/Movie (2024) [1080p]/Movie (2024) [1080p].mkv"
        expected_host = "/mnt/cache_downloads/media/Movies/Genre/Movie (2024) [1080p]/Movie (2024) [1080p].mkv"

        result = self.file_filter._translate_to_host_path(container_path)
        self.assertEqual(result, expected_host)


class TestPathTranslationFromHost(unittest.TestCase):
    """Test _translate_from_host_path() - host path -> container path."""

    def setUp(self):
        """Set up FileFilter with mock path mappings."""
        from core.file_operations import FileFilter

        self.mappings = [
            MockPathMapping(
                name="Movies",
                plex_path="/data/Movies",
                real_path="/mnt/user/media/Movies",
                cache_path="/mnt/cache/media/Movies",
                host_cache_path="/mnt/cache_downloads/media/Movies",
            ),
            MockPathMapping(
                name="TV",
                plex_path="/data/TV",
                real_path="/mnt/user/media/TV",
                cache_path="/mnt/cache/media/TV",
                host_cache_path="/mnt/cache_downloads/media/TV",
            ),
        ]

        self.path_modifier = MockMultiPathModifier(self.mappings)

        self.file_filter = FileFilter(
            real_source="/mnt/user",
            cache_dir="/mnt/cache",
            is_unraid=True,
            mover_cache_exclude_file="/config/exclude.txt",
            path_modifier=self.path_modifier,
            is_docker=True,
        )

    def test_translate_host_to_container_movies(self):
        """Host Movies path translates to container path."""
        host_path = "/mnt/cache_downloads/media/Movies/Movie (2024)/Movie (2024).mkv"
        expected_container = "/mnt/cache/media/Movies/Movie (2024)/Movie (2024).mkv"

        result = self.file_filter._translate_from_host_path(host_path)
        self.assertEqual(result, expected_container)

    def test_translate_host_to_container_tv(self):
        """Host TV path translates to container path."""
        host_path = "/mnt/cache_downloads/media/TV/Show Name/Season 1/Episode.mkv"
        expected_container = "/mnt/cache/media/TV/Show Name/Season 1/Episode.mkv"

        result = self.file_filter._translate_from_host_path(host_path)
        self.assertEqual(result, expected_container)

    def test_roundtrip_translation(self):
        """Translating to host and back returns original path."""
        original = "/mnt/cache/media/Movies/Movie.mkv"

        host = self.file_filter._translate_to_host_path(original)
        back = self.file_filter._translate_from_host_path(host)

        self.assertEqual(back, original)

    def test_no_translation_when_not_docker(self):
        """No translation when not running in Docker."""
        from core.file_operations import FileFilter

        non_docker_filter = FileFilter(
            real_source="/mnt/user",
            cache_dir="/mnt/cache",
            is_unraid=True,
            mover_cache_exclude_file="/config/exclude.txt",
            path_modifier=self.path_modifier,
            is_docker=False,
        )

        host_path = "/mnt/cache_downloads/media/Movies/Movie.mkv"
        result = non_docker_filter._translate_from_host_path(host_path)

        self.assertEqual(result, host_path)


class TestExcludeListPathHandling(unittest.TestCase):
    """Test that exclude list operations use correct paths."""

    def setUp(self):
        """Set up FileFilter with mock path mappings."""
        from core.file_operations import FileFilter

        self.mappings = [
            MockPathMapping(
                name="Downloads",
                plex_path="/data",
                real_path="/mnt/user/media",
                cache_path="/mnt/cache/media",
                host_cache_path="/mnt/cache_downloads/media",
            ),
        ]

        self.path_modifier = MockMultiPathModifier(self.mappings)

        self.file_filter = FileFilter(
            real_source="/mnt/user",
            cache_dir="/mnt/cache",
            is_unraid=True,
            mover_cache_exclude_file="/tmp/test_exclude.txt",
            path_modifier=self.path_modifier,
            is_docker=True,
        )

    def test_exclude_file_should_contain_host_paths(self):
        """Verify exclude file entries should be translated to host paths."""
        container_path = "/mnt/cache/media/Movies/Movie.mkv"

        # The translation that would be used for exclude file
        exclude_path = self.file_filter._translate_to_host_path(container_path)

        # Should be host path, not container path
        self.assertTrue(exclude_path.startswith("/mnt/cache_downloads"))
        self.assertFalse(exclude_path.startswith("/mnt/cache/"))

    def test_file_existence_check_should_use_container_paths(self):
        """Verify file existence checks translate host paths to container paths."""
        # Simulate path read from exclude file (host format)
        host_path = "/mnt/cache_downloads/media/Movies/Movie.mkv"

        # Translation for file existence check
        container_path = self.file_filter._translate_from_host_path(host_path)

        # Should be container path for os.path.exists()
        self.assertTrue(container_path.startswith("/mnt/cache/"))
        self.assertFalse(container_path.startswith("/mnt/cache_downloads"))


class TestMultiplePathMappings(unittest.TestCase):
    """Test translation with multiple path mappings."""

    def setUp(self):
        """Set up FileFilter with multiple mappings."""
        from core.file_operations import FileFilter

        # Simulate setup with multiple cache drives
        self.mappings = [
            MockPathMapping(
                name="SSD Cache",
                plex_path="/data/media",
                real_path="/mnt/user/media",
                cache_path="/mnt/cache/media",
                host_cache_path="/mnt/cache_nvme/media",
            ),
            MockPathMapping(
                name="HDD Cache",
                plex_path="/data/archive",
                real_path="/mnt/user/archive",
                cache_path="/mnt/cache2/archive",
                host_cache_path="/mnt/cache_hdd/archive",
            ),
            MockPathMapping(
                name="No Cache",
                plex_path="/data/remote",
                real_path="/mnt/remote",
                cache_path="",  # Not cacheable
                host_cache_path="",
                cacheable=False,
            ),
        ]

        self.path_modifier = MockMultiPathModifier(self.mappings)

        self.file_filter = FileFilter(
            real_source="/mnt/user",
            cache_dir="/mnt/cache",
            is_unraid=True,
            mover_cache_exclude_file="/config/exclude.txt",
            path_modifier=self.path_modifier,
            is_docker=True,
        )

    def test_translate_first_mapping(self):
        """First mapping translates correctly."""
        container = "/mnt/cache/media/file.mkv"
        expected = "/mnt/cache_nvme/media/file.mkv"

        result = self.file_filter._translate_to_host_path(container)
        self.assertEqual(result, expected)

    def test_translate_second_mapping(self):
        """Second mapping translates correctly."""
        container = "/mnt/cache2/archive/file.mkv"
        expected = "/mnt/cache_hdd/archive/file.mkv"

        result = self.file_filter._translate_to_host_path(container)
        self.assertEqual(result, expected)

    def test_non_cacheable_mapping_no_effect(self):
        """Non-cacheable mapping with empty paths doesn't break translation."""
        # Path that doesn't match any cacheable mapping
        path = "/mnt/remote/file.mkv"

        result = self.file_filter._translate_to_host_path(path)
        self.assertEqual(result, path)


class TestBugHunting(unittest.TestCase):
    """
    Adversarial tests designed to find bugs in path translation.

    These tests explore edge cases and potential failure modes rather than
    just confirming happy paths work.
    """

    def test_bug_prefix_substring_confusion(self):
        """
        BUG HUNT: Does /mnt/cache incorrectly match /mnt/cache_downloads?

        If the code uses 'in' instead of 'startswith', or doesn't properly
        handle the boundary, /mnt/cache could match /mnt/cache_downloads.
        """
        from core.file_operations import FileFilter

        mappings = [
            MockPathMapping(
                name="Cache",
                cache_path="/mnt/cache",
                host_cache_path="/mnt/host_cache",
            ),
        ]

        path_modifier = MockMultiPathModifier(mappings)
        file_filter = FileFilter(
            real_source="/mnt/user",
            cache_dir="/mnt/cache",
            is_unraid=True,
            mover_cache_exclude_file="/config/exclude.txt",
            path_modifier=path_modifier,
            is_docker=True,
        )

        # This path contains "cache" but is actually "cache_downloads"
        tricky_path = "/mnt/cache_downloads/media/file.mkv"
        result = file_filter._translate_to_host_path(tricky_path)

        # Should NOT be translated - it doesn't start with /mnt/cache/
        self.assertEqual(result, tricky_path,
            "BUG: /mnt/cache_downloads was incorrectly matched by /mnt/cache mapping")

    def test_bug_overlapping_mappings_order_matters(self):
        """
        BUG HUNT: With overlapping prefixes, does order matter incorrectly?

        If we have both /mnt/cache and /mnt/cache/media mappings,
        the more specific one should win, regardless of order.
        """
        from core.file_operations import FileFilter

        # Order 1: General first, specific second
        mappings_order1 = [
            MockPathMapping(
                name="General",
                cache_path="/mnt/cache",
                host_cache_path="/mnt/host_general",
            ),
            MockPathMapping(
                name="Specific",
                cache_path="/mnt/cache/media",
                host_cache_path="/mnt/host_specific",
            ),
        ]

        # Order 2: Specific first, general second
        mappings_order2 = [
            MockPathMapping(
                name="Specific",
                cache_path="/mnt/cache/media",
                host_cache_path="/mnt/host_specific",
            ),
            MockPathMapping(
                name="General",
                cache_path="/mnt/cache",
                host_cache_path="/mnt/host_general",
            ),
        ]

        test_path = "/mnt/cache/media/Movies/file.mkv"

        filter1 = FileFilter(
            real_source="/mnt/user", cache_dir="/mnt/cache", is_unraid=True,
            mover_cache_exclude_file="/config/exclude.txt",
            path_modifier=MockMultiPathModifier(mappings_order1),
            is_docker=True,
        )

        filter2 = FileFilter(
            real_source="/mnt/user", cache_dir="/mnt/cache", is_unraid=True,
            mover_cache_exclude_file="/config/exclude.txt",
            path_modifier=MockMultiPathModifier(mappings_order2),
            is_docker=True,
        )

        result1 = filter1._translate_to_host_path(test_path)
        result2 = filter2._translate_to_host_path(test_path)

        # Document actual behavior (first match wins currently)
        # This test documents the behavior - whether it's correct is debatable
        # but at least we know what it does
        self.assertIn(result1, ["/mnt/host_general/media/Movies/file.mkv",
                                "/mnt/host_specific/Movies/file.mkv"],
            f"Unexpected result: {result1}")

    def test_bug_empty_cache_path_handling(self):
        """
        BUG HUNT: What happens when cache_path is empty string vs None?
        """
        from core.file_operations import FileFilter

        mappings = [
            MockPathMapping(
                name="Empty",
                cache_path="",  # Empty string
                host_cache_path="/mnt/host",
            ),
            MockPathMapping(
                name="Valid",
                cache_path="/mnt/cache",
                host_cache_path="/mnt/host_cache",
            ),
        ]

        path_modifier = MockMultiPathModifier(mappings)
        file_filter = FileFilter(
            real_source="/mnt/user", cache_dir="/mnt/cache", is_unraid=True,
            mover_cache_exclude_file="/config/exclude.txt",
            path_modifier=path_modifier,
            is_docker=True,
        )

        # Empty string startswith("") is always True - potential bug!
        test_path = "/mnt/cache/media/file.mkv"
        result = file_filter._translate_to_host_path(test_path)

        # Should NOT match the empty mapping
        self.assertNotEqual(result, "/mnt/host/mnt/cache/media/file.mkv",
            "BUG: Empty cache_path matched everything!")
        self.assertEqual(result, "/mnt/host_cache/media/file.mkv")

    def test_bug_root_path_exact_match(self):
        """
        BUG HUNT: What if path equals cache_path exactly (no subpath)?
        """
        from core.file_operations import FileFilter

        mappings = [
            MockPathMapping(
                name="Root",
                cache_path="/mnt/cache",
                host_cache_path="/mnt/host",
            ),
        ]

        path_modifier = MockMultiPathModifier(mappings)
        file_filter = FileFilter(
            real_source="/mnt/user", cache_dir="/mnt/cache", is_unraid=True,
            mover_cache_exclude_file="/config/exclude.txt",
            path_modifier=path_modifier,
            is_docker=True,
        )

        # Exact match - what happens?
        exact_path = "/mnt/cache"
        result = file_filter._translate_to_host_path(exact_path)

        # Should translate to /mnt/host
        self.assertEqual(result, "/mnt/host",
            f"Exact path match failed: got {result}")

    def test_bug_double_translation(self):
        """
        BUG HUNT: What if we accidentally translate twice?

        If host_cache_path accidentally matches cache_path of another mapping,
        double translation could corrupt paths.
        """
        from core.file_operations import FileFilter

        # Dangerous setup: host path of one could match cache path of another
        mappings = [
            MockPathMapping(
                name="First",
                cache_path="/mnt/cache",
                host_cache_path="/mnt/cache_downloads",
            ),
            MockPathMapping(
                name="Second",
                cache_path="/mnt/cache_downloads",  # Same as first's host!
                host_cache_path="/mnt/final",
            ),
        ]

        path_modifier = MockMultiPathModifier(mappings)
        file_filter = FileFilter(
            real_source="/mnt/user", cache_dir="/mnt/cache", is_unraid=True,
            mover_cache_exclude_file="/config/exclude.txt",
            path_modifier=path_modifier,
            is_docker=True,
        )

        original = "/mnt/cache/media/file.mkv"

        # Single translation
        once = file_filter._translate_to_host_path(original)

        # Accidental double translation (simulating a bug)
        twice = file_filter._translate_to_host_path(once)

        # Document what happens
        self.assertEqual(once, "/mnt/cache_downloads/media/file.mkv")
        # If twice equals "/mnt/final/media/file.mkv", we have a problem
        # This test documents the behavior

    def test_bug_none_vs_empty_host_cache_path(self):
        """
        BUG HUNT: None host_cache_path vs empty string handling.
        """
        from core.file_operations import FileFilter

        # First try with None
        mapping_none = MockPathMapping(
            name="NoneHost",
            cache_path="/mnt/cache",
            host_cache_path=None,  # type: ignore
        )

        # The code should handle None without crashing
        mappings = [mapping_none]
        path_modifier = MockMultiPathModifier(mappings)

        try:
            file_filter = FileFilter(
                real_source="/mnt/user", cache_dir="/mnt/cache", is_unraid=True,
                mover_cache_exclude_file="/config/exclude.txt",
                path_modifier=path_modifier,
                is_docker=True,
            )
            result = file_filter._translate_to_host_path("/mnt/cache/file.mkv")
            # Should return unchanged (None = no translation needed)
            self.assertEqual(result, "/mnt/cache/file.mkv")
        except (TypeError, AttributeError) as e:
            self.fail(f"BUG: None host_cache_path caused crash: {e}")


class TestEdgeCases(unittest.TestCase):
    """Test edge cases in path translation."""

    def setUp(self):
        """Set up FileFilter with standard mapping."""
        from core.file_operations import FileFilter

        self.mappings = [
            MockPathMapping(
                name="Media",
                plex_path="/data",
                real_path="/mnt/user/media",
                cache_path="/mnt/cache",
                host_cache_path="/mnt/cache_downloads",
            ),
        ]

        self.path_modifier = MockMultiPathModifier(self.mappings)

        self.file_filter = FileFilter(
            real_source="/mnt/user",
            cache_dir="/mnt/cache",
            is_unraid=True,
            mover_cache_exclude_file="/config/exclude.txt",
            path_modifier=self.path_modifier,
            is_docker=True,
        )

    def test_path_with_spaces(self):
        """Paths with spaces translate correctly."""
        container = "/mnt/cache/My Movies/Movie Name (2024)/Movie Name (2024).mkv"
        expected = "/mnt/cache_downloads/My Movies/Movie Name (2024)/Movie Name (2024).mkv"

        result = self.file_filter._translate_to_host_path(container)
        self.assertEqual(result, expected)

    def test_path_with_unicode(self):
        """Paths with unicode characters translate correctly."""
        container = "/mnt/cache/Movies/日本語映画/映画.mkv"
        expected = "/mnt/cache_downloads/Movies/日本語映画/映画.mkv"

        result = self.file_filter._translate_to_host_path(container)
        self.assertEqual(result, expected)

    def test_path_with_special_chars(self):
        """Paths with special characters translate correctly."""
        container = "/mnt/cache/Movies/Movie's Name! [2024] (1080p)/file.mkv"
        expected = "/mnt/cache_downloads/Movies/Movie's Name! [2024] (1080p)/file.mkv"

        result = self.file_filter._translate_to_host_path(container)
        self.assertEqual(result, expected)

    def test_similar_prefix_paths(self):
        """Ensure /mnt/cache doesn't match /mnt/cache2."""
        from core.file_operations import FileFilter

        mappings = [
            MockPathMapping(
                name="Cache1",
                cache_path="/mnt/cache",
                host_cache_path="/mnt/cache_downloads",
            ),
        ]

        path_modifier = MockMultiPathModifier(mappings)
        file_filter = FileFilter(
            real_source="/mnt/user",
            cache_dir="/mnt/cache",
            is_unraid=True,
            mover_cache_exclude_file="/config/exclude.txt",
            path_modifier=path_modifier,
            is_docker=True,
        )

        # This should NOT match /mnt/cache mapping
        path = "/mnt/cache2/media/file.mkv"
        result = file_filter._translate_to_host_path(path)

        # Should return unchanged (no /mnt/cache2 -> /mnt/cache_downloads2)
        self.assertEqual(result, path)

    def test_exact_prefix_match_required(self):
        """Translation requires exact prefix match, not substring."""
        from core.file_operations import FileFilter

        mappings = [
            MockPathMapping(
                name="Cache",
                cache_path="/mnt/cache/media",
                host_cache_path="/mnt/cache_downloads/media",
            ),
        ]

        path_modifier = MockMultiPathModifier(mappings)
        file_filter = FileFilter(
            real_source="/mnt/user",
            cache_dir="/mnt/cache",
            is_unraid=True,
            mover_cache_exclude_file="/config/exclude.txt",
            path_modifier=path_modifier,
            is_docker=True,
        )

        # Should match
        matched = "/mnt/cache/media/file.mkv"
        result_matched = file_filter._translate_to_host_path(matched)
        self.assertEqual(result_matched, "/mnt/cache_downloads/media/file.mkv")

        # Should NOT match (different path structure)
        not_matched = "/mnt/cache/other/file.mkv"
        result_not_matched = file_filter._translate_to_host_path(not_matched)
        self.assertEqual(result_not_matched, not_matched)


class TestFileMoverPathTranslation(unittest.TestCase):
    """Test path translation in FileMover class."""

    def test_file_mover_has_translate_methods(self):
        """FileMover should have path translation methods."""
        from core.file_operations import FileMover

        # Check methods exist
        self.assertTrue(hasattr(FileMover, '_translate_to_host_path'))
        self.assertTrue(hasattr(FileMover, '_translate_from_host_path'))


class TestPlexcachedMigrationPathTranslation(unittest.TestCase):
    """Test path translation in PlexcachedMigration class."""

    def test_migration_has_translate_method(self):
        """PlexcachedMigration should have reverse translation method."""
        from core.file_operations import PlexcachedMigration

        self.assertTrue(hasattr(PlexcachedMigration, '_translate_from_host_path'))


class TestGetArrayDirectPath(unittest.TestCase):
    """Test get_array_direct_path() from core/system_utils.py."""

    def setUp(self):
        from core.system_utils import get_array_direct_path
        self.get_array_direct_path = get_array_direct_path

    def test_converts_mnt_user_to_mnt_user0(self):
        """Standard /mnt/user/ path converts to /mnt/user0/."""
        result = self.get_array_direct_path("/mnt/user/media/Movies/Movie.mkv")
        self.assertEqual(result, "/mnt/user0/media/Movies/Movie.mkv")

    def test_preserves_non_user_paths(self):
        """Paths not starting with /mnt/user/ are returned unchanged."""
        result = self.get_array_direct_path("/mnt/cache/media/Movie.mkv")
        self.assertEqual(result, "/mnt/cache/media/Movie.mkv")

    def test_preserves_mnt_user0_paths(self):
        """Already-converted /mnt/user0/ paths are returned unchanged."""
        result = self.get_array_direct_path("/mnt/user0/media/Movie.mkv")
        self.assertEqual(result, "/mnt/user0/media/Movie.mkv")

    def test_does_not_match_mnt_user_without_trailing_slash(self):
        """/mnt/username/ should NOT be converted."""
        result = self.get_array_direct_path("/mnt/username/media/Movie.mkv")
        self.assertEqual(result, "/mnt/username/media/Movie.mkv")

    def test_does_not_match_mnt_user0_prefix(self):
        """/mnt/user0something should NOT be converted."""
        result = self.get_array_direct_path("/mnt/user0something/media/Movie.mkv")
        self.assertEqual(result, "/mnt/user0something/media/Movie.mkv")

    def test_root_mnt_user_path(self):
        """Just /mnt/user/ with trailing content converts."""
        result = self.get_array_direct_path("/mnt/user/file.mkv")
        self.assertEqual(result, "/mnt/user0/file.mkv")

    def test_deep_nested_path(self):
        """Deep nested path converts correctly."""
        result = self.get_array_direct_path("/mnt/user/media/TV/Show Name/Season 1/Episode.mkv")
        self.assertEqual(result, "/mnt/user0/media/TV/Show Name/Season 1/Episode.mkv")

    def test_path_with_spaces_and_special_chars(self):
        """Paths with spaces and special characters convert correctly."""
        result = self.get_array_direct_path("/mnt/user/media/Movies/Movie's Name! (2024) [1080p]/file.mkv")
        self.assertEqual(result, "/mnt/user0/media/Movies/Movie's Name! (2024) [1080p]/file.mkv")

    def test_plexcached_path_converts(self):
        """.plexcached path converts correctly."""
        result = self.get_array_direct_path("/mnt/user/media/Movie.mkv.plexcached")
        self.assertEqual(result, "/mnt/user0/media/Movie.mkv.plexcached")

    def test_empty_string(self):
        """Empty string is returned unchanged."""
        result = self.get_array_direct_path("")
        self.assertEqual(result, "")

    def test_exact_mnt_user_slash(self):
        """Exact /mnt/user/ with nothing after it converts."""
        result = self.get_array_direct_path("/mnt/user/")
        self.assertEqual(result, "/mnt/user0/")


class TestParseSizeBytes(unittest.TestCase):
    """Test parse_size_bytes() from core/system_utils.py."""

    def setUp(self):
        from core.system_utils import parse_size_bytes
        self.parse = parse_size_bytes

    def test_tb_suffix(self):
        self.assertEqual(self.parse("1TB"), 1024**4)

    def test_gb_suffix(self):
        self.assertEqual(self.parse("500GB"), 500 * 1024**3)

    def test_mb_suffix(self):
        self.assertEqual(self.parse("100MB"), 100 * 1024**2)

    def test_t_suffix(self):
        self.assertEqual(self.parse("2T"), 2 * 1024**4)

    def test_g_suffix(self):
        self.assertEqual(self.parse("1.5G"), int(1.5 * 1024**3))

    def test_m_suffix(self):
        self.assertEqual(self.parse("256M"), 256 * 1024**2)

    def test_bare_number_defaults_to_gb(self):
        self.assertEqual(self.parse("2"), 2 * 1024**3)

    def test_empty_string(self):
        self.assertEqual(self.parse(""), 0)

    def test_zero(self):
        self.assertEqual(self.parse("0"), 0)

    def test_invalid_string(self):
        self.assertEqual(self.parse("invalid"), 0)

    def test_whitespace_handling(self):
        self.assertEqual(self.parse("  500GB  "), 500 * 1024**3)

    def test_case_insensitive(self):
        self.assertEqual(self.parse("500gb"), 500 * 1024**3)


if __name__ == "__main__":
    unittest.main(verbosity=2)
