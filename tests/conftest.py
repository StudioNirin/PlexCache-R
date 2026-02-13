"""Shared test fixtures for PlexCache-R test suite."""

import os
import sys
import json
import shutil
import tempfile
from unittest.mock import MagicMock
from dataclasses import dataclass
from typing import Optional

import pytest

# Mock fcntl for Windows compatibility before any imports
sys.modules['fcntl'] = MagicMock()

# Mock apscheduler (optional dependency, not installed in test environment)
for _mod in [
    'apscheduler',
    'apscheduler.schedulers',
    'apscheduler.schedulers.background',
    'apscheduler.triggers',
    'apscheduler.triggers.cron',
    'apscheduler.triggers.interval',
]:
    sys.modules.setdefault(_mod, MagicMock())

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ============================================================================
# Mock dataclasses
# ============================================================================

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


# ============================================================================
# Filesystem fixtures
# ============================================================================

@pytest.fixture
def temp_dir():
    """Provide a temporary directory, cleaned up after test."""
    d = tempfile.mkdtemp(prefix="plexcache_test_")
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def cache_array_dirs(temp_dir):
    """Provide mock cache and array directory structure."""
    cache_dir = os.path.join(temp_dir, "mnt", "cache")
    array_dir = os.path.join(temp_dir, "mnt", "user")
    array_direct_dir = os.path.join(temp_dir, "mnt", "user0")
    os.makedirs(cache_dir)
    os.makedirs(array_dir)
    os.makedirs(array_direct_dir)
    return {
        "root": temp_dir,
        "cache": cache_dir,
        "array": array_dir,
        "array_direct": array_direct_dir,
    }


def create_test_file(path, content="test content", size_bytes=None):
    """Create a test file with given content or specific size.

    Args:
        path: Full path to create file at.
        content: Text content to write (ignored if size_bytes set).
        size_bytes: If set, create file of exactly this size.

    Returns:
        The path of the created file.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if size_bytes is not None:
        with open(path, 'wb') as f:
            f.write(b'\x00' * size_bytes)
    else:
        with open(path, 'w') as f:
            f.write(content)
    return path


# ============================================================================
# Config fixtures
# ============================================================================

@pytest.fixture
def default_path_mappings():
    """Provide standard Docker path mappings for testing."""
    return [
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


@pytest.fixture
def mock_path_modifier(default_path_mappings):
    """Provide a MockMultiPathModifier with default mappings."""
    return MockMultiPathModifier(default_path_mappings)


# ============================================================================
# Tracker fixtures
# ============================================================================

@pytest.fixture
def timestamps_file(temp_dir):
    """Provide a temporary timestamps.json file path."""
    return os.path.join(temp_dir, "timestamps.json")


@pytest.fixture
def tracker_file(temp_dir):
    """Provide a temporary tracker JSON file path."""
    return os.path.join(temp_dir, "tracker.json")


@pytest.fixture
def sample_timestamps():
    """Provide sample timestamp data."""
    return {
        "/mnt/cache/media/Movies/Movie1.mkv": {
            "cached_at": "2026-01-15T10:00:00",
            "source": "ondeck",
        },
        "/mnt/cache/media/TV/Show/S01E01.mkv": {
            "cached_at": "2026-01-10T08:00:00",
            "source": "watchlist",
        },
        "/mnt/cache/media/Movies/Movie2.mkv": {
            "cached_at": "2026-01-20T14:00:00",
            "source": "ondeck",
        },
    }


@pytest.fixture
def sample_ondeck_data():
    """Provide sample OnDeck tracker data."""
    return {
        "/mnt/cache/media/TV/Show/S01E01.mkv": {
            "first_seen": "2026-01-15T10:00:00",
            "last_seen": "2026-02-01T10:00:00",
            "users": ["user1", "user2"],
            "title": "Show - S01E01",
        },
    }


@pytest.fixture
def sample_watchlist_data():
    """Provide sample Watchlist tracker data."""
    return {
        "/mnt/cache/media/Movies/Movie1.mkv": {
            "first_seen": "2026-01-10T10:00:00",
            "last_seen": "2026-02-01T10:00:00",
            "users": ["user1"],
            "title": "Movie 1",
        },
    }
