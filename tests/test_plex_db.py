"""Tests for core.plex_db — Plex SQLite database direct read fallback."""

import os
import sys
import sqlite3
import tempfile
from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pytest

# Mock fcntl for Windows compatibility before any imports
sys.modules.setdefault('fcntl', MagicMock())
for _mod in [
    'apscheduler', 'apscheduler.schedulers', 'apscheduler.schedulers.background',
    'apscheduler.triggers', 'apscheduler.triggers.cron', 'apscheduler.triggers.interval',
]:
    sys.modules.setdefault(_mod, MagicMock())

# Mock plexapi modules so plex_api.py can be imported on systems without plexapi
for _mod in [
    'plexapi', 'plexapi.server', 'plexapi.video', 'plexapi.myplex',
    'plexapi.exceptions',
]:
    sys.modules.setdefault(_mod, MagicMock())

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.plex_db import (
    fetch_on_deck_from_db,
    _connect,
    _resolve_account_ids,
    _fetch_tv_on_deck,
    _fetch_movie_on_deck,
    _resolve_file_path,
)


# ============================================================================
# Fixtures
# ============================================================================

def _create_schema(conn):
    """Create the Plex database schema (relevant tables only)."""
    conn.executescript("""
        CREATE TABLE accounts (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        );

        CREATE TABLE metadata_items (
            id INTEGER PRIMARY KEY,
            metadata_type INTEGER NOT NULL,
            title TEXT NOT NULL,
            parent_id INTEGER,
            "index" INTEGER,
            guid TEXT,
            library_section_id INTEGER,
            duration INTEGER
        );

        CREATE TABLE metadata_item_views (
            id INTEGER PRIMARY KEY,
            account_id INTEGER NOT NULL,
            grandparent_title TEXT,
            parent_index INTEGER,
            "index" INTEGER,
            title TEXT,
            viewed_at TEXT,
            library_section_id INTEGER
        );

        CREATE TABLE metadata_item_settings (
            id INTEGER PRIMARY KEY,
            account_id INTEGER NOT NULL,
            guid TEXT,
            view_offset INTEGER DEFAULT 0,
            view_count INTEGER DEFAULT 0,
            last_viewed_at TEXT
        );

        CREATE TABLE media_items (
            id INTEGER PRIMARY KEY,
            metadata_item_id INTEGER NOT NULL
        );

        CREATE TABLE media_parts (
            id INTEGER PRIMARY KEY,
            media_item_id INTEGER NOT NULL,
            file TEXT NOT NULL
        );
    """)


def _populate_tv_data(conn):
    """Populate test data for a TV show: 'Test Show' with 2 seasons."""
    # Accounts
    conn.execute("INSERT INTO accounts (id, name) VALUES (100, 'SharedUser')")
    conn.execute("INSERT INTO accounts (id, name) VALUES (200, 'AnotherUser')")

    # Show -> Season -> Episode hierarchy
    # Show (metadata_type=2)
    conn.execute("INSERT INTO metadata_items (id, metadata_type, title, parent_id, \"index\", guid, library_section_id) VALUES (1, 2, 'Test Show', NULL, NULL, 'plex://show/1', 1)")

    # Season 1 (metadata_type=3)
    conn.execute("INSERT INTO metadata_items (id, metadata_type, title, parent_id, \"index\", guid, library_section_id) VALUES (10, 3, 'Season 1', 1, 1, 'plex://season/10', 1)")
    # Season 2
    conn.execute("INSERT INTO metadata_items (id, metadata_type, title, parent_id, \"index\", guid, library_section_id) VALUES (20, 3, 'Season 2', 1, 2, 'plex://season/20', 1)")

    # Season 1 episodes (metadata_type=4)
    for ep in range(1, 11):  # S01E01-S01E10
        ep_id = 100 + ep
        conn.execute(
            "INSERT INTO metadata_items (id, metadata_type, title, parent_id, \"index\", guid, library_section_id) VALUES (?, 4, ?, 10, ?, ?, 1)",
            (ep_id, f"S01E{ep:02d}", ep, f"plex://episode/{ep_id}")
        )
        # Media items + parts
        conn.execute("INSERT INTO media_items (id, metadata_item_id) VALUES (?, ?)", (ep_id + 1000, ep_id))
        conn.execute("INSERT INTO media_parts (id, media_item_id, file) VALUES (?, ?, ?)",
                     (ep_id + 2000, ep_id + 1000, f"/data/TV/Test Show/Season 1/S01E{ep:02d}.mkv"))

    # Season 2 episodes
    for ep in range(1, 6):  # S02E01-S02E05
        ep_id = 200 + ep
        conn.execute(
            "INSERT INTO metadata_items (id, metadata_type, title, parent_id, \"index\", guid, library_section_id) VALUES (?, 4, ?, 20, ?, ?, 1)",
            (ep_id, f"S02E{ep:02d}", ep, f"plex://episode/{ep_id}")
        )
        conn.execute("INSERT INTO media_items (id, metadata_item_id) VALUES (?, ?)", (ep_id + 1000, ep_id))
        conn.execute("INSERT INTO media_parts (id, media_item_id, file) VALUES (?, ?, ?)",
                     (ep_id + 2000, ep_id + 1000, f"/data/TV/Test Show/Season 2/S02E{ep:02d}.mkv"))

    conn.commit()


def _populate_movie_data(conn):
    """Populate test data for movies."""
    # Movie (metadata_type=1)
    conn.execute("INSERT INTO metadata_items (id, metadata_type, title, parent_id, \"index\", guid, library_section_id) VALUES (500, 1, 'Half Watched Movie', NULL, NULL, 'plex://movie/500', 2)")
    conn.execute("INSERT INTO metadata_items (id, metadata_type, title, parent_id, \"index\", guid, library_section_id) VALUES (501, 1, 'Fully Watched Movie', NULL, NULL, 'plex://movie/501', 2)")
    conn.execute("INSERT INTO metadata_items (id, metadata_type, title, parent_id, \"index\", guid, library_section_id) VALUES (502, 1, 'Unwatched Movie', NULL, NULL, 'plex://movie/502', 2)")

    # Media items + parts for movies
    for mid in [500, 501, 502]:
        conn.execute("INSERT INTO media_items (id, metadata_item_id) VALUES (?, ?)", (mid + 1000, mid))
        conn.execute("INSERT INTO media_parts (id, media_item_id, file) VALUES (?, ?, ?)",
                     (mid + 2000, mid + 1000, f"/data/Movies/Movie {mid}.mkv"))

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Partially watched (should appear on OnDeck)
    conn.execute("INSERT INTO metadata_item_settings (account_id, guid, view_offset, view_count, last_viewed_at) VALUES (100, 'plex://movie/500', 45000, 0, ?)", (now,))
    # Fully watched (should NOT appear)
    conn.execute("INSERT INTO metadata_item_settings (account_id, guid, view_offset, view_count, last_viewed_at) VALUES (100, 'plex://movie/501', 90000, 1, ?)", (now,))
    # Unwatched (no settings entry — should NOT appear)

    conn.commit()


@pytest.fixture
def db_path(tmp_path):
    """Create a temporary Plex-like database with test data."""
    path = str(tmp_path / "com.plexapp.plugins.library.db")
    conn = sqlite3.connect(path)
    _create_schema(conn)
    _populate_tv_data(conn)
    _populate_movie_data(conn)
    conn.close()
    return path


@pytest.fixture
def db_conn(db_path):
    """Open a connection to the test database."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()


# ============================================================================
# Tests
# ============================================================================

class TestAccountIdResolution:
    """Test _resolve_account_ids with pre-mapped IDs and DB fallback."""

    def test_pre_mapped_ids(self, db_conn):
        """Pre-mapped IDs from settings take priority over DB lookup."""
        user_id_map = {"SharedUser": 100}
        result = _resolve_account_ids(db_conn, ["SharedUser"], user_id_map)
        assert result == {"SharedUser": 100}

    def test_db_fallback_lookup(self, db_conn):
        """Falls back to DB accounts table when no pre-mapped ID."""
        result = _resolve_account_ids(db_conn, ["SharedUser"], {})
        assert result == {"SharedUser": 100}

    def test_case_insensitive_match(self, db_conn):
        """Account name matching is case-insensitive."""
        result = _resolve_account_ids(db_conn, ["shareduser"], {})
        assert "shareduser" in result
        assert result["shareduser"] == 100

    def test_unknown_user(self, db_conn):
        """Unknown usernames are not in the result."""
        result = _resolve_account_ids(db_conn, ["NonExistentUser"], {})
        assert "NonExistentUser" not in result


class TestTvOnDeck:
    """Test TV next-episode resolution from the database."""

    def test_basic_next_episode(self, db_conn):
        """Watched S01E03 → returns S01E04 as OnDeck + prefetch."""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        db_conn.execute(
            "INSERT INTO metadata_item_views (account_id, grandparent_title, parent_index, \"index\", title, viewed_at, library_section_id) VALUES (100, 'Test Show', 1, 3, 'S01E03', ?, 1)",
            (now,)
        )
        db_conn.commit()

        cutoff = datetime.now() - timedelta(days=30)
        items = _fetch_tv_on_deck(db_conn, 100, "SharedUser", [1], cutoff, 3)

        assert len(items) == 4  # S01E04 (ondeck) + S01E05, S01E06, S01E07 (prefetch)
        assert items[0].file_path == "/data/TV/Test Show/Season 1/S01E04.mkv"
        assert items[0].is_current_ondeck is True
        assert items[0].episode_info == {'show': 'Test Show', 'season': 1, 'episode': 4}
        assert items[0].username == "SharedUser"

        # Prefetch episodes
        assert items[1].is_current_ondeck is False
        assert items[1].episode_info['episode'] == 5
        assert items[2].episode_info['episode'] == 6
        assert items[3].episode_info['episode'] == 7

    def test_season_boundary(self, db_conn):
        """Watched S01E10 (last of season) → returns S02E01."""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        db_conn.execute(
            "INSERT INTO metadata_item_views (account_id, grandparent_title, parent_index, \"index\", title, viewed_at, library_section_id) VALUES (100, 'Test Show', 1, 10, 'S01E10', ?, 1)",
            (now,)
        )
        db_conn.commit()

        cutoff = datetime.now() - timedelta(days=30)
        items = _fetch_tv_on_deck(db_conn, 100, "SharedUser", [1], cutoff, 2)

        assert len(items) == 3  # S02E01 + S02E02 + S02E03
        assert items[0].file_path == "/data/TV/Test Show/Season 2/S02E01.mkv"
        assert items[0].is_current_ondeck is True
        assert items[0].episode_info == {'show': 'Test Show', 'season': 2, 'episode': 1}

    def test_caught_up(self, db_conn):
        """Watched last episode of show → returns nothing."""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        db_conn.execute(
            "INSERT INTO metadata_item_views (account_id, grandparent_title, parent_index, \"index\", title, viewed_at, library_section_id) VALUES (100, 'Test Show', 2, 5, 'S02E05', ?, 1)",
            (now,)
        )
        db_conn.commit()

        cutoff = datetime.now() - timedelta(days=30)
        items = _fetch_tv_on_deck(db_conn, 100, "SharedUser", [1], cutoff, 3)

        assert len(items) == 0

    def test_days_to_monitor_filter(self, db_conn):
        """Views older than days_to_monitor are excluded."""
        old_date = (datetime.now() - timedelta(days=200)).strftime("%Y-%m-%d %H:%M:%S")
        db_conn.execute(
            "INSERT INTO metadata_item_views (account_id, grandparent_title, parent_index, \"index\", title, viewed_at, library_section_id) VALUES (100, 'Test Show', 1, 3, 'S01E03', ?, 1)",
            (old_date,)
        )
        db_conn.commit()

        cutoff = datetime.now() - timedelta(days=30)
        items = _fetch_tv_on_deck(db_conn, 100, "SharedUser", [1], cutoff, 3)

        assert len(items) == 0

    def test_valid_sections_filter(self, db_conn):
        """Items in excluded sections are not returned."""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        db_conn.execute(
            "INSERT INTO metadata_item_views (account_id, grandparent_title, parent_index, \"index\", title, viewed_at, library_section_id) VALUES (100, 'Test Show', 1, 3, 'S01E03', ?, 1)",
            (now,)
        )
        db_conn.commit()

        cutoff = datetime.now() - timedelta(days=30)
        # Section 99 doesn't match the show's section (1)
        items = _fetch_tv_on_deck(db_conn, 100, "SharedUser", [99], cutoff, 3)

        assert len(items) == 0

    def test_empty_valid_sections(self, db_conn):
        """Empty valid_sections returns nothing."""
        cutoff = datetime.now() - timedelta(days=30)
        items = _fetch_tv_on_deck(db_conn, 100, "SharedUser", [], cutoff, 3)
        assert len(items) == 0


def _set_episode_durations(conn, season, durations_min):
    """Set duration_ms on consecutive episodes of a season starting at index 1.

    `durations_min` is a list of per-episode runtime in minutes. None means leave
    the column NULL (simulates missing metadata).
    """
    base_id = {1: 100, 2: 200}[season]
    for ep_idx, minutes in enumerate(durations_min, start=1):
        ep_id = base_id + ep_idx
        ms = None if minutes is None else int(minutes * 60_000)
        conn.execute("UPDATE metadata_items SET duration = ? WHERE id = ?", (ms, ep_id))
    conn.commit()


class TestPrefetchMinimumMinutes:
    """Test duration-based prefetch (prefetch_minimum_minutes) on the DB path."""

    def test_extends_buffer_when_runtime_target_not_met(self, db_conn):
        """Short episodes: buffer is extended past number_episodes to meet runtime."""
        # All 10 S01 episodes are 25 min long.
        _set_episode_durations(db_conn, season=1, durations_min=[25] * 10)

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        db_conn.execute(
            "INSERT INTO metadata_item_views (account_id, grandparent_title, parent_index, \"index\", title, viewed_at, library_section_id) VALUES (100, 'Test Show', 1, 1, 'S01E01', ?, 1)",
            (now,)
        )
        db_conn.commit()

        cutoff = datetime.now() - timedelta(days=30)
        # number_episodes=3, prefetch=120 min: buffer must cover ≥120 min.
        # 3 × 25 = 75 < 120 → +1 → 100 < 120 → +1 → 125 ≥ 120 → 5 buffer eps.
        items = _fetch_tv_on_deck(db_conn, 100, "SharedUser", [1], cutoff, 3,
                                   prefetch_minimum_minutes=120)

        # 1 OnDeck (S01E02) + 5 prefetch (E03-E07)
        assert len(items) == 6
        assert items[0].is_current_ondeck is True
        assert items[0].episode_info["episode"] == 2
        assert [i.episode_info["episode"] for i in items[1:]] == [3, 4, 5, 6, 7]

    def test_count_dominates_when_episodes_long(self, db_conn):
        """Long episodes: number_episodes is the limiter (count > runtime need)."""
        # 60-min episodes — 3 of them already exceed any reasonable runtime target.
        _set_episode_durations(db_conn, season=1, durations_min=[60] * 10)

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        db_conn.execute(
            "INSERT INTO metadata_item_views (account_id, grandparent_title, parent_index, \"index\", title, viewed_at, library_section_id) VALUES (100, 'Test Show', 1, 1, 'S01E01', ?, 1)",
            (now,)
        )
        db_conn.commit()

        cutoff = datetime.now() - timedelta(days=30)
        # 3 × 60 = 180 ≥ 120 → 3 buffer eps, count is the limiter.
        items = _fetch_tv_on_deck(db_conn, 100, "SharedUser", [1], cutoff, 3,
                                   prefetch_minimum_minutes=120)

        assert len(items) == 4  # 1 OnDeck + 3 prefetch
        assert [i.episode_info["episode"] for i in items[1:]] == [3, 4, 5]

    def test_null_duration_falls_back_safely(self, db_conn):
        """Missing duration metadata: 45-min fallback applies, buffer leans larger."""
        # All durations NULL — fallback (45 min/ep) drives the count.
        _set_episode_durations(db_conn, season=1, durations_min=[None] * 10)

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        db_conn.execute(
            "INSERT INTO metadata_item_views (account_id, grandparent_title, parent_index, \"index\", title, viewed_at, library_section_id) VALUES (100, 'Test Show', 1, 1, 'S01E01', ?, 1)",
            (now,)
        )
        db_conn.commit()

        cutoff = datetime.now() - timedelta(days=30)
        # 1 × 45 = 45 < 100 → +1 → 90 < 100 → +1 → 135 ≥ 100 → 3 buffer eps.
        items = _fetch_tv_on_deck(db_conn, 100, "SharedUser", [1], cutoff, 1,
                                   prefetch_minimum_minutes=100)

        assert len(items) == 4  # 1 OnDeck + 3 prefetch (driven by fallback)
        # Bias is toward "one too many", never too few.
        total_assumed = 45 * (len(items) - 1)
        assert total_assumed >= 100


class TestMovieOnDeck:
    """Test partially watched movie detection from the database."""

    def test_partially_watched_movie(self, db_conn):
        """Partially watched movie (view_offset > 0, view_count = 0) appears on OnDeck."""
        cutoff = datetime.now() - timedelta(days=30)
        items = _fetch_movie_on_deck(db_conn, 100, "SharedUser", [2], cutoff)

        assert len(items) == 1
        assert items[0].file_path == "/data/Movies/Movie 500.mkv"
        assert items[0].is_current_ondeck is True
        assert items[0].episode_info is None
        assert items[0].username == "SharedUser"

    def test_fully_watched_excluded(self, db_conn):
        """Fully watched movie (view_count > 0) is excluded."""
        cutoff = datetime.now() - timedelta(days=30)
        items = _fetch_movie_on_deck(db_conn, 100, "SharedUser", [2], cutoff)

        # Only the partially watched movie should be returned
        file_paths = [item.file_path for item in items]
        assert "/data/Movies/Movie 501.mkv" not in file_paths

    def test_wrong_section_excluded(self, db_conn):
        """Movies in sections not in valid_sections are excluded."""
        cutoff = datetime.now() - timedelta(days=30)
        items = _fetch_movie_on_deck(db_conn, 100, "SharedUser", [1], cutoff)  # Section 1 = TV only
        assert len(items) == 0


class TestFilePathResolution:
    """Test metadata_item_id → file path resolution."""

    def test_resolve_existing(self, db_conn):
        """Resolve a known metadata item to its file path."""
        path = _resolve_file_path(db_conn, 101)  # S01E01
        assert path == "/data/TV/Test Show/Season 1/S01E01.mkv"

    def test_resolve_nonexistent(self, db_conn):
        """Non-existent metadata item returns None."""
        path = _resolve_file_path(db_conn, 99999)
        assert path is None


class TestFetchOnDeckFromDb:
    """Integration tests for the main public function."""

    def test_full_flow(self, db_path):
        """Full end-to-end: fetch TV + movie OnDeck for a shared user."""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        conn = sqlite3.connect(db_path)
        conn.execute(
            "INSERT INTO metadata_item_views (account_id, grandparent_title, parent_index, \"index\", title, viewed_at, library_section_id) VALUES (100, 'Test Show', 1, 5, 'S01E05', ?, 1)",
            (now,)
        )
        conn.commit()
        conn.close()

        items = fetch_on_deck_from_db(
            db_path=db_path,
            usernames=["SharedUser"],
            valid_sections=[1, 2],
            days_to_monitor=30,
            number_episodes=2,
            user_id_map={"SharedUser": 100}
        )

        # Should have TV items (S01E06 + 2 prefetch) + 1 movie
        tv_items = [i for i in items if i.episode_info is not None]
        movie_items = [i for i in items if i.episode_info is None]

        assert len(tv_items) == 3  # S01E06, S01E07, S01E08
        assert len(movie_items) == 1
        assert tv_items[0].file_path == "/data/TV/Test Show/Season 1/S01E06.mkv"
        assert movie_items[0].file_path == "/data/Movies/Movie 500.mkv"

    def test_missing_db_file(self):
        """Missing database file returns empty list with no crash."""
        items = fetch_on_deck_from_db(
            db_path="/nonexistent/path/db.sqlite",
            usernames=["SharedUser"],
            valid_sections=[1],
            days_to_monitor=30,
            number_episodes=3,
            user_id_map={"SharedUser": 100}
        )
        assert items == []

    def test_empty_db_path(self):
        """Empty db_path returns empty list immediately."""
        items = fetch_on_deck_from_db(
            db_path="",
            usernames=["SharedUser"],
            valid_sections=[1],
            days_to_monitor=30,
            number_episodes=3,
            user_id_map={}
        )
        assert items == []

    def test_unresolvable_user(self, db_path):
        """User with no account ID mapping returns empty list."""
        items = fetch_on_deck_from_db(
            db_path=db_path,
            usernames=["GhostUser"],
            valid_sections=[1],
            days_to_monitor=30,
            number_episodes=3,
            user_id_map={}
        )
        assert items == []

    def test_multiple_users(self, db_path):
        """Fetch OnDeck for multiple users in one call."""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        conn = sqlite3.connect(db_path)
        # SharedUser watched S01E03
        conn.execute(
            "INSERT INTO metadata_item_views (account_id, grandparent_title, parent_index, \"index\", title, viewed_at, library_section_id) VALUES (100, 'Test Show', 1, 3, 'S01E03', ?, 1)",
            (now,)
        )
        # AnotherUser watched S02E02
        conn.execute(
            "INSERT INTO metadata_item_views (account_id, grandparent_title, parent_index, \"index\", title, viewed_at, library_section_id) VALUES (200, 'Test Show', 2, 2, 'S02E02', ?, 1)",
            (now,)
        )
        conn.commit()
        conn.close()

        items = fetch_on_deck_from_db(
            db_path=db_path,
            usernames=["SharedUser", "AnotherUser"],
            valid_sections=[1, 2],
            days_to_monitor=30,
            number_episodes=1,
            user_id_map={"SharedUser": 100, "AnotherUser": 200}
        )

        shared_items = [i for i in items if i.username == "SharedUser"]
        another_items = [i for i in items if i.username == "AnotherUser"]

        # SharedUser: S01E04 + 1 prefetch + 1 movie
        assert any(i.episode_info and i.episode_info['episode'] == 4 for i in shared_items)
        # AnotherUser: S02E03 + 1 prefetch (no movie settings for this user)
        assert any(i.episode_info and i.episode_info['episode'] == 3 and i.episode_info['season'] == 2 for i in another_items)

    def test_rating_key_populated(self, db_path):
        """Rating keys are populated on returned OnDeckItems."""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        conn = sqlite3.connect(db_path)
        conn.execute(
            "INSERT INTO metadata_item_views (account_id, grandparent_title, parent_index, \"index\", title, viewed_at, library_section_id) VALUES (100, 'Test Show', 1, 1, 'S01E01', ?, 1)",
            (now,)
        )
        conn.commit()
        conn.close()

        items = fetch_on_deck_from_db(
            db_path=db_path,
            usernames=["SharedUser"],
            valid_sections=[1],
            days_to_monitor=30,
            number_episodes=0,
            user_id_map={"SharedUser": 100}
        )

        assert len(items) >= 1
        # rating_key should be the metadata_item_id as string
        assert items[0].rating_key is not None
        assert items[0].rating_key == "102"  # S01E02 metadata_item_id
