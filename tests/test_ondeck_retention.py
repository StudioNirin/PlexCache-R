"""Tests for OnDeck retention feature.

Verifies that OnDeckTracker preserves first_seen across runs,
cleanup_unseen() removes stale entries, and is_expired() correctly
expires items based on ondeck_retention_days (per-user).
Also tests that CachePriorityManager gates episode position bonus
on active OnDeck status.
"""

import os
import sys
import json
import tempfile
from datetime import datetime, timedelta
from unittest.mock import MagicMock

# Mock fcntl for Windows compatibility
sys.modules['fcntl'] = MagicMock()

# Mock apscheduler
for _mod in [
    'apscheduler', 'apscheduler.schedulers',
    'apscheduler.schedulers.background', 'apscheduler.triggers',
    'apscheduler.triggers.cron', 'apscheduler.triggers.interval',
]:
    sys.modules.setdefault(_mod, MagicMock())

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from core.file_operations import (
    OnDeckTracker, CachePriorityManager, CacheTimestampTracker,
    WatchlistTracker
)


@pytest.fixture
def tracker(tmp_path):
    """Create a fresh OnDeckTracker with a temp file."""
    return OnDeckTracker(str(tmp_path / "ondeck_tracker.json"))


class TestPrepareForRun:
    """Tests for prepare_for_run() replacing clear_for_run()."""

    def test_preserves_first_seen(self, tracker):
        """first_seen survives across prepare/update cycles."""
        tracker.update_entry("/media/movie.mkv", "Alice")
        entry_before = tracker.get_entry("/media/movie.mkv")
        original_first_seen = entry_before['first_seen']

        # Simulate a new run
        tracker.prepare_for_run()
        tracker.update_entry("/media/movie.mkv", "Alice")

        entry_after = tracker.get_entry("/media/movie.mkv")
        assert entry_after['first_seen'] == original_first_seen

    def test_clears_per_run_fields(self, tracker):
        """users, ondeck_users, episode_info reset on prepare_for_run()."""
        tracker.update_entry(
            "/media/show/s01e01.mkv", "Bob",
            episode_info={"show": "Foundation", "season": 1, "episode": 1},
            is_current_ondeck=True
        )

        entry = tracker.get_entry("/media/show/s01e01.mkv")
        assert entry['users'] == ["Bob"]
        assert entry['ondeck_users'] == ["Bob"]
        assert entry['episode_info'] is not None

        # Prepare for new run — per-run fields should be cleared
        tracker.prepare_for_run()

        entry = tracker.get_entry("/media/show/s01e01.mkv")
        assert entry['users'] == []
        assert entry['ondeck_users'] == []
        assert 'episode_info' not in entry
        # first_seen and last_seen preserved
        assert 'first_seen' in entry
        assert 'last_seen' in entry


class TestCleanupUnseen:
    """Tests for cleanup_unseen() removing entries not refreshed this run."""

    def test_removes_stale_entries(self, tracker):
        """Entries not refreshed during the run are removed."""
        tracker.update_entry("/media/movie1.mkv", "Alice")
        tracker.update_entry("/media/movie2.mkv", "Bob")

        # New run — only refresh movie1
        tracker.prepare_for_run()
        tracker.update_entry("/media/movie1.mkv", "Alice")
        removed = tracker.cleanup_unseen()

        assert removed == 1
        assert tracker.get_entry("/media/movie1.mkv") is not None
        assert tracker.get_entry("/media/movie2.mkv") is None

    def test_keeps_refreshed_entries(self, tracker):
        """Entries refreshed this run are kept."""
        tracker.update_entry("/media/movie1.mkv", "Alice")
        tracker.update_entry("/media/movie2.mkv", "Bob")
        tracker.update_entry("/media/movie3.mkv", "Carol")

        tracker.prepare_for_run()
        tracker.update_entry("/media/movie1.mkv", "Alice")
        tracker.update_entry("/media/movie2.mkv", "Bob")
        tracker.update_entry("/media/movie3.mkv", "Carol")
        removed = tracker.cleanup_unseen()

        assert removed == 0
        assert tracker.get_entry("/media/movie1.mkv") is not None
        assert tracker.get_entry("/media/movie2.mkv") is not None
        assert tracker.get_entry("/media/movie3.mkv") is not None


class TestIsExpired:
    """Tests for is_expired() OnDeck retention check."""

    def test_returns_true_when_old(self, tracker):
        """Item older than retention_days expires."""
        tracker.update_entry("/media/old_movie.mkv", "Alice")

        # Backdate first_seen and user_first_seen to 10 days ago
        old_ts = (datetime.now() - timedelta(days=10)).isoformat()
        entry = tracker._data["/media/old_movie.mkv"]
        entry['first_seen'] = old_ts
        entry['user_first_seen'] = {"Alice": old_ts}
        tracker._save()

        assert tracker.is_expired("/media/old_movie.mkv", retention_days=7) is True

    def test_returns_false_when_fresh(self, tracker):
        """Item within retention_days doesn't expire."""
        tracker.update_entry("/media/fresh_movie.mkv", "Alice")

        # first_seen is now — well within 7 days
        assert tracker.is_expired("/media/fresh_movie.mkv", retention_days=7) is False

    def test_disabled_when_zero(self, tracker):
        """retention_days=0 never expires."""
        tracker.update_entry("/media/movie.mkv", "Alice")

        # Backdate first_seen way in the past
        entry = tracker._data["/media/movie.mkv"]
        entry['first_seen'] = (datetime.now() - timedelta(days=365)).isoformat()
        tracker._save()

        assert tracker.is_expired("/media/movie.mkv", retention_days=0) is False

    def test_returns_false_for_unknown_entry(self, tracker):
        """Unknown file path returns False (conservative)."""
        assert tracker.is_expired("/media/unknown.mkv", retention_days=7) is False

    def test_returns_false_when_no_first_seen(self, tracker):
        """Entry without first_seen returns False (conservative)."""
        tracker._data["/media/no_ts.mkv"] = {"users": ["Alice"], "last_seen": datetime.now().isoformat()}
        tracker._save()

        assert tracker.is_expired("/media/no_ts.mkv", retention_days=7) is False


class TestIntegration:
    """Integration tests for OnDeck retention in the caching workflow."""

    def test_expired_items_not_in_ondeck_items(self, tracker):
        """Expired items are excluded from the ondeck list (simulated _process_media flow)."""
        # Simulate run 1: add items
        tracker.prepare_for_run()
        tracker.update_entry("/media/old.mkv", "Alice")
        tracker.update_entry("/media/new.mkv", "Bob")

        # Backdate old.mkv to 20 days ago (both file-level and per-user)
        old_ts = (datetime.now() - timedelta(days=20)).isoformat()
        tracker._data["/media/old.mkv"]['first_seen'] = old_ts
        tracker._data["/media/old.mkv"]['user_first_seen'] = {"Alice": old_ts}
        tracker._save()

        # Simulate run 2
        tracker.prepare_for_run()
        tracker.update_entry("/media/old.mkv", "Alice")
        tracker.update_entry("/media/new.mkv", "Bob")

        # Filter like _process_media does
        modified_ondeck = ["/media/old.mkv", "/media/new.mkv"]
        retention_days = 14
        expired = {p for p in modified_ondeck if tracker.is_expired(p, retention_days)}
        filtered = [p for p in modified_ondeck if p not in expired]

        assert "/media/old.mkv" not in filtered
        assert "/media/new.mkv" in filtered
        assert len(expired) == 1

    def test_expired_items_eligible_for_move_back(self, tracker):
        """Expired items not in ondeck_items means they're eligible for move-back."""
        tracker.prepare_for_run()
        tracker.update_entry("/media/expired.mkv", "Alice")

        # Backdate (both file-level and per-user)
        old_ts = (datetime.now() - timedelta(days=30)).isoformat()
        tracker._data["/media/expired.mkv"]['first_seen'] = old_ts
        tracker._data["/media/expired.mkv"]['user_first_seen'] = {"Alice": old_ts}
        tracker._save()

        # Simulate the filtering
        modified_ondeck = ["/media/expired.mkv"]
        retention_days = 14
        expired = {p for p in modified_ondeck if tracker.is_expired(p, retention_days)}
        ondeck_items = set(p for p in modified_ondeck if p not in expired)

        # Expired item is NOT in ondeck_items, so move-back logic won't skip it
        assert "/media/expired.mkv" not in ondeck_items

    def test_first_seen_accumulates_across_runs(self, tracker):
        """Simulate multiple prepare/update cycles — first_seen never resets."""
        tracker.prepare_for_run()
        tracker.update_entry("/media/movie.mkv", "Alice")
        original_first_seen = tracker.get_entry("/media/movie.mkv")['first_seen']

        # Simulate 5 more runs
        for _ in range(5):
            tracker.prepare_for_run()
            tracker.update_entry("/media/movie.mkv", "Alice")
            tracker.cleanup_unseen()

        final_entry = tracker.get_entry("/media/movie.mkv")
        assert final_entry['first_seen'] == original_first_seen
        # last_seen should be updated each run
        assert final_entry['last_seen'] >= original_first_seen


class TestPerUserFirstSeen:
    """Tests for per-user first_seen tracking (user_first_seen field)."""

    def test_user_first_seen_set_on_add(self, tracker):
        """user_first_seen is populated when a user is first added."""
        tracker.update_entry("/media/movie.mkv", "Brandon")

        entry = tracker.get_entry("/media/movie.mkv")
        assert 'user_first_seen' in entry
        assert 'Brandon' in entry['user_first_seen']
        assert entry['user_first_seen']['Brandon'] == entry['first_seen']

    def test_second_user_gets_own_timestamp(self, tracker):
        """Second user gets their own user_first_seen, different from first user."""
        tracker.update_entry("/media/movie.mkv", "Brandon")
        entry = tracker.get_entry("/media/movie.mkv")
        brandon_ts = entry['user_first_seen']['Brandon']

        # Simulate time passing
        import time
        time.sleep(0.01)

        tracker.update_entry("/media/movie.mkv", "hawkey19")
        entry = tracker.get_entry("/media/movie.mkv")

        assert 'hawkey19' in entry['user_first_seen']
        assert entry['user_first_seen']['Brandon'] == brandon_ts  # Unchanged
        assert entry['user_first_seen']['hawkey19'] >= brandon_ts

    def test_user_first_seen_preserved_across_runs(self, tracker):
        """user_first_seen survives prepare_for_run/update cycles."""
        tracker.update_entry("/media/movie.mkv", "Brandon")
        entry = tracker.get_entry("/media/movie.mkv")
        original_ufs = entry['user_first_seen']['Brandon']

        # New run
        tracker.prepare_for_run()
        tracker.update_entry("/media/movie.mkv", "Brandon")

        entry = tracker.get_entry("/media/movie.mkv")
        assert entry['user_first_seen']['Brandon'] == original_ufs

    def test_new_entry_has_user_first_seen(self, tracker):
        """Brand new entries get user_first_seen populated."""
        tracker.update_entry("/media/new.mkv", "Alice")
        entry = tracker.get_entry("/media/new.mkv")
        assert entry['user_first_seen'] == {"Alice": entry['first_seen']}


class TestPerUserExpiry:
    """Tests for per-user retention expiry logic."""

    def test_expired_only_when_all_users_expired(self, tracker):
        """Item with one expired + one fresh user is NOT expired."""
        tracker.update_entry("/media/show.mkv", "Brandon")
        tracker.update_entry("/media/show.mkv", "hawkey19")

        # Backdate Brandon to 61 days ago, hawkey19 is fresh (today)
        old_ts = (datetime.now() - timedelta(days=61)).isoformat()
        entry = tracker._data["/media/show.mkv"]
        entry['user_first_seen']['Brandon'] = old_ts
        tracker._save()

        # With 60 day retention: Brandon expired, hawkey19 fresh → NOT expired
        assert tracker.is_expired("/media/show.mkv", retention_days=60) is False

    def test_expired_when_all_users_expired(self, tracker):
        """Item with all users expired IS expired."""
        tracker.update_entry("/media/show.mkv", "Brandon")
        tracker.update_entry("/media/show.mkv", "hawkey19")

        # Backdate both users
        old_ts = (datetime.now() - timedelta(days=61)).isoformat()
        entry = tracker._data["/media/show.mkv"]
        entry['user_first_seen']['Brandon'] = old_ts
        entry['user_first_seen']['hawkey19'] = old_ts
        entry['first_seen'] = old_ts
        tracker._save()

        assert tracker.is_expired("/media/show.mkv", retention_days=60) is True

    def test_new_user_resets_expiry(self, tracker):
        """Adding a fresh user to an otherwise expired item prevents expiry."""
        tracker.update_entry("/media/show.mkv", "Brandon")

        # Backdate Brandon to 61 days ago
        old_ts = (datetime.now() - timedelta(days=61)).isoformat()
        entry = tracker._data["/media/show.mkv"]
        entry['first_seen'] = old_ts
        entry['user_first_seen']['Brandon'] = old_ts
        tracker._save()

        # Was expired for just Brandon
        assert tracker.is_expired("/media/show.mkv", retention_days=60) is True

        # hawkey19 adds it — fresh user_first_seen
        tracker.update_entry("/media/show.mkv", "hawkey19")

        # Now NOT expired because hawkey19 is fresh
        assert tracker.is_expired("/media/show.mkv", retention_days=60) is False

    def test_migration_fallback_to_file_first_seen(self, tracker):
        """Entries without user_first_seen fall back to file-level first_seen."""
        # Manually create an old-format entry (no user_first_seen)
        old_ts = (datetime.now() - timedelta(days=10)).isoformat()
        tracker._data["/media/legacy.mkv"] = {
            'users': ['Alice'],
            'first_seen': old_ts,
            'last_seen': datetime.now().isoformat()
        }
        tracker._save()

        # Falls back to file-level first_seen (10 days > 7 days retention)
        assert tracker.is_expired("/media/legacy.mkv", retention_days=7) is True

    def test_migration_fallback_fresh(self, tracker):
        """Old-format entry within retention is not expired."""
        tracker._data["/media/legacy.mkv"] = {
            'users': ['Alice'],
            'first_seen': datetime.now().isoformat(),
            'last_seen': datetime.now().isoformat()
        }
        tracker._save()

        assert tracker.is_expired("/media/legacy.mkv", retention_days=7) is False

    def test_no_users_returns_false(self, tracker):
        """Entry with empty users list is not expired (conservative)."""
        tracker._data["/media/empty.mkv"] = {
            'users': [],
            'first_seen': (datetime.now() - timedelta(days=100)).isoformat(),
            'last_seen': datetime.now().isoformat()
        }
        tracker._save()

        assert tracker.is_expired("/media/empty.mkv", retention_days=7) is False


class TestCleanupTrimsUserFirstSeen:
    """Tests for cleanup_unseen() trimming stale user_first_seen entries."""

    def test_cleanup_trims_user_first_seen(self, tracker):
        """Stale user entries are removed from user_first_seen during cleanup."""
        # Run 1: both users add the item
        tracker.prepare_for_run()
        tracker.update_entry("/media/movie.mkv", "Brandon")
        tracker.update_entry("/media/movie.mkv", "hawkey19")
        tracker.cleanup_unseen()

        entry = tracker.get_entry("/media/movie.mkv")
        assert 'Brandon' in entry['user_first_seen']
        assert 'hawkey19' in entry['user_first_seen']

        # Run 2: only Brandon refreshes
        tracker.prepare_for_run()
        tracker.update_entry("/media/movie.mkv", "Brandon")
        tracker.cleanup_unseen()

        entry = tracker.get_entry("/media/movie.mkv")
        assert 'Brandon' in entry['user_first_seen']
        assert 'hawkey19' not in entry['user_first_seen']


class TestEpisodePositionGating:
    """Tests for gating episode position bonus on active OnDeck status."""

    @pytest.fixture
    def priority_setup(self, tmp_path):
        """Create trackers and priority manager for scorer tests."""
        ts_file = str(tmp_path / "timestamps.json")
        ondeck_file = str(tmp_path / "ondeck.json")
        watchlist_file = str(tmp_path / "watchlist.json")

        ts_tracker = CacheTimestampTracker(ts_file)
        ondeck_tracker = OnDeckTracker(ondeck_file)
        watchlist_tracker = WatchlistTracker(watchlist_file)

        priority_mgr = CachePriorityManager(
            timestamp_tracker=ts_tracker,
            watchlist_tracker=watchlist_tracker,
            ondeck_tracker=ondeck_tracker,
            number_episodes=5,
        )

        return ts_tracker, ondeck_tracker, priority_mgr

    def _setup_ondeck_episode(self, ts_tracker, ondeck_tracker, cache_path, show, season, episode):
        """Helper to set up an ondeck episode in both trackers."""
        ts_tracker.record_cache_time(cache_path, source="ondeck", media_type="episode")
        ondeck_tracker.update_entry(
            cache_path, "TestUser",
            episode_info={"show": show, "season": season, "episode": episode},
            is_current_ondeck=True
        )

    def test_episode_bonus_gated_when_not_in_active_set(self, priority_setup):
        """Expired items (not in active_ondeck_paths) don't get episode position bonus."""
        ts_tracker, ondeck_tracker, priority_mgr = priority_setup

        cache_path = "/mnt/cache/TV/Show/Season 01/Show - S01E01.mkv"
        self._setup_ondeck_episode(ts_tracker, ondeck_tracker, cache_path, "Show", 1, 1)

        # Without gating (retention disabled) — should get episode bonus
        priority_mgr.active_ondeck_paths = None
        score_ungated = priority_mgr.calculate_priority(cache_path)

        # With gating — path NOT in active set (expired)
        priority_mgr.active_ondeck_paths = set()
        score_gated = priority_mgr.calculate_priority(cache_path)

        # Episode position bonus is +15, so gated score should be lower
        assert score_ungated > score_gated
        assert score_ungated - score_gated == 15

    def test_episode_bonus_awarded_when_in_active_set(self, priority_setup):
        """Active items (in active_ondeck_paths) get the full episode position bonus."""
        ts_tracker, ondeck_tracker, priority_mgr = priority_setup

        cache_path = "/mnt/cache/TV/Show/Season 01/Show - S01E01.mkv"
        self._setup_ondeck_episode(ts_tracker, ondeck_tracker, cache_path, "Show", 1, 1)

        # With gating — path IS in active set
        priority_mgr.active_ondeck_paths = {cache_path}
        score_active = priority_mgr.calculate_priority(cache_path)

        # Without gating (retention disabled)
        priority_mgr.active_ondeck_paths = None
        score_none = priority_mgr.calculate_priority(cache_path)

        # Both should be the same — active item gets full bonus
        assert score_active == score_none

    def test_episode_bonus_when_retention_disabled(self, priority_setup):
        """When active_ondeck_paths is None (retention disabled), all items get bonus."""
        ts_tracker, ondeck_tracker, priority_mgr = priority_setup

        cache_path = "/mnt/cache/TV/Show/Season 01/Show - S01E01.mkv"
        self._setup_ondeck_episode(ts_tracker, ondeck_tracker, cache_path, "Show", 1, 1)

        # Default: None (retention disabled)
        assert priority_mgr.active_ondeck_paths is None
        score = priority_mgr.calculate_priority(cache_path)

        # Should include the +15 episode bonus (base 50 + source 15 + users 5 + episode 15 = 85+)
        # The exact score depends on cache recency and staleness, but should be >= 80
        assert score >= 80
