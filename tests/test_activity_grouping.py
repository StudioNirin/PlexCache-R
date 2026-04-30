"""Tests for the run-grouped Recent Activity transformation."""

from datetime import datetime, timedelta
from unittest.mock import patch

import pytest

from web.services.activity_grouping import (
    group_activity_into_runs,
    LEGACY_RUN_WINDOW,
    SOURCE_LABELS,
)


def _entry(
    timestamp: datetime,
    action: str,
    filename: str,
    *,
    size_bytes: int = 1000,
    run_id: str = None,
    run_source: str = "legacy",
    users=None,
) -> dict:
    """Build a FileActivity-shaped dict for tests."""
    return {
        "timestamp": timestamp.isoformat(),
        "time_display": timestamp.strftime("%H:%M:%S"),
        "date_key": timestamp.date().isoformat(),
        "date_display": "Today",
        "action": action,
        "filename": filename,
        "size": f"{size_bytes} B",
        "size_bytes": size_bytes,
        "users": users or [],
        "run_id": run_id,
        "run_source": run_source,
    }


# Patch get_time_format to avoid touching settings.json during tests.
@pytest.fixture(autouse=True)
def _stub_time_format():
    with patch("web.services.activity_grouping.get_time_format", return_value="24h"):
        yield


# Default to no run summaries; individual tests override when needed.
@pytest.fixture(autouse=True)
def _stub_run_summaries():
    with patch("web.services.activity_grouping.load_run_summaries", return_value={}):
        yield


class TestEmpty:
    def test_empty_input_returns_empty(self):
        assert group_activity_into_runs([]) == []


class TestRunIdBucketing:
    def test_same_run_id_groups_together(self):
        now = datetime(2026, 4, 25, 10, 0, 0)
        activities = [
            _entry(now + timedelta(minutes=2), "Cached", "b.mkv", run_id="abc", run_source="web"),
            _entry(now + timedelta(minutes=1), "Cached", "a.mkv", run_id="abc", run_source="web"),
        ]
        runs = group_activity_into_runs(activities)
        assert len(runs) == 1
        assert runs[0]["run_id"] == "abc"
        assert runs[0]["run_source"] == "web"
        assert runs[0]["files_cached"] == 2

    def test_different_run_ids_split(self):
        now = datetime(2026, 4, 25, 10, 0, 0)
        activities = [
            _entry(now + timedelta(minutes=10), "Cached", "b.mkv", run_id="run2", run_source="web"),
            _entry(now, "Cached", "a.mkv", run_id="run1", run_source="scheduled"),
        ]
        runs = group_activity_into_runs(activities)
        assert len(runs) == 2
        # Newest run first
        assert runs[0]["run_id"] == "run2"
        assert runs[1]["run_id"] == "run1"

    def test_different_run_ids_within_legacy_window_still_split(self):
        """Real runs separated by less than LEGACY_RUN_WINDOW must NOT merge."""
        now = datetime(2026, 4, 25, 10, 0, 0)
        activities = [
            _entry(now + timedelta(minutes=2), "Cached", "b.mkv", run_id="run2", run_source="web"),
            _entry(now, "Cached", "a.mkv", run_id="run1", run_source="web"),
        ]
        runs = group_activity_into_runs(activities)
        assert len(runs) == 2

    def test_run_label_from_source(self):
        now = datetime(2026, 4, 25, 10, 0, 0)
        activities = [
            _entry(now, "Cached", "a.mkv", run_id="r1", run_source="scheduled"),
        ]
        runs = group_activity_into_runs(activities)
        assert runs[0]["label"] == SOURCE_LABELS["scheduled"]


class TestLegacyBucketing:
    def test_close_legacy_entries_cluster(self):
        now = datetime(2026, 4, 25, 10, 0, 0)
        activities = [
            _entry(now + timedelta(minutes=5), "Cached", "b.mkv"),
            _entry(now, "Cached", "a.mkv"),
        ]
        runs = group_activity_into_runs(activities)
        assert len(runs) == 1
        assert runs[0]["run_source"] == "legacy"
        assert runs[0]["label"] == SOURCE_LABELS["legacy"]
        assert runs[0]["files_cached"] == 2

    def test_legacy_gap_beyond_window_splits(self):
        now = datetime(2026, 4, 25, 10, 0, 0)
        activities = [
            _entry(now + LEGACY_RUN_WINDOW + timedelta(minutes=1), "Cached", "b.mkv"),
            _entry(now, "Cached", "a.mkv"),
        ]
        runs = group_activity_into_runs(activities)
        assert len(runs) == 2

    def test_legacy_run_id_stable_within_bucket(self):
        now = datetime(2026, 4, 25, 10, 0, 0)
        activities = [
            _entry(now + timedelta(minutes=5), "Cached", "b.mkv"),
            _entry(now, "Cached", "a.mkv"),
        ]
        runs = group_activity_into_runs(activities)
        assert runs[0]["run_id"].startswith("legacy-")


class TestShowGrouping:
    def test_same_show_episodes_collapse(self):
        now = datetime(2026, 4, 25, 10, 0, 0)
        activities = [
            _entry(now + timedelta(seconds=2), "Cached", "Entourage - S01E03 - Test.mkv", run_id="r1", run_source="web"),
            _entry(now + timedelta(seconds=1), "Cached", "Entourage - S01E02 - Test.mkv", run_id="r1", run_source="web"),
            _entry(now, "Cached", "Entourage - S01E01 - Test.mkv", run_id="r1", run_source="web"),
        ]
        runs = group_activity_into_runs(activities)
        assert len(runs) == 1
        assert len(runs[0]["entries"]) == 1
        group = runs[0]["entries"][0]
        assert group.get("is_group") is True
        assert group["show_name"] == "Entourage"
        assert group["episode_count"] == 3

    def test_movie_stays_singleton(self):
        now = datetime(2026, 4, 25, 10, 0, 0)
        activities = [
            _entry(now, "Cached", "The Bourne Identity (2002).mkv", run_id="r1", run_source="web"),
        ]
        runs = group_activity_into_runs(activities)
        assert runs[0]["entries"][0].get("is_group") is None
        assert runs[0]["entries"][0]["filename"] == "The Bourne Identity (2002).mkv"

    def test_single_episode_does_not_group(self):
        now = datetime(2026, 4, 25, 10, 0, 0)
        activities = [
            _entry(now, "Cached", "Outlander - S08E07 - Evidence.mkv", run_id="r1", run_source="web"),
        ]
        runs = group_activity_into_runs(activities)
        # Single episode passes through as a singleton, not a group of 1
        assert runs[0]["entries"][0].get("is_group") is None


class TestAggregates:
    def test_byte_totals_per_action(self):
        now = datetime(2026, 4, 25, 10, 0, 0)
        activities = [
            _entry(now + timedelta(seconds=2), "Restored", "r.mkv", size_bytes=2000, run_id="r1", run_source="web"),
            _entry(now + timedelta(seconds=1), "Cached", "c2.mkv", size_bytes=500, run_id="r1", run_source="web"),
            _entry(now, "Cached", "c1.mkv", size_bytes=1000, run_id="r1", run_source="web"),
        ]
        runs = group_activity_into_runs(activities)
        assert runs[0]["files_cached"] == 2
        assert runs[0]["files_restored"] == 1
        assert runs[0]["bytes_cached"] == 1500
        assert runs[0]["bytes_restored"] == 2000
        assert runs[0]["bytes_total"] == 3500

    def test_duration_seconds_from_start_to_completed(self):
        now = datetime(2026, 4, 25, 10, 0, 0)
        activities = [
            _entry(now + timedelta(seconds=125), "Cached", "b.mkv", run_id="r1", run_source="web"),
            _entry(now, "Cached", "a.mkv", run_id="r1", run_source="web"),
        ]
        runs = group_activity_into_runs(activities)
        assert runs[0]["duration_seconds"] == 125

    def test_moved_to_array_counts_as_restored(self):
        now = datetime(2026, 4, 25, 10, 0, 0)
        activities = [
            _entry(now, "Moved to Array", "a.mkv", run_id="r1", run_source="scheduled"),
        ]
        runs = group_activity_into_runs(activities)
        assert runs[0]["files_restored"] == 1
        assert runs[0]["files_cached"] == 0


class TestRunSummaryOverride:
    """When a run_summary exists for a bucket's run_id, its started_at /
    completed_at win over the first/last FileActivity timestamp — captures
    the pre-/post-file-move tail (Plex API + scanning + audit).
    """

    def test_summary_started_at_overrides_first_file_timestamp(self):
        now = datetime(2026, 4, 25, 10, 0, 0)
        # Files moved at 10:01 and 10:02, but the actual run started at 10:00
        # (Plex API + scanning took 60s before the first file).
        activities = [
            _entry(now + timedelta(seconds=120), "Cached", "b.mkv", run_id="r1", run_source="web"),
            _entry(now + timedelta(seconds=60), "Cached", "a.mkv", run_id="r1", run_source="web"),
        ]
        summaries = {
            "r1": {
                "run_id": "r1",
                "run_source": "web",
                "started_at": now.isoformat(),
                "completed_at": (now + timedelta(seconds=180)).isoformat(),
            }
        }
        with patch("web.services.activity_grouping.load_run_summaries", return_value=summaries):
            runs = group_activity_into_runs(activities)

        # Without the override the bucket would be 60s; with summary it's 180s.
        assert runs[0]["duration_seconds"] == 180
        assert runs[0]["started_at"] == now.isoformat()
        assert runs[0]["completed_at"] == (now + timedelta(seconds=180)).isoformat()

    def test_missing_summary_falls_back_to_bucket_timestamps(self):
        now = datetime(2026, 4, 25, 10, 0, 0)
        activities = [
            _entry(now + timedelta(seconds=60), "Cached", "b.mkv", run_id="r1", run_source="web"),
            _entry(now, "Cached", "a.mkv", run_id="r1", run_source="web"),
        ]
        # No summary for r1
        runs = group_activity_into_runs(activities)
        assert runs[0]["duration_seconds"] == 60

    def test_malformed_summary_timestamps_ignored(self):
        now = datetime(2026, 4, 25, 10, 0, 0)
        activities = [
            _entry(now + timedelta(seconds=60), "Cached", "b.mkv", run_id="r1", run_source="web"),
            _entry(now, "Cached", "a.mkv", run_id="r1", run_source="web"),
        ]
        summaries = {
            "r1": {"started_at": "not-a-date", "completed_at": "also-bad"}
        }
        with patch("web.services.activity_grouping.load_run_summaries", return_value=summaries):
            runs = group_activity_into_runs(activities)
        # Falls back to bucket timestamps without raising.
        assert runs[0]["duration_seconds"] == 60


class TestOrdering:
    def test_runs_returned_newest_first(self):
        now = datetime(2026, 4, 25, 10, 0, 0)
        activities = [
            _entry(now + timedelta(hours=2), "Cached", "c.mkv", run_id="r3", run_source="web"),
            _entry(now + timedelta(hours=1), "Cached", "b.mkv", run_id="r2", run_source="web"),
            _entry(now, "Cached", "a.mkv", run_id="r1", run_source="web"),
        ]
        runs = group_activity_into_runs(activities)
        assert [r["run_id"] for r in runs] == ["r3", "r2", "r1"]

    def test_entries_within_run_newest_first(self):
        now = datetime(2026, 4, 25, 10, 0, 0)
        activities = [
            _entry(now + timedelta(seconds=10), "Cached", "newest.mkv", run_id="r1", run_source="web"),
            _entry(now, "Cached", "oldest.mkv", run_id="r1", run_source="web"),
        ]
        runs = group_activity_into_runs(activities)
        # First entry should be the newer one
        assert runs[0]["entries"][0]["filename"] == "newest.mkv"


class TestMixedRunIdAndLegacy:
    def test_real_run_breaks_legacy_window(self):
        """A run_id-bearing entry between two legacy entries should split them."""
        now = datetime(2026, 4, 25, 10, 0, 0)
        activities = [
            _entry(now + timedelta(minutes=10), "Cached", "after.mkv"),  # legacy
            _entry(now + timedelta(minutes=5), "Cached", "real.mkv", run_id="r1", run_source="web"),
            _entry(now, "Cached", "before.mkv"),  # legacy
        ]
        runs = group_activity_into_runs(activities)
        # 3 buckets: legacy(after), real(r1), legacy(before)
        assert len(runs) == 3
        sources = [r["run_source"] for r in runs]
        assert sources == ["legacy", "web", "legacy"]
