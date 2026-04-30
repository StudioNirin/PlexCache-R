"""Tests for core/activity.py — shared activity writer module.

Tests cover: FileActivity serialization, load/save persistence, retention pruning,
record_file_activity convenience function, save_last_run_time, save_run_summary,
and load_last_run_summary.
"""

import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# conftest.py handles fcntl/apscheduler mocking and path setup

from core.activity import (
    FileActivity,
    load_activity,
    save_activity,
    record_file_activity,
    save_last_run_time,
    load_last_run_summary,
    load_run_summaries,
    load_run_summary,
    save_run_summary,
    MAX_RECENT_ACTIVITY,
    ACTIVITY_FILE,
    LAST_RUN_FILE,
    RUN_SUMMARIES_FILE,
    _get_activity_retention_hours,
)


# ============================================================================
# TestFileActivity
# ============================================================================

class TestFileActivity:
    """Tests for FileActivity dataclass and serialization."""

    def test_to_dict_24h_format(self):
        with patch('core.activity.get_time_format', return_value='24h'):
            fa = FileActivity(
                timestamp=datetime(2026, 1, 15, 14, 30, 5),
                action="Cached",
                filename="movie.mkv",
                size_bytes=1073741824,
                users=["alice"],
            )
            d = fa.to_dict()

        assert d['time_display'] == "14:30:05"
        assert d['action'] == "Cached"
        assert d['filename'] == "movie.mkv"
        assert d['users'] == ["alice"]
        assert d['timestamp'] == "2026-01-15T14:30:05"

    @pytest.mark.skipif(sys.platform == 'win32', reason="%-I strftime is Linux-only")
    def test_to_dict_12h_format(self):
        with patch('core.activity.get_time_format', return_value='12h'):
            fa = FileActivity(
                timestamp=datetime(2026, 1, 15, 14, 30, 5),
                action="Cached",
                filename="movie.mkv",
            )
            d = fa.to_dict()

        assert "PM" in d['time_display']

    def test_zero_size_dash_display(self):
        with patch('core.activity.get_time_format', return_value='24h'):
            fa = FileActivity(
                timestamp=datetime.now(),
                action="Protected",
                filename="file.mkv",
                size_bytes=0,
            )
            d = fa.to_dict()

        assert d['size'] == "-"

    def test_associated_files_included(self):
        with patch('core.activity.get_time_format', return_value='24h'):
            fa = FileActivity(
                timestamp=datetime.now(),
                action="Cached",
                filename="movie.mkv",
                associated_files=[{"filename": "subs.srt", "size": "50 KB"}],
            )
            d = fa.to_dict()

        assert "associated_files" in d
        assert d["associated_files"][0]["filename"] == "subs.srt"

    def test_associated_files_omitted_when_empty(self):
        with patch('core.activity.get_time_format', return_value='24h'):
            fa = FileActivity(
                timestamp=datetime.now(),
                action="Cached",
                filename="movie.mkv",
            )
            d = fa.to_dict()

        assert "associated_files" not in d

    def test_all_fields_present(self):
        with patch('core.activity.get_time_format', return_value='24h'):
            fa = FileActivity(
                timestamp=datetime.now(),
                action="Cached",
                filename="test.mkv",
                size_bytes=100,
                users=["bob"],
            )
            d = fa.to_dict()

        required_keys = {
            'timestamp', 'time_display', 'date_key', 'date_display',
            'action', 'filename', 'size', 'size_bytes', 'users',
            'run_id', 'run_source',
        }
        assert required_keys == set(d.keys())


# ============================================================================
# TestLoadActivity
# ============================================================================

class TestLoadActivity:
    """Tests for load_activity() disk persistence."""

    def test_missing_file_returns_empty(self, tmp_path):
        with patch('core.activity.ACTIVITY_FILE', tmp_path / "nope.json"):
            result = load_activity()
        assert result == []

    def test_empty_file_returns_empty(self, tmp_path):
        f = tmp_path / "activity.json"
        f.write_text("")
        with patch('core.activity.ACTIVITY_FILE', f):
            result = load_activity()
        assert result == []

    def test_valid_entries_loaded(self, tmp_path):
        now = datetime.now()
        data = [
            {"timestamp": now.isoformat(), "action": "Cached", "filename": "a.mkv", "size_bytes": 100, "users": []},
            {"timestamp": now.isoformat(), "action": "Restored", "filename": "b.mkv", "size_bytes": 200, "users": ["u1"]},
        ]
        f = tmp_path / "activity.json"
        f.write_text(json.dumps(data, indent=2))

        with patch('core.activity.ACTIVITY_FILE', f):
            with patch('core.activity._get_activity_retention_hours', return_value=24):
                result = load_activity()

        assert len(result) == 2
        assert result[0].filename in ("a.mkv", "b.mkv")

    def test_retention_filtering(self, tmp_path):
        now = datetime.now()
        old = now - timedelta(hours=48)
        data = [
            {"timestamp": now.isoformat(), "action": "Cached", "filename": "new.mkv"},
            {"timestamp": old.isoformat(), "action": "Cached", "filename": "old.mkv"},
        ]
        f = tmp_path / "activity.json"
        f.write_text(json.dumps(data, indent=2))

        with patch('core.activity.ACTIVITY_FILE', f):
            with patch('core.activity._get_activity_retention_hours', return_value=24):
                result = load_activity()

        assert len(result) == 1
        assert result[0].filename == "new.mkv"

    def test_malformed_entries_skipped(self, tmp_path):
        now = datetime.now()
        data = [
            {"timestamp": now.isoformat(), "action": "Cached", "filename": "ok.mkv"},
            {"bad_key": "missing required fields"},
            {"timestamp": "not-a-date", "action": "Cached", "filename": "bad.mkv"},
        ]
        f = tmp_path / "activity.json"
        f.write_text(json.dumps(data, indent=2))

        with patch('core.activity.ACTIVITY_FILE', f):
            with patch('core.activity._get_activity_retention_hours', return_value=24):
                result = load_activity()

        assert len(result) == 1
        assert result[0].filename == "ok.mkv"

    def test_sorted_newest_first(self, tmp_path):
        now = datetime.now()
        data = [
            {"timestamp": (now - timedelta(hours=2)).isoformat(), "action": "Cached", "filename": "older.mkv"},
            {"timestamp": now.isoformat(), "action": "Cached", "filename": "newest.mkv"},
            {"timestamp": (now - timedelta(hours=1)).isoformat(), "action": "Cached", "filename": "middle.mkv"},
        ]
        f = tmp_path / "activity.json"
        f.write_text(json.dumps(data, indent=2))

        with patch('core.activity.ACTIVITY_FILE', f):
            with patch('core.activity._get_activity_retention_hours', return_value=24):
                result = load_activity()

        assert result[0].filename == "newest.mkv"
        assert result[1].filename == "middle.mkv"
        assert result[2].filename == "older.mkv"

    def test_capped_at_max(self, tmp_path):
        now = datetime.now()
        data = [
            {"timestamp": (now - timedelta(seconds=i)).isoformat(), "action": "Cached", "filename": f"f{i}.mkv"}
            for i in range(MAX_RECENT_ACTIVITY + 50)
        ]
        f = tmp_path / "activity.json"
        f.write_text(json.dumps(data, indent=2))

        with patch('core.activity.ACTIVITY_FILE', f):
            with patch('core.activity._get_activity_retention_hours', return_value=9999):
                result = load_activity()

        assert len(result) == MAX_RECENT_ACTIVITY


# ============================================================================
# TestSaveActivity
# ============================================================================

class TestSaveActivity:
    """Tests for save_activity() disk persistence."""

    def test_valid_json_with_indent(self, tmp_path):
        f = tmp_path / "activity.json"
        activities = [
            FileActivity(timestamp=datetime.now(), action="Cached", filename="a.mkv", size_bytes=100),
        ]

        with patch('core.activity.ACTIVITY_FILE', f):
            with patch('core.activity._get_activity_retention_hours', return_value=24):
                save_activity(activities)

        content = f.read_text()
        data = json.loads(content)
        assert len(data) == 1
        assert data[0]['action'] == "Cached"
        assert '  "' in content  # indent=2

    def test_retention_filtering_on_save(self, tmp_path):
        f = tmp_path / "activity.json"
        now = datetime.now()
        activities = [
            FileActivity(timestamp=now, action="Cached", filename="new.mkv"),
            FileActivity(timestamp=now - timedelta(hours=48), action="Cached", filename="old.mkv"),
        ]

        with patch('core.activity.ACTIVITY_FILE', f):
            with patch('core.activity._get_activity_retention_hours', return_value=24):
                save_activity(activities)

        data = json.loads(f.read_text())
        assert len(data) == 1
        assert data[0]['filename'] == "new.mkv"

    def test_round_trip_preservation(self, tmp_path):
        f = tmp_path / "activity.json"
        now = datetime.now()
        original = [
            FileActivity(
                timestamp=now,
                action="Cached",
                filename="movie.mkv",
                size_bytes=1024,
                users=["alice", "bob"],
            ),
        ]

        with patch('core.activity.ACTIVITY_FILE', f):
            with patch('core.activity._get_activity_retention_hours', return_value=24):
                save_activity(original)
                loaded = load_activity()

        assert len(loaded) == 1
        assert loaded[0].action == "Cached"
        assert loaded[0].filename == "movie.mkv"
        assert loaded[0].size_bytes == 1024
        assert loaded[0].users == ["alice", "bob"]


# ============================================================================
# TestRecordFileActivity
# ============================================================================

class TestRecordFileActivity:
    """Tests for record_file_activity() convenience function."""

    def test_appends_to_empty_file(self, tmp_path):
        f = tmp_path / "activity.json"

        with patch('core.activity.ACTIVITY_FILE', f):
            with patch('core.activity._get_activity_retention_hours', return_value=24):
                record_file_activity("Cached", "movie.mkv", size_bytes=1024)

        data = json.loads(f.read_text())
        assert len(data) == 1
        assert data[0]['action'] == "Cached"
        assert data[0]['filename'] == "movie.mkv"
        assert data[0]['size_bytes'] == 1024

    def test_merges_with_existing(self, tmp_path):
        f = tmp_path / "activity.json"
        now = datetime.now()
        existing = [{"timestamp": now.isoformat(), "action": "Restored", "filename": "old.mkv", "size_bytes": 0, "users": []}]
        f.write_text(json.dumps(existing, indent=2))

        with patch('core.activity.ACTIVITY_FILE', f):
            with patch('core.activity._get_activity_retention_hours', return_value=24):
                record_file_activity("Cached", "new.mkv", size_bytes=500)

        data = json.loads(f.read_text())
        assert len(data) == 2
        filenames = {e['filename'] for e in data}
        assert "old.mkv" in filenames
        assert "new.mkv" in filenames

    def test_newest_first(self, tmp_path):
        f = tmp_path / "activity.json"

        with patch('core.activity.ACTIVITY_FILE', f):
            with patch('core.activity._get_activity_retention_hours', return_value=24):
                record_file_activity("Cached", "first.mkv")
                record_file_activity("Cached", "second.mkv")

        data = json.loads(f.read_text())
        assert data[0]['filename'] == "second.mkv"
        assert data[1]['filename'] == "first.mkv"

    def test_capped_at_max(self, tmp_path):
        f = tmp_path / "activity.json"
        now = datetime.now()
        existing = [
            {"timestamp": (now - timedelta(seconds=i)).isoformat(), "action": "Cached", "filename": f"f{i}.mkv", "size_bytes": 0, "users": []}
            for i in range(MAX_RECENT_ACTIVITY)
        ]
        f.write_text(json.dumps(existing, indent=2))

        with patch('core.activity.ACTIVITY_FILE', f):
            with patch('core.activity._get_activity_retention_hours', return_value=9999):
                record_file_activity("Cached", "overflow.mkv")

        data = json.loads(f.read_text())
        assert len(data) == MAX_RECENT_ACTIVITY

    def test_with_users_and_associated(self, tmp_path):
        f = tmp_path / "activity.json"

        with patch('core.activity.ACTIVITY_FILE', f):
            with patch('core.activity._get_activity_retention_hours', return_value=24):
                record_file_activity(
                    "Cached", "movie.mkv",
                    size_bytes=1024,
                    users=["alice"],
                    associated_files=[{"filename": "subs.srt", "size": "50 KB"}],
                )

        data = json.loads(f.read_text())
        assert data[0]['users'] == ["alice"]
        assert data[0]['associated_files'] == [{"filename": "subs.srt", "size": "50 KB"}]


# ============================================================================
# TestLastRunTime
# ============================================================================

class TestLastRunTime:
    """Tests for save_last_run_time()."""

    def test_writes_timestamp(self, tmp_path):
        f = tmp_path / "last_run.txt"

        with patch('core.activity.LAST_RUN_FILE', f):
            save_last_run_time()

        content = f.read_text()
        # Should be parseable as an ISO 8601 timestamp
        dt = datetime.fromisoformat(content)
        assert (datetime.now() - dt).total_seconds() < 5


# ============================================================================
# TestRunSummary
# ============================================================================

class TestRunSummary:
    """Tests for save_run_summary() / load_run_summaries() / load_last_run_summary()."""

    @staticmethod
    def _patch_files(tmp_path):
        """Patch both the run-summaries file and the legacy migration file."""
        return patch.multiple(
            'core.activity',
            RUN_SUMMARIES_FILE=tmp_path / "run_summaries.json",
            _LEGACY_RUN_SUMMARY_FILE=tmp_path / "last_run_summary.json",
        )

    def _summary(self, **overrides):
        base = {
            "run_source": "cli",
            "status": "completed",
            "started_at": datetime.now().isoformat(),
            "completed_at": datetime.now().isoformat(),
            "duration_seconds": 10.5,
            "files_cached": 3,
            "files_restored": 1,
            "bytes_cached": 1000,
            "bytes_restored": 500,
            "error_count": 0,
            "dry_run": False,
        }
        base.update(overrides)
        return base

    def test_round_trip(self, tmp_path):
        with self._patch_files(tmp_path):
            save_run_summary("run-1", self._summary())
            loaded = load_last_run_summary()

        assert loaded is not None
        assert loaded["run_id"] == "run-1"
        assert loaded["files_cached"] == 3
        assert loaded["status"] == "completed"

    def test_save_run_summary_requires_run_id(self, tmp_path):
        with self._patch_files(tmp_path):
            save_run_summary("", self._summary())
            assert load_last_run_summary() is None

    def test_multiple_runs_keyed_by_id(self, tmp_path):
        now = datetime.now()
        with self._patch_files(tmp_path):
            save_run_summary("run-1", self._summary(
                started_at=(now - timedelta(minutes=10)).isoformat(),
                files_cached=1,
            ))
            save_run_summary("run-2", self._summary(
                started_at=now.isoformat(),
                files_cached=5,
            ))
            summaries = load_run_summaries()

        assert set(summaries.keys()) == {"run-1", "run-2"}
        assert summaries["run-1"]["files_cached"] == 1
        assert summaries["run-2"]["files_cached"] == 5

    def test_load_run_summary_by_id(self, tmp_path):
        with self._patch_files(tmp_path):
            save_run_summary("run-1", self._summary())
            assert load_run_summary("run-1") is not None
            assert load_run_summary("missing") is None

    def test_load_last_run_summary_excludes_maintenance_by_default(self, tmp_path):
        now = datetime.now()
        with self._patch_files(tmp_path):
            # Older caching run, newer maintenance run
            save_run_summary("cli-1", self._summary(
                run_source="cli",
                started_at=(now - timedelta(minutes=10)).isoformat(),
            ))
            save_run_summary("maint-1", self._summary(
                run_source="maintenance",
                started_at=now.isoformat(),
            ))
            loaded = load_last_run_summary()

        # Even though maintenance is newer, the dashboard widget should
        # surface the most recent caching run.
        assert loaded is not None
        assert loaded["run_id"] == "cli-1"

    def test_load_last_run_summary_can_include_all_sources(self, tmp_path):
        now = datetime.now()
        with self._patch_files(tmp_path):
            save_run_summary("cli-1", self._summary(
                run_source="cli",
                started_at=(now - timedelta(minutes=10)).isoformat(),
            ))
            save_run_summary("maint-1", self._summary(
                run_source="maintenance",
                started_at=now.isoformat(),
            ))
            loaded = load_last_run_summary(run_sources=())

        assert loaded["run_id"] == "maint-1"

    def test_retention_prunes_old_summaries(self, tmp_path):
        old = datetime.now() - timedelta(hours=48)
        with self._patch_files(tmp_path):
            save_run_summary("ancient", self._summary(
                started_at=old.isoformat(),
                completed_at=old.isoformat(),
            ))
            save_run_summary("fresh", self._summary())
            with patch('core.activity._get_activity_retention_hours', return_value=24):
                summaries = load_run_summaries()

        assert "ancient" not in summaries
        assert "fresh" in summaries

    def test_legacy_migration(self, tmp_path):
        legacy_file = tmp_path / "last_run_summary.json"
        new_file = tmp_path / "run_summaries.json"
        legacy_payload = {
            "status": "completed",
            "timestamp": datetime.now().isoformat(),
            "files_cached": 7,
            "duration_seconds": 42.0,
        }
        legacy_file.write_text(json.dumps(legacy_payload, indent=2))

        with patch.multiple(
            'core.activity',
            RUN_SUMMARIES_FILE=new_file,
            _LEGACY_RUN_SUMMARY_FILE=legacy_file,
        ):
            summaries = load_run_summaries()

        assert len(summaries) == 1
        only_entry = next(iter(summaries.values()))
        assert only_entry["files_cached"] == 7
        assert only_entry["run_source"] == "legacy"
        # Legacy file removed after migration
        assert not legacy_file.exists()

    def test_missing_file_returns_none(self, tmp_path):
        with self._patch_files(tmp_path):
            assert load_last_run_summary() is None

    def test_malformed_json_returns_none(self, tmp_path):
        f = tmp_path / "run_summaries.json"
        f.write_text("{ not valid }")
        with self._patch_files(tmp_path):
            assert load_last_run_summary() is None


# ============================================================================
# TestRetentionHours
# ============================================================================

class TestRetentionHours:
    """Tests for _get_activity_retention_hours()."""

    def test_default_when_no_settings(self, tmp_path):
        f = tmp_path / "nope.json"
        with patch('core.activity.SETTINGS_FILE', f):
            assert _get_activity_retention_hours() == 24

    def test_reads_from_settings(self, tmp_path):
        f = tmp_path / "settings.json"
        f.write_text(json.dumps({"activity_retention_hours": 48}))
        with patch('core.activity.SETTINGS_FILE', f):
            assert _get_activity_retention_hours() == 48

    def test_fallback_on_malformed_json(self, tmp_path):
        f = tmp_path / "settings.json"
        f.write_text("broken")
        with patch('core.activity.SETTINGS_FILE', f):
            assert _get_activity_retention_hours() == 24
