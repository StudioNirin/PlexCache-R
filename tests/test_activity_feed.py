"""Tests for activity feed persistence — FileActivity, load_activity, save_activity.

CRITICAL: Both OperationRunner and MaintenanceRunner write to the same file.
The load-merge-save pattern must never lose entries from concurrent writers.

Source: web/services/operation_runner.py (FileActivity, load_activity, save_activity)
"""

import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# conftest.py handles fcntl/apscheduler mocking and path setup

# Mock web.config before importing operation_runner
sys.modules.setdefault('web.config', MagicMock(
    PROJECT_ROOT=MagicMock(),
    DATA_DIR=MagicMock(),
    SETTINGS_FILE=MagicMock(exists=MagicMock(return_value=False)),
    get_time_format=MagicMock(return_value='24h'),
))

from web.services.operation_runner import (
    FileActivity,
    load_activity,
    save_activity,
    MAX_RECENT_ACTIVITY,
    ACTIVITY_FILE,
)


# ============================================================================
# TestFileActivity
# ============================================================================

class TestFileActivity:
    """Tests for FileActivity.to_dict() serialization."""

    def test_to_dict_24h_format(self):
        """to_dict() uses 24h time format when configured."""
        with patch('web.services.operation_runner.get_time_format', return_value='24h'):
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
        """to_dict() uses 12h time format with AM/PM when configured."""
        with patch('web.services.operation_runner.get_time_format', return_value='12h'):
            fa = FileActivity(
                timestamp=datetime(2026, 1, 15, 14, 30, 5),
                action="Cached",
                filename="movie.mkv",
            )
            d = fa.to_dict()

        assert "PM" in d['time_display']

    def test_zero_size_dash_display(self):
        """Zero-byte files show dash instead of '0 B'."""
        with patch('web.services.operation_runner.get_time_format', return_value='24h'):
            fa = FileActivity(
                timestamp=datetime.now(),
                action="Protected",
                filename="file.mkv",
                size_bytes=0,
            )
            d = fa.to_dict()

        assert d['size'] == "-"

    def test_all_fields_present(self):
        """to_dict() includes all required fields."""
        with patch('web.services.operation_runner.get_time_format', return_value='24h'):
            fa = FileActivity(
                timestamp=datetime.now(),
                action="Cached",
                filename="test.mkv",
                size_bytes=100,
                users=["bob"],
            )
            d = fa.to_dict()

        required_keys = {'timestamp', 'time_display', 'date_key', 'date_display', 'action', 'filename', 'size', 'users'}
        assert required_keys == set(d.keys())


# ============================================================================
# TestLoadActivity
# ============================================================================

class TestLoadActivity:
    """Tests for load_activity() disk persistence."""

    def test_missing_file_returns_empty(self, tmp_path):
        """Returns empty list when activity file doesn't exist."""
        with patch('web.services.operation_runner.ACTIVITY_FILE', tmp_path / "nope.json"):
            result = load_activity()
        assert result == []

    def test_empty_file_returns_empty(self, tmp_path):
        """Returns empty list when activity file is empty."""
        f = tmp_path / "activity.json"
        f.write_text("")
        with patch('web.services.operation_runner.ACTIVITY_FILE', f):
            result = load_activity()
        assert result == []

    def test_valid_entries_loaded(self, tmp_path):
        """Loads valid activity entries from JSON."""
        now = datetime.now()
        data = [
            {"timestamp": now.isoformat(), "action": "Cached", "filename": "a.mkv", "size_bytes": 100, "users": []},
            {"timestamp": now.isoformat(), "action": "Restored", "filename": "b.mkv", "size_bytes": 200, "users": ["u1"]},
        ]
        f = tmp_path / "activity.json"
        f.write_text(json.dumps(data, indent=2))

        with patch('web.services.operation_runner.ACTIVITY_FILE', f):
            with patch('web.services.operation_runner._get_activity_retention_hours', return_value=24):
                result = load_activity()

        assert len(result) == 2
        assert result[0].filename in ("a.mkv", "b.mkv")

    def test_retention_filtering(self, tmp_path):
        """Entries older than retention period are filtered out."""
        now = datetime.now()
        old = now - timedelta(hours=48)
        data = [
            {"timestamp": now.isoformat(), "action": "Cached", "filename": "new.mkv"},
            {"timestamp": old.isoformat(), "action": "Cached", "filename": "old.mkv"},
        ]
        f = tmp_path / "activity.json"
        f.write_text(json.dumps(data, indent=2))

        with patch('web.services.operation_runner.ACTIVITY_FILE', f):
            with patch('web.services.operation_runner._get_activity_retention_hours', return_value=24):
                result = load_activity()

        assert len(result) == 1
        assert result[0].filename == "new.mkv"

    def test_malformed_entries_skipped(self, tmp_path):
        """Malformed entries are skipped without crashing."""
        now = datetime.now()
        data = [
            {"timestamp": now.isoformat(), "action": "Cached", "filename": "ok.mkv"},
            {"bad_key": "missing required fields"},
            {"timestamp": "not-a-date", "action": "Cached", "filename": "bad.mkv"},
        ]
        f = tmp_path / "activity.json"
        f.write_text(json.dumps(data, indent=2))

        with patch('web.services.operation_runner.ACTIVITY_FILE', f):
            with patch('web.services.operation_runner._get_activity_retention_hours', return_value=24):
                result = load_activity()

        assert len(result) == 1
        assert result[0].filename == "ok.mkv"

    def test_malformed_json(self, tmp_path):
        """Handles malformed JSON gracefully."""
        f = tmp_path / "activity.json"
        f.write_text("{ not valid json }")

        with patch('web.services.operation_runner.ACTIVITY_FILE', f):
            result = load_activity()

        assert result == []

    def test_sorted_newest_first(self, tmp_path):
        """Results are sorted by timestamp, newest first."""
        now = datetime.now()
        data = [
            {"timestamp": (now - timedelta(hours=2)).isoformat(), "action": "Cached", "filename": "older.mkv"},
            {"timestamp": now.isoformat(), "action": "Cached", "filename": "newest.mkv"},
            {"timestamp": (now - timedelta(hours=1)).isoformat(), "action": "Cached", "filename": "middle.mkv"},
        ]
        f = tmp_path / "activity.json"
        f.write_text(json.dumps(data, indent=2))

        with patch('web.services.operation_runner.ACTIVITY_FILE', f):
            with patch('web.services.operation_runner._get_activity_retention_hours', return_value=24):
                result = load_activity()

        assert result[0].filename == "newest.mkv"
        assert result[1].filename == "middle.mkv"
        assert result[2].filename == "older.mkv"

    def test_capped_at_max(self, tmp_path):
        """Results are capped at MAX_RECENT_ACTIVITY entries."""
        now = datetime.now()
        data = [
            {"timestamp": (now - timedelta(seconds=i)).isoformat(), "action": "Cached", "filename": f"f{i}.mkv"}
            for i in range(MAX_RECENT_ACTIVITY + 50)
        ]
        f = tmp_path / "activity.json"
        f.write_text(json.dumps(data, indent=2))

        with patch('web.services.operation_runner.ACTIVITY_FILE', f):
            with patch('web.services.operation_runner._get_activity_retention_hours', return_value=9999):
                result = load_activity()

        assert len(result) == MAX_RECENT_ACTIVITY


# ============================================================================
# TestSaveActivity
# ============================================================================

class TestSaveActivity:
    """Tests for save_activity() disk persistence."""

    def test_valid_json_with_indent(self, tmp_path):
        """Saves valid JSON with indent=2 formatting."""
        f = tmp_path / "activity.json"
        activities = [
            FileActivity(timestamp=datetime.now(), action="Cached", filename="a.mkv", size_bytes=100),
        ]

        with patch('web.services.operation_runner.ACTIVITY_FILE', f):
            with patch('web.services.operation_runner._get_activity_retention_hours', return_value=24):
                save_activity(activities)

        content = f.read_text()
        data = json.loads(content)
        assert len(data) == 1
        assert data[0]['action'] == "Cached"
        # Check indent=2 formatting (lines should start with spaces, not tabs)
        assert '  "' in content

    def test_retention_filtering_on_save(self, tmp_path):
        """Old entries are filtered out during save."""
        f = tmp_path / "activity.json"
        now = datetime.now()
        activities = [
            FileActivity(timestamp=now, action="Cached", filename="new.mkv"),
            FileActivity(timestamp=now - timedelta(hours=48), action="Cached", filename="old.mkv"),
        ]

        with patch('web.services.operation_runner.ACTIVITY_FILE', f):
            with patch('web.services.operation_runner._get_activity_retention_hours', return_value=24):
                save_activity(activities)

        data = json.loads(f.read_text())
        assert len(data) == 1
        assert data[0]['filename'] == "new.mkv"

    def test_creates_parent_dir(self, tmp_path):
        """Creates parent directory if it doesn't exist."""
        f = tmp_path / "subdir" / "activity.json"
        activities = [
            FileActivity(timestamp=datetime.now(), action="Cached", filename="a.mkv"),
        ]

        with patch('web.services.operation_runner.ACTIVITY_FILE', f):
            with patch('web.services.operation_runner._get_activity_retention_hours', return_value=24):
                save_activity(activities)

        assert f.exists()

    def test_round_trip_preservation(self, tmp_path):
        """Save then load preserves all entry fields."""
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

        with patch('web.services.operation_runner.ACTIVITY_FILE', f):
            with patch('web.services.operation_runner._get_activity_retention_hours', return_value=24):
                save_activity(original)
                loaded = load_activity()

        assert len(loaded) == 1
        assert loaded[0].action == "Cached"
        assert loaded[0].filename == "movie.mkv"
        assert loaded[0].size_bytes == 1024
        assert loaded[0].users == ["alice", "bob"]


# ============================================================================
# TestMergePattern
# ============================================================================

class TestMergePattern:
    """Tests for the concurrent-writer merge pattern.

    CRITICAL: Both OperationRunner and MaintenanceRunner write to the same
    activity file. The correct pattern is load-merge-save, not overwrite.
    """

    def test_new_entry_merged_with_existing(self, tmp_path):
        """New entries merge with existing disk data, not replace it."""
        f = tmp_path / "activity.json"
        now = datetime.now()

        # Writer A saves initial data
        existing = [
            FileActivity(timestamp=now - timedelta(seconds=10), action="Cached", filename="writer_a.mkv"),
        ]
        with patch('web.services.operation_runner.ACTIVITY_FILE', f):
            with patch('web.services.operation_runner._get_activity_retention_hours', return_value=24):
                save_activity(existing)

        # Writer B loads, adds entry, saves
        with patch('web.services.operation_runner.ACTIVITY_FILE', f):
            with patch('web.services.operation_runner._get_activity_retention_hours', return_value=24):
                loaded = load_activity()
                loaded.append(FileActivity(
                    timestamp=now, action="Restored", filename="writer_b.mkv",
                ))
                save_activity(loaded)

        # Both entries should be present
        data = json.loads(f.read_text())
        filenames = {e['filename'] for e in data}
        assert "writer_a.mkv" in filenames
        assert "writer_b.mkv" in filenames

    def test_cap_applied_after_merge(self, tmp_path):
        """After merging, total entries stay within MAX_RECENT_ACTIVITY."""
        f = tmp_path / "activity.json"
        now = datetime.now()

        # Pre-fill with MAX entries
        existing = [
            FileActivity(
                timestamp=now - timedelta(seconds=i),
                action="Cached",
                filename=f"existing_{i}.mkv",
            )
            for i in range(MAX_RECENT_ACTIVITY)
        ]

        with patch('web.services.operation_runner.ACTIVITY_FILE', f):
            with patch('web.services.operation_runner._get_activity_retention_hours', return_value=9999):
                save_activity(existing)

                # Load, add one more, save
                loaded = load_activity()
                assert len(loaded) == MAX_RECENT_ACTIVITY

                loaded.insert(0, FileActivity(
                    timestamp=now + timedelta(seconds=1),
                    action="Cached",
                    filename="overflow.mkv",
                ))
                save_activity(loaded)

                # Reload — should still be capped
                final = load_activity()
                assert len(final) == MAX_RECENT_ACTIVITY


# ============================================================================
# Atomic activity save — _save_activity() holds lock for full sequence
# ============================================================================

class TestSaveActivityAtomicity:
    """Verify OperationRunner._save_activity() uses unlocked helpers under a single lock
    acquisition to prevent race conditions with MaintenanceRunner."""

    def test_save_activity_uses_unlocked_helpers(self, tmp_path):
        """_save_activity(new_entry) must call _load_activity_unlocked and
        _save_activity_unlocked (not the locking versions)."""
        from web.services.operation_runner import OperationRunner

        activity_file = tmp_path / "recent_activity.json"
        activity_file.write_text("[]", encoding="utf-8")

        entry = FileActivity(
            timestamp=datetime.now(),
            action="Cached",
            filename="test.mkv",
            size_bytes=100,
        )

        with patch('web.services.operation_runner.ACTIVITY_FILE', activity_file), \
             patch('web.services.operation_runner._load_activity_unlocked', return_value=[]) as mock_load, \
             patch('web.services.operation_runner._save_activity_unlocked') as mock_save, \
             patch('web.services.operation_runner.load_activity', return_value=[]):
            runner = OperationRunner()
            runner._save_activity(new_entry=entry)

        # Must use unlocked helpers (caller holds the lock)
        mock_load.assert_called_once()
        mock_save.assert_called_once()
        # Verify the entry was inserted
        saved_activities = mock_save.call_args[0][0]
        assert saved_activities[0] is entry

    def test_save_activity_without_entry_uses_public_save(self, tmp_path):
        """_save_activity() without new_entry uses the public save_activity()."""
        from web.services.operation_runner import OperationRunner

        with patch('web.services.operation_runner.save_activity') as mock_pub_save, \
             patch('web.services.operation_runner.load_activity', return_value=[]):
            runner = OperationRunner()
            runner._recent_activity = []
            runner._save_activity(new_entry=None)

        mock_pub_save.assert_called_once()


# ============================================================================
# Atomic last-run summary write
# ============================================================================

class TestLastRunSummaryAtomicWrite:
    """Verify _save_last_run_summary() uses save_json_atomically()."""

    def test_summary_written_atomically(self, tmp_path):
        """Last run summary must use save_json_atomically(), not direct json.dump()."""
        from web.services.operation_runner import (
            OperationRunner, OperationResult, OperationState,
        )

        summary_file = tmp_path / "last_run_summary.json"

        with patch('web.services.operation_runner.load_activity', return_value=[]), \
             patch('web.services.operation_runner.LAST_RUN_SUMMARY_FILE', summary_file), \
             patch('web.services.operation_runner.save_json_atomically') as mock_atomic:
            runner = OperationRunner()
            runner._current_result = OperationResult(
                state=OperationState.COMPLETED,
                started_at=datetime.now(),
                files_cached=3,
                files_restored=1,
                bytes_cached=1000,
                bytes_restored=500,
                duration_seconds=10.5,
            )
            runner._save_last_run_summary()

        assert mock_atomic.called, "save_json_atomically was not called for last run summary"
        call_args = mock_atomic.call_args
        # Check positional or keyword label arg
        positional = call_args[0]
        keyword = call_args[1]
        label = keyword.get("label") if "label" in keyword else (positional[2] if len(positional) > 2 else None)
        assert label == "last run summary", f"Expected label 'last run summary', got {label!r}"
