"""Tests for operation runner - phase detection, progress counters, format helpers."""

import os
import sys
import time
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta

import pytest

# conftest.py handles fcntl/apscheduler mocking and path setup

# Mock web.config before importing operation_runner
sys.modules.setdefault('web.config', MagicMock(
    PROJECT_ROOT=MagicMock(),
    DATA_DIR=MagicMock(),
    SETTINGS_FILE=MagicMock(exists=MagicMock(return_value=False)),
    get_time_format=MagicMock(return_value='24h'),
))

from web.services.operation_runner import OperationRunner, OperationResult, OperationState


# ============================================================================
# Phase detection
# ============================================================================

class TestParsePhase:
    @pytest.fixture
    def runner(self):
        """Create a runner with a RUNNING result."""
        with patch('web.services.operation_runner.load_activity', return_value=[]):
            r = OperationRunner()
        r._current_result = OperationResult(
            state=OperationState.RUNNING,
            started_at=datetime.now(),
        )
        return r

    def test_starting_phase_default(self, runner):
        assert runner._current_result.current_phase == "starting"
        assert runner._current_result.current_phase_display == "Starting..."

    def test_fetching_phase(self, runner):
        runner._parse_phase("10:00:00 - INFO - --- Fetching Media ---")
        assert runner._current_result.current_phase == "fetching"
        assert runner._current_result.current_phase_display == "Fetching media..."

    def test_analyzing_phase(self, runner):
        runner._parse_phase("10:00:00 - INFO - Total media to cache: 5 files")
        assert runner._current_result.current_phase == "analyzing"

    def test_moving_phase(self, runner):
        runner._parse_phase("10:00:00 - INFO - --- Moving Files ---")
        assert runner._current_result.current_phase == "moving"

    def test_restoring_phase_from_returning(self, runner):
        runner._parse_phase("10:00:00 - INFO - Returning to array (3 episodes, instant via .plexcached):")
        assert runner._current_result.current_phase == "restoring"

    def test_restoring_phase_from_copying(self, runner):
        runner._parse_phase("10:00:00 - INFO - Copying to array (2 files, 1.5 GB):")
        assert runner._current_result.current_phase == "restoring"

    def test_caching_phase(self, runner):
        runner._parse_phase("10:00:00 - INFO - Caching to cache drive (5 files):")
        assert runner._current_result.current_phase == "caching"

    def test_evicting_phase(self, runner):
        runner._parse_phase("10:00:00 - INFO - Smart eviction: drive over limit, need to free 2.00GB")
        assert runner._current_result.current_phase == "evicting"

    def test_results_phase(self, runner):
        runner._parse_phase("10:00:00 - INFO - --- Results ---")
        assert runner._current_result.current_phase == "results"
        assert runner._current_result.current_phase_display == "Finishing up..."

    def test_dry_run_prefix(self, runner):
        runner._current_result.dry_run = True
        runner._parse_phase("10:00:00 - INFO - --- Fetching Media ---")
        assert runner._current_result.current_phase_display == "Dry Run: Fetching media..."

    def test_non_matching_line_no_change(self, runner):
        runner._parse_phase("10:00:00 - INFO - Some random log message")
        assert runner._current_result.current_phase == "starting"


# ============================================================================
# File count extraction
# ============================================================================

class TestCountExtraction:
    @pytest.fixture
    def runner(self):
        with patch('web.services.operation_runner.load_activity', return_value=[]):
            r = OperationRunner()
        r._current_result = OperationResult(
            state=OperationState.RUNNING,
            started_at=datetime.now(),
        )
        return r

    def test_restore_count_from_returning(self, runner):
        runner._parse_phase("10:00:00 - INFO - Returning to array (3 episodes, instant via .plexcached):")
        assert runner._current_result.files_to_restore_total == 3

    def test_restore_count_from_copying(self, runner):
        runner._parse_phase("10:00:00 - INFO - Copying to array (2 files, 1.5 GB):")
        assert runner._current_result.files_to_restore_total == 2

    def test_restore_counts_accumulate(self, runner):
        runner._parse_phase("10:00:00 - INFO - Returning to array (3 episodes, instant via .plexcached):")
        runner._parse_phase("10:00:00 - INFO - Copying to array (2 files, 1.5 GB):")
        assert runner._current_result.files_to_restore_total == 5

    def test_cache_count_from_caching_header(self, runner):
        runner._parse_phase("10:00:00 - INFO - Caching to cache drive (5 files):")
        assert runner._current_result.files_to_cache_total == 5

    def test_total_media_ignored_for_count(self, runner):
        """'Total media to cache' is the protection list, not actual moves — should not set count."""
        runner._parse_phase("10:00:00 - INFO - Total media to cache: 213 files")
        assert runner._current_result.files_to_cache_total == 0

    def test_cache_count_only_from_caching_header(self, runner):
        """Only 'Caching to cache drive (N files)' sets the actual move count."""
        runner._parse_phase("10:00:00 - INFO - Total media to cache: 213 files")
        runner._parse_phase("10:00:00 - INFO - Caching to cache drive (5 files):")
        assert runner._current_result.files_to_cache_total == 5


# ============================================================================
# Error counting
# ============================================================================

class TestErrorCounting:
    @pytest.fixture
    def runner(self):
        with patch('web.services.operation_runner.load_activity', return_value=[]):
            r = OperationRunner()
        r._current_result = OperationResult(
            state=OperationState.RUNNING,
            started_at=datetime.now(),
        )
        return r

    def test_error_increments_on_error(self, runner):
        runner._parse_phase("10:00:00 - ERROR - Something went wrong")
        assert runner._current_result.error_count == 1

    def test_error_increments_on_critical(self, runner):
        runner._parse_phase("10:00:00 - CRITICAL - Fatal error")
        assert runner._current_result.error_count == 1

    def test_multiple_errors(self, runner):
        runner._parse_phase("10:00:00 - ERROR - Error 1")
        runner._parse_phase("10:00:00 - ERROR - Error 2")
        runner._parse_phase("10:00:00 - CRITICAL - Error 3")
        assert runner._current_result.error_count == 3

    def test_info_does_not_increment(self, runner):
        runner._parse_phase("10:00:00 - INFO - Everything is fine")
        assert runner._current_result.error_count == 0


# ============================================================================
# File operation counter increments
# ============================================================================

class TestFileOperationCounters:
    @pytest.fixture
    def runner(self):
        with patch('web.services.operation_runner.load_activity', return_value=[]):
            r = OperationRunner()
        r._current_result = OperationResult(
            state=OperationState.RUNNING,
            started_at=datetime.now(),
        )
        return r

    def test_cached_action_increments(self, runner):
        msg = "10:00:00 - INFO -   [Cached] movie.mkv (2.5 GB)"
        with patch.object(runner, '_save_activity'):
            with patch.object(runner, '_get_users_for_file', return_value=[]):
                runner._parse_file_operation(msg)

        assert runner._current_result.files_cached_so_far == 1
        assert runner._current_result.bytes_cached_so_far > 0
        assert runner._current_result.last_completed_file == "movie.mkv"

    def test_restored_action_increments(self, runner):
        msg = "10:00:00 - INFO -   [Restored] episode.mkv (500 MB)"
        with patch.object(runner, '_save_activity'):
            runner._parse_file_operation(msg)

        assert runner._current_result.files_restored_so_far == 1
        assert runner._current_result.bytes_restored_so_far > 0
        assert runner._current_result.last_completed_file == "episode.mkv"

    def test_moved_action_increments_restored(self, runner):
        msg = "10:00:00 - INFO -   [Moved] show.mkv (1 GB)"
        with patch.object(runner, '_save_activity'):
            runner._parse_file_operation(msg)

        assert runner._current_result.files_restored_so_far == 1
        assert runner._current_result.last_completed_file == "show.mkv"

    def test_multiple_actions_accumulate(self, runner):
        with patch.object(runner, '_save_activity'):
            with patch.object(runner, '_get_users_for_file', return_value=[]):
                runner._parse_file_operation("10:00:00 - INFO -   [Cached] m1.mkv (1 GB)")
                runner._parse_file_operation("10:00:00 - INFO -   [Cached] m2.mkv (2 GB)")
                runner._parse_file_operation("10:00:00 - INFO -   [Restored] r1.mkv (500 MB)")

        assert runner._current_result.files_cached_so_far == 2
        assert runner._current_result.files_restored_so_far == 1


# ============================================================================
# get_status_dict() — running state
# ============================================================================

class TestGetStatusDictRunning:
    @pytest.fixture
    def runner(self):
        with patch('web.services.operation_runner.load_activity', return_value=[]):
            r = OperationRunner()
        r._state = OperationState.RUNNING
        r._current_result = OperationResult(
            state=OperationState.RUNNING,
            started_at=datetime.now() - timedelta(minutes=2),
            dry_run=False,
            current_phase="caching",
            current_phase_display="Caching to drive...",
            files_to_cache_total=5,
            files_to_restore_total=3,
            files_cached_so_far=2,
            files_restored_so_far=3,
            bytes_cached_so_far=2 * 1024**3,
            bytes_restored_so_far=1 * 1024**3,
            last_completed_file="movie.mkv",
            error_count=1,
        )
        r._log_messages = ["log1", "log2", "log3", "log4", "log5", "log6"]
        return r

    def test_running_state_fields(self, runner):
        status = runner.get_status_dict()
        assert status["state"] == "running"
        assert status["is_running"] is True
        assert status["current_phase"] == "caching"
        assert status["current_phase_display"] == "Caching to drive..."
        assert status["total_files"] == 8
        assert status["completed_files"] == 5
        assert status["progress_percent"] == 62  # 5/8 = 62.5 -> 62
        assert status["error_count"] == 1
        assert status["last_completed_file"] == "movie.mkv"

    def test_running_state_has_elapsed(self, runner):
        status = runner.get_status_dict()
        assert "elapsed_display" in status
        assert status["elapsed_display"]  # Should be non-empty

    def test_running_state_has_eta(self, runner):
        status = runner.get_status_dict()
        assert "eta_display" in status
        assert status["eta_display"]  # Should be non-empty since completed > 0

    def test_running_state_has_bytes_display(self, runner):
        status = runner.get_status_dict()
        assert "bytes_display" in status
        assert "GB" in status["bytes_display"]

    def test_running_state_has_recent_logs(self, runner):
        status = runner.get_status_dict()
        assert "recent_logs" in status
        assert len(status["recent_logs"]) == 5  # Last 5 of 6
        assert status["recent_logs"][0] == "log2"

    def test_progress_zero_when_no_totals(self, runner):
        runner._current_result.files_to_cache_total = 0
        runner._current_result.files_to_restore_total = 0
        status = runner.get_status_dict()
        assert status["progress_percent"] == 0
        assert status["total_files"] == 0


# ============================================================================
# get_status_dict() — completed state
# ============================================================================

class TestGetStatusDictCompleted:
    @pytest.fixture
    def runner(self):
        with patch('web.services.operation_runner.load_activity', return_value=[]):
            r = OperationRunner()
        r._state = OperationState.COMPLETED
        r._current_result = OperationResult(
            state=OperationState.COMPLETED,
            started_at=datetime.now() - timedelta(minutes=2),
            completed_at=datetime.now(),
            duration_seconds=123.5,
            files_cached=5,
            files_restored=3,
            bytes_cached=2 * 1024**3,
            bytes_restored=1 * 1024**3,
            error_count=0,
        )
        return r

    def test_completed_has_duration_display(self, runner):
        status = runner.get_status_dict()
        assert status["duration_display"] == "2m 03s"

    def test_completed_has_bytes_display(self, runner):
        status = runner.get_status_dict()
        assert "GB" in status["bytes_cached_display"]
        assert "GB" in status["bytes_restored_display"]

    def test_completed_zero_bytes_empty_string(self, runner):
        runner._current_result.bytes_cached = 0
        runner._current_result.bytes_restored = 0
        status = runner.get_status_dict()
        assert status["bytes_cached_display"] == ""
        assert status["bytes_restored_display"] == ""

    def test_completed_error_count(self, runner):
        runner._current_result.error_count = 3
        status = runner.get_status_dict()
        assert status["error_count"] == 3


# ============================================================================
# get_status_dict() — byte-level progress
# ============================================================================

class TestGetStatusDictByteProgress:
    @pytest.fixture
    def runner(self):
        with patch('web.services.operation_runner.load_activity', return_value=[]):
            r = OperationRunner()
        r._state = OperationState.RUNNING
        r._current_result = OperationResult(
            state=OperationState.RUNNING,
            started_at=datetime.now() - timedelta(minutes=2),
            dry_run=False,
            current_phase="caching",
            current_phase_display="Caching to drive...",
            files_to_cache_total=4,
            files_to_restore_total=0,
            files_cached_so_far=1,
            files_restored_so_far=0,
            bytes_cached_so_far=1 * 1024**3,
        )
        r._log_messages = ["log1"]
        return r

    def test_byte_level_progress_overrides_file_level(self, runner):
        runner._current_result.cumulative_bytes_copied = 500 * 1024**2  # 500 MB
        runner._current_result.cumulative_bytes_total = 1024 * 1024**2  # 1 GB
        status = runner.get_status_dict()
        # Byte-level: 500MB / 1GB = ~48% (not file-level 1/4 = 25%)
        assert status["progress_percent"] == 48

    def test_byte_level_bytes_display_format(self, runner):
        runner._current_result.cumulative_bytes_copied = 500 * 1024**2  # 500 MB
        runner._current_result.cumulative_bytes_total = 1 * 1024**3  # 1 GB
        status = runner.get_status_dict()
        assert "/" in status["bytes_display"]
        assert "MB" in status["bytes_display"] or "GB" in status["bytes_display"]

    def test_byte_level_eta_from_rate(self, runner):
        runner._current_result.cumulative_bytes_copied = 500 * 1024**2
        runner._current_result.cumulative_bytes_total = 1 * 1024**3
        runner._current_result.batch_bytes_copied = 500 * 1024**2
        runner._current_result.batch_copy_start_time = time.time() - 10  # 10 seconds ago
        status = runner.get_status_dict()
        # ETA should be non-empty since we have batch rate data
        assert status["eta_display"] != ""

    def test_no_byte_data_falls_back_to_file_level(self, runner):
        # cumulative_bytes_total defaults to 0 — byte-level override should not trigger
        assert runner._current_result.cumulative_bytes_total == 0
        status = runner.get_status_dict()
        # File-level: 1/4 = 25%
        assert status["progress_percent"] == 25


# ============================================================================
# get_status_dict() — idle state
# ============================================================================

class TestGetStatusDictIdle:
    def test_idle_when_no_result(self):
        with patch('web.services.operation_runner.load_activity', return_value=[]):
            runner = OperationRunner()
        status = runner.get_status_dict()
        assert status["state"] == "idle"
        assert status["is_running"] is False
