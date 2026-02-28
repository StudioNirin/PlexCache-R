"""Tests for log parsing functions used by the log viewer and WebSocket.

Source: parse_log_line(), parse_log_content(), _detect_phase() in web/routers/logs.py
"""

import os
import sys
from unittest.mock import MagicMock

# conftest.py handles fcntl/apscheduler mocking and path setup

# Mock web.config before importing logs module (same pattern as test_operation_runner.py)
sys.modules.setdefault('web.config', MagicMock(
    templates=MagicMock(),
    LOGS_DIR=MagicMock(),
))

from web.routers.logs import parse_log_line, parse_log_content, _detect_phase, _LOG_LINE_RE


# ============================================================================
# TestLogLineRegex
# ============================================================================

class TestLogLineRegex:
    """Tests for the _LOG_LINE_RE regex pattern."""

    def test_24h_format(self):
        """Matches 24-hour timestamp format."""
        m = _LOG_LINE_RE.match("14:30:05 - INFO - Starting operation")
        assert m is not None
        assert m.group(1).strip() == "14:30:05"
        assert m.group(2) == "INFO"

    def test_12h_format(self):
        """Matches 12-hour timestamp format with AM/PM."""
        m = _LOG_LINE_RE.match("2:30:05 PM - INFO - Starting operation")
        assert m is not None
        assert "PM" in m.group(1)

    def test_all_levels(self):
        """Matches all supported log levels."""
        for level in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL", "SUMMARY"]:
            m = _LOG_LINE_RE.match(f"10:00:00 - {level} - test message")
            assert m is not None, f"Failed to match level: {level}"
            assert m.group(2).upper() == level

    def test_case_insensitive(self):
        """Matches log levels regardless of case."""
        m = _LOG_LINE_RE.match("10:00:00 - info - test message")
        assert m is not None
        assert m.group(2).upper() == "INFO"

    def test_non_log_lines_dont_match(self):
        """Non-log lines (tracebacks, etc.) don't match the regex."""
        assert _LOG_LINE_RE.match("  File '/app/core/app.py', line 42") is None
        assert _LOG_LINE_RE.match("Traceback (most recent call last):") is None
        assert _LOG_LINE_RE.match("") is None

    def test_leading_zero_timestamps(self):
        """Matches timestamps with leading zeros."""
        m = _LOG_LINE_RE.match("01:05:09 - DEBUG - early morning log")
        assert m is not None
        assert m.group(1).strip() == "01:05:09"


# ============================================================================
# TestDetectPhase
# ============================================================================

class TestDetectPhase:
    """Tests for _detect_phase() phase detection from log messages."""

    def test_fetching_phase(self):
        """Detects fetching phase from marker."""
        assert _detect_phase("--- Fetching Media ---", "") == "fetching"

    def test_analyzing_phase(self):
        """Detects analyzing phase from marker."""
        assert _detect_phase("Total media to cache: 5 files", "") == "analyzing"

    def test_moving_phase(self):
        """Detects moving phase from marker."""
        assert _detect_phase("--- Moving Files ---", "") == "moving"

    def test_restoring_phase(self):
        """Detects restoring phase from 'Returning to array' marker."""
        assert _detect_phase("Returning to array: movie.mkv", "") == "restoring"

    def test_restoring_phase_copy_variant(self):
        """Detects restoring phase from 'Copying to array' marker."""
        assert _detect_phase("Copying to array: movie.mkv", "") == "restoring"

    def test_caching_phase(self):
        """Detects caching phase from marker."""
        assert _detect_phase("Caching to cache drive: movie.mkv", "") == "caching"

    def test_evicting_phase(self):
        """Detects evicting phase from marker."""
        assert _detect_phase("Smart eviction starting", "") == "evicting"

    def test_results_phase(self):
        """Detects results phase from marker."""
        assert _detect_phase("--- Results ---", "") == "results"

    def test_non_marker_preserves_current_phase(self):
        """Non-marker message preserves the current phase."""
        assert _detect_phase("Processed file: movie.mkv", "caching") == "caching"
        assert _detect_phase("Some random log line", "fetching") == "fetching"
        assert _detect_phase("", "") == ""


# ============================================================================
# TestParseLogLine
# ============================================================================

class TestParseLogLine:
    """Tests for parse_log_line() structured log parsing."""

    def test_standard_line_fields(self):
        """Parses a standard log line into all expected fields."""
        result = parse_log_line("14:30:05 - INFO - Starting operation", "")

        assert result['timestamp'] == "14:30:05"
        assert result['level'] == "INFO"
        assert result['message'] == "Starting operation"
        assert result['is_continuation'] is False
        assert result['raw'] == "14:30:05 - INFO - Starting operation"

    def test_continuation_line(self):
        """Non-matching lines become continuation lines."""
        result = parse_log_line("  File '/app/core/app.py', line 42", "caching")

        assert result['is_continuation'] is True
        assert result['timestamp'] == ''
        assert result['level'] == ''
        assert result['phase'] == "caching"
        assert result['message'] == "  File '/app/core/app.py', line 42"

    def test_empty_message(self):
        """Parses a log line with minimal message content."""
        result = parse_log_line("10:00:00 - DEBUG - ", "")

        assert result['level'] == "DEBUG"
        assert result['message'] == ""
        assert result['is_continuation'] is False

    def test_phase_detection_from_message(self):
        """Phase is detected from message content."""
        result = parse_log_line("10:00:00 - INFO - --- Fetching Media ---", "")

        assert result['phase'] == "fetching"

    def test_phase_preserved_when_no_marker(self):
        """Current phase is preserved when message has no phase marker."""
        result = parse_log_line("10:00:00 - INFO - Processing file", "caching")

        assert result['phase'] == "caching"


# ============================================================================
# TestParseLogContent
# ============================================================================

class TestParseLogContent:
    """Tests for parse_log_content() full log parsing."""

    def test_multi_line_parsing(self):
        """Parses multiple log lines into structured data."""
        text = "10:00:00 - INFO - Line one\n10:00:01 - DEBUG - Line two"
        lines, counts = parse_log_content(text)

        assert len(lines) == 2
        assert counts['INFO'] == 1
        assert counts['DEBUG'] == 1

    def test_empty_lines_filtered(self):
        """Empty lines are skipped during parsing."""
        text = "10:00:00 - INFO - Line one\n\n\n10:00:01 - DEBUG - Line two\n"
        lines, counts = parse_log_content(text)

        assert len(lines) == 2

    def test_continuation_inherits_metadata(self):
        """Continuation lines inherit level and timestamp from previous line."""
        text = "10:00:00 - ERROR - Something failed\n  Traceback line here"
        lines, counts = parse_log_content(text)

        assert len(lines) == 2
        assert lines[1]['is_continuation'] is True
        assert lines[1]['level'] == 'ERROR'
        assert lines[1]['timestamp'] == '10:00:00'

    def test_counts_exclude_continuations(self):
        """Level counts only include non-continuation lines."""
        text = "10:00:00 - ERROR - Failure\n  traceback line 1\n  traceback line 2"
        lines, counts = parse_log_content(text)

        assert counts['ERROR'] == 1  # Only the original line counted

    def test_phase_persists_across_lines(self):
        """Phase detected in one line persists to subsequent lines."""
        text = (
            "10:00:00 - INFO - --- Fetching Media ---\n"
            "10:00:01 - INFO - Fetching OnDeck\n"
            "10:00:02 - INFO - Fetching Watchlist"
        )
        lines, counts = parse_log_content(text)

        assert lines[0]['phase'] == 'fetching'
        assert lines[1]['phase'] == 'fetching'
        assert lines[2]['phase'] == 'fetching'

    def test_phase_changes_on_new_marker(self):
        """Phase updates when a new phase marker is encountered."""
        text = (
            "10:00:00 - INFO - --- Fetching Media ---\n"
            "10:00:01 - INFO - Some fetch work\n"
            "10:00:02 - INFO - Total media to cache: 5 files\n"
            "10:00:03 - INFO - Analyzing priorities"
        )
        lines, counts = parse_log_content(text)

        assert lines[0]['phase'] == 'fetching'
        assert lines[1]['phase'] == 'fetching'
        assert lines[2]['phase'] == 'analyzing'
        assert lines[3]['phase'] == 'analyzing'

    def test_malformed_lines_as_continuation(self):
        """Lines that don't match log format are treated as continuations."""
        text = "Not a log line at all\n10:00:00 - INFO - Real log"
        lines, counts = parse_log_content(text)

        assert len(lines) == 2
        assert lines[0]['is_continuation'] is True
        assert lines[1]['is_continuation'] is False

    def test_all_level_counts_initialized(self):
        """All level count keys are present even with empty input."""
        lines, counts = parse_log_content("")

        assert len(lines) == 0
        for level in ['ERROR', 'WARNING', 'INFO', 'DEBUG', 'SUMMARY', 'CRITICAL']:
            assert counts[level] == 0

    def test_summary_level_counted(self):
        """SUMMARY level lines are counted correctly."""
        text = "10:00:00 - SUMMARY - Operation complete: 5 cached, 2 restored"
        lines, counts = parse_log_content(text)

        assert counts['SUMMARY'] == 1
        assert lines[0]['level'] == 'SUMMARY'
