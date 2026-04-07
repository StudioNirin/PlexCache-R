"""Shared activity writer — CLI and Web UI both write here.

Provides file activity recording, last-run timestamps, and run summaries
that the Web UI dashboard reads. This module has NO web framework imports
so it can be used from core/app.py (CLI path) as well as from the web layer.

Both CLI runs and web-triggered runs write to the same files:
  - data/recent_activity.json   (per-file activity feed)
  - data/last_run.txt           (last run timestamp)
  - data/last_run_summary.json  (run statistics)
"""

import json
import logging
import os
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional
from dataclasses import dataclass, field

from core.system_utils import format_bytes
from core.file_operations import save_json_atomically


# ---------------------------------------------------------------------------
# Path resolution (mirrors web/config.py logic, no web imports)
# ---------------------------------------------------------------------------

def _is_docker() -> bool:
    return os.path.exists("/.dockerenv") or os.path.exists("/run/.containerenv")


def _get_project_root() -> Path:
    """Project root: parent of core/."""
    return Path(__file__).parent.parent


def _get_config_dir() -> Path:
    return Path("/config") if _is_docker() else _get_project_root()


def _get_data_dir() -> Path:
    return _get_config_dir() / "data"


def _get_settings_file() -> Path:
    return _get_config_dir() / "plexcache_settings.json"


# Resolve once at import time (same lifetime as the process)
DATA_DIR = _get_data_dir()
SETTINGS_FILE = _get_settings_file()

# File paths
ACTIVITY_FILE = DATA_DIR / "recent_activity.json"
LAST_RUN_FILE = DATA_DIR / "last_run.txt"
LAST_RUN_SUMMARY_FILE = DATA_DIR / "last_run_summary.json"

# Defaults
DEFAULT_ACTIVITY_RETENTION_HOURS = 24
MAX_RECENT_ACTIVITY = 500

# Thread lock for concurrent access to activity file
_activity_file_lock = threading.Lock()

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Settings helpers (no web.config dependency)
# ---------------------------------------------------------------------------

def get_time_format() -> str:
    """Read time_format from settings JSON. Returns '12h' or '24h' (default)."""
    try:
        if SETTINGS_FILE.exists():
            with open(SETTINGS_FILE, 'r', encoding='utf-8') as f:
                settings = json.load(f)
            fmt = settings.get("time_format", "24h")
            if fmt in ("12h", "24h"):
                return fmt
    except (json.JSONDecodeError, IOError):
        pass
    return "24h"


def _get_activity_retention_hours() -> int:
    """Load activity retention hours from settings, with fallback to default."""
    try:
        if SETTINGS_FILE.exists():
            with open(SETTINGS_FILE, 'r', encoding='utf-8') as f:
                settings = json.load(f)
            return settings.get('activity_retention_hours', DEFAULT_ACTIVITY_RETENTION_HOURS)
    except (json.JSONDecodeError, IOError):
        pass
    return DEFAULT_ACTIVITY_RETENTION_HOURS


# ---------------------------------------------------------------------------
# FileActivity dataclass
# ---------------------------------------------------------------------------

@dataclass
class FileActivity:
    """Represents a file operation (cached, restored, protected, etc.)."""
    timestamp: datetime
    action: str  # "Cached", "Restored", "Protected", "Moved to Array", etc.
    filename: str
    size_bytes: int = 0
    users: List[str] = field(default_factory=list)
    associated_files: List[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        fmt = get_time_format()
        if fmt == "12h":
            time_display = self.timestamp.strftime("%-I:%M:%S %p")
        else:
            time_display = self.timestamp.strftime("%H:%M:%S")

        # Date grouping fields (computed at render time, not stored on disk)
        today = datetime.now().date()
        entry_date = self.timestamp.date()
        if entry_date == today:
            date_display = "Today"
        elif entry_date == today - timedelta(days=1):
            date_display = "Yesterday"
        else:
            date_display = self.timestamp.strftime("%a, %b ") + str(self.timestamp.day)

        result = {
            "timestamp": self.timestamp.isoformat(),
            "time_display": time_display,
            "date_key": entry_date.isoformat(),
            "date_display": date_display,
            "action": self.action,
            "filename": self.filename,
            "size": self._format_size(self.size_bytes),
            "users": self.users,
        }
        if self.associated_files:
            result["associated_files"] = self.associated_files
        return result

    def _format_size(self, size_bytes: int) -> str:
        if size_bytes == 0:
            return "-"
        return format_bytes(size_bytes)


# ---------------------------------------------------------------------------
# Activity persistence (load / save)
# ---------------------------------------------------------------------------

def _load_activity_unlocked() -> List[FileActivity]:
    """Load activity from disk without acquiring _activity_file_lock.

    Caller MUST hold _activity_file_lock.
    """
    try:
        if not ACTIVITY_FILE.exists():
            return []
        with open(ACTIVITY_FILE, 'r') as f:
            data = json.load(f)

        cutoff = datetime.now() - timedelta(hours=_get_activity_retention_hours())
        activities = []

        for item in data:
            try:
                timestamp = datetime.fromisoformat(item['timestamp'])
                if timestamp > cutoff:
                    activities.append(FileActivity(
                        timestamp=timestamp,
                        action=item['action'],
                        filename=item['filename'],
                        size_bytes=item.get('size_bytes', 0),
                        users=item.get('users', []),
                        associated_files=item.get('associated_files', [])
                    ))
            except (KeyError, ValueError):
                continue  # Skip malformed entries

        activities.sort(key=lambda x: x.timestamp, reverse=True)
        return activities[:MAX_RECENT_ACTIVITY]

    except Exception as e:
        logger.debug(f"Could not load activity history: {e}")
        return []


def _save_activity_unlocked(activities: List[FileActivity]) -> None:
    """Save activity to disk without acquiring _activity_file_lock.

    Caller MUST hold _activity_file_lock.
    """
    try:
        ACTIVITY_FILE.parent.mkdir(parents=True, exist_ok=True)

        cutoff = datetime.now() - timedelta(hours=_get_activity_retention_hours())

        data = []
        for activity in activities:
            if activity.timestamp > cutoff:
                entry = {
                    'timestamp': activity.timestamp.isoformat(),
                    'action': activity.action,
                    'filename': activity.filename,
                    'size_bytes': activity.size_bytes,
                    'users': activity.users,
                }
                if activity.associated_files:
                    entry['associated_files'] = activity.associated_files
                data.append(entry)

        save_json_atomically(str(ACTIVITY_FILE), data, label="activity")

    except Exception as e:
        logger.debug(f"Could not save activity history: {e}")


def load_activity() -> List[FileActivity]:
    """Load activity from disk, filtering out entries older than retention period."""
    with _activity_file_lock:
        return _load_activity_unlocked()


def save_activity(activities: List[FileActivity]) -> None:
    """Save activity to disk, filtering out old entries."""
    with _activity_file_lock:
        _save_activity_unlocked(activities)


# ---------------------------------------------------------------------------
# Convenience: record a single file activity (load-merge-save)
# ---------------------------------------------------------------------------

def record_file_activity(
    action: str,
    filename: str,
    size_bytes: int = 0,
    users: Optional[List[str]] = None,
    associated_files: Optional[List[dict]] = None,
) -> None:
    """Record a single file activity entry using load-merge-save pattern.

    Thread-safe: acquires _activity_file_lock for the full sequence.
    Safe for concurrent use by CLI and web writers.
    """
    entry = FileActivity(
        timestamp=datetime.now(),
        action=action,
        filename=filename,
        size_bytes=size_bytes,
        users=users or [],
        associated_files=associated_files or [],
    )
    with _activity_file_lock:
        activities = _load_activity_unlocked()
        activities.insert(0, entry)
        activities = activities[:MAX_RECENT_ACTIVITY]
        _save_activity_unlocked(activities)


# ---------------------------------------------------------------------------
# Last run time
# ---------------------------------------------------------------------------

def save_last_run_time() -> None:
    """Save the current timestamp as the last run time."""
    try:
        LAST_RUN_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(LAST_RUN_FILE, 'w') as f:
            f.write(datetime.now().isoformat())
    except IOError:
        pass


# ---------------------------------------------------------------------------
# Run summary
# ---------------------------------------------------------------------------

def load_last_run_summary() -> Optional[dict]:
    """Load the last run summary from disk."""
    try:
        if LAST_RUN_SUMMARY_FILE.exists():
            with open(LAST_RUN_SUMMARY_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
    except (json.JSONDecodeError, IOError):
        pass
    return None


def save_run_summary(summary: dict) -> None:
    """Save a run summary to disk atomically."""
    try:
        LAST_RUN_SUMMARY_FILE.parent.mkdir(parents=True, exist_ok=True)
        save_json_atomically(str(LAST_RUN_SUMMARY_FILE), summary, label="last run summary")
    except IOError:
        pass
