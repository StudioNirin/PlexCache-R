"""Shared activity writer — CLI, Web UI, and Maintenance all write here.

Provides file activity recording, last-run timestamps, and run summaries
that the Web UI dashboard reads. This module has NO web framework imports
so it can be used from core/app.py (CLI path) as well as from the web layer.

All run paths write to the same files:
  - data/recent_activity.json   (per-file activity feed)
  - data/last_run.txt           (last run timestamp)
  - data/run_summaries.json     (run statistics keyed by run_id)
"""

import json
import logging
import os
import re
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
RUN_SUMMARIES_FILE = DATA_DIR / "run_summaries.json"
# Legacy single-dict file; migrated into RUN_SUMMARIES_FILE on first load.
_LEGACY_RUN_SUMMARY_FILE = DATA_DIR / "last_run_summary.json"

# Defaults
DEFAULT_ACTIVITY_RETENTION_HOURS = 24
MAX_RECENT_ACTIVITY = 500

# Run sources excluded from load_last_run_summary() so the dashboard's
# "PlexCache last run" widget reflects caching runs, not maintenance.
_LAST_RUN_DEFAULT_SOURCES = ("cli", "web", "scheduled")

# Thread locks
_activity_file_lock = threading.Lock()
_run_summaries_lock = threading.Lock()

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
    # Run grouping metadata (added 2026-04 for run-grouped Recent Activity view).
    # Pre-existing entries on disk lack these fields; loader defaults run_id=None
    # (treated as legacy, bucketed by 15-min time windows by activity_grouping).
    run_id: Optional[str] = None
    run_source: str = "legacy"  # "scheduled" | "web" | "cli" | "maintenance" | "legacy"

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
            "size_bytes": self.size_bytes,
            "users": self.users,
            "run_id": self.run_id,
            "run_source": self.run_source,
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
                        associated_files=item.get('associated_files', []),
                        run_id=item.get('run_id'),
                        run_source=item.get('run_source', 'legacy'),
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
                if activity.run_id:
                    entry['run_id'] = activity.run_id
                if activity.run_source and activity.run_source != "legacy":
                    entry['run_source'] = activity.run_source
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
    run_id: Optional[str] = None,
    run_source: str = "legacy",
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
        run_id=run_id,
        run_source=run_source,
    )
    with _activity_file_lock:
        activities = _load_activity_unlocked()
        activities.insert(0, entry)
        activities = activities[:MAX_RECENT_ACTIVITY]
        _save_activity_unlocked(activities)


# ---------------------------------------------------------------------------
# Show-episode grouping (shared by completion banner + dashboard)
# ---------------------------------------------------------------------------

# Matches "<show> - S##E##" — the Sonarr/Plex TV naming convention.
# Non-TV files (movies, specials without episode numbering) don't match
# and pass through as singletons.
_SHOW_EPISODE_PATTERN = re.compile(r'^(.+?) - S\d+E\d+', re.IGNORECASE)


def group_episodes_by_show(files: List[dict]) -> List[dict]:
    """Collapse multi-episode TV runs into a single parent row per show.

    Movies and shows with only one episode in the payload stay as
    individual rows (grouping a single entry offers no compression).
    Preserves first-seen order so re-renders don't reshuffle.

    Used by both the completion banner (`OperationRunner`) and the
    Recent Activity grouping service (`web/services/activity_grouping.py`).
    """
    groups: dict = {}
    order: list = []

    for idx, f in enumerate(files):
        match = _SHOW_EPISODE_PATTERN.match(f.get("filename", ""))
        if match:
            show_name = match.group(1).strip()
            key = (f.get("action", ""), show_name)
            if key not in groups:
                groups[key] = {
                    "action": f.get("action", ""),
                    "show_name": show_name,
                    "episodes": [],
                    "total_bytes": 0,
                }
                order.append(key)
            # Preserve per-episode metadata the dashboard renders (time, users)
            # in addition to the fields the completion banner consumes.
            groups[key]["episodes"].append({
                "filename": f.get("filename", ""),
                "size": f.get("size", ""),
                "size_bytes": f.get("size_bytes", 0),
                "associated_files": f.get("associated_files", []),
                "timestamp": f.get("timestamp", ""),
                "time_display": f.get("time_display", ""),
                "users": f.get("users", []),
            })
            groups[key]["total_bytes"] += f.get("size_bytes", 0)
        else:
            key = ("__singleton__", idx)
            groups[key] = f
            order.append(key)

    result: List[dict] = []
    for key in order:
        entry = groups[key]
        if key[0] == "__singleton__":
            result.append(entry)
        elif len(entry["episodes"]) == 1:
            ep = entry["episodes"][0]
            result.append({
                "action": entry["action"],
                "filename": ep["filename"],
                "size": ep.get("size", ""),
                "size_bytes": ep.get("size_bytes", 0),
                "associated_files": ep.get("associated_files", []),
                "timestamp": ep.get("timestamp", ""),
                "time_display": ep.get("time_display", ""),
                "users": ep.get("users", []),
            })
        else:
            # Use the newest episode's time as the group's representative time
            # (episodes arrive newest-first when called from the dashboard path).
            head_ep = entry["episodes"][0]
            result.append({
                "action": entry["action"],
                "is_group": True,
                "show_name": entry["show_name"],
                "episode_count": len(entry["episodes"]),
                "episodes": entry["episodes"],
                "size_bytes": entry["total_bytes"],
                "size": format_bytes(entry["total_bytes"]) if entry["total_bytes"] > 0 else "",
                "time_display": head_ep.get("time_display", ""),
                "users": head_ep.get("users", []),
            })

    return result


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
# Run summaries (keyed by run_id, pruned by activity_retention_hours)
# ---------------------------------------------------------------------------

def _migrate_legacy_run_summary_unlocked(summaries: dict) -> dict:
    """One-shot migration: fold old single-dict last_run_summary.json into the
    new keyed-dict file. Idempotent — only runs when the legacy file exists
    and the new file is empty/missing the same entry. Caller must hold
    _run_summaries_lock.
    """
    if not _LEGACY_RUN_SUMMARY_FILE.exists():
        return summaries
    try:
        with open(_LEGACY_RUN_SUMMARY_FILE, 'r', encoding='utf-8') as f:
            old = json.load(f)
        if not isinstance(old, dict):
            _LEGACY_RUN_SUMMARY_FILE.unlink(missing_ok=True)
            return summaries
        run_id = old.get("run_id") or f"legacy-{old.get('timestamp', datetime.now().isoformat())}"
        if run_id not in summaries:
            entry = dict(old)
            entry.setdefault("run_id", run_id)
            entry.setdefault("run_source", "legacy")
            # Old shape stored the completion timestamp as "timestamp"; map
            # it to completed_at so downstream consumers see a uniform schema.
            if "completed_at" not in entry and "timestamp" in entry:
                entry["completed_at"] = entry["timestamp"]
            entry.setdefault("started_at", entry.get("completed_at", datetime.now().isoformat()))
            summaries[run_id] = entry
        _LEGACY_RUN_SUMMARY_FILE.unlink(missing_ok=True)
        logger.info("Migrated legacy last_run_summary.json into run_summaries.json")
    except (json.JSONDecodeError, IOError, OSError) as e:
        logger.debug(f"Legacy run-summary migration skipped: {e}")
    return summaries


def _prune_summaries(summaries: dict) -> dict:
    """Drop entries older than activity_retention_hours, keyed by started_at."""
    cutoff = datetime.now() - timedelta(hours=_get_activity_retention_hours())
    pruned = {}
    for run_id, entry in summaries.items():
        ts_str = entry.get("started_at") or entry.get("completed_at")
        if not ts_str:
            continue
        try:
            ts = datetime.fromisoformat(ts_str)
        except ValueError:
            continue
        if ts > cutoff:
            pruned[run_id] = entry
    return pruned


def _load_run_summaries_unlocked() -> dict:
    """Load run summaries dict from disk, migrate legacy file, prune. Caller
    must hold _run_summaries_lock.
    """
    summaries: dict = {}
    try:
        if RUN_SUMMARIES_FILE.exists():
            with open(RUN_SUMMARIES_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, dict):
                    summaries = data
    except (json.JSONDecodeError, IOError):
        summaries = {}
    summaries = _migrate_legacy_run_summary_unlocked(summaries)
    return _prune_summaries(summaries)


def load_run_summaries() -> dict:
    """All run summaries keyed by run_id, pruned by retention setting."""
    with _run_summaries_lock:
        return _load_run_summaries_unlocked()


def load_run_summary(run_id: str) -> Optional[dict]:
    """One run summary by id, or None if not found / pruned."""
    return load_run_summaries().get(run_id)


def save_run_summary(run_id: str, summary: dict) -> None:
    """Persist a run summary keyed by run_id. Load-merge-save under a lock so
    concurrent writers (CLI, web/scheduled, maintenance) don't clobber.
    """
    if not run_id:
        logger.debug("save_run_summary called with empty run_id; ignoring")
        return
    try:
        with _run_summaries_lock:
            summaries = _load_run_summaries_unlocked()
            entry = dict(summary)
            entry["run_id"] = run_id
            summaries[run_id] = entry
            summaries = _prune_summaries(summaries)
            RUN_SUMMARIES_FILE.parent.mkdir(parents=True, exist_ok=True)
            save_json_atomically(str(RUN_SUMMARIES_FILE), summaries, label="run summaries")
    except IOError:
        pass


def load_last_run_summary(run_sources: Optional[tuple] = None) -> Optional[dict]:
    """Most-recent run summary by started_at. Backward-compat wrapper for the
    dashboard's "Last Run Summary" widget — defaults to caching runs only
    (cli/web/scheduled), excluding maintenance so the widget keeps its
    semantic meaning.
    """
    sources = run_sources if run_sources is not None else _LAST_RUN_DEFAULT_SOURCES
    summaries = load_run_summaries()
    if not summaries:
        return None
    candidates = [
        entry for entry in summaries.values()
        if not sources or entry.get("run_source") in sources
    ]
    if not candidates:
        return None
    candidates.sort(
        key=lambda e: e.get("started_at") or e.get("completed_at") or "",
        reverse=True,
    )
    return candidates[0]
