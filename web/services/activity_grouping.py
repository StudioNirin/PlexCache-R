"""Run-grouped Recent Activity transformation.

Pure render-layer two-pass transformation over the per-file activity feed:

  1. Bucket entries by ``run_id`` (entries without one — pre-upgrade or
     manually-recorded — fall back to 15-minute time-window clusters so the
     dashboard still has a coherent grouping during the retention overlap).
  2. Within each bucket, apply ``group_episodes_by_show()`` so multi-episode
     TV runs collapse under a single parent row.

The on-disk activity file shape is unchanged; this transformation runs on
each ``GET /operations/activity`` request.
"""

from datetime import datetime, timedelta
from typing import List, Dict, Optional

from core.activity import group_episodes_by_show, get_time_format, load_run_summaries
from core.system_utils import format_bytes, format_duration


# Time gap that splits adjacent legacy entries into separate buckets.
# Picked to match the typical scheduled cadence (hourly) without merging
# distinct manual runs that happen to land minutes apart.
LEGACY_RUN_WINDOW = timedelta(minutes=15)

# Actions that count toward the "restored" pill on the run header.
RESTORE_ACTIONS = {"Restored", "Moved", "Moved to Array", "Restored Backup"}

SOURCE_LABELS = {
    "scheduled": "Scheduled Run",
    "web": "Web UI Run",
    "cli": "CLI Run",
    "maintenance": "Maintenance Run",
    "legacy": "Previous Activity",
}


def _format_time(dt: datetime, time_format: str) -> str:
    """Portable time-of-day formatter (no strftime %-I, which fails on Windows)."""
    if time_format == "12h":
        hour = dt.hour % 12 or 12
        suffix = "AM" if dt.hour < 12 else "PM"
        return f"{hour}:{dt.minute:02d} {suffix}"
    return dt.strftime("%H:%M")


def _format_time_range(started: datetime, completed: datetime, time_format: str) -> str:
    start_str = _format_time(started, time_format)
    end_str = _format_time(completed, time_format)
    if start_str == end_str:
        return start_str
    return f"{start_str} – {end_str}"


def _format_date_display(dt: datetime) -> str:
    today = datetime.now().date()
    entry_date = dt.date()
    if entry_date == today:
        return "Today"
    if entry_date == today - timedelta(days=1):
        return "Yesterday"
    return dt.strftime("%a, %b ") + str(dt.day)


def group_activity_into_runs(
    activities: List[dict],
    legacy_window: timedelta = LEGACY_RUN_WINDOW,
) -> List[dict]:
    """Two-pass: bucket by run_id (legacy → time window), then per-bucket show grouping.

    Args:
        activities: List of ``FileActivity.to_dict()`` outputs, sorted newest-first.
        legacy_window: Time gap that splits adjacent legacy entries into
                       separate buckets (default 15 minutes).

    Returns:
        List of run-group dicts ordered newest-first, each shaped like::

            {
                "run_id": str,
                "run_source": "scheduled" | "web" | "cli" | "maintenance" | "legacy",
                "label": str,                # e.g. "Scheduled Run"
                "started_at": isoformat,
                "completed_at": isoformat,
                "duration_seconds": float,
                "duration_display": str,
                "files_cached": int,
                "files_restored": int,
                "files_total": int,
                "bytes_cached": int,
                "bytes_restored": int,
                "bytes_total": int,
                "bytes_total_display": str,
                "time_range": str,           # "10:24 – 10:27"
                "date_key": "YYYY-MM-DD",
                "date_display": str,         # "Today" / "Yesterday" / "Mon, Apr 23"
                "entries": List[dict],       # show-grouped per-file entries
            }
    """
    if not activities:
        return []

    time_format = get_time_format()
    # Run summaries (keyed by run_id) carry the *real* run start/end times —
    # the period the script actually spent on Plex API + scanning + file
    # moves. Without them we'd fall back to first/last FileActivity timestamp,
    # which only covers the file-move window and misses the pre-/post-tail.
    run_summaries = load_run_summaries()

    # Pass 1: bucket entries (iterate chronologically so legacy windows cluster correctly)
    buckets: List[Dict] = []
    current_bucket: Optional[Dict] = None
    last_legacy_timestamp: Optional[datetime] = None
    legacy_counter = 0

    for entry in reversed(activities):
        run_id = entry.get("run_id")
        run_source = entry.get("run_source", "legacy") or "legacy"
        timestamp = datetime.fromisoformat(entry["timestamp"])

        if run_id:
            key = ("run", run_id)
            last_legacy_timestamp = None
        else:
            if last_legacy_timestamp is None or (timestamp - last_legacy_timestamp) > legacy_window:
                legacy_counter += 1
            last_legacy_timestamp = timestamp
            key = ("legacy", legacy_counter)

        if current_bucket is None or current_bucket["key"] != key:
            current_bucket = {
                "key": key,
                "run_id": run_id or f"legacy-{legacy_counter}",
                "run_source": run_source,
                "entries": [entry],
                "started_at": timestamp,
                "completed_at": timestamp,
            }
            buckets.append(current_bucket)
        else:
            current_bucket["entries"].append(entry)
            if timestamp < current_bucket["started_at"]:
                current_bucket["started_at"] = timestamp
            if timestamp > current_bucket["completed_at"]:
                current_bucket["completed_at"] = timestamp

    # Pass 2: per-bucket show grouping + run-level aggregates
    result: List[dict] = []
    for bucket in buckets:
        # Sort bucket entries newest-first for the rendered view
        sorted_entries = sorted(
            bucket["entries"],
            key=lambda e: datetime.fromisoformat(e["timestamp"]),
            reverse=True,
        )
        grouped_entries = group_episodes_by_show(sorted_entries)

        # Aggregates from raw entries (each file counts once, not per group)
        files_cached = sum(1 for e in bucket["entries"] if e.get("action") == "Cached")
        files_restored = sum(1 for e in bucket["entries"] if e.get("action") in RESTORE_ACTIONS)
        bytes_cached = sum(e.get("size_bytes", 0) for e in bucket["entries"] if e.get("action") == "Cached")
        bytes_restored = sum(e.get("size_bytes", 0) for e in bucket["entries"] if e.get("action") in RESTORE_ACTIONS)
        total_bytes = sum(e.get("size_bytes", 0) for e in bucket["entries"])

        # Prefer summary-recorded times when available — they cover the full
        # run, not just the first-to-last-file window.
        summary = run_summaries.get(bucket["run_id"])
        run_started_at = bucket["started_at"]
        run_completed_at = bucket["completed_at"]
        if summary:
            try:
                if summary.get("started_at"):
                    run_started_at = datetime.fromisoformat(summary["started_at"])
                if summary.get("completed_at"):
                    run_completed_at = datetime.fromisoformat(summary["completed_at"])
            except ValueError:
                pass

        duration = (run_completed_at - run_started_at).total_seconds()

        result.append({
            "run_id": bucket["run_id"],
            "run_source": bucket["run_source"],
            "label": SOURCE_LABELS.get(bucket["run_source"], "Activity"),
            "started_at": run_started_at.isoformat(),
            "completed_at": run_completed_at.isoformat(),
            "duration_seconds": duration,
            "duration_display": format_duration(duration) if duration > 0 else "",
            "files_cached": files_cached,
            "files_restored": files_restored,
            "files_total": len(bucket["entries"]),
            "bytes_cached": bytes_cached,
            "bytes_restored": bytes_restored,
            "bytes_total": total_bytes,
            "bytes_total_display": format_bytes(total_bytes) if total_bytes > 0 else "",
            "time_range": _format_time_range(run_started_at, run_completed_at, time_format),
            "date_key": run_started_at.date().isoformat(),
            "date_display": _format_date_display(run_started_at),
            "entries": grouped_entries,
        })

    # Newest run first (we built buckets chronologically)
    result.reverse()
    return result
