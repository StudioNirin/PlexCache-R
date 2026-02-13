"""Operation runner service - runs PlexCache operations in background"""

import asyncio
import json
import logging
import re
import threading
import time
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Optional, List, Dict, Callable
from dataclasses import dataclass, field

from web.config import PROJECT_ROOT, DATA_DIR, SETTINGS_FILE as CONFIG_SETTINGS_FILE, get_time_format

# Activity persistence settings - use DATA_DIR for Docker compatibility
ACTIVITY_FILE = DATA_DIR / "recent_activity.json"
LAST_RUN_FILE = DATA_DIR / "last_run.txt"
SETTINGS_FILE = CONFIG_SETTINGS_FILE
DEFAULT_ACTIVITY_RETENTION_HOURS = 24


def save_last_run_time():
    """Save the current timestamp as the last run time."""
    try:
        LAST_RUN_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(LAST_RUN_FILE, 'w') as f:
            f.write(datetime.now().isoformat())
    except IOError:
        pass


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


class OperationState(str, Enum):
    """Operation states"""
    IDLE = "idle"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class FileActivity:
    """Represents a file operation"""
    timestamp: datetime
    action: str  # "Cached", "Restored", "Protected", "Moved to Array", etc.
    filename: str
    size_bytes: int = 0
    users: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        fmt = get_time_format()
        if fmt == "12h":
            time_display = self.timestamp.strftime("%-I:%M:%S %p")
        else:
            time_display = self.timestamp.strftime("%H:%M:%S")
        return {
            "timestamp": self.timestamp.isoformat(),
            "time_display": time_display,
            "action": self.action,
            "filename": self.filename,
            "size": self._format_size(self.size_bytes),
            "users": self.users
        }

    def _format_size(self, size_bytes: int) -> str:
        if size_bytes == 0:
            return "-"
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.2f} PB"


MAX_RECENT_ACTIVITY = 500


def load_activity() -> List[FileActivity]:
    """Load activity from disk, filtering out entries older than retention period."""
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
                        users=item.get('users', [])
                    ))
            except (KeyError, ValueError):
                continue  # Skip malformed entries

        # Sort by timestamp descending (newest first) and limit
        activities.sort(key=lambda x: x.timestamp, reverse=True)
        return activities[:MAX_RECENT_ACTIVITY]

    except Exception as e:
        logging.debug(f"Could not load activity history: {e}")
        return []


def save_activity(activities: List[FileActivity]) -> None:
    """Save activity to disk, filtering out old entries."""
    try:
        # Ensure data directory exists
        ACTIVITY_FILE.parent.mkdir(parents=True, exist_ok=True)

        cutoff = datetime.now() - timedelta(hours=_get_activity_retention_hours())

        # Filter to only entries within retention period
        data = []
        for activity in activities:
            if activity.timestamp > cutoff:
                data.append({
                    'timestamp': activity.timestamp.isoformat(),
                    'action': activity.action,
                    'filename': activity.filename,
                    'size_bytes': activity.size_bytes,
                    'users': activity.users
                })

        with open(ACTIVITY_FILE, 'w') as f:
            json.dump(data, f, indent=2)

    except Exception as e:
        logging.debug(f"Could not save activity history: {e}")


@dataclass
class OperationResult:
    """Result of a completed operation"""
    state: OperationState
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_seconds: float = 0
    files_cached: int = 0
    files_restored: int = 0
    bytes_cached: int = 0
    bytes_restored: int = 0
    error_message: Optional[str] = None
    dry_run: bool = False
    log_messages: List[str] = field(default_factory=list)
    recent_activity: List[FileActivity] = field(default_factory=list)
    # Phase tracking (Enhancement 1)
    current_phase: str = "starting"
    current_phase_display: str = "Starting..."
    files_to_cache_total: int = 0
    files_to_restore_total: int = 0
    files_cached_so_far: int = 0
    files_restored_so_far: int = 0
    bytes_cached_so_far: int = 0
    bytes_restored_so_far: int = 0
    last_completed_file: str = ""
    error_count: int = 0
    # Byte-level batch progress (from FileMover callback)
    batch_bytes_copied: int = 0
    batch_bytes_total: int = 0
    batch_copy_start_time: Optional[float] = None
    # Cumulative across all batches (array + cache)
    cumulative_bytes_copied: int = 0
    cumulative_bytes_total: int = 0
    _prev_batch_cumulative: int = 0  # internal: snapshot at batch start


class WebLogHandler(logging.Handler):
    """Custom log handler that captures messages for the web UI"""

    def __init__(self, callback: Callable[[str], None]):
        super().__init__()
        self.callback = callback
        fmt = get_time_format()
        datefmt = '%-I:%M:%S %p' if fmt == '12h' else '%H:%M:%S'
        self.setFormatter(logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt=datefmt
        ))

    def emit(self, record):
        try:
            msg = self.format(record)
            self.callback(msg)
        except Exception:
            pass


class OperationRunner:
    """Service for running PlexCache operations"""

    def __init__(self):
        self._lock = threading.Lock()
        self._state = OperationState.IDLE
        self._current_result: Optional[OperationResult] = None
        self._thread: Optional[threading.Thread] = None
        self._log_messages: List[str] = []
        self._max_log_messages = 500
        self._subscribers: List[asyncio.Queue] = []
        self._recent_activity: List[FileActivity] = load_activity()
        self._max_recent_activity = MAX_RECENT_ACTIVITY
        self._stop_requested = False  # Flag to signal operation should stop
        self._app_instance: Optional["PlexCacheApp"] = None  # Reference to running app
        self._current_run_files: List[dict] = []  # Files processed in current run only
        # Track current operation type based on headers
        self._current_operation: Optional[str] = None
        # Patterns to match file operation headers and content
        self._return_header = re.compile(r'Returning to array \((\d+)\s+\w+')
        self._copy_header = re.compile(r'Copying to array \((\d+)\s+\w+')
        self._cache_header = re.compile(r'Caching to|Moving Files|Moved to cache:\s*(\d+)')
        self._file_entry = re.compile(r'^  (.+)$')  # Indented file entries (legacy)
        self._results_pattern = re.compile(r'Moved to cache:\s*(\d+)|Moved to array:\s*(\d+)')
        # New pattern for real-time completion logs: "  [Action] filename (size)"
        self._action_entry = re.compile(r'^  \[(Cached|Restored|Moved)\]\s+(.+?)(?:\s+\(([^)]+)\))?$')
        # Tracker data for user lookups (loaded on operation start)
        self._ondeck_tracker: Dict = {}
        self._watchlist_tracker: Dict = {}

    def _load_trackers(self) -> None:
        """Load OnDeck and Watchlist trackers for user lookups"""
        ondeck_file = DATA_DIR / "ondeck_tracker.json"
        watchlist_file = DATA_DIR / "watchlist_tracker.json"

        try:
            if ondeck_file.exists():
                with open(ondeck_file, 'r', encoding='utf-8') as f:
                    self._ondeck_tracker = json.load(f)
                logging.debug(f"Loaded OnDeck tracker: {len(self._ondeck_tracker)} entries")
            else:
                logging.debug(f"OnDeck tracker file not found: {ondeck_file}")
        except (json.JSONDecodeError, IOError) as e:
            logging.debug(f"Failed to load OnDeck tracker: {e}")
            self._ondeck_tracker = {}

        try:
            if watchlist_file.exists():
                with open(watchlist_file, 'r', encoding='utf-8') as f:
                    self._watchlist_tracker = json.load(f)
                logging.debug(f"Loaded Watchlist tracker: {len(self._watchlist_tracker)} entries")
            else:
                logging.debug(f"Watchlist tracker file not found: {watchlist_file}")
        except (json.JSONDecodeError, IOError) as e:
            logging.debug(f"Failed to load Watchlist tracker: {e}")
            self._watchlist_tracker = {}

    def _get_users_for_file(self, filename: str) -> List[str]:
        """Look up users associated with a file from trackers.

        Reads fresh data from disk since PlexCacheApp updates trackers during operation.
        """
        users = set()

        # Load fresh tracker data (PlexCacheApp updates these during the run)
        ondeck_file = DATA_DIR / "ondeck_tracker.json"
        watchlist_file = DATA_DIR / "watchlist_tracker.json"

        ondeck_data = {}
        watchlist_data = {}

        try:
            if ondeck_file.exists():
                with open(ondeck_file, 'r', encoding='utf-8') as f:
                    ondeck_data = json.load(f)
        except (json.JSONDecodeError, IOError):
            pass

        try:
            if watchlist_file.exists():
                with open(watchlist_file, 'r', encoding='utf-8') as f:
                    watchlist_data = json.load(f)
        except (json.JSONDecodeError, IOError):
            pass

        # Search in OnDeck tracker (keys are full paths, we match by filename)
        for path, info in ondeck_data.items():
            if filename in path or path.endswith(filename):
                if isinstance(info, dict) and "users" in info:
                    users.update(info["users"])

        # Search in Watchlist tracker
        for path, info in watchlist_data.items():
            if filename in path or path.endswith(filename):
                if isinstance(info, dict) and "users" in info:
                    users.update(info["users"])

        return sorted(users)

    def _save_activity(self, new_entry: FileActivity = None) -> None:
        """Save activity to disk.

        If new_entry is provided, loads existing entries from disk first
        to avoid overwriting entries added by MaintenanceRunner.
        """
        if new_entry:
            activities = load_activity()
            activities.insert(0, new_entry)
            activities = activities[:MAX_RECENT_ACTIVITY]
            save_activity(activities)
        else:
            save_activity(self._recent_activity)

    @property
    def state(self) -> OperationState:
        """Get current operation state"""
        with self._lock:
            return self._state

    @property
    def is_running(self) -> bool:
        """Check if an operation is currently running"""
        return self.state == OperationState.RUNNING

    @property
    def stop_requested(self) -> bool:
        """Check if a stop has been requested"""
        with self._lock:
            return self._stop_requested

    @property
    def current_result(self) -> Optional[OperationResult]:
        """Get the current/last operation result"""
        with self._lock:
            return self._current_result

    @property
    def log_messages(self) -> List[str]:
        """Get captured log messages"""
        with self._lock:
            return list(self._log_messages)

    @property
    def recent_activity(self) -> List[dict]:
        """Get recent file activity as list of dicts.

        Reloads from disk to include entries written by MaintenanceRunner.
        """
        activities = load_activity()
        return [a.to_dict() for a in activities]

    def _parse_size(self, size_str: str) -> int:
        """Parse a size string like '1.5 GB' into bytes."""
        try:
            # Handle various formats: "1.5 GB", "500 MB", "1.2GB", etc.
            size_str = size_str.strip().upper()
            # Check longest units first to avoid 'B' matching before 'GB'
            units_ordered = [
                ('TB', 1024 ** 4),
                ('GB', 1024 ** 3),
                ('MB', 1024 ** 2),
                ('KB', 1024),
                ('B', 1),
            ]
            for unit, mult in units_ordered:
                if unit in size_str:
                    num_str = size_str.replace(unit, '').strip()
                    return int(float(num_str) * mult)
            return 0
        except (ValueError, TypeError):
            return 0

    # Phase detection markers (order matters — checked top-to-bottom)
    _PHASE_MARKERS = [
        ("--- Results ---", "results", "Finishing up..."),
        ("Smart eviction", "evicting", "Running eviction..."),
        ("Caching to cache drive", "caching", "Caching to drive..."),
        ("Returning to array", "restoring", "Returning to array..."),
        ("Copying to array", "restoring", "Returning to array..."),
        ("--- Moving Files ---", "moving", "Moving files..."),
        ("Total media to cache:", "analyzing", "Analyzing libraries..."),
        ("--- Fetching Media ---", "fetching", "Fetching media..."),
    ]

    # Regex to extract file count from "Caching to cache drive (N file(s)):"
    _cache_count_re = re.compile(r'Caching to cache drive \((\d+)\s+\w+')
    # Regex to extract file count from "Total media to cache: N files"
    _total_media_re = re.compile(r'Total media to cache:\s*(\d+)')

    @staticmethod
    def _format_duration(seconds: float) -> str:
        """Format seconds into human-readable duration like '1m 23s' or '45s'"""
        seconds = max(0, seconds)
        if seconds < 60:
            return f"{int(seconds)}s"
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        if minutes < 60:
            return f"{minutes}m {secs:02d}s"
        hours = int(minutes // 60)
        mins = minutes % 60
        return f"{hours}h {mins:02d}m"

    @staticmethod
    def _format_bytes(num_bytes: int) -> str:
        """Format bytes into human-readable string like '2.1 GB' or '450 MB'"""
        size = float(num_bytes)
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size < 1024 or unit == 'TB':
                return f"{size:.1f} {unit}" if unit != 'B' else f"{int(size)} B"
            size /= 1024
        return f"{size:.1f} TB"

    def _parse_phase(self, msg: str):
        """Detect phase transitions and extract counts from log messages."""
        # Strip timestamp prefix for clean matching
        clean_msg = msg
        for sep in (' - INFO - ', ' - DEBUG - ', ' - WARNING - '):
            if sep in msg:
                clean_msg = msg.split(sep, 1)[-1]
                break

        # Count errors
        if ' - ERROR - ' in msg or ' - CRITICAL - ' in msg:
            with self._lock:
                if self._current_result:
                    self._current_result.error_count += 1

        # Detect phase transitions
        for marker, phase_key, phase_display in self._PHASE_MARKERS:
            if marker in clean_msg:
                with self._lock:
                    if self._current_result:
                        self._current_result.current_phase = phase_key
                        prefix = "Dry Run: " if self._current_result.dry_run else ""
                        self._current_result.current_phase_display = prefix + phase_display
                break

        # Extract file count totals from header lines
        with self._lock:
            if not self._current_result:
                return

            # "Returning to array (N episodes/files ...)" or "Copying to array (N ...)"
            m = self._return_header.search(clean_msg)
            if not m:
                m = self._copy_header.search(clean_msg)
            if m:
                self._current_result.files_to_restore_total += int(m.group(1))
                return

            # "Caching to cache drive (N file(s)):"
            m = self._cache_count_re.search(clean_msg)
            if m:
                self._current_result.files_to_cache_total = int(m.group(1))
                return

            # "Total media to cache: N files" — only set if caching header hasn't set it yet
            m = self._total_media_re.search(clean_msg)
            if m and self._current_result.files_to_cache_total == 0:
                self._current_result.files_to_cache_total = int(m.group(1))

    def _parse_file_operation(self, msg: str):
        """Parse log message to extract file operations"""
        # Strip timestamp prefix if present (format: HH:MM:SS - LEVEL - message)
        clean_msg = msg
        if ' - INFO - ' in msg:
            clean_msg = msg.split(' - INFO - ', 1)[-1]
        elif ' - DEBUG - ' in msg:
            clean_msg = msg.split(' - DEBUG - ', 1)[-1]

        # Check for operation headers that set context
        if self._return_header.search(clean_msg):
            self._current_operation = "Restored"
            return
        if self._copy_header.search(clean_msg):
            self._current_operation = "Moved"
            return
        if 'Caching to' in clean_msg or '--- Moving Files ---' in clean_msg:
            self._current_operation = "Cached"
            return
        if '--- Results ---' in clean_msg:
            self._current_operation = None  # Reset at results section
            return

        # Check for real-time completion format: "  [Action] filename (size)"
        # This captures actual file completions with sizes, not preview headers
        action_match = self._action_entry.match(clean_msg)
        if action_match:
            action = action_match.group(1)  # Cached, Restored, or Moved
            filename = action_match.group(2).strip()
            size_str = action_match.group(3)  # May be None

            # Parse size if provided
            size_bytes = 0
            if size_str:
                size_bytes = self._parse_size(size_str)

            # Look up users for cached files (restored/moved don't have users)
            users = []
            if action == "Cached":
                users = self._get_users_for_file(filename)

            activity = FileActivity(
                timestamp=datetime.now(),
                action=action,
                filename=filename,
                size_bytes=size_bytes,
                users=users
            )
            with self._lock:
                self._recent_activity.insert(0, activity)
                if len(self._recent_activity) > self._max_recent_activity:
                    self._recent_activity = self._recent_activity[:self._max_recent_activity]
                # Track files for this run's completion summary
                self._current_run_files.insert(0, {
                    "action": action,
                    "filename": filename,
                    "size": activity._format_size(size_bytes),
                })
                # Increment real-time progress counters
                if self._current_result:
                    self._current_result.last_completed_file = filename
                    if action == "Cached":
                        self._current_result.files_cached_so_far += 1
                        self._current_result.bytes_cached_so_far += size_bytes
                    elif action in ("Restored", "Moved"):
                        self._current_result.files_restored_so_far += 1
                        self._current_result.bytes_restored_so_far += size_bytes
            # Persist to disk (load-merge-save to avoid overwriting maintenance entries)
            self._save_activity(new_entry=activity)
            return

        # Note: Legacy preview header entries (without [Action] prefix) are intentionally
        # NOT captured here - we only want actual completion events with sizes

    def _add_log_message(self, msg: str):
        """Add a log message and notify subscribers"""
        with self._lock:
            self._log_messages.append(msg)
            # Keep only last N messages
            if len(self._log_messages) > self._max_log_messages:
                self._log_messages = self._log_messages[-self._max_log_messages:]

        # Try to parse file operations and phase transitions from log message
        self._parse_file_operation(msg)
        self._parse_phase(msg)

        # Notify async subscribers
        for queue in self._subscribers:
            try:
                queue.put_nowait(msg)
            except asyncio.QueueFull:
                pass

    def subscribe_logs(self) -> asyncio.Queue:
        """Subscribe to log messages (for WebSocket streaming)"""
        queue = asyncio.Queue(maxsize=100)
        self._subscribers.append(queue)
        return queue

    def unsubscribe_logs(self, queue: asyncio.Queue):
        """Unsubscribe from log messages"""
        if queue in self._subscribers:
            self._subscribers.remove(queue)

    def start_operation(self, dry_run: bool = False, verbose: bool = False) -> bool:
        """
        Start a PlexCache operation in a background thread.

        Args:
            dry_run: If True, simulate without moving files
            verbose: If True, enable DEBUG level logging

        Returns:
            True if operation started, False if already running or maintenance is running
        """
        # Check mutual exclusion with MaintenanceRunner
        from web.services.maintenance_runner import get_maintenance_runner
        if get_maintenance_runner().is_running:
            logging.info("Operation blocked - maintenance action in progress")
            return False

        with self._lock:
            if self._state == OperationState.RUNNING:
                return False

            self._state = OperationState.RUNNING
            self._log_messages = []
            self._stop_requested = False  # Reset stop flag for new operation
            self._app_instance = None  # Clear previous app reference
            self._current_run_files = []  # Reset per-run file list
            # Activity stacks across runs (not cleared) - capped at _max_recent_activity
            self._current_operation = None
            self._current_result = OperationResult(
                state=OperationState.RUNNING,
                started_at=datetime.now(),
                dry_run=dry_run,
                current_phase="starting",
                current_phase_display="Dry Run: Starting..." if dry_run else "Starting...",
            )

        # Start operation in background thread
        self._thread = threading.Thread(
            target=self._run_operation,
            args=(dry_run, verbose),
            daemon=True
        )
        self._thread.start()

        return True

    def dismiss(self) -> None:
        """Reset COMPLETED/FAILED state back to IDLE so the banner shows scheduler info."""
        with self._lock:
            if self._state in (OperationState.COMPLETED, OperationState.FAILED):
                self._state = OperationState.IDLE
                if self._current_result:
                    self._current_result.state = OperationState.IDLE

    def stop_operation(self) -> bool:
        """
        Request the current operation to stop.

        Returns:
            True if stop was requested, False if no operation running
        """
        app_to_stop = None
        with self._lock:
            if self._state != OperationState.RUNNING:
                return False

            self._stop_requested = True
            # Store reference to app so we can signal it outside the lock
            app_to_stop = self._app_instance

        # Log message and signal app outside the lock to avoid deadlock
        # (_add_log_message also acquires self._lock)
        self._add_log_message("Stop requested - stopping after current file...")

        # Signal the PlexCacheApp to stop
        if app_to_stop and hasattr(app_to_stop, 'request_stop'):
            app_to_stop.request_stop()

        return True

    def _run_operation(self, dry_run: bool, verbose: bool = False):
        """Run the PlexCache operation (called in background thread)"""
        start_time = time.time()
        error_message = None
        app = None  # Track app for cleanup

        # Load trackers for user lookups during log parsing
        self._load_trackers()

        # Set up custom log handler to capture messages
        web_handler = WebLogHandler(self._add_log_message)
        web_handler.setLevel(logging.DEBUG if verbose else logging.INFO)

        # Get root logger and add our handler
        root_logger = logging.getLogger()
        root_logger.addHandler(web_handler)

        try:
            mode_str = []
            if dry_run:
                mode_str.append("dry_run")
            if verbose:
                mode_str.append("verbose")
            mode_display = f" ({', '.join(mode_str)})" if mode_str else ""
            self._add_log_message(f"Starting PlexCache operation{mode_display}...")

            # Import PlexCacheApp here to avoid circular imports
            from core.app import PlexCacheApp

            config_file = str(SETTINGS_FILE)

            # Byte-level progress callback for smooth operation banner updates
            def _bytes_cb(bytes_copied: int, bytes_total: int):
                with self._lock:
                    if self._current_result:
                        r = self._current_result
                        if bytes_copied == 0:
                            # New batch starting — snapshot cumulative progress
                            r.batch_copy_start_time = time.time()
                            r._prev_batch_cumulative = r.cumulative_bytes_copied
                            r.cumulative_bytes_total = r._prev_batch_cumulative + bytes_total
                        r.batch_bytes_copied = bytes_copied
                        r.batch_bytes_total = bytes_total
                        r.cumulative_bytes_copied = r._prev_batch_cumulative + bytes_copied

            # Create and run the app
            app = PlexCacheApp(
                config_file=config_file,
                dry_run=dry_run,
                quiet=False,
                verbose=verbose,
                bytes_progress_callback=_bytes_cb
            )

            # Store reference so stop_operation can signal it
            with self._lock:
                self._app_instance = app

            app.run()

            # Extract results from real-time log counters (accurate: only counts
            # files actually [Cached]/[Restored]/[Moved], not "Already cached")
            with self._lock:
                self._current_result.files_cached = self._current_result.files_cached_so_far
                self._current_result.files_restored = self._current_result.files_restored_so_far
                self._current_result.bytes_cached = self._current_result.bytes_cached_so_far
                self._current_result.bytes_restored = self._current_result.bytes_restored_so_far

            # Check if we were stopped early
            if self._stop_requested:
                self._add_log_message("Operation stopped by user")
            else:
                self._add_log_message("Operation completed successfully")

        except Exception as e:
            error_message = str(e)
            self._add_log_message(f"ERROR: {error_message}")
            logging.exception("Operation failed")

        finally:
            # Clear app reference
            with self._lock:
                self._app_instance = None

            # Release the instance lock to allow future operations
            if app and hasattr(app, 'instance_lock') and app.instance_lock:
                try:
                    app.instance_lock.release()
                except Exception:
                    pass  # Ignore errors during cleanup

            # Remove our custom handler
            root_logger.removeHandler(web_handler)

            # Update final state
            duration = time.time() - start_time
            with self._lock:
                self._current_result.completed_at = datetime.now()
                self._current_result.duration_seconds = duration
                self._current_result.log_messages = list(self._log_messages)

                if error_message:
                    self._current_result.state = OperationState.FAILED
                    self._current_result.error_message = error_message
                    self._state = OperationState.FAILED
                else:
                    self._current_result.state = OperationState.COMPLETED
                    self._state = OperationState.COMPLETED

            # Always save last run time when operation finishes (success or failure)
            save_last_run_time()

    def get_status_dict(self) -> dict:
        """Get status as a dictionary for API responses"""
        result = self.current_result

        if result is None:
            return {
                "state": OperationState.IDLE.value,
                "is_running": False,
                "message": "No operations run yet"
            }

        status = {
            "state": result.state.value,
            "is_running": result.state == OperationState.RUNNING,
            "dry_run": result.dry_run,
            "started_at": result.started_at.isoformat() if result.started_at else None,
            "completed_at": result.completed_at.isoformat() if result.completed_at else None,
            "duration_seconds": round(result.duration_seconds, 1),
            "files_cached": result.files_cached,
            "files_restored": result.files_restored,
            "bytes_cached": result.bytes_cached,
            "bytes_restored": result.bytes_restored,
            "error_message": result.error_message
        }

        if result.state == OperationState.RUNNING:
            # Phase and progress fields
            status["current_phase"] = result.current_phase
            status["current_phase_display"] = result.current_phase_display
            status["files_to_cache_total"] = result.files_to_cache_total
            status["files_to_restore_total"] = result.files_to_restore_total
            status["files_cached_so_far"] = result.files_cached_so_far
            status["files_restored_so_far"] = result.files_restored_so_far
            status["error_count"] = result.error_count
            status["last_completed_file"] = result.last_completed_file

            total_files = result.files_to_cache_total + result.files_to_restore_total
            completed_files = result.files_cached_so_far + result.files_restored_so_far
            status["total_files"] = total_files
            status["completed_files"] = completed_files

            # Progress percent (meaningful only when we know totals)
            if total_files > 0:
                status["progress_percent"] = min(int(completed_files / total_files * 100), 100)
            else:
                status["progress_percent"] = 0

            # Elapsed time
            elapsed = 0
            if result.started_at:
                elapsed = (datetime.now() - result.started_at).total_seconds()
            status["elapsed_display"] = self._format_duration(elapsed)

            # ETA (file-level average)
            if completed_files > 0 and total_files > 0 and elapsed > 0:
                avg = elapsed / completed_files
                remaining = total_files - completed_files
                status["eta_display"] = self._format_duration(avg * remaining)
            else:
                status["eta_display"] = ""

            # Bytes display (total moved so far)
            total_bytes = result.bytes_cached_so_far + result.bytes_restored_so_far
            if total_bytes > 0:
                status["bytes_display"] = self._format_bytes(total_bytes)
            else:
                status["bytes_display"] = ""

            # Byte-level progress (smooth updates during active copies)
            cumul_total = result.cumulative_bytes_total
            cumul_copied = result.cumulative_bytes_copied

            if cumul_total > 0:
                # Override file-level progress with smoother byte-level
                status["progress_percent"] = min(int(cumul_copied / cumul_total * 100), 100)
                status["bytes_display"] = f"{self._format_bytes(cumul_copied)} / {self._format_bytes(cumul_total)}"

                # ETA from current batch byte rate
                if result.batch_bytes_copied > 0 and result.batch_copy_start_time:
                    copy_elapsed = time.time() - result.batch_copy_start_time
                    if copy_elapsed > 0:
                        rate = result.batch_bytes_copied / copy_elapsed
                        remaining = cumul_total - cumul_copied
                        if rate > 0:
                            status["eta_display"] = self._format_duration(remaining / rate)

            # Recent log messages (last 5) for hover mini-log
            with self._lock:
                status["recent_logs"] = list(self._log_messages[-5:])

            status["message"] = result.current_phase_display

        elif result.state == OperationState.COMPLETED:
            # Formatted display fields for richer completion banner
            status["duration_display"] = self._format_duration(result.duration_seconds)
            status["bytes_cached_display"] = self._format_bytes(result.bytes_cached) if result.bytes_cached > 0 else ""
            status["bytes_restored_display"] = self._format_bytes(result.bytes_restored) if result.bytes_restored > 0 else ""
            status["error_count"] = result.error_count

            # Files processed in this run for hover detail
            with self._lock:
                status["recent_files"] = list(self._current_run_files[:8])

            if result.dry_run:
                status["message"] = f"Dry run completed in {self._format_duration(result.duration_seconds)}"
            else:
                status["message"] = f"Completed: {result.files_cached} cached, {result.files_restored} restored ({self._format_duration(result.duration_seconds)})"

        elif result.state == OperationState.FAILED:
            status["message"] = f"Failed: {result.error_message}"
            status["error_count"] = result.error_count
        else:
            status["message"] = "Ready"

        return status


# Singleton instance
_operation_runner: Optional[OperationRunner] = None


def get_operation_runner() -> OperationRunner:
    """Get or create the operation runner singleton"""
    global _operation_runner
    if _operation_runner is None:
        _operation_runner = OperationRunner()
    return _operation_runner
