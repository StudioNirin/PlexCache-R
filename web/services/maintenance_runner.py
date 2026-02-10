"""Maintenance runner service - runs heavy maintenance actions in a background thread"""

import logging
import os
import threading
import time
from datetime import datetime
from enum import Enum
from typing import Optional, Callable, Any, List
from dataclasses import dataclass, field

from web.services.maintenance_service import ActionResult

logger = logging.getLogger(__name__)


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


def _format_bytes(num_bytes: int) -> str:
    """Format bytes into human-readable string like '2.1 GB' or '450 MB'"""
    size = float(num_bytes)
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024 or unit == 'TB':
            return f"{size:.1f} {unit}" if unit != 'B' else f"{int(size)} B"
        size /= 1024
    return f"{size:.1f} TB"


# Actions that should run asynchronously (heavy I/O)
ASYNC_ACTIONS = {
    "protect-with-backup",
    "sync-to-array",
    "fix-with-backup",
    "restore-plexcached",
    "delete-plexcached",
}

# Human-readable display names for actions
ACTION_DISPLAY = {
    "protect-with-backup": "Keeping {count} file(s) on cache...",
    "sync-to-array": "Moving {count} file(s) to array...",
    "fix-with-backup": "Fixing {count} file(s) with backup...",
    "restore-plexcached": "Restoring {count} backup(s)...",
    "delete-plexcached": "Deleting {count} backup(s)...",
}


class MaintenanceState(str, Enum):
    """Maintenance runner states"""
    IDLE = "idle"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class MaintenanceResult:
    """Result of a maintenance action"""
    state: MaintenanceState
    action_name: str = ""
    action_display: str = ""
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_seconds: float = 0
    action_result: Optional[ActionResult] = None
    error_message: Optional[str] = None
    file_count: int = 0
    files_processed: int = 0       # Count of completed files
    current_file: str = ""         # Basename of file currently being processed
    current_file_index: int = 0    # 1-based index (0 = not started)
    bytes_total: int = 0           # Total bytes of current file being copied
    bytes_copied: int = 0          # Bytes copied so far for current file
    copy_start_time: Optional[float] = None  # time.time() when current copy began


class MaintenanceRunner:
    """Service for running heavy maintenance actions in a background thread.

    Similar to OperationRunner but simpler - no log parsing, no PlexCacheApp coupling.
    Just runs a service method and captures the ActionResult.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._state = MaintenanceState.IDLE
        self._result: Optional[MaintenanceResult] = None
        self._thread: Optional[threading.Thread] = None
        self._stop_requested = False

    @property
    def state(self) -> MaintenanceState:
        with self._lock:
            return self._state

    @property
    def is_running(self) -> bool:
        return self.state == MaintenanceState.RUNNING

    @property
    def stop_requested(self) -> bool:
        with self._lock:
            return self._stop_requested

    @property
    def result(self) -> Optional[MaintenanceResult]:
        with self._lock:
            return self._result

    def start_action(
        self,
        action_name: str,
        service_method: Callable,
        method_args: tuple = (),
        method_kwargs: Optional[dict] = None,
        file_count: int = 0,
        on_complete: Optional[Callable] = None,
    ) -> bool:
        """Start a maintenance action in a background thread.

        Args:
            action_name: Action identifier (e.g., "protect-with-backup")
            service_method: The maintenance service method to call
            method_args: Positional args for the method
            method_kwargs: Keyword args for the method
            file_count: Number of files being processed (for display)
            on_complete: Optional callback when action completes

        Returns:
            True if started, False if already running or blocked
        """
        if method_kwargs is None:
            method_kwargs = {}

        # Check mutual exclusion with OperationRunner
        from web.services.operation_runner import get_operation_runner
        if get_operation_runner().is_running:
            logger.info("Maintenance action blocked - PlexCache operation in progress")
            return False

        with self._lock:
            if self._state == MaintenanceState.RUNNING:
                logger.info("Maintenance action blocked - another maintenance action in progress")
                return False

            self._state = MaintenanceState.RUNNING
            self._stop_requested = False

            display = ACTION_DISPLAY.get(action_name, "Running maintenance action...")
            display = display.format(count=file_count)

            self._result = MaintenanceResult(
                state=MaintenanceState.RUNNING,
                action_name=action_name,
                action_display=display,
                started_at=datetime.now(),
                file_count=file_count,
            )

        # Inject stop_check into kwargs so service methods can check for stop
        method_kwargs["stop_check"] = lambda: self._stop_requested

        # Inject progress_callback so service methods can report per-file progress
        def _progress_callback(current_index: int, total: int, filename: str):
            with self._lock:
                if self._result:
                    self._result.current_file_index = current_index
                    self._result.current_file = filename
                    self._result.files_processed = current_index - 1  # previous file is done
                    # Reset byte progress for new file
                    self._result.bytes_total = 0
                    self._result.bytes_copied = 0
                    self._result.copy_start_time = None

        method_kwargs["progress_callback"] = _progress_callback

        # Inject bytes_progress_callback for chunked copy progress
        def _bytes_callback(bytes_copied: int, bytes_total: int):
            with self._lock:
                if self._result:
                    if bytes_copied == 0:
                        self._result.copy_start_time = time.time()
                    self._result.bytes_copied = bytes_copied
                    self._result.bytes_total = bytes_total

        method_kwargs["bytes_progress_callback"] = _bytes_callback

        self._thread = threading.Thread(
            target=self._run_action,
            args=(action_name, service_method, method_args, method_kwargs, on_complete),
            daemon=True,
        )
        self._thread.start()

        logger.info(f"Maintenance action started: {action_name} ({file_count} files)")
        return True

    def stop_action(self) -> bool:
        """Request the current maintenance action to stop.

        Returns:
            True if stop was requested, False if not running
        """
        with self._lock:
            if self._state != MaintenanceState.RUNNING:
                return False
            self._stop_requested = True

        logger.info("Maintenance action stop requested")
        return True

    def dismiss(self):
        """Reset COMPLETED/FAILED state back to IDLE."""
        with self._lock:
            if self._state in (MaintenanceState.COMPLETED, MaintenanceState.FAILED):
                self._state = MaintenanceState.IDLE
                # Keep _result for reference but update state
                if self._result:
                    self._result.state = MaintenanceState.IDLE

    # Maps action names to activity feed display strings
    ACTION_ACTIVITY_LABELS = {
        "protect-with-backup": "Protected",
        "sync-to-array": "Moved to Array",
        "fix-with-backup": "Fixed",
        "restore-plexcached": "Restored Backup",
        "delete-plexcached": "Deleted Backup",
    }

    def _record_maintenance_activity(self, action_name: str, action_result: ActionResult):
        """Record maintenance file operations to the shared activity feed."""
        if not action_result or not action_result.affected_paths:
            return

        label = self.ACTION_ACTIVITY_LABELS.get(action_name)
        if not label:
            return

        from web.services.operation_runner import FileActivity, load_activity, save_activity, MAX_RECENT_ACTIVITY

        now = datetime.now()
        new_entries = []

        for path in action_result.affected_paths:
            filename = os.path.basename(path)
            # Try to get file size (file may be gone after delete/move)
            try:
                size_bytes = os.path.getsize(path)
            except OSError:
                size_bytes = 0

            new_entries.append(FileActivity(
                timestamp=now,
                action=label,
                filename=filename,
                size_bytes=size_bytes,
            ))

        # Load existing, prepend new, cap, save
        activities = load_activity()
        activities = new_entries + activities
        activities = activities[:MAX_RECENT_ACTIVITY]
        save_activity(activities)

    def _run_action(
        self,
        action_name: str,
        service_method: Callable,
        method_args: tuple,
        method_kwargs: dict,
        on_complete: Optional[Callable],
    ):
        """Execute the maintenance action in the background thread."""
        start_time = time.time()
        error_message = None
        action_result = None

        try:
            action_result = service_method(*method_args, **method_kwargs)

            if self._stop_requested:
                logger.info(f"Maintenance action stopped by user: {action_name}")
            else:
                logger.info(f"Maintenance action completed: {action_name}")

            # Record successful file operations to the activity feed
            if action_result and action_result.affected_paths:
                try:
                    self._record_maintenance_activity(action_name, action_result)
                except Exception as e:
                    logger.error(f"Failed to record maintenance activity: {e}")

        except Exception as e:
            error_message = str(e)
            logger.exception(f"Maintenance action failed: {action_name}")

        finally:
            duration = time.time() - start_time

            with self._lock:
                self._result.completed_at = datetime.now()
                self._result.duration_seconds = duration
                self._result.action_result = action_result

                # Clear progress fields on completion
                if not self._stop_requested:
                    self._result.files_processed = self._result.file_count
                self._result.current_file = ""
                self._result.current_file_index = 0
                self._result.bytes_total = 0
                self._result.bytes_copied = 0
                self._result.copy_start_time = None

                if error_message:
                    self._result.state = MaintenanceState.FAILED
                    self._result.error_message = error_message
                    self._state = MaintenanceState.FAILED
                else:
                    self._result.state = MaintenanceState.COMPLETED
                    self._state = MaintenanceState.COMPLETED

            # Call on_complete callback (e.g., cache invalidation)
            if on_complete:
                try:
                    on_complete()
                except Exception as e:
                    logger.error(f"on_complete callback failed: {e}")

    def get_status_dict(self) -> dict:
        """Get status as a dictionary for banner rendering."""
        result = self.result

        if result is None or self.state == MaintenanceState.IDLE:
            return {
                "state": MaintenanceState.IDLE.value,
                "is_running": False,
            }

        status = {
            "state": result.state.value,
            "is_running": result.state == MaintenanceState.RUNNING,
            "action_name": result.action_name,
            "action_display": result.action_display,
            "started_at": result.started_at.isoformat() if result.started_at else None,
            "completed_at": result.completed_at.isoformat() if result.completed_at else None,
            "duration_seconds": round(result.duration_seconds, 1),
            "file_count": result.file_count,
            "error_message": result.error_message,
            "files_processed": result.files_processed,
            "current_file": result.current_file,
            "current_file_index": result.current_file_index,
        }

        # Elapsed time
        elapsed = 0
        if result.started_at:
            if result.completed_at:
                elapsed = result.duration_seconds
            else:
                elapsed = (datetime.now() - result.started_at).total_seconds()
        status["elapsed_display"] = _format_duration(elapsed)

        # Progress percent, bytes display, and ETA (running only)
        if result.file_count > 0 and result.state == MaintenanceState.RUNNING:
            # Blended progress: completed files + fractional current file from bytes
            file_fraction = 0
            if result.bytes_total > 0:
                file_fraction = result.bytes_copied / result.bytes_total
            overall = (result.files_processed + file_fraction) / result.file_count
            status["progress_percent"] = min(int(overall * 100), 100)

            # Bytes display (only while copying)
            if result.bytes_total > 0:
                status["bytes_display"] = f"{_format_bytes(result.bytes_copied)} / {_format_bytes(result.bytes_total)}"
            else:
                status["bytes_display"] = ""

            # ETA from copy byte rate when actively copying
            if result.bytes_total > 0 and result.bytes_copied > 0 and result.copy_start_time:
                copy_elapsed = time.time() - result.copy_start_time
                if copy_elapsed > 0:
                    rate = result.bytes_copied / copy_elapsed
                    current_remaining = (result.bytes_total - result.bytes_copied) / rate
                    # Estimate remaining files from average completed-file time
                    future_files = result.file_count - result.files_processed - 1
                    future_time = 0
                    if future_files > 0 and result.files_processed > 0:
                        future_time = future_files * (elapsed / result.files_processed)
                    status["eta_display"] = _format_duration(current_remaining + future_time)
                else:
                    status["eta_display"] = ""
            elif result.files_processed > 0 and elapsed > 0:
                # Fallback: file-level average (for non-copy operations like rename/delete)
                avg = elapsed / result.files_processed
                remaining = result.file_count - result.files_processed
                status["eta_display"] = _format_duration(avg * remaining)
            else:
                status["eta_display"] = ""
        else:
            status["progress_percent"] = 100 if result.state != MaintenanceState.RUNNING else 0
            status["bytes_display"] = ""
            status["eta_display"] = ""

        # Add action result details for completed state
        if result.action_result:
            status["result_message"] = result.action_result.message
            status["result_success"] = result.action_result.success
            status["affected_count"] = result.action_result.affected_count
            status["errors"] = result.action_result.errors

        return status


# Singleton instance
_maintenance_runner: Optional[MaintenanceRunner] = None


def get_maintenance_runner() -> MaintenanceRunner:
    """Get or create the maintenance runner singleton"""
    global _maintenance_runner
    if _maintenance_runner is None:
        _maintenance_runner = MaintenanceRunner()
    return _maintenance_runner
