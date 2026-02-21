"""Scheduler service - manages scheduled PlexCache operations"""

import json
import logging
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any
from dataclasses import dataclass

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from web.config import PROJECT_ROOT, DATA_DIR, SETTINGS_FILE, get_time_format

logger = logging.getLogger(__name__)

# Schedule presets (in hours)
INTERVAL_PRESETS = {
    "1h": 1,
    "2h": 2,
    "4h": 4,
    "6h": 6,
    "12h": 12,
    "24h": 24,
}


@dataclass
class ScheduleConfig:
    """Schedule configuration"""
    enabled: bool = False
    schedule_type: str = "interval"  # "interval" or "cron"
    interval_hours: int = 4
    interval_start_time: str = "00:00"  # HH:MM format - anchor time for intervals
    cron_expression: str = "0 */4 * * *"
    dry_run: bool = False
    verbose: bool = False

    def to_dict(self) -> dict:
        return {
            "enabled": self.enabled,
            "schedule_type": self.schedule_type,
            "interval_hours": self.interval_hours,
            "interval_start_time": self.interval_start_time,
            "cron_expression": self.cron_expression,
            "dry_run": self.dry_run,
            "verbose": self.verbose,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ScheduleConfig":
        return cls(
            enabled=data.get("enabled", False),
            schedule_type=data.get("schedule_type", "interval"),
            interval_hours=data.get("interval_hours", 4),
            interval_start_time=data.get("interval_start_time", "00:00"),
            cron_expression=data.get("cron_expression", "0 */4 * * *"),
            dry_run=data.get("dry_run", False),
            verbose=data.get("verbose", False),
        )


class SchedulerService:
    """Service for managing scheduled PlexCache operations"""

    JOB_ID = "plexcache_scheduled_run"
    PLEX_CACHE_JOB_ID = "plex_cache_refresh"

    def __init__(self):
        self._scheduler = BackgroundScheduler(
            job_defaults={
                'coalesce': True,  # Combine missed runs into one
                'max_instances': 1,  # Only one instance at a time
                'misfire_grace_time': 60 * 60,  # 1 hour grace time for missed jobs
            }
        )
        self._config: Optional[ScheduleConfig] = None
        self._settings_file = SETTINGS_FILE
        self._last_run: Optional[datetime] = None
        self._next_run: Optional[datetime] = None
        self._started = False

    def start(self):
        """Start the scheduler"""
        if self._started:
            return

        self._scheduler.start()
        self._started = True

        # Load last run time from activity/logs
        self._load_last_run()

        # Load and apply saved schedule
        self._load_config()
        if self._config and self._config.enabled:
            self._apply_schedule()

        # Always start the Plex cache refresh job (hourly)
        self._start_plex_cache_refresh_job()

        logger.info("Scheduler service started")

    def _load_last_run(self):
        """Load last run time from data/last_run.txt.

        This file is written by operation_runner when operations complete.
        Falls back to recent_activity.json for backwards compatibility.
        """
        # Primary: Check last_run.txt
        last_run_file = DATA_DIR / "last_run.txt"
        if last_run_file.exists():
            try:
                with open(last_run_file, 'r') as f:
                    timestamp_str = f.read().strip()
                    if timestamp_str:
                        self._last_run = datetime.fromisoformat(timestamp_str)
                        logger.debug(f"Loaded last run: {self._last_run}")
                        return
            except (IOError, ValueError):
                pass

        # Fallback: Check recent_activity.json
        activity_file = DATA_DIR / "recent_activity.json"
        if activity_file.exists():
            try:
                with open(activity_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if data and len(data) > 0:
                        latest = max(data, key=lambda x: x.get('timestamp', ''))
                        if 'timestamp' in latest:
                            self._last_run = datetime.fromisoformat(latest['timestamp'])
                            logger.debug(f"Loaded last run from activity: {self._last_run}")
            except (json.JSONDecodeError, IOError, ValueError):
                pass

    def _start_plex_cache_refresh_job(self):
        """Start the hourly Plex cache refresh job"""
        try:
            # Remove existing job if any
            if self._scheduler.get_job(self.PLEX_CACHE_JOB_ID):
                self._scheduler.remove_job(self.PLEX_CACHE_JOB_ID)

            self._scheduler.add_job(
                self._refresh_plex_cache,
                trigger=IntervalTrigger(hours=1),
                id=self.PLEX_CACHE_JOB_ID,
                name="Plex Cache Refresh",
                replace_existing=True,
            )
            logger.info("Plex cache refresh job scheduled (hourly)")
        except Exception as e:
            logger.error(f"Failed to start Plex cache refresh job: {e}")

    def _refresh_plex_cache(self):
        """Refresh Plex data cache (libraries, users)"""
        from web.services import get_settings_service
        settings_service = get_settings_service()
        settings_service.refresh_plex_cache()

    def stop(self):
        """Stop the scheduler"""
        if self._started:
            self._scheduler.shutdown(wait=False)
            self._started = False
            logger.info("Scheduler service stopped")

    def _load_config(self):
        """Load schedule config from settings file"""
        try:
            if self._settings_file.exists():
                with open(self._settings_file, 'r') as f:
                    settings = json.load(f)

                schedule_data = settings.get("schedule", {})
                self._config = ScheduleConfig.from_dict(schedule_data)
            else:
                self._config = ScheduleConfig()
        except Exception as e:
            logger.error(f"Failed to load schedule config: {e}")
            self._config = ScheduleConfig()

    def _save_config(self):
        """Save schedule config to settings file"""
        try:
            settings = {}
            if self._settings_file.exists():
                with open(self._settings_file, 'r') as f:
                    settings = json.load(f)

            settings["schedule"] = self._config.to_dict()

            with open(self._settings_file, 'w') as f:
                json.dump(settings, f, indent=2)

        except Exception as e:
            logger.error(f"Failed to save schedule config: {e}")

    def _run_scheduled_job(self):
        """Execute the scheduled PlexCache operation"""
        from web.services import get_operation_runner
        from web.services.maintenance_runner import get_maintenance_runner

        runner = get_operation_runner()

        if runner.is_running:
            logger.info("Scheduled run skipped - operation already in progress")
            return

        if get_maintenance_runner().is_running:
            logger.info("Scheduled run skipped - maintenance action in progress")
            return

        logger.info("Starting scheduled PlexCache run")

        # Use config settings for dry_run and verbose
        dry_run = self._config.dry_run if self._config else False
        verbose = self._config.verbose if self._config else False

        runner.start_operation(dry_run=dry_run, verbose=verbose)
        # Note: last_run.txt is updated by operation_runner when operation completes

    def _apply_schedule(self):
        """Apply the current schedule configuration"""
        # Remove existing job if any
        if self._scheduler.get_job(self.JOB_ID):
            self._scheduler.remove_job(self.JOB_ID)
            self._next_run = None

        if not self._config or not self._config.enabled:
            logger.info("Schedule disabled")
            return

        try:
            if self._config.schedule_type == "interval":
                # Parse start time (HH:MM format) and create anchor datetime
                start_time = self._config.interval_start_time or "00:00"
                try:
                    hour, minute = map(int, start_time.split(":"))
                except (ValueError, AttributeError):
                    hour, minute = 0, 0

                # Create anchor time for today
                now = datetime.now()
                anchor = now.replace(hour=hour, minute=minute, second=0, microsecond=0)

                # Use anchor as start_date - APScheduler will calculate next run from this
                trigger = IntervalTrigger(
                    hours=self._config.interval_hours,
                    start_date=anchor
                )
                schedule_desc = f"every {self._config.interval_hours} hour(s) starting at {start_time}"
            else:
                trigger = CronTrigger.from_crontab(self._config.cron_expression)
                schedule_desc = f"cron: {self._config.cron_expression}"

            self._scheduler.add_job(
                self._run_scheduled_job,
                trigger=trigger,
                id=self.JOB_ID,
                name="PlexCache Scheduled Run",
                replace_existing=True,
            )

            # Get next run time
            job = self._scheduler.get_job(self.JOB_ID)
            if job:
                self._next_run = job.next_run_time

            logger.info(f"Schedule enabled: {schedule_desc}")
            if self._next_run:
                logger.info(f"Next scheduled run: {self._next_run.strftime('%Y-%m-%d %H:%M:%S')}")

        except Exception as e:
            logger.error(f"Failed to apply schedule: {e}")

    def get_config(self) -> ScheduleConfig:
        """Get current schedule configuration"""
        if not self._config:
            self._load_config()
        return self._config or ScheduleConfig()

    def update_config(self, config: ScheduleConfig) -> Dict[str, Any]:
        """Update schedule configuration"""
        self._config = config
        self._save_config()

        if self._started:
            self._apply_schedule()

        return {
            "success": True,
            "message": "Schedule updated",
            "next_run": self._next_run.isoformat() if self._next_run else None,
        }

    def _format_time_display(self, time_str: str) -> str:
        """Convert HH:MM to display format based on user's time_format setting."""
        try:
            hour, minute = map(int, time_str.split(":"))
            if get_time_format() == "12h":
                period = "AM" if hour < 12 else "PM"
                hour_12 = hour % 12
                if hour_12 == 0:
                    hour_12 = 12
                if minute == 0:
                    return f"{hour_12} {period}"
                return f"{hour_12}:{minute:02d} {period}"
            else:
                return f"{hour}:{minute:02d}"
        except (ValueError, AttributeError):
            return time_str

    def _datetime_display_fmt(self) -> str:
        """Return strftime format string for date+time based on user's time_format setting."""
        if get_time_format() == "12h":
            return "%Y-%m-%d %-I:%M %p"
        return "%Y-%m-%d %H:%M"

    def _format_relative_time(self, target: datetime) -> str:
        """Format a future datetime as relative time (e.g., 'in 12m', 'in 2h 30m')"""
        now = datetime.now()
        if target <= now:
            return "now"

        diff = target - now
        total_seconds = int(diff.total_seconds())
        total_minutes = total_seconds // 60

        if total_minutes < 1:
            return "in <1m"
        elif total_minutes < 60:
            return f"in {total_minutes}m"
        else:
            hours = total_minutes // 60
            minutes = total_minutes % 60
            if minutes == 0:
                return f"in {hours}h"
            return f"in {hours}h {minutes}m"

    def get_status(self) -> Dict[str, Any]:
        """Get scheduler status"""
        job = self._scheduler.get_job(self.JOB_ID) if self._started else None

        config = self.get_config()

        # Format schedule description
        if config.schedule_type == "interval":
            start_time = config.interval_start_time or "00:00"
            start_time_display = self._format_time_display(start_time)
            schedule_desc = f"Every {config.interval_hours}h from {start_time_display}"
        else:
            schedule_desc = f"Cron: {config.cron_expression}"

        # Calculate relative time for next run
        next_run_relative = None
        if job and job.next_run_time:
            # Convert to naive datetime if it has timezone info
            next_time = job.next_run_time
            if next_time.tzinfo is not None:
                next_time = next_time.replace(tzinfo=None)
            next_run_relative = self._format_relative_time(next_time)

        # Read fresh last run time from file
        last_run_dt = None
        last_run_file = DATA_DIR / "last_run.txt"
        if last_run_file.exists():
            try:
                with open(last_run_file, 'r') as f:
                    timestamp_str = f.read().strip()
                    if timestamp_str:
                        last_run_dt = datetime.fromisoformat(timestamp_str)
            except (IOError, ValueError):
                pass

        return {
            "enabled": config.enabled,
            "running": self._started,
            "schedule_type": config.schedule_type,
            "schedule_description": schedule_desc if config.enabled else "Disabled",
            "interval_hours": config.interval_hours,
            "interval_start_time": config.interval_start_time,
            "cron_expression": config.cron_expression,
            "dry_run": config.dry_run,
            "verbose": config.verbose,
            "next_run": job.next_run_time.isoformat() if job and job.next_run_time else None,
            "next_run_display": job.next_run_time.strftime(self._datetime_display_fmt()) if job and job.next_run_time else "Not scheduled",
            "next_run_relative": next_run_relative,
            "last_run": last_run_dt.isoformat() if last_run_dt else None,
            "last_run_display": last_run_dt.strftime(self._datetime_display_fmt()) if last_run_dt else "Never",
        }

    def validate_cron(self, expression: str) -> Dict[str, Any]:
        """Validate a cron expression"""
        try:
            trigger = CronTrigger.from_crontab(expression)
            # Get next few run times
            next_runs = []
            from datetime import timedelta
            base = datetime.now()
            display_fmt = self._datetime_display_fmt()
            for i in range(3):
                next_time = trigger.get_next_fire_time(None, base)
                if next_time:
                    next_runs.append(next_time.strftime(display_fmt))
                    base = next_time + timedelta(seconds=1)

            return {
                "valid": True,
                "message": "Valid cron expression",
                "next_runs": next_runs,
            }
        except Exception as e:
            return {
                "valid": False,
                "message": str(e),
                "next_runs": [],
            }


# Singleton instance
_scheduler_service: Optional[SchedulerService] = None
_scheduler_service_lock = threading.Lock()


def get_scheduler_service() -> SchedulerService:
    """Get or create the scheduler service singleton"""
    global _scheduler_service
    if _scheduler_service is None:
        with _scheduler_service_lock:
            if _scheduler_service is None:
                _scheduler_service = SchedulerService()
    return _scheduler_service
