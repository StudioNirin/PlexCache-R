"""
Logging configuration for PlexCache.
Handles log setup, rotation, and notification handlers.
"""

import json
import logging
import os
import subprocess
import threading
import time
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional, List

import requests

# Global lock for thread-safe console output (shared with tqdm)
_console_lock = threading.RLock()


def get_console_lock() -> threading.RLock:
    """Get the global console output lock for use with tqdm."""
    return _console_lock


class ThreadSafeStreamHandler(logging.StreamHandler):
    """A StreamHandler that uses a global lock for thread-safe console output.

    This prevents interleaving of log messages with tqdm progress bars
    when multiple threads are logging simultaneously.
    """

    def emit(self, record):
        """Emit a record with thread-safe locking."""
        with _console_lock:
            super().emit(record)


# Define SUMMARY as the highest level so "summary" mode webhooks only get summaries
# CRITICAL=50, so SUMMARY=100 ensures it's higher than all standard levels
SUMMARY = 100
logging.addLevelName(SUMMARY, 'SUMMARY')


class VerboseMessageFilter(logging.Filter):
    """Filter to downgrade certain verbose messages to DEBUG level.

    Some messages (like datetime parsing failures for empty strings) are
    logged at INFO level by libraries but should be DEBUG level for our use case.
    """

    # Patterns of messages that should be downgraded to DEBUG
    DOWNGRADE_PATTERNS = [
        "Failed to parse",  # datetime parsing failures
        "to datetime as timestamp",
    ]

    def filter(self, record: logging.LogRecord) -> bool:
        """Return True to allow the record, False to suppress it."""
        if record.levelno == logging.INFO:
            msg = record.getMessage()
            for pattern in self.DOWNGRADE_PATTERNS:
                if pattern in msg:
                    # Check if we're in verbose/debug mode
                    effective_level = logging.getLogger().getEffectiveLevel()
                    if effective_level <= logging.DEBUG:
                        # Verbose mode: show as DEBUG
                        record.levelno = logging.DEBUG
                        record.levelname = 'DEBUG'
                        return True
                    else:
                        # Normal mode: suppress entirely
                        return False
        return True


class UnraidHandler(logging.Handler):
    """Custom logging handler for Unraid notifications."""

    SUMMARY = SUMMARY

    def __init__(self, enabled_levels: Optional[List[str]] = None):
        super().__init__()
        self.notify_cmd_base = "/usr/local/emhttp/webGui/scripts/notify"
        self._summary_data: Optional[dict] = None
        # List of enabled notification types: "summary", "error", "warning"
        self.enabled_levels = enabled_levels if enabled_levels else ["summary"]
        if not os.path.isfile(self.notify_cmd_base) or not os.access(self.notify_cmd_base, os.X_OK):
            logging.warning(f"{self.notify_cmd_base} does not exist or is not executable. Unraid notifications will not be sent.")
            self.notify_cmd_base = None

    def set_summary_data(self, data: dict) -> None:
        """Set structured summary data for checking errors-only mode."""
        self._summary_data = data

    def emit(self, record):
        if not self.notify_cmd_base:
            return

        if record.levelno == SUMMARY:
            # Only send summary if "summary" is enabled
            if "summary" in self.enabled_levels:
                self.send_summary_unraid_notification(record)
        elif record.levelno >= logging.ERROR:
            # Send errors if "error" is enabled
            if "error" in self.enabled_levels:
                self.send_unraid_notification(record)
        elif record.levelno >= logging.WARNING:
            # Send warnings if "warning" is enabled
            if "warning" in self.enabled_levels:
                self.send_unraid_notification(record)

    def send_summary_unraid_notification(self, record):
        icon = 'normal'
        notify_cmd = f'{self.notify_cmd_base} -e "PlexCache" -s "Summary" -d "{record.msg}" -i "{icon}"'
        subprocess.call(notify_cmd, shell=True)

    def send_unraid_notification(self, record):
        # Map logging levels to icons
        level_to_icon = {
            'WARNING': 'warning',
            'ERROR': 'alert',
            'INFO': 'normal',
            'DEBUG': 'normal',
            'CRITICAL': 'alert'
        }

        icon = level_to_icon.get(record.levelname, 'normal')

        # Prepare the command with necessary arguments
        notify_cmd = f'{self.notify_cmd_base} -e "PlexCache" -s "{record.levelname}" -d "{record.msg}" -i "{icon}"'

        # Execute the command
        subprocess.call(notify_cmd, shell=True)


class WebhookHandler(logging.Handler):
    """Custom logging handler for webhook notifications with rich formatting.

    Supports Discord (embeds), Slack (Block Kit), and generic webhooks.
    Platform is auto-detected from the webhook URL.
    """

    SUMMARY = SUMMARY

    # Platform detection patterns
    DISCORD_PATTERNS = ['discord.com/api/webhooks/', 'discordapp.com/api/webhooks/']
    SLACK_PATTERNS = ['hooks.slack.com/services/']

    # Color codes for Discord embeds (decimal format)
    COLORS = {
        'success': 3066993,   # Green (#2ECC71)
        'warning': 16776960,  # Yellow (#FFFF00)
        'error': 15158332,    # Red (#E74C3C)
        'info': 3447003,      # Blue (#3498DB)
    }

    def __init__(self, webhook_url: str, enabled_levels: Optional[List[str]] = None):
        super().__init__()
        self.webhook_url = webhook_url
        self.platform = self._detect_platform(webhook_url)
        self._summary_data: Optional[dict] = None
        # List of enabled notification types: "summary", "error", "warning"
        self.enabled_levels = enabled_levels if enabled_levels else ["summary"]

    def _detect_platform(self, url: str) -> str:
        """Auto-detect webhook platform from URL."""
        url_lower = url.lower()
        for pattern in self.DISCORD_PATTERNS:
            if pattern in url_lower:
                return 'discord'
        for pattern in self.SLACK_PATTERNS:
            if pattern in url_lower:
                return 'slack'
        return 'generic'

    def set_summary_data(self, data: dict) -> None:
        """Set structured summary data for rich formatting.

        Expected keys:
            - cached_count: int - Files moved to cache
            - cached_bytes: int - Bytes moved to cache
            - restored_count: int - Files restored to array
            - restored_bytes: int - Bytes restored to array
            - already_cached: int - Files already on cache
            - duration_seconds: float - Execution time
            - had_errors: bool - Whether errors occurred
            - had_warnings: bool - Whether warnings occurred
        """
        self._summary_data = data

    def emit(self, record):
        # Check which notification types are enabled
        if record.levelno == SUMMARY:
            # Only send summary if "summary" is enabled
            if "summary" in self.enabled_levels:
                self._send_summary(record)
        elif record.levelno >= logging.ERROR:
            # Send errors if "error" is enabled
            if "error" in self.enabled_levels:
                self._send_message(record)
        elif record.levelno >= logging.WARNING:
            # Send warnings if "warning" is enabled
            if "warning" in self.enabled_levels:
                self._send_message(record)

    def _send_summary(self, record):
        """Send summary notification with rich formatting if available."""
        try:
            if self.platform == 'discord':
                payload = self._build_discord_summary(record)
            elif self.platform == 'slack':
                payload = self._build_slack_summary(record)
            else:
                payload = self._build_generic_summary(record)

            self._send_payload(payload)
        except Exception as e:
            logging.error(f"Failed to send webhook summary: {e}")

    def _send_message(self, record):
        """Send individual log message."""
        try:
            if self.platform == 'discord':
                payload = self._build_discord_message(record)
            elif self.platform == 'slack':
                payload = self._build_slack_message(record)
            else:
                payload = {"content": record.msg}

            self._send_payload(payload)
        except Exception as e:
            logging.error(f"Failed to send webhook message: {e}")

    def _send_payload(self, payload: dict) -> bool:
        """Send payload to webhook URL."""
        headers = {"Content-Type": "application/json"}
        try:
            response = requests.post(
                self.webhook_url,
                data=json.dumps(payload),
                headers=headers,
                timeout=10
            )
            # Discord returns 204, Slack returns 200
            if response.status_code not in [200, 204]:
                logging.error(f"Webhook failed: HTTP {response.status_code}")
                return False
            return True
        except requests.RequestException as e:
            logging.error(f"Webhook request failed: {e}")
            return False

    def _format_bytes(self, bytes_val: int) -> str:
        """Format bytes to human-readable string."""
        if bytes_val >= 1024**3:
            return f"{bytes_val / (1024**3):.2f} GB"
        elif bytes_val >= 1024**2:
            return f"{bytes_val / (1024**2):.2f} MB"
        elif bytes_val >= 1024:
            return f"{bytes_val / 1024:.2f} KB"
        return f"{bytes_val} B"

    def _format_duration(self, seconds: float) -> str:
        """Format duration to human-readable string."""
        if seconds < 60:
            return f"{int(seconds)}s"
        elif seconds < 3600:
            mins = int(seconds // 60)
            secs = int(seconds % 60)
            return f"{mins}m {secs}s" if secs > 0 else f"{mins}m"
        else:
            hours = int(seconds // 3600)
            mins = int((seconds % 3600) // 60)
            return f"{hours}h {mins}m" if mins > 0 else f"{hours}h"

    def _get_status_color(self) -> int:
        """Get color based on summary data status."""
        if self._summary_data:
            if self._summary_data.get('had_errors'):
                return self.COLORS['error']
            if self._summary_data.get('had_warnings'):
                return self.COLORS['warning']
        return self.COLORS['success']

    def _build_discord_summary(self, record) -> dict:
        """Build Discord embed for summary."""
        fields = []

        if self._summary_data:
            data = self._summary_data

            # Cached files
            if data.get('cached_count', 0) > 0:
                cached_str = f"{data['cached_count']} file{'s' if data['cached_count'] != 1 else ''}"
                if data.get('cached_bytes', 0) > 0:
                    cached_str += f"\n({self._format_bytes(data['cached_bytes'])})"
                fields.append({
                    "name": "📥 Cached",
                    "value": cached_str,
                    "inline": True
                })

            # Restored files
            if data.get('restored_count', 0) > 0:
                restored_str = f"{data['restored_count']} file{'s' if data['restored_count'] != 1 else ''}"
                if data.get('restored_bytes', 0) > 0:
                    restored_str += f"\n({self._format_bytes(data['restored_bytes'])})"
                fields.append({
                    "name": "📤 Restored",
                    "value": restored_str,
                    "inline": True
                })

            # Already cached
            if data.get('already_cached', 0) > 0:
                fields.append({
                    "name": "✓ Already Cached",
                    "value": f"{data['already_cached']} file{'s' if data['already_cached'] != 1 else ''}",
                    "inline": True
                })

            # Spacer before duration (creates visual separation)
            if fields and data.get('duration_seconds', 0) > 0:
                fields.append({"name": "\u200b", "value": "\u200b", "inline": False})

            # Duration
            if data.get('duration_seconds', 0) > 0:
                fields.append({
                    "name": "⏱️ Duration",
                    "value": self._format_duration(data['duration_seconds']),
                    "inline": True
                })

        # If no structured data, fall back to message parsing
        if not fields:
            fields.append({
                "name": "Summary",
                "value": record.msg or "No files moved",
                "inline": False
            })

        # Determine title based on activity and dry run status
        dry_run_prefix = "[DRY RUN] " if self._summary_data and self._summary_data.get('dry_run') else ""
        if self._summary_data:
            total_moved = (self._summary_data.get('cached_count', 0) +
                          self._summary_data.get('restored_count', 0))
            if total_moved > 0:
                title = f"{dry_run_prefix}PlexCache Summary"
            else:
                title = f"{dry_run_prefix}PlexCache - No Changes"
        else:
            title = "PlexCache Summary"

        embed = {
            "title": title,
            "color": self._get_status_color(),
            "fields": fields,
            "footer": {
                "text": "PlexCache-R"
            },
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }

        return {"embeds": [embed]}

    def _build_discord_message(self, record) -> dict:
        """Build Discord embed for individual log message."""
        # Map log levels to colors
        level_colors = {
            logging.ERROR: self.COLORS['error'],
            logging.WARNING: self.COLORS['warning'],
            logging.INFO: self.COLORS['info'],
            logging.DEBUG: self.COLORS['info'],
        }
        color = level_colors.get(record.levelno, self.COLORS['info'])

        embed = {
            "title": f"PlexCache - {record.levelname}",
            "description": record.msg,
            "color": color,
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }

        return {"embeds": [embed]}

    def _build_slack_summary(self, record) -> dict:
        """Build Slack Block Kit for summary."""
        # Determine header text based on dry run status
        dry_run_prefix = "[DRY RUN] " if self._summary_data and self._summary_data.get('dry_run') else ""
        header_text = f"{dry_run_prefix}PlexCache Summary"

        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": header_text,
                    "emoji": True
                }
            }
        ]

        if self._summary_data:
            data = self._summary_data
            fields = []

            if data.get('cached_count', 0) > 0:
                size_str = f" ({self._format_bytes(data['cached_bytes'])})" if data.get('cached_bytes') else ""
                fields.append({
                    "type": "mrkdwn",
                    "text": f"*Cached:* {data['cached_count']} file{'s' if data['cached_count'] != 1 else ''}{size_str}"
                })

            if data.get('restored_count', 0) > 0:
                size_str = f" ({self._format_bytes(data['restored_bytes'])})" if data.get('restored_bytes') else ""
                fields.append({
                    "type": "mrkdwn",
                    "text": f"*Restored:* {data['restored_count']} file{'s' if data['restored_count'] != 1 else ''}{size_str}"
                })

            if data.get('already_cached', 0) > 0:
                fields.append({
                    "type": "mrkdwn",
                    "text": f"*Already Cached:* {data['already_cached']} file{'s' if data['already_cached'] != 1 else ''}"
                })

            if data.get('duration_seconds', 0) > 0:
                fields.append({
                    "type": "mrkdwn",
                    "text": f"*Duration:* {self._format_duration(data['duration_seconds'])}"
                })

            if fields:
                blocks.append({
                    "type": "section",
                    "fields": fields[:10]  # Slack limits to 10 fields
                })
        else:
            # Fallback to plain message
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": record.msg or "No files moved"
                }
            })

        # Add context footer
        blocks.append({
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"PlexCache-R • {datetime.now().strftime('%Y-%m-%d %H:%M')}"
                }
            ]
        })

        return {"blocks": blocks}

    def _build_slack_message(self, record) -> dict:
        """Build Slack message for individual log entry."""
        # Use emoji for level indicator
        level_emoji = {
            logging.ERROR: "🔴",
            logging.WARNING: "🟡",
            logging.INFO: "🔵",
            logging.DEBUG: "⚪",
        }
        emoji = level_emoji.get(record.levelno, "⚪")

        return {
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"{emoji} *PlexCache - {record.levelname}*\n{record.msg}"
                    }
                }
            ]
        }

    def _build_generic_summary(self, record) -> dict:
        """Build generic JSON payload for unknown webhook types."""
        # Try content first (Discord-compatible), fall back to text (Slack-compatible)
        dry_run_prefix = "[DRY RUN] " if self._summary_data and self._summary_data.get('dry_run') else ""
        message = f"{dry_run_prefix}PlexCache Summary:\n" + (record.msg or "No files moved")
        return {"content": message, "text": message}


class LoggingManager:
    """Manages logging configuration and setup."""

    def __init__(self, logs_folder: str, log_level: str = "",
                 max_log_files: int = 24, keep_error_logs_days: int = 7):
        self.logs_folder = Path(logs_folder)
        self.log_level = log_level
        self.max_log_files = max_log_files
        self.keep_error_logs_days = keep_error_logs_days
        self.log_file_pattern = "plexcache_log_*.log"
        self.current_log_file: Optional[Path] = None  # Track current log file for error preservation
        self.logger = logging.getLogger()
        self.summary_messages = []
        self.files_moved = False
        self._webhook_handler: Optional[WebhookHandler] = None  # Reference for rich summaries
        self._unraid_handler: Optional[UnraidHandler] = None  # Reference for summary data
        self._summary_data: dict = {}  # Structured data for rich webhook formatting
        
    def setup_logging(self) -> None:
        """Set up logging configuration."""
        # Clear any existing handlers to prevent duplicates when running multiple times
        # (e.g., when web UI runs multiple operations in the same process)
        self._clear_existing_handlers()

        self._ensure_logs_folder()
        self._setup_log_file()
        self._set_log_level()
        self._clean_old_log_files()
        # Add filter to downgrade verbose library messages to DEBUG
        self.logger.addFilter(VerboseMessageFilter())
        # Suppress noisy third-party library debug logs
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("requests").setLevel(logging.WARNING)
        logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
        # Suppress python-multipart form parser debug spam (various logger names)
        logging.getLogger("multipart").setLevel(logging.WARNING)
        logging.getLogger("multipart.multipart").setLevel(logging.WARNING)

    def _clear_existing_handlers(self) -> None:
        """Remove existing handlers from the root logger to prevent duplicates."""
        # Keep track of handler types we manage
        managed_types = (RotatingFileHandler, ThreadSafeStreamHandler, UnraidHandler, WebhookHandler)

        handlers_to_remove = [h for h in self.logger.handlers if isinstance(h, managed_types)]
        for handler in handlers_to_remove:
            try:
                handler.close()
            except Exception:
                pass
            self.logger.removeHandler(handler)

    def update_settings(self, max_log_files: int = None, keep_error_logs_days: int = None) -> None:
        """Update logging settings after config is loaded.

        This allows settings to be updated from config values after initial setup.
        Re-runs log cleanup with the updated max_log_files value.
        """
        if max_log_files is not None:
            self.max_log_files = max_log_files
        if keep_error_logs_days is not None:
            self.keep_error_logs_days = keep_error_logs_days

        # Re-run cleanup with updated max_log_files
        self._clean_old_log_files()
        
    def _ensure_logs_folder(self) -> None:
        """Ensure the logs folder exists."""
        if not self.logs_folder.exists():
            try:
                self.logs_folder.mkdir(parents=True, exist_ok=True)
            except PermissionError:
                raise PermissionError(f"{self.logs_folder} not writable, please fix the variable accordingly.")
    
    def _setup_log_file(self) -> None:
        """Set up the log file with rotation."""
        current_time = datetime.now().strftime("%Y%m%d_%H%M")
        log_file = self.logs_folder / f"plexcache_log_{current_time}.log"
        self.current_log_file = log_file  # Track for error preservation
        latest_log_file = self.logs_folder / "plexcache_log_latest.log"

        # Configure the rotating file handler
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=20*1024*1024,
            backupCount=self.max_log_files
        )
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        file_handler.addFilter(VerboseMessageFilter())  # Apply filter to handler
        self.logger.addHandler(file_handler)

        # Add console handler for stdout output (thread-safe to prevent tqdm interleaving)
        console_handler = ThreadSafeStreamHandler()
        console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        console_handler.addFilter(VerboseMessageFilter())  # Apply filter to handler
        self.logger.addHandler(console_handler)

        # Ensure the logs folder exists
        if not self.logs_folder.exists():
            self.logs_folder.mkdir(parents=True, exist_ok=True)

        # Create or update the symbolic link to the latest log file
        try:
            if latest_log_file.exists() or latest_log_file.is_symlink():
                latest_log_file.unlink()
            latest_log_file.symlink_to(log_file)
        except FileExistsError:
            # If still exists for some reason, remove and retry
            latest_log_file.unlink()
            latest_log_file.symlink_to(log_file)

        
    def _set_log_level(self) -> None:
        """Set the logging level."""
        if self.log_level:
            log_level = self.log_level.lower()
            level_mapping = {
                "debug": logging.DEBUG,
                "info": logging.INFO,
                "warning": logging.WARNING,
                "error": logging.ERROR,
                "critical": logging.CRITICAL
            }
            
            if log_level in level_mapping:
                self.logger.setLevel(level_mapping[log_level])
            else:
                logging.warning(f"Invalid log_level: {log_level}. Using default level: INFO")
                self.logger.setLevel(logging.INFO)
        else:
            self.logger.setLevel(logging.INFO)
    
    def _clean_old_log_files(self) -> None:
        """Clean old log files to maintain the maximum count."""
        existing_log_files = list(self.logs_folder.glob(self.log_file_pattern))
        existing_log_files.sort(key=lambda x: x.stat().st_mtime)
        
        while len(existing_log_files) > self.max_log_files:
            os.remove(existing_log_files.pop(0))
    
    def setup_notification_handlers(self, notification_config, is_unraid: bool, is_docker: bool) -> None:
        """Set up notification handlers based on configuration."""
        notification_type = notification_config.notification_type.lower()
        
        # Determine notification type
        if notification_type == "system":
            if is_unraid and not is_docker:
                notification_type = "unraid"
            else:
                notification_type = ""
        elif notification_type == "both":
            if is_unraid and is_docker:
                notification_type = "webhook"
        
        # Set up Unraid handler
        if notification_type in ["both", "unraid"]:
            # Get enabled levels (new list-based) or fall back to legacy level
            unraid_levels = self._get_enabled_levels(
                notification_config.unraid_levels,
                notification_config.unraid_level
            )
            unraid_handler = UnraidHandler(enabled_levels=unraid_levels)
            # Set handler level to DEBUG so all messages pass through, filtering is done in emit()
            unraid_handler.setLevel(logging.DEBUG)
            self.logger.addHandler(unraid_handler)
            self._unraid_handler = unraid_handler  # Store reference for summary data
            logging.debug(f"Unraid notifications enabled for: {unraid_levels}")

        # Set up Webhook handler
        if notification_type in ["both", "webhook"] and notification_config.webhook_url:
            # Get enabled levels (new list-based) or fall back to legacy level
            webhook_levels = self._get_enabled_levels(
                notification_config.webhook_levels,
                notification_config.webhook_level
            )
            webhook_handler = WebhookHandler(notification_config.webhook_url, enabled_levels=webhook_levels)
            # Set handler level to DEBUG so all messages pass through, filtering is done in emit()
            webhook_handler.setLevel(logging.DEBUG)
            self.logger.addHandler(webhook_handler)
            self._webhook_handler = webhook_handler  # Store reference for rich summaries
            logging.debug(f"Webhook configured: {webhook_handler.platform} platform, levels: {webhook_levels}")
    
    def _set_handler_level(self, handler: logging.Handler, level_str: str) -> None:
        """Set the level for a logging handler."""
        if level_str:
            level_str = level_str.lower()
            level_mapping = {
                "debug": logging.DEBUG,
                "info": logging.INFO,
                "warning": logging.WARNING,
                "error": logging.ERROR,
                "critical": logging.CRITICAL,
                "summary": SUMMARY
            }
            
            if level_str in level_mapping:
                handler.setLevel(level_mapping[level_str])
            else:
                logging.warning(f"Invalid notification level: {level_str}. Using default level: ERROR")
                handler.setLevel(logging.ERROR)
        else:
            handler.setLevel(logging.ERROR)

    def _get_enabled_levels(self, levels_list: Optional[List[str]], legacy_level: str) -> List[str]:
        """Get enabled notification levels, with backward compatibility for legacy config.

        Args:
            levels_list: New list-based config (e.g., ["summary", "error"])
            legacy_level: Old string-based config (e.g., "summary", "error", "warning")

        Returns:
            List of enabled levels
        """
        # If new list config is provided and not empty, use it
        if levels_list:
            return levels_list

        # Fall back to legacy level string
        if not legacy_level:
            return ["summary"]  # Default

        legacy_level = legacy_level.lower()

        # Convert legacy level to list format
        # "summary" -> ["summary"] (only summary)
        # "error" -> ["error"] (only errors, no summary)
        # "warning" -> ["warning", "error"] (warnings and errors)
        # "info" -> ["summary"] (treat as summary for webhook compatibility)
        # "debug" -> ["summary", "error", "warning"] (everything)
        if legacy_level == "summary":
            return ["summary"]
        elif legacy_level == "error":
            return ["error"]
        elif legacy_level == "warning":
            return ["warning", "error"]
        elif legacy_level == "debug":
            return ["summary", "error", "warning"]
        else:
            return ["summary"]  # Default fallback

    def add_summary_message(self, message: str) -> None:
        """Add a message to the summary."""
        if self.files_moved:
            self.summary_messages.append(message)
        else:
            self.summary_messages = [message]
            self.files_moved = True

    def set_summary_data(self, cached_count: int = 0, cached_bytes: int = 0,
                         restored_count: int = 0, restored_bytes: int = 0,
                         already_cached: int = 0, duration_seconds: float = 0,
                         had_errors: bool = False, had_warnings: bool = False,
                         dry_run: bool = False) -> None:
        """Set structured summary data for rich webhook formatting.

        This data is passed to WebhookHandler to generate rich embeds
        with fields, colors, and proper formatting.

        Args:
            cached_count: Number of files moved to cache
            cached_bytes: Total bytes moved to cache
            restored_count: Number of files restored to array
            restored_bytes: Total bytes restored to array
            already_cached: Number of files already on cache (skipped)
            duration_seconds: Total execution time
            had_errors: Whether any errors occurred during run
            had_warnings: Whether any warnings occurred during run
            dry_run: Whether this was a dry run (no files actually moved)
        """
        self._summary_data = {
            'cached_count': cached_count,
            'cached_bytes': cached_bytes,
            'restored_count': restored_count,
            'restored_bytes': restored_bytes,
            'already_cached': already_cached,
            'duration_seconds': duration_seconds,
            'had_errors': had_errors,
            'had_warnings': had_warnings,
            'dry_run': dry_run,
        }

    def log_summary(self) -> None:
        """Log the summary message.

        Uses newlines for multi-line output when there are multiple messages.
        Passes structured data to webhook handler for rich formatting.
        """
        # Pass structured data to notification handlers before logging
        if self._webhook_handler and self._summary_data:
            self._webhook_handler.set_summary_data(self._summary_data)
        if self._unraid_handler and self._summary_data:
            self._unraid_handler.set_summary_data(self._summary_data)

        if self.summary_messages:
            if len(self.summary_messages) == 1:
                summary_message = self.summary_messages[0]
            else:
                # Multi-line format for multiple messages
                summary_message = '\n  ' + '\n  '.join(self.summary_messages)
            self.logger.log(SUMMARY, summary_message)
    
    def _preserve_error_log(self) -> None:
        """Preserve the current log file if it contains warnings or errors.

        Copies logs with WARNING/ERROR/CRITICAL entries to logs/errors/ subfolder
        for longer retention. Only runs if keep_error_logs_days > 0.
        """
        if self.keep_error_logs_days <= 0:
            return

        if not self.current_log_file or not self.current_log_file.exists():
            return

        # Check if log contains warning/error entries
        try:
            with open(self.current_log_file, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()

            # Look for WARNING, ERROR, or CRITICAL level entries
            has_errors = any(
                level in content
                for level in [' - WARNING - ', ' - ERROR - ', ' - CRITICAL - ']
            )

            if not has_errors:
                return

            # Create errors subfolder
            errors_folder = self.logs_folder / "errors"
            errors_folder.mkdir(exist_ok=True)

            # Copy to errors folder
            import shutil
            dest_file = errors_folder / self.current_log_file.name
            shutil.copy2(self.current_log_file, dest_file)
            logging.debug(f"Preserved error log: {dest_file}")

        except Exception as e:
            # Don't fail the run if error preservation fails
            logging.debug(f"Could not preserve error log: {e}")

    def _clean_old_error_logs(self) -> None:
        """Clean up error logs older than keep_error_logs_days.

        Only runs if keep_error_logs_days > 0.
        """
        if self.keep_error_logs_days <= 0:
            return

        errors_folder = self.logs_folder / "errors"
        if not errors_folder.exists():
            return

        try:
            cutoff_time = time.time() - (self.keep_error_logs_days * 24 * 60 * 60)

            for log_file in errors_folder.glob(self.log_file_pattern):
                try:
                    if log_file.stat().st_mtime < cutoff_time:
                        log_file.unlink()
                        logging.debug(f"Removed old error log: {log_file.name}")
                except OSError:
                    pass  # Ignore files we can't access/delete

        except Exception as e:
            logging.debug(f"Could not clean old error logs: {e}")

    def shutdown(self) -> None:
        """Shutdown logging, preserving error logs if configured."""
        # Preserve error log before shutdown (must happen before handlers close)
        self._preserve_error_log()
        # Clean up old error logs
        self._clean_old_error_logs()
        logging.shutdown() 
