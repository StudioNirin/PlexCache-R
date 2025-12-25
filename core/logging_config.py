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
from typing import Optional

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


# Define a new level called SUMMARY that is equivalent to INFO level
SUMMARY = logging.WARNING + 1
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
    
    def __init__(self):
        super().__init__()
        self.notify_cmd_base = "/usr/local/emhttp/webGui/scripts/notify"
        if not os.path.isfile(self.notify_cmd_base) or not os.access(self.notify_cmd_base, os.X_OK):
            logging.warning(f"{self.notify_cmd_base} does not exist or is not executable. Unraid notifications will not be sent.")
            self.notify_cmd_base = None

    def emit(self, record):
        if self.notify_cmd_base:
            if record.levelno == SUMMARY:
                self.send_summary_unraid_notification(record)
            else: 
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
    """Custom logging handler for webhook notifications."""
    
    SUMMARY = SUMMARY
    
    def __init__(self, webhook_url: str):
        super().__init__()
        self.webhook_url = webhook_url

    def emit(self, record):
        if record.levelno == SUMMARY:
            self.send_summary_webhook_message(record)
        else:
            self.send_webhook_message(record)

    def send_summary_webhook_message(self, record):
        summary = "Plex Cache Summary:\n" + record.msg
        payload = {
            "content": summary
        }
        headers = {
            "Content-Type": "application/json"
        }
        response = requests.post(self.webhook_url, data=json.dumps(payload), headers=headers)
        if not response.status_code == 204:
            logging.error(f"Failed to send summary message. Error code: {response.status_code}")

    def send_webhook_message(self, record):
        payload = {
            "content": record.msg
        }
        headers = {
            "Content-Type": "application/json"
        }
        response = requests.post(self.webhook_url, data=json.dumps(payload), headers=headers)
        if not response.status_code == 204:
            logging.error(f"Failed to send message. Error code: {response.status_code}")


class LoggingManager:
    """Manages logging configuration and setup."""
    
    def __init__(self, logs_folder: str, log_level: str = "", max_log_files: int = 5):
        self.logs_folder = Path(logs_folder)
        self.log_level = log_level
        self.max_log_files = max_log_files
        self.log_file_pattern = "plexcache_log_*.log"
        self.logger = logging.getLogger()
        self.summary_messages = []
        self.files_moved = False
        
    def setup_logging(self) -> None:
        """Set up logging configuration."""
        self._ensure_logs_folder()
        self._setup_log_file()
        self._set_log_level()
        self._clean_old_log_files()
        # Add filter to downgrade verbose library messages to DEBUG
        self.logger.addFilter(VerboseMessageFilter())
        # Suppress noisy HTTP request logs from urllib3/requests
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("requests").setLevel(logging.WARNING)
        logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
        
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
            unraid_handler = UnraidHandler()
            self._set_handler_level(unraid_handler, notification_config.unraid_level)
            self.logger.addHandler(unraid_handler)
        
        # Set up Webhook handler
        if notification_type in ["both", "webhook"] and notification_config.webhook_url:
            webhook_handler = WebhookHandler(notification_config.webhook_url)
            self._set_handler_level(webhook_handler, notification_config.webhook_level)
            self.logger.addHandler(webhook_handler)
    
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
    
    def add_summary_message(self, message: str) -> None:
        """Add a message to the summary."""
        if self.files_moved:
            self.summary_messages.append(message)
        else:
            self.summary_messages = [message]
            self.files_moved = True
    
    def log_summary(self) -> None:
        """Log the summary message.

        Uses newlines for multi-line output when there are multiple messages.
        """
        if self.summary_messages:
            if len(self.summary_messages) == 1:
                summary_message = self.summary_messages[0]
            else:
                # Multi-line format for multiple messages
                summary_message = '\n  ' + '\n  '.join(self.summary_messages)
            self.logger.log(SUMMARY, summary_message)
    
    def shutdown(self) -> None:
        """Shutdown logging."""
        logging.shutdown() 
