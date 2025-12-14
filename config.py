"""
Configuration management for PlexCache.
Handles loading, validation, and management of application settings.
"""

import json
import os
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass

# Get the directory where config.py is located
_SCRIPT_DIR = Path(os.path.dirname(os.path.abspath(__file__)))


@dataclass
class NotificationConfig:
    """Configuration for notification settings."""
    notification_type: str = "system"  # "Unraid", "Webhook", "Both", or "System"
    unraid_level: str = "summary"
    webhook_level: str = ""
    webhook_url: str = ""
    webhook_headers: Optional[Dict[str, str]] = None

    def __post_init__(self):
        if self.webhook_headers is None:
            self.webhook_headers = {}


@dataclass
class PathConfig:
    """Configuration for file paths and directories."""
    script_folder: str = str(_SCRIPT_DIR)
    logs_folder: str = str(_SCRIPT_DIR / "logs")
    plex_source: str = ""
    real_source: str = ""
    cache_dir: str = ""
    nas_library_folders: Optional[List[str]] = None
    plex_library_folders: Optional[List[str]] = None

    def __post_init__(self):
        if self.nas_library_folders is None:
            self.nas_library_folders = []
        if self.plex_library_folders is None:
            self.plex_library_folders = []


@dataclass
class PlexConfig:
    """Configuration for Plex server settings."""
    plex_url: str = ""
    plex_token: str = ""
    valid_sections: Optional[List[int]] = None
    number_episodes: int = 10
    days_to_monitor: int = 183
    users_toggle: bool = True
    skip_ondeck: Optional[List[str]] = None
    skip_watchlist: Optional[List[str]] = None
    users: Optional[List[dict]] = None  # User list from settings file

    def __post_init__(self):
        if self.valid_sections is None:
            self.valid_sections = []
        if self.skip_ondeck is None:
            self.skip_ondeck = []
        if self.skip_watchlist is None:
            self.skip_watchlist = []
        if self.users is None:
            self.users = []


@dataclass
class CacheConfig:
    """Configuration for caching behavior."""
    watchlist_toggle: bool = True
    watchlist_episodes: int = 5
    watched_move: bool = True

    # Remote watchlist via RSS
    remote_watchlist_toggle: bool = False
    remote_watchlist_rss_url: str = ""

    # Cache retention: how long files stay on cache before being moved back to array
    # Files cached less than this many hours ago will not be restored to array
    # Applies to all cached files (OnDeck, Watchlist, etc.) to protect against accidental changes
    cache_retention_hours: int = 12

    # Watchlist retention: auto-expire watchlist items after X days
    # Files are removed from cache X days after being added to watchlist, even if still on watchlist
    # 0 = disabled (files stay as long as they're on any user's watchlist)
    # Supports fractional days (e.g., 0.5 = 12 hours) for testing
    watchlist_retention_days: float = 0

    # Cache size limit: maximum space PlexCache can use on the cache drive
    # Supports formats: "250GB", "500MB", "50%", or just "250" (defaults to GB)
    # Empty string or "0" means no limit
    cache_limit: str = ""
    cache_limit_bytes: int = 0  # Parsed value in bytes (computed from cache_limit)

    # Smart cache eviction settings
    # cache_eviction_mode: "smart" (priority-based), "fifo" (oldest first), or "none" (disabled)
    cache_eviction_mode: str = "none"
    # Start evicting when cache reaches this percentage of cache_limit (e.g., 90 = 90%)
    cache_eviction_threshold_percent: int = 90
    # Only evict items with priority score below this threshold (0-100)
    eviction_min_priority: int = 60



@dataclass
class PerformanceConfig:
    """Configuration for performance settings."""
    max_concurrent_moves_array: int = 2
    max_concurrent_moves_cache: int = 5
    retry_limit: int = 5
    delay: int = 10
    permissions: int = 0o777


class ConfigManager:
    """Manages application configuration loading and validation."""
    
    def __init__(self, config_file: str):
        self.config_file = Path(config_file)
        self.settings_data: Dict[str, Any] = {}
        self.notification = NotificationConfig()
        self.paths = PathConfig()
        self.plex = PlexConfig()
        self.cache = CacheConfig()
        self.performance = PerformanceConfig()
        self.debug = False
        self.exit_if_active_session = False
        
    def load_config(self) -> None:
        """Load configuration from file and validate."""
        logging.debug(f"Loading configuration from: {self.config_file}")
        
        if not self.config_file.exists():
            logging.error(f"Settings file not found: {self.config_file}")
            raise FileNotFoundError(f"Settings file not found: {self.config_file}")
        
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                self.settings_data = json.load(f)
            logging.debug("Configuration file loaded successfully")
        except json.JSONDecodeError as e:
            logging.error(f"Invalid JSON in settings file: {type(e).__name__}: {e}")
            raise ValueError(f"Invalid JSON in settings file: {e}")
        
        logging.debug("Processing configuration...")
        self._validate_required_fields()
        self._validate_types()
        self._process_first_start()
        self._load_all_configs()
        self._validate_values()
        self._save_updated_config()
        logging.debug("Configuration loaded and validated successfully")
    
    def _process_first_start(self) -> None:
        """Handle first start configuration."""
        firststart = self.settings_data.get('firststart')
        if firststart:
            self.debug = True
            logging.warning("First start is set to true, setting debug mode temporarily to true.")
            del self.settings_data['firststart']
        else:
            self.debug = self.settings_data.get('debug', False)
            if firststart is not None:
                del self.settings_data['firststart']
    
    def _load_all_configs(self) -> None:
        """Load all configuration sections."""
        self._load_plex_config()
        self._load_cache_config()
        self._load_path_config()
        self._load_performance_config()
        self._load_notification_config()
        self._load_misc_config()
    
    def _load_plex_config(self) -> None:
        """Load Plex-related configuration."""
        self.plex.plex_url = self.settings_data['PLEX_URL']
        self.plex.plex_token = self.settings_data['PLEX_TOKEN']
        self.plex.number_episodes = self.settings_data['number_episodes']
        self.plex.valid_sections = self.settings_data['valid_sections']
        self.plex.days_to_monitor = self.settings_data['days_to_monitor']
        self.plex.users_toggle = self.settings_data['users_toggle']
        
        # Handle skip settings
        skip_users = self.settings_data.get('skip_users')
        if skip_users is not None:
            self.plex.skip_ondeck = self.settings_data.get('skip_ondeck', skip_users)
            self.plex.skip_watchlist = self.settings_data.get('skip_watchlist', skip_users)
            del self.settings_data['skip_users']
        else:
            self.plex.skip_ondeck = self.settings_data.get('skip_ondeck', [])
            self.plex.skip_watchlist = self.settings_data.get('skip_watchlist', [])

        # Load users list (contains tokens for all users including remote)
        self.plex.users = self.settings_data.get('users', [])
    
    def _load_cache_config(self) -> None:
        """Load cache-related configuration."""
        self.cache.watchlist_toggle = self.settings_data['watchlist_toggle']
        self.cache.watchlist_episodes = self.settings_data['watchlist_episodes']
        self.cache.watched_move = self.settings_data['watched_move']

        # Load remote watchlist settings
        self.cache.remote_watchlist_toggle = self.settings_data.get('remote_watchlist_toggle', False)
        self.cache.remote_watchlist_rss_url = self.settings_data.get('remote_watchlist_rss_url', "")

        # Log deprecation warning for old cache expiry settings (these are now ignored)
        if 'watchlist_cache_expiry' in self.settings_data or 'watched_cache_expiry' in self.settings_data:
            logging.debug("Note: watchlist_cache_expiry and watched_cache_expiry settings are deprecated and ignored. Data is now always fetched fresh.")

        # Load cache retention setting (default 12 hours)
        self.cache.cache_retention_hours = self.settings_data.get('cache_retention_hours', 12)

        # Load watchlist retention setting (default 0 = disabled)
        self.cache.watchlist_retention_days = self.settings_data.get('watchlist_retention_days', 0)

        # Load and parse cache limit setting
        self.cache.cache_limit = self.settings_data.get('cache_limit', "")
        self.cache.cache_limit_bytes = self._parse_cache_limit(self.cache.cache_limit)

        # Load smart eviction settings (default: disabled)
        self.cache.cache_eviction_mode = self.settings_data.get('cache_eviction_mode', "none")
        self.cache.cache_eviction_threshold_percent = self.settings_data.get('cache_eviction_threshold_percent', 90)
        self.cache.eviction_min_priority = self.settings_data.get('eviction_min_priority', 60)

        # Validate eviction settings
        if self.cache.cache_eviction_mode not in ("smart", "fifo", "none"):
            logging.warning(f"Invalid cache_eviction_mode '{self.cache.cache_eviction_mode}', using 'none'")
            self.cache.cache_eviction_mode = "none"
        if not 1 <= self.cache.cache_eviction_threshold_percent <= 100:
            logging.warning(f"Invalid cache_eviction_threshold_percent '{self.cache.cache_eviction_threshold_percent}', using 90")
            self.cache.cache_eviction_threshold_percent = 90
        if not 0 <= self.cache.eviction_min_priority <= 100:
            logging.warning(f"Invalid eviction_min_priority '{self.cache.eviction_min_priority}', using 60")
            self.cache.eviction_min_priority = 60

    def _load_path_config(self) -> None:
        """Load path-related configuration."""
        self.paths.plex_source = self._add_trailing_slashes(self.settings_data['plex_source'])
        self.paths.real_source = self._add_trailing_slashes(self.settings_data['real_source'])
        self.paths.cache_dir = self._add_trailing_slashes(self.settings_data['cache_dir'])
        self.paths.nas_library_folders = self._remove_all_slashes(self.settings_data['nas_library_folders'])
        self.paths.plex_library_folders = self._remove_all_slashes(self.settings_data['plex_library_folders'])
    
    def _load_performance_config(self) -> None:
        """Load performance-related configuration."""
        self.performance.max_concurrent_moves_array = self.settings_data['max_concurrent_moves_array']
        self.performance.max_concurrent_moves_cache = self.settings_data['max_concurrent_moves_cache']

    def _load_notification_config(self) -> None:
        """Load notification-related configuration."""
        self.notification.notification_type = self.settings_data.get('notification_type', 'system')
        self.notification.unraid_level = self.settings_data.get('unraid_level', 'summary')
        self.notification.webhook_level = self.settings_data.get('webhook_level', '')
        self.notification.webhook_url = self.settings_data.get('webhook_url', '')
        self.notification.webhook_headers = self.settings_data.get('webhook_headers', {})

    def _load_misc_config(self) -> None:
        """Load miscellaneous configuration."""
        self.exit_if_active_session = self.settings_data.get('exit_if_active_session')
        if self.exit_if_active_session is None:
            self.exit_if_active_session = not self.settings_data.get('skip', False)
            if 'skip' in self.settings_data:
                del self.settings_data['skip']
        
        # Remove deprecated settings
        if 'unraid' in self.settings_data:
            del self.settings_data['unraid']
    
    def _validate_required_fields(self) -> None:
        """Validate that all required fields exist in the configuration."""
        logging.debug("Validating required fields...")

        required_fields = [
            'PLEX_URL', 'PLEX_TOKEN', 'number_episodes', 'valid_sections',
            'days_to_monitor', 'users_toggle', 'watchlist_toggle',
            'watchlist_episodes', 'watched_move', 'plex_source', 'cache_dir',
            'real_source', 'nas_library_folders', 'plex_library_folders',
            'max_concurrent_moves_array', 'max_concurrent_moves_cache'
        ]

        missing_fields = [field for field in required_fields if field not in self.settings_data]
        if missing_fields:
            logging.error(f"Missing required fields in settings: {missing_fields}")
            raise ValueError(f"Missing required fields in settings: {missing_fields}")

        logging.debug("Required fields validation successful")

    def _validate_types(self) -> None:
        """Validate that configuration values have correct types."""
        logging.debug("Validating configuration types...")

        type_checks = {
            'PLEX_URL': str,
            'PLEX_TOKEN': str,
            'number_episodes': int,
            'valid_sections': list,
            'days_to_monitor': int,
            'users_toggle': bool,
            'watchlist_toggle': bool,
            'watchlist_episodes': int,
            'watched_move': bool,
            'plex_source': str,
            'cache_dir': str,
            'real_source': str,
            'nas_library_folders': list,
            'plex_library_folders': list,
            'max_concurrent_moves_array': int,
            'max_concurrent_moves_cache': int,
        }

        type_errors = []
        for field, expected_type in type_checks.items():
            if field in self.settings_data:
                value = self.settings_data[field]
                if not isinstance(value, expected_type):
                    type_errors.append(
                        f"'{field}' expected {expected_type.__name__}, got {type(value).__name__}"
                    )

        if type_errors:
            error_msg = "Type validation errors: " + "; ".join(type_errors)
            logging.error(error_msg)
            raise TypeError(error_msg)

        logging.debug("Type validation successful")

    def _validate_values(self) -> None:
        """Validate configuration value ranges and constraints."""
        logging.debug("Validating configuration values...")
        errors = []

        # Validate non-empty paths
        path_fields = ['plex_source', 'real_source', 'cache_dir']
        for field in path_fields:
            if not self.settings_data.get(field, '').strip():
                errors.append(f"'{field}' cannot be empty")

        # Validate positive integers
        positive_int_fields = [
            'number_episodes', 'days_to_monitor', 'watchlist_episodes',
            'max_concurrent_moves_array', 'max_concurrent_moves_cache'
        ]
        for field in positive_int_fields:
            value = self.settings_data.get(field, 0)
            if value < 0:
                errors.append(f"'{field}' must be non-negative, got {value}")

        # Validate non-empty URL and token
        if not self.settings_data.get('PLEX_URL', '').strip():
            errors.append("'PLEX_URL' cannot be empty")
        if not self.settings_data.get('PLEX_TOKEN', '').strip():
            errors.append("'PLEX_TOKEN' cannot be empty")

        if errors:
            error_msg = "Configuration validation errors: " + "; ".join(errors)
            logging.error(error_msg)
            raise ValueError(error_msg)

        logging.debug("Value validation successful")
    
    def _save_updated_config(self) -> None:
        """Save updated configuration back to file."""
        try:
            self.settings_data.update({
                'cache_dir': self.paths.cache_dir,
                'real_source': self.paths.real_source,
                'plex_source': self.paths.plex_source,
                'nas_library_folders': self.paths.nas_library_folders,
                'plex_library_folders': self.paths.plex_library_folders,
                'skip_ondeck': self.plex.skip_ondeck,
                'skip_watchlist': self.plex.skip_watchlist,
                'exit_if_active_session': self.exit_if_active_session,
            })

            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(self.settings_data, f, indent=4)
        except Exception as e:
            logging.error(f"Error saving settings: {type(e).__name__}: {e}")
            raise
    
    def _parse_cache_limit(self, limit_str: str) -> int:
        """Parse cache limit string and return bytes.

        Supports formats:
        - "250GB" or "250gb" -> 250 * 1024^3 bytes
        - "500MB" or "500mb" -> 500 * 1024^2 bytes
        - "50%" -> percentage of total cache drive size (computed at runtime)
        - "250" -> defaults to GB (250 * 1024^3 bytes)
        - "" or "0" -> 0 (no limit)

        Returns:
            Bytes as int, or negative value for percentage (e.g., -50 for 50%)
        """
        if not limit_str or limit_str.strip() == "0":
            return 0

        limit_str = limit_str.strip().upper()

        try:
            # Check for percentage
            if limit_str.endswith('%'):
                percent = int(limit_str[:-1])
                if percent <= 0 or percent > 100:
                    logging.warning(f"Invalid cache_limit percentage '{limit_str}', must be 1-100. Using no limit.")
                    return 0
                # Return negative value to indicate percentage (will be computed at runtime)
                return -percent

            # Check for size units
            if limit_str.endswith('GB'):
                size = float(limit_str[:-2])
                return int(size * 1024 * 1024 * 1024)
            elif limit_str.endswith('MB'):
                size = float(limit_str[:-2])
                return int(size * 1024 * 1024)
            elif limit_str.endswith('TB'):
                size = float(limit_str[:-2])
                return int(size * 1024 * 1024 * 1024 * 1024)
            else:
                # No unit specified, default to GB
                size = float(limit_str)
                return int(size * 1024 * 1024 * 1024)

        except ValueError:
            logging.warning(f"Invalid cache_limit value '{limit_str}'. Using no limit.")
            return 0

    @staticmethod
    def _add_trailing_slashes(value: str) -> str:
        """Add trailing slashes to a path."""
        if ':' not in value:  # Not a Windows path
            if not value.startswith("/"):
                value = "/" + value
            if not value.endswith("/"):
                value = value + "/"
        return value
    
    @staticmethod
    def _remove_all_slashes(value_list: List[str]) -> List[str]:
        """Remove all slashes from a list of paths."""
        return [value.strip('/\\') for value in value_list]
    
    def get_mover_exclude_file(self) -> Path:
        """Get the path for the mover exclude file."""
        script_folder = Path(self.paths.script_folder)
        return script_folder / "plexcache_mover_files_to_exclude.txt"
    
    def get_unraid_mover_exclusions_file(self) -> Path:
        """Get the path for the final Unraid mover exclusions file."""
        script_folder = Path(self.paths.script_folder)
        return script_folder / "unraid_mover_exclusions.txt"

    def get_timestamp_file(self) -> Path:
        """Get the path for the cache timestamp tracking file."""
        script_folder = Path(self.paths.script_folder)
        return script_folder / "plexcache_timestamps.json"

    def get_watchlist_tracker_file(self) -> Path:
        """Get the path for the watchlist retention tracker file."""
        script_folder = Path(self.paths.script_folder)
        return script_folder / "plexcache_watchlist_tracker.json" 
