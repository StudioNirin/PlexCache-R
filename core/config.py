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

# Project root detection: if we're in core/, go up one level
# This allows paths to work correctly whether config.py is in root or core/
if _SCRIPT_DIR.name == 'core':
    _PROJECT_ROOT = _SCRIPT_DIR.parent
else:
    _PROJECT_ROOT = _SCRIPT_DIR


@dataclass
class NotificationConfig:
    """Configuration for notification settings."""
    notification_type: str = "system"  # "Unraid", "Webhook", "Both", or "System"
    unraid_level: str = "summary"  # Legacy - kept for backward compatibility
    webhook_level: str = ""  # Legacy - kept for backward compatibility
    webhook_url: str = ""
    # New list-based levels (can select multiple: summary, error, warning)
    unraid_levels: Optional[List[str]] = None
    webhook_levels: Optional[List[str]] = None

    def __post_init__(self):
        # Initialize lists if None
        if self.unraid_levels is None:
            self.unraid_levels = []
        if self.webhook_levels is None:
            self.webhook_levels = []


@dataclass
class PathMapping:
    """Single path mapping configuration for multi-path support.

    Maps a Plex container path to its real filesystem path and optional cache path.
    Allows per-library control over caching behavior.

    Attributes:
        name: Human-readable identifier for logging/diagnostics
        plex_path: Path as Plex sees it (container mount point)
        real_path: Actual filesystem path where PlexCache runs
        cache_path: Cache destination path (None if not cacheable) - container view
        host_cache_path: Host cache path for Docker (None = same as cache_path)
            When running in Docker with remapped volumes, the container sees
            /mnt/cache but the host (Unraid mover) sees /mnt/cache_downloads.
            This field stores the host-side path for the exclude file.
        cacheable: Whether files from this mapping can be moved to cache
        enabled: Toggle mapping on/off without deleting config
    """
    name: str = ""
    plex_path: str = ""
    real_path: str = ""
    cache_path: Optional[str] = None
    host_cache_path: Optional[str] = None  # For Docker: host-side cache path
    cacheable: bool = True
    enabled: bool = True


@dataclass
class PathConfig:
    """Configuration for file paths and directories."""
    script_folder: str = str(_PROJECT_ROOT)
    logs_folder: str = str(_PROJECT_ROOT / "logs")
    data_folder: str = str(_PROJECT_ROOT / "data")

    # Multi-path mapping support (new)
    path_mappings: Optional[List[PathMapping]] = None

    # Legacy single-path fields (deprecated, kept for migration)
    plex_source: str = ""
    real_source: str = ""
    cache_dir: str = ""

    nas_library_folders: Optional[List[str]] = None
    plex_library_folders: Optional[List[str]] = None

    def __post_init__(self):
        if self.path_mappings is None:
            self.path_mappings = []
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

    # Cache drive size: manual override for total cache drive capacity
    # Useful for ZFS pools where auto-detection shows dataset size instead of pool size
    # Supports formats: "3.7TB", "500GB", "250" (defaults to GB)
    # Empty string means auto-detect (use statvfs/shutil.disk_usage)
    cache_drive_size: str = ""
    cache_drive_size_bytes: int = 0  # Parsed value in bytes (0 = auto-detect)

    # Cache size limit: maximum space PlexCache can use on the cache drive
    # Supports formats: "250GB", "500MB", "50%", or just "250" (defaults to GB)
    # Empty string or "0" means no limit
    cache_limit: str = ""
    cache_limit_bytes: int = 0  # Parsed value in bytes (computed from cache_limit)

    # Minimum free space: safety floor to keep on the cache drive
    # Stops caching when free space drops below this, regardless of cache_limit
    # Supports formats: "50GB", "5%", or just "50" (defaults to GB)
    # Empty string or "0" means disabled
    min_free_space: str = ""
    min_free_space_bytes: int = 0  # Parsed value in bytes (computed from min_free_space)

    # PlexCache quota: maximum space for PlexCache-managed files only
    # Unlike cache_limit (which counts all drive usage), this only counts tracked files
    # Supports formats: "500GB", "250MB", "50%", or just "500" (defaults to GB)
    # Empty string or "0" means disabled
    plexcache_quota: str = ""
    plexcache_quota_bytes: int = 0  # Parsed value in bytes

    # Smart cache eviction settings
    # cache_eviction_mode: "smart" (priority-based), "fifo" (oldest first), or "none" (disabled)
    cache_eviction_mode: str = "none"
    # Start evicting when cache reaches this percentage of cache_limit (e.g., 90 = 90%)
    cache_eviction_threshold_percent: int = 90
    # Only evict items with priority score below this threshold (0-100)
    eviction_min_priority: int = 60

    # .plexcached backup files: when moving files to cache, rename array file to .plexcached
    # This provides a backup on the array in case the cache drive fails.
    # Disable this if you use Mover Tuning with cache:prefer shares
    # WARNING: If disabled, cached files CANNOT be recovered if the cache drive fails
    create_plexcached_backups: bool = True

    # Hard-linked files handling (e.g., files linked to seed/downloads folder for torrenting)
    # "skip" - Don't cache hard-linked files; they'll be cached after seeding completes
    # "move" - Cache hard-linked files; seed copy preserved via remaining hard link
    hardlinked_files: str = "skip"

    # Clean up empty parent folders on cache after moving files to array
    # Disable if you use year-based or other intentional empty folder structures
    cleanup_empty_folders: bool = True

    # Excluded folders: skip these directories during cache scanning
    # Hidden directories (dot-prefixed like .Trash, .Recycle.Bin) are always skipped automatically
    # Use this for non-dot-prefixed folders like Synology @Recycle, #recycle, etc.
    excluded_folders: Optional[List[str]] = None

    def __post_init__(self):
        if self.excluded_folders is None:
            self.excluded_folders = []


@dataclass
class PerformanceConfig:
    """Configuration for performance settings."""
    max_concurrent_moves_array: int = 2
    max_concurrent_moves_cache: int = 5
    retry_limit: int = 5
    delay: int = 10
    permissions: int = 0o777


@dataclass
class LoggingConfig:
    """Configuration for logging settings."""
    # Maximum number of log files to keep (default: 24 for hourly runs = 1 day)
    max_log_files: int = 24
    # Keep error logs (containing WARNING/ERROR) for this many days (default: 7)
    # Logs are preserved in logs/errors/ subfolder
    keep_error_logs_days: int = 7


def migrate_path_settings(settings: Dict[str, Any]) -> Tuple[Dict[str, Any], bool]:
    """Migrate legacy single-path settings to multi-path format.

    Converts old plex_source/real_source/cache_dir settings to the new
    path_mappings array format. Preserves original settings for backwards
    compatibility during the transition period.

    Args:
        settings: The raw settings dictionary from JSON file.

    Returns:
        Tuple of (updated_settings, was_migrated).
        was_migrated is True if migration was performed.
    """
    # Already migrated - has path_mappings array
    if "path_mappings" in settings:
        return settings, False

    # Check for legacy settings
    plex_source = settings.get("plex_source", "")
    real_source = settings.get("real_source", "")
    cache_dir = settings.get("cache_dir", "")

    # No legacy settings to migrate - need both plex_source and real_source
    if not plex_source or not real_source:
        return settings, False

    logging.info("Migrating legacy path settings to multi-path format...")

    # Create single mapping from legacy settings
    mapping = {
        "name": "Default (migrated)",
        "plex_path": plex_source,
        "real_path": real_source,
        "cache_path": cache_dir,
        "cacheable": True,
        "enabled": True
    }

    settings["path_mappings"] = [mapping]

    # Keep legacy fields for backwards compatibility (other code may still use them)
    # They will be deprecated over time as code is updated to use path_mappings

    logging.info(f"Migration complete: created mapping '{mapping['name']}'")
    logging.info(f"  plex_path: {mapping['plex_path']}")
    logging.info(f"  real_path: {mapping['real_path']}")
    logging.info(f"  cache_path: {mapping['cache_path']}")

    return settings, True


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
        self.logging = LoggingConfig()
        self.debug = False
        self.exit_if_active_session = False
        self._path_settings_migrated = False
        
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

        # Migrate legacy path settings to multi-path format if needed
        self.settings_data, self._path_settings_migrated = migrate_path_settings(self.settings_data)

        logging.debug("Processing configuration...")
        self._validate_required_fields()
        self._validate_types()
        self._process_first_start()
        self._load_all_configs()
        self._validate_values()
        self._save_updated_config()

        # Ensure data folder exists and migrate tracking files if needed
        self.ensure_data_folder()

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
        self._load_logging_config()
        self._load_misc_config()
    
    def _load_plex_config(self) -> None:
        """Load Plex-related configuration."""
        self.plex.plex_url = self.settings_data['PLEX_URL']
        self.plex.plex_token = self.settings_data['PLEX_TOKEN']
        self.plex.number_episodes = self.settings_data['number_episodes']
        self.plex.valid_sections = self.settings_data['valid_sections']
        self.plex.days_to_monitor = self.settings_data['days_to_monitor']
        self.plex.users_toggle = self.settings_data['users_toggle']
        
        # Load users list first (contains tokens and per-user skip settings)
        self.plex.users = self.settings_data.get('users', [])

        # Auto-migrate legacy top-level skip lists to per-user booleans
        self._migrate_skip_lists_to_per_user()

        # Build skip lists from per-user booleans (single source of truth)
        self.plex.skip_ondeck = []
        self.plex.skip_watchlist = []
        for u in self.plex.users:
            if u.get('skip_ondeck'):
                if u.get('title'):
                    self.plex.skip_ondeck.append(u['title'])
                if u.get('token'):
                    self.plex.skip_ondeck.append(u['token'])
            if u.get('skip_watchlist'):
                if u.get('title'):
                    self.plex.skip_watchlist.append(u['title'])
                if u.get('token'):
                    self.plex.skip_watchlist.append(u['token'])
    
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

        # Load and parse cache drive size override (for ZFS/etc)
        self.cache.cache_drive_size = self.settings_data.get('cache_drive_size', "")
        self.cache.cache_drive_size_bytes = self._parse_size_bytes(self.cache.cache_drive_size)

        # Load and parse cache limit setting
        self.cache.cache_limit = self.settings_data.get('cache_limit', "")
        self.cache.cache_limit_bytes = self._parse_cache_limit(self.cache.cache_limit)

        # Load and parse min free space setting
        self.cache.min_free_space = self.settings_data.get('min_free_space', "")
        self.cache.min_free_space_bytes = self._parse_cache_limit(self.cache.min_free_space)

        # Load and parse plexcache quota setting
        self.cache.plexcache_quota = self.settings_data.get('plexcache_quota', "")
        self.cache.plexcache_quota_bytes = self._parse_cache_limit(self.cache.plexcache_quota)

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

        # Load .plexcached backup setting (default True for safety)
        self.cache.create_plexcached_backups = self.settings_data.get('create_plexcached_backups', True)

        # Load hard-linked files handling setting (default "skip" for safety)
        hardlinked_files = self.settings_data.get('hardlinked_files', 'skip')
        if hardlinked_files not in ('skip', 'move'):
            logging.warning(f"Invalid hardlinked_files '{hardlinked_files}', using 'skip'")
            hardlinked_files = 'skip'
        self.cache.hardlinked_files = hardlinked_files

        # Load cleanup_empty_folders setting (default True to preserve existing behavior)
        self.cache.cleanup_empty_folders = self.settings_data.get('cleanup_empty_folders', True)

        # Load excluded folders for directory scanning
        excluded_folders = self.settings_data.get('excluded_folders', [])
        if isinstance(excluded_folders, list):
            # Filter out empty strings
            self.cache.excluded_folders = [f for f in excluded_folders if f and isinstance(f, str)]
        else:
            self.cache.excluded_folders = []

    def _load_path_config(self) -> None:
        """Load path-related configuration."""
        # Load cache_dir (always required)
        self.paths.cache_dir = self._add_trailing_slashes(self.settings_data['cache_dir'])

        # Load legacy single-path settings (optional if path_mappings configured)
        plex_source = self.settings_data.get('plex_source', '')
        real_source = self.settings_data.get('real_source', '')
        self.paths.plex_source = self._add_trailing_slashes(plex_source) if plex_source else ''
        self.paths.real_source = self._add_trailing_slashes(real_source) if real_source else ''

        # Load legacy library folder arrays (optional if path_mappings configured)
        self.paths.nas_library_folders = self._remove_all_slashes(
            self.settings_data.get('nas_library_folders', [])
        )
        self.paths.plex_library_folders = self._remove_all_slashes(
            self.settings_data.get('plex_library_folders', [])
        )

        # Load multi-path mappings (new format)
        self.paths.path_mappings = []
        for mapping_data in self.settings_data.get('path_mappings', []):
            cache_path = self._add_trailing_slashes(mapping_data['cache_path']) if mapping_data.get('cache_path') else None
            host_cache_path = self._add_trailing_slashes(mapping_data['host_cache_path']) if mapping_data.get('host_cache_path') else None
            mapping = PathMapping(
                name=mapping_data.get('name', 'Unnamed'),
                plex_path=self._add_trailing_slashes(mapping_data.get('plex_path', '')),
                real_path=self._add_trailing_slashes(mapping_data.get('real_path', '')),
                cache_path=cache_path,
                host_cache_path=host_cache_path,
                cacheable=mapping_data.get('cacheable', True),
                enabled=mapping_data.get('enabled', True)
            )
            self.paths.path_mappings.append(mapping)
            if host_cache_path and host_cache_path != cache_path:
                logging.debug(f"Loaded path mapping: {mapping.name} ({mapping.plex_path} -> {mapping.real_path})")
                logging.debug(f"  Cache: {mapping.cache_path} -> Host: {mapping.host_cache_path}")
            else:
                logging.debug(f"Loaded path mapping: {mapping.name} ({mapping.plex_path} -> {mapping.real_path})")
    
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
        # New list-based levels (can select multiple: summary, error, warning)
        self.notification.unraid_levels = self.settings_data.get('unraid_levels', [])
        self.notification.webhook_levels = self.settings_data.get('webhook_levels', [])

    def _load_logging_config(self) -> None:
        """Load logging-related configuration."""
        # Max log files (default: 24 for hourly runs)
        self.logging.max_log_files = self.settings_data.get('max_log_files', 24)
        if self.logging.max_log_files < 1:
            logging.warning(f"Invalid max_log_files '{self.logging.max_log_files}', using 24")
            self.logging.max_log_files = 24

        # Error log retention (default: 7 days)
        self.logging.keep_error_logs_days = self.settings_data.get('keep_error_logs_days', 7)
        if self.logging.keep_error_logs_days < 0:
            logging.warning(f"Invalid keep_error_logs_days '{self.logging.keep_error_logs_days}', using 7")
            self.logging.keep_error_logs_days = 7

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

    def _migrate_skip_lists_to_per_user(self) -> None:
        """Migrate legacy top-level skip lists to per-user booleans.

        Old format (legacy):
            "skip_ondeck": ["Paige", "John"],
            "skip_watchlist": ["Paige"]

        New format (single source of truth):
            "users": [
                {"title": "Paige", "skip_ondeck": true, "skip_watchlist": true},
                {"title": "John", "skip_ondeck": true}
            ]

        This migration runs once on startup, updates the config, and saves it.
        """
        # Check for legacy skip lists or deprecated skip_users
        legacy_skip_ondeck = self.settings_data.get('skip_ondeck', [])
        legacy_skip_watchlist = self.settings_data.get('skip_watchlist', [])
        legacy_skip_users = self.settings_data.get('skip_users', [])

        # Handle deprecated skip_users (applied to both ondeck and watchlist)
        if legacy_skip_users:
            legacy_skip_ondeck = legacy_skip_ondeck or legacy_skip_users
            legacy_skip_watchlist = legacy_skip_watchlist or legacy_skip_users

        # Nothing to migrate if no legacy lists exist
        if not legacy_skip_ondeck and not legacy_skip_watchlist:
            return

        # Check if already migrated (users have skip booleans set)
        users_have_skip_settings = any(
            u.get('skip_ondeck') or u.get('skip_watchlist')
            for u in self.plex.users
        )
        if users_have_skip_settings:
            # Already migrated, just clean up legacy fields
            self._remove_legacy_skip_fields()
            return

        # Migrate: set per-user booleans based on legacy lists
        migrated = False
        for user in self.plex.users:
            username = user.get('title', '')
            token = user.get('token', '')

            # Check if user is in skip_ondeck list (by name or token)
            if username in legacy_skip_ondeck or token in legacy_skip_ondeck:
                user['skip_ondeck'] = True
                migrated = True

            # Check if user is in skip_watchlist list (by name or token)
            if username in legacy_skip_watchlist or token in legacy_skip_watchlist:
                user['skip_watchlist'] = True
                migrated = True

        if migrated:
            # Update settings_data with migrated users
            self.settings_data['users'] = self.plex.users
            logging.info(f"Migrated skip settings to per-user booleans")

        # Remove legacy fields and save
        self._remove_legacy_skip_fields()

    def _remove_legacy_skip_fields(self) -> None:
        """Remove legacy skip list fields from settings and save."""
        changed = False
        for field in ['skip_ondeck', 'skip_watchlist', 'skip_users']:
            if field in self.settings_data:
                del self.settings_data[field]
                changed = True

        if changed:
            try:
                with open(self.config_file, 'w', encoding='utf-8') as f:
                    json.dump(self.settings_data, f, indent=4)
                logging.debug("Removed legacy skip list fields from config")
            except Exception as e:
                logging.warning(f"Could not save migrated config: {e}")

    def _validate_required_fields(self) -> None:
        """Validate that all required fields exist in the configuration."""
        logging.debug("Validating required fields...")

        # Check if path_mappings is configured (makes legacy path fields optional)
        has_path_mappings = bool(self.settings_data.get('path_mappings'))

        # Core required fields (always required)
        required_fields = [
            'PLEX_URL', 'PLEX_TOKEN', 'number_episodes', 'valid_sections',
            'days_to_monitor', 'users_toggle', 'watchlist_toggle',
            'watchlist_episodes', 'watched_move', 'cache_dir',
            'max_concurrent_moves_array', 'max_concurrent_moves_cache'
        ]

        # Legacy path fields (only required if path_mappings not configured)
        if not has_path_mappings:
            required_fields.extend([
                'plex_source', 'real_source', 'nas_library_folders', 'plex_library_folders'
            ])

        missing_fields = [field for field in required_fields if field not in self.settings_data]
        if missing_fields:
            logging.error(f"Missing required fields in settings: {missing_fields}")
            raise ValueError(f"Missing required fields in settings: {missing_fields}")

        logging.debug("Required fields validation successful")

    def _validate_types(self) -> None:
        """Validate that configuration values have correct types."""
        logging.debug("Validating configuration types...")

        # Check if path_mappings is configured (makes legacy path fields optional)
        has_path_mappings = bool(self.settings_data.get('path_mappings'))

        # Core type checks (always validated)
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
            'cache_dir': str,
            'max_concurrent_moves_array': int,
            'max_concurrent_moves_cache': int,
        }

        # Legacy path field types (only checked if path_mappings not configured)
        if not has_path_mappings:
            type_checks.update({
                'plex_source': str,
                'real_source': str,
                'nas_library_folders': list,
                'plex_library_folders': list,
            })

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

        # Check if path_mappings is configured (makes legacy path fields optional)
        has_path_mappings = bool(self.settings_data.get('path_mappings'))

        # Validate non-empty paths (legacy fields only required if no path_mappings)
        if has_path_mappings:
            path_fields = ['cache_dir']  # Only cache_dir needed with path_mappings
        else:
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
            # Core settings (always saved)
            # Note: skip_ondeck/skip_watchlist are now derived from per-user booleans
            # in the users list, so we don't save them as top-level keys anymore
            self.settings_data.update({
                'cache_dir': self.paths.cache_dir,
                'exit_if_active_session': self.exit_if_active_session,
            })

            # Remove legacy top-level skip lists if they exist (migrated to per-user booleans)
            self.settings_data.pop('skip_ondeck', None)
            self.settings_data.pop('skip_watchlist', None)

            # Legacy path fields (only save if they have values - allows clean removal)
            if self.paths.plex_source:
                self.settings_data['plex_source'] = self.paths.plex_source
            if self.paths.real_source:
                self.settings_data['real_source'] = self.paths.real_source
            if self.paths.nas_library_folders:
                self.settings_data['nas_library_folders'] = self.paths.nas_library_folders
            if self.paths.plex_library_folders:
                self.settings_data['plex_library_folders'] = self.paths.plex_library_folders

            # Save path_mappings if present
            if self.paths.path_mappings:
                self.settings_data['path_mappings'] = [
                    {
                        'name': m.name,
                        'plex_path': m.plex_path,
                        'real_path': m.real_path,
                        'cache_path': m.cache_path,
                        'host_cache_path': m.host_cache_path,
                        'cacheable': m.cacheable,
                        'enabled': m.enabled
                    }
                    for m in self.paths.path_mappings
                ]

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

    def _parse_size_bytes(self, size_str: str) -> int:
        """Parse size string and return bytes.

        Supports formats:
        - "3.7TB" or "3.7tb" -> 3.7 * 1024^4 bytes
        - "250GB" or "250gb" -> 250 * 1024^3 bytes
        - "500MB" or "500mb" -> 500 * 1024^2 bytes
        - "250" -> defaults to GB (250 * 1024^3 bytes)
        - "" -> 0 (auto-detect)

        Returns:
            Bytes as int, or 0 for auto-detect
        """
        if not size_str or size_str.strip() == "0":
            return 0

        size_str = size_str.strip().upper()

        try:
            # Check for size units
            if size_str.endswith('TB'):
                size = float(size_str[:-2])
                return int(size * 1024 * 1024 * 1024 * 1024)
            elif size_str.endswith('GB'):
                size = float(size_str[:-2])
                return int(size * 1024 * 1024 * 1024)
            elif size_str.endswith('MB'):
                size = float(size_str[:-2])
                return int(size * 1024 * 1024)
            elif size_str.endswith('T'):
                size = float(size_str[:-1])
                return int(size * 1024 * 1024 * 1024 * 1024)
            elif size_str.endswith('G'):
                size = float(size_str[:-1])
                return int(size * 1024 * 1024 * 1024)
            elif size_str.endswith('M'):
                size = float(size_str[:-1])
                return int(size * 1024 * 1024)
            else:
                # No unit specified, default to GB
                size = float(size_str)
                return int(size * 1024 * 1024 * 1024)

        except ValueError:
            logging.warning(f"Invalid size value '{size_str}'. Using auto-detect.")
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
    
    def get_data_folder(self) -> Path:
        """Get the path for the data folder (tracking files)."""
        return Path(self.paths.data_folder)

    def get_cached_files_file(self) -> Path:
        """Get the path for the cached files log."""
        script_folder = Path(self.paths.script_folder)
        return script_folder / "plexcache_cached_files.txt"

    def get_unraid_mover_exclusions_file(self) -> Path:
        """Get the path for the final Unraid mover exclusions file."""
        script_folder = Path(self.paths.script_folder)
        return script_folder / "unraid_mover_exclusions.txt"

    def get_timestamp_file(self) -> Path:
        """Get the path for the cache timestamp tracking file."""
        return self.get_data_folder() / "timestamps.json"

    def get_watchlist_tracker_file(self) -> Path:
        """Get the path for the watchlist retention tracker file."""
        return self.get_data_folder() / "watchlist_tracker.json"

    def get_ondeck_tracker_file(self) -> Path:
        """Get the path for the OnDeck tracker file."""
        return self.get_data_folder() / "ondeck_tracker.json"

    def get_user_tokens_file(self) -> Path:
        """Get the path for the user tokens cache file."""
        return self.get_data_folder() / "user_tokens.json"

    def get_rss_cache_file(self) -> Path:
        """Get the path for the RSS feed cache file."""
        return self.get_data_folder() / "rss_cache.json"

    def get_lock_file(self) -> Path:
        """Get the path for the instance lock file."""
        script_folder = Path(self.paths.script_folder)
        return script_folder / "plexcache.lock"

    def has_legacy_path_arrays(self) -> bool:
        """Check if legacy path arrays are still in use.

        Returns True if nas_library_folders or plex_library_folders are populated
        alongside path_mappings. These legacy arrays are deprecated and should be
        migrated to path_mappings.

        Returns:
            True if legacy arrays are present and should be deprecated.
        """
        has_mappings = bool(self.paths.path_mappings)
        has_legacy = bool(self.paths.nas_library_folders) or bool(self.paths.plex_library_folders)
        return has_mappings and has_legacy

    def get_legacy_array_info(self) -> str:
        """Get info about legacy path arrays for deprecation messages.

        Returns:
            String describing which legacy arrays are present.
        """
        arrays = []
        if self.paths.nas_library_folders:
            arrays.append(f"nas_library_folders ({len(self.paths.nas_library_folders)} entries)")
        if self.paths.plex_library_folders:
            arrays.append(f"plex_library_folders ({len(self.paths.plex_library_folders)} entries)")
        return ", ".join(arrays) if arrays else "none"

    def ensure_data_folder(self) -> None:
        """Ensure the data folder exists and migrate tracking files from root if needed."""
        import shutil

        data_folder = self.get_data_folder()
        script_folder = Path(self.paths.script_folder)

        # Create data folder if it doesn't exist
        if not data_folder.exists():
            data_folder.mkdir(parents=True, exist_ok=True)
            logging.debug(f"Created data folder: {data_folder}")

        # Define migration mapping: (old_name_in_root, new_name_in_data)
        migrations = [
            ("plexcache_timestamps.json", "timestamps.json"),
            ("plexcache_ondeck_tracker.json", "ondeck_tracker.json"),
            ("plexcache_watchlist_tracker.json", "watchlist_tracker.json"),
            ("plexcache_user_tokens.json", "user_tokens.json"),
            ("plexcache_rss_cache.json", "rss_cache.json"),
        ]

        migrated_count = 0
        for old_name, new_name in migrations:
            old_path = script_folder / old_name
            new_path = data_folder / new_name

            # Skip if old file doesn't exist or new file already exists
            if not old_path.exists():
                continue
            if new_path.exists():
                logging.debug(f"Skipping migration of {old_name}: {new_name} already exists in data/")
                continue

            # Migrate the file
            try:
                shutil.move(str(old_path), str(new_path))
                logging.info(f"Migrated {old_name} -> data/{new_name}")
                migrated_count += 1
            except (OSError, shutil.Error) as e:
                logging.warning(f"Failed to migrate {old_name}: {e}")

        if migrated_count > 0:
            logging.info(f"Migrated {migrated_count} tracking file(s) to data/ folder")
