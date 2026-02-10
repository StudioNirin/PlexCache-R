"""Settings service - load and save PlexCache settings"""

import json
import logging
import os
import threading
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field, asdict

from web.config import PROJECT_ROOT, DATA_DIR, SETTINGS_FILE

logger = logging.getLogger(__name__)

# File cache for Plex data (web UI) - use DATA_DIR for Docker compatibility
WEB_PLEX_CACHE_FILE = DATA_DIR / "web_plex_cache.json"


@dataclass
class PathMapping:
    """Represents a path mapping configuration"""
    name: str
    plex_path: str
    real_path: str
    cache_path: Optional[str] = None
    cacheable: bool = True
    enabled: bool = True


@dataclass
class PlexSettings:
    """Plex server settings"""
    plex_url: str = ""
    plex_token: str = ""
    valid_sections: List[int] = field(default_factory=list)
    days_to_monitor: int = 183
    number_episodes: int = 5


@dataclass
class CacheSettings:
    """Cache behavior settings"""
    watchlist_toggle: bool = True
    watchlist_episodes: int = 3
    watchlist_retention_days: int = 0
    watched_move: bool = True
    cache_retention_hours: int = 12
    cache_drive_size: str = ""  # Manual override for drive size (for ZFS)
    cache_limit: str = "250GB"
    cache_eviction_mode: str = "none"
    cache_eviction_threshold_percent: int = 95
    eviction_min_priority: int = 60
    remote_watchlist_toggle: bool = False
    remote_watchlist_rss_url: str = ""


@dataclass
class NotificationSettings:
    """Notification settings"""
    notification_type: str = "system"
    unraid_level: str = "summary"
    webhook_url: str = ""
    webhook_level: str = "summary"


class SettingsService:
    """Service for loading and saving PlexCache settings"""

    def __init__(self):
        self.settings_file = SETTINGS_FILE
        self._cached_settings: Optional[Dict] = None
        self._last_loaded: Optional[datetime] = None
        # Cache for Plex data (libraries, users) - expires after 1 hour
        self._plex_libraries_cache: Optional[List[Dict]] = None
        self._plex_users_cache: Optional[List[Dict]] = None
        self._plex_cache_time: Optional[datetime] = None
        self._plex_cache_ttl = 3600  # 1 hour
        self._cache_lock = threading.Lock()
        self._last_plex_error: Optional[str] = None  # Last Plex connection error
        # Load from file cache on startup
        self._load_plex_cache_from_file()

    def _load_raw(self) -> Dict[str, Any]:
        """Load raw settings from file"""
        if not self.settings_file.exists():
            return {}

        try:
            with open(self.settings_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}

    def _save_raw(self, settings: Dict[str, Any]) -> bool:
        """Save raw settings to file"""
        try:
            with open(self.settings_file, 'w', encoding='utf-8') as f:
                json.dump(settings, f, indent=2)
            self._cached_settings = None  # Invalidate cache
            return True
        except IOError:
            return False

    def _sanitize_path(self, path: Optional[str]) -> Optional[str]:
        """Strip whitespace from path to prevent issues like '/mnt/user0 ' creating bogus directories"""
        if path is None:
            return None
        return path.strip()

    def _sanitize_path_mapping(self, mapping: Dict[str, Any]) -> Dict[str, Any]:
        """Sanitize all path fields in a path mapping"""
        sanitized = mapping.copy()
        path_fields = ["plex_path", "real_path", "cache_path"]
        for field in path_fields:
            if field in sanitized and sanitized[field]:
                sanitized[field] = self._sanitize_path(sanitized[field])
        return sanitized

    def _load_plex_cache_from_file(self):
        """Load Plex data cache from file on startup"""
        try:
            if WEB_PLEX_CACHE_FILE.exists():
                with open(WEB_PLEX_CACHE_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # Check if cache is still valid
                    cache_time_str = data.get("cache_time")
                    if cache_time_str:
                        cache_time = datetime.fromisoformat(cache_time_str)
                        elapsed = (datetime.now() - cache_time).total_seconds()
                        if elapsed < self._plex_cache_ttl:
                            self._plex_libraries_cache = data.get("libraries", [])
                            self._plex_users_cache = data.get("users", [])
                            self._plex_cache_time = cache_time
        except (json.JSONDecodeError, IOError, ValueError):
            pass

    def _save_plex_cache_to_file(self):
        """Save Plex data cache to file"""
        try:
            # Ensure data directory exists
            WEB_PLEX_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
            data = {
                "cache_time": self._plex_cache_time.isoformat() if self._plex_cache_time else None,
                "libraries": self._plex_libraries_cache or [],
                "users": self._plex_users_cache or []
            }
            with open(WEB_PLEX_CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2)
        except IOError:
            pass

    def get_all(self) -> Dict[str, Any]:
        """Get all settings as a dictionary"""
        return self._load_raw()

    def get_plex_settings(self) -> Dict[str, Any]:
        """Get Plex-related settings"""
        raw = self._load_raw()
        return {
            "plex_url": raw.get("PLEX_URL", ""),
            "plex_token": raw.get("PLEX_TOKEN", ""),
            "valid_sections": raw.get("valid_sections", []),
            "days_to_monitor": raw.get("days_to_monitor", 183),
            "number_episodes": raw.get("number_episodes", 5),
            "users_toggle": raw.get("users_toggle", True),
            "skip_ondeck": raw.get("skip_ondeck", []),
            "skip_watchlist": raw.get("skip_watchlist", [])
        }

    def save_plex_settings(self, settings: Dict[str, Any]) -> bool:
        """Save Plex settings"""
        raw = self._load_raw()

        # Check if URL or token changed - if so, invalidate cache
        old_url = raw.get("PLEX_URL", "")
        old_token = raw.get("PLEX_TOKEN", "")
        new_url = settings.get("plex_url", old_url)
        new_token = settings.get("plex_token", old_token)

        raw["PLEX_URL"] = new_url
        raw["PLEX_TOKEN"] = new_token
        if "valid_sections" in settings:
            raw["valid_sections"] = settings["valid_sections"]
        if "days_to_monitor" in settings:
            raw["days_to_monitor"] = int(float(settings["days_to_monitor"]))
        if "number_episodes" in settings:
            raw["number_episodes"] = int(float(settings["number_episodes"]))
        if "users_toggle" in settings:
            raw["users_toggle"] = settings["users_toggle"]
        if "skip_ondeck" in settings:
            raw["skip_ondeck"] = settings["skip_ondeck"]
        if "skip_watchlist" in settings:
            raw["skip_watchlist"] = settings["skip_watchlist"]

        result = self._save_raw(raw)

        # Invalidate cache if credentials changed to force fresh fetch
        if result and (old_url != new_url or old_token != new_token):
            self.invalidate_plex_cache()

        return result

    def get_path_mappings(self) -> List[Dict[str, Any]]:
        """Get path mappings"""
        raw = self._load_raw()
        return raw.get("path_mappings", [])

    def save_path_mappings(self, mappings: List[Dict[str, Any]]) -> bool:
        """Save path mappings (sanitizes paths to strip whitespace)"""
        raw = self._load_raw()
        raw["path_mappings"] = [self._sanitize_path_mapping(m) for m in mappings]
        return self._save_raw(raw)

    def add_path_mapping(self, mapping: Dict[str, Any]) -> bool:
        """Add a new path mapping (sanitizes paths to strip whitespace)"""
        raw = self._load_raw()
        mappings = raw.get("path_mappings", [])
        mappings.append(self._sanitize_path_mapping(mapping))
        raw["path_mappings"] = mappings
        return self._save_raw(raw)

    def update_path_mapping(self, index: int, mapping: Dict[str, Any]) -> bool:
        """Update an existing path mapping by index (sanitizes paths to strip whitespace)"""
        raw = self._load_raw()
        mappings = raw.get("path_mappings", [])
        if 0 <= index < len(mappings):
            mappings[index] = self._sanitize_path_mapping(mapping)
            raw["path_mappings"] = mappings
            return self._save_raw(raw)
        return False

    def delete_path_mapping(self, index: int) -> bool:
        """Delete a path mapping by index"""
        raw = self._load_raw()
        mappings = raw.get("path_mappings", [])
        if 0 <= index < len(mappings):
            mappings.pop(index)
            raw["path_mappings"] = mappings
            return self._save_raw(raw)
        return False

    def get_cache_settings(self) -> Dict[str, Any]:
        """Get cache behavior settings"""
        raw = self._load_raw()
        return {
            "watchlist_toggle": raw.get("watchlist_toggle", True),
            "watchlist_episodes": raw.get("watchlist_episodes", 3),
            "watchlist_retention_days": raw.get("watchlist_retention_days", 0),
            "watched_move": raw.get("watched_move", True),
            "create_plexcached_backups": raw.get("create_plexcached_backups", True),
            "hardlinked_files": raw.get("hardlinked_files", "skip"),
            "cache_retention_hours": raw.get("cache_retention_hours", 12),
            "cache_drive_size": raw.get("cache_drive_size", ""),
            "cache_limit": raw.get("cache_limit", "250GB"),
            "cache_eviction_mode": raw.get("cache_eviction_mode", "none"),
            "cache_eviction_threshold_percent": raw.get("cache_eviction_threshold_percent", 95),
            "eviction_min_priority": raw.get("eviction_min_priority", 60),
            "remote_watchlist_toggle": raw.get("remote_watchlist_toggle", False),
            "remote_watchlist_rss_url": raw.get("remote_watchlist_rss_url", ""),
            "activity_retention_hours": raw.get("activity_retention_hours", 24),
            # Scanning
            "excluded_folders": raw.get("excluded_folders", []),
            # Advanced settings
            "max_concurrent_moves_array": raw.get("max_concurrent_moves_array", 2),
            "max_concurrent_moves_cache": raw.get("max_concurrent_moves_cache", 5),
            "exit_if_active_session": raw.get("exit_if_active_session", False)
        }

    def save_cache_settings(self, settings: Dict[str, Any]) -> bool:
        """Save cache settings"""
        raw = self._load_raw()

        # Safe int converter that handles float strings like "365.0"
        safe_int = lambda x: int(float(x))

        # Map form field names to settings keys
        field_mapping = {
            "watchlist_toggle": ("watchlist_toggle", lambda x: x == "on" or x is True),
            "watchlist_episodes": ("watchlist_episodes", safe_int),
            "watchlist_retention_days": ("watchlist_retention_days", float),
            "watched_move": ("watched_move", lambda x: x == "on" or x is True),
            "create_plexcached_backups": ("create_plexcached_backups", lambda x: x == "on" or x is True),
            "hardlinked_files": ("hardlinked_files", str),
            "cache_retention_hours": ("cache_retention_hours", safe_int),
            "cache_drive_size": ("cache_drive_size", str),
            "cache_limit": ("cache_limit", str),
            "cache_eviction_mode": ("cache_eviction_mode", str),
            "cache_eviction_threshold_percent": ("cache_eviction_threshold_percent", safe_int),
            "eviction_min_priority": ("eviction_min_priority", safe_int),
            "remote_watchlist_toggle": ("remote_watchlist_toggle", lambda x: x == "on" or x is True),
            "remote_watchlist_rss_url": ("remote_watchlist_rss_url", str),
            "activity_retention_hours": ("activity_retention_hours", safe_int),
            # Advanced settings
            "max_concurrent_moves_array": ("max_concurrent_moves_array", safe_int),
            "max_concurrent_moves_cache": ("max_concurrent_moves_cache", safe_int),
            "exit_if_active_session": ("exit_if_active_session", lambda x: x == "on" or x is True)
        }

        for form_field, (setting_key, converter) in field_mapping.items():
            if form_field in settings:
                try:
                    raw[setting_key] = converter(settings[form_field])
                except (ValueError, TypeError):
                    pass  # Keep existing value on conversion error

        # Handle list fields separately (not through field_mapping)
        if "excluded_folders" in settings:
            folders = settings["excluded_folders"]
            if isinstance(folders, list):
                # Filter out empty strings
                raw["excluded_folders"] = [f.strip() for f in folders if f and f.strip()]
            else:
                raw["excluded_folders"] = []

        return self._save_raw(raw)

    def get_notification_settings(self) -> Dict[str, Any]:
        """Get notification settings"""
        raw = self._load_raw()
        return {
            "notification_type": raw.get("notification_type", "system"),
            "unraid_level": raw.get("unraid_level", "summary"),
            "webhook_url": raw.get("webhook_url", ""),
            "webhook_level": raw.get("webhook_level", "summary"),
            # New list-based levels
            "unraid_levels": raw.get("unraid_levels", []),
            "webhook_levels": raw.get("webhook_levels", [])
        }

    def save_notification_settings(self, settings: Dict[str, Any]) -> bool:
        """Save notification settings"""
        raw = self._load_raw()
        raw["notification_type"] = settings.get("notification_type", raw.get("notification_type", "system"))
        raw["webhook_url"] = settings.get("webhook_url", raw.get("webhook_url", ""))
        # New list-based levels
        raw["unraid_levels"] = settings.get("unraid_levels", raw.get("unraid_levels", []))
        raw["webhook_levels"] = settings.get("webhook_levels", raw.get("webhook_levels", []))
        # Legacy fields for backward compatibility
        raw["unraid_level"] = settings.get("unraid_level", raw.get("unraid_level", "summary"))
        raw["webhook_level"] = settings.get("webhook_level", raw.get("webhook_level", "summary"))
        return self._save_raw(raw)

    def get_logging_settings(self) -> Dict[str, Any]:
        """Get logging settings"""
        raw = self._load_raw()
        return {
            "max_log_files": raw.get("max_log_files", 24),
            "keep_error_logs_days": raw.get("keep_error_logs_days", 7),
            "time_format": raw.get("time_format", "24h")
        }

    def save_logging_settings(self, settings: Dict[str, Any]) -> bool:
        """Save logging settings"""
        raw = self._load_raw()

        # Validate and save max_log_files (int(float()) handles "5.0" strings)
        if "max_log_files" in settings:
            try:
                max_log_files = int(float(settings["max_log_files"]))
                if max_log_files >= 1:
                    raw["max_log_files"] = max_log_files
            except (ValueError, TypeError):
                pass

        # Validate and save keep_error_logs_days
        if "keep_error_logs_days" in settings:
            try:
                keep_error_logs_days = int(float(settings["keep_error_logs_days"]))
                if keep_error_logs_days >= 0:
                    raw["keep_error_logs_days"] = keep_error_logs_days
            except (ValueError, TypeError):
                pass

        # Validate and save time_format
        if "time_format" in settings:
            time_format = settings["time_format"]
            if time_format in ("12h", "24h"):
                raw["time_format"] = time_format

        return self._save_raw(raw)

    def check_plex_connection(self) -> bool:
        """Check if Plex server is reachable"""
        settings = self.get_plex_settings()
        plex_url = settings.get("plex_url", "")
        plex_token = settings.get("plex_token", "")

        if not plex_url or not plex_token:
            return False

        try:
            import requests
            # Simple health check
            url = plex_url.rstrip('/') + '/'
            response = requests.get(
                url,
                headers={"X-Plex-Token": plex_token},
                timeout=5
            )
            return response.status_code == 200
        except Exception:
            return False

    def _is_plex_cache_valid(self) -> bool:
        """Check if Plex cache is still valid"""
        if self._plex_cache_time is None:
            return False
        elapsed = (datetime.now() - self._plex_cache_time).total_seconds()
        return elapsed < self._plex_cache_ttl

    def invalidate_plex_cache(self):
        """Invalidate the Plex data cache"""
        self._plex_libraries_cache = None
        self._plex_users_cache = None
        self._plex_cache_time = None

    def get_plex_libraries(self, plex_url: Optional[str] = None, plex_token: Optional[str] = None) -> List[Dict[str, Any]]:
        """Fetch library sections from Plex server (cached)

        Args:
            plex_url: Optional Plex URL (uses saved settings if not provided)
            plex_token: Optional Plex token (uses saved settings if not provided)
        """
        with self._cache_lock:
            # Return cached data if valid (only when using saved credentials)
            if plex_url is None and plex_token is None:
                if self._is_plex_cache_valid() and self._plex_libraries_cache is not None:
                    return self._plex_libraries_cache

        # Use provided credentials or fall back to saved settings
        if plex_url is None or plex_token is None:
            settings = self.get_plex_settings()
            plex_url = plex_url or settings.get("plex_url", "")
            plex_token = plex_token or settings.get("plex_token", "")

        if not plex_url or not plex_token:
            return []

        try:
            from plexapi.server import PlexServer
            plex = PlexServer(plex_url, plex_token, timeout=10)

            libraries = []
            for section in plex.library.sections():
                # Get library locations (paths) for path mapping generation
                locations = []
                try:
                    locations = list(section.locations) if hasattr(section, 'locations') else []
                except Exception:
                    pass

                libraries.append({
                    "id": int(section.key),
                    "title": section.title,
                    "type": section.type,  # 'movie', 'show', 'artist', 'photo'
                    "type_label": {
                        "movie": "Movies",
                        "show": "TV Shows",
                        "artist": "Music",
                        "photo": "Photos"
                    }.get(section.type, section.type.title()),
                    "locations": locations  # Plex paths for this library
                })

            with self._cache_lock:
                self._plex_libraries_cache = sorted(libraries, key=lambda x: x["id"])
                self._plex_cache_time = datetime.now()
                self._save_plex_cache_to_file()
            return self._plex_libraries_cache
        except Exception:
            # Return empty but also return file cache if available
            with self._cache_lock:
                if self._plex_libraries_cache:
                    return self._plex_libraries_cache
            return []

    def get_plex_users(self, plex_url: Optional[str] = None, plex_token: Optional[str] = None) -> List[Dict[str, Any]]:
        """Fetch users from Plex server (cached, including main account)

        Args:
            plex_url: Optional Plex URL (uses saved settings if not provided)
            plex_token: Optional Plex token (uses saved settings if not provided)
        """
        # Check for prefetched users from setup wizard (background fetch)
        if hasattr(self, '_prefetched_users') and self._prefetched_users:
            prefetched = self._prefetched_users
            self._prefetched_users = None  # Clear after use

            # Convert prefetched format to expected format and cache
            users = []
            # Add main account first - use provided credentials or saved settings
            if plex_url is None or plex_token is None:
                settings = self.get_plex_settings()
                plex_url = plex_url or settings.get("plex_url", "")
                plex_token = plex_token or settings.get("plex_token", "")
            if plex_url and plex_token:
                try:
                    from plexapi.server import PlexServer
                    plex = PlexServer(plex_url, plex_token, timeout=10)
                    account = plex.myPlexAccount()
                    users.append({
                        "username": account.username,
                        "title": account.title or account.username,
                        "is_admin": True,
                        "is_home": True
                    })
                except Exception:
                    pass

            # Add prefetched users
            for u in prefetched:
                if u.get('has_access', True):
                    users.append({
                        "username": u.get('title', ''),
                        "title": u.get('title', ''),
                        "is_admin": False,
                        "is_home": u.get('is_home', False)
                    })

            with self._cache_lock:
                self._plex_users_cache = users
                self._plex_cache_time = datetime.now()
            return users

        with self._cache_lock:
            # Return cached data if valid AND not empty (only when using saved credentials)
            if plex_url is None and plex_token is None:
                if self._is_plex_cache_valid() and self._plex_users_cache:
                    return self._plex_users_cache

        # Use provided credentials or fall back to saved settings
        if plex_url is None or plex_token is None:
            settings = self.get_plex_settings()
            plex_url = plex_url or settings.get("plex_url", "")
            plex_token = plex_token or settings.get("plex_token", "")

        if not plex_url or not plex_token:
            self._last_plex_error = "Missing Plex URL or token"
            return []

        try:
            import logging
            from plexapi.server import PlexServer
            plex = PlexServer(plex_url, plex_token, timeout=10)

            users = []
            account_error = None
            shared_users_error = None

            # Add main account first
            try:
                account = plex.myPlexAccount()
                users.append({
                    "username": account.username,
                    "title": account.title or account.username,
                    "is_admin": True,
                    "is_home": True
                })
                logging.info(f"Fetched main account: {account.username}")
            except Exception as e:
                account_error = str(e)
                logging.warning(f"Could not get main account: {e}")

            # Add shared users
            try:
                account = plex.myPlexAccount()
                shared_count = 0
                for user in account.users():
                    # Check if user has access to this server
                    try:
                        token = user.get_token(plex.machineIdentifier)
                        if token is None:
                            continue
                    except Exception:
                        continue

                    is_home = getattr(user, "home", False)
                    users.append({
                        "username": user.title,
                        "title": user.title,
                        "is_admin": False,
                        "is_home": bool(is_home)
                    })
                    shared_count += 1
                logging.info(f"Fetched {shared_count} shared users")
            except Exception as e:
                shared_users_error = str(e)
                logging.warning(f"Could not get shared users: {e}")

            # Set error if we got no users
            if not users:
                if account_error:
                    self._last_plex_error = f"Could not get account info: {account_error[:100]}"
                elif shared_users_error:
                    self._last_plex_error = f"Could not get shared users: {shared_users_error[:100]}"
                else:
                    self._last_plex_error = "No users found (connection OK but no account data returned)"
            else:
                self._last_plex_error = None  # Clear error on success

            with self._cache_lock:
                self._plex_users_cache = users
                self._plex_cache_time = datetime.now()
                self._save_plex_cache_to_file()
            return self._plex_users_cache
        except Exception as e:
            import logging
            error_msg = str(e)
            # Provide more helpful error messages
            if "Connection refused" in error_msg or "Errno 111" in error_msg:
                self._last_plex_error = f"Cannot connect to Plex server. Is it running and accessible from Docker?"
            elif "timed out" in error_msg.lower() or "timeout" in error_msg.lower():
                self._last_plex_error = f"Connection timed out. Try using the local IP (e.g., http://192.168.x.x:32400) instead of .plex.direct URL"
            elif "Name or service not known" in error_msg or "getaddrinfo failed" in error_msg:
                self._last_plex_error = f"Cannot resolve hostname. Try using http://YOUR_LOCAL_IP:32400"
            elif "401" in error_msg or "Unauthorized" in error_msg:
                self._last_plex_error = "Invalid Plex token. Try re-authenticating."
            else:
                self._last_plex_error = f"Plex connection error: {error_msg[:100]}"

            logging.warning(f"Failed to fetch Plex users: {e}")

            # Return file cache if available
            with self._cache_lock:
                if self._plex_users_cache:
                    return self._plex_users_cache
            return []

    def get_last_plex_error(self) -> Optional[str]:
        """Get the last Plex connection error message"""
        return getattr(self, '_last_plex_error', None)

    def get_user_settings(self) -> Dict[str, Any]:
        """Get user-related settings including the full users list."""
        raw = self._load_raw()
        return {
            "users_toggle": raw.get("users_toggle", True),
            "users": raw.get("users", []),
            "skip_ondeck": raw.get("skip_ondeck", []),
            "skip_watchlist": raw.get("skip_watchlist", []),
            "remote_watchlist_toggle": raw.get("remote_watchlist_toggle", False),
            "remote_watchlist_rss_url": raw.get("remote_watchlist_rss_url", "")
        }

    def sync_users_from_plex(self) -> Dict[str, Any]:
        """Sync users from Plex API, preserving skip preferences.

        Returns:
            Dict with: success, users, added_count, removed_count, error
        """
        settings = self.get_plex_settings()
        plex_url = settings.get("plex_url", "")
        plex_token = settings.get("plex_token", "")

        if not plex_url or not plex_token:
            return {"success": False, "error": "Missing Plex URL or token. Configure in Plex settings first."}

        try:
            from plexapi.server import PlexServer

            plex = PlexServer(plex_url, plex_token, timeout=15)
            account = plex.myPlexAccount()
            machine_id = plex.machineIdentifier

            raw = self._load_raw()
            existing_users = {u.get("title"): u for u in raw.get("users", [])}
            existing_skip_ondeck = set(raw.get("skip_ondeck", []))
            existing_skip_watchlist = set(raw.get("skip_watchlist", []))

            new_users = []
            added_count = 0

            # Add main account (admin)
            admin_name = account.title or account.username
            admin_existing = existing_users.get(admin_name, {})
            new_users.append({
                "title": admin_name,
                "id": getattr(account, "id", None),
                "uuid": getattr(account, "uuid", None),
                "token": plex_token,
                "is_local": True,
                "is_admin": True,
                "skip_ondeck": admin_existing.get("skip_ondeck", False),
                "skip_watchlist": admin_existing.get("skip_watchlist", False)
            })
            if admin_name not in existing_users:
                added_count += 1

            # Add shared users
            for user in account.users():
                name = user.title
                try:
                    token = user.get_token(machine_id)
                    if token is None:
                        continue
                except Exception:
                    continue

                # Extract user ID and UUID
                user_id = getattr(user, "id", None)
                user_uuid = None
                thumb = getattr(user, "thumb", "")
                if thumb and "/users/" in thumb:
                    try:
                        user_uuid = thumb.split("/users/")[1].split("/")[0]
                    except (IndexError, AttributeError):
                        pass

                is_home = getattr(user, "home", False)
                is_local = bool(is_home)

                # Preserve existing preferences - check both by name and by username in skip lists
                existing = existing_users.get(name, {})
                skip_ondeck = existing.get("skip_ondeck", name in existing_skip_ondeck)
                # All users can have watchlist disabled (local via API, remote via RSS filtering)
                skip_watchlist = existing.get("skip_watchlist", name in existing_skip_watchlist)

                if name not in existing_users:
                    added_count += 1

                new_users.append({
                    "title": name,
                    "id": user_id,
                    "uuid": user_uuid,
                    "token": token,
                    "is_local": is_local,
                    "is_admin": False,
                    "skip_ondeck": skip_ondeck,
                    "skip_watchlist": skip_watchlist
                })

            # Detect removed users
            current_names = {u["title"] for u in new_users}
            removed_count = len(set(existing_users.keys()) - current_names)

            # Save updated users and rebuild skip lists
            raw["users"] = new_users
            raw["skip_ondeck"] = [u["title"] for u in new_users if u.get("skip_ondeck")]
            # All users can have watchlist disabled (local via API, remote via RSS filtering)
            raw["skip_watchlist"] = [u["title"] for u in new_users if u.get("skip_watchlist")]
            self._save_raw(raw)

            # Clear error on success
            self._last_plex_error = None

            return {
                "success": True,
                "users": new_users,
                "added_count": added_count,
                "removed_count": removed_count
            }

        except Exception as e:
            error_msg = str(e)
            if "Connection refused" in error_msg or "Errno 111" in error_msg:
                error = "Cannot connect to Plex server. Is it running?"
            elif "timed out" in error_msg.lower() or "timeout" in error_msg.lower():
                error = "Connection timed out. Check your Plex URL."
            elif "401" in error_msg or "Unauthorized" in error_msg:
                error = "Invalid Plex token. Try re-authenticating."
            else:
                error = f"Plex error: {error_msg[:100]}"

            self._last_plex_error = error
            return {"success": False, "error": error}

    def save_user_settings(self, users: List[Dict[str, Any]], users_toggle: bool,
                           remote_watchlist_toggle: bool = False,
                           remote_watchlist_rss_url: str = "") -> bool:
        """Save user preferences and rebuild skip lists.

        Args:
            users: List of user dicts with skip_ondeck/skip_watchlist flags
            users_toggle: Whether multi-user support is enabled
            remote_watchlist_toggle: Whether remote watchlist RSS is enabled
            remote_watchlist_rss_url: RSS URL for remote watchlists
        """
        raw = self._load_raw()

        # Update users array
        raw["users"] = users
        raw["users_toggle"] = users_toggle
        raw["remote_watchlist_toggle"] = remote_watchlist_toggle
        raw["remote_watchlist_rss_url"] = remote_watchlist_rss_url

        # Rebuild skip lists from user preferences (use title/username for skip lists)
        raw["skip_ondeck"] = [u["title"] for u in users if u.get("skip_ondeck")]
        # All users can have watchlist disabled (local via API, remote via RSS filtering)
        raw["skip_watchlist"] = [u["title"] for u in users if u.get("skip_watchlist")]

        return self._save_raw(raw)

    def get_last_run_time(self) -> Optional[str]:
        """Get the last time PlexCache ran.

        Reads from data/last_run.txt which is written when operations complete.
        Falls back to recent_activity.json for backwards compatibility.
        """
        last_run_dt = None

        # Primary: Check last_run.txt (written by operation_runner on completion)
        last_run_file = DATA_DIR / "last_run.txt"
        if last_run_file.exists():
            try:
                with open(last_run_file, 'r') as f:
                    timestamp_str = f.read().strip()
                    if timestamp_str:
                        last_run_dt = datetime.fromisoformat(timestamp_str)
            except (IOError, ValueError):
                pass

        # Fallback: Check recent_activity.json for older installs
        if last_run_dt is None:
            activity_file = DATA_DIR / "recent_activity.json"
            if activity_file.exists():
                try:
                    with open(activity_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        if data and len(data) > 0:
                            latest = max(data, key=lambda x: x.get('timestamp', ''))
                            if 'timestamp' in latest:
                                last_run_dt = datetime.fromisoformat(latest['timestamp'])
                except (json.JSONDecodeError, IOError, ValueError):
                    pass

        if last_run_dt is None:
            return None

        # Format relative time
        now = datetime.now()
        diff = now - last_run_dt

        if diff.days > 0:
            return f"{diff.days}d ago"
        elif diff.seconds >= 3600:
            hours = diff.seconds // 3600
            return f"{hours}h ago"
        elif diff.seconds >= 60:
            minutes = diff.seconds // 60
            return f"{minutes}m ago"
        else:
            return "Just now"

    def prefetch_plex_data(self):
        """
        Prefetch Plex libraries and users in background thread.
        Called on startup to warm the cache.
        """
        def _fetch():
            logger.info("Prefetching Plex data in background...")
            try:
                # Force refresh by invalidating cache first if it's stale
                if not self._is_plex_cache_valid():
                    self.get_plex_libraries()
                    self.get_plex_users()
                    logger.info("Plex data prefetch complete")
                else:
                    logger.info("Plex data cache is still valid, skipping prefetch")
            except Exception as e:
                logger.warning(f"Plex data prefetch failed: {e}")

        thread = threading.Thread(target=_fetch, daemon=True)
        thread.start()

    def refresh_plex_cache(self):
        """
        Force refresh Plex cache (called by scheduler hourly).
        Runs synchronously for scheduler use.
        """
        logger.info("Refreshing Plex data cache...")
        try:
            # Invalidate current cache to force refresh
            self.invalidate_plex_cache()
            self.get_plex_libraries()
            self.get_plex_users()
            logger.info("Plex data cache refreshed successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to refresh Plex cache: {e}")
            return False

    def export_settings(self, include_sensitive: bool = True) -> Dict[str, Any]:
        """Export all settings as a dictionary.

        Args:
            include_sensitive: If False, redacts tokens, URLs, usernames, and identifiers

        Returns:
            Settings dictionary ready for JSON serialization
        """
        import copy
        import re
        settings = copy.deepcopy(self._load_raw())

        if not include_sensitive:
            # Remove main Plex token
            if "PLEX_TOKEN" in settings:
                settings["PLEX_TOKEN"] = ""

            # Redact Plex URL (preserve structure, hide IP/machine ID)
            if "PLEX_URL" in settings and settings["PLEX_URL"]:
                url = settings["PLEX_URL"]
                # Preserve protocol and port, redact the host
                match = re.match(r'(https?://)(.+?)(:(\d+))?(/.*)?$', url)
                if match:
                    port = f":{match.group(4)}" if match.group(4) else ""
                    settings["PLEX_URL"] = f"{match.group(1)}[REDACTED]{port}"
                else:
                    settings["PLEX_URL"] = "[REDACTED]"

            # Redact client ID
            if "plexcache_client_id" in settings:
                settings["plexcache_client_id"] = "[REDACTED]"

            # Redact RSS URL (contains auth-embedded feed GUID)
            if "remote_watchlist_rss_url" in settings and settings["remote_watchlist_rss_url"]:
                url = settings["remote_watchlist_rss_url"]
                match = re.match(r'(https?://[^/]+/)(.+)', url)
                if match:
                    settings["remote_watchlist_rss_url"] = f"{match.group(1)}[REDACTED]"
                else:
                    settings["remote_watchlist_rss_url"] = "[REDACTED]"

            # Remove webhook URL (may contain API keys)
            if "webhook_url" in settings:
                settings["webhook_url"] = ""

            # Anonymize users in both _cached_users and users arrays
            for i, user in enumerate(settings.get("_cached_users", []), 1):
                if "username" in user:
                    user["username"] = f"User_{i}"
                if "title" in user:
                    user["title"] = f"User {i}"

            for i, user in enumerate(settings.get("users", []), 1):
                if "title" in user:
                    user["title"] = f"User {i}"
                if "token" in user:
                    user["token"] = ""
                if "id" in user:
                    user["id"] = 0
                if "uuid" in user:
                    user["uuid"] = "[REDACTED]"

        return settings

    def validate_import_settings(self, settings_data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate imported settings structure.

        Args:
            settings_data: Settings dictionary to validate

        Returns:
            Dict with: valid (bool), errors (list), warnings (list)
        """
        errors = []
        warnings = []

        # Check if it's a dict
        if not isinstance(settings_data, dict):
            errors.append("Settings must be a JSON object (dictionary)")
            return {"valid": False, "errors": errors, "warnings": warnings}

        # Check for empty settings
        if not settings_data:
            errors.append("Settings file is empty")
            return {"valid": False, "errors": errors, "warnings": warnings}

        # Check for required Plex settings (warn if missing)
        if not settings_data.get("PLEX_URL"):
            warnings.append("Missing PLEX_URL - will need to configure Plex connection")
        if not settings_data.get("PLEX_TOKEN"):
            warnings.append("Missing PLEX_TOKEN - will need to re-authenticate with Plex")

        # Check path mappings structure
        path_mappings = settings_data.get("path_mappings", [])
        if path_mappings:
            if not isinstance(path_mappings, list):
                errors.append("path_mappings must be a list")
            else:
                for i, mapping in enumerate(path_mappings):
                    if not isinstance(mapping, dict):
                        errors.append(f"Path mapping {i + 1} must be an object")
                    elif not mapping.get("plex_path") or not mapping.get("real_path"):
                        warnings.append(f"Path mapping '{mapping.get('name', i + 1)}' missing plex_path or real_path")

        # Check users structure
        users = settings_data.get("users", [])
        if users:
            if not isinstance(users, list):
                errors.append("users must be a list")
            else:
                users_without_tokens = sum(1 for u in users if not u.get("token"))
                if users_without_tokens > 0:
                    warnings.append(f"{users_without_tokens} user(s) missing tokens - will need to sync users")

        # Check cache limit format
        cache_limit = settings_data.get("cache_limit", "")
        if cache_limit and not any(cache_limit.upper().endswith(suffix) for suffix in ["GB", "TB", "MB"]):
            warnings.append(f"cache_limit '{cache_limit}' may not be a valid format (expected e.g., '250GB')")

        # Check for unknown top-level keys (informational)
        known_keys = {
            "PLEX_URL", "PLEX_TOKEN", "valid_sections", "path_mappings", "users",
            "users_toggle", "skip_ondeck", "skip_watchlist", "watchlist_toggle",
            "watchlist_episodes", "watchlist_retention_days", "watched_move",
            "create_plexcached_backups", "hardlinked_files", "cache_retention_hours",
            "cache_limit", "cache_eviction_mode", "cache_eviction_threshold_percent",
            "eviction_min_priority", "remote_watchlist_toggle", "remote_watchlist_rss_url",
            "notification_type", "unraid_level", "unraid_levels", "webhook_url",
            "webhook_level", "webhook_levels", "max_log_files", "keep_error_logs_days",
            "days_to_monitor", "number_episodes", "activity_retention_hours",
            "excluded_folders",
            # Advanced settings
            "max_concurrent_moves_array", "max_concurrent_moves_cache", "exit_if_active_session",
            # Legacy keys that may exist
            "plex_source", "real_source", "nas_library_folders", "plex_library_folders"
        }
        unknown_keys = set(settings_data.keys()) - known_keys
        if unknown_keys:
            warnings.append(f"Unknown settings will be preserved: {', '.join(sorted(unknown_keys)[:5])}")

        return {
            "valid": len(errors) == 0,
            "errors": errors,
            "warnings": warnings
        }

    def import_settings(self, settings_data: Dict[str, Any], merge: bool = False) -> Dict[str, Any]:
        """Import settings from dictionary.

        Args:
            settings_data: Settings dictionary to import
            merge: If True, merge with existing (imported values win).
                   If False, replace entirely.

        Returns:
            Dict with: success (bool), message (str)
        """
        try:
            if merge:
                # Load existing and merge
                current = self._load_raw()
                # Deep merge for nested structures
                for key, value in settings_data.items():
                    if key == "path_mappings" and isinstance(value, list):
                        # For path mappings, replace entirely if provided
                        current[key] = value
                    elif key == "users" and isinstance(value, list):
                        # For users, replace entirely if provided
                        current[key] = value
                    else:
                        current[key] = value
                settings_to_save = current
            else:
                # Replace entirely
                settings_to_save = settings_data

            success = self._save_raw(settings_to_save)

            if success:
                # Invalidate caches since settings changed
                self.invalidate_plex_cache()
                return {"success": True, "message": "Settings imported successfully"}
            else:
                return {"success": False, "message": "Failed to save settings file"}

        except Exception as e:
            return {"success": False, "message": f"Import error: {str(e)}"}


# Singleton instance
_settings_service: Optional[SettingsService] = None


def get_settings_service() -> SettingsService:
    """Get or create the settings service singleton"""
    global _settings_service
    if _settings_service is None:
        _settings_service = SettingsService()
    return _settings_service
