"""Web cache service - manages in-memory and disk caching for web UI performance"""

import json
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Optional
from dataclasses import dataclass

from web.config import PROJECT_ROOT, DATA_DIR


@dataclass
class CacheEntry:
    """A cached data entry with timestamp"""
    data: Any
    updated_at: datetime

    def age_seconds(self) -> float:
        """Get age of cache entry in seconds"""
        return (datetime.now() - self.updated_at).total_seconds()

    def is_stale(self, max_age_seconds: int) -> bool:
        """Check if cache entry is older than max_age_seconds"""
        return self.age_seconds() > max_age_seconds


class WebCacheService:
    """
    Manages caching for expensive operations like file scanning.

    Features:
    - In-memory cache with TTL
    - Disk persistence for instant startup
    - Background refresh task
    - Thread-safe operations
    """

    # Default TTL of 5 minutes
    DEFAULT_TTL_SECONDS = 300

    # Background refresh interval (5 minutes)
    REFRESH_INTERVAL_SECONDS = 300

    def __init__(self):
        self._cache: Dict[str, CacheEntry] = {}
        self._lock = threading.RLock()
        self._refresh_thread: Optional[threading.Thread] = None
        self._stop_refresh = threading.Event()
        self._refresh_callbacks: Dict[str, Callable] = {}
        self._disk_cache_file = DATA_DIR / "web_ui_cache.json"

        # Load from disk on init
        self._load_from_disk()

    def get(self, key: str, max_age_seconds: Optional[int] = None) -> Optional[Any]:
        """
        Get cached data if available and not stale.

        Args:
            key: Cache key
            max_age_seconds: Max age in seconds (None = use default TTL)

        Returns:
            Cached data or None if not available/stale
        """
        if max_age_seconds is None:
            max_age_seconds = self.DEFAULT_TTL_SECONDS

        with self._lock:
            entry = self._cache.get(key)
            if entry and not entry.is_stale(max_age_seconds):
                return entry.data
        return None

    def get_with_age(self, key: str) -> tuple:
        """
        Get cached data along with its age, regardless of staleness.

        Returns:
            (data, updated_at) or (None, None) if not cached
        """
        with self._lock:
            entry = self._cache.get(key)
            if entry:
                return entry.data, entry.updated_at
        return None, None

    def set(self, key: str, data: Any, save_to_disk: bool = True):
        """
        Store data in cache.

        Args:
            key: Cache key
            data: Data to cache
            save_to_disk: Whether to persist to disk
        """
        with self._lock:
            self._cache[key] = CacheEntry(data=data, updated_at=datetime.now())

        if save_to_disk:
            self._save_to_disk()

    def invalidate(self, key: str):
        """Remove a specific cache entry"""
        with self._lock:
            if key in self._cache:
                del self._cache[key]

    def invalidate_all(self):
        """Clear all cache entries"""
        with self._lock:
            self._cache.clear()

    def get_last_updated(self, key: str) -> Optional[datetime]:
        """Get the timestamp when a cache entry was last updated"""
        with self._lock:
            entry = self._cache.get(key)
            return entry.updated_at if entry else None

    def register_refresh_callback(self, key: str, callback: Callable):
        """
        Register a callback function to refresh a specific cache key.

        Args:
            key: Cache key
            callback: Function that returns the fresh data
        """
        self._refresh_callbacks[key] = callback

    def refresh(self, key: str) -> Any:
        """
        Force refresh a specific cache key using its registered callback.

        Returns:
            Fresh data or None if no callback registered
        """
        callback = self._refresh_callbacks.get(key)
        if callback:
            try:
                data = callback()
                self.set(key, data)
                return data
            except Exception as e:
                print(f"[WebCache] Error refreshing '{key}': {e}")
        return None

    def refresh_all(self):
        """Refresh all registered cache keys"""
        for key in self._refresh_callbacks:
            self.refresh(key)

    def start_background_refresh(self, interval_seconds: Optional[int] = None):
        """
        Start background thread that periodically refreshes all cached data.

        Args:
            interval_seconds: Refresh interval (None = use default)
        """
        if self._refresh_thread and self._refresh_thread.is_alive():
            return  # Already running

        if interval_seconds is None:
            interval_seconds = self.REFRESH_INTERVAL_SECONDS

        self._stop_refresh.clear()

        def refresh_loop():
            while not self._stop_refresh.is_set():
                # Wait for interval or stop signal
                if self._stop_refresh.wait(timeout=interval_seconds):
                    break  # Stop signal received

                # Refresh all registered callbacks
                print(f"[WebCache] Background refresh triggered at {datetime.now().strftime('%H:%M:%S')}")
                self.refresh_all()

        self._refresh_thread = threading.Thread(target=refresh_loop, daemon=True, name="WebCacheRefresh")
        self._refresh_thread.start()
        print(f"[WebCache] Background refresh started (interval: {interval_seconds}s)")

    def stop_background_refresh(self):
        """Stop the background refresh thread"""
        self._stop_refresh.set()
        if self._refresh_thread:
            self._refresh_thread.join(timeout=5)

    def _load_from_disk(self):
        """Load cached data from disk"""
        if not self._disk_cache_file.exists():
            return

        try:
            with open(self._disk_cache_file, 'r', encoding='utf-8') as f:
                disk_data = json.load(f)

            with self._lock:
                for key, entry_data in disk_data.items():
                    # Parse the stored datetime
                    updated_at = datetime.fromisoformat(entry_data['updated_at'])
                    self._cache[key] = CacheEntry(
                        data=entry_data['data'],
                        updated_at=updated_at
                    )
            print(f"[WebCache] Loaded {len(disk_data)} entries from disk cache")
        except (json.JSONDecodeError, IOError, KeyError) as e:
            print(f"[WebCache] Warning: Could not load disk cache: {e}")

    def _save_to_disk(self):
        """Save current cache to disk"""
        try:
            # Ensure data directory exists
            self._disk_cache_file.parent.mkdir(parents=True, exist_ok=True)

            with self._lock:
                disk_data = {}
                for key, entry in self._cache.items():
                    # Only save serializable data
                    try:
                        disk_data[key] = {
                            'data': entry.data,
                            'updated_at': entry.updated_at.isoformat()
                        }
                    except (TypeError, ValueError):
                        # Skip non-serializable entries
                        pass

            with open(self._disk_cache_file, 'w', encoding='utf-8') as f:
                json.dump(disk_data, f, indent=2, default=str)

        except IOError as e:
            print(f"[WebCache] Warning: Could not save disk cache: {e}")


# Cache keys
CACHE_KEY_DASHBOARD_STATS = "dashboard_stats"
CACHE_KEY_MAINTENANCE_AUDIT = "maintenance_audit"
CACHE_KEY_MAINTENANCE_HEALTH = "maintenance_health"

# Singleton instance
_web_cache_service: Optional[WebCacheService] = None


def get_web_cache_service() -> WebCacheService:
    """Get or create the web cache service singleton"""
    global _web_cache_service
    if _web_cache_service is None:
        _web_cache_service = WebCacheService()
    return _web_cache_service


def init_web_cache():
    """
    Initialize the web cache service and start background refresh.
    Call this on application startup.
    """
    service = get_web_cache_service()

    # Import here to avoid circular imports
    from web.services.maintenance_service import get_maintenance_service
    from web.services.cache_service import get_cache_service
    from web.services import get_settings_service, get_operation_runner, get_scheduler_service

    # Register refresh callbacks
    maintenance_svc = get_maintenance_service()

    def refresh_dashboard_stats():
        """Refresh dashboard stats - mirrors _get_dashboard_stats in dashboard.py"""
        from web.services.operation_runner import load_last_run_summary, OperationRunner as _OR

        cache_svc = get_cache_service()
        settings_svc = get_settings_service()
        operation_runner = get_operation_runner()
        scheduler_svc = get_scheduler_service()

        cache_stats = cache_svc.get_cache_stats()
        plex_connected = settings_svc.check_plex_connection()
        last_run = settings_svc.get_last_run_time() or "Never"
        op_status = operation_runner.get_status_dict()
        schedule_status = scheduler_svc.get_status()
        health = maintenance_svc.get_health_summary()

        stats = {
            "cache_files": cache_stats["cache_files"],
            "cache_size": cache_stats["cache_size"],
            "cache_limit": cache_stats["cache_limit"],
            "usage_percent": cache_stats["usage_percent"],
            "ondeck_count": cache_stats["ondeck_count"],
            "watchlist_count": cache_stats["watchlist_count"],
            "last_run": last_run,
            "is_running": operation_runner.is_running,
            "plex_connected": plex_connected,
            "schedule_enabled": schedule_status.get("enabled", False),
            "next_run": schedule_status.get("next_run_display", "Not scheduled"),
            "next_run_relative": schedule_status.get("next_run_relative"),
            "health_status": health["status"],
            "health_issues": health["orphaned_count"],
            "health_warnings": health["stale_exclude_count"] + health["stale_timestamp_count"],
            "health_orphaned_count": health["orphaned_count"],
            "health_stale_exclude_count": health["stale_exclude_count"],
            "health_stale_timestamp_count": health["stale_timestamp_count"],
            "last_run_summary": None,
        }

        summary = load_last_run_summary()
        if summary:
            total_bytes = summary.get("bytes_cached", 0) + summary.get("bytes_restored", 0)
            stats["last_run_summary"] = {
                "status": summary.get("status", "unknown"),
                "total_bytes_display": _OR._format_bytes(total_bytes) if total_bytes else "",
                "duration_display": _OR._format_duration(summary.get("duration_seconds", 0)),
                "error_count": summary.get("error_count", 0),
                "dry_run": summary.get("dry_run", False),
            }

        return stats

    def refresh_maintenance_audit():
        """Refresh maintenance audit - convert to dict for serialization"""
        results = maintenance_svc.run_full_audit()
        return _audit_results_to_dict(results)

    def refresh_maintenance_health():
        """Refresh maintenance health summary"""
        return maintenance_svc.get_health_summary()

    service.register_refresh_callback(CACHE_KEY_DASHBOARD_STATS, refresh_dashboard_stats)
    service.register_refresh_callback(CACHE_KEY_MAINTENANCE_AUDIT, refresh_maintenance_audit)
    service.register_refresh_callback(CACHE_KEY_MAINTENANCE_HEALTH, refresh_maintenance_health)

    # Do initial refresh in background if cache is empty or very stale (> 1 hour)
    def initial_refresh():
        for key in [CACHE_KEY_DASHBOARD_STATS, CACHE_KEY_MAINTENANCE_HEALTH]:
            data, updated_at = service.get_with_age(key)
            if data is None or (updated_at and (datetime.now() - updated_at).total_seconds() > 3600):
                print(f"[WebCache] Initial refresh: {key}")
                service.refresh(key)

    # Run initial refresh in background thread
    threading.Thread(target=initial_refresh, daemon=True, name="WebCacheInit").start()

    # Start background refresh (every 5 minutes)
    service.start_background_refresh()

    return service


def _audit_results_to_dict(results) -> dict:
    """Convert AuditResults to a JSON-serializable dict"""
    return {
        'cache_file_count': results.cache_file_count,
        'exclude_entry_count': results.exclude_entry_count,
        'timestamp_entry_count': results.timestamp_entry_count,
        'health_status': results.health_status,
        'unprotected_count': len(results.unprotected_files),
        'orphaned_count': len(results.orphaned_plexcached),
        'stale_exclude_count': len(results.stale_exclude_entries),
        'stale_timestamp_count': len(results.stale_timestamp_entries),
        'duplicates_count': len(results.duplicates),
    }
