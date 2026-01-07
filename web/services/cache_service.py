"""Cache service - reads cached file data and calculates priorities"""

import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass

from web.config import PROJECT_ROOT


@dataclass
class CachedFile:
    """Represents a cached file with all its metadata"""
    path: str
    filename: str
    size: int
    size_display: str
    cached_at: datetime
    cache_age_hours: float
    source: str  # ondeck, watchlist, pre-existing, unknown
    priority_score: int
    users: List[str]
    is_ondeck: bool
    is_watchlist: bool
    episode_info: Optional[Dict[str, Any]] = None


class CacheService:
    """Service for reading cache data and calculating priorities"""

    def __init__(self):
        self.exclude_file = PROJECT_ROOT / "plexcache_mover_files_to_exclude.txt"
        self.timestamps_file = PROJECT_ROOT / "data" / "timestamps.json"
        self.ondeck_file = PROJECT_ROOT / "data" / "ondeck_tracker.json"
        self.watchlist_file = PROJECT_ROOT / "data" / "watchlist_tracker.json"
        self.settings_file = PROJECT_ROOT / "plexcache_settings.json"

    def _load_json_file(self, path: Path) -> Dict:
        """Load a JSON file, returning empty dict if not found"""
        if not path.exists():
            return {}
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}

    def _load_settings(self) -> Dict:
        """Load settings file"""
        return self._load_json_file(self.settings_file)

    def _format_size(self, size_bytes: int) -> str:
        """Format bytes into human-readable string"""
        if size_bytes == 0:
            return "0 B"

        units = ['B', 'KB', 'MB', 'GB', 'TB']
        unit_index = 0
        size = float(size_bytes)

        while size >= 1024 and unit_index < len(units) - 1:
            size /= 1024
            unit_index += 1

        if unit_index == 0:
            return f"{int(size)} B"
        return f"{size:.1f} {units[unit_index]}"

    def _parse_cache_limit(self, limit_str: str) -> int:
        """Parse cache limit string to bytes (e.g., '250GB' -> bytes)"""
        if not limit_str:
            return 250 * 1024**3  # Default 250GB

        limit_str = limit_str.strip().upper()

        # Handle percentage - return 0 to indicate we need disk check
        if '%' in limit_str:
            return 0

        # Parse size string
        multipliers = {
            'B': 1,
            'KB': 1024,
            'MB': 1024**2,
            'GB': 1024**3,
            'TB': 1024**4
        }

        for unit, mult in multipliers.items():
            if limit_str.endswith(unit):
                try:
                    value = float(limit_str[:-len(unit)].strip())
                    return int(value * mult)
                except ValueError:
                    pass

        # Default fallback
        return 250 * 1024**3

    def get_cached_files_list(self) -> List[str]:
        """Get list of cached file paths from exclude file"""
        if not self.exclude_file.exists():
            return []

        try:
            with open(self.exclude_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            return [line.strip() for line in lines if line.strip()]
        except IOError:
            return []

    def get_timestamps(self) -> Dict[str, Dict]:
        """Load timestamps data"""
        data = self._load_json_file(self.timestamps_file)
        # Handle old format (plain timestamps) vs new format (dict with cached_at, source)
        normalized = {}
        for path, value in data.items():
            if isinstance(value, dict):
                normalized[path] = value
            else:
                # Old format - just a timestamp string
                normalized[path] = {
                    "cached_at": value,
                    "source": "unknown"
                }
        return normalized

    def get_ondeck_tracker(self) -> Dict:
        """Load OnDeck tracker data"""
        return self._load_json_file(self.ondeck_file)

    def get_watchlist_tracker(self) -> Dict:
        """Load Watchlist tracker data"""
        return self._load_json_file(self.watchlist_file)

    def calculate_priority(
        self,
        cache_path: str,
        timestamps: Dict,
        ondeck: Dict,
        watchlist: Dict,
        settings: Dict
    ) -> int:
        """
        Calculate priority score (0-100) for a cached file.

        Higher score = keep longer, lower score = evict first.

        Factors:
        - Base: 50
        - Source type: +20 for ondeck, +0 for watchlist
        - User count: +5 per user (max +15)
        - Cache recency: +15 (<24h), +10 (<72h), +5 (<7d), 0 (older)
        - Watchlist/OnDeck age: +10 (<7d), 0 (7-60d), -10 (>60d)
        - Episode position: +15 (current), +10 (next few), 0 (far ahead)
        """
        score = 50
        now = datetime.now()

        # Get timestamp info
        ts_info = timestamps.get(cache_path, {})
        cached_at_str = ts_info.get("cached_at") if isinstance(ts_info, dict) else ts_info
        source = ts_info.get("source", "unknown") if isinstance(ts_info, dict) else "unknown"

        # Try to find in ondeck/watchlist trackers (they use plex paths)
        # We need to check if any tracker path ends with similar structure
        ondeck_info = None
        watchlist_info = None

        # Simple path matching - check if any tracked file matches
        cache_basename = os.path.basename(cache_path)
        for plex_path, info in ondeck.items():
            if os.path.basename(plex_path) == cache_basename:
                ondeck_info = info
                source = "ondeck"
                break

        for plex_path, info in watchlist.items():
            if os.path.basename(plex_path) == cache_basename:
                watchlist_info = info
                if not ondeck_info:
                    source = "watchlist"
                break

        # Factor 1: Source type
        if source == "ondeck":
            score += 20

        # Factor 2: User count
        users = set()
        if ondeck_info and "users" in ondeck_info:
            users.update(ondeck_info["users"])
        if watchlist_info and "users" in watchlist_info:
            users.update(watchlist_info["users"])

        user_bonus = min(len(users) * 5, 15)
        score += user_bonus

        # Factor 3: Cache recency
        if cached_at_str:
            try:
                cached_at = datetime.fromisoformat(cached_at_str)
                hours_cached = (now - cached_at).total_seconds() / 3600

                if hours_cached < 24:
                    score += 15
                elif hours_cached < 72:
                    score += 10
                elif hours_cached < 168:  # 7 days
                    score += 5
            except (ValueError, TypeError):
                pass

        # Factor 4: Watchlist/OnDeck age
        if watchlist_info and "watchlisted_at" in watchlist_info:
            try:
                watchlisted_at = datetime.fromisoformat(watchlist_info["watchlisted_at"])
                days_on_watchlist = (now - watchlisted_at).days

                if days_on_watchlist < 7:
                    score += 10
                elif days_on_watchlist > 60:
                    score -= 10
            except (ValueError, TypeError):
                pass

        if ondeck_info and "last_seen" in ondeck_info:
            try:
                last_seen = datetime.fromisoformat(ondeck_info["last_seen"])
                days_since_seen = (now - last_seen).days

                if days_since_seen < 7:
                    score += 10
                elif days_since_seen > 60:
                    score -= 10
            except (ValueError, TypeError):
                pass

        # Factor 5: Episode position (for TV)
        if ondeck_info and "episode_info" in ondeck_info:
            ep_info = ondeck_info["episode_info"]
            if ep_info.get("is_current_ondeck"):
                score += 15
            else:
                # Check episodes ahead
                number_episodes = settings.get("number_episodes", 5)
                half_prefetch = number_episodes // 2
                # If it's a prefetched episode, give partial bonus
                if ep_info.get("episode"):
                    score += 10

        return max(0, min(100, score))

    def get_all_cached_files(
        self,
        source_filter: str = "all",
        search: str = "",
        sort_by: str = "priority",
        sort_dir: str = "desc"
    ) -> List[CachedFile]:
        """
        Get all cached files with their metadata and priority scores.

        Args:
            source_filter: "all", "ondeck", or "watchlist"
            search: Search string to filter filenames
            sort_by: Column to sort by ("filename", "size", "priority", "age", "users")
            sort_dir: Sort direction ("asc" or "desc")

        Returns:
            List of CachedFile objects sorted by specified column
        """
        cached_paths = self.get_cached_files_list()
        timestamps = self.get_timestamps()
        ondeck = self.get_ondeck_tracker()
        watchlist = self.get_watchlist_tracker()
        settings = self._load_settings()

        now = datetime.now()
        files = []

        for cache_path in cached_paths:
            filename = os.path.basename(cache_path)

            # Apply search filter
            if search and search.lower() not in filename.lower():
                continue

            # Get file size
            try:
                size = os.path.getsize(cache_path) if os.path.exists(cache_path) else 0
            except OSError:
                size = 0

            # Get timestamp info
            ts_info = timestamps.get(cache_path, {})
            if isinstance(ts_info, dict):
                cached_at_str = ts_info.get("cached_at")
                source = ts_info.get("source", "unknown")
            else:
                cached_at_str = ts_info
                source = "unknown"

            # Parse cached_at
            try:
                cached_at = datetime.fromisoformat(cached_at_str) if cached_at_str else now
            except (ValueError, TypeError):
                cached_at = now

            cache_age_hours = (now - cached_at).total_seconds() / 3600

            # Check ondeck/watchlist trackers
            is_ondeck = False
            is_watchlist = False
            users = set()
            episode_info = None
            cache_basename = os.path.basename(cache_path)

            for plex_path, info in ondeck.items():
                if os.path.basename(plex_path) == cache_basename:
                    is_ondeck = True
                    source = "ondeck"
                    if "users" in info:
                        users.update(info["users"])
                    episode_info = info.get("episode_info")
                    break

            for plex_path, info in watchlist.items():
                if os.path.basename(plex_path) == cache_basename:
                    is_watchlist = True
                    if not is_ondeck:
                        source = "watchlist"
                    if "users" in info:
                        users.update(info["users"])
                    break

            # Apply source filter
            if source_filter == "ondeck" and not is_ondeck:
                continue
            if source_filter == "watchlist" and not is_watchlist:
                continue

            # Calculate priority
            priority = self.calculate_priority(
                cache_path, timestamps, ondeck, watchlist, settings
            )

            files.append(CachedFile(
                path=cache_path,
                filename=filename,
                size=size,
                size_display=self._format_size(size),
                cached_at=cached_at,
                cache_age_hours=cache_age_hours,
                source=source,
                priority_score=priority,
                users=list(users),
                is_ondeck=is_ondeck,
                is_watchlist=is_watchlist,
                episode_info=episode_info
            ))

        # Sort by specified column
        reverse = (sort_dir == "desc")

        sort_keys = {
            "filename": lambda f: f.filename.lower(),
            "size": lambda f: f.size,
            "priority": lambda f: f.priority_score,
            "age": lambda f: f.cache_age_hours,
            "users": lambda f: len(f.users),
            "source": lambda f: (f.is_ondeck, f.is_watchlist),  # OnDeck first, then Watchlist
        }

        sort_key = sort_keys.get(sort_by, sort_keys["priority"])
        files.sort(key=sort_key, reverse=reverse)

        return files

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics for dashboard"""
        import shutil

        cached_paths = self.get_cached_files_list()
        ondeck = self.get_ondeck_tracker()
        watchlist = self.get_watchlist_tracker()
        settings = self._load_settings()

        # Calculate total size of cached files (PlexCache-managed files)
        cached_files_size = 0
        for cache_path in cached_paths:
            try:
                if os.path.exists(cache_path):
                    cached_files_size += os.path.getsize(cache_path)
            except OSError:
                pass

        # Get actual cache drive usage
        cache_dir = settings.get("cache_dir", "")
        disk_used = 0
        disk_total = 0
        usage_percent = 0

        if cache_dir and os.path.exists(cache_dir):
            try:
                disk = shutil.disk_usage(cache_dir)
                disk_used = disk.used
                disk_total = disk.total
                usage_percent = int((disk.used / disk.total) * 100)
            except (OSError, AttributeError):
                pass

        # Count ondeck and watchlist items
        ondeck_count = len(ondeck)
        watchlist_count = len(watchlist)

        return {
            "cache_files": len(cached_paths),
            "cache_size": self._format_size(disk_used),  # Actual disk used
            "cache_size_bytes": disk_used,
            "cache_limit": self._format_size(disk_total),  # Actual disk total
            "cache_limit_bytes": disk_total,
            "usage_percent": usage_percent,
            "cached_files_size": self._format_size(cached_files_size),  # PlexCache files only
            "cached_files_size_bytes": cached_files_size,
            "ondeck_count": ondeck_count,
            "watchlist_count": watchlist_count
        }

    def get_drive_details(self) -> Dict[str, Any]:
        """Get comprehensive cache drive details for the drive info page"""
        import shutil

        cached_paths = self.get_cached_files_list()
        timestamps = self.get_timestamps()
        ondeck = self.get_ondeck_tracker()
        watchlist = self.get_watchlist_tracker()
        settings = self._load_settings()

        now = datetime.now()

        # Get all cached files with metadata
        all_files = self.get_all_cached_files()

        # Storage Overview
        cache_dir = settings.get("cache_dir", "")
        disk_used = 0
        disk_total = 0
        disk_free = 0

        if cache_dir and os.path.exists(cache_dir):
            try:
                disk = shutil.disk_usage(cache_dir)
                disk_used = disk.used
                disk_total = disk.total
                disk_free = disk.free
            except (OSError, AttributeError):
                pass

        # Calculate sizes by source
        ondeck_size = sum(f.size for f in all_files if f.is_ondeck)
        watchlist_size = sum(f.size for f in all_files if f.is_watchlist and not f.is_ondeck)
        other_size = sum(f.size for f in all_files if not f.is_ondeck and not f.is_watchlist)
        total_cached_size = sum(f.size for f in all_files)

        ondeck_count = sum(1 for f in all_files if f.is_ondeck)
        watchlist_count = sum(1 for f in all_files if f.is_watchlist and not f.is_ondeck)
        other_count = sum(1 for f in all_files if not f.is_ondeck and not f.is_watchlist)

        # Calculate percentages of cache
        def calc_percent(size, total):
            return round((size / total * 100), 1) if total > 0 else 0

        # Largest files (top 10)
        largest_files = sorted(all_files, key=lambda f: f.size, reverse=True)[:10]

        # Oldest cached files (top 10)
        oldest_files = sorted(all_files, key=lambda f: f.cached_at)[:10]

        # Files nearing watchlist expiration
        watchlist_retention_days = settings.get("watchlist_retention_days", 14)
        expiring_soon = []
        for f in all_files:
            if f.is_watchlist:
                # Find watchlist entry to get watchlisted_at date
                for plex_path, info in watchlist.items():
                    if os.path.basename(plex_path) == f.filename:
                        if "watchlisted_at" in info:
                            try:
                                watchlisted_at = datetime.fromisoformat(info["watchlisted_at"])
                                days_remaining = watchlist_retention_days - (now - watchlisted_at).days
                                if days_remaining <= 3 and days_remaining > 0:
                                    expiring_soon.append({
                                        "file": f,
                                        "days_remaining": days_remaining
                                    })
                            except (ValueError, TypeError):
                                pass
                        break
        expiring_soon.sort(key=lambda x: x["days_remaining"])

        # Recent activity (last 24h and 7d counts)
        files_last_24h = sum(1 for f in all_files if f.cache_age_hours <= 24)
        files_last_7d = sum(1 for f in all_files if f.cache_age_hours <= 168)

        # Recently cached files (last 24h)
        recently_cached = [f for f in all_files if f.cache_age_hours <= 24]
        recently_cached.sort(key=lambda f: f.cached_at, reverse=True)

        # Configuration
        config = {
            "cache_dir": cache_dir,
            "cache_limit": settings.get("cache_limit", "N/A"),
            "cache_retention_hours": settings.get("cache_retention_hours", 72),
            "watchlist_retention_days": watchlist_retention_days,
            "number_episodes": settings.get("number_episodes", 5)
        }

        return {
            # Storage Overview
            "storage": {
                "total": disk_total,
                "total_display": self._format_size(disk_total),
                "used": disk_used,
                "used_display": self._format_size(disk_used),
                "free": disk_free,
                "free_display": self._format_size(disk_free),
                "usage_percent": calc_percent(disk_used, disk_total),
                "cached_size": total_cached_size,
                "cached_size_display": self._format_size(total_cached_size),
                "cached_percent": calc_percent(total_cached_size, disk_total),
                "file_count": len(all_files)
            },
            # Breakdown by source
            "breakdown": {
                "ondeck": {
                    "count": ondeck_count,
                    "size": ondeck_size,
                    "size_display": self._format_size(ondeck_size),
                    "percent": calc_percent(ondeck_size, total_cached_size) if total_cached_size > 0 else 0
                },
                "watchlist": {
                    "count": watchlist_count,
                    "size": watchlist_size,
                    "size_display": self._format_size(watchlist_size),
                    "percent": calc_percent(watchlist_size, total_cached_size) if total_cached_size > 0 else 0
                },
                "other": {
                    "count": other_count,
                    "size": other_size,
                    "size_display": self._format_size(other_size),
                    "percent": calc_percent(other_size, total_cached_size) if total_cached_size > 0 else 0
                }
            },
            # File analysis
            "largest_files": largest_files[:10],
            "oldest_files": oldest_files[:10],
            "expiring_soon": expiring_soon[:5],
            # Activity
            "activity": {
                "files_last_24h": files_last_24h,
                "files_last_7d": files_last_7d,
                "recently_cached": recently_cached[:5]
            },
            # Configuration
            "config": config
        }

    def get_priority_report(self) -> str:
        """Generate a human-readable priority report"""
        files = self.get_all_cached_files()

        if not files:
            return "No cached files to analyze."

        lines = []
        lines.append("=" * 70)
        lines.append("CACHE PRIORITY REPORT")
        lines.append("=" * 70)
        lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"Total files: {len(files)}")
        lines.append("")

        # Summary by priority tier
        high = [f for f in files if f.priority_score >= 70]
        medium = [f for f in files if 40 <= f.priority_score < 70]
        low = [f for f in files if f.priority_score < 40]

        lines.append("SUMMARY BY TIER:")
        lines.append(f"  High priority (70-100):   {len(high)} files")
        lines.append(f"  Medium priority (40-69):  {len(medium)} files")
        lines.append(f"  Low priority (0-39):      {len(low)} files (eviction candidates)")
        lines.append("")

        # Summary by source
        ondeck_files = [f for f in files if f.is_ondeck]
        watchlist_files = [f for f in files if f.is_watchlist and not f.is_ondeck]
        other_files = [f for f in files if not f.is_ondeck and not f.is_watchlist]

        lines.append("SUMMARY BY SOURCE:")
        lines.append(f"  OnDeck:     {len(ondeck_files)} files")
        lines.append(f"  Watchlist:  {len(watchlist_files)} files")
        lines.append(f"  Other:      {len(other_files)} files")
        lines.append("")

        lines.append("-" * 70)
        lines.append("DETAILED FILE LIST (sorted by priority, descending)")
        lines.append("-" * 70)
        lines.append("")

        for f in files:
            priority_tier = "HIGH" if f.priority_score >= 70 else "MED" if f.priority_score >= 40 else "LOW"
            user_str = f", users: {', '.join(f.users)}" if f.users else ""

            lines.append(f"[{priority_tier}] Score: {f.priority_score}")
            lines.append(f"  File: {f.filename}")
            lines.append(f"  Size: {f.size_display}, Source: {f.source}, Age: {f.cache_age_hours:.1f}h{user_str}")
            lines.append("")

        return "\n".join(lines)


    def evict_file(self, cache_path: str) -> Dict[str, Any]:
        """
        Evict a file from cache - restore .plexcached backup and remove from tracking.

        Returns dict with success status and message.
        """
        import shutil

        result = {"success": False, "message": ""}

        # Normalize path
        cache_path = cache_path.strip()

        if not cache_path:
            result["message"] = "No file path provided"
            return result

        # Check if file is in exclude list
        cached_files = self.get_cached_files_list()
        if cache_path not in cached_files:
            result["message"] = "File not found in cache list"
            return result

        settings = self._load_settings()

        # Find the array path (.plexcached backup)
        # Need to convert cache path back to array path
        path_mappings = settings.get("path_mappings", [])
        array_path = None

        for mapping in path_mappings:
            if not mapping.get("enabled", True):
                continue
            cache_prefix = mapping.get("cache_path", "")
            real_prefix = mapping.get("real_path", "")

            if cache_prefix and cache_path.startswith(cache_prefix):
                # Convert cache path to array path
                relative_path = cache_path[len(cache_prefix):]
                array_path = real_prefix.rstrip("/\\") + "/" + relative_path.lstrip("/\\")
                break

        if not array_path:
            # Fallback: try using single source/dest from settings
            cache_dir = settings.get("cache_dir", "")
            real_source = settings.get("real_source", "")
            if cache_dir and real_source and cache_path.startswith(cache_dir):
                relative_path = cache_path[len(cache_dir):]
                array_path = real_source.rstrip("/\\") + "/" + relative_path.lstrip("/\\")

        plexcached_path = f"{array_path}.plexcached" if array_path else None

        try:
            # Step 1: Restore .plexcached backup if it exists
            if plexcached_path and os.path.exists(plexcached_path):
                # Rename .plexcached back to original
                os.rename(plexcached_path, array_path)

            # Step 2: Delete cache copy if it exists
            if os.path.exists(cache_path):
                os.remove(cache_path)

            # Step 3: Remove from exclude file
            self._remove_from_exclude_file(cache_path)

            # Step 4: Remove from timestamps
            self._remove_from_timestamps(cache_path)

            result["success"] = True
            result["message"] = f"Evicted: {os.path.basename(cache_path)}"

        except PermissionError as e:
            result["message"] = f"Permission denied: {str(e)}"
        except OSError as e:
            result["message"] = f"Error evicting file: {str(e)}"

        return result

    def evict_files(self, cache_paths: List[str]) -> Dict[str, Any]:
        """
        Evict multiple files from cache.

        Returns dict with success count and any errors.
        """
        success_count = 0
        errors = []

        for path in cache_paths:
            result = self.evict_file(path)
            if result["success"]:
                success_count += 1
            else:
                errors.append(f"{os.path.basename(path)}: {result['message']}")

        return {
            "success": success_count > 0,
            "evicted_count": success_count,
            "total_count": len(cache_paths),
            "errors": errors
        }

    def _remove_from_exclude_file(self, cache_path: str):
        """Remove a path from the exclude file"""
        if not self.exclude_file.exists():
            return

        try:
            with open(self.exclude_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()

            # Filter out the path
            new_lines = [line for line in lines if line.strip() != cache_path]

            with open(self.exclude_file, 'w', encoding='utf-8') as f:
                f.writelines(new_lines)
        except IOError:
            pass

    def _remove_from_timestamps(self, cache_path: str):
        """Remove a path from the timestamps file"""
        if not self.timestamps_file.exists():
            return

        try:
            with open(self.timestamps_file, 'r', encoding='utf-8') as f:
                timestamps = json.load(f)

            if cache_path in timestamps:
                del timestamps[cache_path]

                with open(self.timestamps_file, 'w', encoding='utf-8') as f:
                    json.dump(timestamps, f, indent=2)
        except (IOError, json.JSONDecodeError):
            pass


# Singleton instance
_cache_service: Optional[CacheService] = None


def get_cache_service() -> CacheService:
    """Get or create the cache service singleton"""
    global _cache_service
    if _cache_service is None:
        _cache_service = CacheService()
    return _cache_service
