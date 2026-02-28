"""Cache service - reads cached file data and calculates priorities"""

import json
import logging
import os
import re
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass

from web.config import PROJECT_ROOT, DATA_DIR, CONFIG_DIR, SETTINGS_FILE
from core.system_utils import get_disk_usage, detect_zfs, get_array_direct_path, parse_size_bytes, format_bytes, translate_container_to_host_path, translate_host_to_container_path, remove_from_exclude_file, remove_from_timestamps_file
from core.file_operations import get_media_identity, find_matching_plexcached, save_json_atomically


# Subtitle file extensions (case-insensitive)
SUBTITLE_EXTENSIONS = {'.srt', '.ass', '.sub', '.idx', '.vtt', '.ssa', '.sup', '.smi'}


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
    subtitle_count: int = 0  # Number of associated subtitle files
    subtitle_paths: Optional[List[str]] = None  # Paths to associated subtitles


class CacheService:
    """Service for reading cache data and calculating priorities"""

    def __init__(self):
        # Use CONFIG_DIR for Docker compatibility (/config in Docker, project root otherwise)
        self.exclude_file = CONFIG_DIR / "plexcache_cached_files.txt"
        self.timestamps_file = DATA_DIR / "timestamps.json"
        self.ondeck_file = DATA_DIR / "ondeck_tracker.json"
        self.watchlist_file = DATA_DIR / "watchlist_tracker.json"
        self.settings_file = SETTINGS_FILE

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

    def _get_cache_dir(self, settings: Dict = None) -> str:
        """Get the cache directory, preferring path_mappings over cache_dir setting.

        This ensures consistency between what the UI shows and what the script uses.
        The script uses path_mappings[].cache_path for actual file operations, so
        disk usage stats should use the same path.
        """
        if settings is None:
            settings = self._load_settings()

        # First check path_mappings for an enabled, cacheable mapping
        path_mappings = settings.get("path_mappings", [])
        for mapping in path_mappings:
            if mapping.get("enabled", True) and mapping.get("cacheable") and mapping.get("cache_path"):
                return mapping.get("cache_path")

        # Fall back to cache_dir setting
        return settings.get("cache_dir", "")

    def _translate_container_to_host_path(self, path: str) -> str:
        """Translate container cache path to host path for exclude file."""
        settings = self._load_settings()
        return translate_container_to_host_path(path, settings.get('path_mappings', []))

    def _translate_host_to_container_path(self, path: str) -> str:
        """Translate host cache path to container path."""
        settings = self._load_settings()
        return translate_host_to_container_path(path, settings.get('path_mappings', []))

    def _get_cache_dir_for_display(self, settings: Dict = None) -> str:
        """Get the cache directory for UI display, preferring host paths.

        For Docker users, this returns host_cache_path so the UI shows the
        actual Unraid path rather than the container-internal path.
        Also attempts to find a common parent if multiple mappings exist.
        """
        if settings is None:
            settings = self._load_settings()

        path_mappings = settings.get("path_mappings", [])
        display_paths = []

        for mapping in path_mappings:
            if mapping.get("enabled", True) and mapping.get("cacheable"):
                # Prefer host_cache_path for display, fall back to cache_path
                host_path = mapping.get("host_cache_path", "")
                cache_path = mapping.get("cache_path", "")
                display_path = host_path if host_path else cache_path
                if display_path:
                    display_paths.append(display_path.rstrip('/'))

        if not display_paths:
            # Fall back to cache_dir setting
            return settings.get("cache_dir", "")

        if len(display_paths) == 1:
            return display_paths[0]

        # Find common parent path across all mappings
        common = os.path.commonpath(display_paths)
        if common and common != '/':
            return common

        # If no common parent (shouldn't happen), return first path
        return display_paths[0]

    def _is_subtitle_file(self, filename: str) -> bool:
        """Check if a filename is a subtitle file based on extension."""
        # Handle language suffixes like .en.srt, .es.srt
        lower_name = filename.lower()
        for ext in SUBTITLE_EXTENSIONS:
            if lower_name.endswith(ext):
                return True
        return False

    def _get_video_base_name(self, subtitle_path: str) -> str:
        """Get the base video filename from a subtitle path.

        Handles language codes in subtitle names, e.g.:
        - Movie.en.srt -> Movie
        - Movie.srt -> Movie
        - Show.S01E01.es.ass -> Show.S01E01
        """
        filename = os.path.basename(subtitle_path)
        lower_name = filename.lower()

        # Find and remove subtitle extension
        for ext in SUBTITLE_EXTENSIONS:
            if lower_name.endswith(ext):
                filename = filename[:-len(ext)]
                lower_name = lower_name[:-len(ext)]
                break

        # Remove language code suffix if present (e.g., .en, .es, .hi, .pt-br)
        # Match .lang or .lang-region at the end (e.g., .en, .es, .pt-br, .zh-hans)
        lang_pattern = r'\.[a-z]{2,3}(-[a-z]{2,4})?$'
        match = re.search(lang_pattern, lower_name, re.IGNORECASE)
        if match:
            filename = filename[:match.start()]

        return filename

    def get_cached_files_list(self) -> List[str]:
        """Get list of cached file paths from timestamps.json (primary) or exclude file (fallback)"""
        # Primary: Use timestamps.json as the source of truth for cached files
        timestamps = self.get_timestamps()
        if timestamps:
            return list(timestamps.keys())

        # Fallback: Use exclude file for backwards compatibility
        # Note: Exclude file contains HOST paths (for Unraid mover), need to translate to container paths
        if not self.exclude_file.exists():
            return []

        try:
            with open(self.exclude_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            # Translate host paths to container paths for file operations inside Docker
            paths = [line.strip() for line in lines if line.strip()]
            return [self._translate_host_to_container_path(p) for p in paths]
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

        # Factor 1: Source type (+15 for ondeck, +0 for watchlist)
        if source == "ondeck":
            score += 15

        # Factor 2: User count (+5 per user, max +15)
        users = set()
        if ondeck_info and "users" in ondeck_info:
            users.update(ondeck_info["users"])
        if watchlist_info and "users" in watchlist_info:
            users.update(watchlist_info["users"])

        user_bonus = min(len(users) * 5, 15)
        score += user_bonus

        # Factor 3: Cache recency (+5 if <24h, +3 if <72h, 0 otherwise)
        if cached_at_str:
            try:
                cached_at = datetime.fromisoformat(cached_at_str)
                hours_cached = (now - cached_at).total_seconds() / 3600

                if hours_cached < 24:
                    score += 5
                elif hours_cached < 72:
                    score += 3
                # >72h: no adjustment (0)
            except (ValueError, TypeError):
                pass

        # Factor 4: Watchlist age (+10 if <7 days, -10 if >60 days)
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

        # Factor 5: OnDeck staleness (uses first_seen, not last_seen)
        # +5 if <7 days, 0 if 7-14 days, -5 if 14-30 days, -10 if >30 days
        if source == "ondeck" and ondeck_info and "first_seen" in ondeck_info:
            try:
                first_seen = datetime.fromisoformat(ondeck_info["first_seen"])
                days_on_ondeck = (now - first_seen).days

                if days_on_ondeck < 7:
                    score += 5
                elif days_on_ondeck < 14:
                    pass  # no adjustment
                elif days_on_ondeck < 30:
                    score -= 5
                else:
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

    def calculate_priority_with_breakdown(
        self,
        cache_path: str,
        timestamps: Dict,
        ondeck: Dict,
        watchlist: Dict,
        settings: Dict
    ) -> Tuple[int, Dict[str, Any]]:
        """
        Calculate priority score with detailed breakdown for UI display.

        Returns:
            Tuple of (final_score, breakdown_dict) where breakdown_dict contains
            base score, each bonus, and human-readable factors list.
        """
        breakdown = {
            "base": 50,
            "source_bonus": 0,
            "user_bonus": 0,
            "recency_bonus": 0,
            "age_bonus": 0,
            "episode_bonus": 0,
            "factors": []
        }
        score = 50
        now = datetime.now()

        # Get timestamp info
        ts_info = timestamps.get(cache_path, {})
        cached_at_str = ts_info.get("cached_at") if isinstance(ts_info, dict) else ts_info
        source = ts_info.get("source", "unknown") if isinstance(ts_info, dict) else "unknown"

        # Try to find in ondeck/watchlist trackers
        ondeck_info = None
        watchlist_info = None
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

        # Factor 1: Source type (+15 for ondeck, +0 for watchlist)
        if source == "ondeck":
            score += 15
            breakdown["source_bonus"] = 15
            breakdown["factors"].append({"label": "OnDeck source", "value": 15})

        # Factor 2: User count (+5 per user, max +15)
        users = set()
        if ondeck_info and "users" in ondeck_info:
            users.update(ondeck_info["users"])
        if watchlist_info and "users" in watchlist_info:
            users.update(watchlist_info["users"])

        user_bonus = min(len(users) * 5, 15)
        if user_bonus > 0:
            score += user_bonus
            breakdown["user_bonus"] = user_bonus
            user_label = f"Multiple users ({len(users)})" if len(users) > 1 else "Single user"
            breakdown["factors"].append({"label": user_label, "value": user_bonus})

        # Factor 3: Cache recency (+5 if <24h, +3 if <72h, 0 otherwise)
        if cached_at_str:
            try:
                cached_at = datetime.fromisoformat(cached_at_str)
                hours_cached = (now - cached_at).total_seconds() / 3600

                if hours_cached < 24:
                    score += 5
                    breakdown["recency_bonus"] = 5
                    breakdown["factors"].append({"label": "Recently cached (<24h)", "value": 5})
                elif hours_cached < 72:
                    score += 3
                    breakdown["recency_bonus"] = 3
                    breakdown["factors"].append({"label": "Cached recently (<72h)", "value": 3})
                # >72h: no adjustment (0)
            except (ValueError, TypeError):
                pass

        # Factor 4: Watchlist age (+10 if <7 days, -10 if >60 days)
        if watchlist_info and "watchlisted_at" in watchlist_info:
            try:
                watchlisted_at = datetime.fromisoformat(watchlist_info["watchlisted_at"])
                days_on_watchlist = (now - watchlisted_at).days

                if days_on_watchlist < 7:
                    score += 10
                    breakdown["age_bonus"] = 10
                    breakdown["factors"].append({"label": "Fresh on watchlist (<7d)", "value": 10})
                elif days_on_watchlist > 60:
                    score -= 10
                    breakdown["age_bonus"] = -10
                    breakdown["factors"].append({"label": "Stale watchlist (>60d)", "value": -10})
            except (ValueError, TypeError):
                pass

        # Factor 5: OnDeck staleness (uses first_seen, not last_seen)
        # +5 if <7 days, 0 if 7-14 days, -5 if 14-30 days, -10 if >30 days
        if source == "ondeck" and ondeck_info and "first_seen" in ondeck_info:
            try:
                first_seen = datetime.fromisoformat(ondeck_info["first_seen"])
                days_on_ondeck = (now - first_seen).days

                if days_on_ondeck < 7:
                    score += 5
                    breakdown["staleness_bonus"] = 5
                    breakdown["factors"].append({"label": "Fresh on OnDeck (<7d)", "value": 5})
                elif days_on_ondeck < 14:
                    pass  # no adjustment
                elif days_on_ondeck < 30:
                    score -= 5
                    breakdown["staleness_bonus"] = -5
                    breakdown["factors"].append({"label": "Getting stale (14-30d)", "value": -5})
                else:
                    score -= 10
                    breakdown["staleness_bonus"] = -10
                    breakdown["factors"].append({"label": "Stale OnDeck (>30d)", "value": -10})
            except (ValueError, TypeError):
                pass

        # Factor 5: Episode position (for TV)
        if ondeck_info and "episode_info" in ondeck_info:
            ep_info = ondeck_info["episode_info"]
            if ep_info.get("is_current_ondeck"):
                score += 15
                breakdown["episode_bonus"] = 15
                breakdown["factors"].append({"label": "Current episode", "value": 15})
            elif ep_info.get("episode"):
                score += 10
                breakdown["episode_bonus"] = 10
                breakdown["factors"].append({"label": "Prefetched episode", "value": 10})

        final_score = max(0, min(100, score))
        return final_score, breakdown

    def get_all_cached_files(
        self,
        source_filter: str = "all",
        search: str = "",
        sort_by: str = "priority",
        sort_dir: str = "desc"
    ) -> List[CachedFile]:
        """
        Get all cached files with their metadata and priority scores.

        Subtitle files are grouped with their parent video file rather than
        shown as separate entries. The video file inherits subtitle count.

        Args:
            source_filter: "all", "ondeck", "watchlist", or "other"
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

        # First pass: separate subtitles from videos and build lookup structures
        subtitle_paths = []
        video_paths = []

        for cache_path in cached_paths:
            filename = os.path.basename(cache_path)
            if self._is_subtitle_file(filename):
                subtitle_paths.append(cache_path)
            else:
                video_paths.append(cache_path)

        # Build a map of directory + video base name -> video path for subtitle matching
        # Key: (directory, video_base_without_extension)
        video_by_base = {}
        for video_path in video_paths:
            directory = os.path.dirname(video_path)
            filename = os.path.basename(video_path)
            # Get base name without extension
            base_name = os.path.splitext(filename)[0]
            video_by_base[(directory, base_name.lower())] = video_path

        # Group subtitles with their parent videos
        # Map video_path -> list of subtitle paths
        video_subtitles = {}
        orphan_subtitles = []

        for sub_path in subtitle_paths:
            directory = os.path.dirname(sub_path)
            sub_base = self._get_video_base_name(sub_path).lower()

            # Find matching video in same directory
            video_path = video_by_base.get((directory, sub_base))
            if video_path:
                if video_path not in video_subtitles:
                    video_subtitles[video_path] = []
                video_subtitles[video_path].append(sub_path)
            else:
                # Orphan subtitle - no matching video found
                orphan_subtitles.append(sub_path)

        # Build the file list with grouped subtitles
        files = []

        for cache_path in video_paths:
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
            if source_filter == "other" and (is_ondeck or is_watchlist):
                continue

            # Calculate priority
            priority = self.calculate_priority(
                cache_path, timestamps, ondeck, watchlist, settings
            )

            # Get associated subtitles
            subs = video_subtitles.get(cache_path, [])

            files.append(CachedFile(
                path=cache_path,
                filename=filename,
                size=size,
                size_display=format_bytes(size),
                cached_at=cached_at,
                cache_age_hours=cache_age_hours,
                source=source,
                priority_score=priority,
                users=list(users),
                is_ondeck=is_ondeck,
                is_watchlist=is_watchlist,
                episode_info=episode_info,
                subtitle_count=len(subs),
                subtitle_paths=subs if subs else None
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

        # Use get_all_cached_files() for consistency with Storage page
        # This groups subtitles with their parent videos instead of counting separately
        all_files = self.get_all_cached_files()
        ondeck = self.get_ondeck_tracker()
        watchlist = self.get_watchlist_tracker()
        settings = self._load_settings()

        # Calculate total size from grouped files (includes subtitle sizes)
        cached_files_size = sum(f.size for f in all_files)

        # Get actual cache drive usage (use path_mappings cache_path for consistency)
        cache_dir = self._get_cache_dir(settings)
        disk_used = 0
        disk_total = 0
        usage_percent = 0

        if cache_dir and os.path.exists(cache_dir):
            try:
                drive_size_override = parse_size_bytes(settings.get("cache_drive_size", ""))
                disk = get_disk_usage(cache_dir, drive_size_override)
                disk_used = disk.used
                disk_total = disk.total
                usage_percent = int((disk.used / disk.total) * 100) if disk_total > 0 else 0
            except (OSError, AttributeError) as e:
                logging.warning(f"Could not get disk usage for {cache_dir}: {e}")

        # Count ondeck and watchlist items
        ondeck_count = len(ondeck)
        watchlist_count = len(watchlist)

        # Calculate eviction threshold status
        eviction_over_threshold = False
        eviction_over_by = 0
        eviction_over_by_display = None
        eviction_mode = settings.get("cache_eviction_mode", "none")
        cache_limit_bytes = 0

        if eviction_mode != "none" and disk_total > 0:
            # Get the configured cache limit (not disk total)
            cache_limit_setting = settings.get("cache_limit", "")

            if cache_limit_setting and cache_limit_setting not in ["", "N/A", "none", "None", "0"]:
                try:
                    limit_str = str(cache_limit_setting).strip()
                    if limit_str.endswith("%"):
                        percent_val = int(limit_str.rstrip("%"))
                        cache_limit_bytes = int(disk_total * percent_val / 100)
                    else:
                        import re
                        match = re.match(r'^([\d.]+)\s*(T|TB|G|GB|M|MB)?$', limit_str, re.IGNORECASE)
                        if match:
                            value = float(match.group(1))
                            unit = (match.group(2) or "GB").upper()
                            if unit in ("T", "TB"):
                                cache_limit_bytes = int(value * 1024**4)
                            elif unit in ("G", "GB"):
                                cache_limit_bytes = int(value * 1024**3)
                            elif unit in ("M", "MB"):
                                cache_limit_bytes = int(value * 1024**2)
                except (ValueError, TypeError) as e:
                    logging.warning(f"Could not parse cache_limit '{cache_limit_setting}': {e}")

            if cache_limit_bytes > 0:
                eviction_threshold_percent = settings.get("cache_eviction_threshold_percent", 95)
                eviction_threshold_bytes = int(cache_limit_bytes * eviction_threshold_percent / 100)

                if disk_used > eviction_threshold_bytes:
                    eviction_over_threshold = True
                    eviction_over_by = disk_used - eviction_threshold_bytes
                    eviction_over_by_display = format_bytes(eviction_over_by)

        cache_limit_exceeded = False
        if cache_limit_bytes > 0 and disk_used >= cache_limit_bytes:
            cache_limit_exceeded = True

        # Check min_free_space floor
        min_free_space_warning = False
        min_free_space_setting = settings.get("min_free_space", "")
        if min_free_space_setting and min_free_space_setting not in ["", "0"] and disk_total > 0:
            try:
                limit_str = str(min_free_space_setting).strip()
                min_free_bytes = 0
                if limit_str.upper().endswith('%'):
                    percent_val = int(limit_str.rstrip('%'))
                    min_free_bytes = int(disk_total * percent_val / 100)
                else:
                    import re as _re
                    match = _re.match(r'^([\d.]+)\s*(T|TB|G|GB|M|MB)?$', limit_str, _re.IGNORECASE)
                    if match:
                        value = float(match.group(1))
                        unit = (match.group(2) or "GB").upper()
                        if unit in ("T", "TB"):
                            min_free_bytes = int(value * 1024**4)
                        elif unit in ("G", "GB"):
                            min_free_bytes = int(value * 1024**3)
                        elif unit in ("M", "MB"):
                            min_free_bytes = int(value * 1024**2)
                disk_free = disk_total - disk_used
                if min_free_bytes > 0 and disk_free < min_free_bytes:
                    min_free_space_warning = True
            except (ValueError, TypeError):
                pass

        # Check plexcache quota
        plexcache_quota_exceeded = False
        plexcache_quota_setting = settings.get("plexcache_quota", "")
        if plexcache_quota_setting and plexcache_quota_setting not in ["", "0"]:
            try:
                quota_str = str(plexcache_quota_setting).strip()
                quota_bytes = 0
                if quota_str.upper().endswith('%'):
                    if disk_total > 0:
                        percent_val = int(quota_str.rstrip('%'))
                        quota_bytes = int(disk_total * percent_val / 100)
                else:
                    match = re.match(r'^([\d.]+)\s*(T|TB|G|GB|M|MB)?$', quota_str, re.IGNORECASE)
                    if match:
                        value = float(match.group(1))
                        unit = (match.group(2) or "GB").upper()
                        if unit in ("T", "TB"):
                            quota_bytes = int(value * 1024**4)
                        elif unit in ("G", "GB"):
                            quota_bytes = int(value * 1024**3)
                        elif unit in ("M", "MB"):
                            quota_bytes = int(value * 1024**2)
                if quota_bytes > 0 and cached_files_size >= quota_bytes:
                    plexcache_quota_exceeded = True
            except (ValueError, TypeError):
                pass

        return {
            "cache_files": len(all_files),  # Grouped count (subtitles with videos)
            "cache_size": format_bytes(disk_used),  # Actual disk used
            "cache_size_bytes": disk_used,
            "cache_limit": format_bytes(disk_total),  # Actual disk total
            "cache_limit_bytes": disk_total,
            "usage_percent": usage_percent,
            "cached_files_size": format_bytes(cached_files_size),  # PlexCache files only
            "cached_files_size_bytes": cached_files_size,
            "ondeck_count": ondeck_count,
            "watchlist_count": watchlist_count,
            "eviction_over_threshold": eviction_over_threshold,
            "eviction_over_by_display": eviction_over_by_display,
            "cache_limit_exceeded": cache_limit_exceeded,
            "min_free_space_warning": min_free_space_warning,
            "plexcache_quota_exceeded": plexcache_quota_exceeded
        }

    def get_drive_details(self, expiring_within_days: int = 3) -> Dict[str, Any]:
        """Get comprehensive cache drive details for the drive info page

        Args:
            expiring_within_days: Show files expiring within this many days (default 3)
        """
        import shutil

        cached_paths = self.get_cached_files_list()
        timestamps = self.get_timestamps()
        ondeck = self.get_ondeck_tracker()
        watchlist = self.get_watchlist_tracker()
        settings = self._load_settings()

        now = datetime.now()

        # Get all cached files with metadata
        all_files = self.get_all_cached_files()

        # Storage Overview (use path_mappings cache_path for consistency)
        cache_dir = self._get_cache_dir(settings)
        disk_used = 0
        disk_total = 0
        disk_free = 0
        is_zfs = False
        has_manual_drive_size = False

        if cache_dir and os.path.exists(cache_dir):
            try:
                drive_size_override = parse_size_bytes(settings.get("cache_drive_size", ""))
                has_manual_drive_size = drive_size_override > 0
                disk = get_disk_usage(cache_dir, drive_size_override)
                disk_used = disk.used
                disk_total = disk.total
                disk_free = disk.free
                # Detect ZFS filesystem (values may be inaccurate without manual override)
                is_zfs = detect_zfs(cache_dir)
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
        # Show files within N days of expiring OR already expired (still on cache)
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
                                days_on_watchlist = (now - watchlisted_at).days
                                days_remaining = watchlist_retention_days - days_on_watchlist
                                # Show if within expiring_within_days OR already expired
                                if days_remaining <= expiring_within_days:
                                    expiring_soon.append({
                                        "file": f,
                                        "days_remaining": days_remaining,
                                        "days_on_watchlist": days_on_watchlist
                                    })
                            except (ValueError, TypeError):
                                pass
                        break
        # Sort by days remaining (most urgent first, expired items at top)
        expiring_soon.sort(key=lambda x: x["days_remaining"])

        # Recent activity (last 24h and 7d counts)
        files_last_24h = sum(1 for f in all_files if f.cache_age_hours <= 24)
        files_last_7d = sum(1 for f in all_files if f.cache_age_hours <= 168)

        # Recently cached files (last 24h)
        recently_cached = [f for f in all_files if f.cache_age_hours <= 24]
        recently_cached.sort(key=lambda f: f.cached_at, reverse=True)

        # Calculate cache limit info
        cache_limit_setting = settings.get("cache_limit", "")
        cache_limit_bytes = 0
        cache_limit_display = None
        cache_limit_percent = None

        if cache_limit_setting and cache_limit_setting not in ["", "N/A", "none", "None", "0"]:
            try:
                limit_str = str(cache_limit_setting).strip()
                if limit_str.endswith("%"):
                    # Percentage-based limit
                    percent_val = int(limit_str.rstrip("%"))
                    cache_limit_bytes = int(disk_total * percent_val / 100)
                    cache_limit_display = f"{percent_val}% = {format_bytes(cache_limit_bytes)}"
                    cache_limit_percent = percent_val
                else:
                    cache_limit_bytes = parse_size_bytes(limit_str)
                    if cache_limit_bytes > 0:
                        cache_limit_display = format_bytes(cache_limit_bytes)
                        if disk_total > 0:
                            cache_limit_percent = round(cache_limit_bytes / disk_total * 100, 1)
            except (ValueError, TypeError):
                pass

        # Calculate usage against limit
        cache_limit_used_percent = 0
        cache_limit_available = 0
        if cache_limit_bytes > 0:
            # Use actual drive usage (not just tracked) to calculate available
            effective_usage = max(disk_used, total_cached_size)
            cache_limit_used_percent = round(effective_usage / cache_limit_bytes * 100, 1)
            cache_limit_available = max(0, cache_limit_bytes - effective_usage)

        # Calculate min_free_space info
        min_free_space_setting = settings.get("min_free_space", "")
        min_free_space_bytes = 0
        min_free_space_display = None
        min_free_space_percent = None
        min_free_space_warning = False
        min_free_space_available = 0

        if min_free_space_setting and min_free_space_setting not in ["", "0"] and disk_total > 0:
            try:
                mfs_str = str(min_free_space_setting).strip()
                if mfs_str.upper().endswith('%'):
                    percent_val = int(mfs_str.rstrip('%').rstrip().upper().rstrip('%'))
                    min_free_space_bytes = int(disk_total * percent_val / 100)
                    min_free_space_display = f"{percent_val}% = {format_bytes(min_free_space_bytes)}"
                    min_free_space_percent = percent_val
                else:
                    min_free_space_bytes = parse_size_bytes(mfs_str)
                    if min_free_space_bytes > 0:
                        min_free_space_display = format_bytes(min_free_space_bytes)
                        if disk_total > 0:
                            min_free_space_percent = round(min_free_space_bytes / disk_total * 100, 1)
            except (ValueError, TypeError):
                pass

        if min_free_space_bytes > 0:
            min_free_space_available = max(0, disk_free - min_free_space_bytes)
            if disk_free < min_free_space_bytes:
                min_free_space_warning = True

        # Calculate plexcache_quota info (only counts PlexCache-managed files)
        plexcache_quota_setting = settings.get("plexcache_quota", "")
        plexcache_quota_bytes = 0
        plexcache_quota_display = None
        plexcache_quota_percent = None
        plexcache_quota_warning = False
        plexcache_quota_available = 0
        plexcache_quota_used_percent = 0

        if plexcache_quota_setting and plexcache_quota_setting not in ["", "0"]:
            try:
                quota_str = str(plexcache_quota_setting).strip()
                if quota_str.upper().endswith('%'):
                    if disk_total > 0:
                        percent_val = int(quota_str.rstrip('%'))
                        plexcache_quota_bytes = int(disk_total * percent_val / 100)
                        plexcache_quota_display = f"{percent_val}% = {format_bytes(plexcache_quota_bytes)}"
                        plexcache_quota_percent = percent_val
                else:
                    plexcache_quota_bytes = parse_size_bytes(quota_str)
                    if plexcache_quota_bytes > 0:
                        plexcache_quota_display = format_bytes(plexcache_quota_bytes)
                        if disk_total > 0:
                            plexcache_quota_percent = round(plexcache_quota_bytes / disk_total * 100, 1)
            except (ValueError, TypeError):
                pass

        if plexcache_quota_bytes > 0:
            plexcache_quota_used_percent = round(total_cached_size / plexcache_quota_bytes * 100, 1) if plexcache_quota_bytes > 0 else 0
            plexcache_quota_available = max(0, plexcache_quota_bytes - total_cached_size)
            if total_cached_size >= plexcache_quota_bytes:
                plexcache_quota_warning = True

        # Calculate eviction threshold (for visual display)
        eviction_threshold_setting = settings.get("cache_eviction_threshold_percent", 95)
        eviction_threshold_bytes = 0
        eviction_threshold_display = None
        eviction_threshold_percent_of_drive = 0
        eviction_over_threshold = False
        eviction_over_by = 0
        eviction_approaching = False

        if cache_limit_bytes > 0:
            eviction_threshold_bytes = int(cache_limit_bytes * eviction_threshold_setting / 100)
            eviction_threshold_display = format_bytes(eviction_threshold_bytes)
            if disk_total > 0:
                eviction_threshold_percent_of_drive = round(eviction_threshold_bytes / disk_total * 100, 1)
            # Check if drive usage is over threshold
            if disk_used > eviction_threshold_bytes:
                eviction_over_threshold = True
                eviction_over_by = disk_used - eviction_threshold_bytes
            # Check if approaching threshold (within 90% of threshold)
            elif disk_used > eviction_threshold_bytes * 0.9:
                eviction_approaching = True

        # Calculate "other files" (non-PlexCache) usage for stacked bar
        other_drive_size = max(0, disk_used - total_cached_size)
        other_drive_percent = calc_percent(other_drive_size, disk_total) if disk_total > 0 else 0

        # Determine cache status for context-aware coloring
        # "safe" = green, "approaching" = orange, "over" = red
        if eviction_over_threshold:
            cache_bar_status = "over"
        elif eviction_approaching:
            cache_bar_status = "approaching"
        else:
            cache_bar_status = "safe"

        # Configuration
        eviction_mode = settings.get("cache_eviction_mode", "none")
        # Use display path (host path) for UI, not container path
        display_cache_dir = self._get_cache_dir_for_display(settings)
        config = {
            "cache_dir": display_cache_dir,
            "cache_limit": settings.get("cache_limit", "N/A"),
            "cache_retention_hours": settings.get("cache_retention_hours", 72),
            "watchlist_retention_days": watchlist_retention_days,
            "number_episodes": settings.get("number_episodes", 5),
            "eviction_mode": eviction_mode,
            "eviction_enabled": eviction_mode != "none",
            "eviction_threshold_percent": settings.get("cache_eviction_threshold_percent", 95),
            "eviction_min_priority": settings.get("eviction_min_priority", 60)
        }

        return {
            # Storage Overview
            "storage": {
                "total": disk_total,
                "total_display": format_bytes(disk_total),
                "used": disk_used,
                "used_display": format_bytes(disk_used),
                "free": disk_free,
                "free_display": format_bytes(disk_free),
                "usage_percent": calc_percent(disk_used, disk_total),
                "cached_size": total_cached_size,
                "cached_size_display": format_bytes(total_cached_size),
                "cached_percent": calc_percent(total_cached_size, disk_total),
                "file_count": len(all_files),
                # ZFS detection (values may need manual override)
                "is_zfs": is_zfs,
                "has_manual_drive_size": has_manual_drive_size,
                # Cache limit info
                "cache_limit_bytes": cache_limit_bytes,
                "cache_limit_display": cache_limit_display,
                "cache_limit_percent": cache_limit_percent,
                "cache_limit_used_percent": cache_limit_used_percent,
                "cache_limit_available": cache_limit_available,
                "cache_limit_available_display": format_bytes(cache_limit_available) if cache_limit_bytes > 0 else None,
                # Eviction threshold info
                "eviction_threshold_bytes": eviction_threshold_bytes,
                "eviction_threshold_display": eviction_threshold_display,
                "eviction_threshold_setting": eviction_threshold_setting,
                "eviction_threshold_percent_of_drive": eviction_threshold_percent_of_drive,
                "eviction_over_threshold": eviction_over_threshold,
                "eviction_approaching": eviction_approaching,
                "eviction_over_by": eviction_over_by,
                "eviction_over_by_display": format_bytes(eviction_over_by) if eviction_over_by > 0 else None,
                # Min free space info
                "min_free_space_bytes": min_free_space_bytes,
                "min_free_space_display": min_free_space_display,
                "min_free_space_percent": min_free_space_percent,
                "min_free_space_warning": min_free_space_warning,
                "min_free_space_available": min_free_space_available,
                "min_free_space_available_display": format_bytes(min_free_space_available) if min_free_space_bytes > 0 else None,
                # PlexCache quota info
                "plexcache_quota_bytes": plexcache_quota_bytes,
                "plexcache_quota_display": plexcache_quota_display,
                "plexcache_quota_percent": plexcache_quota_percent,
                "plexcache_quota_used_percent": plexcache_quota_used_percent,
                "plexcache_quota_available": plexcache_quota_available,
                "plexcache_quota_available_display": format_bytes(plexcache_quota_available) if plexcache_quota_bytes > 0 else None,
                "plexcache_quota_warning": plexcache_quota_warning,
                # Stacked bar data
                "other_drive_size": other_drive_size,
                "other_drive_size_display": format_bytes(other_drive_size),
                "other_drive_percent": other_drive_percent,
                "cache_bar_status": cache_bar_status  # "safe", "approaching", or "over"
            },
            # Breakdown by source
            "breakdown": {
                "ondeck": {
                    "count": ondeck_count,
                    "size": ondeck_size,
                    "size_display": format_bytes(ondeck_size),
                    "percent": calc_percent(ondeck_size, total_cached_size) if total_cached_size > 0 else 0
                },
                "watchlist": {
                    "count": watchlist_count,
                    "size": watchlist_size,
                    "size_display": format_bytes(watchlist_size),
                    "percent": calc_percent(watchlist_size, total_cached_size) if total_cached_size > 0 else 0
                },
                "other": {
                    "count": other_count,
                    "size": other_size,
                    "size_display": format_bytes(other_size),
                    "percent": calc_percent(other_size, total_cached_size) if total_cached_size > 0 else 0
                }
            },
            # File analysis (scrollable panels can show more data)
            "largest_files": largest_files[:50],
            "oldest_files": oldest_files[:50],
            "expiring_soon": expiring_soon[:50],
            "expiring_within_days": expiring_within_days,
            # Activity
            "activity": {
                "files_last_24h": files_last_24h,
                "files_last_7d": files_last_7d,
                "recently_cached": recently_cached[:50]
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
        high = [f for f in files if f.priority_score >= 90]
        medium = [f for f in files if 70 <= f.priority_score < 90]
        low = [f for f in files if f.priority_score < 70]

        lines.append("SUMMARY BY TIER:")
        lines.append(f"  High priority (90-100):   {len(high)} files")
        lines.append(f"  Medium priority (70-89):  {len(medium)} files")
        lines.append(f"  Low priority (0-69):      {len(low)} files (eviction candidates)")
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
            priority_tier = "HIGH" if f.priority_score >= 90 else "MED" if f.priority_score >= 70 else "LOW"
            user_str = f", users: {', '.join(f.users)}" if f.users else ""

            lines.append(f"[{priority_tier}] Score: {f.priority_score}")
            lines.append(f"  File: {f.filename}")
            lines.append(f"  Size: {f.size_display}, Source: {f.source}, Age: {f.cache_age_hours:.1f}h{user_str}")
            lines.append("")

        return "\n".join(lines)

    def get_priority_report_data(self) -> Dict[str, Any]:
        """
        Generate structured priority report data for the UI.

        Returns dict with summary stats, tier distribution, files with breakdowns,
        and eviction settings.
        """
        import shutil

        cached_paths = self.get_cached_files_list()
        timestamps = self.get_timestamps()
        ondeck = self.get_ondeck_tracker()
        watchlist = self.get_watchlist_tracker()
        settings = self._load_settings()

        now = datetime.now()

        # Get all cached files
        all_files = self.get_all_cached_files()

        # Build files list with priority breakdowns
        files_with_breakdown = []
        for f in all_files:
            _, breakdown = self.calculate_priority_with_breakdown(
                f.path, timestamps, ondeck, watchlist, settings
            )
            files_with_breakdown.append({
                "path": f.path,
                "filename": f.filename,
                "size": f.size,
                "size_display": f.size_display,
                "cached_at": f.cached_at.isoformat() if f.cached_at else None,
                "cache_age_hours": f.cache_age_hours,
                "source": f.source,
                "priority_score": f.priority_score,
                "users": f.users,
                "is_ondeck": f.is_ondeck,
                "is_watchlist": f.is_watchlist,
                "priority_breakdown": breakdown
            })

        # Calculate tier distribution
        high_files = [f for f in files_with_breakdown if f["priority_score"] >= 90]
        medium_files = [f for f in files_with_breakdown if 70 <= f["priority_score"] < 90]
        low_files = [f for f in files_with_breakdown if f["priority_score"] < 70]

        total_count = len(files_with_breakdown)
        total_size = sum(f["size"] for f in files_with_breakdown)

        def calc_percent(count: int, total: int) -> float:
            return round((count / total * 100), 1) if total > 0 else 0

        tiers = {
            "high": {
                "count": len(high_files),
                "percent": calc_percent(len(high_files), total_count),
                "size": sum(f["size"] for f in high_files),
                "size_display": format_bytes(sum(f["size"] for f in high_files))
            },
            "medium": {
                "count": len(medium_files),
                "percent": calc_percent(len(medium_files), total_count),
                "size": sum(f["size"] for f in medium_files),
                "size_display": format_bytes(sum(f["size"] for f in medium_files))
            },
            "low": {
                "count": len(low_files),
                "percent": calc_percent(len(low_files), total_count),
                "size": sum(f["size"] for f in low_files),
                "size_display": format_bytes(sum(f["size"] for f in low_files))
            }
        }

        # Summary by source
        ondeck_count = sum(1 for f in files_with_breakdown if f["is_ondeck"])
        watchlist_count = sum(1 for f in files_with_breakdown if f["is_watchlist"] and not f["is_ondeck"])
        other_count = sum(1 for f in files_with_breakdown if not f["is_ondeck"] and not f["is_watchlist"])

        summary = {
            "total": total_count,
            "ondeck_count": ondeck_count,
            "watchlist_count": watchlist_count,
            "other_count": other_count,
            "total_size": total_size,
            "total_size_display": format_bytes(total_size)
        }

        # Eviction settings and current status
        eviction_mode = settings.get("cache_eviction_mode", "none")
        eviction_threshold = settings.get("cache_eviction_threshold_percent", 95)
        eviction_min_priority = settings.get("eviction_min_priority", 60)

        # Calculate current drive usage (use path_mappings cache_path for consistency)
        cache_dir = self._get_cache_dir(settings)
        current_usage_percent = 0
        disk_used = 0
        disk_total = 0

        if cache_dir and os.path.exists(cache_dir):
            try:
                drive_size_override = parse_size_bytes(settings.get("cache_drive_size", ""))
                disk = get_disk_usage(cache_dir, drive_size_override)
                disk_used = disk.used
                disk_total = disk.total
                current_usage_percent = round((disk.used / disk.total) * 100, 1) if disk_total > 0 else 0
            except (OSError, AttributeError):
                pass

        # Calculate how many files would be evicted at current threshold
        would_evict_count = 0
        would_evict_size = 0
        if eviction_mode != "none":
            # Files below min_priority are eviction candidates
            eviction_candidates = [f for f in files_with_breakdown if f["priority_score"] < eviction_min_priority]
            would_evict_count = len(eviction_candidates)
            would_evict_size = sum(f["size"] for f in eviction_candidates)

        eviction = {
            "mode": eviction_mode,
            "enabled": eviction_mode != "none",
            "threshold_percent": eviction_threshold,
            "min_priority": eviction_min_priority,
            "current_usage_percent": current_usage_percent,
            "would_evict_count": would_evict_count,
            "would_evict_size": would_evict_size,
            "would_evict_size_display": format_bytes(would_evict_size)
        }

        return {
            "summary": summary,
            "tiers": tiers,
            "files": files_with_breakdown,
            "eviction": eviction
        }

    def simulate_eviction(self, threshold_percent: int) -> Dict[str, Any]:
        """
        Simulate which files would be evicted at a given threshold.

        Args:
            threshold_percent: Simulated eviction threshold (50-100)

        Returns dict with files that would be evicted and space freed.
        """
        import shutil
        import re

        settings = self._load_settings()
        cache_dir = self._get_cache_dir(settings)

        # Get current drive usage (use manual override if configured)
        disk_used = 0
        disk_total = 0
        if cache_dir and os.path.exists(cache_dir):
            try:
                drive_size_override = parse_size_bytes(settings.get("cache_drive_size", ""))
                disk = get_disk_usage(cache_dir, drive_size_override)
                disk_used = disk.used
                disk_total = disk.total
            except (OSError, AttributeError):
                pass

        # Calculate cache_limit_bytes (must match core/app.py logic)
        # Eviction threshold is a percentage of cache_limit, not total drive
        cache_limit_bytes = 0
        cache_limit_setting = settings.get("cache_limit", "")
        if cache_limit_setting and cache_limit_setting not in ["", "N/A", "none", "None", "0"]:
            try:
                limit_str = str(cache_limit_setting).strip()
                if limit_str.endswith("%"):
                    # Percentage of drive size
                    percent_val = int(limit_str.rstrip("%"))
                    cache_limit_bytes = int(disk_total * percent_val / 100)
                else:
                    cache_limit_bytes = parse_size_bytes(limit_str)
            except (ValueError, TypeError):
                pass

        # Fall back to disk_total if no cache_limit set
        if cache_limit_bytes == 0:
            cache_limit_bytes = disk_total

        # Calculate target bytes at threshold (percentage of cache_limit, not disk_total)
        target_bytes = int(cache_limit_bytes * threshold_percent / 100) if cache_limit_bytes > 0 else 0
        bytes_to_free = max(0, disk_used - target_bytes)

        # Get all files sorted by priority (lowest first = evict first)
        all_files = self.get_all_cached_files(sort_by="priority", sort_dir="asc")

        # Determine which files would be evicted
        would_evict = []
        freed_so_far = 0

        for f in all_files:
            if freed_so_far >= bytes_to_free:
                break
            would_evict.append({
                "path": f.path,
                "filename": f.filename,
                "size": f.size,
                "size_display": f.size_display,
                "priority_score": f.priority_score,
                "source": f.source
            })
            freed_so_far += f.size

        remaining_count = len(all_files) - len(would_evict)

        return {
            "threshold_percent": threshold_percent,
            "cache_limit_bytes": cache_limit_bytes,
            "cache_limit_display": format_bytes(cache_limit_bytes),
            "current_usage_percent": round((disk_used / cache_limit_bytes * 100), 1) if cache_limit_bytes > 0 else 0,
            "target_usage_percent": threshold_percent,
            "target_bytes": target_bytes,
            "target_bytes_display": format_bytes(target_bytes),
            "bytes_to_free": bytes_to_free,
            "bytes_to_free_display": format_bytes(bytes_to_free),
            "would_evict": would_evict,
            "total_freed": freed_so_far,
            "total_freed_display": format_bytes(freed_so_far),
            "remaining_count": remaining_count
        }

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
            # Track whether array copy is confirmed
            array_confirmed = False

            # Step 1: Restore .plexcached backup if it exists
            if plexcached_path and os.path.exists(plexcached_path):
                # Rename .plexcached back to original
                os.rename(plexcached_path, array_path)
                array_confirmed = True
            elif array_path and os.path.exists(get_array_direct_path(array_path)):
                # Array file truly exists on array (verified via /mnt/user0/, not FUSE)
                # CRITICAL: Using /mnt/user0/ avoids false positive where /mnt/user/
                # shows the cache file as if it exists on array
                array_confirmed = True
            elif os.path.exists(cache_path):
                # No backup and no array copy - must copy from cache first to prevent data loss
                if array_path:
                    import shutil
                    # CRITICAL: Copy to /mnt/user0/ (array direct), NOT /mnt/user/ (FUSE)
                    # If we copy to /mnt/user/, Unraid's cache policy may put the file
                    # back on cache (if shareUseCache=yes), causing data loss when we
                    # delete the "original" cache file.
                    array_direct_path = get_array_direct_path(array_path)
                    array_direct_dir = os.path.dirname(array_direct_path)
                    os.makedirs(array_direct_dir, exist_ok=True)
                    shutil.copy2(cache_path, array_direct_path)
                    # Verify copy succeeded on array
                    if os.path.exists(array_direct_path):
                        cache_size = os.path.getsize(cache_path)
                        array_size = os.path.getsize(array_direct_path)
                        if cache_size == array_size:
                            array_confirmed = True
                        else:
                            os.remove(array_direct_path)  # Remove failed copy
                            result["message"] = "Size mismatch during copy - eviction aborted to prevent data loss"
                            return result
                    else:
                        result["message"] = "Failed to create array copy - eviction aborted to prevent data loss"
                        return result
                else:
                    result["message"] = "Cannot determine array path - eviction aborted to prevent data loss"
                    return result

            # Step 2: Delete cache copy ONLY if array copy is confirmed
            if not array_confirmed:
                result["message"] = "Array copy not confirmed - eviction aborted to prevent data loss"
                return result

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

    def _cache_to_real(self, cache_path: str, path_mappings: List[Dict]) -> Optional[str]:
        """Convert a cache path to a real (array) path via path_mappings prefix swap."""
        for mapping in path_mappings:
            if not mapping.get('enabled', True):
                continue
            cache_prefix = mapping.get('cache_path', '').rstrip('/')
            real_prefix = mapping.get('real_path', '').rstrip('/')
            if cache_prefix and cache_path.startswith(cache_prefix):
                return real_prefix + cache_path[len(cache_prefix):]
        return None

    def _real_to_cache(self, real_path: str, path_mappings: List[Dict]) -> Optional[str]:
        """Convert a real (array) path to a cache path via path_mappings prefix swap."""
        for mapping in path_mappings:
            if not mapping.get('enabled', True):
                continue
            real_prefix = mapping.get('real_path', '').rstrip('/')
            cache_prefix = mapping.get('cache_path', '').rstrip('/')
            if real_prefix and real_path.startswith(real_prefix):
                return cache_prefix + real_path[len(real_prefix):]
        return None

    def _plex_to_real(self, plex_path: str, path_mappings: List[Dict]) -> Optional[str]:
        """Convert a Plex path to a real (array) path via path_mappings prefix swap."""
        for mapping in path_mappings:
            if not mapping.get('enabled', True):
                continue
            plex_prefix = mapping.get('plex_path', '').rstrip('/')
            real_prefix = mapping.get('real_path', '').rstrip('/')
            if plex_prefix and plex_path.startswith(plex_prefix):
                return real_prefix + plex_path[len(plex_prefix):]
        return None

    def _real_to_plex(self, real_path: str, path_mappings: List[Dict]) -> Optional[str]:
        """Convert a real (array) path to a Plex path via path_mappings prefix swap."""
        for mapping in path_mappings:
            if not mapping.get('enabled', True):
                continue
            real_prefix = mapping.get('real_path', '').rstrip('/')
            plex_prefix = mapping.get('plex_path', '').rstrip('/')
            if real_prefix and real_path.startswith(real_prefix):
                return plex_prefix + real_path[len(real_prefix):]
        return None

    def _add_to_exclude_file(self, cache_path: str):
        """Add a cache path to the exclude file (with host path translation and dedup)."""
        settings = self._load_settings()
        host_path = translate_container_to_host_path(cache_path, settings.get('path_mappings', []))

        existing = set()
        if self.exclude_file.exists():
            try:
                with open(self.exclude_file, 'r', encoding='utf-8') as f:
                    existing = {line.strip() for line in f if line.strip()}
            except IOError:
                pass

        if host_path not in existing:
            with open(self.exclude_file, 'a', encoding='utf-8') as f:
                f.write(host_path + '\n')

    def check_for_upgrades(self, stale_exclude_entries: List[str]) -> Dict[str, Any]:
        """Check if stale exclude entries are actually media upgrades (Sonarr/Radarr swaps).

        For each stale entry that has a rating_key in the OnDeck tracker, queries Plex
        to see if the file path has changed (indicating an upgrade). If so, transfers
        all tracking data from the old path to the new path.

        Args:
            stale_exclude_entries: Cache paths that are in the exclude list but not on cache.

        Returns:
            Dict with upgrades_found, upgrades_resolved, and details list.
        """
        logger = logging.getLogger(__name__)
        result = {"upgrades_found": 0, "upgrades_resolved": 0, "details": []}

        if not stale_exclude_entries:
            return result

        settings = self._load_settings()
        path_mappings = settings.get('path_mappings', [])
        ondeck_data = self.get_ondeck_tracker()

        if not ondeck_data:
            return result

        # For each stale entry, check if it maps to an ondeck entry with a rating_key
        # Note: OnDeck tracker keys are REAL paths (/mnt/user/...), not Plex paths
        candidates: List[Tuple[str, str, str]] = []  # (cache_path, rating_key, real_path)
        for cache_path in stale_exclude_entries:
            real_path = self._cache_to_real(cache_path, path_mappings)
            if not real_path:
                continue
            # Look up by real path (OnDeck tracker key format)
            entry = ondeck_data.get(real_path)
            if entry and entry.get('rating_key'):
                candidates.append((cache_path, entry['rating_key'], real_path))

        if not candidates:
            return result

        # Connect to Plex once for all candidates
        plex_url = settings.get('plex_url', '') or settings.get('PLEX_URL', '')
        plex_token = settings.get('plex_token', '') or settings.get('PLEX_TOKEN', '')
        if not plex_url or not plex_token:
            logger.debug("Upgrade check skipped: no Plex credentials configured")
            return result

        try:
            from plexapi.server import PlexServer
            plex = PlexServer(plex_url, plex_token, timeout=10)
        except Exception as e:
            logger.warning(f"Upgrade check: could not connect to Plex: {e}")
            return result

        # Check each candidate against Plex
        for cache_path, rating_key, old_real_path in candidates:
            try:
                item = plex.fetchItem(int(rating_key))
            except Exception as e:
                logger.debug(f"Upgrade check: fetchItem({rating_key}) failed: {e}")
                continue

            # Get current file path from Plex (returns plex-internal path)
            try:
                new_plex_path = item.media[0].parts[0].file
            except (IndexError, AttributeError):
                logger.debug(f"Upgrade check: no file path for rating_key={rating_key}")
                continue

            # Convert Plex path to real path for comparison with tracker key
            new_real_path = self._plex_to_real(new_plex_path, path_mappings)
            if not new_real_path:
                logger.debug(f"Upgrade check: cannot convert plex path for rk={rating_key}: {new_plex_path}")
                continue

            if new_real_path == old_real_path:
                continue  # Same path, not an upgrade

            # Upgrade detected
            result["upgrades_found"] += 1

            new_cache_path = self._real_to_cache(new_real_path, path_mappings)
            if not new_cache_path:
                logger.warning(f"Upgrade detected (rk={rating_key}) but cannot convert new real path: {new_real_path}")
                continue

            # Verify the new file actually exists on cache
            if not os.path.exists(new_cache_path):
                logger.debug(f"Upgrade detected (rk={rating_key}) but new file not yet on cache: {new_cache_path}")
                continue

            # Derive plex paths for watchlist tracker (uses plex path keys)
            old_plex_path = self._real_to_plex(old_real_path, path_mappings)

            logger.info(f"[UPGRADE] Detected file upgrade (rk={rating_key}): "
                        f"{os.path.basename(cache_path)} -> {os.path.basename(new_cache_path)}")

            success = self._transfer_upgrade_tracking(
                old_cache_path=cache_path,
                old_real_path=old_real_path,
                old_plex_path=old_plex_path,
                new_cache_path=new_cache_path,
                new_real_path=new_real_path,
                new_plex_path=new_plex_path,
                rating_key=rating_key,
                settings=settings,
                path_mappings=path_mappings,
            )

            if success:
                result["upgrades_resolved"] += 1
                result["details"].append({
                    "rating_key": rating_key,
                    "old_file": os.path.basename(cache_path),
                    "new_file": os.path.basename(new_cache_path),
                })

        if result["upgrades_resolved"] > 0:
            logger.info(f"[UPGRADE] Resolved {result['upgrades_resolved']} media upgrade(s) via web audit")

        return result

    def _transfer_upgrade_tracking(
        self,
        old_cache_path: str,
        old_real_path: str,
        old_plex_path: str,
        new_cache_path: str,
        new_real_path: str,
        new_plex_path: str,
        rating_key: str,
        settings: Dict,
        path_mappings: List[Dict],
    ) -> bool:
        """Transfer all tracking data from old path to new path after a media upgrade.

        Updates: exclude list, timestamps, OnDeck tracker, watchlist tracker,
        and .plexcached backups.

        Returns True if transfer succeeded.
        """
        logger = logging.getLogger(__name__)

        try:
            # 1. Exclude list: remove old, add new
            self._remove_from_exclude_file(old_cache_path)
            self._add_to_exclude_file(new_cache_path)

            # 2. Timestamps: preserve source from old entry, remove old, add new
            timestamps = self.get_timestamps()
            old_ts = timestamps.get(old_cache_path, {})
            old_source = old_ts.get('source', 'unknown') if isinstance(old_ts, dict) else 'unknown'
            remove_from_timestamps_file(self.timestamps_file, old_cache_path)

            # Add new timestamp entry
            ts_data = {}
            if self.timestamps_file.exists():
                try:
                    with open(self.timestamps_file, 'r', encoding='utf-8') as f:
                        ts_data = json.load(f)
                except (json.JSONDecodeError, IOError):
                    pass
            ts_data[new_cache_path] = {
                "cached_at": datetime.now().isoformat(),
                "source": old_source,
            }
            save_json_atomically(str(self.timestamps_file), ts_data, label="timestamps")

            # 3. OnDeck tracker: remove old entry (new entry created on next operation run)
            # OnDeck tracker keys are real paths (/mnt/user/...)
            ondeck_data = self.get_ondeck_tracker()
            if old_real_path in ondeck_data:
                del ondeck_data[old_real_path]
                save_json_atomically(str(self.ondeck_file), ondeck_data, label="ondeck tracker")

            # 4. Watchlist tracker: transfer entry if exists
            # Watchlist tracker keys are plex paths (/data/...)
            watchlist_data = self.get_watchlist_tracker()
            if old_plex_path and old_plex_path in watchlist_data:
                watchlist_data[new_plex_path] = watchlist_data.pop(old_plex_path)
                save_json_atomically(str(self.watchlist_file), watchlist_data, label="watchlist tracker")

            # 5. Handle .plexcached backups
            self._handle_upgrade_plexcached(
                old_real_path, new_real_path, new_cache_path, rating_key, settings
            )

            logger.info(f"[UPGRADE] Tracking transfer complete (rk={rating_key})")
            return True

        except Exception as e:
            logger.error(f"[UPGRADE] Failed to transfer tracking (rk={rating_key}): {e}")
            return False

    def _handle_upgrade_plexcached(
        self,
        old_real_path: str,
        new_real_path: str,
        new_cache_path: str,
        rating_key: str,
        settings: Dict,
    ) -> None:
        """Handle .plexcached backup files during a media upgrade.

        Creates new backup first, verifies it, then deletes old backup.
        If new backup fails, old backup is preserved.
        """
        logger = logging.getLogger(__name__)

        if not settings.get('create_plexcached_backups', True):
            return

        import shutil

        old_array_path = get_array_direct_path(old_real_path)
        old_array_dir = os.path.dirname(old_array_path)
        old_identity = get_media_identity(old_real_path)
        old_plexcached = find_matching_plexcached(old_array_dir, old_identity, old_real_path)

        if old_plexcached and os.path.isfile(old_plexcached):
            # Create new backup FIRST if setting enabled (before deleting old)
            new_backup_ok = False
            if settings.get('backup_upgraded_files', True) and os.path.isfile(new_cache_path):
                new_array_path = get_array_direct_path(new_real_path)
                new_plexcached = new_array_path + '.plexcached'

                if not os.path.isfile(new_plexcached):
                    try:
                        new_array_dir = os.path.dirname(new_array_path)
                        os.makedirs(new_array_dir, exist_ok=True)
                        shutil.copy2(new_cache_path, new_plexcached)
                        src_size = os.path.getsize(new_cache_path)
                        dst_size = os.path.getsize(new_plexcached)
                        if src_size == dst_size:
                            logger.info(f"[UPGRADE] Created new backup: {os.path.basename(new_plexcached)} "
                                        f"({format_bytes(src_size)}, rk={rating_key})")
                            new_backup_ok = True
                        else:
                            logger.warning(f"[UPGRADE] Backup size mismatch for {os.path.basename(new_plexcached)}")
                            os.remove(new_plexcached)
                    except OSError as e:
                        logger.warning(f"[UPGRADE] Failed to create new backup: {e}")
                else:
                    # New backup already exists
                    new_backup_ok = True
            else:
                # No new backup needed — safe to delete old
                new_backup_ok = True

            # Only delete old backup after new one is confirmed (or not needed)
            if new_backup_ok:
                try:
                    os.remove(old_plexcached)
                    logger.info(f"[UPGRADE] Deleted outdated backup: {os.path.basename(old_plexcached)} (rk={rating_key})")
                except OSError as e:
                    logger.warning(f"[UPGRADE] Failed to delete old backup: {e}")
            else:
                logger.warning(f"[UPGRADE] Keeping old backup (new backup failed): {os.path.basename(old_plexcached)} (rk={rating_key})")

    def _remove_from_exclude_file(self, cache_path: str):
        """Remove a path from the exclude file"""
        settings = self._load_settings()
        remove_from_exclude_file(self.exclude_file, cache_path, settings.get('path_mappings', []))

    def _remove_from_timestamps(self, cache_path: str):
        """Remove a path from the timestamps file"""
        remove_from_timestamps_file(self.timestamps_file, cache_path)


# Singleton instance
_cache_service: Optional[CacheService] = None
_cache_service_lock = threading.Lock()


def get_cache_service() -> CacheService:
    """Get or create the cache service singleton"""
    global _cache_service
    if _cache_service is None:
        with _cache_service_lock:
            if _cache_service is None:
                _cache_service = CacheService()
    return _cache_service
