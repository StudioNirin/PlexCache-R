"""
File operations for PlexCache.
Handles file moving, filtering, subtitle operations, and path modifications.
"""

import os
import shutil
import logging
import threading
import json
import time
import tempfile
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from typing import List, Set, Optional, Tuple, Dict, TYPE_CHECKING, Callable
import re

from core.logging_config import get_console_lock
from core.system_utils import resolve_user0_to_disk, get_disk_free_space_bytes, get_disk_number_from_path, get_array_direct_path

if TYPE_CHECKING:
    from core.config import PathMapping

# Extension used to mark array files that have been cached
PLEXCACHED_EXTENSION = ".plexcached"

# Minimum free space (in bytes) required for metadata operations during rename
MINIMUM_SPACE_FOR_RENAME = 100 * 1024 * 1024  # 100 MB

# Subtitle file extensions (excluded from cross-type upgrade detection)
SUBTITLE_EXTENSIONS = {'.srt', '.sub', '.ass', '.ssa', '.vtt', '.idx', '.sbv'}


def save_json_atomically(filepath: str, data, label: str = "data") -> None:
    """Save JSON data to file atomically (write-to-temp-then-rename).

    Creates a temp file in the same directory, writes data, then atomically
    replaces the target file. This prevents corruption from interrupted writes.

    Args:
        filepath: Target file path.
        data: JSON-serializable data to write.
        label: Human-readable label for error messages.
    """
    try:
        dir_name = os.path.dirname(filepath) or '.'
        fd, tmp_path = tempfile.mkstemp(dir=dir_name, suffix='.tmp')
        try:
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2)
            os.replace(tmp_path, filepath)
        except BaseException:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise
    except IOError as e:
        logging.error(f"Could not save {label} file: {type(e).__name__}: {e}")


def is_subtitle_file(filepath: str) -> bool:
    """Check if a file is a subtitle based on its extension."""
    ext = os.path.splitext(filepath)[1].lower()
    return ext in SUBTITLE_EXTENSIONS


def format_bytes(bytes_value: int) -> str:
    """Format bytes into human-readable string (e.g., '1.5 GB').

    Canonical implementation — import from core.system_utils.
    Re-exported here for convenience.
    """
    from core.system_utils import format_bytes as _fb
    return _fb(bytes_value)


def format_duration(seconds: float) -> str:
    """Format seconds into human-readable duration like '1m 23s' or '45s'.

    Canonical implementation — import from core.system_utils.
    Re-exported here for convenience.
    """
    from core.system_utils import format_duration as _fd
    return _fd(seconds)


def get_media_identity(filepath: str) -> str:
    """Extract the core media identity from a filename, ignoring quality/codec info.

    This allows matching files that have been upgraded by Radarr/Sonarr.

    Examples:
        Movie: "Wreck-It Ralph (2012) [WEBDL-1080p].mkv" -> "Wreck-It Ralph (2012)"
        Movie: "Wreck-It Ralph (2012) [HEVC-1080p].mkv" -> "Wreck-It Ralph (2012)"
        TV: "From - S01E02 - The Way Things Are Now [HDTV-1080p].mkv" -> "From - S01E02 - The Way Things Are Now"

    Args:
        filepath: Full path or just filename

    Returns:
        The base media identity (title + year for movies, show + episode for TV)
    """
    filename = os.path.basename(filepath)
    # Strip .plexcached suffix first if present, then remove the media extension
    if filename.endswith('.plexcached'):
        filename = filename[:-len('.plexcached')]
    name = os.path.splitext(filename)[0]
    # Remove everything from first '[' onwards (quality/codec info)
    if '[' in name:
        name = name[:name.index('[')].strip()
    # Remove trailing ' -' or '-' if present (sometimes left over)
    name = name.rstrip(' -').rstrip('-').strip()
    return name


def find_matching_plexcached(array_path: str, media_identity: str, source_file: str) -> Optional[str]:
    """Find a .plexcached file in the array path that matches the media identity.

    This handles the case where Radarr/Sonarr upgraded a file - the .plexcached
    backup may have a different quality suffix but same core identity.

    Only matches files of the same type (video matches video, subtitle matches subtitle)
    to prevent cross-type false matches.

    Args:
        array_path: Directory path on the array to search
        media_identity: The core media identity to match (from get_media_identity)
        source_file: The file being cached/uncached (used to determine file type)

    Returns:
        Full path to matching .plexcached file, or None if not found
    """
    if not os.path.isdir(array_path):
        return None

    source_is_subtitle = is_subtitle_file(source_file)

    try:
        for entry in os.scandir(array_path):
            if entry.is_file() and entry.name.endswith(PLEXCACHED_EXTENSION):
                # Only match same file type (video<->video, subtitle<->subtitle)
                entry_original_name = entry.name.replace(PLEXCACHED_EXTENSION, '')
                entry_is_subtitle = is_subtitle_file(entry_original_name)
                if source_is_subtitle != entry_is_subtitle:
                    continue
                entry_identity = get_media_identity(entry.name)
                if entry_identity == media_identity:
                    return entry.path
    except (OSError, PermissionError) as e:
        logging.debug(f"Error scanning for .plexcached files in {array_path}: {e}")

    return None


class JSONTracker:
    """Base class for thread-safe JSON file trackers.

    Provides common functionality for loading, saving, and accessing
    JSON-based tracking data with thread safety.

    Subclasses should:
    - Call super().__init__(tracker_file, tracker_name) in their __init__
    - Override _post_load() for any migration or post-load processing
    - Use self._data dict for storage
    """

    def __init__(self, tracker_file: str, tracker_name: str = "tracker"):
        """Initialize the tracker.

        Args:
            tracker_file: Path to the JSON file storing tracker data.
            tracker_name: Human-readable name for logging (e.g., "watchlist", "OnDeck").
        """
        self.tracker_file = tracker_file
        self._tracker_name = tracker_name
        self._lock = threading.Lock()
        self._data: Dict[str, dict] = {}
        self._load()

    def _load(self) -> None:
        """Load tracker data from file."""
        try:
            if os.path.exists(self.tracker_file):
                with open(self.tracker_file, 'r', encoding='utf-8') as f:
                    self._data = json.load(f)
                self._post_load()
                logging.debug(f"Loaded {len(self._data)} {self._tracker_name} entries from {self.tracker_file}")
        except (json.JSONDecodeError, IOError) as e:
            logging.warning(f"Could not load {self._tracker_name} file: {type(e).__name__}: {e}")
            self._data = {}

    def _post_load(self) -> None:
        """Hook for subclasses to perform post-load processing (e.g., migration)."""
        pass

    def _save(self) -> None:
        """Save tracker data to file atomically (write-to-temp-then-rename)."""
        save_json_atomically(self.tracker_file, self._data, self._tracker_name)

    def _find_entry_by_filename(self, file_path: str) -> Optional[Tuple[str, dict]]:
        """Find a tracker entry by matching filename when full path doesn't match.

        This handles cases where the cache file has modified paths (/mnt/cache_downloads/...)
        but the tracker stores original paths (/mnt/user/...).

        Args:
            file_path: The file path to search for.

        Returns:
            Tuple of (matched_path, entry) if found, None otherwise.
        """
        target_filename = os.path.basename(file_path)
        for stored_path, entry in self._data.items():
            if os.path.basename(stored_path) == target_filename:
                return (stored_path, entry)
        return None

    def get_entry(self, file_path: str) -> Optional[dict]:
        """Get the tracker entry for a file.

        Args:
            file_path: The path to the media file.

        Returns:
            The entry dict or None if not found.
        """
        with self._lock:
            if file_path in self._data:
                return self._data[file_path]
            result = self._find_entry_by_filename(file_path)
            if result:
                return result[1]
            return None

    def remove_entry(self, file_path: str) -> None:
        """Remove a file's tracker entry.

        Args:
            file_path: The path to the file.
        """
        with self._lock:
            if file_path in self._data:
                del self._data[file_path]
                self._save()
                logging.debug(f"Removed {self._tracker_name} entry for: {file_path}")

    def cleanup_stale_entries(self, max_days_since_seen: int = 7) -> int:
        """Remove entries that haven't been seen recently.

        Args:
            max_days_since_seen: Remove entries not seen in this many days.

        Returns:
            Number of entries removed.
        """
        with self._lock:
            stale = []
            now = datetime.now()
            for path, entry in self._data.items():
                last_seen_str = entry.get('last_seen')
                if last_seen_str:
                    try:
                        last_seen = datetime.fromisoformat(last_seen_str)
                        days_since = (now - last_seen).total_seconds() / 86400
                        if days_since > max_days_since_seen:
                            stale.append(path)
                    except ValueError:
                        stale.append(path)
                else:
                    # No last_seen field - keep if it has other valid timestamps
                    if not entry.get('watchlisted_at') and not entry.get('cached_at'):
                        stale.append(path)

            for path in stale:
                del self._data[path]

            if stale:
                self._save()
                logging.info(f"Cleaned up {len(stale)} stale {self._tracker_name} entries")

            return len(stale)

    def mark_cached(self, file_path: str, source: str, cached_at: Optional[str] = None) -> None:
        """Mark an entry as cached with source and timestamp.

        Sets is_cached=True, cache_source, and cached_at on the matching entry.
        Uses filename fallback if full path doesn't match directly.
        No-op if entry doesn't exist (file may not be on OnDeck/Watchlist).

        Args:
            file_path: Path to the media file.
            source: Cache source (e.g., "ondeck", "watchlist", "pre-existing").
            cached_at: ISO timestamp string. Defaults to now.
        """
        with self._lock:
            entry = None
            key = file_path
            if file_path in self._data:
                entry = self._data[file_path]
            else:
                result = self._find_entry_by_filename(file_path)
                if result:
                    key, entry = result

            if entry is None:
                return

            entry['is_cached'] = True
            entry['cache_source'] = source
            entry['cached_at'] = cached_at or datetime.now().isoformat()
            self._save()
            logging.debug(f"Marked {self._tracker_name} entry as cached: {os.path.basename(key)} (source={source})")

    def mark_uncached(self, file_path: str) -> None:
        """Clear cache status from an entry.

        Sets is_cached=False and removes cache_source and cached_at.
        No-op if entry doesn't exist.

        Args:
            file_path: Path to the media file.
        """
        with self._lock:
            entry = None
            key = file_path
            if file_path in self._data:
                entry = self._data[file_path]
            else:
                result = self._find_entry_by_filename(file_path)
                if result:
                    key, entry = result

            if entry is None:
                return

            entry['is_cached'] = False
            entry.pop('cache_source', None)
            entry.pop('cached_at', None)
            self._save()
            logging.debug(f"Marked {self._tracker_name} entry as uncached: {os.path.basename(key)}")

    def get_cached_entries(self) -> Dict[str, dict]:
        """Return entries that are currently marked as cached.

        Returns:
            Dict of {path: entry} for entries where is_cached is True.
        """
        with self._lock:
            return {
                path: dict(entry)
                for path, entry in self._data.items()
                if entry.get('is_cached', False)
            }


class CacheTimestampTracker:
    """Thread-safe tracker for when files were cached and their source.

    Maintains a JSON file with timestamps and source info for cached files.
    Used to implement cache retention periods - files cached less than X hours ago
    won't be moved back to array even if they're no longer in OnDeck/watchlist.

    Storage format:
    {
        "/path/to/file.mkv": {
            "cached_at": "2025-12-02T14:26:27.156439",
            "source": "ondeck"  # or "watchlist"
        }
    }

    Backwards compatible with old format (plain timestamp string).
    """

    def __init__(self, timestamp_file: str):
        """Initialize the tracker with the path to the timestamp file.

        Args:
            timestamp_file: Path to the JSON file storing timestamps.
        """
        self.timestamp_file = timestamp_file
        self._lock = threading.Lock()
        self._timestamps: Dict[str, dict] = {}
        self._subtitle_to_parent: Dict[str, str] = {}  # reverse index: subtitle path -> parent video path
        self._load()

    def _load(self) -> None:
        """Load timestamps from file, migrating old format if needed."""
        try:
            if os.path.exists(self.timestamp_file):
                with open(self.timestamp_file, 'r', encoding='utf-8') as f:
                    raw_data = json.load(f)

                # Migrate old format (plain string) to new format (dict)
                migrated = False
                for path, value in raw_data.items():
                    if isinstance(value, str):
                        # Old format: just a timestamp string
                        self._timestamps[path] = {
                            "cached_at": value,
                            "source": "unknown"  # Can't determine source for old entries
                        }
                        migrated = True
                    elif isinstance(value, dict):
                        # New format: dict with cached_at and source
                        self._timestamps[path] = value
                    else:
                        logging.warning(f"Invalid timestamp entry for {path}: {value}")

                if migrated:
                    self._save()
                    logging.info("Migrated timestamp file to new format with source tracking")

                # Build reverse index from existing subtitle associations
                self._build_subtitle_reverse_index()

                # Migrate standalone subtitle entries to parent associations
                self._migrate_standalone_subtitles()

                logging.debug(f"Loaded {len(self._timestamps)} timestamps from {self.timestamp_file}")
        except (json.JSONDecodeError, IOError) as e:
            logging.warning(f"Could not load timestamp file: {type(e).__name__}: {e}")
            self._timestamps = {}

    def _save(self) -> None:
        """Save timestamps to file atomically (write-to-temp-then-rename)."""
        save_json_atomically(self.timestamp_file, self._timestamps, "timestamp")

    def record_cache_time(self, cache_file_path: str, source: str = "unknown",
                          original_inode: Optional[int] = None,
                          media_type: Optional[str] = None,
                          episode_info: Optional[Dict] = None) -> None:
        """Record the current time and source when a file was cached.

        Only records if no entry exists - never overwrites existing timestamps.

        Args:
            cache_file_path: The path to the cached file.
            source: Where the file came from - "ondeck", "watchlist", "pre-existing", or "unknown".
            original_inode: For hard-linked files, the original inode number for restoration.
            media_type: Plex media type - "episode" or "movie" (None for legacy/unknown).
            episode_info: For episodes, dict with 'show', 'season', 'episode' keys.
        """
        with self._lock:
            # Never overwrite existing timestamps - file was cached when it was first recorded
            if cache_file_path in self._timestamps:
                logging.debug(f"Timestamp already exists for: {cache_file_path}")
                return

            entry = {
                "cached_at": datetime.now().isoformat(),
                "source": source
            }
            if original_inode is not None:
                entry["original_inode"] = original_inode
            if media_type is not None:
                entry["media_type"] = media_type
            if episode_info is not None:
                entry["episode_info"] = episode_info
            self._timestamps[cache_file_path] = entry
            self._save()
            logging.debug(f"Recorded cache timestamp for: {cache_file_path} (source: {source})")

    def remove_entry(self, cache_file_path: str) -> None:
        """Remove a file's timestamp entry (when file is restored to array).

        If removing a parent video, also clears its subtitles from the reverse index.
        If removing a subtitle, also removes it from the parent's subtitles list.

        Args:
            cache_file_path: The path to the cached file.
        """
        with self._lock:
            if cache_file_path in self._timestamps:
                # If this is a parent with subtitles, clear reverse index entries
                entry = self._timestamps[cache_file_path]
                if isinstance(entry, dict) and "subtitles" in entry:
                    for sub_path in entry["subtitles"]:
                        self._subtitle_to_parent.pop(sub_path, None)
                del self._timestamps[cache_file_path]
                self._save()
                logging.debug(f"Removed cache timestamp for: {cache_file_path}")
            elif cache_file_path in self._subtitle_to_parent:
                # This is a subtitle — remove from parent's list and reverse index
                parent_path = self._subtitle_to_parent.pop(cache_file_path)
                parent_entry = self._timestamps.get(parent_path)
                if parent_entry and isinstance(parent_entry, dict) and "subtitles" in parent_entry:
                    try:
                        parent_entry["subtitles"].remove(cache_file_path)
                    except ValueError:
                        pass
                    if not parent_entry["subtitles"]:
                        del parent_entry["subtitles"]
                self._save()
                logging.debug(f"Removed subtitle entry for: {cache_file_path}")

    def get_original_inode(self, cache_file_path: str) -> Optional[int]:
        """Get the original inode for a hard-linked file (for restoration).

        Args:
            cache_file_path: The path to the cached file.

        Returns:
            The original inode number if this was a hard-linked file, None otherwise.
        """
        with self._lock:
            entry = self._timestamps.get(cache_file_path)
            if entry and isinstance(entry, dict):
                return entry.get("original_inode")
            return None

    def is_within_retention_period(self, cache_file_path: str, retention_hours: int) -> bool:
        """Check if a file is still within its cache retention period.

        For subtitles associated with a parent video, delegates to the parent's entry.

        Args:
            cache_file_path: The path to the cached file.
            retention_hours: How many hours files should stay on cache.

        Returns:
            True if the file was cached less than retention_hours ago, False otherwise.
            Returns False if no timestamp exists (file should be allowed to move).
        """
        with self._lock:
            if cache_file_path not in self._timestamps:
                # Check if this is a subtitle with a parent
                parent = self._subtitle_to_parent.get(cache_file_path)
                if parent and parent in self._timestamps:
                    cache_file_path = parent
                else:
                    # No timestamp means we don't know when it was cached
                    # Default to allowing the move
                    return False

            try:
                entry = self._timestamps[cache_file_path]
                # Handle both old format (string) and new format (dict)
                if isinstance(entry, str):
                    cached_time_str = entry
                else:
                    cached_time_str = entry.get("cached_at", "")

                if not cached_time_str:
                    return False

                cached_time = datetime.fromisoformat(cached_time_str)
                age_hours = (datetime.now() - cached_time).total_seconds() / 3600

                if age_hours < retention_hours:
                    logging.debug(
                        f"File still within retention period ({age_hours:.1f}h < {retention_hours}h): "
                        f"{cache_file_path}"
                    )
                    return True
                else:
                    logging.debug(
                        f"File retention period expired ({age_hours:.1f}h >= {retention_hours}h): "
                        f"{cache_file_path}"
                    )
                    return False
            except (ValueError, TypeError) as e:
                logging.warning(f"Invalid timestamp for {cache_file_path}: {e}")
                return False

    def get_retention_remaining(self, cache_file_path: str, retention_hours: int) -> float:
        """Get hours remaining in retention period for a cached file.

        For subtitles associated with a parent video, delegates to the parent's entry.

        Args:
            cache_file_path: The path to the cached file.
            retention_hours: The configured retention period in hours.

        Returns:
            Hours remaining (positive if within retention, 0 or negative if expired).
            Returns 0 if no timestamp exists.
        """
        with self._lock:
            if cache_file_path not in self._timestamps:
                parent = self._subtitle_to_parent.get(cache_file_path)
                if parent and parent in self._timestamps:
                    cache_file_path = parent
                else:
                    return 0

            try:
                entry = self._timestamps[cache_file_path]
                if isinstance(entry, str):
                    cached_time_str = entry
                else:
                    cached_time_str = entry.get("cached_at", "")

                if not cached_time_str:
                    return 0

                cached_time = datetime.fromisoformat(cached_time_str)
                age_hours = (datetime.now() - cached_time).total_seconds() / 3600
                return retention_hours - age_hours
            except (ValueError, TypeError):
                return 0

    def get_source(self, cache_file_path: str) -> str:
        """Get the source (ondeck/watchlist) for a cached file.

        For subtitles associated with a parent video, delegates to the parent's entry.

        Args:
            cache_file_path: The path to the cached file.

        Returns:
            The source string ("ondeck", "watchlist", or "unknown").
        """
        with self._lock:
            if cache_file_path not in self._timestamps:
                parent = self._subtitle_to_parent.get(cache_file_path)
                if parent and parent in self._timestamps:
                    cache_file_path = parent
                else:
                    return "unknown"
            entry = self._timestamps[cache_file_path]
            if isinstance(entry, dict):
                return entry.get("source", "unknown")
            return "unknown"

    def get_media_type(self, cache_file_path: str) -> Optional[str]:
        """Get the Plex media type for a cached file.

        For subtitles associated with a parent video, delegates to the parent's entry.

        Args:
            cache_file_path: The path to the cached file.

        Returns:
            "episode", "movie", or None if not stored.
        """
        with self._lock:
            entry = self._timestamps.get(cache_file_path)
            if not entry:
                parent = self._subtitle_to_parent.get(cache_file_path)
                if parent:
                    entry = self._timestamps.get(parent)
            if entry and isinstance(entry, dict):
                return entry.get("media_type")
            return None

    def get_episode_info(self, cache_file_path: str) -> Optional[Dict]:
        """Get episode info for a cached file.

        For subtitles associated with a parent video, delegates to the parent's entry.

        Args:
            cache_file_path: The path to the cached file.

        Returns:
            Dict with 'show', 'season', 'episode' keys, or None if not stored.
        """
        with self._lock:
            entry = self._timestamps.get(cache_file_path)
            if not entry:
                parent = self._subtitle_to_parent.get(cache_file_path)
                if parent:
                    entry = self._timestamps.get(parent)
            if entry and isinstance(entry, dict):
                return entry.get("episode_info")
            return None

    def associate_subtitles(self, subtitle_map: Dict[str, List[str]]) -> None:
        """Bulk-link subtitle files to their parent video entries.

        For each (video, [subtitles]) pair:
        - Adds a "subtitles" list to the parent's timestamp entry
        - Removes any standalone subtitle entries from _timestamps
        - Updates the reverse index

        Args:
            subtitle_map: Dict mapping parent video cache paths to lists of subtitle cache paths.
        """
        with self._lock:
            changed = False
            for parent_path, sub_paths in subtitle_map.items():
                if not sub_paths:
                    continue
                parent_entry = self._timestamps.get(parent_path)
                if parent_entry is None or not isinstance(parent_entry, dict):
                    # Parent not tracked — leave subtitles as standalone
                    continue

                existing_subs = set(parent_entry.get("subtitles", []))
                for sub_path in sub_paths:
                    if sub_path not in existing_subs:
                        existing_subs.add(sub_path)
                        changed = True
                    # Remove standalone subtitle entry if it exists
                    if sub_path in self._timestamps:
                        del self._timestamps[sub_path]
                        changed = True
                    # Update reverse index
                    self._subtitle_to_parent[sub_path] = parent_path

                parent_entry["subtitles"] = sorted(existing_subs)

            if changed:
                self._save()
                logging.debug(f"Associated subtitles for {len(subtitle_map)} parent videos")

    def get_subtitles(self, parent_path: str) -> List[str]:
        """Get the list of subtitle files associated with a parent video.

        Args:
            parent_path: Cache path of the parent video file.

        Returns:
            List of subtitle cache paths, or empty list if none.
        """
        with self._lock:
            entry = self._timestamps.get(parent_path)
            if entry and isinstance(entry, dict):
                return list(entry.get("subtitles", []))
            return []

    def find_parent_video(self, subtitle_path: str) -> Optional[str]:
        """Find the parent video for a subtitle file via the reverse index.

        Args:
            subtitle_path: Cache path of the subtitle file.

        Returns:
            Cache path of the parent video, or None if not associated.
        """
        with self._lock:
            return self._subtitle_to_parent.get(subtitle_path)

    def _build_subtitle_reverse_index(self) -> None:
        """Build _subtitle_to_parent from existing subtitle lists in entries.

        Called during _load() — no lock needed (called within __init__).
        """
        self._subtitle_to_parent.clear()
        for parent_path, entry in self._timestamps.items():
            if isinstance(entry, dict) and "subtitles" in entry:
                for sub_path in entry["subtitles"]:
                    self._subtitle_to_parent[sub_path] = parent_path

    def _migrate_standalone_subtitles(self) -> None:
        """One-time migration: move standalone subtitle entries to parent associations.

        Scans all entries for subtitle files not already in _subtitle_to_parent.
        Derives the parent video path and links them if the parent exists.
        Called during _load() — no lock needed (called within __init__).
        """
        standalone_subs = []
        for path in list(self._timestamps.keys()):
            if is_subtitle_file(path) and path not in self._subtitle_to_parent:
                standalone_subs.append(path)

        if not standalone_subs:
            return

        migrated_count = 0
        for sub_path in standalone_subs:
            parent_path = self._derive_parent_video_path(sub_path)
            if parent_path and parent_path in self._timestamps:
                # Link to parent
                parent_entry = self._timestamps[parent_path]
                if isinstance(parent_entry, dict):
                    subs = parent_entry.get("subtitles", [])
                    if sub_path not in subs:
                        subs.append(sub_path)
                    parent_entry["subtitles"] = sorted(subs)
                    self._subtitle_to_parent[sub_path] = parent_path
                    del self._timestamps[sub_path]
                    migrated_count += 1

        if migrated_count:
            self._save()
            logging.info(f"Migrated {migrated_count} subtitle entries to parent video associations")

    @staticmethod
    def _derive_parent_video_path(subtitle_path: str) -> Optional[str]:
        """Derive the parent video path from a subtitle path.

        Strips subtitle extension and optional language code to get the base name,
        then checks for common video extensions in the same directory.

        Args:
            subtitle_path: Path to the subtitle file.

        Returns:
            Path to the likely parent video file, or None if not determinable.
        """
        directory = os.path.dirname(subtitle_path)
        filename = os.path.basename(subtitle_path)
        lower_name = filename.lower()

        # Strip subtitle extension
        for ext in SUBTITLE_EXTENSIONS:
            if lower_name.endswith(ext):
                filename = filename[:-len(ext)]
                lower_name = lower_name[:-len(ext)]
                break
        else:
            return None  # Not a subtitle file

        # Strip optional language code (e.g., .en, .es, .pt-br, .zh-hans)
        lang_pattern = r'\.[a-z]{2,3}(-[a-z]{2,4})?$'
        match = re.search(lang_pattern, lower_name, re.IGNORECASE)
        if match:
            filename = filename[:match.start()]

        # Try common video extensions
        video_extensions = ['.mkv', '.mp4', '.avi', '.m4v', '.wmv', '.flv', '.mov', '.ts']
        for vext in video_extensions:
            candidate = os.path.join(directory, filename + vext)
            if os.path.exists(candidate):
                return candidate

        return None

    def enrich_media_info(self, cache_file_path: str, media_type: Optional[str] = None,
                          episode_info: Optional[Dict] = None) -> None:
        """Enrich an existing entry with media type metadata.

        Only updates fields that are currently None/missing. Used to backfill
        metadata on pre-existing cached files when they appear in OnDeck/Watchlist.
        Does nothing if the entry doesn't exist or already has media_type set.

        Args:
            cache_file_path: The path to the cached file.
            media_type: "episode" or "movie".
            episode_info: For episodes, dict with 'show', 'season', 'episode' keys.
        """
        if media_type is None:
            return
        with self._lock:
            entry = self._timestamps.get(cache_file_path)
            if entry is None or not isinstance(entry, dict):
                return
            if entry.get("media_type") is not None:
                return  # Already has metadata, don't overwrite
            entry["media_type"] = media_type
            if episode_info is not None:
                entry["episode_info"] = episode_info
            self._save()
            logging.debug(f"Enriched media info for: {cache_file_path} (type: {media_type})")

    def cleanup_missing_files(self) -> int:
        """Remove entries for files that no longer exist on cache.

        Also prunes missing subtitle files from parent entries' subtitle lists
        and updates the reverse index.

        Returns:
            Number of entries removed.
        """
        with self._lock:
            missing = [path for path in self._timestamps if not os.path.exists(path)]
            for path in missing:
                # If parent with subtitles, clear reverse index
                entry = self._timestamps[path]
                if isinstance(entry, dict) and "subtitles" in entry:
                    for sub_path in entry["subtitles"]:
                        self._subtitle_to_parent.pop(sub_path, None)
                del self._timestamps[path]

            # Prune missing subtitle files from remaining parent entries
            missing_subs = 0
            for path, entry in self._timestamps.items():
                if isinstance(entry, dict) and "subtitles" in entry:
                    original_count = len(entry["subtitles"])
                    entry["subtitles"] = [s for s in entry["subtitles"] if os.path.exists(s)]
                    removed_count = original_count - len(entry["subtitles"])
                    if removed_count > 0:
                        missing_subs += removed_count
                        # Update reverse index
                        for sub_path in list(self._subtitle_to_parent):
                            if self._subtitle_to_parent[sub_path] == path and sub_path not in entry["subtitles"]:
                                del self._subtitle_to_parent[sub_path]
                    if not entry["subtitles"]:
                        del entry["subtitles"]

            total_removed = len(missing) + missing_subs
            if total_removed:
                self._save()
                if missing_subs:
                    logging.info(f"Cleaned up {len(missing)} stale timestamp entries and {missing_subs} missing subtitle references")
                else:
                    logging.info(f"Cleaned up {len(missing)} stale timestamp entries")
            return total_removed


class WatchlistTracker(JSONTracker):
    """Thread-safe tracker for watchlist retention.

    Tracks when files were added to watchlists and by which users.
    Used to implement watchlist retention - files auto-expire X days after
    being added to a watchlist, even if still on the watchlist.

    Storage format:
    {
        "/path/to/file.mkv": {
            "watchlisted_at": "2025-12-02T14:26:27.156439",
            "users": ["Brandon", "Home"],
            "last_seen": "2025-12-03T10:00:00.000000"
        }
    }
    """

    def __init__(self, tracker_file: str):
        """Initialize the tracker with the path to the tracker file.

        Args:
            tracker_file: Path to the JSON file storing watchlist data.
        """
        super().__init__(tracker_file, "watchlist")

    def update_entry(self, file_path: str, username: str, watchlisted_at: Optional[datetime]) -> None:
        """Update or create an entry for a watchlist item.

        If the item already exists and the new watchlisted_at is more recent,
        update the timestamp (this extends retention when another user adds it).

        Args:
            file_path: The path to the media file.
            username: The user who has this on their watchlist.
            watchlisted_at: When the user added it to their watchlist (from Plex API).
        """
        with self._lock:
            now_iso = datetime.now().isoformat()

            if file_path in self._data:
                entry = self._data[file_path]
                # Add user if not already in list
                if username not in entry.get('users', []):
                    entry.setdefault('users', []).append(username)

                # Update watchlisted_at if the new timestamp is more recent
                if watchlisted_at:
                    # Normalize to naive datetime for comparison (strip timezone info)
                    new_ts_naive = watchlisted_at.replace(tzinfo=None) if watchlisted_at.tzinfo else watchlisted_at
                    new_ts_iso = new_ts_naive.isoformat()
                    existing_ts = entry.get('watchlisted_at')
                    if existing_ts:
                        try:
                            existing_dt = datetime.fromisoformat(existing_ts)
                            # Also strip timezone from existing if present
                            existing_dt_naive = existing_dt.replace(tzinfo=None) if existing_dt.tzinfo else existing_dt
                            if new_ts_naive > existing_dt_naive:
                                entry['watchlisted_at'] = new_ts_iso
                                logging.debug(f"[USER:{username}] Updated watchlist timestamp: {file_path}")
                        except ValueError:
                            entry['watchlisted_at'] = new_ts_iso
                    else:
                        entry['watchlisted_at'] = new_ts_iso

                # Always update last_seen
                entry['last_seen'] = now_iso
            else:
                # New entry - normalize timezone-aware datetimes to naive
                if watchlisted_at:
                    watchlisted_at_naive = watchlisted_at.replace(tzinfo=None) if watchlisted_at.tzinfo else watchlisted_at
                    watchlisted_at_iso = watchlisted_at_naive.isoformat()
                else:
                    watchlisted_at_iso = now_iso
                self._data[file_path] = {
                    'watchlisted_at': watchlisted_at_iso,
                    'users': [username],
                    'last_seen': now_iso
                }
                logging.debug(f"[USER:{username}] Added new watchlist entry: {file_path}")

            self._save()

    def is_expired(self, file_path: str, retention_days: int) -> bool:
        """Check if a watchlist item has expired based on retention period.

        Args:
            file_path: The path to the media file.
            retention_days: Number of days before expiry.

        Returns:
            True if the item was added more than retention_days ago, False otherwise.
            Returns False if no entry exists (conservative - don't expire unknown items).
        """
        if retention_days <= 0:
            # Retention disabled
            return False

        with self._lock:
            entry = None
            matched_path = file_path

            if file_path in self._data:
                entry = self._data[file_path]
            else:
                # Try to find by filename (handles path prefix mismatches)
                result = self._find_entry_by_filename(file_path)
                if result:
                    matched_path, entry = result

            if entry is None:
                # No entry found - conservative, don't expire
                return False
            watchlisted_at_str = entry.get('watchlisted_at')
            if not watchlisted_at_str:
                return False

            try:
                watchlisted_at = datetime.fromisoformat(watchlisted_at_str)
                age_days = (datetime.now() - watchlisted_at).total_seconds() / 86400

                if age_days > retention_days:
                    users = entry.get('users', ['unknown'])
                    filename = os.path.basename(file_path)
                    for user in users:
                        logging.debug(
                            f"[USER:{user}] Watchlist retention expired ({age_days:.1f} days > {retention_days} days): {filename}"
                        )
                    return True
                return False
            except (ValueError, TypeError) as e:
                logging.warning(f"Invalid watchlisted_at timestamp for {file_path}: {e}")
                return False

    def cleanup_missing_files(self) -> int:
        """Remove entries for files that no longer exist.

        Note: Currently disabled because tracker stores Plex paths (/data/...)
        which are internal to the Plex Docker container and don't map directly
        to filesystem paths. The cleanup_stale_entries() method handles cleanup
        based on last_seen timestamp instead.

        Returns:
            Number of entries removed (always 0 for now).
        """
        # Disabled: Plex paths are internal to Docker, not filesystem paths
        # Cleanup is handled by cleanup_stale_entries() based on last_seen
        return 0


class OnDeckTracker(JSONTracker):
    """Thread-safe tracker for OnDeck items and their users.

    Tracks which users have each file OnDeck, similar to WatchlistTracker.
    Used for priority scoring - items OnDeck for multiple users have higher priority.
    Also tracks episode position info for TV shows to enable episode position awareness.

    Storage format:
    {
        "/path/to/file.mkv": {
            "users": ["Brandon", "Home"],
            "first_seen": "2025-12-01T10:00:00.000000",
            "last_seen": "2025-12-03T10:00:00.000000",
            "user_first_seen": {
                "Brandon": "2025-12-01T10:00:00.000000",
                "Home": "2025-12-03T10:00:00.000000"
            },
            "episode_info": {
                "show": "Foundation",
                "season": 2,
                "episode": 5,
                "is_current_ondeck": true
            },
            "ondeck_users": ["Brandon"]
        }
    }

    Fields:
    - users: All users who have this file in their OnDeck queue (current or prefetched)
    - first_seen: When item was first added to OnDeck (for staleness calculation)
    - last_seen: When item was last seen during a scan
    - user_first_seen: Per-user first_seen timestamps (for per-user retention expiry)
    - episode_info: For TV episodes, contains show/season/episode and whether this is
                   the actual OnDeck episode vs a prefetched next episode
    - ondeck_users: Users for whom this is the CURRENT OnDeck episode (not prefetched)
    """

    def __init__(self, tracker_file: str):
        """Initialize the tracker with the path to the tracker file.

        Args:
            tracker_file: Path to the JSON file storing OnDeck data.
        """
        super().__init__(tracker_file, "OnDeck")

    def update_entry(self, file_path: str, username: str,
                     episode_info: Optional[Dict[str, any]] = None,
                     is_current_ondeck: bool = False) -> None:
        """Update or create an entry for an OnDeck item.

        Args:
            file_path: The path to the media file.
            username: The user who has this on their OnDeck.
            episode_info: For TV episodes, dict with 'show', 'season', 'episode' keys.
            is_current_ondeck: True if this is the actual OnDeck episode (not prefetched next).
        """
        with self._lock:
            now_iso = datetime.now().isoformat()

            # Track that this entry was seen this run (for cleanup_unseen)
            if hasattr(self, '_seen_this_run'):
                self._seen_this_run.add(file_path)

            if file_path in self._data:
                entry = self._data[file_path]
                # Add user if not already in list
                if username not in entry.get('users', []):
                    entry.setdefault('users', []).append(username)
                # Always update last_seen
                entry['last_seen'] = now_iso
                # Backfill first_seen for existing entries (migration)
                if 'first_seen' not in entry:
                    entry['first_seen'] = now_iso
                # Per-user first_seen (for per-user retention expiry)
                ufs = entry.setdefault('user_first_seen', {})
                if username not in ufs:
                    ufs[username] = now_iso

                # Track ondeck_users separately (users for whom this is current ondeck)
                if is_current_ondeck:
                    if username not in entry.get('ondeck_users', []):
                        entry.setdefault('ondeck_users', []).append(username)

                # Update episode_info if provided and not already set, or update is_current_ondeck
                if episode_info:
                    if 'episode_info' not in entry:
                        entry['episode_info'] = {
                            'show': episode_info.get('show'),
                            'season': episode_info.get('season'),
                            'episode': episode_info.get('episode'),
                            'is_current_ondeck': is_current_ondeck
                        }
                    elif is_current_ondeck and not entry['episode_info'].get('is_current_ondeck'):
                        # Upgrade to current ondeck if it was previously just prefetched
                        entry['episode_info']['is_current_ondeck'] = True
            else:
                # New entry
                new_entry = {
                    'users': [username],
                    'first_seen': now_iso,
                    'last_seen': now_iso,
                    'user_first_seen': {username: now_iso}
                }
                if is_current_ondeck:
                    new_entry['ondeck_users'] = [username]
                if episode_info:
                    new_entry['episode_info'] = {
                        'show': episode_info.get('show'),
                        'season': episode_info.get('season'),
                        'episode': episode_info.get('episode'),
                        'is_current_ondeck': is_current_ondeck
                    }
                self._data[file_path] = new_entry
                logging.debug(f"[USER:{username}] Added new OnDeck entry: {file_path}")

            self._save()

    def get_user_count(self, file_path: str) -> int:
        """Get the number of users who have this file OnDeck.

        Args:
            file_path: The path to the media file.

        Returns:
            Number of users, or 0 if not found.
        """
        entry = self.get_entry(file_path)
        if entry:
            return len(entry.get('users', []))
        return 0

    def get_episode_info(self, file_path: str) -> Optional[Dict[str, any]]:
        """Get episode info for a file.

        Args:
            file_path: The path to the media file.

        Returns:
            Episode info dict with 'show', 'season', 'episode', 'is_current_ondeck' keys,
            or None if not a TV episode or no info available.
        """
        entry = self.get_entry(file_path)
        if entry:
            return entry.get('episode_info')
        return None

    def get_ondeck_positions_for_show(self, show_name: str) -> List[Tuple[int, int]]:
        """Get all current OnDeck positions for a show.

        Finds all entries for the given show that are marked as current OnDeck
        (not prefetched), and returns their season/episode positions.

        Args:
            show_name: The show name to look up (case-insensitive).

        Returns:
            List of (season, episode) tuples for current OnDeck positions.
        """
        with self._lock:
            positions = []
            show_lower = show_name.lower()
            for path, entry in self._data.items():
                ep_info = entry.get('episode_info')
                if ep_info and ep_info.get('is_current_ondeck'):
                    entry_show = ep_info.get('show', '').lower()
                    if entry_show == show_lower:
                        season = ep_info.get('season')
                        episode = ep_info.get('episode')
                        if season is not None and episode is not None:
                            positions.append((season, episode))
            return positions

    def get_earliest_ondeck_position(self, show_name: str) -> Optional[Tuple[int, int]]:
        """Get the earliest (furthest behind) OnDeck position for a show.

        Useful for determining how many episodes a file is "ahead" of the
        user who is furthest behind in the show.

        Args:
            show_name: The show name to look up (case-insensitive).

        Returns:
            Tuple of (season, episode) for the earliest OnDeck position,
            or None if no OnDeck entries for this show.
        """
        positions = self.get_ondeck_positions_for_show(show_name)
        if not positions:
            return None
        # Sort by (season, episode) and return the earliest
        positions.sort()
        return positions[0]

    def prepare_for_run(self) -> None:
        """Prepare tracker for a new run while preserving first_seen timestamps.

        Clears per-run fields (users, ondeck_users, episode_info) on all entries
        so they can be repopulated by update_entry() calls. Initializes the
        _seen_this_run set to track which entries are refreshed this run.

        Unlike the old clear_for_run(), this does NOT delete entries — first_seen
        timestamps are preserved so OnDeck retention can accumulate correctly.
        """
        with self._lock:
            self._seen_this_run = set()
            for file_path, entry in self._data.items():
                entry['users'] = []
                entry['ondeck_users'] = []
                entry.pop('episode_info', None)
            # Don't save yet — update_entry() calls will save as entries are refreshed
            logging.debug("Prepared OnDeck tracker for new run (preserved first_seen timestamps)")

    def cleanup_stale_entries(self, max_days_since_seen: int = 1) -> int:
        """Remove entries that haven't been seen recently.

        OnDeck items change frequently, so we use a shorter retention than watchlist.

        Args:
            max_days_since_seen: Remove entries not seen in this many days.

        Returns:
            Number of entries removed.
        """
        with self._lock:
            stale = []
            now = datetime.now()
            for path, entry in self._data.items():
                last_seen_str = entry.get('last_seen')
                if last_seen_str:
                    try:
                        last_seen = datetime.fromisoformat(last_seen_str)
                        days_since = (now - last_seen).total_seconds() / 86400
                        if days_since > max_days_since_seen:
                            stale.append(path)
                    except ValueError:
                        stale.append(path)
                else:
                    stale.append(path)

            for path in stale:
                del self._data[path]

            if stale:
                self._save()
                logging.debug(f"Cleaned up {len(stale)} stale OnDeck tracker entries")

            return len(stale)

    def cleanup_unseen(self) -> int:
        """Remove entries not seen during the current run.

        Called after all update_entry() calls to remove items that fell off
        OnDeck naturally (no longer reported by Plex for any user).
        Also trims user_first_seen on surviving entries to only include
        current users.

        Returns:
            Number of entries removed.
        """
        with self._lock:
            seen = getattr(self, '_seen_this_run', None)
            if seen is None:
                # prepare_for_run() wasn't called, skip cleanup
                return 0

            unseen = [path for path in self._data if path not in seen]
            for path in unseen:
                del self._data[path]

            # Trim user_first_seen on surviving entries to only include current users
            for path, entry in self._data.items():
                ufs = entry.get('user_first_seen')
                if ufs:
                    current_users = set(entry.get('users', []))
                    stale_users = [u for u in ufs if u not in current_users]
                    for u in stale_users:
                        del ufs[u]

            if unseen:
                self._save()
                logging.debug(f"Removed {len(unseen)} OnDeck entries no longer on any user's OnDeck")

            return len(unseen)

    def is_expired(self, file_path: str, retention_days: float) -> bool:
        """Check if an OnDeck item has expired based on per-user retention.

        An item only expires when ALL current users have exceeded the retention
        period. If ANY user is still within retention, the item stays protected.

        Args:
            file_path: The path to the media file.
            retention_days: Number of days before expiry. 0 = disabled.

        Returns:
            True if all current users have exceeded retention_days.
            Returns False if disabled, no entry exists, no users, or any
            user is still within retention.
        """
        if retention_days <= 0:
            return False

        with self._lock:
            entry = self._data.get(file_path)
            if entry is None:
                return False

            now = datetime.now()
            current_users = entry.get('users', [])
            user_first_seen = entry.get('user_first_seen', {})

            if not current_users:
                return False  # No users = conservative, don't expire

            filename = os.path.basename(file_path)
            max_age = 0.0

            for user in current_users:
                ufs_str = user_first_seen.get(user)
                if not ufs_str:
                    # Migration: no per-user data, fall back to file-level first_seen
                    ufs_str = entry.get('first_seen')
                if not ufs_str:
                    return False  # No timestamp = conservative

                try:
                    first_seen = datetime.fromisoformat(ufs_str)
                    age_days = (now - first_seen).total_seconds() / 86400
                    max_age = max(max_age, age_days)
                    if age_days <= retention_days:
                        logging.debug(
                            f"OnDeck retention: {filename} kept alive by {user} "
                            f"({age_days:.1f} days <= {retention_days} days)"
                        )
                        return False  # This user is still within retention
                except (ValueError, TypeError):
                    return False  # Bad timestamp = conservative

            # All current users exceeded retention
            logging.debug(
                f"OnDeck retention expired for all {len(current_users)} user(s) "
                f"({max_age:.1f} days > {retention_days} days): {filename}"
            )
            return True


# Priority score ranges for UI display and documentation
# These are calculated from the scoring factors in CachePriorityManager:
# - Base: 50
# - OnDeck source: +15, Watchlist: +0
# - Users: +5 to +15 (1-3+ users)
# - Cache recency: +5 (fresh), +3 (recent), +0 (old)
# - Watchlist age: +10 (fresh <7d), +0 (7-60d), -10 (>60d)
# - OnDeck staleness: +5 (fresh <7d), +0 (7-14d), -5 (14-30d), -10 (>30d)
# - Episode position: +15 (current), +10 (next few), +0 (far ahead)
PRIORITY_RANGE_ONDECK_MIN = 60   # Stale OnDeck (30+ days), 1 user, old cache
PRIORITY_RANGE_ONDECK_MAX = 100  # Fresh OnDeck, 3+ users, current episode, fresh cache
PRIORITY_RANGE_WATCHLIST_MIN = 45   # Old watchlist (60+ days), 1 user, old cache
PRIORITY_RANGE_WATCHLIST_MAX = 80   # Fresh watchlist, 3+ users, fresh cache


class CachePriorityManager:
    """Manages priority scoring and smart eviction for cached files.

    Uses metadata from CacheTimestampTracker, WatchlistTracker, and OnDeckTracker
    to calculate priority scores. Lower-priority items are evicted first when
    cache space is needed.

    Priority Score (0-100):
    - Base score: 50
    - Source type: +15 for ondeck, +0 for watchlist (OnDeck = actively watching)
    - User count: +5 per user (max +15) - multiple users = popular
    - Cache recency: +5 (<24h), +3 (<72h), +0 otherwise
    - Watchlist age: +10 if fresh (<7d), 0 if 7-60d, -10 if >60d
    - OnDeck staleness: +5 if fresh (<7d), 0 if 7-14d, -5 if 14-30d, -10 if >30d
    - Episode position: +15 for current OnDeck, +10 for next X episodes, 0 otherwise

    Eviction Philosophy:
    - Watchlist items are evicted first (lower base priority)
    - OnDeck items that sit too long without being watched decay in priority
    - Fresh items (recently added) get slight priority boost
    - Current/next episodes in a series get higher priority
    """

    def __init__(self, timestamp_tracker: CacheTimestampTracker,
                 watchlist_tracker: WatchlistTracker,
                 ondeck_tracker: OnDeckTracker,
                 eviction_min_priority: int = 60,
                 number_episodes: int = 5):
        """Initialize the priority manager.

        Args:
            timestamp_tracker: Tracker for cache timestamps and source.
            watchlist_tracker: Tracker for watchlist items and users.
            ondeck_tracker: Tracker for OnDeck items and users.
            eviction_min_priority: Only evict items with priority below this threshold.
            number_episodes: Number of episodes prefetched after OnDeck (for position scoring).
        """
        self.timestamp_tracker = timestamp_tracker
        self.watchlist_tracker = watchlist_tracker
        self.ondeck_tracker = ondeck_tracker
        self.eviction_min_priority = eviction_min_priority
        self.number_episodes = number_episodes
        self.active_ondeck_paths: Optional[Set[str]] = None  # Set by app when retention is enabled

    def calculate_priority(self, cache_path: str) -> int:
        """Calculate 0-100 priority score for a cached file.

        Higher score = more likely to be watched soon = keep longer.
        Lower score = evict first when space is needed.
        Subtitle files delegate to their parent video's score.

        Eviction philosophy: Watchlist items evicted first, OnDeck protected.

        Args:
            cache_path: Path to the cached file.

        Returns:
            Priority score between 0 and 100.
        """
        # Subtitle delegation: use parent's priority so they're evicted together
        if is_subtitle_file(cache_path):
            parent = self.timestamp_tracker.find_parent_video(cache_path)
            if parent:
                return self.calculate_priority(parent)

        score = 50  # Base score

        # Factor 1: Source Type (+15 for ondeck, +0 for watchlist)
        # OnDeck means user is actively watching this content - protect it
        source = self.timestamp_tracker.get_source(cache_path)
        is_ondeck = source == "ondeck"
        if is_ondeck:
            score += 15

        # Factor 2: User Count (+5 per user, max +15)
        # Items on multiple users' OnDeck/watchlists are more popular
        user_count = 0

        # Check OnDeck tracker first
        ondeck_entry = self.ondeck_tracker.get_entry(cache_path)
        if ondeck_entry:
            user_count = len(ondeck_entry.get('users', []))

        # Also check watchlist tracker if not found or for additional users
        watchlist_entry = self.watchlist_tracker.get_entry(cache_path)
        if watchlist_entry:
            watchlist_users = len(watchlist_entry.get('users', []))
            user_count = max(user_count, watchlist_users)

        score += min(user_count * 5, 15)

        # Factor 3: Cache Recency (+5 if cached in last 24h, +3 if <72h)
        # Small bonus for recently cached to avoid immediate churn
        hours_cached = self._get_hours_since_cached(cache_path)
        if hours_cached >= 0:  # -1 means no timestamp
            if hours_cached < 24:
                score += 5
            elif hours_cached < 72:
                score += 3
            # >72h: no adjustment (0)

        # Factor 4: Watchlist Age (+10 fresh, 0 if >30 days, -10 if >60 days)
        # Recently added to watchlist = user intends to watch soon
        # Old watchlist items (>60 days) = likely forgotten
        if watchlist_entry and watchlist_entry.get('watchlisted_at'):
            days_on_watchlist = self._get_days_on_watchlist(watchlist_entry)
            if days_on_watchlist >= 0:
                if days_on_watchlist < 7:
                    score += 10  # Fresh watchlist item
                elif days_on_watchlist > 60:
                    score -= 10  # Very old, likely forgotten
                # 7-60 days: no adjustment (0)

        # Factor 5: OnDeck Staleness (+5 if fresh, decay over time)
        # Items sitting on OnDeck too long without progress should lose priority
        # Uses first_seen (when added to OnDeck) not last_seen (updated every scan)
        if is_ondeck and ondeck_entry:
            first_seen_str = ondeck_entry.get('first_seen')
            if first_seen_str:
                days_on_ondeck = self._get_days_since_first_seen(first_seen_str)
                if days_on_ondeck >= 0:
                    if days_on_ondeck < 7:
                        score += 5   # Fresh - just added to OnDeck
                    elif days_on_ondeck < 14:
                        pass         # Normal - no adjustment (0)
                    elif days_on_ondeck < 30:
                        score -= 5   # Getting stale
                    else:
                        score -= 10  # Stale - on OnDeck for 30+ days

        # Factor 6: Episode Position (+15 for current OnDeck, +10 for next X episodes, 0 otherwise)
        # Current/next episodes in a series get higher priority
        # X = half of number_episodes setting (so if prefetching 5 episodes, prioritize next 2-3)
        # Only award bonus if item is actively protected (not expired from ondeck retention)
        # active_ondeck_paths=None means retention is disabled, so all ondeck items get bonus
        if self._is_tv_episode(cache_path) and (
            self.active_ondeck_paths is None or cache_path in self.active_ondeck_paths
        ):
            episodes_ahead = self._get_episodes_ahead_of_ondeck(cache_path)
            if episodes_ahead >= 0:  # -1 means not applicable
                if episodes_ahead == 0:
                    score += 15  # Current OnDeck episode - highest priority
                elif episodes_ahead <= max(1, self.number_episodes // 2):
                    score += 10  # Next few episodes - high priority
                # episodes_ahead > half of number_episodes: no adjustment (0)
                # Per StudioNirin: far-ahead episodes should NOT get negative scores

        return max(0, min(100, score))

    def _get_days_since_last_seen(self, last_seen_str: str) -> float:
        """Get days since an item was last seen in OnDeck/watchlist.

        Args:
            last_seen_str: ISO format timestamp string.

        Returns:
            Days since last seen, or -1 if invalid timestamp.
        """
        try:
            last_seen = datetime.fromisoformat(last_seen_str)
            return (datetime.now() - last_seen).total_seconds() / 86400
        except (ValueError, TypeError):
            return -1

    def _get_days_since_first_seen(self, first_seen_str: str) -> float:
        """Get days since an item was first added to OnDeck.

        Args:
            first_seen_str: ISO format timestamp string.

        Returns:
            Days since first added to OnDeck, or -1 if invalid timestamp.
        """
        try:
            first_seen = datetime.fromisoformat(first_seen_str)
            return (datetime.now() - first_seen).total_seconds() / 86400
        except (ValueError, TypeError):
            return -1

    def get_all_priorities(self, cached_files: List[str]) -> List[Tuple[str, int]]:
        """Get priority scores for all cached files.

        Args:
            cached_files: List of cache file paths.

        Returns:
            List of (cache_path, priority_score) tuples, sorted by score ascending
            (lowest priority first, for eviction order).
        """
        priorities = []
        for cache_path in cached_files:
            score = self.calculate_priority(cache_path)
            priorities.append((cache_path, score))

        # Sort by score ascending (lowest priority first)
        priorities.sort(key=lambda x: x[1])
        return priorities

    def get_eviction_candidates(self, cached_files: List[str], target_bytes: int) -> List[str]:
        """Get files to evict to free target_bytes of space.

        Only considers files with priority below eviction_min_priority.
        Returns lowest-priority files first, accumulating until target_bytes reached.

        Args:
            cached_files: List of cache file paths.
            target_bytes: Amount of space needed to free.

        Returns:
            List of cache file paths to evict, in eviction order.
        """
        if target_bytes <= 0:
            return []

        # Get all priorities, sorted by score ascending
        priorities = self.get_all_priorities(cached_files)

        candidates = []
        bytes_accumulated = 0

        for cache_path, score in priorities:
            # Only evict files below minimum priority threshold
            if score >= self.eviction_min_priority:
                logging.debug(f"Skipping eviction candidate (score {score} >= {self.eviction_min_priority}): {os.path.basename(cache_path)}")
                continue

            # Check file exists and get size
            if not os.path.exists(cache_path):
                continue

            try:
                file_size = os.path.getsize(cache_path)
            except (OSError, IOError):
                continue

            candidates.append(cache_path)
            bytes_accumulated += file_size

            logging.debug(f"Eviction candidate (score {score}): {os.path.basename(cache_path)} ({file_size / (1024**2):.1f}MB)")

            if bytes_accumulated >= target_bytes:
                break

        return candidates

    def get_priority_report(self, cached_files: List[str]) -> str:
        """Generate a human-readable priority report for all cached files.

        Sorted by: Score (desc), Source (ondeck first), Days cached (asc)

        Args:
            cached_files: List of cache file paths.

        Returns:
            Formatted string showing priority scores and metadata.
        """
        priorities = self.get_all_priorities(cached_files)

        # Build list of report entries with all metadata for sorting
        entries = []
        stale_entries = []  # Track files that no longer exist on disk
        for cache_path, score in priorities:
            # Get file info
            try:
                if os.path.exists(cache_path):
                    size_bytes = os.path.getsize(cache_path)
                    size_str = f"{size_bytes / (1024**3):.1f}GB" if size_bytes >= 1024**3 else f"{size_bytes / (1024**2):.0f}MB"
                else:
                    # File doesn't exist - track as stale and skip
                    filename = os.path.basename(cache_path)
                    if len(filename) > 50:
                        filename = filename[:47] + "..."
                    stale_entries.append(filename)
                    continue
            except (OSError, IOError):
                # Can't access file - track as stale and skip
                filename = os.path.basename(cache_path)
                if len(filename) > 50:
                    filename = filename[:47] + "..."
                stale_entries.append(filename)
                continue

            source = self.timestamp_tracker.get_source(cache_path)
            hours_cached = self._get_hours_since_cached(cache_path)
            days_cached = hours_cached / 24 if hours_cached >= 0 else -1

            # Get user count from OnDeck and Watchlist trackers
            user_count = 0
            ondeck_entry = self.ondeck_tracker.get_entry(cache_path)
            if ondeck_entry:
                user_count = len(ondeck_entry.get('users', []))
            watchlist_entry = self.watchlist_tracker.get_entry(cache_path)
            if watchlist_entry:
                watchlist_users = len(watchlist_entry.get('users', []))
                user_count = max(user_count, watchlist_users)

            filename = os.path.basename(cache_path)
            if len(filename) > 35:
                filename = filename[:32] + "..."

            entries.append({
                'score': score,
                'source': source,
                'days': days_cached,
                'size_str': size_str,
                'size_bytes': size_bytes,
                'user_count': user_count,
                'filename': filename
            })

        # Sort by: Score (desc), Source (ondeck=0, watchlist=1, unknown=2), Days (asc)
        source_order = {'ondeck': 0, 'watchlist': 1, 'unknown': 2}
        entries.sort(key=lambda e: (-e['score'], source_order.get(e['source'], 2), e['days']))

        # Build report
        lines = []
        lines.append("Cache Priority Report")
        lines.append("=" * 70)
        lines.append(f"{'Score':>5} | {'Size':>8} | {'Source':>9} | {'Users':>5} | {'Days':>4} | File")
        lines.append("-" * 70)

        evictable_count = 0
        evictable_bytes = 0

        for entry in entries:
            evict_marker = " *" if entry['score'] < self.eviction_min_priority else ""
            lines.append(f"{entry['score']:>5} | {entry['size_str']:>8} | {entry['source']:>9} | {entry['user_count']:>5} | {entry['days']:>4.0f} | {entry['filename']}{evict_marker}")

            if entry['score'] < self.eviction_min_priority:
                evictable_count += 1
                evictable_bytes += entry['size_bytes']

        lines.append("-" * 70)
        lines.append(f"Items below eviction threshold ({self.eviction_min_priority}): {evictable_count}")
        lines.append(f"Space that would be freed: {evictable_bytes / (1024**3):.2f}GB")
        lines.append("")
        lines.append("* = Would be evicted when space is needed")

        # List stale entries if any
        if stale_entries:
            lines.append("")
            lines.append(f"Stale entries (file not found): {len(stale_entries)} — run app to clean")
            for stale_file in sorted(stale_entries):
                lines.append(f"  - {stale_file}")

        return "\n".join(lines)

    def _get_hours_since_cached(self, cache_path: str) -> float:
        """Get hours since file was cached.

        Args:
            cache_path: Path to the cached file.

        Returns:
            Hours since cached, or -1 if no timestamp.
        """
        # Use the retention_remaining method with a large retention to get the age
        remaining = self.timestamp_tracker.get_retention_remaining(cache_path, 10000)
        if remaining == 0:
            return -1  # No timestamp
        # remaining = retention - age, so age = retention - remaining
        return 10000 - remaining

    def _get_days_on_watchlist(self, entry: dict) -> float:
        """Get days since item was added to watchlist.

        Args:
            entry: Watchlist tracker entry dict.

        Returns:
            Days on watchlist, or -1 if no timestamp.
        """
        watchlisted_at_str = entry.get('watchlisted_at')
        if not watchlisted_at_str:
            return -1

        try:
            watchlisted_at = datetime.fromisoformat(watchlisted_at_str)
            return (datetime.now() - watchlisted_at).total_seconds() / 86400
        except (ValueError, TypeError):
            return -1

    def _get_episodes_ahead_of_ondeck(self, cache_path: str) -> int:
        """Get how many episodes this file is ahead of the OnDeck position.

        For TV episodes, calculates the distance from the earliest OnDeck position
        for the same show. This is used to prioritize current/next episodes over
        episodes further in the future.

        Args:
            cache_path: Path to the cached file.

        Returns:
            Number of episodes ahead of OnDeck position:
            - 0: This IS the current OnDeck episode
            - 1-N: Number of episodes ahead
            - -1: Not a TV episode, or no OnDeck position found for this show
        """
        # Get episode info for this file (try OnDeck first, then persistent tracker)
        ep_info = self.ondeck_tracker.get_episode_info(cache_path)
        if not ep_info:
            ep_info = self.timestamp_tracker.get_episode_info(cache_path)
        if not ep_info:
            return -1  # Not a TV episode or no info available

        show = ep_info.get('show')
        season = ep_info.get('season')
        episode = ep_info.get('episode')

        if not show or season is None or episode is None:
            return -1

        # Check if this IS the current OnDeck episode
        if ep_info.get('is_current_ondeck'):
            return 0

        # Get the earliest OnDeck position for this show
        ondeck_pos = self.ondeck_tracker.get_earliest_ondeck_position(show)
        if not ondeck_pos:
            return -1  # No OnDeck position found for this show

        ondeck_season, ondeck_episode = ondeck_pos

        # Calculate how many episodes ahead this file is
        if season < ondeck_season:
            # This episode is BEFORE the OnDeck position (shouldn't happen, but handle it)
            return -1
        elif season == ondeck_season:
            if episode <= ondeck_episode:
                # Same season, same or earlier episode
                return 0
            else:
                # Same season, later episode
                return episode - ondeck_episode
        else:
            # Later season - estimate distance
            # Assume ~13 episodes per season for estimation
            episodes_per_season = 13
            seasons_ahead = season - ondeck_season
            episodes_remaining_in_ondeck_season = episodes_per_season - ondeck_episode
            full_seasons_between = max(0, seasons_ahead - 1) * episodes_per_season
            return episodes_remaining_in_ondeck_season + full_seasons_between + episode

    def _is_tv_episode(self, cache_path: str) -> bool:
        """Check if a cached file is a TV episode.

        Checks OnDeckTracker first (current run), then CacheTimestampTracker
        (persistent metadata from Plex API).

        Args:
            cache_path: Path to the cached file.

        Returns:
            True if this is a TV episode with episode info, False otherwise.
        """
        # Check OnDeckTracker (current run)
        ep_info = self.ondeck_tracker.get_episode_info(cache_path)
        if ep_info is not None and ep_info.get('show') is not None:
            return True
        # Check CacheTimestampTracker (persistent Plex API metadata)
        mt = self.timestamp_tracker.get_media_type(cache_path)
        if mt is not None:
            return mt == "episode"
        return False


class PlexcachedMigration:
    """One-time migration to create .plexcached backups for existing cached files.

    For users upgrading from older versions, files may exist on cache without
    a corresponding .plexcached backup on the array. This migration scans the
    exclude file and creates .plexcached backups for any files that need them.
    """

    MIGRATION_FLAG = "plexcache_migration_v2.complete"

    def __init__(self, exclude_file: str, cache_dir: str, real_source: str,
                 script_folder: str, is_unraid: bool = False,
                 path_modifier: Optional['MultiPathModifier'] = None,
                 is_docker: bool = False):
        """Initialize the migration helper.

        Args:
            exclude_file: Path to plexcache_cached_files.txt
            cache_dir: Cache directory path (e.g., /mnt/cache_downloads/)
            real_source: Array source path (e.g., /mnt/user/)
            script_folder: Folder where the script lives (for flag file)
            is_unraid: Whether running on Unraid (affects path handling)
            path_modifier: MultiPathModifier for multi-path setups (uses path_mappings)
            is_docker: Whether running in Docker (affects path translation)
        """
        self.exclude_file = exclude_file
        self.cache_dir = cache_dir
        self.real_source = real_source
        self.is_unraid = is_unraid
        self.path_modifier = path_modifier
        self.is_docker = is_docker

        # Store flag file in persistent location
        # In Docker: /config/data/ (persistent volume)
        # Otherwise: script_folder (project root)
        if is_docker:
            flag_dir = os.path.join(script_folder, 'data')
        else:
            flag_dir = script_folder
        self.flag_file = os.path.join(flag_dir, self.MIGRATION_FLAG)

    def needs_migration(self) -> bool:
        """Check if migration has already been completed."""
        return not os.path.exists(self.flag_file)

    def _read_exclude_file(self) -> Tuple[List[str], int]:
        """Read and deduplicate the exclude file.

        Returns:
            Tuple of (deduplicated_cache_files, duplicates_removed_count)
        """
        if not os.path.exists(self.exclude_file):
            return [], 0

        with open(self.exclude_file, 'r') as f:
            all_lines = [line.strip() for line in f if line.strip()]
            cache_files = list(dict.fromkeys(all_lines))
            duplicates_removed = len(all_lines) - len(cache_files)

        return cache_files, duplicates_removed

    def _translate_from_host_path(self, host_path: str) -> str:
        """Translate host cache path back to container cache path for file existence checks.

        In Docker, the exclude file contains host paths like /mnt/cache_downloads but
        the container sees /mnt/cache. This reverse-translates for os.path.exists() checks.
        """
        if not self.is_docker or not self.path_modifier:
            return host_path

        path_mappings = getattr(self.path_modifier, 'mappings', [])

        for mapping in path_mappings:
            if not mapping.cache_path or not mapping.host_cache_path:
                continue
            if mapping.cache_path == mapping.host_cache_path:
                continue  # No translation needed

            host_prefix = mapping.host_cache_path.rstrip('/')
            # Ensure prefix match is at a path boundary (not partial directory name)
            if host_path == host_prefix or host_path.startswith(host_prefix + '/'):
                cache_prefix = mapping.cache_path.rstrip('/')
                translated = host_path.replace(host_prefix, cache_prefix, 1)
                return translated

        return host_path

    def _find_files_needing_migration(self, cache_files: List[str]) -> Tuple[List[Tuple[str, str, str]], int]:
        """Find files that need .plexcached backup creation.

        Args:
            cache_files: List of cache file paths from exclude file.

        Returns:
            Tuple of (files_needing_migration, total_bytes)
            where files_needing_migration is a list of (cache_file, array_file, plexcached_file) tuples.
        """
        files_needing_migration = []

        for cache_file in cache_files:
            # In Docker, exclude file has host paths but we need container paths for file operations
            check_path = self._translate_from_host_path(cache_file)
            if not os.path.isfile(check_path):
                logging.debug(f"Cache file no longer exists, skipping: {cache_file}")
                continue

            # Derive array path from cache path using path_mappings if available
            if self.path_modifier:
                array_file, mapping = self.path_modifier.convert_cache_to_real(check_path)
                if array_file is None:
                    logging.debug(f"No path mapping found for cache file, skipping: {cache_file}")
                    continue
            else:
                # Legacy fallback: simple string replacement
                array_file = check_path.replace(self.cache_dir, self.real_source, 1)

            # On Unraid, check user0 (direct array) for .plexcached
            # This is the authoritative location - .plexcached should be on array
            if self.is_unraid:
                array_file_direct = get_array_direct_path(array_file)
                plexcached_file = array_file_direct + PLEXCACHED_EXTENSION

                # Check if .plexcached exists on array
                if os.path.isfile(plexcached_file):
                    logging.debug(f"Already has .plexcached backup: {cache_file}")
                    continue

                # Check if original exists on array (file wasn't cached yet)
                if os.path.isfile(array_file_direct):
                    logging.debug(f"Original exists on array, no migration needed: {cache_file}")
                    continue

                array_file_check = array_file_direct
            else:
                array_file_check = array_file
                plexcached_file = array_file + PLEXCACHED_EXTENSION

                # Check if .plexcached already exists OR original exists on array
                if os.path.isfile(plexcached_file):
                    logging.debug(f"Already has .plexcached backup: {cache_file}")
                    continue

                if os.path.isfile(array_file_check):
                    logging.debug(f"Original exists on array, no migration needed: {cache_file}")
                    continue

            # This file needs migration (use container path for file operations)
            files_needing_migration.append((check_path, array_file_check, plexcached_file))

        # Calculate total size
        total_bytes = 0
        for container_path, _, _ in files_needing_migration:
            try:
                total_bytes += os.path.getsize(container_path)
            except OSError:
                pass

        return files_needing_migration, total_bytes

    def _migrate_single_file(self, args: Tuple[str, str, str]) -> int:
        """Migrate a single file by creating its .plexcached backup.

        Args:
            args: Tuple of (cache_file, array_file, plexcached_file)

        Returns:
            0 on success, 1 on error, 2 on critical error (stop migration)
        """
        cache_file, array_file, plexcached_file = args
        thread_id = threading.get_ident()

        # Check if critical error occurred - skip remaining files
        if getattr(self, '_critical_error', False):
            return 2

        try:
            # Get file size for progress
            try:
                file_size = os.path.getsize(cache_file)
            except OSError:
                file_size = 0

            filename = os.path.basename(cache_file)

            # Register as active before starting copy
            with self._migration_lock:
                self._active_files[thread_id] = (filename, file_size)
                self._print_progress()

            # Ensure directory exists
            array_dir = os.path.dirname(plexcached_file)
            if not os.path.exists(array_dir):
                os.makedirs(array_dir, exist_ok=True)

            # Copy cache file to array as .plexcached (preserving ownership on Linux)
            if self.is_unraid:
                # Get source ownership before copy
                stat_info = os.stat(cache_file)
                src_uid = stat_info.st_uid
                src_gid = stat_info.st_gid

                shutil.copy2(cache_file, plexcached_file)

                # Restore original ownership (shutil.copy2 doesn't preserve uid/gid)
                os.chown(plexcached_file, src_uid, src_gid)
                logging.debug(f"  Preserved ownership: uid={src_uid}, gid={src_gid}")
            else:
                shutil.copy2(cache_file, plexcached_file)

            # Verify copy succeeded
            if os.path.isfile(plexcached_file):
                with self._migration_lock:
                    self._migrated += 1
                    self._completed_bytes += file_size
                    if thread_id in self._active_files:
                        del self._active_files[thread_id]
                    self._print_progress()
                # Log to file (outside lock for performance)
                logging.info(f"Migrated: {filename} ({format_bytes(file_size)})")
                return 0
            else:
                logging.error(f"Failed to verify: {plexcached_file}")
                with self._migration_lock:
                    self._errors += 1
                    if thread_id in self._active_files:
                        del self._active_files[thread_id]
                return 1

        except OSError as e:
            # Detect critical errors that should stop the entire migration
            # errno 28 = ENOSPC (No space left on device)
            # errno 1 = EPERM (Operation not permitted)
            # errno 13 = EACCES (Permission denied)
            critical_errors = {28, 1, 13}
            is_critical = e.errno in critical_errors

            logging.error(f"Error migrating {cache_file}: {type(e).__name__}: {e}")

            with self._migration_lock:
                self._errors += 1
                if thread_id in self._active_files:
                    del self._active_files[thread_id]

                if is_critical and not getattr(self, '_critical_error', False):
                    self._critical_error = True
                    error_type = {28: "No space left on device", 1: "Operation not permitted", 13: "Permission denied"}.get(e.errno, str(e))
                    logging.error(f"CRITICAL: {error_type} - Stopping migration early")

            return 2 if is_critical else 1

        except Exception as e:
            logging.error(f"Error migrating {cache_file}: {type(e).__name__}: {e}")
            with self._migration_lock:
                self._errors += 1
                if thread_id in self._active_files:
                    del self._active_files[thread_id]
            return 1

    def run_migration(self, dry_run: bool = False, max_concurrent: int = 5) -> Tuple[int, int, int]:
        """Run the migration to create .plexcached backups.

        Args:
            dry_run: If True, only log what would be done without making changes.
            max_concurrent: Maximum number of concurrent file copies.

        Returns:
            Tuple of (files_migrated, files_skipped, errors)
        """
        if not self.needs_migration():
            logging.info("Migration already complete, skipping")
            return 0, 0, 0

        # Read and deduplicate exclude file
        cache_files, duplicates_removed = self._read_exclude_file()

        if not cache_files:
            logging.info("No exclude file or empty, nothing to migrate")
            self._mark_complete()
            return 0, 0, 0

        logging.info("=== PlexCache-D Migration ===")
        if duplicates_removed > 0:
            logging.info(f"Removed {duplicates_removed} duplicate entries from exclude list")
        logging.info(f"Checking {len(cache_files)} unique files in exclude list...")

        # Find files that need migration
        files_needing_migration, total_bytes = self._find_files_needing_migration(cache_files)

        if not files_needing_migration:
            logging.info("All files already have backups, no migration needed")
            self._mark_complete()
            return 0, len(cache_files), 0

        total_gb = total_bytes / (1024 ** 3)
        logging.info(f"Found {len(files_needing_migration)} files needing .plexcached backup ({total_gb:.2f} GB)")

        if dry_run:
            logging.info("[DRY RUN] Would create the following backups:")
            for cache_file, _, plexcached_file in files_needing_migration:
                logging.info(f"  {cache_file} -> {plexcached_file}")
            return 0, 0, 0

        # Perform migration with progress tracking
        logging.info(f"Starting migration with {max_concurrent} concurrent copies...")

        # Initialize thread-safe counters
        self._migration_lock = threading.Lock()
        self._migrated = 0
        self._errors = 0
        self._completed_bytes = 0
        self._total_files = len(files_needing_migration)
        self._total_bytes = total_bytes
        self._active_files = {}
        self._last_display_lines = 0
        self._critical_error = False  # Flag to stop migration on critical errors

        with ThreadPoolExecutor(max_workers=max_concurrent) as executor:
            list(executor.map(self._migrate_single_file, files_needing_migration))

        # Print final progress
        with self._migration_lock:
            self._print_progress(final=True)

        migrated = self._migrated
        errors = self._errors
        skipped = len(cache_files) - len(files_needing_migration)

        if self._critical_error:
            logging.info(f"=== Migration Stopped (Critical Error) ===")
        else:
            logging.info(f"=== Migration Complete ===")
        logging.info(f"  Migrated: {migrated} files")
        logging.info(f"  Skipped (already had backup): {skipped} files")
        logging.info(f"  Errors: {errors}")

        if errors == 0:
            self._mark_complete()
        elif self._critical_error:
            logging.warning("Migration stopped due to critical error (disk full or permission issue)")
            logging.warning("Please resolve the issue and restart PlexCache-D to continue migration")
        else:
            logging.warning("Migration had errors - will retry on next run")

        return migrated, skipped, errors

    def _mark_complete(self) -> None:
        """Create the flag file to indicate migration is complete."""
        try:
            with open(self.flag_file, 'w') as f:
                f.write(f"Migration completed: {datetime.now().isoformat()}\n")
            logging.info(f"Migration flag created: {self.flag_file}")
        except IOError as e:
            logging.error(f"Could not create migration flag: {type(e).__name__}: {e}")

    def _print_progress(self, final: bool = False) -> None:
        """Print progress bar for migration with active file queue display."""
        if self._total_files == 0:
            return

        completed = self._migrated
        percentage = (completed / self._total_files) * 100
        bar_width = 30
        filled = int(bar_width * completed / self._total_files)
        bar = '█' * filled + '░' * (bar_width - filled)

        # Format data progress
        completed_str = format_bytes(self._completed_bytes)
        total_str = format_bytes(self._total_bytes)
        data_progress = f"{completed_str} / {total_str}"

        active_files = list(self._active_files.values())

        # Use console lock to prevent interleaving with logging
        with get_console_lock():
            # Clear previous display first (move up and clear each line)
            if self._last_display_lines > 0:
                for _ in range(self._last_display_lines):
                    print('\033[A\033[2K', end='')

            if final:
                # Print final summary
                print(f"[{bar}] 100% ({completed}/{self._total_files}) - {data_progress} - Migration complete")
                self._last_display_lines = 0
            else:
                # Build the display lines
                lines = []
                lines.append(f"[{bar}] {percentage:.0f}% ({completed}/{self._total_files}) - {data_progress} - Migrating...")

                if active_files:
                    lines.append(f"  Currently copying ({len(active_files)} active):")
                    for filename, file_size in active_files[:5]:  # Limit to 5 active files shown
                        display_name = filename[:50] + '...' if len(filename) > 50 else filename
                        size_str = format_bytes(file_size)
                        lines.append(f"    -> {display_name} ({size_str})")
                    if len(active_files) > 5:
                        lines.append(f"    ... and {len(active_files) - 5} more")

                # Print all lines and track count for next clear
                for line in lines:
                    print(line)
                self._last_display_lines = len(lines)


class MultiPathModifier:
    """Handles path conversion with multiple mapping support.

    Replaces the legacy FilePathModifier for setups with multiple path mappings.
    Supports:
    - Multiple independent path mappings (e.g., local array + remote NAS)
    - Per-mapping cache configuration
    - Non-cacheable paths (remote storage that shouldn't be cached)
    - Longest-prefix matching for overlapping paths

    Attributes:
        mappings: List of PathMapping objects, sorted by plex_path length (descending)
                  for longest-prefix matching.
    """

    def __init__(self, mappings: List['PathMapping']):
        """Initialize with list of path mappings.

        Args:
            mappings: List of PathMapping objects. Will be filtered to enabled only
                      and sorted by plex_path length (descending) for longest-prefix matching.
        """
        # Import here to avoid circular imports
        from core.config import PathMapping

        # Keep all mappings for disabled path checking, sorted by plex_path length (longest first)
        self.all_mappings = sorted(
            mappings,
            key=lambda m: len(m.plex_path),
            reverse=True
        )

        # Filter to enabled mappings for actual path conversion
        self.mappings = [m for m in self.all_mappings if m.enabled]

        # Track disabled skips across calls for consolidated logging
        self._accumulated_disabled_skips = {}

        if not self.mappings:
            logging.warning("No enabled path mappings configured!")
        else:
            enabled_count = len(self.mappings)
            total_count = len(self.all_mappings)
            logging.debug(f"MultiPathModifier initialized with {total_count} mappings ({enabled_count} enabled)")
            for m in self.mappings:
                cacheable_str = "cacheable" if m.cacheable else "NOT cacheable"
                logging.debug(f"  {m.name}: {m.plex_path} -> {m.real_path} ({cacheable_str})")

    def convert_plex_to_real(self, plex_path: str) -> Tuple[str, Optional['PathMapping']]:
        """Convert Plex path to real filesystem path.

        Args:
            plex_path: Path as returned by Plex API.

        Returns:
            Tuple of (converted_path, mapping_used).
            If no mapping matches, returns (original_path, None).
        """
        # Check if already converted (matches any real_path prefix)
        for mapping in self.mappings:
            if plex_path.startswith(mapping.real_path):
                logging.debug(f"Path already in real format, skipping: {plex_path}")
                return (plex_path, mapping)

        # Find matching mapping (longest prefix wins due to sort order)
        for mapping in self.mappings:
            if plex_path.startswith(mapping.plex_path):
                converted = plex_path.replace(mapping.plex_path, mapping.real_path, 1)
                logging.debug(f"Converted path using '{mapping.name}': {plex_path} -> {converted}")
                return (converted, mapping)

        # Check if path matches a disabled mapping (skip silently)
        for mapping in self.all_mappings:
            if not mapping.enabled and plex_path.startswith(mapping.plex_path):
                logging.debug(f"Skipping disabled mapping '{mapping.name}': {plex_path}")
                return (plex_path, None)

        # Extract library folder for cleaner message (e.g., /nas/TV Shows UHD/)
        path_parts = plex_path.lstrip('/').split('/')
        if len(path_parts) >= 2:
            library_hint = f"/{path_parts[0]}/{path_parts[1]}/"
        elif path_parts:
            library_hint = f"/{path_parts[0]}/"
        else:
            library_hint = plex_path
        logging.info(f"Skipping unmapped path {library_hint} - add to path_mappings with enabled:false to silence")
        logging.debug(f"Full unmapped path: {plex_path}")
        return (plex_path, None)

    def convert_real_to_cache(self, real_path: str) -> Tuple[Optional[str], Optional['PathMapping']]:
        """Convert real filesystem path to cache path.

        Args:
            real_path: Actual filesystem path.

        Returns:
            Tuple of (cache_path, mapping_used).
            Returns (None, mapping) if path is not cacheable.
            Returns (None, None) if no mapping matches.
        """
        for mapping in self.mappings:
            if real_path.startswith(mapping.real_path):
                if not mapping.cacheable or not mapping.cache_path:
                    logging.debug(f"Path not cacheable ({mapping.name}): {real_path}")
                    return (None, mapping)
                cache = real_path.replace(mapping.real_path, mapping.cache_path, 1)
                return (cache, mapping)

        # Check if path matches a disabled mapping (skip silently)
        for mapping in self.all_mappings:
            if not mapping.enabled and real_path.startswith(mapping.real_path):
                logging.debug(f"Skipping disabled mapping '{mapping.name}': {real_path}")
                return (None, None)

        logging.debug(f"No mapping found for real path: {real_path}")
        return (None, None)

    def convert_cache_to_real(self, cache_path: str) -> Tuple[Optional[str], Optional['PathMapping']]:
        """Convert cache path back to real filesystem path.

        Args:
            cache_path: Path on cache drive.

        Returns:
            Tuple of (real_path, mapping_used).
            Returns (None, None) if no mapping matches.
        """
        for mapping in self.mappings:
            if mapping.cache_path and cache_path.startswith(mapping.cache_path):
                real = cache_path.replace(mapping.cache_path, mapping.real_path, 1)
                return (real, mapping)

        # Check if path matches a disabled mapping (skip silently)
        for mapping in self.all_mappings:
            if not mapping.enabled and mapping.cache_path and cache_path.startswith(mapping.cache_path):
                logging.debug(f"Skipping disabled mapping '{mapping.name}': {cache_path}")
                return (None, None)

        logging.debug(f"No mapping found for cache path: {cache_path}")
        return (None, None)

    def is_cacheable(self, real_path: str) -> bool:
        """Check if a real filesystem path is cacheable.

        Args:
            real_path: Actual filesystem path.

        Returns:
            True if path belongs to a cacheable mapping, False otherwise.
        """
        for mapping in self.mappings:
            if real_path.startswith(mapping.real_path):
                return mapping.cacheable
        return False

    def get_mapping_for_path(self, path: str) -> Optional['PathMapping']:
        """Get the mapping that handles a given path.

        Args:
            path: Any path (plex, real, or cache).

        Returns:
            The PathMapping that handles this path, or None.
        """
        for mapping in self.mappings:
            if (path.startswith(mapping.plex_path) or
                path.startswith(mapping.real_path) or
                (mapping.cache_path and path.startswith(mapping.cache_path))):
                return mapping
        return None

    def modify_file_paths(self, files: List[str]) -> List[str]:
        """Convert a list of Plex paths to real paths.

        Compatibility method - replaces legacy FilePathModifier.modify_file_paths().

        Args:
            files: List of Plex paths.

        Returns:
            List of converted real paths.
        """
        if files is None:
            return []

        logging.debug("Converting file paths using multi-path mappings...")
        result = []
        disabled_skips = {}  # mapping_name -> count

        for file_path in files:
            converted, mapping = self.convert_plex_to_real(file_path)
            result.append(converted)

            # Track files skipped due to disabled mappings
            if mapping is None:
                # Check if it matched a disabled mapping
                for m in self.all_mappings:
                    if not m.enabled and file_path.startswith(m.plex_path):
                        disabled_skips[m.name] = disabled_skips.get(m.name, 0) + 1
                        break

        # Accumulate disabled skips for consolidated logging later
        for name, count in disabled_skips.items():
            self._accumulated_disabled_skips[name] = self._accumulated_disabled_skips.get(name, 0) + count

        return result

    def log_disabled_skips_summary(self) -> None:
        """Log a summary of all accumulated disabled library skips and reset the counter.

        Call this once after all path processing is complete (e.g., end of _process_media).
        """
        if self._accumulated_disabled_skips:
            total_skipped = sum(self._accumulated_disabled_skips.values())
            mapping_names = ', '.join(sorted(self._accumulated_disabled_skips.keys()))
            logging.info(f"Skipped {total_skipped} files from disabled libraries ({mapping_names})")
            self._accumulated_disabled_skips = {}

    def get_mapping_stats(self) -> Dict[str, Dict[str, any]]:
        """Get statistics about path mappings.

        Returns:
            Dict mapping names to stats (plex_path, real_path, cacheable, enabled).
        """
        return {
            m.name: {
                'plex_path': m.plex_path,
                'real_path': m.real_path,
                'cache_path': m.cache_path,
                'cacheable': m.cacheable,
                'enabled': m.enabled
            }
            for m in self.mappings
        }


class SubtitleFinder:
    """Handles subtitle file discovery and operations."""
    
    def __init__(self, subtitle_extensions: Optional[List[str]] = None):
        if subtitle_extensions is None:
            subtitle_extensions = [".srt", ".vtt", ".sbv", ".sub", ".idx"]
        self.subtitle_extensions = subtitle_extensions
    
    def get_media_subtitles_grouped(self, media_files: List[str], files_to_skip: Optional[Set[str]] = None) -> Dict[str, List[str]]:
        """Get subtitle files grouped by their parent video file.

        Args:
            media_files: List of media file paths.
            files_to_skip: Set of file paths to skip.

        Returns:
            Dict mapping each video path to its list of subtitle paths.
            Videos without subtitles have an empty list.
        """
        logging.debug("Fetching subtitles (grouped)...")

        files_to_skip = set() if files_to_skip is None else set(files_to_skip)
        processed_files = set()
        result: Dict[str, List[str]] = {}

        for file in media_files:
            if file in files_to_skip or file in processed_files:
                continue
            processed_files.add(file)

            subtitle_files = []
            directory_path = os.path.dirname(file)
            if os.path.exists(directory_path):
                subtitle_files = self._find_subtitle_files(directory_path, file)
                for subtitle_file in subtitle_files:
                    logging.debug(f"Subtitle found: {subtitle_file}")

            result[file] = subtitle_files

        return result

    def get_media_subtitles(self, media_files: List[str], files_to_skip: Optional[Set[str]] = None) -> List[str]:
        """Get subtitle files for media files (flat list including originals).

        Args:
            media_files: List of media file paths.
            files_to_skip: Set of file paths to skip.

        Returns:
            List of all media files plus their subtitle files.
        """
        logging.debug("Fetching subtitles...")
        grouped = self.get_media_subtitles_grouped(media_files, files_to_skip)
        all_files = list(media_files)
        for subs in grouped.values():
            all_files.extend(subs)
        return all_files
    
    def _find_subtitle_files(self, directory_path: str, file: str) -> List[str]:
        """Find subtitle files in a directory for a given media file."""
        file_basename = os.path.basename(file)
        file_name, _ = os.path.splitext(file_basename)

        try:
            subtitle_files = [
                entry.path
                for entry in os.scandir(directory_path)
                if entry.is_file() and entry.name.startswith(file_name) and
                   entry.name != file_basename and entry.name.endswith(tuple(self.subtitle_extensions))
            ]
        except PermissionError as e:
            logging.error(f"Cannot access directory {directory_path}. Permission denied. {type(e).__name__}: {e}")
            subtitle_files = []
        except OSError as e:
            logging.error(f"Cannot access directory {directory_path}. {type(e).__name__}: {e}")
            subtitle_files = []

        return subtitle_files


class FileFilter:
    """Handles file filtering based on destination and conditions."""

    def __init__(self, real_source: str, cache_dir: str, is_unraid: bool,
                 mover_cache_exclude_file: str,
                 timestamp_tracker: Optional['CacheTimestampTracker'] = None,
                 cache_retention_hours: int = 12,
                 ondeck_tracker: Optional['OnDeckTracker'] = None,
                 watchlist_tracker: Optional['WatchlistTracker'] = None,
                 path_modifier: Optional['MultiPathModifier'] = None,
                 is_docker: bool = False,
                 use_symlinks: bool = False):
        self.real_source = real_source
        self.cache_dir = cache_dir
        self.is_unraid = is_unraid
        self.mover_cache_exclude_file = mover_cache_exclude_file or ""
        self.timestamp_tracker = timestamp_tracker
        self.cache_retention_hours = cache_retention_hours
        self.ondeck_tracker = ondeck_tracker
        self.watchlist_tracker = watchlist_tracker
        self.path_modifier = path_modifier  # For multi-path support
        self.is_docker = is_docker  # For path translation in Docker
        self.use_symlinks = use_symlinks  # Whether to create/preserve symlinks at original locations
        self.last_already_cached_count = 0  # Track files already on cache during filtering
        self._media_info_map = {}  # Plex media type info (set via set_media_info_map)

    def set_media_info_map(self, media_info_map: Dict[str, Dict]) -> None:
        """Set the media info map for metadata-first classification.

        Args:
            media_info_map: Dict mapping file paths to {'media_type': str, 'episode_info': dict|None}.
        """
        self._media_info_map = media_info_map or {}

    def _lookup_media_info(self, file_path: str) -> Optional[Tuple[str, Optional[Dict]]]:
        """Look up media type from available sources (trackers > regex fallback).

        Checks subtitle parent delegation first, then OnDeckTracker (current run),
        media_info_map (watchlist), and CacheTimestampTracker (persistent) in order
        of authority.

        Args:
            file_path: Path to the media file.

        Returns:
            Tuple of (media_type, episode_info) if found, None for regex fallback.
        """
        # 0. Subtitle delegation: if this is a subtitle with a tracked parent, use parent's info
        if is_subtitle_file(file_path) and self.timestamp_tracker:
            parent = self.timestamp_tracker.find_parent_video(file_path)
            if parent:
                return self._lookup_media_info(parent)

        # 1. OnDeckTracker (current run, most authoritative for OnDeck items)
        if self.ondeck_tracker:
            ep_info = self.ondeck_tracker.get_episode_info(file_path)
            if ep_info is not None:
                return ("episode", ep_info)
            # Check if it's a known entry without episode_info (movie)
            entry = self.ondeck_tracker.get_entry(file_path)
            if entry is not None and 'episode_info' not in entry:
                return ("movie", None)

        # 2. media_info_map (covers watchlist items not yet cached)
        if self._media_info_map:
            info = self._media_info_map.get(file_path)
            if info and info.get("media_type"):
                return (info["media_type"], info.get("episode_info"))

        # 3. CacheTimestampTracker (persistent, covers cached files from prior runs)
        if self.timestamp_tracker:
            mt = self.timestamp_tracker.get_media_type(file_path)
            if mt:
                return (mt, self.timestamp_tracker.get_episode_info(file_path))

        return None  # Caller falls back to regex

    def _create_symlink(self, symlink_path: str, target_path: str) -> bool:
        """Create a symlink at symlink_path pointing to target_path.

        Non-fatal: logs warning on failure, returns False.
        """
        try:
            if os.path.islink(symlink_path):
                os.remove(symlink_path)
            parent_dir = os.path.dirname(symlink_path)
            if parent_dir and not os.path.isdir(parent_dir):
                os.makedirs(parent_dir, exist_ok=True)
            os.symlink(target_path, symlink_path)
            logging.debug(f"Created symlink: {symlink_path} -> {target_path}")
            return True
        except OSError as e:
            logging.warning(f"Could not create symlink at {symlink_path}: {e}")
            return False

    def _translate_to_host_path(self, cache_path: str) -> str:
        """Translate container cache path to host cache path for exclude file.

        In Docker, the container sees /mnt/cache but the host (Unraid mover)
        sees /mnt/cache_downloads. This translates using host_cache_path from path_mappings.
        """
        if not self.is_docker or not self.path_modifier:
            return cache_path

        # MultiPathModifier stores mappings in 'mappings' attribute, not 'path_mappings'
        path_mappings = getattr(self.path_modifier, 'mappings', [])

        for mapping in path_mappings:
            if not mapping.cache_path or not mapping.host_cache_path:
                continue
            if mapping.cache_path == mapping.host_cache_path:
                continue  # No translation needed

            cache_prefix = mapping.cache_path.rstrip('/')
            # Ensure prefix match is at a path boundary (not partial directory name)
            # e.g., /mnt/cache should NOT match /mnt/cache_downloads
            if cache_path == cache_prefix or cache_path.startswith(cache_prefix + '/'):
                host_prefix = mapping.host_cache_path.rstrip('/')
                translated = cache_path.replace(cache_prefix, host_prefix, 1)
                return translated

        return cache_path

    def _translate_from_host_path(self, host_path: str) -> str:
        """Translate host cache path back to container cache path for file existence checks.

        In Docker, the exclude file contains host paths like /mnt/cache_downloads but
        the container sees /mnt/cache. This reverse-translates for os.path.exists() checks.
        """
        if not self.is_docker or not self.path_modifier:
            return host_path

        path_mappings = getattr(self.path_modifier, 'mappings', [])

        for mapping in path_mappings:
            if not mapping.cache_path or not mapping.host_cache_path:
                continue
            if mapping.cache_path == mapping.host_cache_path:
                continue  # No translation needed

            host_prefix = mapping.host_cache_path.rstrip('/')
            # Ensure prefix match is at a path boundary (not partial directory name)
            if host_path == host_prefix or host_path.startswith(host_prefix + '/'):
                cache_prefix = mapping.cache_path.rstrip('/')
                translated = host_path.replace(host_prefix, cache_prefix, 1)
                return translated

        return host_path

    def _add_to_exclude_file(self, cache_file_name: str) -> None:
        """Add a file to the exclude list."""
        if self.mover_cache_exclude_file:
            # Translate container path to host path for exclude file (Docker)
            exclude_path = self._translate_to_host_path(cache_file_name)

            # Read existing entries to avoid duplicates
            existing = set()
            if os.path.exists(self.mover_cache_exclude_file):
                with open(self.mover_cache_exclude_file, "r") as f:
                    existing = {line.strip() for line in f if line.strip()}
            if exclude_path not in existing:
                with open(self.mover_cache_exclude_file, "a") as f:
                    f.write(f"{exclude_path}\n")
                if exclude_path != cache_file_name:
                    logging.debug(f"Added to exclude file (translated): {exclude_path}")
                else:
                    logging.debug(f"Added to exclude file: {exclude_path}")

    def filter_files(self, files: List[str], destination: str,
                    media_to_cache: Optional[List[str]] = None,
                    files_to_skip: Optional[Set[str]] = None) -> List[str]:
        """Filter files based on destination and conditions."""
        if media_to_cache is None:
            media_to_cache = []

        processed_files = set()
        media_to = []
        cache_files_to_exclude = []
        cache_files_removed = []  # Track cache files removed during filtering

        if not files:
            return []

        non_cacheable_count = 0
        for file in files:
            if file in processed_files or (files_to_skip and file in files_to_skip):
                continue
            processed_files.add(file)

            cache_path, cache_file_name = self._get_cache_paths(file)

            # Skip non-cacheable files (e.g., remote NAS in multi-path mode)
            if cache_file_name is None:
                non_cacheable_count += 1
                logging.debug(f"Skipping non-cacheable path: {file}")
                continue

            cache_files_to_exclude.append(cache_file_name)

            if destination == 'array':
                should_add, was_removed = self._should_add_to_array(file, cache_file_name, media_to_cache)
                if was_removed:
                    cache_files_removed.append(cache_file_name)
                if should_add:
                    media_to.append(file)
                    logging.debug(f"Adding file to array: {file}")

            elif destination == 'cache':
                if self._should_add_to_cache(file, cache_file_name):
                    media_to.append(file)
                    logging.debug(f"Adding file to cache: {file}")

        # Remove any cache files that were deleted during filtering from the exclude list
        if cache_files_removed:
            self.remove_files_from_exclude_list(cache_files_removed)

        # Log non-cacheable files summary
        if non_cacheable_count > 0:
            logging.info(f"Skipped {non_cacheable_count} files from non-cacheable path mappings")

        return media_to
    
    def _should_add_to_array(self, file: str, cache_file_name: str, media_to_cache: List[str]) -> Tuple[bool, bool]:
        """Determine if a file should be added to the array.

        Also detects when Radarr/Sonarr has upgraded a file - if the same media
        exists on array with a different quality, we should still move the
        upgraded version to array (handled by _move_to_array upgrade logic).

        Returns:
            Tuple of (should_add, cache_was_removed):
            - should_add: True if file should be added to array move queue
            - cache_was_removed: True if cache file was removed (needs exclude list update)
        """
        if file in media_to_cache:
            # Look up which users still need this file
            users = []
            if self.ondeck_tracker:
                entry = self.ondeck_tracker.get_entry(file)
                if entry:
                    users.extend(entry.get('users', []))
            if self.watchlist_tracker and not users:
                entry = self.watchlist_tracker.get_entry(file)
                if entry:
                    users.extend(entry.get('users', []))

            filename = os.path.basename(file)
            if users:
                user_list = ', '.join(users[:3])  # Show first 3 users
                if len(users) > 3:
                    user_list += f" +{len(users) - 3} more"
                logging.debug(f"Keeping in cache (OnDeck/Watchlist for {user_list}): {filename}")
            else:
                logging.debug(f"Keeping in cache (still needed): {filename}")
            return False, False

        # Note: Retention period check is handled upstream in get_files_to_move_back_to_array()
        # which correctly distinguishes between TV shows (retention applies) and movies (no retention)

        array_file = get_array_direct_path(file) if self.is_unraid else file
        array_path = os.path.dirname(array_file)

        # Check if exact file already exists on array (symlinks don't count — they point to cache)
        # Guard: If .plexcached exists, the original was renamed — don't trust the existence
        # check (on ZFS Unraid, /mnt/user0/ FUSE can show cache files as array files).
        # Let _move_to_array() handle the restore properly.
        plexcached_on_array = array_file + PLEXCACHED_EXTENSION
        if os.path.isfile(array_file) and not os.path.islink(array_file) and not os.path.isfile(plexcached_on_array):
            # File already exists in the array - check if there's a cache version to clean up
            cache_removed = False
            if os.path.isfile(cache_file_name):
                try:
                    os.remove(cache_file_name)
                    logging.info(f"Removed orphaned cache file (array copy exists): {os.path.basename(cache_file_name)}")
                    cache_removed = True
                except OSError as e:
                    logging.error(f"Failed to remove cache file {cache_file_name}: {type(e).__name__}: {e}")
            return False, cache_removed  # No need to add to array

        # Check for upgrade scenario: old .plexcached with different filename but same media identity
        # In this case, we still want to move the file so _move_to_array can handle the upgrade
        # NOTE: Only treat as upgrade if the .plexcached has a DIFFERENT name than expected
        expected_plexcached = array_file + PLEXCACHED_EXTENSION
        cache_identity = get_media_identity(cache_file_name)
        old_plexcached = find_matching_plexcached(array_path, cache_identity, cache_file_name)
        if old_plexcached and old_plexcached != expected_plexcached:
            # Found a .plexcached with different filename - this is a true upgrade scenario
            # Let _move_to_array handle it
            logging.debug(f"Found old .plexcached for upgrade: {old_plexcached}")
            return True, False

        return True, False  # File should be added to the array

    def protect_cached_file(self, file: str, cache_file_name: str = None) -> bool:
        """Protect a file that is already on the cache drive.

        Runs all protection side effects: adds to exclude list, records timestamp,
        renames array original to .plexcached backup, and re-creates symlinks if enabled.

        Args:
            file: The real/array path of the file.
            cache_file_name: Optional pre-resolved cache path. If None, resolved via _get_cache_paths().

        Returns:
            True if the file is on cache and was protected, False if not on cache.
        """
        if cache_file_name is None:
            _, cache_file_name = self._get_cache_paths(file)
            if cache_file_name is None:
                return False

        if not os.path.isfile(cache_file_name):
            return False

        array_file = get_array_direct_path(file) if self.is_unraid else file

        # Add to exclude list so Unraid mover doesn't move it back
        self._add_to_exclude_file(cache_file_name)

        # Record timestamp if not already tracked (for retention)
        if self.timestamp_tracker:
            self.timestamp_tracker.record_cache_time(cache_file_name, "pre-existing")

        # Mark as cached in OnDeck/Watchlist trackers
        if self.ondeck_tracker:
            self.ondeck_tracker.mark_cached(file, "pre-existing")
        if self.watchlist_tracker:
            self.watchlist_tracker.mark_cached(file, "pre-existing")

        logging.debug(f"File already on cache, added to exclude list: {os.path.basename(cache_file_name)}")

        # Track count of files already on cache
        self.last_already_cached_count += 1

        # If array version also exists, rename it to .plexcached (preserve as backup)
        # This ensures we have a recovery option if the cache drive fails
        #
        # Defense in depth: If array_file is a /mnt/user/ path (ZFS, no conversion),
        # probe /mnt/user0/ to verify a real array copy exists. On hybrid ZFS shares,
        # /mnt/user/ shows the cache file through FUSE — operating on it would destroy
        # the only copy.
        actual_array_file = array_file
        if array_file.startswith('/mnt/user/'):
            user0_path = '/mnt/user0/' + array_file[len('/mnt/user/'):]
            if os.path.isfile(user0_path):
                actual_array_file = user0_path
            elif os.path.exists('/mnt/user0'):
                # /mnt/user0 exists but file not there — FUSE is showing cache file
                logging.debug(f"Skipping array backup: file not at {user0_path} (FUSE/cache only)")
                actual_array_file = None

        # Symlinks don't count as real array files — they point to the cache copy
        if actual_array_file and os.path.isfile(actual_array_file) and not os.path.islink(actual_array_file):
            plexcached_file = actual_array_file + PLEXCACHED_EXTENSION
            # Only rename if .plexcached doesn't already exist
            if not os.path.isfile(plexcached_file):
                try:
                    os.rename(actual_array_file, plexcached_file)
                    logging.info(f"Created backup of array file: {os.path.basename(plexcached_file)}")
                except FileNotFoundError:
                    pass  # File already removed
                except OSError as e:
                    logging.error(f"Failed to create backup of array file {actual_array_file}: {type(e).__name__}: {e}")
                # Create symlink at original location if enabled
                if self.use_symlinks:
                    self._create_symlink(array_file, cache_file_name)
            else:
                # .plexcached backup already exists, safe to remove duplicate array file
                try:
                    os.remove(actual_array_file)
                    logging.debug(f"Removed redundant array file (backup exists): {os.path.basename(actual_array_file)}")
                except FileNotFoundError:
                    pass
                except OSError as e:
                    logging.error(f"Failed to remove array file {actual_array_file}: {type(e).__name__}: {e}")
                # Create symlink at original location if enabled
                if self.use_symlinks:
                    self._create_symlink(array_file, cache_file_name)

        # Re-create symlink if it's missing (e.g., Plex scan or manual deletion removed it)
        if self.use_symlinks and not os.path.islink(array_file) and not os.path.isfile(array_file):
            self._create_symlink(array_file, cache_file_name)

        return True

    def _should_add_to_cache(self, file: str, cache_file_name: str) -> bool:
        """Determine if a file should be added to the cache."""
        if os.path.isfile(cache_file_name):
            self.protect_cached_file(file, cache_file_name)
            return False
        return True
    
    def _get_cache_paths(self, file: str) -> Tuple[str, Optional[str]]:
        """Get cache path and filename for a given file.

        Returns:
            Tuple of (cache_path, cache_file_name).
            cache_file_name is None if the file is not cacheable (multi-path mode).
        """
        # Use multi-path modifier if available
        if self.path_modifier:
            cache_file_name, mapping = self.path_modifier.convert_real_to_cache(file)
            if cache_file_name is None:
                # File is not cacheable (e.g., remote NAS)
                return "", None
            cache_path = os.path.dirname(cache_file_name)
            return cache_path, cache_file_name

        # Legacy single-path mode
        cache_path = os.path.dirname(file).replace(self.real_source, self.cache_dir, 1)
        cache_file_name = os.path.join(cache_path, os.path.basename(file))
        return cache_path, cache_file_name

    def _build_needed_media_sets(self, current_ondeck_items: Set[str],
                                  current_watchlist_items: Set[str]) -> Tuple[Dict[str, Dict[int, int]], Set[str]]:
        """Build tracking sets of media that should be kept in cache.

        Uses Plex API metadata when available (from OnDeckTracker, media_info_map, or
        CacheTimestampTracker), falling back to regex path parsing for legacy entries.

        Args:
            current_ondeck_items: Set of OnDeck file paths.
            current_watchlist_items: Set of watchlist file paths.

        Returns:
            Tuple of (tv_show_min_episodes dict, needed_movies set).
            tv_show_min_episodes maps show_name -> {season: min_episode_to_keep}
        """
        tv_show_min_episodes: Dict[str, Dict[int, int]] = {}
        needed_movies: Set[str] = set()

        for item in current_ondeck_items | current_watchlist_items:
            # Try Plex API metadata first (avoids regex misclassification)
            lookup = self._lookup_media_info(item)
            if lookup:
                media_type, ep_info = lookup
                if media_type == "episode" and ep_info:
                    show_name = ep_info.get("show")
                    season_num = ep_info.get("season")
                    episode_num = ep_info.get("episode")
                    if show_name and season_num is not None and episode_num is not None:
                        if show_name not in tv_show_min_episodes:
                            tv_show_min_episodes[show_name] = {}
                        if season_num not in tv_show_min_episodes[show_name]:
                            tv_show_min_episodes[show_name][season_num] = episode_num
                        else:
                            tv_show_min_episodes[show_name][season_num] = min(
                                tv_show_min_episodes[show_name][season_num], episode_num
                            )
                        continue
                if media_type == "movie":
                    media_name = self._extract_media_name(item)
                    if media_name:
                        needed_movies.add(media_name)
                    continue

            # Fallback: regex classification (legacy/edge cases)
            tv_info = self._extract_tv_info(item)
            if tv_info:
                show_name, season_num, episode_num = tv_info
                if show_name not in tv_show_min_episodes:
                    tv_show_min_episodes[show_name] = {}
                if season_num not in tv_show_min_episodes[show_name]:
                    tv_show_min_episodes[show_name][season_num] = episode_num
                else:
                    tv_show_min_episodes[show_name][season_num] = min(
                        tv_show_min_episodes[show_name][season_num], episode_num
                    )
            else:
                media_name = self._extract_media_name(item)
                if media_name:
                    needed_movies.add(media_name)

        logging.debug(f"TV shows on deck/watchlist: {list(tv_show_min_episodes.keys())}")
        logging.debug(f"Movies on deck/watchlist: {len(needed_movies)}")
        return tv_show_min_episodes, needed_movies

    def _is_tv_episode_still_needed(self, show_name: str, season_num: int, episode_num: int,
                                     tv_show_min_episodes: Dict[str, Dict[int, int]]) -> bool:
        """Check if a TV episode should be kept in cache based on OnDeck position.

        Args:
            show_name: Name of the TV show.
            season_num: Season number of the episode.
            episode_num: Episode number.
            tv_show_min_episodes: Dict of show -> {season: min_episode}.

        Returns:
            True if episode should be kept, False if it can be moved back.
        """
        if show_name not in tv_show_min_episodes:
            return False  # Show not on deck/watchlist

        min_ondeck_season = min(tv_show_min_episodes[show_name].keys())

        if season_num < min_ondeck_season:
            # Previous season - user has moved past this
            logging.debug(f"TV episode in previous season (S{season_num:02d} < S{min_ondeck_season:02d}): {show_name}")
            return False
        elif season_num > min_ondeck_season:
            # Future season - keep (user may have pre-cached ahead)
            logging.debug(f"TV episode in future season, keeping: {show_name} S{season_num:02d}E{episode_num:02d}")
            return True
        else:
            # Same season - check episode number
            min_episode = tv_show_min_episodes[show_name][season_num]
            if episode_num >= min_episode:
                logging.debug(f"TV episode still needed (E{episode_num:02d} >= E{min_episode:02d}): {show_name}")
                return True
            else:
                logging.debug(f"TV episode watched (E{episode_num:02d} < E{min_episode:02d}): {show_name}")
                return False

    def get_files_to_move_back_to_array(self, current_ondeck_items: Set[str],
                                       current_watchlist_items: Set[str],
                                       files_to_skip: Optional[Set[str]] = None) -> Tuple[List[str], List[str], List[str]]:
        """Get files in cache that should be moved back to array because they're no longer needed.

        For TV shows: Episodes before the OnDeck episode are considered watched and will be moved back.
                      Episodes >= OnDeck episode are kept (they're upcoming/current).
        For movies: Moved back when no longer on OnDeck or watchlist.

        Retention period applies uniformly to all cached files to protect against
        accidental unwatching or watchlist removal.

        Args:
            current_ondeck_items: Set of file paths currently on deck.
            current_watchlist_items: Set of file paths currently on watchlist.
            files_to_skip: Optional set of file paths to skip (e.g., active sessions).
                          These files will NOT be marked for removal from exclude list.

        Returns:
            Tuple of (files_to_move_back, stale_entries, move_back_exclude_paths):
            - files_to_move_back: Array paths for files to move back.
            - stale_entries: Exclude list paths for files no longer on cache (safe to remove immediately).
            - move_back_exclude_paths: Exclude list paths for files being moved back
              (only safe to remove after the move succeeds).
        """
        files_to_move_back = []
        stale_entries = []
        move_back_exclude_paths = []
        retention_holds = []

        try:
            # Read exclude file
            if not os.path.exists(self.mover_cache_exclude_file):
                logging.info("No exclude file found, nothing to move back")
                return files_to_move_back, stale_entries, move_back_exclude_paths

            with open(self.mover_cache_exclude_file, 'r') as f:
                cache_files = [line.strip() for line in f if line.strip()]
            logging.debug(f"Found {len(cache_files)} files in exclude list")

            # Build tracking sets for needed media
            tv_show_min_episodes, needed_movies = self._build_needed_media_sets(
                current_ondeck_items, current_watchlist_items
            )

            # Check each cached file
            for cache_file in cache_files:
                # In Docker, exclude file has host paths but we need container paths to check existence
                check_path = self._translate_from_host_path(cache_file)
                if not os.path.exists(check_path):
                    logging.debug(f"Cache file no longer exists: {cache_file}")
                    stale_entries.append(cache_file)
                    continue

                # Try stored metadata first for classification (check_path matches timestamp tracker keys)
                tv_info = None
                lookup = self._lookup_media_info(check_path)
                if lookup:
                    media_type, ep_info = lookup
                    if media_type == "episode" and ep_info:
                        show = ep_info.get("show")
                        season = ep_info.get("season")
                        episode = ep_info.get("episode")
                        if show and season is not None and episode is not None:
                            tv_info = (show, season, episode)

                # Fallback to regex if no stored metadata
                if tv_info is None:
                    tv_info = self._extract_tv_info(cache_file)

                # Determine if file should be kept
                if tv_info:
                    show_name, season_num, episode_num = tv_info
                    if self._is_tv_episode_still_needed(show_name, season_num, episode_num, tv_show_min_episodes):
                        continue
                    media_name = show_name
                else:
                    media_name = self._extract_media_name(cache_file)
                    if media_name is None:
                        logging.warning(f"Could not extract media name from path: {cache_file}")
                        continue
                    if media_name in needed_movies:
                        logging.debug(f"Movie still needed, keeping in cache: {media_name}")
                        continue

                # Check retention period (use container path for timestamp lookup)
                if self.timestamp_tracker and self.cache_retention_hours > 0:
                    if self.timestamp_tracker.is_within_retention_period(check_path, self.cache_retention_hours):
                        remaining = self.timestamp_tracker.get_retention_remaining(check_path, self.cache_retention_hours)
                        display_name = self._extract_display_name(cache_file)
                        retention_holds.append((media_name, remaining, display_name))
                        remaining_str = f"{remaining:.0f}h" if remaining >= 1 else f"{remaining * 60:.0f}m"
                        logging.debug(f"Retention hold ({remaining_str} left): {display_name}")
                        continue

                # Skip files with active sessions - don't remove from exclude list (fixes #50)
                # The file may not be in ondeck/watchlist API response but is actively being played
                if files_to_skip and check_path in files_to_skip:
                    display_name = self._extract_display_name(cache_file)
                    logging.debug(f"Active session, keeping protected: {display_name}")
                    continue

                # Move file back to array (use container path for path conversion)
                if self.path_modifier:
                    array_file, _ = self.path_modifier.convert_cache_to_real(check_path)
                    if array_file is None:
                        logging.warning(f"Could not convert cache path to array path: {cache_file}")
                        continue
                else:
                    array_file = check_path.replace(self.cache_dir, self.real_source, 1)

                display_name = self._extract_display_name(cache_file)
                logging.debug(f"Media no longer needed, will move back to array: {display_name} - {cache_file}")
                files_to_move_back.append(array_file)
                move_back_exclude_paths.append(cache_file)

            # Log retention summary
            if retention_holds:
                grouped = self._group_retention_holds(retention_holds)
                for line in self._format_retention_summary(grouped):
                    logging.info(line)
            if files_to_move_back:
                logging.debug(f"Found {len(files_to_move_back)} files to move back to array")

        except Exception as e:
            logging.exception(f"Error getting files to move back to array: {type(e).__name__}: {e}")

        return files_to_move_back, stale_entries, move_back_exclude_paths

    def _extract_tv_info(self, file_path: str) -> Optional[Tuple[str, int, int]]:
        """
        Extract TV show information from a file path.
        Returns (show_name, season_number, episode_number) or None if not a TV show.
        """
        try:
            normalized_path = os.path.normpath(file_path)
            path_parts = normalized_path.split(os.sep)

            # Find show name from folder structure
            show_name = None
            season_num = None

            for i, part in enumerate(path_parts):
                # Match Season folders
                season_match = re.match(r'^(Season|Series)\s*(\d+)', part, re.IGNORECASE)
                if season_match:
                    season_num = int(season_match.group(2))
                    if i > 0:
                        show_name = path_parts[i - 1]
                    break
                # Match numeric-only season folders
                if re.match(r'^\d+$', part):
                    season_num = int(part)
                    if i > 0:
                        show_name = path_parts[i - 1]
                    break
                # Match Specials folder (treat as season 0)
                if re.match(r'^Specials$', part, re.IGNORECASE):
                    season_num = 0
                    if i > 0:
                        show_name = path_parts[i - 1]
                    break

            if show_name is None or season_num is None:
                return None

            # Extract episode number from filename (e.g., "Show - S01E03 - Title.mkv")
            filename = os.path.basename(file_path)

            # Pattern 1: S01E02, S1E2, s01e02 (most common)
            ep_match = re.search(r'[Ss](\d+)\s*[Ee](\d+)', filename)
            if ep_match:
                episode_num = int(ep_match.group(2))
                return (show_name, season_num, episode_num)

            # Pattern 2: 1x02, 1 x 02, 01x02 (alternate format)
            ep_match = re.search(r'(\d+)\s*x\s*(\d+)', filename, re.IGNORECASE)
            if ep_match:
                episode_num = int(ep_match.group(2))
                return (show_name, season_num, episode_num)

            # Pattern 3: Episode 02, Ep 02, E02 (standalone episode)
            ep_match = re.search(r'(?:Episode|Ep\.?|E)\s*(\d+)', filename, re.IGNORECASE)
            if ep_match:
                episode_num = int(ep_match.group(1))
                return (show_name, season_num, episode_num)

            return None

        except Exception:
            return None

    def _extract_media_name(self, file_path: str) -> Optional[str]:
        """
        Extract a comparable media identifier from a file path.
        - For movies: returns cleaned file title
        - For TV shows: returns show name (but episode comparison is handled separately)
        """
        try:
            normalized_path = os.path.normpath(file_path)
            path_parts = normalized_path.split(os.sep)

            # Check if this is a TV show
            for i, part in enumerate(path_parts):
                if (
                    re.match(r'^(Season|Series)\s*\d+', part, re.IGNORECASE)
                    or re.match(r'^\d+$', part)
                    or re.match(r'^Specials$', part, re.IGNORECASE)
                ):
                    if i > 0:
                        return path_parts[i - 1]
                    break

            # For movies: return cleaned filename
            filename = os.path.basename(file_path)
            name, ext = os.path.splitext(filename)

            # Handle subtitle files - strip language code suffixes (e.g., ".en", ".eng", ".en.hi", ".forced")
            subtitle_extensions = {'.srt', '.sub', '.ass', '.ssa', '.vtt', '.idx'}
            if ext.lower() in subtitle_extensions:
                # Strip common language code patterns from the end (loop for multiple suffixes like ".en.hi")
                pattern = r'\.(en|eng|es|spa|fr|fra|de|deu|ger|it|ita|pt|por|ja|jpn|ko|kor|zh|chi|forced|sdh|cc|hi)$'
                prev_name = None
                while prev_name != name:
                    prev_name = name
                    name = re.sub(pattern, '', name, flags=re.IGNORECASE)

            cleaned = re.sub(r'\s*\([^)]*\)$', '', name).strip()
            return cleaned

        except Exception:
            return None

    def _extract_display_name(self, file_path: str) -> str:
        """Extract a human-readable display name from a file path.

        For TV shows: Returns "Show - S##E## - Title" format
        For movies: Returns "Movie Title (Year)" format

        Args:
            file_path: Full path to the media file

        Returns:
            Human-readable display name
        """
        try:
            filename = os.path.basename(file_path)
            name = os.path.splitext(filename)[0]

            # Remove quality/codec info in brackets
            if '[' in name:
                name = name[:name.index('[')].strip()

            # Clean up trailing dashes
            name = name.rstrip(' -').rstrip('-').strip()

            return name if name else os.path.basename(file_path)
        except Exception:
            return os.path.basename(file_path)

    def _group_retention_holds(self, holds: List[Tuple[str, float, str]]) -> Dict[str, List[Tuple[float, str]]]:
        """Group retention holds by media title.

        Args:
            holds: List of (media_name, hours_remaining, display_name) tuples

        Returns:
            Dict mapping media_name to list of (hours_remaining, display_name) tuples
        """
        from collections import defaultdict
        grouped = defaultdict(list)
        for media_name, hours, display_name in holds:
            grouped[media_name].append((hours, display_name))
        return grouped

    def _format_retention_summary(self, grouped: Dict[str, List[Tuple[float, str]]], max_titles: int = 6) -> List[str]:
        """Format grouped retention holds for logging.

        Args:
            grouped: Dict from _group_retention_holds()
            max_titles: Maximum titles to show before summarizing

        Returns:
            List of formatted log lines
        """
        lines = []
        total_count = sum(len(v) for v in grouped.values())

        if total_count == 0:
            return lines

        # Use "episodes" for TV shows (majority of cached content)
        unit = "episode" if total_count == 1 else "episodes"
        lines.append(f"Retention holds ({total_count} {unit}):")

        # Sort by count descending
        sorted_titles = sorted(grouped.items(), key=lambda x: len(x[1]), reverse=True)

        shown_count = 0
        for i, (title, entries) in enumerate(sorted_titles):
            if i >= max_titles:
                remaining_titles = len(sorted_titles) - max_titles
                remaining_count = total_count - shown_count
                unit = "episode" if remaining_count == 1 else "episodes"
                lines.append(f"  ...and {remaining_titles} more titles ({remaining_count} {unit})")
                break

            hours_list = [h for h, _ in entries]
            min_h, max_h = min(hours_list), max(hours_list)
            # Compare rounded values to avoid "3-3h" when values like 3.2 and 3.8 round to same
            min_rounded, max_rounded = round(min_h), round(max_h)
            if min_rounded == max_rounded:
                time_str = f"{min_rounded}h" if min_rounded >= 1 else f"{round(min_h * 60)}m"
            else:
                time_str = f"{min_rounded}-{max_rounded}h"

            count = len(entries)
            unit = "episode" if count == 1 else "episodes"
            lines.append(f"  {title}: {count} {unit} ({time_str} remaining)")
            shown_count += count

        return lines

    def remove_files_from_exclude_list(self, cache_paths_to_remove: List[str]) -> bool:
        """Remove specified files from the exclude list. Returns True on success."""
        try:
            if not os.path.exists(self.mover_cache_exclude_file):
                logging.warning("Exclude file does not exist, cannot remove files")
                return False

            # Read current exclude list
            with open(self.mover_cache_exclude_file, 'r') as f:
                current_files = [line.strip() for line in f if line.strip()]

            original_count = len(current_files)

            # Translate container paths to host paths (Docker path mapping)
            # The exclude file contains host paths, but we receive container paths
            paths_to_remove_set = set(
                self._translate_to_host_path(p) for p in cache_paths_to_remove
            )

            # Remove specified files
            updated_files = [f for f in current_files if f not in paths_to_remove_set]

            # Only write if we actually removed something
            removed_count = original_count - len(updated_files)
            if removed_count > 0:
                with open(self.mover_cache_exclude_file, 'w') as f:
                    for file_path in updated_files:
                        f.write(f"{file_path}\n")
                logging.info(f"Cleaned up {removed_count} stale entries from exclude list")

            return True

        except Exception as e:
            logging.exception(f"Error removing files from exclude list: {type(e).__name__}: {e}")
            return False

    def clean_stale_exclude_entries(self) -> int:
        """
        Remove exclude list entries for files that no longer exist on cache.

        This is a self-healing mechanism: if files are manually deleted from cache,
        or if the cache drive has issues, stale entries are automatically cleaned up.

        Does NOT add new files - only removes entries where the file no longer exists.
        This ensures we don't interfere with Mover Tuning's management of other files.

        Returns:
            Number of stale entries removed.
        """
        if not self.mover_cache_exclude_file or not os.path.exists(self.mover_cache_exclude_file):
            return 0

        try:
            with open(self.mover_cache_exclude_file, 'r') as f:
                current_entries = [line.strip() for line in f if line.strip()]

            if not current_entries:
                return 0

            # Keep only entries where file still exists
            valid_entries = []
            stale_entries = []

            for entry in current_entries:
                # In Docker, exclude file has host paths but we need container paths to check existence
                check_path = self._translate_from_host_path(entry)
                if os.path.exists(check_path):
                    valid_entries.append(entry)
                else:
                    stale_entries.append(entry)
                    logging.debug(f"Removing stale exclude entry: {entry}")

            # Only rewrite file if we found stale entries
            if stale_entries:
                with open(self.mover_cache_exclude_file, 'w') as f:
                    for entry in valid_entries:
                        f.write(entry + '\n')
                logging.info(f"Cleaned {len(stale_entries)} stale entries from exclude list")

            return len(stale_entries)

        except Exception as e:
            logging.warning(f"Error cleaning stale exclude entries: {type(e).__name__}: {e}")
            return 0


class _ByteProgressAggregator:
    """Thread-safe byte progress aggregator for parallel file operations.

    Collects per-chunk byte updates from multiple workers and reports
    aggregate (total_copied, total_bytes) to an external callback.
    """

    def __init__(self, total_bytes: int, external_callback: Optional[Callable]):
        self._lock = threading.Lock()
        self._total_bytes = total_bytes
        self._copied_bytes = 0
        self._external_callback = external_callback

    def make_worker_callback(self) -> Callable:
        """Create a per-worker callback that aggregates byte progress."""
        last_reported = [0]

        def callback(copied: int, file_size: int):
            delta = copied - last_reported[0]
            last_reported[0] = copied
            with self._lock:
                self._copied_bytes += delta
                if self._external_callback:
                    self._external_callback(self._copied_bytes, self._total_bytes)

        return callback


class FileMover:
    """Handles file moving operations.

    For moves TO CACHE:
    - Copy file from array to cache
    - Rename array file to .plexcached (preserves original on array)
    - Add to exclude file
    - Record timestamp for cache retention

    For moves TO ARRAY:
    - Rename .plexcached file back to original name
    - Delete cache copy
    - Remove from exclude file
    - Remove timestamp entry
    """

    def __init__(self, real_source: str, cache_dir: str, is_unraid: bool,
                 file_utils, debug: bool = False, mover_cache_exclude_file: Optional[str] = None,
                 timestamp_tracker: Optional['CacheTimestampTracker'] = None,
                 path_modifier: Optional['MultiPathModifier'] = None,
                 stop_check: Optional[Callable[[], bool]] = None,
                 create_plexcached_backups: bool = True,
                 hardlinked_files: str = "skip",
                 cleanup_empty_folders: bool = True,
                 use_symlinks: bool = False,
                 bytes_progress_callback: Optional[Callable[[int, int], None]] = None,
                 ondeck_tracker: Optional['OnDeckTracker'] = None,
                 watchlist_tracker: Optional['WatchlistTracker'] = None):
        self.real_source = real_source
        self.cache_dir = cache_dir
        self.is_unraid = is_unraid
        self.file_utils = file_utils
        self.debug = debug
        self.mover_cache_exclude_file = mover_cache_exclude_file
        self.timestamp_tracker = timestamp_tracker
        self.path_modifier = path_modifier  # For multi-path support
        self._stop_check = stop_check  # Callback to check if stop requested
        self.create_plexcached_backups = create_plexcached_backups  # Whether to create .plexcached backups
        self.hardlinked_files = hardlinked_files  # How to handle hard-linked files: "skip" or "move"
        self.cleanup_empty_folders = cleanup_empty_folders  # Whether to remove empty parent folders after moves
        self.use_symlinks = use_symlinks  # Whether to create symlinks at original locations after caching
        self._bytes_progress_callback = bytes_progress_callback  # Byte-level progress for operation banner
        self.ondeck_tracker = ondeck_tracker
        self.watchlist_tracker = watchlist_tracker
        self._exclude_file_lock = threading.Lock()
        # Progress tracking
        self._progress_lock = threading.Lock()
        self._completed_count = 0
        self._total_count = 0
        self._completed_bytes = 0
        self._total_bytes = 0
        self._active_files = {}  # Thread ID -> (filename, size)
        self._last_display_lines = 0
        # Source tracking: maps cache file paths to their source (ondeck/watchlist)
        self._source_map: Dict[str, str] = {}
        # Track actual moves by destination for accurate reporting
        self.last_cache_moves_count = 0
        # Flag to signal stop to running threads
        self._stop_requested = False
        # Hard-link tracking: maps cache file paths to inode numbers for restoration
        self._hardlink_inodes: Dict[str, int] = {}
        # Track successful array moves for deferred exclude list cleanup (issue #13)
        self._successful_array_moves: List[str] = []
        self._successful_array_moves_lock = threading.Lock()

    def move_media_files(self, files: List[str], destination: str,
                        max_concurrent_moves_array: int, max_concurrent_moves_cache: int,
                        source_map: Optional[Dict[str, str]] = None,
                        media_info_map: Optional[Dict[str, Dict]] = None) -> None:
        """Move media files to the specified destination.

        Args:
            files: List of file paths to move.
            destination: Either 'cache' or 'array'.
            max_concurrent_moves_array: Max concurrent moves to array.
            max_concurrent_moves_cache: Max concurrent moves to cache.
            source_map: Optional dict mapping file paths to their source ('ondeck' or 'watchlist').
            media_info_map: Optional dict mapping file paths to Plex media type info.
        """
        # Store source map and media info map for use during moves
        self._source_map = source_map or {}
        self._media_info_map = media_info_map or {}
        # Reset successful array moves tracker for deferred exclude list cleanup
        if destination == 'array':
            self._successful_array_moves = []
        logging.debug(f"Moving media files to {destination}...")
        logging.debug(f"Total files to process: {len(files)}")

        processed_files = set()
        move_commands = []
        total_bytes = 0

        # Iterate over each file to move
        for file_to_move in files:
            if file_to_move in processed_files:
                continue

            processed_files.add(file_to_move)

            # Get the user path, cache path, cache file name, and user file name
            user_path, cache_path, cache_file_name, user_file_name = self._get_paths(file_to_move)

            # Get the move command for the current file
            move = self._get_move_command(destination, cache_file_name, user_path, user_file_name, cache_path)

            if move is not None:
                # Get file size for progress tracking
                src_file = move[0]
                try:
                    file_size = os.path.getsize(src_file)
                except OSError:
                    file_size = 0
                total_bytes += file_size
                # Include original file_to_move path for source map lookup
                move_commands.append((move, cache_file_name, file_size, file_to_move))
                logging.debug(f"Added move command for: {file_to_move}")
            else:
                logging.debug(f"No move command generated for: {file_to_move}")

        logging.debug(f"Generated {len(move_commands)} move commands for {destination}")

        # Track actual cache moves for accurate diagnostic reporting
        if destination == 'cache':
            self.last_cache_moves_count = len(move_commands)

        # Execute the move commands
        self._execute_move_commands(move_commands, max_concurrent_moves_array,
                                  max_concurrent_moves_cache, destination, total_bytes)
    
    def _get_paths(self, file_to_move: str) -> Tuple[str, str, str, str]:
        """Get all necessary paths for file moving.

        Returns:
            Tuple of (user_path, cache_path, cache_file_name, user_file_name).
        """
        # Get the user path
        user_path = os.path.dirname(file_to_move)

        # Use multi-path modifier if available
        if self.path_modifier:
            cache_file_name, mapping = self.path_modifier.convert_real_to_cache(file_to_move)
            if cache_file_name is None:
                # This shouldn't happen - non-cacheable files should be filtered earlier
                logging.warning(f"Non-cacheable file reached FileMover: {file_to_move}")
                logging.debug(f"Path conversion failed - input: {file_to_move}")
                # Fall back to legacy behavior
                relative_path = os.path.relpath(user_path, self.real_source)
                cache_path = os.path.join(self.cache_dir, relative_path)
                cache_file_name = os.path.join(cache_path, os.path.basename(file_to_move))
            else:
                cache_path = os.path.dirname(cache_file_name)
                logging.debug(f"Path conversion: {file_to_move} -> {cache_file_name} (mapping: {mapping.name if mapping else 'None'})")
        else:
            # Legacy single-path mode
            relative_path = os.path.relpath(user_path, self.real_source)
            cache_path = os.path.join(self.cache_dir, relative_path)
            cache_file_name = os.path.join(cache_path, os.path.basename(file_to_move))

        # Modify the user path if unraid is True
        if self.is_unraid:
            user_path = get_array_direct_path(user_path)

        # Get the user file name by joining the user path with the base name of the file to move
        user_file_name = os.path.join(user_path, os.path.basename(file_to_move))

        return user_path, cache_path, cache_file_name, user_file_name
    
    def _get_move_command(self, destination: str, cache_file_name: str,
                         user_path: str, user_file_name: str, cache_path: str) -> Optional[Tuple[str, str]]:
        """Get the move command for a file.

        For cache destination:
        - If file already on cache: just add to exclude (return None, handled separately)
        - If file on array: return command to copy+rename

        For array destination:
        - If .plexcached file exists: return command to restore+delete cache copy
        - If file exists on cache but no .plexcached: return command to copy to array+delete cache copy
        - If file already exists on array: skip (return None)
        """
        move = None
        if destination == 'array':
            # Check if file already exists on array (no action needed)
            # A symlink to the cache file makes isfile() return True, so exclude symlinks
            # Guard: If .plexcached exists, the original was renamed — don't trust the
            # existence check (ZFS FUSE leak). Let _move_to_array() handle the restore.
            plexcached_on_array = user_file_name + PLEXCACHED_EXTENSION
            if os.path.isfile(user_file_name) and not os.path.islink(user_file_name) and not os.path.isfile(plexcached_on_array):
                logging.debug(f"File already exists on array, skipping: {user_file_name}")
                return None

            # Check if .plexcached version exists on array (restore scenario)
            plexcached_file = user_file_name + PLEXCACHED_EXTENSION
            if os.path.isfile(plexcached_file):
                if not self.debug:
                    self.file_utils.create_directory_with_permissions(user_path, cache_file_name)
                move = (cache_file_name, user_path)
                logging.debug(f"Will restore from .plexcached: {plexcached_file}")
            # Check if file exists on cache but has no .plexcached backup (copy scenario)
            elif os.path.isfile(cache_file_name):
                if not self.debug:
                    self.file_utils.create_directory_with_permissions(user_path, cache_file_name)
                move = (cache_file_name, user_path)
                logging.debug(f"Will copy from cache (no .plexcached): {cache_file_name}")
            else:
                logging.warning(f"Cannot move to array - file not found on cache or as .plexcached: {cache_file_name}")
        elif destination == 'cache':
            # Debug: Log the paths being checked for cache operations
            cache_exists = os.path.isfile(cache_file_name)
            array_exists = os.path.isfile(user_file_name)
            logging.debug(f"Cache path check: {cache_file_name} exists={cache_exists}")
            logging.debug(f"Array path check: {user_file_name} exists={array_exists}")

            # Check if file is already on cache
            if cache_exists:
                # File already on cache - ensure it's in exclude file
                self._add_to_exclude_file(cache_file_name)

                # Check for stale exclude entries from upgrades (e.g., Radarr replaced the file)
                # Same media identity but different filename = old entry is stale
                self._cleanup_stale_exclude_entries(cache_file_name)

                logging.debug(f"File already on cache, ensured in exclude list: {os.path.basename(cache_file_name)}")
                return None

            # Check if file exists on array to copy
            if array_exists:
                # Check for hard links - files with multiple hard links (e.g., from jdupes
                # for seeding) require special handling:
                # - FUSE has issues renaming hard-linked files to .plexcached
                # - But we can still cache them by deleting the array link instead of renaming
                # - The other hard link (e.g., in downloads/) preserves the data for seeding
                is_hardlinked = False
                inode = None
                try:
                    stat_info = os.stat(user_file_name)
                    if stat_info.st_nlink > 1:
                        is_hardlinked = True
                        inode = stat_info.st_ino
                        if self.hardlinked_files == "skip":
                            logging.warning(
                                f"Skipping hard-linked file (has {stat_info.st_nlink} links): "
                                f"{os.path.basename(user_file_name)} - Set hardlinked_files to 'move' to cache these files"
                            )
                            return None
                        else:  # hardlinked_files == "move"
                            logging.info(
                                f"Caching hard-linked file (has {stat_info.st_nlink} links, seed copy preserved): "
                                f"{os.path.basename(user_file_name)}"
                            )
                            # Track inode for potential hard-link restoration when moving back
                            self._hardlink_inodes[cache_file_name] = inode
                except OSError as e:
                    logging.debug(f"Could not check hard link count for {user_file_name}: {e}")

                # Only create directories if not in debug mode (true dry-run)
                if not self.debug:
                    self.file_utils.create_directory_with_permissions(cache_path, user_file_name)
                move = (user_file_name, cache_path)
        return move

    def _translate_to_host_path(self, cache_path: str, log_translation: bool = False) -> str:
        """Translate container cache path to host cache path.

        In Docker, the container might see /mnt/cache/Movies/... but the host
        (where Unraid mover runs) sees /mnt/cache_downloads/Movies/...
        This method translates paths using the host_cache_path from path_mappings.

        Used for:
        - Exclude file entries (so Unraid mover sees correct paths)
        - Log display (so users see actual host paths, not container paths)

        Args:
            cache_path: The cache path as seen by the container
            log_translation: If True, log debug message when translation occurs

        Returns:
            The translated path for the host, or original if no translation needed
        """
        if not self.path_modifier:
            return cache_path

        # MultiPathModifier stores mappings in 'mappings' attribute
        path_mappings = getattr(self.path_modifier, 'mappings', [])

        for mapping in path_mappings:
            if not mapping.cache_path or not mapping.host_cache_path:
                continue
            if mapping.cache_path == mapping.host_cache_path:
                continue  # No translation needed

            cache_prefix = mapping.cache_path.rstrip('/')
            # Ensure prefix match is at a path boundary (not partial directory name)
            # e.g., /mnt/cache should NOT match /mnt/cache_downloads
            if cache_path == cache_prefix or cache_path.startswith(cache_prefix + '/'):
                host_prefix = mapping.host_cache_path.rstrip('/')
                translated = cache_path.replace(cache_prefix, host_prefix, 1)
                if log_translation:
                    logging.debug(f"Path translation: {cache_path} -> {translated}")
                return translated

        return cache_path

    def _translate_from_host_path(self, host_path: str) -> str:
        """Translate host cache path back to container cache path.

        Reverse of _translate_to_host_path. Used when reading entries from the
        exclude file (which are in host path format) and needing to check file
        existence inside the container.

        Args:
            host_path: The host path (as stored in exclude file)

        Returns:
            The container path for file operations, or original if no translation needed
        """
        if not self.path_modifier:
            return host_path

        path_mappings = getattr(self.path_modifier, 'mappings', [])

        for mapping in path_mappings:
            if not mapping.cache_path or not mapping.host_cache_path:
                continue
            if mapping.cache_path == mapping.host_cache_path:
                continue  # No translation needed

            host_prefix = mapping.host_cache_path.rstrip('/')
            # Ensure prefix match is at a path boundary (not partial directory name)
            if host_path == host_prefix or host_path.startswith(host_prefix + '/'):
                cache_prefix = mapping.cache_path.rstrip('/')
                translated = host_path.replace(host_prefix, cache_prefix, 1)
                return translated

        return host_path

    def _add_to_exclude_file(self, cache_file_name: str) -> None:
        """Add a file to the exclude list (thread-safe).

        The path is translated to host cache path if running in Docker with
        different volume mappings (e.g., container sees /mnt/cache but host
        sees /mnt/cache_downloads).
        """
        if self.mover_cache_exclude_file:
            # Translate container path to host path for exclude file
            exclude_path = self._translate_to_host_path(cache_file_name)

            with self._exclude_file_lock:
                # Read existing entries to avoid duplicates
                existing = set()
                if os.path.exists(self.mover_cache_exclude_file):
                    with open(self.mover_cache_exclude_file, "r") as f:
                        existing = {line.strip() for line in f if line.strip()}
                if exclude_path not in existing:
                    with open(self.mover_cache_exclude_file, "a") as f:
                        f.write(f"{exclude_path}\n")
                    if exclude_path != cache_file_name:
                        logging.debug(f"Added to exclude file (translated): {exclude_path}")
                    else:
                        logging.debug(f"Added to exclude file: {exclude_path}")
                else:
                    logging.debug(f"Already in exclude file: {exclude_path}")
        else:
            logging.warning(f"No exclude file configured, cannot track: {cache_file_name}")

    def _remove_from_exclude_file(self, cache_file_name: str) -> None:
        """Remove a file from the exclude list (thread-safe).

        The path is translated to host cache path to match what was written.
        """
        if self.mover_cache_exclude_file and os.path.exists(self.mover_cache_exclude_file):
            # Translate container path to host path for exclude file
            exclude_path = self._translate_to_host_path(cache_file_name)

            with self._exclude_file_lock:
                try:
                    with open(self.mover_cache_exclude_file, "r") as f:
                        lines = [line.strip() for line in f if line.strip()]
                    if exclude_path in lines:
                        lines.remove(exclude_path)
                        with open(self.mover_cache_exclude_file, "w") as f:
                            for line in lines:
                                f.write(f"{line}\n")
                        logging.debug(f"Removed from exclude file: {exclude_path}")
                except Exception as e:
                    logging.warning(f"Failed to remove from exclude file: {e}")

    def _cleanup_stale_exclude_entries(self, current_cache_file: str) -> None:
        """Remove stale exclude entries for the same media with different filenames.

        When Radarr/Sonarr upgrades a file on the cache, the old filename becomes stale
        in the exclude list. This finds and removes those entries.

        Note: Exclude file entries are in host path format (translated), so we need
        to translate paths before comparison.
        """
        if not self.mover_cache_exclude_file or not os.path.exists(self.mover_cache_exclude_file):
            return

        current_identity = get_media_identity(current_cache_file)
        # Translate to host path format (what's in the exclude file)
        current_host_path = self._translate_to_host_path(current_cache_file)
        current_dir = os.path.dirname(current_host_path)

        with self._exclude_file_lock:
            try:
                with open(self.mover_cache_exclude_file, "r") as f:
                    lines = [line.strip() for line in f if line.strip()]

                stale_entries = []
                for entry in lines:
                    # Skip if it's the current file (already in host path format)
                    if entry == current_host_path:
                        continue

                    # Only check entries in the same directory (same media folder)
                    if os.path.dirname(entry) != current_dir:
                        continue

                    # Check if same media identity but file no longer exists
                    # Note: entry is in host path format, need container path for existence check
                    entry_identity = get_media_identity(entry)
                    container_path = self._translate_from_host_path(entry)
                    if entry_identity == current_identity and not os.path.exists(container_path):
                        stale_entries.append(entry)

                if stale_entries:
                    updated_lines = [line for line in lines if line not in stale_entries]
                    with open(self.mover_cache_exclude_file, "w") as f:
                        for line in updated_lines:
                            f.write(f"{line}\n")
                    for entry in stale_entries:
                        old_name = os.path.basename(entry)
                        new_name = os.path.basename(current_cache_file)
                        logging.info(f"Cleaned up stale exclude entry from upgrade: {old_name} -> {new_name}")

            except Exception as e:
                logging.warning(f"Failed to cleanup stale exclude entries: {e}")

    def _execute_move_commands(self, move_commands: List[Tuple[Tuple[str, str], str, int]],
                             max_concurrent_moves_array: int, max_concurrent_moves_cache: int,
                             destination: str, total_bytes: int) -> None:
        """Execute the move commands with progress tracking using tqdm."""
        from tqdm import tqdm

        total_count = len(move_commands)
        if total_count == 0:
            return

        # Initialize shared progress state for tqdm
        self._tqdm_pbar = None
        self._completed_bytes = 0
        self._total_bytes = total_bytes

        # Byte-level progress aggregator for operation banner
        self._byte_aggregator = None
        if self._bytes_progress_callback and total_bytes > 0 and not self.debug:
            self._byte_aggregator = _ByteProgressAggregator(total_bytes, self._bytes_progress_callback)
            self._bytes_progress_callback(0, total_bytes)  # Signal batch start

        # Get console lock for thread-safe tqdm output
        console_lock = get_console_lock()

        if self.debug:
            # Debug mode - no actual moves, just log what would happen
            with tqdm(total=total_count, desc=f"Moving to {destination}", unit="file",
                      bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]") as pbar:
                for move_cmd, cache_file_name, file_size, original_path in move_commands:
                    (src, dest) = move_cmd
                    if destination == 'cache':
                        plexcached_file = src + PLEXCACHED_EXTENSION
                        with console_lock:
                            tqdm.write(f"[DEBUG] Would copy: {src} -> {cache_file_name}")
                            tqdm.write(f"[DEBUG] Would rename: {src} -> {plexcached_file}")
                    elif destination == 'array':
                        array_file = os.path.join(dest, os.path.basename(src))
                        plexcached_file = array_file + PLEXCACHED_EXTENSION
                        with console_lock:
                            tqdm.write(f"[DEBUG] Would rename: {plexcached_file} -> {array_file}")
                            tqdm.write(f"[DEBUG] Would delete: {src}")
                    pbar.update(1)
        else:
            # Real move with thread pool
            max_concurrent_moves = max_concurrent_moves_array if destination == 'array' else max_concurrent_moves_cache

            # Create tqdm progress bar with data size info
            # ncols=80 keeps bar compact, mininterval=0.5 forces more frequent updates
            import sys
            from concurrent.futures import as_completed
            total_size_str = format_bytes(total_bytes)

            # Reset stop flag for this batch
            self._stop_requested = False
            stopped_early = False
            cancelled_count = 0

            with tqdm(total=total_count, desc=f"Moving to {destination} (0 B / {total_size_str})",
                      unit="file", bar_format="{l_bar}{bar:20}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
                      mininterval=0.5, ncols=80, file=sys.stdout) as pbar:
                self._tqdm_pbar = pbar

                from concurrent.futures import wait, FIRST_COMPLETED
                results = []

                with ThreadPoolExecutor(max_workers=max_concurrent_moves) as executor:
                    # Throttled submission: only keep max_workers tasks in flight
                    # This allows stop requests to take effect quickly
                    pending = set()
                    cmd_iter = iter(move_commands)
                    all_submitted = False

                    while True:
                        # Check for stop request
                        if self._stop_check and self._stop_check():
                            self._stop_requested = True
                        if self._stop_requested:
                            stopped_early = True
                            # Cancel any pending (not-yet-started) futures
                            for f in pending:
                                if f.cancel():
                                    cancelled_count += 1
                            logging.info(f"Stop requested - cancelling remaining file moves")
                            break

                        # Submit new tasks up to max_workers (only if not all submitted)
                        while not all_submitted and len(pending) < max_concurrent_moves:
                            try:
                                move_cmd = next(cmd_iter)
                                future = executor.submit(self._move_file, move_cmd, destination)
                                pending.add(future)
                            except StopIteration:
                                all_submitted = True
                                break

                        # Exit if no pending tasks
                        if not pending:
                            break

                        # Wait for at least one task to complete (with 1s timeout for stop checks)
                        done, pending = wait(pending, timeout=1.0, return_when=FIRST_COMPLETED)
                        for future in done:
                            try:
                                results.append(future.result())
                            except Exception as e:
                                logging.error(f"Move task failed: {e}")
                                results.append(1)  # Error code

                    # Collect any remaining results if we stopped early
                    if stopped_early and pending:
                        # Wait for in-progress tasks to finish (they'll stop at next chunk boundary)
                        # No timeout needed - copies check stop flag every 10MB (~1 sec max)
                        done, still_pending = wait(pending, timeout=30.0)  # 30s safety timeout
                        for future in done:
                            try:
                                results.append(future.result())
                            except Exception as e:
                                results.append(1)
                        # Any truly stuck tasks get counted as errors
                        for future in still_pending:
                            results.append(1)

                errors = [result for result in results if result == 1]
                partial_successes = [result for result in results if result == 2]
                skipped_space = [result for result in results if result == 3]
                stopped_copies = [result for result in results if result == 4]

            self._tqdm_pbar = None

            # Build summary message based on what happened
            issues = []
            if stopped_early:
                skipped = total_count - len(results) - cancelled_count
                if skipped > 0 or cancelled_count > 0:
                    issues.append(f"stopped ({cancelled_count} cancelled, {skipped} skipped)")
            if stopped_copies:
                issues.append(f"{len(stopped_copies)} copies cancelled mid-transfer")
            if errors:
                issues.append(f"{len(errors)} errors")
            if partial_successes:
                issues.append(f"{len(partial_successes)} partial (missing .plexcached)")
            if skipped_space:
                issues.append(f"{len(skipped_space)} skipped (insufficient disk space)")

            if issues:
                logging.warning(f"Finished moving files: {', '.join(issues)}")
            else:
                logging.debug(f"Finished moving {total_count} files successfully.")
    
    def _move_file(self, move_cmd_with_cache: Tuple[Tuple[str, str], str, int, str], destination: str) -> int:
        """Move a single file using the .plexcached approach.

        For cache destination:
        1. Copy file from array to cache
        2. Rename array file to .plexcached
        3. Add to exclude file

        For array destination:
        1. Rename .plexcached file back to original
        2. Delete cache copy
        3. (Exclude file update handled separately by caller)
        """
        from tqdm import tqdm

        (src, dest), cache_file_name, file_size, original_path = move_cmd_with_cache
        filename = os.path.basename(src)
        thread_id = threading.get_ident()

        # Get per-worker byte progress callback
        worker_byte_cb = self._byte_aggregator.make_worker_callback() if self._byte_aggregator else None

        # Register as active before starting
        with self._progress_lock:
            self._active_files[thread_id] = (filename, file_size)

        try:
            if destination == 'cache':
                result = self._move_to_cache(src, dest, cache_file_name, original_path, byte_callback=worker_byte_cb)
            elif destination == 'array':
                result = self._move_to_array(src, dest, cache_file_name, byte_callback=worker_byte_cb)
                if result == 0:
                    with self._successful_array_moves_lock:
                        self._successful_array_moves.append(cache_file_name)
            else:
                result = 0

            # Finalize byte progress for this file (handles instant renames
            # where copy_file_with_permissions wasn't called)
            if worker_byte_cb and result in (0, 2) and file_size > 0:
                worker_byte_cb(file_size, file_size)

            # Update tqdm progress bar + remove from active
            with self._progress_lock:
                self._completed_bytes += file_size
                self._active_files.pop(thread_id, None)
                if self._tqdm_pbar:
                    # Update description to show data progress
                    completed_str = format_bytes(self._completed_bytes)
                    total_str = format_bytes(self._total_bytes)
                    self._tqdm_pbar.set_description(f"Moving to {destination} ({completed_str} / {total_str})")
                    self._tqdm_pbar.update(1)
                    self._tqdm_pbar.refresh()  # Force display update

            return result
        except Exception as e:
            # Still update progress on error + remove from active
            with self._progress_lock:
                self._active_files.pop(thread_id, None)
                if self._tqdm_pbar:
                    self._tqdm_pbar.update(1)
            with get_console_lock():
                tqdm.write(f"Error moving {filename}: {type(e).__name__}: {e}")
            return 1

    def _move_to_cache(self, array_file: str, cache_path: str, cache_file_name: str,
                       original_path: str = None, byte_callback=None) -> int:
        """Copy file to cache and handle array original.

        When create_plexcached_backups is True (default):
        - Rename array file to .plexcached (preserves backup on array)
        - If cache drive fails, backup can be restored

        When create_plexcached_backups is False:
        - Delete array file after verified copy to cache
        - No backup on array (faster, works with hard-linked files)
        - WARNING: No recovery possible if cache drive fails

        Order of operations ensures data safety:
        1. Check for and clean up old .plexcached if this is an upgrade (backup mode only)
        2. Copy file to cache
        3. Verify copy succeeded
        4. Rename to .plexcached OR delete array file (based on setting)
        5. Verify operation succeeded
        6. Record timestamp for cache retention

        If interrupted at any point, the original array file remains safe.
        Worst case: an orphaned cache copy exists that can be deleted.
        """
        # Defense in depth: If array_file is a /mnt/user/ path (ZFS, no conversion),
        # probe /mnt/user0/ for the real array file. On hybrid ZFS shares, /mnt/user/
        # shows the cache file through FUSE — renaming it would corrupt the only copy.
        if array_file.startswith('/mnt/user/'):
            user0_path = '/mnt/user0/' + array_file[len('/mnt/user/'):]
            if os.path.isfile(user0_path):
                logging.debug(f"Using array-direct path for .plexcached rename: {user0_path}")
                array_file = user0_path
            elif not os.path.exists('/mnt/user0'):
                raise IOError(
                    f"Cannot safely create .plexcached backup: /mnt/user0 not accessible. "
                    f"If running in Docker, ensure /mnt/user0 is mounted as a volume "
                    f"(e.g., -v /mnt/user0:/mnt/user0)."
                )
            else:
                # /mnt/user0 exists but file not found — no array copy to back up
                # The file visible at /mnt/user/ is the cache copy through FUSE
                logging.debug(f"No array copy at {user0_path}, skipping .plexcached backup")

        plexcached_file = array_file + PLEXCACHED_EXTENSION
        array_path = os.path.dirname(array_file)

        try:
            old_cache_file_to_remove = None

            # Step 0: Check for upgrade scenario - clean up old .plexcached if needed
            # Only relevant when backups are enabled
            if self.create_plexcached_backups and not os.path.isfile(plexcached_file):
                cache_identity = get_media_identity(cache_file_name)
                old_plexcached = find_matching_plexcached(array_path, cache_identity, array_file)
                if old_plexcached and old_plexcached != plexcached_file:
                    old_name = os.path.basename(old_plexcached).replace(PLEXCACHED_EXTENSION, '')
                    new_name = os.path.basename(cache_file_name)
                    logging.info(f"Upgrade detected during cache: {old_name} -> {new_name}")
                    os.remove(old_plexcached)
                    logging.debug(f"Deleted old .plexcached: {old_plexcached}")
                    # Build the old cache file path for exclude list cleanup
                    # The exclude list stores full cache paths, so join the cache directory with the old filename
                    old_cache_file_to_remove = os.path.join(os.path.dirname(cache_file_name), old_name)

            # Step 1: Ensure cache directory exists, then copy file
            cache_dir = os.path.dirname(cache_file_name)
            if not os.path.exists(cache_dir):
                self.file_utils.create_directory_with_permissions(cache_dir, array_file)
                logging.debug(f"Created cache directory: {cache_dir}")

            # For Docker: translate cache path to host path for log display
            display_dest = self._translate_to_host_path(cache_file_name) if self.file_utils.is_docker else None
            logging.debug(f"Starting copy: {array_file} -> {display_dest or cache_file_name}")

            # Build stop check that checks both callback and direct flag
            def combined_stop_check():
                if self._stop_requested:
                    return True
                if self._stop_check and self._stop_check():
                    return True
                return False

            self.file_utils.copy_file_with_permissions(
                array_file, cache_file_name, verbose=True, display_dest=display_dest,
                stop_check=combined_stop_check, progress_callback=byte_callback
            )
            logging.debug(f"Copy complete: {os.path.basename(array_file)}")

            # Validate copy succeeded
            if not os.path.isfile(cache_file_name):
                raise IOError(f"Copy verification failed: cache file not created at {cache_file_name}")

            # Step 2: Handle array file based on backup setting and hard-link status
            # Hard-linked files must be deleted (not renamed) to avoid FUSE issues
            is_hardlinked = cache_file_name in self._hardlink_inodes
            if self.create_plexcached_backups and not is_hardlinked:
                # Rename array file to .plexcached (preserves backup)
                os.rename(array_file, plexcached_file)
                logging.debug(f"Renamed array file: {array_file} -> {plexcached_file}")

                # Validate rename succeeded with FUSE diagnostic logging
                parent_dir = os.path.dirname(array_file)

                # Diagnostic: List directory contents after rename
                try:
                    dir_contents = os.listdir(parent_dir)
                    original_name = os.path.basename(array_file)
                    plexcached_name = os.path.basename(plexcached_file)
                    logging.debug(f"FUSE diag: directory listing after rename:")
                    logging.debug(f"  - Original '{original_name}' in listing: {original_name in dir_contents}")
                    logging.debug(f"  - Plexcached '{plexcached_name}' in listing: {plexcached_name in dir_contents}")
                except OSError as e:
                    logging.debug(f"FUSE diag: listdir failed: {e}")

                # Diagnostic: Check file existence with isfile
                original_isfile = os.path.isfile(array_file)
                plexcached_isfile = os.path.isfile(plexcached_file)
                logging.debug(f"FUSE diag: os.path.isfile - original={original_isfile}, plexcached={plexcached_isfile}")

                # Diagnostic: Check with os.stat (bypasses some caching)
                original_stat_exists = False
                plexcached_stat_exists = False
                try:
                    os.stat(array_file)
                    original_stat_exists = True
                except FileNotFoundError:
                    pass
                except OSError as e:
                    logging.debug(f"FUSE diag: stat(original) error: {e}")

                try:
                    os.stat(plexcached_file)
                    plexcached_stat_exists = True
                except FileNotFoundError:
                    pass
                except OSError as e:
                    logging.debug(f"FUSE diag: stat(plexcached) error: {e}")

                logging.debug(f"FUSE diag: os.stat - original={original_stat_exists}, plexcached={plexcached_stat_exists}")

                # Diagnostic: Check with os.access
                original_access = os.access(array_file, os.F_OK)
                plexcached_access = os.access(plexcached_file, os.F_OK)
                logging.debug(f"FUSE diag: os.access(F_OK) - original={original_access}, plexcached={plexcached_access}")

                # Diagnostic: Try to resolve to physical disk path (Unraid-specific)
                if array_file.startswith('/mnt/user0/'):
                    relative_path = array_file[len('/mnt/user0/'):]
                    for disk_num in range(1, 10):  # Check first 9 disks
                        disk_path = f'/mnt/disk{disk_num}/{relative_path}'
                        disk_plexcached = disk_path + '.plexcached'
                        if os.path.exists(disk_path) or os.path.exists(disk_plexcached):
                            logging.debug(f"FUSE diag: Found on disk{disk_num}: original={os.path.exists(disk_path)}, plexcached={os.path.exists(disk_plexcached)}")

                # Final verification using isfile (standard check)
                if os.path.isfile(array_file):
                    raise IOError(f"Rename verification failed: original array file still exists at {array_file}")
                if not os.path.isfile(plexcached_file):
                    raise IOError(f"Rename verification failed: .plexcached file not created at {plexcached_file}")
            else:
                # Delete array file (no backup - either backups disabled or hard-linked file)
                os.remove(array_file)
                if is_hardlinked:
                    logging.debug(f"Deleted array link (hard-linked file, seed copy preserved): {array_file}")
                else:
                    logging.debug(f"Deleted array file (backups disabled): {array_file}")
                # Verify deletion
                if os.path.isfile(array_file):
                    raise IOError(f"Delete verification failed: array file still exists at {array_file}")

            # Step 3: Create symlink at original location for non-Unraid Plex compatibility
            if self.use_symlinks and original_path:
                self._create_symlink(original_path, cache_file_name)

            # Step 4: Add to exclude file (and remove old entry if upgrade)
            self._add_to_exclude_file(cache_file_name)
            if old_cache_file_to_remove:
                self._remove_from_exclude_file(old_cache_file_to_remove)

            # Step 4: Record timestamp for cache retention with source and media type info
            if self.timestamp_tracker:
                # Look up source from the source map using the original path (e.g., /mnt/user/...)
                source = self._source_map.get(original_path, "unknown") if original_path else "unknown"
                # Include original inode for hard-linked files (for restoration)
                original_inode = self._hardlink_inodes.get(cache_file_name)
                # Look up media type info from Plex API metadata
                media_info = self._media_info_map.get(original_path, {}) if original_path else {}
                self.timestamp_tracker.record_cache_time(
                    cache_file_name, source, original_inode,
                    media_type=media_info.get("media_type"),
                    episode_info=media_info.get("episode_info")
                )

            # Mark as cached in OnDeck/Watchlist trackers
            cache_source = self._source_map.get(original_path, "unknown") if original_path else "unknown"
            if self.ondeck_tracker:
                self.ondeck_tracker.mark_cached(original_path or cache_file_name, cache_source)
            if self.watchlist_tracker:
                self.watchlist_tracker.mark_cached(original_path or cache_file_name, cache_source)

            # Log successful move - both to logging (for web UI) and tqdm (for CLI progress bar)
            from tqdm import tqdm
            from core.logging_config import mark_file_activity
            file_size = os.path.getsize(cache_file_name)
            size_str = format_bytes(file_size)
            display_name = os.path.basename(cache_file_name)
            # Log with indented format for web UI activity capture
            logging.info(f"  [Cached] {display_name} ({size_str})")
            with get_console_lock():
                tqdm.write(f"Successfully cached: {display_name} ({size_str})")

            # Mark that file activity occurred (for notification level filtering)
            mark_file_activity()

            return 0
        except InterruptedError as e:
            # Copy was cancelled by stop request - clean up partial file
            logging.info(f"Copy cancelled (stop requested): {os.path.basename(cache_file_name)}")
            self._cleanup_failed_cache_copy(array_file, cache_file_name, original_path)
            return 4  # Stopped by user
        except Exception as e:
            logging.error(f"Error copying to cache: {type(e).__name__}: {e}")
            # Attempt cleanup on failure
            self._cleanup_failed_cache_copy(array_file, cache_file_name, original_path)
            return 1

    def _check_array_disk_space(self, cache_file: str, plexcached_file: str,
                                 array_file: str) -> Tuple[bool, str]:
        """Pre-flight check for sufficient disk space on the target array disk.

        On Unraid, resolves the /mnt/user0/ path to the actual /mnt/diskX/ path
        and checks available space. Calculates required space based on whether
        this will be a rename operation or a copy operation.

        Args:
            cache_file: Path to the file on cache.
            plexcached_file: Path to the .plexcached file on array.
            array_file: Path where the restored file should end up.

        Returns:
            Tuple of (has_sufficient_space, reason_if_not).
            reason_if_not is empty string if space is sufficient.
        """
        if not self.is_unraid:
            # Non-Unraid systems don't need this check (no disk abstraction)
            return True, ""

        # Determine which file to check for disk resolution
        check_path = plexcached_file if os.path.isfile(plexcached_file) else array_file

        # Resolve to actual disk path
        disk_path = resolve_user0_to_disk(check_path)
        if disk_path is None:
            # Couldn't resolve - fall back to checking via user0 path
            logging.debug(f"Could not resolve disk path for: {check_path}")
            disk_path = check_path

        disk_name = get_disk_number_from_path(disk_path) or "unknown disk"

        # Get available space on that disk
        free_space = get_disk_free_space_bytes(disk_path)

        # Calculate required space based on scenario
        cache_size = os.path.getsize(cache_file) if os.path.isfile(cache_file) else 0

        if os.path.isfile(plexcached_file):
            plexcached_size = os.path.getsize(plexcached_file)

            if cache_size == 0 or cache_size == plexcached_size:
                # Pure rename - just need metadata buffer
                space_required = MINIMUM_SPACE_FOR_RENAME
                operation = "rename"
            else:
                # In-place upgrade - we delete old first, then copy new
                # Space needed: max(0, new_size - old_size) + buffer
                space_required = max(0, cache_size - plexcached_size) + MINIMUM_SPACE_FOR_RENAME
                operation = f"upgrade ({format_bytes(plexcached_size)} -> {format_bytes(cache_size)})"
        else:
            # No .plexcached - need full file size + buffer
            space_required = cache_size + MINIMUM_SPACE_FOR_RENAME
            operation = "copy (no .plexcached)"

        if free_space < space_required:
            reason = (
                f"Insufficient space on {disk_name} for {operation}: "
                f"need {format_bytes(space_required)}, have {format_bytes(free_space)}. "
                f"File will remain on cache. Free up space on {disk_name} or manually relocate the .plexcached file."
            )
            return False, reason

        logging.debug(
            f"Disk space check passed for {disk_name}: "
            f"need {format_bytes(space_required)}, have {format_bytes(free_space)} ({operation})"
        )
        return True, ""

    def _find_file_by_inode(self, inode: int, search_hint_path: str) -> Optional[str]:
        """Find a file with the specified inode number on the array.

        Used for hard-link restoration - finds the remaining hard link (e.g., seed copy)
        so we can create a new hard link instead of copying.

        Args:
            inode: The inode number to search for.
            search_hint_path: A path hint to determine which disk to search.

        Returns:
            Path to a file with the matching inode, or None if not found.
        """
        try:
            # Determine the disk path from the search hint
            # Convert /mnt/user0/... to /mnt/diskN/... and search there
            if not search_hint_path.startswith('/mnt/user'):
                logging.debug(f"Cannot search for inode - not an Unraid path: {search_hint_path}")
                return None

            # Try to find which disk the file was originally on
            # Check disks 1-30 (typical Unraid setup)
            relative_path = search_hint_path
            if relative_path.startswith('/mnt/user0/'):
                relative_path = relative_path[len('/mnt/user0/'):]
            elif relative_path.startswith('/mnt/user/'):
                relative_path = relative_path[len('/mnt/user/'):]

            # Get the data folder (first component after media type)
            # e.g., "data/media/tv/..." -> search in /mnt/diskN/data/
            path_parts = relative_path.split('/')
            if len(path_parts) >= 1:
                search_base = path_parts[0]  # e.g., "data"
            else:
                search_base = ""

            for disk_num in range(1, 31):
                disk_path = f'/mnt/disk{disk_num}'
                if not os.path.isdir(disk_path):
                    continue

                search_path = os.path.join(disk_path, search_base) if search_base else disk_path
                if not os.path.isdir(search_path):
                    continue

                # Use find command to search for file by inode
                try:
                    import subprocess
                    result = subprocess.run(
                        ['find', search_path, '-inum', str(inode), '-type', 'f', '-print', '-quit'],
                        capture_output=True, text=True, timeout=30
                    )
                    if result.returncode == 0 and result.stdout.strip():
                        found_file = result.stdout.strip()
                        logging.debug(f"Found file with inode {inode}: {found_file}")
                        return found_file
                except subprocess.TimeoutExpired:
                    logging.debug(f"Inode search timed out on {search_path}")
                    continue
                except Exception as e:
                    logging.debug(f"Error searching {search_path} for inode: {e}")
                    continue

            logging.debug(f"No file found with inode {inode}")
            return None
        except Exception as e:
            logging.warning(f"Error searching for file by inode: {e}")
            return None

    def _move_to_array(self, cache_file: str, array_path: str, cache_file_name: str,
                       byte_callback=None) -> int:
        """Move file from cache back to array.

        Handles five scenarios:
        0. Hard-linked file with stored inode: Create hard link from existing seed copy (fast)
        1a. Exact .plexcached exists, same size: Rename it back to original (fast)
        1b. Exact .plexcached exists, different size: In-place upgrade detected
            - Delete old .plexcached, copy upgraded cache file to array
        2. Different .plexcached exists (same media, different filename/quality)
           - Delete old .plexcached, copy upgraded cache file to array
        3. No .plexcached: Copy from cache to array, then delete cache copy

        Before any operation, performs a pre-flight disk space check on Unraid
        systems to prevent "No space left on device" errors.

        Returns:
            0: Success - array file exists and cache deleted
            1: Error - exception occurred during operation
            3: Skipped - insufficient disk space (file remains on cache)
        """
        try:
            # Derive the original array file path and .plexcached path
            array_file = os.path.join(array_path, os.path.basename(cache_file))

            # Remove symlink at original location before restoring (must happen before
            # user0 conversion since on non-Unraid, array_file IS the original path)
            if self.use_symlinks and os.path.islink(array_file):
                self._remove_symlink(array_file)

            # Defense in depth: If array_path is a /mnt/user/ path (ZFS, no conversion),
            # probe /mnt/user0/ for the real array location. On hybrid ZFS shares,
            # /mnt/user/ shows the cache file through FUSE.
            if array_file.startswith('/mnt/user/'):
                user0_file = '/mnt/user0/' + array_file[len('/mnt/user/'):]
                user0_plexcached = user0_file + PLEXCACHED_EXTENSION
                if os.path.isfile(user0_file) or os.path.isfile(user0_plexcached):
                    logging.debug(f"Using array-direct path for restore: {user0_file}")
                    array_file = user0_file
                    array_path = os.path.dirname(user0_file)
                elif os.path.exists('/mnt/user0'):
                    # /mnt/user0 exists but no file or .plexcached there
                    # Check if the directory exists — if so, use user0 path for writes
                    user0_dir = os.path.dirname(user0_file)
                    if os.path.isdir(user0_dir):
                        array_file = user0_file
                        array_path = user0_dir

            plexcached_file = array_file + PLEXCACHED_EXTENSION

            # Track operation type for activity logging
            operation_type = "Restored"  # Default to restore (fast rename)

            # Pre-flight check: verify sufficient disk space on target disk
            has_space, reason = self._check_array_disk_space(cache_file, plexcached_file, array_file)
            if not has_space:
                logging.warning(f"Skipping restore for {os.path.basename(cache_file)}: {reason}")
                return 3  # Skipped due to insufficient space

            # Scenario 0: Check for hard-linked file restoration
            # If we have the original inode, try to find a file with that inode and create a hard link
            original_inode = None
            if self.timestamp_tracker:
                original_inode = self.timestamp_tracker.get_original_inode(cache_file_name)

            if original_inode is not None and not os.path.isfile(array_file):
                # Try to find a file with the original inode on the array
                source_file = self._find_file_by_inode(original_inode, array_path)
                if source_file:
                    try:
                        os.link(source_file, array_file)
                        logging.info(f"Restored hard link from seed copy: {os.path.basename(array_file)}")
                        logging.debug(f"Hard link created: {source_file} -> {array_file}")
                        # Skip to cache deletion since array file is now restored
                        if os.path.isfile(cache_file):
                            os.remove(cache_file)
                            logging.debug(f"Deleted cache file: {cache_file}")
                        # Remove timestamp entry
                        if self.timestamp_tracker:
                            self.timestamp_tracker.remove_entry(cache_file_name)
                        from tqdm import tqdm
                        with get_console_lock():
                            tqdm.write(f"Restored to array (hard link): {os.path.basename(array_file)}")
                        return 0
                    except OSError as e:
                        logging.warning(f"Could not create hard link, falling back to copy: {e}")
                else:
                    logging.debug(f"Original inode {original_inode} not found on array, falling back to copy")

            # Scenario 1: Exact .plexcached exists (same filename)
            if os.path.isfile(plexcached_file):
                # Check for in-place upgrade (same filename, different size)
                cache_size = os.path.getsize(cache_file) if os.path.isfile(cache_file) else 0
                plexcached_size = os.path.getsize(plexcached_file)

                if cache_size > 0 and cache_size != plexcached_size:
                    # In-place upgrade: same filename but different file content
                    operation_type = "Moved"  # Copy operation
                    logging.info(f"In-place upgrade detected ({format_bytes(plexcached_size)} -> {format_bytes(cache_size)}): {os.path.basename(cache_file)}")
                    os.remove(plexcached_file)
                    # For Docker: translate cache path to host path for log display
                    display_src = self._translate_to_host_path(cache_file) if self.file_utils.is_docker else None

                    # Build stop check for cancellable copy
                    def combined_stop_check():
                        if self._stop_requested:
                            return True
                        if self._stop_check and self._stop_check():
                            return True
                        return False

                    self.file_utils.copy_file_with_permissions(
                        cache_file, array_file, verbose=True, display_src=display_src,
                        stop_check=combined_stop_check, progress_callback=byte_callback
                    )
                    logging.debug(f"Copied upgraded file to array: {array_file}")

                    # Verify copy succeeded
                    if os.path.isfile(array_file):
                        array_size = os.path.getsize(array_file)
                        if cache_size != array_size:
                            logging.error(f"Size mismatch after copy! Cache: {cache_size}, Array: {array_size}. Keeping cache file.")
                            os.remove(array_file)
                            return 1
                else:
                    # Same size (or cache missing), just rename back (fast)
                    # Always rename rather than checking if original "exists" via /mnt/user0/.
                    # On ZFS Unraid, /mnt/user0/ FUSE can show cache files as array files,
                    # making existence checks unreliable. Renaming is always safe — if the
                    # file genuinely exists, os.rename() atomically replaces it with the
                    # identical .plexcached content (which IS the original, just renamed).
                    operation_type = "Restored"
                    os.rename(plexcached_file, array_file)
                    logging.debug(f"Restored array file: {plexcached_file} -> {array_file}")

            # Scenario 2: Check for filename-change upgrade (different .plexcached with same media identity)
            elif os.path.isfile(cache_file):
                cache_identity = get_media_identity(cache_file)
                old_plexcached = find_matching_plexcached(array_path, cache_identity, cache_file)

                # Scenario 2: Upgraded file - old .plexcached exists with different name
                if old_plexcached and old_plexcached != plexcached_file:
                    operation_type = "Moved"  # Copy operation (upgrade)
                    old_name = os.path.basename(old_plexcached).replace(PLEXCACHED_EXTENSION, '')
                    new_name = os.path.basename(cache_file)
                    logging.info(f"Upgrade detected: {old_name} -> {new_name}")

                    # Delete the old .plexcached (it's outdated)
                    os.remove(old_plexcached)
                    logging.debug(f"Deleted old .plexcached: {old_plexcached}")

                    # Copy the upgraded cache file to array (preserving ownership)
                    cache_size = os.path.getsize(cache_file)
                    # For Docker: translate cache path to host path for log display
                    display_src = self._translate_to_host_path(cache_file) if self.file_utils.is_docker else None

                    # Build stop check for cancellable copy
                    def combined_stop_check():
                        if self._stop_requested:
                            return True
                        if self._stop_check and self._stop_check():
                            return True
                        return False

                    self.file_utils.copy_file_with_permissions(
                        cache_file, array_file, verbose=True, display_src=display_src,
                        stop_check=combined_stop_check, progress_callback=byte_callback
                    )
                    logging.debug(f"Copied upgraded file to array: {array_file}")

                    # Verify copy succeeded
                    if os.path.isfile(array_file):
                        array_size = os.path.getsize(array_file)
                        if cache_size != array_size:
                            logging.error(f"Size mismatch after copy! Cache: {cache_size}, Array: {array_size}. Keeping cache file.")
                            os.remove(array_file)
                            return 1

                # Scenario 3: No .plexcached at all - copy to array (preserving ownership)
                # CRITICAL: Use /mnt/user0/ (array direct) to check if file truly exists on array
                elif not os.path.isfile(get_array_direct_path(array_file)):
                    operation_type = "Moved"  # Copy operation (no backup)
                    logging.debug(f"No .plexcached found, copying from cache to array: {cache_file}")
                    cache_size = os.path.getsize(cache_file)
                    # For Docker: translate cache path to host path for log display
                    display_src = self._translate_to_host_path(cache_file) if self.file_utils.is_docker else None
                    # CRITICAL: Copy to /mnt/user0/ (array direct), NOT /mnt/user/ (FUSE)
                    # If we copy to /mnt/user/, Unraid's cache policy may put the file
                    # back on cache (if shareUseCache=yes), causing data loss
                    array_direct_file = get_array_direct_path(array_file)
                    array_direct_dir = os.path.dirname(array_direct_file)
                    os.makedirs(array_direct_dir, exist_ok=True)

                    # Build stop check for cancellable copy
                    def combined_stop_check():
                        if self._stop_requested:
                            return True
                        if self._stop_check and self._stop_check():
                            return True
                        return False

                    self.file_utils.copy_file_with_permissions(
                        cache_file, array_direct_file, verbose=True, display_src=display_src,
                        stop_check=combined_stop_check, progress_callback=byte_callback
                    )
                    logging.debug(f"Copied to array: {array_direct_file}")

                    # Verify copy succeeded by comparing file sizes
                    if os.path.isfile(array_direct_file):
                        array_size = os.path.getsize(array_direct_file)
                        if cache_size != array_size:
                            logging.error(f"Size mismatch after copy! Cache: {cache_size}, Array: {array_size}. Keeping cache file.")
                            os.remove(array_direct_file)
                            return 1

            # Delete cache copy only if array file truly exists on array
            # CRITICAL: Use /mnt/user0/ to avoid FUSE false positive where cache file appears as array file
            if os.path.isfile(get_array_direct_path(array_file)):
                if os.path.isfile(cache_file):
                    os.remove(cache_file)
                    logging.debug(f"Deleted cache file: {cache_file}")
                    # Clean up empty parent folders (per File and Folder Management Policy)
                    if self.cleanup_empty_folders:
                        self._cleanup_empty_parent_folders(cache_file)
                else:
                    logging.debug(f"Cache file already removed: {cache_file}")

                # Remove timestamp entry
                if self.timestamp_tracker:
                    self.timestamp_tracker.remove_entry(cache_file)

                # Log successful operation for web UI activity capture
                display_name = os.path.basename(array_file)
                try:
                    file_size = os.path.getsize(array_file)
                    size_str = format_bytes(file_size)
                except OSError:
                    size_str = "-"
                logging.info(f"  [{operation_type}] {display_name} ({size_str})")

                # Mark that file activity occurred (for notification level filtering)
                from core.logging_config import mark_file_activity
                mark_file_activity()

                return 0
            else:
                # This shouldn't happen, but log it if it does
                logging.error(f"Failed to create array file: {array_file}")
                return 1

        except InterruptedError as e:
            # Copy was cancelled by stop request - clean up partial array file
            logging.info(f"Copy cancelled (stop requested): {os.path.basename(cache_file)}")
            # Try to clean up partial array file if it exists
            if 'array_direct_file' in dir() and os.path.isfile(array_direct_file):
                try:
                    os.remove(array_direct_file)
                    logging.debug(f"Cleaned up partial array file: {array_direct_file}")
                except OSError:
                    pass
            elif os.path.isfile(array_file):
                try:
                    os.remove(array_file)
                    logging.debug(f"Cleaned up partial array file: {array_file}")
                except OSError:
                    pass
            return 4  # Stopped by user
        except Exception as e:
            logging.error(f"Error restoring to array: {type(e).__name__}: {e}")
            return 1

    def _cleanup_empty_parent_folders(self, file_path: str) -> int:
        """Clean up empty parent folders after a file is removed.

        Implements the File and Folder Management Policy: PlexCache only removes
        folders that it emptied by moving files out. This method walks up the
        directory tree from the deleted file's parent, removing empty folders
        until it hits the cache_dir boundary or a non-empty folder.

        Args:
            file_path: Path to the file that was just deleted

        Returns:
            Number of folders removed
        """
        folders_removed = 0
        current_dir = os.path.dirname(file_path)

        # Normalize paths for comparison
        cache_boundary = os.path.normpath(self.cache_dir)

        while current_dir:
            normalized_current = os.path.normpath(current_dir)

            # Stop if we've reached or passed the cache boundary
            # We should never delete the cache_dir itself or anything above it
            if normalized_current == cache_boundary or not normalized_current.startswith(cache_boundary):
                break

            try:
                # Check if directory is empty
                if not os.path.exists(current_dir):
                    break

                contents = os.listdir(current_dir)
                if contents:
                    # Folder not empty, stop climbing
                    logging.debug(f"Folder not empty, stopping cleanup: {current_dir}")
                    break

                # Folder is empty, remove it
                os.rmdir(current_dir)
                logging.debug(f"Removed empty folder (PlexCache cleanup): {current_dir}")
                folders_removed += 1

                # Move up to parent
                current_dir = os.path.dirname(current_dir)

            except OSError as e:
                logging.debug(f"Could not remove folder {current_dir}: {type(e).__name__}: {e}")
                break

        return folders_removed

    def _create_symlink(self, symlink_path: str, target_path: str) -> bool:
        """Create a symlink at symlink_path pointing to target_path.

        Used on non-Unraid systems so Plex can still find files at their original
        locations after the original is renamed to .plexcached or deleted.

        Non-fatal: logs warning on failure, returns False. Caching proceeds regardless.
        Uses absolute paths for Docker compatibility.
        """
        try:
            # Remove existing symlink if present (e.g., re-caching same file)
            if os.path.islink(symlink_path):
                os.remove(symlink_path)
                logging.debug(f"Removed existing symlink: {symlink_path}")

            # Ensure parent directory exists
            parent_dir = os.path.dirname(symlink_path)
            if parent_dir and not os.path.isdir(parent_dir):
                os.makedirs(parent_dir, exist_ok=True)

            os.symlink(target_path, symlink_path)
            logging.debug(f"Created symlink: {symlink_path} -> {target_path}")
            return True
        except OSError as e:
            logging.warning(f"Could not create symlink at {symlink_path}: {e}")
            return False

    def _remove_symlink(self, path: str) -> bool:
        """Remove a symlink at the given path if it is a symlink.

        Returns True if a symlink was removed, False if not a symlink.
        """
        if os.path.islink(path):
            try:
                os.remove(path)
                logging.debug(f"Removed symlink: {path}")
                return True
            except OSError as e:
                logging.warning(f"Could not remove symlink at {path}: {e}")
                return False
        return False

    def _cleanup_failed_cache_copy(self, array_file: str, cache_file_name: str,
                                   original_path: str = None) -> None:
        """Clean up after a failed cache copy operation."""
        plexcached_file = array_file + PLEXCACHED_EXTENSION
        try:
            # Remove symlink if one was created before the failure
            if self.use_symlinks:
                symlink_location = original_path or array_file
                if os.path.islink(symlink_location):
                    os.remove(symlink_location)
                    logging.debug(f"Cleanup: Removed symlink at {symlink_location}")

            # If we renamed the array file but copy failed, rename it back
            if os.path.isfile(plexcached_file) and not os.path.isfile(array_file):
                os.rename(plexcached_file, array_file)
                logging.info(f"Cleanup: Restored array file after failed copy")
            # Remove partial cache file if it exists
            if os.path.isfile(cache_file_name):
                os.remove(cache_file_name)
                logging.info(f"Cleanup: Removed partial cache file")
        except Exception as e:
            logging.error(f"Error during cleanup: {type(e).__name__}: {e}")


class PlexcachedRestorer:
    """Emergency restore utility to rename all .plexcached files back to originals."""

    def __init__(self, search_paths: List[str]):
        """Initialize with paths to search for .plexcached files."""
        self.search_paths = search_paths

    def find_plexcached_files(self) -> List[str]:
        """Find all .plexcached files in the search paths."""
        plexcached_files = []
        for search_path in self.search_paths:
            if not os.path.exists(search_path):
                logging.warning(f"Search path does not exist: {search_path}")
                continue
            for root, dirs, files in os.walk(search_path):
                # Skip hidden directories (dot-prefixed like .Trash, .Recycle.Bin)
                dirs[:] = [d for d in dirs if not d.startswith('.')]
                for filename in files:
                    if filename.endswith(PLEXCACHED_EXTENSION):
                        plexcached_files.append(os.path.join(root, filename))
        return plexcached_files

    def restore_all(self, dry_run: bool = False) -> Tuple[int, int]:
        """Restore all .plexcached files to their original names.

        Args:
            dry_run: If True, only log what would be done without making changes.

        Returns:
            Tuple of (success_count, error_count)
        """
        plexcached_files = self.find_plexcached_files()
        logging.info(f"Found {len(plexcached_files)} .plexcached files to restore")

        if not plexcached_files:
            return 0, 0

        success_count = 0
        error_count = 0

        for plexcached_file in plexcached_files:
            # Remove .plexcached extension to get original filename
            original_file = plexcached_file[:-len(PLEXCACHED_EXTENSION)]

            if dry_run:
                logging.info(f"[DRY RUN] Would restore: {plexcached_file} -> {original_file}")
                success_count += 1
                continue

            try:
                # Check if original location has a symlink (from use_symlinks mode)
                if os.path.islink(original_file):
                    try:
                        os.remove(original_file)
                        logging.info(f"Removed symlink before restore: {original_file}")
                    except OSError as e:
                        logging.warning(f"Cannot remove symlink at {original_file}: {e}")
                        error_count += 1
                        continue
                # Check if original already exists (shouldn't happen, but be safe)
                elif os.path.exists(original_file):
                    logging.warning(f"Original file already exists, skipping: {original_file}")
                    error_count += 1
                    continue

                os.rename(plexcached_file, original_file)
                logging.info(f"Restored: {plexcached_file} -> {original_file}")
                success_count += 1
            except Exception as e:
                logging.error(f"Failed to restore {plexcached_file}: {type(e).__name__}: {e}")
                error_count += 1

        logging.info(f"Restore complete: {success_count} succeeded, {error_count} failed")
        return success_count, error_count


# Note: CacheCleanup class was removed per File and Folder Management Policy.
# Empty folder cleanup is now handled immediately during file operations
# by FileMover._cleanup_empty_parent_folders() - PlexCache only removes
# folders that it empties by moving files out, not arbitrary empty folders.
