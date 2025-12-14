"""
File operations for PlexCache.
Handles file moving, filtering, subtitle operations, and path modifications.
"""

import os
import shutil
import logging
import threading
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from typing import List, Set, Optional, Tuple, Dict
import re

# Extension used to mark array files that have been cached
PLEXCACHED_EXTENSION = ".plexcached"


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
    # Remove extension
    name = os.path.splitext(filename)[0]
    # Remove .plexcached extension if present
    if name.endswith('.plexcached'):
        name = name[:-len('.plexcached')]
        name = os.path.splitext(name)[0]  # Remove the actual extension too
    # Remove everything from first '[' onwards (quality/codec info)
    if '[' in name:
        name = name[:name.index('[')].strip()
    # Remove trailing ' -' or '-' if present (sometimes left over)
    name = name.rstrip(' -').rstrip('-').strip()
    return name


def find_matching_plexcached(array_path: str, media_identity: str) -> Optional[str]:
    """Find a .plexcached file in the array path that matches the media identity.

    This handles the case where Radarr/Sonarr upgraded a file - the .plexcached
    backup may have a different quality suffix but same core identity.

    Args:
        array_path: Directory path on the array to search
        media_identity: The core media identity to match (from get_media_identity)

    Returns:
        Full path to matching .plexcached file, or None if not found
    """
    if not os.path.isdir(array_path):
        return None

    try:
        for entry in os.scandir(array_path):
            if entry.is_file() and entry.name.endswith(PLEXCACHED_EXTENSION):
                entry_identity = get_media_identity(entry.name)
                if entry_identity == media_identity:
                    return entry.path
    except (OSError, PermissionError) as e:
        logging.debug(f"Error scanning for .plexcached files in {array_path}: {e}")

    return None


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

                logging.debug(f"Loaded {len(self._timestamps)} timestamps from {self.timestamp_file}")
        except (json.JSONDecodeError, IOError) as e:
            logging.warning(f"Could not load timestamp file: {type(e).__name__}: {e}")
            self._timestamps = {}

    def _save(self) -> None:
        """Save timestamps to file."""
        try:
            with open(self.timestamp_file, 'w', encoding='utf-8') as f:
                json.dump(self._timestamps, f, indent=2)
        except IOError as e:
            logging.error(f"Could not save timestamp file: {type(e).__name__}: {e}")

    def record_cache_time(self, cache_file_path: str, source: str = "unknown") -> None:
        """Record the current time and source when a file was cached.

        Only records if no entry exists - never overwrites existing timestamps.

        Args:
            cache_file_path: The path to the cached file.
            source: Where the file came from - "ondeck", "watchlist", "pre-existing", or "unknown".
        """
        with self._lock:
            # Never overwrite existing timestamps - file was cached when it was first recorded
            if cache_file_path in self._timestamps:
                logging.debug(f"Timestamp already exists for: {cache_file_path}")
                return

            self._timestamps[cache_file_path] = {
                "cached_at": datetime.now().isoformat(),
                "source": source
            }
            self._save()
            logging.debug(f"Recorded cache timestamp for: {cache_file_path} (source: {source})")

    def remove_entry(self, cache_file_path: str) -> None:
        """Remove a file's timestamp entry (when file is restored to array).

        Args:
            cache_file_path: The path to the cached file.
        """
        with self._lock:
            if cache_file_path in self._timestamps:
                del self._timestamps[cache_file_path]
                self._save()
                logging.debug(f"Removed cache timestamp for: {cache_file_path}")

    def is_within_retention_period(self, cache_file_path: str, retention_hours: int) -> bool:
        """Check if a file is still within its cache retention period.

        Args:
            cache_file_path: The path to the cached file.
            retention_hours: How many hours files should stay on cache.

        Returns:
            True if the file was cached less than retention_hours ago, False otherwise.
            Returns False if no timestamp exists (file should be allowed to move).
        """
        with self._lock:
            if cache_file_path not in self._timestamps:
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

        Args:
            cache_file_path: The path to the cached file.
            retention_hours: The configured retention period in hours.

        Returns:
            Hours remaining (positive if within retention, 0 or negative if expired).
            Returns 0 if no timestamp exists.
        """
        with self._lock:
            if cache_file_path not in self._timestamps:
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

        Args:
            cache_file_path: The path to the cached file.

        Returns:
            The source string ("ondeck", "watchlist", or "unknown").
        """
        with self._lock:
            if cache_file_path not in self._timestamps:
                return "unknown"
            entry = self._timestamps[cache_file_path]
            if isinstance(entry, dict):
                return entry.get("source", "unknown")
            return "unknown"

    def cleanup_missing_files(self) -> int:
        """Remove entries for files that no longer exist on cache.

        Returns:
            Number of entries removed.
        """
        with self._lock:
            missing = [path for path in self._timestamps if not os.path.exists(path)]
            for path in missing:
                del self._timestamps[path]
            if missing:
                self._save()
                logging.info(f"Cleaned up {len(missing)} stale timestamp entries")
            return len(missing)


class WatchlistTracker:
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
        self.tracker_file = tracker_file
        self._lock = threading.Lock()
        self._data: Dict[str, dict] = {}
        self._load()

    def _load(self) -> None:
        """Load tracker data from file."""
        try:
            if os.path.exists(self.tracker_file):
                with open(self.tracker_file, 'r', encoding='utf-8') as f:
                    self._data = json.load(f)
                logging.debug(f"Loaded {len(self._data)} watchlist entries from {self.tracker_file}")
        except (json.JSONDecodeError, IOError) as e:
            logging.warning(f"Could not load watchlist tracker file: {type(e).__name__}: {e}")
            self._data = {}

    def _save(self) -> None:
        """Save tracker data to file."""
        try:
            with open(self.tracker_file, 'w', encoding='utf-8') as f:
                json.dump(self._data, f, indent=2)
        except IOError as e:
            logging.error(f"Could not save watchlist tracker file: {type(e).__name__}: {e}")

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
                                logging.debug(f"Updated watchlist timestamp for {file_path} (user {username} added later)")
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
                logging.debug(f"Added new watchlist entry: {file_path} (user: {username})")

            self._save()

    def _find_entry_by_filename(self, file_path: str) -> Optional[Tuple[str, dict]]:
        """Find a tracker entry by matching filename when full path doesn't match.

        This handles cases where the cache file has modified paths (/mnt/user/...)
        but the tracker stores original paths (/data/...).

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
            # Try to find by filename (handles path prefix mismatches)
            result = self._find_entry_by_filename(file_path)
            if result:
                return result[1]
            return None

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
                    logging.debug(
                        f"Watchlist retention expired ({age_days:.1f} days > {retention_days} days): "
                        f"{os.path.basename(file_path)} (users: {', '.join(users)})"
                    )
                    return True
                return False
            except (ValueError, TypeError) as e:
                logging.warning(f"Invalid watchlisted_at timestamp for {file_path}: {e}")
                return False

    def cleanup_stale_entries(self, max_days_since_seen: int = 7) -> int:
        """Remove entries that haven't been seen on any watchlist recently.

        This cleans up entries for files that were removed from all watchlists.

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
                logging.info(f"Cleaned up {len(stale)} stale watchlist tracker entries")

            return len(stale)

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


class OnDeckTracker:
    """Thread-safe tracker for OnDeck items and their users.

    Tracks which users have each file OnDeck, similar to WatchlistTracker.
    Used for priority scoring - items OnDeck for multiple users have higher priority.
    Also tracks episode position info for TV shows to enable episode position awareness.

    Storage format:
    {
        "/path/to/file.mkv": {
            "users": ["Brandon", "Home"],
            "last_seen": "2025-12-03T10:00:00.000000",
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
    - episode_info: For TV episodes, contains show/season/episode and whether this is
                   the actual OnDeck episode vs a prefetched next episode
    - ondeck_users: Users for whom this is the CURRENT OnDeck episode (not prefetched)
    """

    def __init__(self, tracker_file: str):
        """Initialize the tracker with the path to the tracker file.

        Args:
            tracker_file: Path to the JSON file storing OnDeck data.
        """
        self.tracker_file = tracker_file
        self._lock = threading.Lock()
        self._data: Dict[str, dict] = {}
        self._load()

    def _load(self) -> None:
        """Load tracker data from file."""
        try:
            if os.path.exists(self.tracker_file):
                with open(self.tracker_file, 'r', encoding='utf-8') as f:
                    self._data = json.load(f)
                logging.debug(f"Loaded {len(self._data)} OnDeck entries from {self.tracker_file}")
        except (json.JSONDecodeError, IOError) as e:
            logging.warning(f"Could not load OnDeck tracker file: {type(e).__name__}: {e}")
            self._data = {}

    def _save(self) -> None:
        """Save tracker data to file."""
        try:
            with open(self.tracker_file, 'w', encoding='utf-8') as f:
                json.dump(self._data, f, indent=2)
        except IOError as e:
            logging.error(f"Could not save OnDeck tracker file: {type(e).__name__}: {e}")

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

            if file_path in self._data:
                entry = self._data[file_path]
                # Add user if not already in list
                if username not in entry.get('users', []):
                    entry.setdefault('users', []).append(username)
                # Always update last_seen
                entry['last_seen'] = now_iso

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
                    'last_seen': now_iso
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
                logging.debug(f"Added new OnDeck entry: {file_path} (user: {username})")

            self._save()

    def _find_entry_by_filename(self, file_path: str) -> Optional[Tuple[str, dict]]:
        """Find a tracker entry by matching filename when full path doesn't match.

        This handles cases where the cache file has modified paths (/mnt/cache_downloads/...)
        but the tracker stores original paths (/mnt/user/...).

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
            # Try to find by filename (handles path prefix mismatches)
            result = self._find_entry_by_filename(file_path)
            if result:
                return result[1]
            return None

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

    def clear_for_run(self) -> None:
        """Clear all entries at the start of a run.

        OnDeck status is ephemeral - items are only OnDeck for the current run.
        This is called at the start of each run to reset the tracker.
        """
        with self._lock:
            self._data = {}
            self._save()
            logging.debug("Cleared OnDeck tracker for new run")

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


class CachePriorityManager:
    """Manages priority scoring and smart eviction for cached files.

    Uses metadata from CacheTimestampTracker, WatchlistTracker, and OnDeckTracker
    to calculate priority scores. Lower-priority items are evicted first when
    cache space is needed.

    Priority Score (0-100):
    - Base score: 50
    - Source type: +20 for ondeck, +0 for watchlist (OnDeck = actively watching)
    - User count: +5 per user (max +15) - multiple users = popular
    - Cache recency: +5 to +15 based on hours cached (avoid churn)
    - Watchlist age: +10 if fresh, 0 if >30 days, -10 if >60 days
    - OnDeck age: +10 if recently watched, 0 if >30 days, -10 if >60 days
    - Episode position: +15 for current OnDeck, +10 for next X episodes, 0 otherwise

    Eviction Philosophy:
    - Watchlist items are evicted first (lower base priority)
    - Only when watchlist is exhausted should OnDeck items be considered
    - Recently added items (watchlist or ondeck) get priority boost
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

    def calculate_priority(self, cache_path: str) -> int:
        """Calculate 0-100 priority score for a cached file.

        Higher score = more likely to be watched soon = keep longer.
        Lower score = evict first when space is needed.

        Eviction philosophy: Watchlist items evicted first, OnDeck protected.

        Args:
            cache_path: Path to the cached file.

        Returns:
            Priority score between 0 and 100.
        """
        score = 50  # Base score

        # Factor 1: Source Type (+20 for ondeck, +0 for watchlist)
        # OnDeck means user is actively watching this content - protect it
        source = self.timestamp_tracker.get_source(cache_path)
        is_ondeck = source == "ondeck"
        if is_ondeck:
            score += 20

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

        # Factor 3: Cache Recency (+15 if cached in last 24h, scaled down)
        # Recently cached = recent interest, avoid churn from moving back and forth
        hours_cached = self._get_hours_since_cached(cache_path)
        if hours_cached >= 0:  # -1 means no timestamp
            if hours_cached < 24:
                score += 15
            elif hours_cached < 72:
                score += 10
            elif hours_cached < 168:  # 7 days
                score += 5

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

        # Factor 5: OnDeck Age (+10 if recently watched, 0 if >30 days, -10 if >60 days)
        # Items that haven't been watched lately get lower priority
        # But still protected vs watchlist due to +20 base for ondeck
        if is_ondeck and ondeck_entry:
            last_seen_str = ondeck_entry.get('last_seen')
            if last_seen_str:
                days_since_seen = self._get_days_since_last_seen(last_seen_str)
                if days_since_seen >= 0:
                    if days_since_seen < 7:
                        score += 10  # Recently watched
                    elif days_since_seen > 60:
                        score -= 10  # Stale OnDeck item
                    # 7-60 days: no adjustment (0)

        # Factor 6: Episode Position (+15 for current OnDeck, +10 for next X episodes, 0 otherwise)
        # Current/next episodes in a series get higher priority
        # X = half of number_episodes setting (so if prefetching 5 episodes, prioritize next 2-3)
        if self._is_tv_episode(cache_path):
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
        # Get episode info for this file
        ep_info = self.ondeck_tracker.get_episode_info(cache_path)
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

        Args:
            cache_path: Path to the cached file.

        Returns:
            True if this is a TV episode with episode info, False otherwise.
        """
        ep_info = self.ondeck_tracker.get_episode_info(cache_path)
        return ep_info is not None and ep_info.get('show') is not None


class PlexcachedMigration:
    """One-time migration to create .plexcached backups for existing cached files.

    For users upgrading from older versions, files may exist on cache without
    a corresponding .plexcached backup on the array. This migration scans the
    exclude file and creates .plexcached backups for any files that need them.
    """

    MIGRATION_FLAG = "plexcache_migration_v2.complete"

    def __init__(self, exclude_file: str, cache_dir: str, real_source: str,
                 script_folder: str, is_unraid: bool = False):
        """Initialize the migration helper.

        Args:
            exclude_file: Path to plexcache_mover_files_to_exclude.txt
            cache_dir: Cache directory path (e.g., /mnt/cache_downloads/)
            real_source: Array source path (e.g., /mnt/user/)
            script_folder: Folder where the script lives (for flag file)
            is_unraid: Whether running on Unraid (affects path handling)
        """
        self.exclude_file = exclude_file
        self.cache_dir = cache_dir
        self.real_source = real_source
        self.flag_file = os.path.join(script_folder, self.MIGRATION_FLAG)
        self.is_unraid = is_unraid

    def needs_migration(self) -> bool:
        """Check if migration has already been completed."""
        return not os.path.exists(self.flag_file)

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

        if not os.path.exists(self.exclude_file):
            logging.info("No exclude file found, nothing to migrate")
            self._mark_complete()
            return 0, 0, 0

        # Read exclude file to get list of cached files (deduplicated)
        with open(self.exclude_file, 'r') as f:
            all_lines = [line.strip() for line in f if line.strip()]
            cache_files = list(dict.fromkeys(all_lines))
            duplicates_removed = len(all_lines) - len(cache_files)

        if not cache_files:
            logging.info("Exclude file is empty, nothing to migrate")
            self._mark_complete()
            return 0, 0, 0

        logging.info("=== PlexCache-R Migration ===")
        if duplicates_removed > 0:
            logging.info(f"Removed {duplicates_removed} duplicate entries from exclude list")
        logging.info(f"Checking {len(cache_files)} unique files in exclude list...")

        files_needing_migration = []

        # Find files that need migration (on cache but no .plexcached on array)
        for cache_file in cache_files:
            if not os.path.isfile(cache_file):
                logging.debug(f"Cache file no longer exists, skipping: {cache_file}")
                continue

            # Derive array path from cache path
            array_file = cache_file.replace(self.cache_dir, self.real_source, 1)

            # On Unraid, check user0 (direct array) path
            if self.is_unraid:
                array_file_check = array_file.replace("/mnt/user/", "/mnt/user0/", 1)
            else:
                array_file_check = array_file

            plexcached_file = array_file_check + PLEXCACHED_EXTENSION

            # Check if .plexcached already exists OR original exists on array
            if os.path.isfile(plexcached_file):
                logging.debug(f"Already has .plexcached backup: {cache_file}")
                continue

            if os.path.isfile(array_file_check):
                logging.debug(f"Original exists on array, no migration needed: {cache_file}")
                continue

            # This file needs migration
            files_needing_migration.append((cache_file, array_file_check, plexcached_file))

        if not files_needing_migration:
            logging.info("All files already have backups, no migration needed")
            self._mark_complete()
            return 0, len(cache_files), 0

        # Calculate total size of files to migrate
        total_bytes = 0
        for cache_file, _, _ in files_needing_migration:
            try:
                total_bytes += os.path.getsize(cache_file)
            except OSError:
                pass

        total_gb = total_bytes / (1024 ** 3)
        logging.info(f"Found {len(files_needing_migration)} files needing .plexcached backup ({total_gb:.2f} GB)")

        if dry_run:
            logging.info("[DRY RUN] Would create the following backups:")
            for cache_file, array_file, plexcached_file in files_needing_migration:
                logging.info(f"  {cache_file} -> {plexcached_file}")
            return 0, 0, 0

        # Perform migration with progress tracking using thread pool
        logging.info(f"Starting migration with {max_concurrent} concurrent copies...")

        # Thread-safe counters and state
        self._migration_lock = threading.Lock()
        self._migrated = 0
        self._errors = 0
        self._completed_bytes = 0
        self._total_files = len(files_needing_migration)
        self._total_bytes = total_bytes
        self._active_files = {}  # Thread ID -> (filename, size)
        self._last_display_lines = 0

        def migrate_file(args):
            cache_file, array_file, plexcached_file = args
            thread_id = threading.get_ident()
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

                # Copy cache file to array as .plexcached
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
                    logging.info(f"Migrated: {filename} ({self._format_bytes(file_size)})")
                    return 0
                else:
                    logging.error(f"Failed to verify: {plexcached_file}")
                    with self._migration_lock:
                        self._errors += 1
                        if thread_id in self._active_files:
                            del self._active_files[thread_id]
                    return 1

            except Exception as e:
                logging.error(f"Error migrating {cache_file}: {type(e).__name__}: {e}")
                with self._migration_lock:
                    self._errors += 1
                    if thread_id in self._active_files:
                        del self._active_files[thread_id]
                return 1

        with ThreadPoolExecutor(max_workers=max_concurrent) as executor:
            list(executor.map(migrate_file, files_needing_migration))

        # Print final progress
        with self._migration_lock:
            self._print_progress(final=True)

        migrated = self._migrated
        errors = self._errors

        skipped = len(cache_files) - len(files_needing_migration)
        logging.info(f"=== Migration Complete ===")
        logging.info(f"  Migrated: {migrated} files")
        logging.info(f"  Skipped (already had backup): {skipped} files")
        logging.info(f"  Errors: {errors}")

        if errors == 0:
            self._mark_complete()
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

    def _format_bytes(self, bytes_value: int) -> str:
        """Format bytes into human-readable string (e.g., '1.5 GB')."""
        if bytes_value < 1024:
            return f"{bytes_value} B"
        elif bytes_value < 1024 ** 2:
            return f"{bytes_value / 1024:.1f} KB"
        elif bytes_value < 1024 ** 3:
            return f"{bytes_value / (1024 ** 2):.1f} MB"
        else:
            return f"{bytes_value / (1024 ** 3):.1f} GB"

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
        completed_str = self._format_bytes(self._completed_bytes)
        total_str = self._format_bytes(self._total_bytes)
        data_progress = f"{completed_str} / {total_str}"

        active_files = list(self._active_files.values())

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
                    size_str = self._format_bytes(file_size)
                    lines.append(f"    -> {display_name} ({size_str})")
                if len(active_files) > 5:
                    lines.append(f"    ... and {len(active_files) - 5} more")

            # Print all lines and track count for next clear
            for line in lines:
                print(line)
            self._last_display_lines = len(lines)


class FilePathModifier:
    """Handles file path modifications and conversions."""
    
    def __init__(self, plex_source: str, real_source: str, 
                 plex_library_folders: List[str], nas_library_folders: List[str]):
        self.plex_source = plex_source
        self.real_source = real_source
        self.plex_library_folders = plex_library_folders
        self.nas_library_folders = nas_library_folders
    
    def modify_file_paths(self, files: List[str]) -> List[str]:
        """Modify file paths from Plex paths to real system paths."""
        if files is None:
            return []

        logging.debug("Editing file paths...")

        result = []
        for file_path in files:
            # Skip paths already in real_source format (prevents double-conversion)
            # This is critical when plex_source is "/" - all paths start with "/"
            if file_path.startswith(self.real_source):
                logging.debug(f"Path already converted, skipping: {file_path}")
                result.append(file_path)
                continue

            # Pass through paths that are already converted (don't start with plex_source)
            if not file_path.startswith(self.plex_source):
                result.append(file_path)
                continue

            logging.debug(f"Original path: {file_path}")

            # Replace the plex_source with the real_source in the file path
            file_path = file_path.replace(self.plex_source, self.real_source, 1)

            # Determine which library folder is in the file path
            folder_matched = False
            for j, folder in enumerate(self.plex_library_folders):
                if folder in file_path:
                    # Replace the plex library folder with the corresponding NAS library folder
                    file_path = file_path.replace(folder, self.nas_library_folders[j])
                    folder_matched = True
                    break

            if not folder_matched:
                logging.warning(f"Path conversion: No matching library folder found for: {file_path}")

            result.append(file_path)
            logging.debug(f"Edited path: {file_path}")

        return result


class SubtitleFinder:
    """Handles subtitle file discovery and operations."""
    
    def __init__(self, subtitle_extensions: Optional[List[str]] = None):
        if subtitle_extensions is None:
            subtitle_extensions = [".srt", ".vtt", ".sbv", ".sub", ".idx"]
        self.subtitle_extensions = subtitle_extensions
    
    def get_media_subtitles(self, media_files: List[str], files_to_skip: Optional[Set[str]] = None) -> List[str]:
        """Get subtitle files for media files."""
        logging.debug("Fetching subtitles...")
        
        files_to_skip = set() if files_to_skip is None else set(files_to_skip)
        processed_files = set()
        all_media_files = media_files.copy()
        
        for file in media_files:
            if file in files_to_skip or file in processed_files:
                continue
            processed_files.add(file)
            
            directory_path = os.path.dirname(file)
            if os.path.exists(directory_path):
                subtitle_files = self._find_subtitle_files(directory_path, file)
                all_media_files.extend(subtitle_files)
                for subtitle_file in subtitle_files:
                    logging.debug(f"Subtitle found: {subtitle_file}")

        return all_media_files
    
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
                 watchlist_tracker: Optional['WatchlistTracker'] = None):
        self.real_source = real_source
        self.cache_dir = cache_dir
        self.is_unraid = is_unraid
        self.mover_cache_exclude_file = mover_cache_exclude_file or ""
        self.timestamp_tracker = timestamp_tracker
        self.cache_retention_hours = cache_retention_hours
        self.ondeck_tracker = ondeck_tracker
        self.watchlist_tracker = watchlist_tracker

    def _add_to_exclude_file(self, cache_file_name: str) -> None:
        """Add a file to the exclude list."""
        if self.mover_cache_exclude_file:
            # Read existing entries to avoid duplicates
            existing = set()
            if os.path.exists(self.mover_cache_exclude_file):
                with open(self.mover_cache_exclude_file, "r") as f:
                    existing = {line.strip() for line in f if line.strip()}
            if cache_file_name not in existing:
                with open(self.mover_cache_exclude_file, "a") as f:
                    f.write(f"{cache_file_name}\n")
                logging.debug(f"Added to exclude file: {cache_file_name}")

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

        for file in files:
            if file in processed_files or (files_to_skip and file in files_to_skip):
                continue
            processed_files.add(file)

            cache_file_name = self._get_cache_paths(file)[1]
            cache_files_to_exclude.append(cache_file_name)

            if destination == 'array':
                should_add, was_removed = self._should_add_to_array(file, cache_file_name, media_to_cache)
                if was_removed:
                    cache_files_removed.append(cache_file_name)
                if should_add:
                    media_to.append(file)
                    logging.info(f"Adding file to array: {file}")

            elif destination == 'cache':
                if self._should_add_to_cache(file, cache_file_name):
                    media_to.append(file)
                    logging.info(f"Adding file to cache: {file}")

        # Remove any cache files that were deleted during filtering from the exclude list
        if cache_files_removed:
            self.remove_files_from_exclude_list(cache_files_removed)

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

        array_file = file.replace("/mnt/user/", "/mnt/user0/", 1) if self.is_unraid else file
        array_path = os.path.dirname(array_file)

        # Check if exact file already exists on array
        if os.path.isfile(array_file):
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
        old_plexcached = find_matching_plexcached(array_path, cache_identity)
        if old_plexcached and old_plexcached != expected_plexcached:
            # Found a .plexcached with different filename - this is a true upgrade scenario
            # Let _move_to_array handle it
            logging.debug(f"Found old .plexcached for upgrade: {old_plexcached}")
            return True, False

        return True, False  # File should be added to the array

    def _should_add_to_cache(self, file: str, cache_file_name: str) -> bool:
        """Determine if a file should be added to the cache."""
        array_file = file.replace("/mnt/user/", "/mnt/user0/", 1) if self.is_unraid else file

        # Check if file already exists on cache
        if os.path.isfile(cache_file_name):
            # File already on cache - ensure it's protected
            self._add_to_exclude_file(cache_file_name)

            # Record timestamp if not already tracked (for retention)
            if self.timestamp_tracker:
                self.timestamp_tracker.record_cache_time(cache_file_name, "pre-existing")

            logging.debug(f"File already on cache, added to exclude list: {os.path.basename(cache_file_name)}")

            # If array version also exists, remove it (cache is authoritative)
            if os.path.isfile(array_file):
                try:
                    os.remove(array_file)
                    logging.info(f"Removed array version of file: {array_file}")
                except FileNotFoundError:
                    pass  # File already removed
                except OSError as e:
                    logging.error(f"Failed to remove array file {array_file}: {type(e).__name__}: {e}")

            return False

        return True
    
    def _get_cache_paths(self, file: str) -> Tuple[str, str]:
        """Get cache path and filename for a given file."""
        # Get the cache path by replacing the real source directory with the cache directory
        cache_path = os.path.dirname(file).replace(self.real_source, self.cache_dir, 1)
        
        # Get the cache file name by joining the cache path with the base name of the file
        cache_file_name = os.path.join(cache_path, os.path.basename(file))
        
        return cache_path, cache_file_name

    def get_files_to_move_back_to_array(self, current_ondeck_items: Set[str],
                                       current_watchlist_items: Set[str]) -> Tuple[List[str], List[str]]:
        """Get files in cache that should be moved back to array because they're no longer needed.

        For TV shows: Episodes before the OnDeck episode are considered watched and will be moved back.
                      Episodes >= OnDeck episode are kept (they're upcoming/current).
        For movies: Moved back when no longer on OnDeck or watchlist.

        Retention period applies uniformly to all cached files to protect against
        accidental unwatching or watchlist removal.
        """
        files_to_move_back = []
        cache_paths_to_remove = []
        retained_count = 0

        try:
            # Read the exclude file to get all files currently in cache
            if not os.path.exists(self.mover_cache_exclude_file):
                logging.info("No exclude file found, nothing to move back")
                return files_to_move_back, cache_paths_to_remove

            with open(self.mover_cache_exclude_file, 'r') as f:
                cache_files = [line.strip() for line in f if line.strip()]

            logging.debug(f"Found {len(cache_files)} files in exclude list")

            # Build TV show tracking: {show_name: {season: min_episode}}
            # This tracks the minimum episode number that should be kept for each show/season
            tv_show_min_episodes: Dict[str, Dict[int, int]] = {}

            # Build movie set for simple name matching
            needed_movies = set()

            # Process OnDeck and watchlist items
            for item in current_ondeck_items | current_watchlist_items:
                tv_info = self._extract_tv_info(item)
                if tv_info:
                    show_name, season_num, episode_num = tv_info
                    if show_name not in tv_show_min_episodes:
                        tv_show_min_episodes[show_name] = {}
                    # Keep the minimum episode number for each season (the "current" episode)
                    if season_num not in tv_show_min_episodes[show_name]:
                        tv_show_min_episodes[show_name][season_num] = episode_num
                    else:
                        tv_show_min_episodes[show_name][season_num] = min(
                            tv_show_min_episodes[show_name][season_num], episode_num
                        )
                else:
                    # It's a movie - use filename matching
                    media_name = self._extract_media_name(item)
                    if media_name:
                        needed_movies.add(media_name)

            logging.debug(f"TV shows on deck/watchlist: {list(tv_show_min_episodes.keys())}")
            logging.debug(f"Movies on deck/watchlist: {len(needed_movies)}")

            # Check each file in cache
            for cache_file in cache_files:
                if not os.path.exists(cache_file):
                    logging.debug(f"Cache file no longer exists: {cache_file}")
                    cache_paths_to_remove.append(cache_file)
                    continue

                # Check if this is a TV show
                tv_info = self._extract_tv_info(cache_file)

                if tv_info:
                    # TV show episode logic
                    show_name, season_num, episode_num = tv_info

                    # Check if this show is still being watched
                    if show_name in tv_show_min_episodes:
                        # Find the minimum (current) season for this show
                        min_ondeck_season = min(tv_show_min_episodes[show_name].keys())

                        if season_num < min_ondeck_season:
                            # Previous season - user has moved past this season
                            logging.debug(f"TV episode in previous season (S{season_num:02d} < S{min_ondeck_season:02d}), will check retention: {show_name} S{season_num:02d}E{episode_num:02d}")
                        elif season_num > min_ondeck_season:
                            # Future season - keep (user may have pre-cached ahead)
                            logging.debug(f"TV episode in future season (S{season_num:02d} > S{min_ondeck_season:02d}), keeping: {show_name} S{season_num:02d}E{episode_num:02d}")
                            continue
                        else:
                            # Same season as OnDeck - check episode number
                            min_episode = tv_show_min_episodes[show_name][season_num]
                            if episode_num >= min_episode:
                                # Current or upcoming episode
                                logging.debug(f"TV episode still needed (E{episode_num:02d} >= E{min_episode:02d}), keeping: {show_name} S{season_num:02d}E{episode_num:02d}")
                                continue
                            else:
                                # Watched episode in current season
                                logging.debug(f"TV episode watched (E{episode_num:02d} < E{min_episode:02d}), will check retention: {show_name} S{season_num:02d}E{episode_num:02d}")
                    # Show not on deck/watchlist - will be moved back
                    media_name = show_name
                else:
                    # Movie logic - use filename matching
                    media_name = self._extract_media_name(cache_file)
                    if media_name is None:
                        logging.warning(f"Could not extract media name from path: {cache_file}")
                        continue

                    if media_name in needed_movies:
                        logging.debug(f"Movie still needed, keeping in cache: {media_name}")
                        continue

                # Media is no longer needed - check if retention period applies
                if self.timestamp_tracker and self.cache_retention_hours > 0:
                    if self.timestamp_tracker.is_within_retention_period(cache_file, self.cache_retention_hours):
                        remaining = self.timestamp_tracker.get_retention_remaining(cache_file, self.cache_retention_hours)
                        remaining_str = f"{remaining:.0f}h" if remaining >= 1 else f"{remaining * 60:.0f}m"
                        logging.info(f"Retention hold ({remaining_str} left): {media_name}")
                        retained_count += 1
                        continue

                # Media is no longer needed and retention doesn't apply, move this file back to array
                array_file = cache_file.replace(self.cache_dir, self.real_source, 1)

                logging.info(f"Media no longer needed, will move back to array: {media_name} - {cache_file}")
                files_to_move_back.append(array_file)
                cache_paths_to_remove.append(cache_file)

            if retained_count > 0:
                logging.info(f"Retained {retained_count} files due to cache retention period ({self.cache_retention_hours}h)")
            if files_to_move_back:
                logging.info(f"Found {len(files_to_move_back)} files to move back to array")

        except Exception as e:
            logging.exception(f"Error getting files to move back to array: {type(e).__name__}: {e}")

        return files_to_move_back, cache_paths_to_remove

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
                season_match = re.match(r'^(Season|Series)\s*(\d+)$', part, re.IGNORECASE)
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
                    re.match(r'^(Season|Series)\s*\d+$', part, re.IGNORECASE)
                    or re.match(r'^\d+$', part)
                    or re.match(r'^Specials$', part, re.IGNORECASE)
                ):
                    if i > 0:
                        return path_parts[i - 1]
                    break

            # For movies: return cleaned filename
            filename = os.path.basename(file_path)
            name, ext = os.path.splitext(filename)

            # Handle subtitle files - strip language code suffix (e.g., ".en", ".eng", ".es", ".forced")
            subtitle_extensions = {'.srt', '.sub', '.ass', '.ssa', '.vtt', '.idx'}
            if ext.lower() in subtitle_extensions:
                # Strip common language code patterns from the end
                name = re.sub(r'\.(en|eng|es|spa|fr|fra|de|deu|ger|it|ita|pt|por|ja|jpn|ko|kor|zh|chi|forced|sdh|cc|hi)$', '', name, flags=re.IGNORECASE)

            cleaned = re.sub(r'\s*\([^)]*\)$', '', name).strip()
            return cleaned

        except Exception:
            return None

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

            # Convert to set for O(1) lookup instead of O(n)
            paths_to_remove_set = set(cache_paths_to_remove)

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
                if os.path.exists(entry):
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
                 timestamp_tracker: Optional['CacheTimestampTracker'] = None):
        self.real_source = real_source
        self.cache_dir = cache_dir
        self.is_unraid = is_unraid
        self.file_utils = file_utils
        self.debug = debug
        self.mover_cache_exclude_file = mover_cache_exclude_file
        self.timestamp_tracker = timestamp_tracker
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

    def _format_size(self, size_bytes: int) -> str:
        """Format file size in human-readable format."""
        if size_bytes >= 1024 ** 3:
            return f"{size_bytes / (1024 ** 3):.1f}GB"
        elif size_bytes >= 1024 ** 2:
            return f"{size_bytes / (1024 ** 2):.1f}MB"
        elif size_bytes >= 1024:
            return f"{size_bytes / 1024:.1f}KB"
        return f"{size_bytes}B"

    def move_media_files(self, files: List[str], destination: str,
                        max_concurrent_moves_array: int, max_concurrent_moves_cache: int,
                        source_map: Optional[Dict[str, str]] = None) -> None:
        """Move media files to the specified destination.

        Args:
            files: List of file paths to move.
            destination: Either 'cache' or 'array'.
            max_concurrent_moves_array: Max concurrent moves to array.
            max_concurrent_moves_cache: Max concurrent moves to cache.
            source_map: Optional dict mapping file paths to their source ('ondeck' or 'watchlist').
        """
        # Store source map for use during moves
        self._source_map = source_map or {}
        logging.info(f"Moving media files to {destination}...")
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

        logging.info(f"Generated {len(move_commands)} move commands for {destination}")

        # Execute the move commands
        self._execute_move_commands(move_commands, max_concurrent_moves_array,
                                  max_concurrent_moves_cache, destination, total_bytes)
    
    def _get_paths(self, file_to_move: str) -> Tuple[str, str, str, str]:
        """Get all necessary paths for file moving."""
        # Get the user path
        user_path = os.path.dirname(file_to_move)
        
        # Get the relative path from the real source directory
        relative_path = os.path.relpath(user_path, self.real_source)
        
        # Get the cache path by joining the cache directory with the relative path
        cache_path = os.path.join(self.cache_dir, relative_path)
        
        # Get the cache file name by joining the cache path with the base name of the file to move
        cache_file_name = os.path.join(cache_path, os.path.basename(file_to_move))
        
        # Modify the user path if unraid is True
        if self.is_unraid:
            user_path = user_path.replace("/mnt/user/", "/mnt/user0/", 1)

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
            if os.path.isfile(user_file_name):
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
            # Check if file is already on cache
            if os.path.isfile(cache_file_name):
                # File already on cache - ensure it's in exclude file
                self._add_to_exclude_file(cache_file_name)

                # Check for stale exclude entries from upgrades (e.g., Radarr replaced the file)
                # Same media identity but different filename = old entry is stale
                self._cleanup_stale_exclude_entries(cache_file_name)

                logging.debug(f"File already on cache, ensured in exclude list: {os.path.basename(cache_file_name)}")
                return None

            # Check if file exists on array to copy
            if os.path.isfile(user_file_name):
                # Only create directories if not in debug mode (true dry-run)
                if not self.debug:
                    self.file_utils.create_directory_with_permissions(cache_path, user_file_name)
                move = (user_file_name, cache_path)
        return move

    def _add_to_exclude_file(self, cache_file_name: str) -> None:
        """Add a file to the exclude list (thread-safe)."""
        if self.mover_cache_exclude_file:
            with self._exclude_file_lock:
                # Read existing entries to avoid duplicates
                existing = set()
                if os.path.exists(self.mover_cache_exclude_file):
                    with open(self.mover_cache_exclude_file, "r") as f:
                        existing = {line.strip() for line in f if line.strip()}
                if cache_file_name not in existing:
                    with open(self.mover_cache_exclude_file, "a") as f:
                        f.write(f"{cache_file_name}\n")
                    logging.debug(f"Added to exclude file: {cache_file_name}")
                else:
                    logging.debug(f"Already in exclude file: {cache_file_name}")
        else:
            logging.warning(f"No exclude file configured, cannot track: {cache_file_name}")

    def _remove_from_exclude_file(self, cache_file_name: str) -> None:
        """Remove a file from the exclude list (thread-safe)."""
        if self.mover_cache_exclude_file and os.path.exists(self.mover_cache_exclude_file):
            with self._exclude_file_lock:
                try:
                    with open(self.mover_cache_exclude_file, "r") as f:
                        lines = [line.strip() for line in f if line.strip()]
                    if cache_file_name in lines:
                        lines.remove(cache_file_name)
                        with open(self.mover_cache_exclude_file, "w") as f:
                            for line in lines:
                                f.write(f"{line}\n")
                        logging.debug(f"Removed from exclude file: {cache_file_name}")
                except Exception as e:
                    logging.warning(f"Failed to remove from exclude file: {e}")

    def _cleanup_stale_exclude_entries(self, current_cache_file: str) -> None:
        """Remove stale exclude entries for the same media with different filenames.

        When Radarr/Sonarr upgrades a file on the cache, the old filename becomes stale
        in the exclude list. This finds and removes those entries.
        """
        if not self.mover_cache_exclude_file or not os.path.exists(self.mover_cache_exclude_file):
            return

        current_identity = get_media_identity(current_cache_file)
        current_dir = os.path.dirname(current_cache_file)

        with self._exclude_file_lock:
            try:
                with open(self.mover_cache_exclude_file, "r") as f:
                    lines = [line.strip() for line in f if line.strip()]

                stale_entries = []
                for entry in lines:
                    # Skip if it's the current file
                    if entry == current_cache_file:
                        continue

                    # Only check entries in the same directory (same media folder)
                    if os.path.dirname(entry) != current_dir:
                        continue

                    # Check if same media identity but file no longer exists
                    entry_identity = get_media_identity(entry)
                    if entry_identity == current_identity and not os.path.exists(entry):
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

        if self.debug:
            # Debug mode - no actual moves, just log what would happen
            with tqdm(total=total_count, desc=f"Moving to {destination}", unit="file",
                      bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]") as pbar:
                for move_cmd, cache_file_name, file_size, original_path in move_commands:
                    (src, dest) = move_cmd
                    if destination == 'cache':
                        plexcached_file = src + PLEXCACHED_EXTENSION
                        tqdm.write(f"[DEBUG] Would copy: {src} -> {cache_file_name}")
                        tqdm.write(f"[DEBUG] Would rename: {src} -> {plexcached_file}")
                    elif destination == 'array':
                        array_file = os.path.join(dest, os.path.basename(src))
                        plexcached_file = array_file + PLEXCACHED_EXTENSION
                        tqdm.write(f"[DEBUG] Would rename: {plexcached_file} -> {array_file}")
                        tqdm.write(f"[DEBUG] Would delete: {src}")
                    pbar.update(1)
        else:
            # Real move with thread pool
            max_concurrent_moves = max_concurrent_moves_array if destination == 'array' else max_concurrent_moves_cache

            # Create tqdm progress bar with data size info
            # ncols=80 keeps bar compact, mininterval=0.5 forces more frequent updates
            import sys
            total_size_str = self._format_bytes(total_bytes)
            with tqdm(total=total_count, desc=f"Moving to {destination} (0 B / {total_size_str})",
                      unit="file", bar_format="{l_bar}{bar:20}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
                      mininterval=0.5, ncols=80, file=sys.stdout) as pbar:
                self._tqdm_pbar = pbar

                from functools import partial
                with ThreadPoolExecutor(max_workers=max_concurrent_moves) as executor:
                    results = list(executor.map(partial(self._move_file, destination=destination), move_commands))

                errors = [result for result in results if result == 1]
                partial_successes = [result for result in results if result == 2]

            self._tqdm_pbar = None

            if partial_successes:
                logging.warning(f"Finished moving files: {len(errors)} errors, {len(partial_successes)} partial (missing .plexcached)")
            elif errors:
                logging.warning(f"Finished moving files with {len(errors)} errors.")
            else:
                logging.info(f"Finished moving {total_count} files successfully.")
    
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

        try:
            if destination == 'cache':
                result = self._move_to_cache(src, dest, cache_file_name, original_path)
            elif destination == 'array':
                result = self._move_to_array(src, dest, cache_file_name)
            else:
                result = 0

            # Update tqdm progress bar
            with self._progress_lock:
                self._completed_bytes += file_size
                if self._tqdm_pbar:
                    # Update description to show data progress
                    completed_str = self._format_bytes(self._completed_bytes)
                    total_str = self._format_bytes(self._total_bytes)
                    self._tqdm_pbar.set_description(f"Moving to {destination} ({completed_str} / {total_str})")
                    self._tqdm_pbar.update(1)
                    self._tqdm_pbar.refresh()  # Force display update

            return result
        except Exception as e:
            # Still update progress on error
            with self._progress_lock:
                if self._tqdm_pbar:
                    self._tqdm_pbar.update(1)
            tqdm.write(f"Error moving {filename}: {type(e).__name__}: {e}")
            return 1

    def _move_to_cache(self, array_file: str, cache_path: str, cache_file_name: str,
                       original_path: str = None) -> int:
        """Copy file to cache and rename array original to .plexcached.

        Order of operations ensures data safety:
        1. Check for and clean up old .plexcached if this is an upgrade
        2. Copy file to cache
        3. Verify copy succeeded
        4. Rename original to .plexcached (only after verified copy)
        5. Verify rename succeeded
        6. Record timestamp for cache retention

        If interrupted at any point, the original array file remains safe.
        Worst case: an orphaned cache copy exists that can be deleted.
        """
        plexcached_file = array_file + PLEXCACHED_EXTENSION
        array_path = os.path.dirname(array_file)

        try:
            # Step 0: Check for upgrade scenario - clean up old .plexcached if needed
            old_cache_file_to_remove = None
            if not os.path.isfile(plexcached_file):
                cache_identity = get_media_identity(cache_file_name)
                old_plexcached = find_matching_plexcached(array_path, cache_identity)
                if old_plexcached and old_plexcached != plexcached_file:
                    old_name = os.path.basename(old_plexcached).replace(PLEXCACHED_EXTENSION, '')
                    new_name = os.path.basename(cache_file_name)
                    logging.info(f"Upgrade detected during cache: {old_name} -> {new_name}")
                    os.remove(old_plexcached)
                    logging.debug(f"Deleted old .plexcached: {old_plexcached}")
                    # Build the old cache file path for exclude list cleanup
                    # The exclude list stores full cache paths, so join the cache directory with the old filename
                    old_cache_file_to_remove = os.path.join(os.path.dirname(cache_file_name), old_name)

            # Step 1: Copy file from array to cache (preserving metadata)
            logging.debug(f"Starting copy: {array_file} -> {cache_file_name}")
            shutil.copy2(array_file, cache_file_name)
            logging.debug(f"Copy complete: {os.path.basename(array_file)}")

            # Validate copy succeeded
            if not os.path.isfile(cache_file_name):
                raise IOError(f"Copy verification failed: cache file not created at {cache_file_name}")

            # Step 2: Rename array file to .plexcached
            os.rename(array_file, plexcached_file)
            logging.debug(f"Renamed array file: {array_file} -> {plexcached_file}")

            # Validate rename succeeded
            if os.path.isfile(array_file):
                raise IOError(f"Rename verification failed: original array file still exists at {array_file}")
            if not os.path.isfile(plexcached_file):
                raise IOError(f"Rename verification failed: .plexcached file not created at {plexcached_file}")

            # Step 3: Add to exclude file (and remove old entry if upgrade)
            self._add_to_exclude_file(cache_file_name)
            if old_cache_file_to_remove:
                self._remove_from_exclude_file(old_cache_file_to_remove)

            # Step 4: Record timestamp for cache retention with source info
            if self.timestamp_tracker:
                # Look up source from the source map using the original path (e.g., /mnt/user/...)
                source = self._source_map.get(original_path, "unknown") if original_path else "unknown"
                self.timestamp_tracker.record_cache_time(cache_file_name, source)

            # Log successful move using tqdm.write to avoid progress bar interference
            from tqdm import tqdm
            file_size = os.path.getsize(cache_file_name)
            size_str = self._format_bytes(file_size) if hasattr(self, '_format_bytes') else f"{file_size} bytes"
            tqdm.write(f"Successfully cached: {os.path.basename(cache_file_name)} ({size_str})")

            return 0
        except Exception as e:
            logging.error(f"Error copying to cache: {type(e).__name__}: {e}")
            # Attempt cleanup on failure
            self._cleanup_failed_cache_copy(array_file, cache_file_name)
            return 1

    def _move_to_array(self, cache_file: str, array_path: str, cache_file_name: str) -> int:
        """Move file from cache back to array.

        Handles four scenarios:
        1a. Exact .plexcached exists, same size: Rename it back to original (fast)
        1b. Exact .plexcached exists, different size: In-place upgrade detected
            - Delete old .plexcached, copy upgraded cache file to array
        2. Different .plexcached exists (same media, different filename/quality)
           - Delete old .plexcached, copy upgraded cache file to array
        3. No .plexcached: Copy from cache to array, then delete cache copy

        Returns:
            0: Success - array file exists and cache deleted
            1: Error - exception occurred during operation
        """
        try:
            # Derive the original array file path and .plexcached path
            array_file = os.path.join(array_path, os.path.basename(cache_file))
            plexcached_file = array_file + PLEXCACHED_EXTENSION

            # Scenario 1: Exact .plexcached exists (same filename)
            if os.path.isfile(plexcached_file):
                # Check for in-place upgrade (same filename, different size)
                cache_size = os.path.getsize(cache_file) if os.path.isfile(cache_file) else 0
                plexcached_size = os.path.getsize(plexcached_file)

                if cache_size > 0 and cache_size != plexcached_size:
                    # In-place upgrade: same filename but different file content
                    logging.info(f"In-place upgrade detected ({self._format_size(plexcached_size)} -> {self._format_size(cache_size)}): {os.path.basename(cache_file)}")
                    os.remove(plexcached_file)
                    shutil.copy2(cache_file, array_file)
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
                    os.rename(plexcached_file, array_file)
                    logging.debug(f"Restored array file: {plexcached_file} -> {array_file}")

            # Scenario 2: Check for filename-change upgrade (different .plexcached with same media identity)
            elif os.path.isfile(cache_file):
                cache_identity = get_media_identity(cache_file)
                old_plexcached = find_matching_plexcached(array_path, cache_identity)

                # Scenario 2: Upgraded file - old .plexcached exists with different name
                if old_plexcached and old_plexcached != plexcached_file:
                    old_name = os.path.basename(old_plexcached).replace(PLEXCACHED_EXTENSION, '')
                    new_name = os.path.basename(cache_file)
                    logging.info(f"Upgrade detected: {old_name} -> {new_name}")

                    # Delete the old .plexcached (it's outdated)
                    os.remove(old_plexcached)
                    logging.debug(f"Deleted old .plexcached: {old_plexcached}")

                    # Copy the upgraded cache file to array
                    cache_size = os.path.getsize(cache_file)
                    shutil.copy2(cache_file, array_file)
                    logging.debug(f"Copied upgraded file to array: {array_file}")

                    # Verify copy succeeded
                    if os.path.isfile(array_file):
                        array_size = os.path.getsize(array_file)
                        if cache_size != array_size:
                            logging.error(f"Size mismatch after copy! Cache: {cache_size}, Array: {array_size}. Keeping cache file.")
                            os.remove(array_file)
                            return 1

                # Scenario 3: No .plexcached at all - copy to array
                elif not os.path.isfile(array_file):
                    logging.debug(f"No .plexcached found, copying from cache to array: {cache_file}")
                    cache_size = os.path.getsize(cache_file)
                    shutil.copy2(cache_file, array_file)
                    logging.debug(f"Copied to array: {array_file}")

                    # Verify copy succeeded by comparing file sizes
                    if os.path.isfile(array_file):
                        array_size = os.path.getsize(array_file)
                        if cache_size != array_size:
                            logging.error(f"Size mismatch after copy! Cache: {cache_size}, Array: {array_size}. Keeping cache file.")
                            os.remove(array_file)
                            return 1

            # Delete cache copy only if array file now exists
            if os.path.isfile(array_file):
                if os.path.isfile(cache_file):
                    os.remove(cache_file)
                    logging.debug(f"Deleted cache file: {cache_file}")
                else:
                    logging.debug(f"Cache file already removed: {cache_file}")

                # Remove timestamp entry
                if self.timestamp_tracker:
                    self.timestamp_tracker.remove_entry(cache_file)

                # Log successful restore using tqdm.write to avoid progress bar interference
                from tqdm import tqdm
                file_size = os.path.getsize(array_file)
                size_str = self._format_bytes(file_size) if hasattr(self, '_format_bytes') else f"{file_size} bytes"
                tqdm.write(f"Successfully restored to array: {os.path.basename(array_file)} ({size_str})")

                return 0
            else:
                # This shouldn't happen, but log it if it does
                logging.error(f"Failed to create array file: {array_file}")
                return 1

        except Exception as e:
            logging.error(f"Error restoring to array: {type(e).__name__}: {e}")
            return 1

    def _cleanup_failed_cache_copy(self, array_file: str, cache_file_name: str) -> None:
        """Clean up after a failed cache copy operation."""
        plexcached_file = array_file + PLEXCACHED_EXTENSION
        try:
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

    def _format_bytes(self, bytes_value: int) -> str:
        """Format bytes into human-readable string (e.g., '1.5 GB')."""
        if bytes_value < 1024:
            return f"{bytes_value} B"
        elif bytes_value < 1024 ** 2:
            return f"{bytes_value / 1024:.1f} KB"
        elif bytes_value < 1024 ** 3:
            return f"{bytes_value / (1024 ** 2):.1f} MB"
        else:
            return f"{bytes_value / (1024 ** 3):.1f} GB"

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
                # Check if original already exists (shouldn't happen, but be safe)
                if os.path.exists(original_file):
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


class CacheCleanup:
    """Handles cleanup of empty folders in cache directories."""

    # Directories that should never be cleaned (safety check)
    _PROTECTED_PATHS = {'/', '/mnt', '/mnt/user', '/mnt/user0', '/home', '/var', '/etc', '/usr'}

    def __init__(self, cache_dir: str, library_folders: List[str] = None):
        if not cache_dir or not cache_dir.strip():
            raise ValueError("cache_dir cannot be empty")

        normalized_cache_dir = os.path.normpath(cache_dir)
        if normalized_cache_dir in self._PROTECTED_PATHS:
            raise ValueError(f"cache_dir cannot be a protected system directory: {cache_dir}")

        self.cache_dir = cache_dir
        self.library_folders = library_folders or []

    def cleanup_empty_folders(self) -> None:
        """Remove empty folders from cache directories."""
        logging.debug("Starting cache cleanup process...")
        cleaned_count = 0

        # Use configured library folders, or fall back to scanning cache_dir subdirectories
        if self.library_folders:
            subdirs_to_clean = self.library_folders
        else:
            # Fallback: scan all subdirectories in cache_dir
            try:
                subdirs_to_clean = [d for d in os.listdir(self.cache_dir)
                                   if os.path.isdir(os.path.join(self.cache_dir, d))]
            except OSError as e:
                logging.error(f"Could not list cache directory {self.cache_dir}: {type(e).__name__}: {e}")
                subdirs_to_clean = []

        for subdir in subdirs_to_clean:
            subdir_path = os.path.join(self.cache_dir, subdir)
            if os.path.exists(subdir_path):
                logging.debug(f"Cleaning up {subdir} directory: {subdir_path}")
                cleaned_count += self._cleanup_directory(subdir_path)
            else:
                logging.debug(f"Directory does not exist, skipping: {subdir_path}")
        
        if cleaned_count > 0:
            logging.info(f"Cleaned up {cleaned_count} empty folders")
    
    def _cleanup_directory(self, directory_path: str) -> int:
        """Recursively remove empty folders from a directory."""
        cleaned_count = 0
        
        try:
            # Walk through the directory tree from bottom up
            for root, dirs, files in os.walk(directory_path, topdown=False):
                for dir_name in dirs:
                    dir_path = os.path.join(root, dir_name)
                    try:
                        # Check if directory is empty
                        if not os.listdir(dir_path):
                            os.rmdir(dir_path)
                            logging.debug(f"Removed empty folder: {dir_path}")
                            cleaned_count += 1
                    except OSError as e:
                        logging.debug(f"Could not remove directory {dir_path}: {type(e).__name__}: {e}")
        except Exception as e:
            logging.error(f"Error cleaning up directory {directory_path}: {type(e).__name__}: {e}")
        
        return cleaned_count 
