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

# Extension used to mark array files that have been cached
PLEXCACHED_EXTENSION = ".plexcached"


class CacheTimestampTracker:
    """Thread-safe tracker for when files were cached.

    Maintains a JSON file with timestamps of when each file was copied to cache.
    Used to implement cache retention periods - files cached less than X hours ago
    won't be moved back to array even if they're no longer in OnDeck/watchlist.
    """

    def __init__(self, timestamp_file: str):
        """Initialize the tracker with the path to the timestamp file.

        Args:
            timestamp_file: Path to the JSON file storing timestamps.
        """
        self.timestamp_file = timestamp_file
        self._lock = threading.Lock()
        self._timestamps: Dict[str, str] = {}
        self._load()

    def _load(self) -> None:
        """Load timestamps from file."""
        try:
            if os.path.exists(self.timestamp_file):
                with open(self.timestamp_file, 'r', encoding='utf-8') as f:
                    self._timestamps = json.load(f)
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

    def record_cache_time(self, cache_file_path: str) -> None:
        """Record the current time as when a file was cached.

        Args:
            cache_file_path: The path to the cached file.
        """
        with self._lock:
            self._timestamps[cache_file_path] = datetime.now().isoformat()
            self._save()
            logging.debug(f"Recorded cache timestamp for: {cache_file_path}")

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
                cached_time = datetime.fromisoformat(self._timestamps[cache_file_path])
                age_hours = (datetime.now() - cached_time).total_seconds() / 3600

                if age_hours < retention_hours:
                    logging.debug(
                        f"File still within retention period ({age_hours:.1f}h < {retention_hours}h): "
                        f"{cache_file_path}"
                    )
                    return True
                return False
            except (ValueError, TypeError) as e:
                logging.warning(f"Invalid timestamp for {cache_file_path}: {e}")
                return False

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

        logging.info("Editing file paths...")

        result = []
        for file_path in files:
            # Pass through paths that are already converted (don't start with plex_source)
            if not file_path.startswith(self.plex_source):
                result.append(file_path)
                continue

            logging.info(f"Original path: {file_path}")

            # Replace the plex_source with the real_source in the file path
            file_path = file_path.replace(self.plex_source, self.real_source, 1)

            # Determine which library folder is in the file path
            for j, folder in enumerate(self.plex_library_folders):
                if folder in file_path:
                    # Replace the plex library folder with the corresponding NAS library folder
                    file_path = file_path.replace(folder, self.nas_library_folders[j])
                    break

            result.append(file_path)
            logging.info(f"Edited path: {file_path}")

        return result


class SubtitleFinder:
    """Handles subtitle file discovery and operations."""
    
    def __init__(self, subtitle_extensions: Optional[List[str]] = None):
        if subtitle_extensions is None:
            subtitle_extensions = [".srt", ".vtt", ".sbv", ".sub", ".idx"]
        self.subtitle_extensions = subtitle_extensions
    
    def get_media_subtitles(self, media_files: List[str], files_to_skip: Optional[Set[str]] = None) -> List[str]:
        """Get subtitle files for media files."""
        logging.info("Fetching subtitles...")
        
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
                    logging.info(f"Subtitle found: {subtitle_file}")

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
                 cache_retention_hours: int = 12):
        self.real_source = real_source
        self.cache_dir = cache_dir
        self.is_unraid = is_unraid
        self.mover_cache_exclude_file = mover_cache_exclude_file or ""
        self.timestamp_tracker = timestamp_tracker
        self.cache_retention_hours = cache_retention_hours
    
    def filter_files(self, files: List[str], destination: str, 
                    media_to_cache: Optional[List[str]] = None, 
                    files_to_skip: Optional[Set[str]] = None) -> List[str]:
        """Filter files based on destination and conditions."""
        if media_to_cache is None:
            media_to_cache = []

        processed_files = set()
        media_to = []
        cache_files_to_exclude = []

        if not files:
            return []

        for file in files:
            if file in processed_files or (files_to_skip and file in files_to_skip):
                continue
            processed_files.add(file)
            
            cache_file_name = self._get_cache_paths(file)[1]
            cache_files_to_exclude.append(cache_file_name)
            
            if destination == 'array':
                if self._should_add_to_array(file, cache_file_name, media_to_cache):
                    media_to.append(file)
                    logging.info(f"Adding file to array: {file}")

            elif destination == 'cache':
                if self._should_add_to_cache(file, cache_file_name):
                    media_to.append(file)
                    logging.info(f"Adding file to cache: {file}")

        return media_to
    
    def _should_add_to_array(self, file: str, cache_file_name: str, media_to_cache: List[str]) -> bool:
        """Determine if a file should be added to the array."""
        if file in media_to_cache:
            return False

        array_file = file.replace("/mnt/user/", "/mnt/user0/", 1) if self.is_unraid else file

        if os.path.isfile(array_file):
            # File already exists in the array, try to remove cache version
            try:
                os.remove(cache_file_name)
                logging.info(f"Removed cache version of file: {cache_file_name}")
            except FileNotFoundError:
                pass  # File already removed or never existed
            except OSError as e:
                logging.error(f"Failed to remove cache file {cache_file_name}: {type(e).__name__}: {e}")
            return False  # No need to add to array
        return True  # Otherwise, the file should be added to the array

    def _should_add_to_cache(self, file: str, cache_file_name: str) -> bool:
        """Determine if a file should be added to the cache."""
        array_file = file.replace("/mnt/user/", "/mnt/user0/", 1) if self.is_unraid else file

        if os.path.isfile(cache_file_name) and os.path.isfile(array_file):
            # Remove the array version when the file exists in the cache
            try:
                os.remove(array_file)
                logging.info(f"Removed array version of file: {array_file}")
            except FileNotFoundError:
                pass  # File already removed
            except OSError as e:
                logging.error(f"Failed to remove array file {array_file}: {type(e).__name__}: {e}")
            return False

        return not os.path.isfile(cache_file_name)
    
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

        Files within the cache retention period will be kept even if not in OnDeck/watchlist.
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

            logging.info(f"Found {len(cache_files)} files in exclude list")

            # Get shows that are still needed (in OnDeck or watchlist)
            needed_shows = set()
            for item in current_ondeck_items | current_watchlist_items:
                # Extract show name from path (e.g., "House Hunters (1999)" from "/path/to/House Hunters (1999) {imdb-tt0369117}/Season 263/...")
                show_name = self._extract_show_name(item)
                if show_name is not None:
                    needed_shows.add(show_name)

            # Check each file in cache
            for cache_file in cache_files:
                if not os.path.exists(cache_file):
                    logging.debug(f"Cache file no longer exists: {cache_file}")
                    cache_paths_to_remove.append(cache_file)
                    continue

                # Extract show name from cache file
                show_name = self._extract_show_name(cache_file)
                if show_name is None:
                    continue

                # If show is still needed, keep this file in cache
                if show_name in needed_shows:
                    logging.debug(f"Show still needed, keeping in cache: {show_name}")
                    continue

                # Check if file is within cache retention period
                if self.timestamp_tracker and self.cache_retention_hours > 0:
                    if self.timestamp_tracker.is_within_retention_period(cache_file, self.cache_retention_hours):
                        logging.info(f"File within retention period, keeping in cache: {cache_file}")
                        retained_count += 1
                        continue

                # Show is no longer needed and retention period has passed, move this file back to array
                array_file = cache_file.replace(self.cache_dir, self.real_source, 1)

                logging.info(f"Show no longer needed, will move back to array: {show_name} - {cache_file}")
                files_to_move_back.append(array_file)
                cache_paths_to_remove.append(cache_file)

            if retained_count > 0:
                logging.info(f"Retained {retained_count} files due to cache retention period ({self.cache_retention_hours}h)")
            logging.info(f"Found {len(files_to_move_back)} files to move back to array")

        except Exception as e:
            logging.exception(f"Error getting files to move back to array: {type(e).__name__}: {e}")

        return files_to_move_back, cache_paths_to_remove

    def _extract_show_name(self, file_path: str) -> Optional[str]:
        """Extract show name from file path. Returns None if not found."""
        try:
            # Normalize path and split using OS separator
            normalized_path = os.path.normpath(file_path)
            path_parts = normalized_path.split(os.sep)
            for i, part in enumerate(path_parts):
                if part.startswith('Season') or part.isdigit():
                    if i > 0:
                        return path_parts[i-1]
                    break
            return None
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

            # Convert to set for O(1) lookup instead of O(n)
            paths_to_remove_set = set(cache_paths_to_remove)

            # Remove specified files
            updated_files = [f for f in current_files if f not in paths_to_remove_set]

            # Write back updated list
            with open(self.mover_cache_exclude_file, 'w') as f:
                for file_path in updated_files:
                    f.write(f"{file_path}\n")

            logging.info(f"Removed {len(cache_paths_to_remove)} files from exclude list")
            return True

        except Exception as e:
            logging.exception(f"Error removing files from exclude list: {type(e).__name__}: {e}")
            return False


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
    
    def move_media_files(self, files: List[str], destination: str, 
                        max_concurrent_moves_array: int, max_concurrent_moves_cache: int) -> None:
        """Move media files to the specified destination."""
        logging.info(f"Moving media files to {destination}...")
        logging.debug(f"Total files to process: {len(files)}")
        
        processed_files = set()
        move_commands = []
        cache_file_names = []

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
                move_commands.append((move, cache_file_name))
                logging.debug(f"Added move command for: {file_to_move}")
            else:
                logging.debug(f"No move command generated for: {file_to_move}")
        
        logging.info(f"Generated {len(move_commands)} move commands for {destination}")
        
        # Execute the move commands
        self._execute_move_commands(move_commands, max_concurrent_moves_array, 
                                  max_concurrent_moves_cache, destination)
    
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
        - If .plexcached file exists: return command to restore+delete
        """
        move = None
        if destination == 'array':
            # Check if .plexcached version exists on array
            plexcached_file = user_file_name + PLEXCACHED_EXTENSION
            if os.path.isfile(plexcached_file):
                # Only create directories if not in debug mode (true dry-run)
                if not self.debug:
                    self.file_utils.create_directory_with_permissions(user_path, cache_file_name)
                move = (cache_file_name, user_path)
        elif destination == 'cache':
            # Check if file is already on cache
            if os.path.isfile(cache_file_name):
                # File already on cache - just ensure it's in exclude file
                self._add_to_exclude_file(cache_file_name)
                logging.info(f"File already on cache, added to exclude: {cache_file_name}")
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
    
    def _execute_move_commands(self, move_commands: List[Tuple[Tuple[str, str], str]],
                             max_concurrent_moves_array: int, max_concurrent_moves_cache: int,
                             destination: str) -> None:
        """Execute the move commands."""
        if self.debug:
            for move_cmd, cache_file_name in move_commands:
                (src, dest) = move_cmd
                if destination == 'cache':
                    plexcached_file = src + PLEXCACHED_EXTENSION
                    logging.info(f"[DEBUG] Would copy: {src} -> {cache_file_name}")
                    logging.info(f"[DEBUG] Would rename: {src} -> {plexcached_file}")
                elif destination == 'array':
                    array_file = os.path.join(dest, os.path.basename(src))
                    plexcached_file = array_file + PLEXCACHED_EXTENSION
                    logging.info(f"[DEBUG] Would rename: {plexcached_file} -> {array_file}")
                    logging.info(f"[DEBUG] Would delete: {src}")
        else:
            max_concurrent_moves = max_concurrent_moves_array if destination == 'array' else max_concurrent_moves_cache
            from functools import partial
            with ThreadPoolExecutor(max_workers=max_concurrent_moves) as executor:
                results = list(executor.map(partial(self._move_file, destination=destination), move_commands))
                errors = [result for result in results if result == 1]
                partial_successes = [result for result in results if result == 2]
                if partial_successes:
                    logging.warning(f"Finished moving files: {len(errors)} errors, {len(partial_successes)} partial (missing .plexcached)")
                else:
                    logging.info(f"Finished moving files with {len(errors)} errors.")
    
    def _move_file(self, move_cmd_with_cache: Tuple[Tuple[str, str], str], destination: str) -> int:
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
        (src, dest), cache_file_name = move_cmd_with_cache
        try:
            if destination == 'cache':
                return self._move_to_cache(src, dest, cache_file_name)
            elif destination == 'array':
                return self._move_to_array(src, dest, cache_file_name)
            return 0
        except Exception as e:
            logging.error(f"Error moving file: {type(e).__name__}: {e}")
            return 1

    def _move_to_cache(self, array_file: str, cache_path: str, cache_file_name: str) -> int:
        """Copy file to cache and rename array original to .plexcached.

        Order of operations ensures data safety:
        1. Copy file to cache
        2. Verify copy succeeded
        3. Rename original to .plexcached (only after verified copy)
        4. Verify rename succeeded
        5. Record timestamp for cache retention

        If interrupted at any point, the original array file remains safe.
        Worst case: an orphaned cache copy exists that can be deleted.
        """
        plexcached_file = array_file + PLEXCACHED_EXTENSION
        try:
            # Step 1: Copy file from array to cache (preserving metadata)
            logging.info(f"Starting copy: {array_file} -> {cache_file_name}")
            shutil.copy2(array_file, cache_file_name)
            logging.info(f"Copy complete: {os.path.basename(array_file)}")

            # Validate copy succeeded
            if not os.path.isfile(cache_file_name):
                raise IOError(f"Copy verification failed: cache file not created at {cache_file_name}")

            # Step 2: Rename array file to .plexcached
            os.rename(array_file, plexcached_file)
            logging.info(f"Renamed array file: {array_file} -> {plexcached_file}")

            # Validate rename succeeded
            if os.path.isfile(array_file):
                raise IOError(f"Rename verification failed: original array file still exists at {array_file}")
            if not os.path.isfile(plexcached_file):
                raise IOError(f"Rename verification failed: .plexcached file not created at {plexcached_file}")

            # Step 3: Add to exclude file
            self._add_to_exclude_file(cache_file_name)

            # Step 4: Record timestamp for cache retention
            if self.timestamp_tracker:
                self.timestamp_tracker.record_cache_time(cache_file_name)

            return 0
        except Exception as e:
            logging.error(f"Error copying to cache: {type(e).__name__}: {e}")
            # Attempt cleanup on failure
            self._cleanup_failed_cache_copy(array_file, cache_file_name)
            return 1

    def _move_to_array(self, cache_file: str, array_path: str, cache_file_name: str) -> int:
        """Restore .plexcached file and delete cache copy.

        Returns:
            0: Success - both .plexcached renamed and cache deleted
            1: Error - exception occurred during operation
            2: Partial - .plexcached file was missing (cache still deleted if present)
        """
        try:
            # Derive the original array file path and .plexcached path
            array_file = os.path.join(array_path, os.path.basename(cache_file))
            plexcached_file = array_file + PLEXCACHED_EXTENSION
            plexcached_missing = False

            # Step 1: Rename .plexcached back to original
            if os.path.isfile(plexcached_file):
                os.rename(plexcached_file, array_file)
                logging.info(f"Restored array file: {plexcached_file} -> {array_file}")
            else:
                logging.warning(f"No .plexcached file found to restore: {plexcached_file}")
                plexcached_missing = True

            # Step 2: Delete cache copy
            if os.path.isfile(cache_file):
                os.remove(cache_file)
                logging.info(f"Deleted cache file: {cache_file}")
            else:
                logging.debug(f"Cache file already removed: {cache_file}")

            # Step 3: Remove timestamp entry
            if self.timestamp_tracker:
                self.timestamp_tracker.remove_entry(cache_file)

            # Return appropriate status
            if plexcached_missing:
                return 2  # Partial success - no .plexcached to restore
            return 0
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
        logging.info("Starting cache cleanup process...")
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
        else:
            logging.info("No empty folders found to clean up")
    
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