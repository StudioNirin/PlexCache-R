"""
File operations for PlexCache.
Handles file moving, filtering, subtitle operations, and path modifications.
"""

import os
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import List, Set, Optional, Tuple


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
                 mover_cache_exclude_file: str):
        self.real_source = real_source
        self.cache_dir = cache_dir
        self.is_unraid = is_unraid
        self.mover_cache_exclude_file = mover_cache_exclude_file or ""
    
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
        """Get files in cache that should be moved back to array because they're no longer needed."""
        files_to_move_back = []
        cache_paths_to_remove = []
        
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
                
                # Show is no longer needed, move this file back to array
                array_file = cache_file.replace(self.cache_dir, self.real_source, 1)
                
                logging.info(f"Show no longer needed, will move back to array: {show_name} - {cache_file}")
                files_to_move_back.append(array_file)
                cache_paths_to_remove.append(cache_file)
            
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
    """Handles file moving operations."""

    def __init__(self, real_source: str, cache_dir: str, is_unraid: bool,
                 file_utils, debug: bool = False, mover_cache_exclude_file: Optional[str] = None):
        self.real_source = real_source
        self.cache_dir = cache_dir
        self.is_unraid = is_unraid
        self.file_utils = file_utils
        self.debug = debug
        self.mover_cache_exclude_file = mover_cache_exclude_file
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
        """Get the move command for a file."""
        move = None
        if destination == 'array':
            # Only create directories if not in debug mode (true dry-run)
            if not self.debug:
                self.file_utils.create_directory_with_permissions(user_path, cache_file_name)
            if os.path.isfile(cache_file_name):
                move = (cache_file_name, user_path)
        elif destination == 'cache':
            # Only create directories if not in debug mode (true dry-run)
            if not self.debug:
                self.file_utils.create_directory_with_permissions(cache_path, user_file_name)
            if not os.path.isfile(cache_file_name):
                move = (user_file_name, cache_path)
        return move
    
    def _execute_move_commands(self, move_commands: List[Tuple[Tuple[str, str], str]], 
                             max_concurrent_moves_array: int, max_concurrent_moves_cache: int, 
                             destination: str) -> None:
        """Execute the move commands."""
        if self.debug:
            for move_cmd, cache_file_name in move_commands:
                logging.info(move_cmd)
        else:
            max_concurrent_moves = max_concurrent_moves_array if destination == 'array' else max_concurrent_moves_cache
            from functools import partial
            with ThreadPoolExecutor(max_workers=max_concurrent_moves) as executor:
                results = list(executor.map(partial(self._move_file, destination=destination), move_commands))
                errors = [result for result in results if result != 0]
                logging.info(f"Finished moving files with {len(errors)} errors.")
    
    def _move_file(self, move_cmd_with_cache: Tuple[Tuple[str, str], str], destination: str) -> int:
        """Move a single file and update exclude file if moving to cache."""
        (src, dest), cache_file_name = move_cmd_with_cache
        try:
            self.file_utils.move_file(src, dest)
            logging.info(f"Moved file from {src} to {dest} with original permissions and owner.")
            # Only append to exclude file if moving to cache and move succeeded
            # Use lock to prevent concurrent writes from corrupting the file
            if destination == 'cache' and self.mover_cache_exclude_file:
                with self._exclude_file_lock:
                    with open(self.mover_cache_exclude_file, "a") as f:
                        f.write(f"{cache_file_name}\n")
            return 0
        except Exception as e:
            logging.error(f"Error moving file: {type(e).__name__}: {e}")
            return 1


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