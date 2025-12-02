"""
Main PlexCache application.
Orchestrates all components and provides the main business logic.
"""

import sys
import time
import logging
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Set, Optional
import os

from config import ConfigManager
from logging_config import LoggingManager
from system_utils import SystemDetector, PathConverter, FileUtils
from plex_api import PlexManager, CacheManager
from file_operations import FilePathModifier, SubtitleFinder, FileFilter, FileMover, CacheCleanup, PlexcachedRestorer


class PlexCacheApp:
    """Main PlexCache application class."""
    
    def __init__(self, config_file: str, skip_cache: bool = False, debug: bool = False):
        self.config_file = config_file
        self.skip_cache = skip_cache
        self.debug = debug
        self.start_time = time.time()
        
        # Initialize components
        self.config_manager = ConfigManager(config_file)
        self.system_detector = SystemDetector()
        self.path_converter = PathConverter(self.system_detector.is_linux)
        self.file_utils = FileUtils(self.system_detector.is_linux)
        
        # Will be initialized after config loading
        self.logging_manager = None
        self.plex_manager = None
        self.file_path_modifier = None
        self.subtitle_finder = None
        self.file_filter = None
        self.file_mover = None
        
        # State variables
        self.files_to_skip = []
        self.media_to_cache = []
        self.media_to_array = []
        self.ondeck_items = set()
        
    def run(self) -> None:
        """Run the main application."""
        try:
            # Setup logging first before any log messages
            self._setup_logging()
            logging.info("Starting PlexCache application...")
            logging.info("Phase 1: Logging setup complete")

            # Load configuration
            logging.info("Phase 2: Loading configuration")
            self.config_manager.load_config()
                  
            # Initialize components that depend on config
            logging.info("Phase 3: Initializing components")
            self._initialize_components()
            
            # Check paths
            logging.info("Phase 4: Validating paths")
            self._check_paths()
            
            # Connect to Plex
            logging.info("Phase 5: Connecting to Plex")
            self._connect_to_plex()
            
            # Check for active sessions
            logging.info("Phase 6: Checking active sessions")
            self._check_active_sessions()
            
            # Set debug mode
            logging.info("Phase 7: Setting debug mode")
            self._set_debug_mode()
            
            # Process media
            logging.info("Phase 8: Processing media")
            self._process_media()
            
            # Move files
            logging.info("Phase 9: Moving files")
            self._move_files()
            
            # Log summary and cleanup
            logging.info("Phase 10: Finalizing")
            self._finish()
            
            logging.info("PlexCache application completed successfully")
            
        except Exception as e:
            if self.logging_manager:
                logging.critical(f"Application error: {type(e).__name__}: {e}", exc_info=True)
            else:
                print(f"Application error: {type(e).__name__}: {e}")
            raise
    
    def _setup_logging(self) -> None:
        """Set up logging system."""
        self.logging_manager = LoggingManager(
            logs_folder=self.config_manager.paths.logs_folder,
            log_level="",  # Will be set from config
            max_log_files=5
        )
        self.logging_manager.setup_logging()
        self.logging_manager.setup_notification_handlers(
            self.config_manager.notification,
            self.system_detector.is_unraid,
            self.system_detector.is_docker
        )
        logging.info("*** PlexCache ***")
    
    def _initialize_components(self) -> None:
        """Initialize components that depend on configuration."""
        logging.info("Initializing application components...")
        
        # Initialize Plex manager
        logging.debug("Initializing Plex manager...")
        self.plex_manager = PlexManager(
            plex_url=self.config_manager.plex.plex_url,
            plex_token=self.config_manager.plex.plex_token,
            retry_limit=self.config_manager.performance.retry_limit,
            delay=self.config_manager.performance.delay
        )
        
        # Initialize file operation components
        logging.debug("Initializing file operation components...")
        self.file_path_modifier = FilePathModifier(
            plex_source=self.config_manager.paths.plex_source,
            real_source=self.config_manager.paths.real_source,
            plex_library_folders=self.config_manager.paths.plex_library_folders or [],
            nas_library_folders=self.config_manager.paths.nas_library_folders or []
        )
        
        self.subtitle_finder = SubtitleFinder()
        
        # Get cache files
        watchlist_cache, watched_cache, mover_exclude = self.config_manager.get_cache_files()
        logging.debug(f"Cache files: watchlist={watchlist_cache}, watched={watched_cache}, exclude={mover_exclude}")

        # Create exclude file on startup if it doesn't exist
        # This allows users to configure Mover settings before any files are moved
        if not mover_exclude.exists():
            mover_exclude.touch()
            logging.info(f"Created mover exclude file: {mover_exclude}")
        
        self.file_filter = FileFilter(
            real_source=self.config_manager.paths.real_source,
            cache_dir=self.config_manager.paths.cache_dir,
            is_unraid=self.system_detector.is_unraid,
            mover_cache_exclude_file=str(mover_exclude)
        )
        
        self.file_mover = FileMover(
            real_source=self.config_manager.paths.real_source,
            cache_dir=self.config_manager.paths.cache_dir,
            is_unraid=self.system_detector.is_unraid,
            file_utils=self.file_utils,
            debug=self.debug,
            mover_cache_exclude_file=str(mover_exclude)
        )
        
        self.cache_cleanup = CacheCleanup(
            self.config_manager.paths.cache_dir,
            self.config_manager.paths.nas_library_folders
        )
        logging.info("All components initialized successfully")
    
    def _check_paths(self) -> None:
        """Check that required paths exist and are accessible."""
        for path in [self.config_manager.paths.real_source, self.config_manager.paths.cache_dir]:
            self.file_utils.check_path_exists(path)
    
    def _connect_to_plex(self) -> None:
        """Connect to the Plex server."""
        self.plex_manager.connect()
    
    def _check_active_sessions(self) -> None:
        """Check for active Plex sessions."""
        sessions = self.plex_manager.get_active_sessions()
        if sessions:
            if self.config_manager.exit_if_active_session:
                logging.warning('There is an active session. Exiting...')
                sys.exit('There is an active session. Exiting...')
            else:
                self._process_active_sessions(sessions)
        else:
            logging.info('No active sessions found. Proceeding...')
    
    def _process_active_sessions(self, sessions: List) -> None:
        """Process active sessions and add files to skip list."""
        for session in sessions:
            try:
                media_path = self._get_media_path_from_session(session)
                if media_path:
                    logging.info(f"Skipping active session file: {media_path}")
                    self.files_to_skip.append(media_path)
            except Exception as e:
                logging.error(f"Error processing session {session}: {type(e).__name__}: {e}")

    def _get_media_path_from_session(self, session) -> Optional[str]:
        """Extract media file path from a Plex session. Returns None if unable to extract."""
        try:
            media = str(session.source())
            # Use regex for safer parsing: extract ID between first two colons
            match = re.search(r':(\d+):', media)
            if not match:
                logging.warning(f"Could not parse media ID from session source: {media}")
                return None

            media_id = int(match.group(1))
            media_item = self.plex_manager.plex.fetchItem(media_id)
            media_title = media_item.title
            media_type = media_item.type

            if media_type == "episode":
                show_title = media_item.grandparentTitle
                logging.warning(f"Active session detected, skipping: {show_title} - {media_title}")
            elif media_type == "movie":
                logging.warning(f"Active session detected, skipping: {media_title}")

            # Safely access media parts with bounds checking
            if not media_item.media:
                logging.warning(f"Media item '{media_title}' has no media entries")
                return None
            if not media_item.media[0].parts:
                logging.warning(f"Media item '{media_title}' has no parts")
                return None

            return media_item.media[0].parts[0].file

        except (ValueError, AttributeError) as e:
            logging.error(f"Error extracting media path: {type(e).__name__}: {e}")
            return None
    
    def _is_cache_expired(self, cache_file: Path, expiry_hours: int) -> bool:
        """Check if a cache file is expired. Returns True if expired or file doesn't exist."""
        if self.skip_cache or self.debug:
            return True
        try:
            if not cache_file.exists():
                return True
            mtime = datetime.fromtimestamp(cache_file.stat().st_mtime)
            return datetime.now() - mtime > timedelta(hours=expiry_hours)
        except (OSError, FileNotFoundError):
            # File was deleted between exists() check and stat() call
            return True

    def _set_debug_mode(self) -> None:
        """Set debug mode if enabled."""
        if self.debug:
            logging.getLogger().setLevel(logging.DEBUG)
            logging.warning("Debug mode is active, NO FILE WILL BE MOVED.")
        else:
            logging.getLogger().setLevel(logging.INFO)
    
    def _process_media(self) -> None:
        """Process all media types (onDeck, watchlist, watched)."""
        logging.info("Starting media processing...")

        # Use a set to collect already-modified paths (real source paths)
        modified_paths_set = set()

        # Fetch OnDeck Media
        logging.info("Fetching OnDeck media...")
        ondeck_media = self.plex_manager.get_on_deck_media(
            self.config_manager.plex.valid_sections or [],
            self.config_manager.plex.days_to_monitor,
            self.config_manager.plex.number_episodes,
            self.config_manager.plex.users_toggle,
            self.config_manager.plex.skip_ondeck or []
        )

        logging.info(f"Found {len(ondeck_media)} OnDeck items")

        # Edit file paths for OnDeck media (convert plex paths to real paths)
        logging.debug("Modifying file paths for OnDeck media...")
        modified_ondeck = self.file_path_modifier.modify_file_paths(list(ondeck_media))

        # Store modified OnDeck items for filtering later
        self.ondeck_items = set(modified_ondeck)
        modified_paths_set.update(self.ondeck_items)

        # Fetch subtitles for OnDeck media (already using real paths)
        logging.debug("Finding subtitles for OnDeck media...")
        ondeck_with_subtitles = self.subtitle_finder.get_media_subtitles(list(self.ondeck_items), files_to_skip=set(self.files_to_skip))
        subtitle_count = len(ondeck_with_subtitles) - len(self.ondeck_items)
        modified_paths_set.update(ondeck_with_subtitles)
        logging.debug(f"Found {subtitle_count} subtitle files for OnDeck media")

        # Process watchlist (returns already-modified paths)
        if self.config_manager.cache.watchlist_toggle:
            logging.info("Processing watchlist media...")
            watchlist_items = self._process_watchlist()
            if watchlist_items:
                modified_paths_set.update(watchlist_items)
                logging.info(f"Added {len(watchlist_items)} watchlist items to cache set")
        else:
            logging.info("Watchlist processing is disabled")

        # Process watched media
        if self.config_manager.cache.watched_move:
            logging.info("Processing watched media...")
            self._process_watched_media()
            logging.info(f"Added {len(self.media_to_array)} watched items to array move list")
        else:
            logging.info("Watched media processing is disabled")

        # Run modify_file_paths on all collected paths to ensure consistent path format
        # (some sources like _process_watchlist may return a mix of modified and unmodified paths)
        logging.debug("Finalizing media to cache list...")
        self.media_to_cache = self.file_path_modifier.modify_file_paths(list(modified_paths_set))
        logging.info(f"Total media items to cache: {len(self.media_to_cache)}")

        # Check for files that should be moved back to array (no longer needed in cache)
        logging.info("Checking for files to move back to array...")
        self._check_files_to_move_back_to_array()

    def _process_watchlist(self) -> set:
        """Process watchlist media (local API + remote RSS) and return a set of modified file paths and subtitles."""
        result_set = set()
        try:
            watchlist_cache, _, _ = self.config_manager.get_cache_files()
            watchlist_media_set, last_updated = CacheManager.load_media_from_cache(watchlist_cache)
            current_watchlist_set = set()

            logging.debug(f"Watchlist cache exists: {watchlist_cache.exists()}")
            logging.debug(f"Watchlist cache last updated: {last_updated}")
            logging.debug(f"Current watchlist items in cache: {len(watchlist_media_set)}")

            if self.system_detector.is_connected():
                # Determine if cache should be refreshed
                cache_expired = self._is_cache_expired(
                    watchlist_cache,
                    self.config_manager.cache.watchlist_cache_expiry
                )
                logging.debug(f"Cache expired: {cache_expired}")

                if cache_expired:
                    logging.info(f"Cache expired: {watchlist_cache}")
                    
                    # Delete old cache file if it exists
                    if watchlist_cache.exists():
                        try:
                            watchlist_cache.unlink()
                            logging.info(f"Cache file deleted: {watchlist_cache}")
                        except Exception as e:
                            logging.error(f"Failed to delete cache file {watchlist_cache}: {e}")
                    
                    # Reset memory sets to avoid old data
                    watchlist_media_set.clear()
                    current_watchlist_set.clear()
                    result_set.clear()
                    
                    # --- Local Plex users ---
                    fetched_watchlist = list(self.plex_manager.get_watchlist_media(
                        self.config_manager.plex.valid_sections,
                        self.config_manager.cache.watchlist_episodes,
                        self.config_manager.plex.users_toggle,
                        self.config_manager.plex.skip_watchlist
                    ))
                    for file_path in fetched_watchlist:
                        current_watchlist_set.add(file_path)
                        if file_path not in watchlist_media_set:
                            result_set.add(file_path)

                    watchlist_media_set.intersection_update(current_watchlist_set)
                    watchlist_media_set.update(result_set)

                    # --- Remote users via RSS ---
                    if self.config_manager.cache.remote_watchlist_toggle and self.config_manager.cache.remote_watchlist_rss_url:
                        logging.info("Fetching watchlist via RSS feed for remote users...")
                        try:
                            # Use get_watchlist_media with rss_url parameter; users_toggle=False because this is just RSS
                            remote_items = list(
                                self.plex_manager.get_watchlist_media(
                                    valid_sections=self.config_manager.plex.valid_sections,
                                    watchlist_episodes=self.config_manager.cache.watchlist_episodes,
                                    users_toggle=False,  # only RSS, no local Plex users
                                    skip_watchlist=[],
                                    rss_url=self.config_manager.cache.remote_watchlist_rss_url
                                )
                            )
                            logging.info(f"Found {len(remote_items)} remote watchlist items from RSS")
                            current_watchlist_set.update(remote_items)
                            result_set.update(remote_items)
                        except Exception as e:
                            logging.error(f"Failed to fetch remote watchlist via RSS: {str(e)}")


                    # Modify file paths and fetch subtitles
                    modified_items = self.file_path_modifier.modify_file_paths(list(result_set))
                    result_set.update(modified_items)
                    subtitles = self.subtitle_finder.get_media_subtitles(modified_items, files_to_skip=set(self.files_to_skip))
                    result_set.update(subtitles)

                    # Update cache file
                    CacheManager.save_media_to_cache(watchlist_cache, list(result_set))

                else:
                    logging.info("Loading watchlist media from cache...")
                    result_set.update(watchlist_media_set)
            else:
                logging.warning("Unable to connect to the internet, skipping fetching new watchlist media due to plexapi limitation.")
                logging.info("Loading watchlist media from cache...")
                result_set.update(watchlist_media_set)

        except Exception as e:
            logging.exception(f"An error occurred while processing the watchlist: {type(e).__name__}: {e}")

        return result_set

    
    def _process_watched_media(self) -> None:
        """Process watched media."""
        try:
            _, watched_cache, _ = self.config_manager.get_cache_files()
            watched_media_set, last_updated = CacheManager.load_media_from_cache(watched_cache)
            current_media_set = set()

            # Check if cache should be refreshed
            cache_expired = self._is_cache_expired(
                watched_cache,
                self.config_manager.cache.watched_cache_expiry
            )
            
            if cache_expired:
                logging.info("Fetching watched media...")

                # Get watched media from Plex server
                fetched_media = list(self.plex_manager.get_watched_media(
                    self.config_manager.plex.valid_sections,
                    last_updated,
                    self.config_manager.plex.users_toggle
                ))
                
                # Add fetched media to the current media set
                for file_path in fetched_media:
                    current_media_set.add(file_path)

                    # Check if file is not already in the watched media set
                    if file_path not in watched_media_set:
                        self.media_to_array.append(file_path)

                # Add new media to the watched media set
                watched_media_set.update(self.media_to_array)
                
                # Modify file paths and add subtitles
                self.media_to_array = self.file_path_modifier.modify_file_paths(self.media_to_array)
                self.media_to_array.extend(
                    self.subtitle_finder.get_media_subtitles(self.media_to_array, files_to_skip=set(self.files_to_skip))
                )

                # Save updated watched media set to cache file
                CacheManager.save_media_to_cache(watched_cache, self.media_to_array)

            else:
                logging.info("Loading watched media from cache...")
                # Add watched media from cache to the media array
                self.media_to_array.extend(watched_media_set)

        except Exception as e:
            logging.exception(f"An error occurred while processing the watched media: {type(e).__name__}: {e}")
    
    def _move_files(self) -> None:
        """Move files to their destinations."""
        # Move watched files to array
        if self.config_manager.cache.watched_move:
            self._safe_move_files(self.media_to_array, 'array')

        # Move files to cache
        logging.debug(f"Files being passed to cache move: {self.media_to_cache}")
        self._safe_move_files(self.media_to_cache, 'cache')

    def _safe_move_files(self, files: List[str], destination: str) -> None:
        """Safely move files with consistent error handling."""
        try:
            self._check_free_space_and_move_files(
                files, destination,
                self.config_manager.paths.real_source,
                self.config_manager.paths.cache_dir
            )
        except Exception as e:
            error_msg = f"Error moving media files to {destination}: {type(e).__name__}: {e}"
            if self.debug:
                logging.error(error_msg)
            else:
                logging.critical(error_msg)
                sys.exit(1)
    
    def _check_free_space_and_move_files(self, media_files: List[str], destination: str, 
                                        real_source: str, cache_dir: str) -> None:
        """Check free space and move files."""
        media_files_filtered = self.file_filter.filter_files(
            media_files, destination, self.media_to_cache, set(self.files_to_skip)
        )
        
        total_size, total_size_unit = self.file_utils.get_total_size_of_files(media_files_filtered)
        
        if total_size > 0:
            print(f"Moving {total_size:.2f} {total_size_unit} to {destination}")
            self.logging_manager.add_summary_message(
                f"Total size of media files moved to {destination}: {total_size:.2f} {total_size_unit}"
            )
            
            free_space, free_space_unit = self.file_utils.get_free_space(
                cache_dir if destination == 'cache' else real_source
            )
            
            # Check if enough space
            # Multipliers convert to KB as base unit (KB=1, MB=1024, GB=1024^2, TB=1024^3)
            size_multipliers = {'KB': 1, 'MB': 1024, 'GB': 1024**2, 'TB': 1024**3}
            total_size_kb = total_size * size_multipliers.get(total_size_unit, 1)
            free_space_kb = free_space * size_multipliers.get(free_space_unit, 1)
            
            if total_size_kb > free_space_kb:
                if not self.debug:
                    sys.exit(f"Not enough space on {destination} drive.")
                else:
                    logging.error(f"Not enough space on {destination} drive.")
            
            self.file_mover.move_media_files(
                media_files_filtered, destination,
                self.config_manager.performance.max_concurrent_moves_array,
                self.config_manager.performance.max_concurrent_moves_cache
            )
        else:
            if not self.logging_manager.files_moved:
                self.logging_manager.summary_messages = ["There were no files to move to any destination."]
    
    def _check_files_to_move_back_to_array(self):
        """Check for files in cache that should be moved back to array because they're no longer needed."""
        try:
            # Get current OnDeck and watchlist items (already processed and path-modified)
            current_ondeck_items = self.ondeck_items
            current_watchlist_items = set()
            
            # Get watchlist items from the processed media
            if self.config_manager.cache.watchlist_toggle:
                watchlist_cache, _, _ = self.config_manager.get_cache_files()
                if watchlist_cache.exists():
                    watchlist_media_set, _ = CacheManager.load_media_from_cache(watchlist_cache)
                    current_watchlist_items = set(self.file_path_modifier.modify_file_paths(list(watchlist_media_set)))
            
            # Get files that should be moved back to array (tracked by exclude file)
            files_to_move_back, cache_paths_to_remove = self.file_filter.get_files_to_move_back_to_array(
                current_ondeck_items, current_watchlist_items
            )
            
            if files_to_move_back:
                logging.info(f"Found {len(files_to_move_back)} files to move back to array")
                self.media_to_array.extend(files_to_move_back)
                # Remove these files from the exclude list since they're no longer in cache
                self.file_filter.remove_files_from_exclude_list(cache_paths_to_remove)
            else:
                logging.info("No files need to be moved back to array")
        except Exception as e:
            logging.exception(f"Error checking files to move back to array: {type(e).__name__}: {e}")
    
    def _finish(self) -> None:
        """Finish the application and log summary."""
        end_time = time.time()
        execution_time_seconds = end_time - self.start_time
        execution_time = self._convert_time(execution_time_seconds)

        self.logging_manager.add_summary_message(f"The script took approximately {execution_time} to execute.")
        self.logging_manager.log_summary()

        logging.info(f"Execution time of the script: {execution_time}")
        logging.info("Thank you for using PlexCache-R: https://github.com/StudioNirin/PlexCache-R")
        logging.info("Special thanks to: - Bexem - BBergle - and everyone who contributed!")
        logging.info("*** The End ***")
        
        # Clean up empty folders in cache
        self.cache_cleanup.cleanup_empty_folders()
        
        self.logging_manager.shutdown()

    def _convert_time(self, execution_time_seconds: float) -> str:
        """Convert execution time to human-readable format."""
        days, remainder = divmod(execution_time_seconds, 86400)
        hours, remainder = divmod(remainder, 3600)
        minutes, seconds = divmod(remainder, 60)

        result_str = ""
        if days > 0:
            result_str += f"{int(days)} day{'s' if days > 1 else ''}, "
        if hours > 0:
            result_str += f"{int(hours)} hour{'s' if hours > 1 else ''}, "
        if minutes > 0:
            result_str += f"{int(minutes)} minute{'s' if minutes > 1 else ''}, "
        if seconds > 0:
            result_str += f"{int(seconds)} second{'s' if seconds > 1 else ''}"

        return result_str.rstrip(", ") or "less than 1 second"


def main():
    """Main entry point."""
    skip_cache = "--skip-cache" in sys.argv
    debug = "--debug" in sys.argv
    restore_plexcached = "--restore-plexcached" in sys.argv

    # Derive config path from the script's actual location (matches plexcache_setup.py behavior)
    script_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    config_file = str(script_dir / "plexcache_settings.json")

    # Handle emergency restore mode
    if restore_plexcached:
        _run_plexcached_restore(config_file, debug)
        return

    app = PlexCacheApp(config_file, skip_cache, debug)
    app.run()


def _run_plexcached_restore(config_file: str, debug: bool) -> None:
    """Run the emergency .plexcached restore process."""
    import logging
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    logging.info("*** PlexCache Emergency Restore Mode ***")
    logging.info("This will restore all .plexcached files back to their original names.")

    # Load config to get the real_source path
    config_manager = ConfigManager(config_file)
    config_manager.load_config()

    # Search in the real_source directory (where array files live)
    search_paths = [config_manager.paths.real_source]
    logging.info(f"Searching for .plexcached files in: {search_paths}")

    restorer = PlexcachedRestorer(search_paths)

    # First do a dry run to show what would be restored
    print("\n=== Dry Run (showing what would be restored) ===")
    plexcached_files = restorer.find_plexcached_files()

    if not plexcached_files:
        print("No .plexcached files found. Nothing to restore.")
        return

    print(f"Found {len(plexcached_files)} .plexcached file(s):\n")
    for f in plexcached_files:
        original = f[:-len(".plexcached")]
        print(f"  {f}")
        print(f"    -> {original}")

    if debug:
        print("\nDebug mode: No files will be restored.")
        return

    # Prompt for confirmation
    print("\nWARNING: This will rename all .plexcached files back to their originals.")
    print("This should only be used in emergencies when you need to restore array files.")
    response = input("Type 'RESTORE' to proceed: ")

    if response.strip() == "RESTORE":
        logging.info("=== Performing restore ===")
        success, errors = restorer.restore_all(dry_run=False)
        logging.info(f"Restore complete: {success} files restored, {errors} errors")
    else:
        logging.info("Restore cancelled.")


if __name__ == "__main__":
    main() 
