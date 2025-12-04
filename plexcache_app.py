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
from file_operations import FilePathModifier, SubtitleFinder, FileFilter, FileMover, CacheCleanup, PlexcachedRestorer, CacheTimestampTracker, WatchlistTracker, PlexcachedMigration


class PlexCacheApp:
    """Main PlexCache application class."""

    def __init__(self, config_file: str, skip_cache: bool = False, dry_run: bool = False,
                 quiet: bool = False, verbose: bool = False):
        self.config_file = config_file
        self.skip_cache = skip_cache
        self.dry_run = dry_run  # Don't move files, just simulate
        self.debug = dry_run  # Alias for backwards compatibility in code
        self.quiet = quiet  # Override notification level to errors-only
        self.verbose = verbose  # Enable DEBUG level logging
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
        self.watchlist_items = set()
        self.source_map = {}  # Maps file paths to source ('ondeck' or 'watchlist')
        
    def run(self) -> None:
        """Run the main application."""
        try:
            # Setup logging first before any log messages
            self._setup_logging()
            if self.dry_run:
                logging.warning("DRY-RUN MODE - No files will be moved")
            if self.verbose:
                logging.debug("VERBOSE MODE - Showing DEBUG level logs")

            # Load configuration
            logging.debug("Loading configuration...")
            self.config_manager.load_config()

            # Set up notification handlers now that config is loaded
            self._setup_notification_handlers()

            # Initialize components that depend on config
            logging.debug("Initializing components...")
            self._initialize_components()

            # Check paths
            logging.debug("Validating paths...")
            self._check_paths()

            # Connect to Plex
            self._connect_to_plex()

            # Set debug mode (before processing)
            self._set_debug_mode()

            # Check for active sessions
            self._check_active_sessions()

            # Process media
            self._process_media()

            # Move files
            self._move_files()

            # Log summary and cleanup
            self._finish()
            
        except Exception as e:
            if self.logging_manager:
                logging.critical(f"Application error: {type(e).__name__}: {e}", exc_info=True)
            else:
                print(f"Application error: {type(e).__name__}: {e}")
            raise
    
    def _setup_logging(self) -> None:
        """Set up logging system (basic logging only, notifications set up after config load)."""
        self.logging_manager = LoggingManager(
            logs_folder=self.config_manager.paths.logs_folder,
            log_level="",  # Will be set from config
            max_log_files=5
        )
        self.logging_manager.setup_logging()
        logging.info("")
        logging.info("=== PlexCache-R ===")

    def _setup_notification_handlers(self) -> None:
        """Set up notification handlers after config is loaded."""
        # Override notification level if --quiet flag is used
        notification_config = self.config_manager.notification
        if self.quiet:
            notification_config.unraid_level = "error"
            notification_config.webhook_level = "error"

        self.logging_manager.setup_notification_handlers(
            notification_config,
            self.system_detector.is_unraid,
            self.system_detector.is_docker
        )
    
    def _initialize_components(self) -> None:
        """Initialize components that depend on configuration."""
        logging.debug("Initializing application components...")
        
        # Initialize Plex manager with token cache
        logging.debug("Initializing Plex manager...")
        token_cache_file = os.path.join(
            self.config_manager.paths.script_folder,
            "plexcache_user_tokens.json"
        )
        self.plex_manager = PlexManager(
            plex_url=self.config_manager.plex.plex_url,
            plex_token=self.config_manager.plex.plex_token,
            retry_limit=self.config_manager.performance.retry_limit,
            delay=self.config_manager.performance.delay,
            token_cache_file=token_cache_file
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
        timestamp_file = self.config_manager.get_timestamp_file()
        logging.debug(f"Cache files: watchlist={watchlist_cache}, watched={watched_cache}, exclude={mover_exclude}")
        logging.debug(f"Timestamp file: {timestamp_file}")

        # Create exclude file on startup if it doesn't exist
        # This allows users to configure Mover settings before any files are moved
        if not mover_exclude.exists():
            mover_exclude.touch()
            logging.info(f"Created mover exclude file: {mover_exclude}")

        # Run one-time migration to create .plexcached backups for existing cached files
        migration = PlexcachedMigration(
            exclude_file=str(mover_exclude),
            cache_dir=self.config_manager.paths.cache_dir,
            real_source=self.config_manager.paths.real_source,
            script_folder=self.config_manager.paths.script_folder,
            is_unraid=self.system_detector.is_unraid
        )
        if migration.needs_migration():
            logging.info("Running one-time migration for .plexcached backups...")
            max_concurrent = self.config_manager.performance.max_concurrent_moves_array
            migration.run_migration(dry_run=self.debug, max_concurrent=max_concurrent)

        # Initialize the cache timestamp tracker for retention period tracking
        self.timestamp_tracker = CacheTimestampTracker(str(timestamp_file))

        # Initialize the watchlist tracker for watchlist retention
        watchlist_tracker_file = self.config_manager.get_watchlist_tracker_file()
        self.watchlist_tracker = WatchlistTracker(str(watchlist_tracker_file))

        self.file_filter = FileFilter(
            real_source=self.config_manager.paths.real_source,
            cache_dir=self.config_manager.paths.cache_dir,
            is_unraid=self.system_detector.is_unraid,
            mover_cache_exclude_file=str(mover_exclude),
            timestamp_tracker=self.timestamp_tracker,
            cache_retention_hours=self.config_manager.cache.cache_retention_hours
        )

        self.file_mover = FileMover(
            real_source=self.config_manager.paths.real_source,
            cache_dir=self.config_manager.paths.cache_dir,
            is_unraid=self.system_detector.is_unraid,
            file_utils=self.file_utils,
            debug=self.debug,
            mover_cache_exclude_file=str(mover_exclude),
            timestamp_tracker=self.timestamp_tracker
        )
        
        self.cache_cleanup = CacheCleanup(
            self.config_manager.paths.cache_dir,
            self.config_manager.paths.nas_library_folders
        )
        logging.debug("All components initialized successfully")
    
    def _check_paths(self) -> None:
        """Check that required paths exist and are accessible."""
        for path in [self.config_manager.paths.real_source, self.config_manager.paths.cache_dir]:
            self.file_utils.check_path_exists(path)
    
    def _connect_to_plex(self) -> None:
        """Connect to the Plex server and load user tokens."""
        self.plex_manager.connect()

        # Load user tokens once at startup (reduces plex.tv API calls)
        if self.config_manager.plex.users_toggle:
            # Combine all skip lists for token loading
            skip_users = list(set(
                (self.config_manager.plex.skip_ondeck or []) +
                (self.config_manager.plex.skip_watchlist or [])
            ))
            # Pass users from settings file (includes remote users with tokens)
            self.plex_manager.load_user_tokens(
                skip_users=skip_users,
                settings_users=self.config_manager.plex.users
            )
    
    def _check_active_sessions(self) -> None:
        """Check for active Plex sessions."""
        sessions = self.plex_manager.get_active_sessions()
        if sessions:
            if self.config_manager.exit_if_active_session:
                logging.warning('There is an active session. Exiting...')
                sys.exit('There is an active session. Exiting...')
            else:
                self._process_active_sessions(sessions)
                if self.files_to_skip:
                    logging.info(f"Skipped {len(self.files_to_skip)} active session(s)")
    
    def _process_active_sessions(self, sessions: List) -> None:
        """Process active sessions and add files to skip list."""
        for session in sessions:
            try:
                media_path = self._get_media_path_from_session(session)
                if media_path:
                    # Convert Plex path to real path so it matches during filtering
                    converted_paths = self.file_path_modifier.modify_file_paths([media_path])
                    if converted_paths:
                        converted_path = converted_paths[0]
                        logging.debug(f"Skipping active session file: {converted_path}")
                        self.files_to_skip.append(converted_path)
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
                logging.debug(f"Active session detected, skipping: {show_title} - {media_title}")
            elif media_type == "movie":
                logging.debug(f"Active session detected, skipping: {media_title}")

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
        """Set logging level based on verbose flag."""
        if self.verbose:
            logging.getLogger().setLevel(logging.DEBUG)
        else:
            logging.getLogger().setLevel(logging.INFO)
    
    def _process_media(self) -> None:
        """Process all media types (onDeck, watchlist, watched)."""
        logging.info("")
        logging.info("--- Fetching Media ---")

        # Use a set to collect already-modified paths (real source paths)
        modified_paths_set = set()

        # Fetch OnDeck Media
        logging.debug("Fetching OnDeck media...")
        ondeck_media = self.plex_manager.get_on_deck_media(
            self.config_manager.plex.valid_sections or [],
            self.config_manager.plex.days_to_monitor,
            self.config_manager.plex.number_episodes,
            self.config_manager.plex.users_toggle,
            self.config_manager.plex.skip_ondeck or []
        )

        # Edit file paths for OnDeck media (convert plex paths to real paths)
        logging.debug("Modifying file paths for OnDeck media...")
        modified_ondeck = self.file_path_modifier.modify_file_paths(list(ondeck_media))

        # Store modified OnDeck items for filtering later
        self.ondeck_items = set(modified_ondeck)
        modified_paths_set.update(self.ondeck_items)

        # Track source for OnDeck items
        for item in self.ondeck_items:
            self.source_map[item] = "ondeck"

        # Fetch subtitles for OnDeck media (already using real paths)
        logging.debug("Finding subtitles for OnDeck media...")
        ondeck_with_subtitles = self.subtitle_finder.get_media_subtitles(list(self.ondeck_items), files_to_skip=set(self.files_to_skip))
        subtitle_count = len(ondeck_with_subtitles) - len(self.ondeck_items)
        modified_paths_set.update(ondeck_with_subtitles)
        logging.debug(f"Found {subtitle_count} subtitle files for OnDeck media")

        # Track source for OnDeck subtitles
        for item in ondeck_with_subtitles:
            if item not in self.source_map:
                self.source_map[item] = "ondeck"

        # Process watchlist (returns already-modified paths)
        watchlist_count = 0
        if self.config_manager.cache.watchlist_toggle:
            logging.debug("Processing watchlist media...")
            watchlist_items = self._process_watchlist()
            if watchlist_items:
                # Store watchlist items (don't override ondeck source for items in both)
                self.watchlist_items = watchlist_items
                modified_paths_set.update(watchlist_items)
                watchlist_count = len(watchlist_items)

                # Track source for watchlist items (only if not already tracked as ondeck)
                for item in watchlist_items:
                    if item not in self.source_map:
                        self.source_map[item] = "watchlist"

        # Process watched media
        watched_count = 0
        if self.config_manager.cache.watched_move:
            logging.debug("Processing watched media...")
            self._process_watched_media()
            watched_count = len(self.media_to_array)

        # Run modify_file_paths on all collected paths to ensure consistent path format
        logging.debug("Finalizing media to cache list...")
        self.media_to_cache = self.file_path_modifier.modify_file_paths(list(modified_paths_set))

        # Log consolidated summary
        logging.info(f"OnDeck: {len(ondeck_media)} items, Watchlist: {watchlist_count} items, Watched: {watched_count} items")

        # Check for files that should be moved back to array (no longer needed in cache)
        logging.debug("Checking for files to move back to array...")
        self._check_files_to_move_back_to_array()

    def _process_watchlist(self) -> set:
        """Process watchlist media (local API + remote RSS) and return a set of modified file paths and subtitles.

        Also updates the watchlist tracker with watchlistedAt timestamps for retention tracking.
        """
        result_set = set()
        retention_days = self.config_manager.cache.watchlist_retention_days
        expired_count = 0

        try:
            watchlist_cache, _, _ = self.config_manager.get_cache_files()
            watchlist_media_set, last_updated = CacheManager.load_media_from_cache(watchlist_cache)
            current_watchlist_set = set()

            logging.debug(f"Watchlist cache exists: {watchlist_cache.exists()}")
            logging.debug(f"Watchlist cache last updated: {last_updated}")
            logging.debug(f"Current watchlist items in cache: {len(watchlist_media_set)}")
            if retention_days > 0:
                logging.debug(f"Watchlist retention enabled: {retention_days} days")

            if self.system_detector.is_connected():
                # Determine if cache should be refreshed
                cache_expired = self._is_cache_expired(
                    watchlist_cache,
                    self.config_manager.cache.watchlist_cache_expiry
                )
                logging.debug(f"Cache expired: {cache_expired}")

                if cache_expired:
                    logging.debug(f"Cache expired: {watchlist_cache}")

                    # Delete old cache file if it exists
                    if watchlist_cache.exists():
                        try:
                            watchlist_cache.unlink()
                            logging.debug(f"Cache file deleted: {watchlist_cache}")
                        except Exception as e:
                            logging.error(f"Failed to delete cache file {watchlist_cache}: {e}")

                    # Reset memory sets to avoid old data
                    watchlist_media_set.clear()
                    current_watchlist_set.clear()
                    result_set.clear()

                    # --- Local Plex users ---
                    # API now returns (file_path, username, watchlisted_at) tuples
                    # Build list of home users from settings (only home users have accessible watchlists)
                    home_users = [
                        u.get("title") for u in self.config_manager.plex.users
                        if u.get("is_local", False)
                    ]
                    fetched_watchlist = list(self.plex_manager.get_watchlist_media(
                        self.config_manager.plex.valid_sections,
                        self.config_manager.cache.watchlist_episodes,
                        self.config_manager.plex.users_toggle,
                        self.config_manager.plex.skip_watchlist,
                        home_users=home_users
                    ))

                    for item in fetched_watchlist:
                        file_path, username, watchlisted_at = item

                        # Update watchlist tracker with timestamp
                        self.watchlist_tracker.update_entry(file_path, username, watchlisted_at)

                        # Check watchlist retention (skip expired items)
                        if retention_days > 0:
                            # Use original file_path for consistency with update_entry
                            if self.watchlist_tracker.is_expired(file_path, retention_days):
                                expired_count += 1
                                continue

                        current_watchlist_set.add(file_path)
                        if file_path not in watchlist_media_set:
                            result_set.add(file_path)

                    watchlist_media_set.intersection_update(current_watchlist_set)
                    watchlist_media_set.update(result_set)

                    # --- Remote users via RSS ---
                    if self.config_manager.cache.remote_watchlist_toggle and self.config_manager.cache.remote_watchlist_rss_url:
                        logging.debug("Fetching watchlist via RSS feed for remote users...")
                        try:
                            # Use get_watchlist_media with rss_url parameter; users_toggle=False because this is just RSS
                            # RSS items return (file_path, username, None) - no watchlistedAt available
                            remote_items = list(
                                self.plex_manager.get_watchlist_media(
                                    valid_sections=self.config_manager.plex.valid_sections,
                                    watchlist_episodes=self.config_manager.cache.watchlist_episodes,
                                    users_toggle=False,  # only RSS, no local Plex users
                                    skip_watchlist=[],
                                    rss_url=self.config_manager.cache.remote_watchlist_rss_url
                                )
                            )
                            logging.debug(f"Found {len(remote_items)} remote watchlist items from RSS")
                            rss_expired_count = 0
                            for item in remote_items:
                                file_path, username, watchlisted_at = item
                                # Update tracker (RSS items use pubDate from feed)
                                self.watchlist_tracker.update_entry(file_path, username, watchlisted_at)

                                # Check watchlist retention (skip expired items)
                                if retention_days > 0:
                                    if self.watchlist_tracker.is_expired(file_path, retention_days):
                                        rss_expired_count += 1
                                        continue

                                current_watchlist_set.add(file_path)
                                result_set.add(file_path)

                            if rss_expired_count > 0:
                                expired_count += rss_expired_count
                                logging.debug(f"Skipped {rss_expired_count} RSS watchlist items due to retention expiry")
                        except Exception as e:
                            logging.error(f"Failed to fetch remote watchlist via RSS: {str(e)}")

                    if expired_count > 0:
                        logging.debug(f"Skipped {expired_count} watchlist items due to retention expiry ({retention_days} days)")

                    # Modify file paths and fetch subtitles
                    modified_items = self.file_path_modifier.modify_file_paths(list(result_set))
                    result_set.update(modified_items)
                    subtitles = self.subtitle_finder.get_media_subtitles(modified_items, files_to_skip=set(self.files_to_skip))
                    result_set.update(subtitles)

                    # Update cache file
                    CacheManager.save_media_to_cache(watchlist_cache, list(result_set))

                else:
                    logging.debug("Loading watchlist media from cache...")
                    result_set.update(watchlist_media_set)
            else:
                logging.warning("Unable to connect to the internet, skipping fetching new watchlist media due to plexapi limitation.")
                logging.debug("Loading watchlist media from cache...")
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
                logging.debug("Fetching watched media...")

                # Get watched media from Plex server
                fetched_media = list(self.plex_manager.get_watched_media(
                    self.config_manager.plex.valid_sections,
                    last_updated,
                    self.config_manager.plex.users_toggle
                ))
                
                # Add fetched media to the current media set
                retention_hours = self.config_manager.cache.cache_retention_hours
                for file_path in fetched_media:
                    current_media_set.add(file_path)

                    # Check if file is not already in the watched media set
                    if file_path not in watched_media_set:
                        # Check retention period before adding to move queue
                        if retention_hours > 0 and self.timestamp_tracker:
                            # Convert Plex path to real path first, then to cache path
                            # file_path is raw Plex path like /data/Movies/...
                            # We need to convert to cache path like /mnt/cache_downloads/Movies/...
                            modified_paths = self.file_path_modifier.modify_file_paths([file_path])
                            if modified_paths:
                                real_path = modified_paths[0]  # /mnt/user/Movies/...
                                cache_path = real_path.replace(
                                    self.config_manager.paths.real_source,
                                    self.config_manager.paths.cache_dir, 1
                                )
                                logging.debug(f"Checking retention for watched file: {cache_path}")
                                if self.timestamp_tracker.is_within_retention_period(cache_path, retention_hours):
                                    logging.info(f"Watched file within retention period ({retention_hours}h), skipping move to array: {os.path.basename(file_path)}")
                                    continue
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
                logging.debug("Loading watched media from cache...")
                # Add watched media from cache to the media array
                self.media_to_array.extend(watched_media_set)

        except Exception as e:
            logging.exception(f"An error occurred while processing the watched media: {type(e).__name__}: {e}")
    
    def _move_files(self) -> None:
        """Move files to their destinations."""
        logging.info("")
        logging.info("--- Moving Files ---")

        # Move watched files to array
        if self.config_manager.cache.watched_move:
            self._safe_move_files(self.media_to_array, 'array')

        # Move files to cache
        logging.debug(f"Files being passed to cache move: {self.media_to_cache}")
        self._safe_move_files(self.media_to_cache, 'cache')

    def _safe_move_files(self, files: List[str], destination: str) -> None:
        """Safely move files with consistent error handling."""
        try:
            # Pass source map only when moving to cache
            source_map = self.source_map if destination == 'cache' else None
            self._check_free_space_and_move_files(
                files, destination,
                self.config_manager.paths.real_source,
                self.config_manager.paths.cache_dir,
                source_map
            )
        except Exception as e:
            error_msg = f"Error moving media files to {destination}: {type(e).__name__}: {e}"
            if self.debug:
                logging.error(error_msg)
            else:
                logging.critical(error_msg)
                sys.exit(1)
    
    def _apply_cache_limit(self, media_files: List[str], cache_dir: str) -> List[str]:
        """Apply cache size limit, filtering out files that would exceed the limit.

        Returns the list of files that fit within the cache limit.
        Files are prioritized in the order they appear (OnDeck items should come first).
        """
        cache_limit_bytes = self.config_manager.cache.cache_limit_bytes

        # No limit set
        if cache_limit_bytes == 0:
            return media_files

        # Calculate effective limit (handle percentage)
        if cache_limit_bytes < 0:
            # Negative value indicates percentage
            percent = abs(cache_limit_bytes)
            try:
                total_drive_size = self.file_utils.get_total_drive_size(cache_dir)
                cache_limit_bytes = int(total_drive_size * percent / 100)
                limit_readable = f"{percent}% of {total_drive_size / (1024**3):.1f}GB = {cache_limit_bytes / (1024**3):.1f}GB"
            except Exception as e:
                logging.warning(f"Could not calculate cache drive size for percentage limit: {e}")
                return media_files
        else:
            limit_readable = f"{cache_limit_bytes / (1024**3):.1f}GB"

        # Calculate current PlexCache usage from exclude file
        current_usage = 0
        _, _, exclude_file = self.config_manager.get_cache_files()
        if exclude_file.exists():
            try:
                with open(exclude_file, 'r') as f:
                    cached_files = [line.strip() for line in f if line.strip()]
                for cached_file in cached_files:
                    try:
                        if os.path.exists(cached_file):
                            current_usage += os.path.getsize(cached_file)
                    except (OSError, FileNotFoundError):
                        pass
            except Exception as e:
                logging.warning(f"Error reading exclude file for cache limit calculation: {e}")

        current_usage_gb = current_usage / (1024**3)
        logging.info(f"Cache limit: {limit_readable}, current usage: {current_usage_gb:.2f}GB")

        # Filter files that fit within limit
        available_space = cache_limit_bytes - current_usage
        files_to_cache = []
        skipped_count = 0
        skipped_size = 0

        for file in media_files:
            try:
                file_size = os.path.getsize(file)
                if file_size <= available_space:
                    files_to_cache.append(file)
                    available_space -= file_size
                else:
                    skipped_count += 1
                    skipped_size += file_size
            except (OSError, FileNotFoundError):
                # File doesn't exist or can't be accessed, skip it
                pass

        if skipped_count > 0:
            skipped_gb = skipped_size / (1024**3)
            logging.warning(f"Cache limit reached: skipped {skipped_count} files ({skipped_gb:.2f}GB) that would exceed the {limit_readable} limit")

        return files_to_cache

    def _check_free_space_and_move_files(self, media_files: List[str], destination: str,
                                        real_source: str, cache_dir: str,
                                        source_map: dict = None) -> None:
        """Check free space and move files."""
        media_files_filtered = self.file_filter.filter_files(
            media_files, destination, self.media_to_cache, set(self.files_to_skip)
        )

        # Apply cache size limit when moving to cache
        if destination == 'cache':
            media_files_filtered = self._apply_cache_limit(media_files_filtered, cache_dir)

        total_size, total_size_unit = self.file_utils.get_total_size_of_files(media_files_filtered)
        
        if total_size > 0:
            logging.info(f"Moving {total_size:.2f} {total_size_unit} to {destination}")
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
                self.config_manager.performance.max_concurrent_moves_cache,
                source_map
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

                    # Filter out expired watchlist items - they should be moved back to array
                    # Check expiry using original paths (as stored in tracker), then convert to modified paths
                    retention_days = self.config_manager.cache.watchlist_retention_days
                    if retention_days > 0 and self.watchlist_tracker:
                        non_expired_original = set()
                        expired_count = 0
                        for original_path in watchlist_media_set:
                            if not self.watchlist_tracker.is_expired(original_path, retention_days):
                                non_expired_original.add(original_path)
                            else:
                                logging.debug(f"Watchlist item expired, eligible for move back: {os.path.basename(original_path)}")
                                expired_count += 1
                        if expired_count > 0:
                            logging.info(f"Excluding {expired_count} expired watchlist items from 'needed' check")
                        # Convert non-expired items to modified paths
                        current_watchlist_items = set(self.file_path_modifier.modify_file_paths(list(non_expired_original)))
                    else:
                        current_watchlist_items = set(self.file_path_modifier.modify_file_paths(list(watchlist_media_set)))
            
            # Get files that should be moved back to array (tracked by exclude file)
            files_to_move_back, cache_paths_to_remove = self.file_filter.get_files_to_move_back_to_array(
                current_ondeck_items, current_watchlist_items
            )

            if files_to_move_back:
                logging.debug(f"Found {len(files_to_move_back)} files to move back to array")
                self.media_to_array.extend(files_to_move_back)

            # Always clean up stale entries from exclude list (files that no longer exist on cache)
            if cache_paths_to_remove:
                self.file_filter.remove_files_from_exclude_list(cache_paths_to_remove)
        except Exception as e:
            logging.exception(f"Error checking files to move back to array: {type(e).__name__}: {e}")
    
    def _finish(self) -> None:
        """Finish the application and log summary."""
        end_time = time.time()
        execution_time_seconds = end_time - self.start_time
        execution_time = self._convert_time(execution_time_seconds)

        self.logging_manager.log_summary()

        # Clean up empty folders in cache
        self.cache_cleanup.cleanup_empty_folders()

        # Clean up stale timestamp entries for files that no longer exist
        if hasattr(self, 'timestamp_tracker') and self.timestamp_tracker:
            self.timestamp_tracker.cleanup_missing_files()

        # Clean up stale watchlist tracker entries
        if hasattr(self, 'watchlist_tracker') and self.watchlist_tracker:
            self.watchlist_tracker.cleanup_stale_entries()
            self.watchlist_tracker.cleanup_missing_files()

        logging.info("")
        logging.info(f"Completed in {execution_time}")
        logging.info("===================")

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
    dry_run = "--dry-run" in sys.argv or "--debug" in sys.argv  # --debug is alias for backwards compatibility
    restore_plexcached = "--restore-plexcached" in sys.argv
    quiet = "--quiet" in sys.argv or "--notify-errors-only" in sys.argv
    verbose = "--verbose" in sys.argv or "-v" in sys.argv or "--v" in sys.argv

    # Derive config path from the script's actual location (matches plexcache_setup.py behavior)
    script_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    config_file = str(script_dir / "plexcache_settings.json")

    # Handle emergency restore mode
    if restore_plexcached:
        _run_plexcached_restore(config_file, dry_run, verbose)
        return

    app = PlexCacheApp(config_file, skip_cache, dry_run, quiet, verbose)
    app.run()


def _run_plexcached_restore(config_file: str, dry_run: bool, verbose: bool = False) -> None:
    """Run the emergency .plexcached restore process."""
    import logging
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
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

    if dry_run:
        print("\nDry-run mode: No files will be restored.")
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
