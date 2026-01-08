"""
Main PlexCache application.
Orchestrates all components and provides the main business logic.
"""

import sys
import time
import logging
import re
import shutil
import subprocess
from pathlib import Path
from typing import List, Set, Optional, Tuple
import os

from core.config import ConfigManager
from core.logging_config import LoggingManager
from core.system_utils import SystemDetector, FileUtils, SingleInstanceLock
from core.plex_api import PlexManager, OnDeckItem
from core.file_operations import MultiPathModifier, SubtitleFinder, FileFilter, FileMover, PlexcachedRestorer, CacheTimestampTracker, WatchlistTracker, OnDeckTracker, CachePriorityManager, PlexcachedMigration


class PlexCacheApp:
    """Main PlexCache application class."""

    def __init__(self, config_file: str, dry_run: bool = False,
                 quiet: bool = False, verbose: bool = False):
        self.config_file = config_file
        self.dry_run = dry_run  # Don't move files, just simulate
        self.quiet = quiet  # Override notification level to errors-only
        self.verbose = verbose  # Enable DEBUG level logging
        self.start_time = time.time()
        
        # Initialize components
        self.config_manager = ConfigManager(config_file)
        self.system_detector = SystemDetector()
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
        # Tracking for restore vs move operations (for summary)
        self.restored_count = 0
        self.restored_bytes = 0
        self.moved_to_array_count = 0
        self.moved_to_array_bytes = 0
        self.cached_bytes = 0
        
    def run(self) -> None:
        """Run the main application."""
        try:
            # Setup logging first before any log messages
            self._setup_logging()
            if self.dry_run:
                logging.warning("DRY-RUN MODE - No files will be moved")
            if self.verbose:
                logging.info("VERBOSE MODE - Showing DEBUG level logs")

            # Prevent multiple instances from running simultaneously
            # Compute project root: if we're in core/, go up one level
            script_dir = Path(os.path.dirname(os.path.abspath(__file__)))
            project_root = script_dir.parent if script_dir.name == 'core' else script_dir
            lock_file = str(project_root / "plexcache.lock")
            self.instance_lock = SingleInstanceLock(lock_file)
            if not self.instance_lock.acquire():
                logging.critical("Another instance of PlexCache is already running. Exiting.")
                print("ERROR: Another instance of PlexCache is already running. Exiting.")
                return

            # Check if Unraid mover is running (prevents race condition)
            if self._is_mover_running():
                logging.warning("Unraid mover is currently running. Exiting to prevent race condition.")
                logging.warning("PlexCache will run on the next scheduled execution after mover completes.")
                print("WARNING: Unraid mover is running. Exiting to avoid conflicts.")
                return

            # Load configuration
            logging.debug("Loading configuration...")
            self.config_manager.load_config()

            # Set up notification handlers now that config is loaded
            self._setup_notification_handlers()

            # Set debug mode early so all debug messages show
            self._set_debug_mode()

            # Log startup diagnostics after log level is configured
            if self.verbose:
                self._log_startup_diagnostics()

            # Initialize components that depend on config
            logging.debug("Initializing components...")
            self._initialize_components()

            # Clean up stale exclude list entries (self-healing)
            if not self.dry_run:
                self.file_filter.clean_stale_exclude_entries()
            else:
                logging.debug("[DRY RUN] Would clean stale exclude list entries")

            # Check paths
            logging.debug("Validating paths...")
            self._check_paths()

            # Connect to Plex
            self._connect_to_plex()

            # Check for active sessions
            self._check_active_sessions()

            # Process media
            self._process_media()

            # Move files
            self._move_files()


            # Update Unraid mover exclusion file
            logging.debug("Updating Unraid mover exclusions...")
            try:
                self._update_unraid_mover_exclusions()
                logging.debug("Unraid mover exclusions updated.")
            except Exception as e:
                logging.error(f"Failed to update Unraid mover exclusions: {e}")


            # Log summary and cleanup
            self._finish()
            
        except Exception as e:
            if self.logging_manager:
                logging.critical(f"Application error: {type(e).__name__}: {e}", exc_info=True)
            else:
                print(f"Application error: {type(e).__name__}: {e}")
            raise
    

    def _update_unraid_mover_exclusions(self, tag_line: str = "### Plexcache exclusions below this line") -> None:
        """
        Update the Unraid mover exclusions file by inserting or updating the
        PlexCache exclusions section. Paths are retrieved from the config.
        """

        # Get paths from config
        exclusion_path = self.config_manager.get_unraid_mover_exclusions_file()
        plexcache_path = self.config_manager.get_mover_exclude_file()

        # Ensure the main file exists
        if not exclusion_path.exists():
            exclusion_path.parent.mkdir(parents=True, exist_ok=True)
            exclusion_path.touch()

        # Read current exclusion file
        with open(exclusion_path, "r", encoding="utf-8") as f:
            lines = f.read().splitlines()

        # Ensure the tag line exists
        if tag_line not in lines:
            if lines and lines[-1].strip() != "":
                lines.append("")  # pad newline if last line isn't empty
            lines.append(tag_line)

        # Keep only content above the tag (inclusive)
        tag_index = lines.index(tag_line)
        lines = lines[:tag_index + 1]

        # Load new exclusion entries from plexcache file
        if plexcache_path.exists():
            with open(plexcache_path, "r", encoding="utf-8") as f:
                new_entries = [ln.strip() for ln in f if ln.strip()]
        else:
            new_entries = []

        # Append the new entries
        lines.extend(new_entries)

        # Write updated file back
        with open(exclusion_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")




    def _setup_logging(self) -> None:
        """Set up logging system (basic logging only, notifications set up after config load).

        Note: Logging settings (max_log_files, keep_error_logs_days) are loaded
        from config after initial setup. The LoggingManager uses sensible defaults
        until config is loaded.
        """
        self.logging_manager = LoggingManager(
            logs_folder=self.config_manager.paths.logs_folder,
            log_level="",  # Will be set from config
            max_log_files=24,  # Default: 24 for hourly runs
            keep_error_logs_days=7  # Default: preserve error logs for 7 days
        )
        self.logging_manager.setup_logging()
        logging.info("")
        logging.info("=== PlexCache-R ===")

    def _setup_notification_handlers(self) -> None:
        """Set up notification handlers after config is loaded."""
        # Update logging settings from config (max_log_files, keep_error_logs_days)
        self.logging_manager.update_settings(
            max_log_files=self.config_manager.logging.max_log_files,
            keep_error_logs_days=self.config_manager.logging.keep_error_logs_days
        )

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

    def _log_startup_diagnostics(self) -> None:
        """Log system diagnostics at startup in verbose mode for debugging."""
        import platform

        logging.debug("=== Startup Diagnostics ===")
        logging.debug(f"Platform: {platform.system()} {platform.release()}")
        logging.debug(f"Python: {platform.python_version()}")

        if self.system_detector.is_linux:
            try:
                import pwd
                uid = os.getuid()
                gid = os.getgid()
                username = pwd.getpwuid(uid).pw_name
                logging.debug(f"Running as: {username} (uid={uid}, gid={gid})")
            except Exception as e:
                logging.debug(f"Could not get user info: {e}")
        else:
            logging.debug(f"Running as: {os.getlogin() if hasattr(os, 'getlogin') else 'unknown'}")

        logging.debug(f"Unraid detected: {self.system_detector.is_unraid}")
        logging.debug(f"Docker detected: {self.system_detector.is_docker}")
        logging.debug("===========================")

    def _log_results_summary(self) -> None:
        """Log results summary at end of run.

        Shows key metrics at INFO level for all runs, with additional
        detail at DEBUG level for verbose mode.
        """
        logging.info("")
        logging.info("--- Results ---")

        # Get accurate counts from file_filter and file_mover
        already_cached = getattr(self.file_filter, 'last_already_cached_count', 0) if self.file_filter else 0
        actually_moved = getattr(self.file_mover, 'last_cache_moves_count', 0) if self.file_mover else 0
        moved_to_array = len(self.media_to_array)

        logging.info(f"Already cached: {already_cached} files")
        logging.info(f"Moved to cache: {actually_moved} files")
        logging.info(f"Moved to array: {moved_to_array} files")

        # Additional detail at DEBUG level
        # Note: Empty folder cleanup now happens immediately during file operations
        # (per File and Folder Management Policy) and is logged at DEBUG level as it occurs

    def _is_mover_running(self) -> bool:
        """Check if the Unraid mover is currently running.

        This prevents race conditions where PlexCache caches files while
        the mover is actively moving files, which can result in files
        being moved back to the array before they're added to the exclude list.

        Returns:
            True if mover is running, False otherwise.
        """
        if not self.system_detector.is_unraid:
            return False

        try:
            # Check for mover process using pgrep
            result = subprocess.run(
                ['pgrep', '-f', '/usr/local/sbin/mover'],
                capture_output=True,
                text=True
            )
            if result.returncode == 0 and result.stdout.strip():
                return True

            # Also check for the age_mover script (CA Mover Tuning plugin)
            result = subprocess.run(
                ['pgrep', '-f', 'age_mover'],
                capture_output=True,
                text=True
            )
            return result.returncode == 0 and result.stdout.strip() != ''

        except (subprocess.SubprocessError, FileNotFoundError):
            # If we can't check, assume mover is not running
            return False

    def _init_plex_manager(self) -> None:
        """Initialize the Plex manager with token cache."""
        logging.debug("Initializing Plex manager...")
        token_cache_file = str(self.config_manager.get_user_tokens_file())
        rss_cache_file = str(self.config_manager.get_rss_cache_file())
        self.plex_manager = PlexManager(
            plex_url=self.config_manager.plex.plex_url,
            plex_token=self.config_manager.plex.plex_token,
            retry_limit=self.config_manager.performance.retry_limit,
            delay=self.config_manager.performance.delay,
            token_cache_file=token_cache_file,
            rss_cache_file=rss_cache_file
        )

    def _init_path_modifier(self) -> None:
        """Initialize path modifier and subtitle finder."""
        logging.debug("Initializing file operation components...")

        all_mappings = self.config_manager.paths.path_mappings or []
        enabled_mappings = [m for m in all_mappings if m.enabled]
        logging.info(f"Using multi-path mode with {len(all_mappings)} mappings ({len(enabled_mappings)} enabled)")
        self.file_path_modifier = MultiPathModifier(mappings=all_mappings)

        if self.config_manager.has_legacy_path_arrays():
            legacy_info = self.config_manager.get_legacy_array_info()
            logging.info(f"Legacy path arrays detected: {legacy_info}")
            logging.info("These are deprecated and can be removed from your settings file.")
            logging.info("Path conversion now uses path_mappings exclusively.")

        self.subtitle_finder = SubtitleFinder()

    def _init_trackers(self, mover_exclude, timestamp_file) -> None:
        """Initialize timestamp, watchlist, and OnDeck trackers."""
        # Run one-time migration to create .plexcached backups
        migration = PlexcachedMigration(
            exclude_file=str(mover_exclude),
            cache_dir=self.config_manager.paths.cache_dir,
            real_source=self.config_manager.paths.real_source,
            script_folder=self.config_manager.paths.script_folder,
            is_unraid=self.system_detector.is_unraid,
            path_modifier=self.file_path_modifier
        )
        if migration.needs_migration():
            logging.info("Running one-time migration for .plexcached backups...")
            max_concurrent = self.config_manager.performance.max_concurrent_moves_array
            migration.run_migration(dry_run=self.dry_run, max_concurrent=max_concurrent)

        self.timestamp_tracker = CacheTimestampTracker(str(timestamp_file))

        watchlist_tracker_file = self.config_manager.get_watchlist_tracker_file()
        self.watchlist_tracker = WatchlistTracker(str(watchlist_tracker_file))

        ondeck_tracker_file = str(self.config_manager.get_ondeck_tracker_file())
        self.ondeck_tracker = OnDeckTracker(ondeck_tracker_file)

    def _init_file_operations(self, mover_exclude) -> None:
        """Initialize file filter and file mover."""
        self.file_filter = FileFilter(
            real_source=self.config_manager.paths.real_source,
            cache_dir=self.config_manager.paths.cache_dir,
            is_unraid=self.system_detector.is_unraid,
            mover_cache_exclude_file=str(mover_exclude),
            timestamp_tracker=self.timestamp_tracker,
            cache_retention_hours=self.config_manager.cache.cache_retention_hours,
            ondeck_tracker=self.ondeck_tracker,
            watchlist_tracker=self.watchlist_tracker,
            path_modifier=self.file_path_modifier
        )

        self.file_mover = FileMover(
            real_source=self.config_manager.paths.real_source,
            cache_dir=self.config_manager.paths.cache_dir,
            is_unraid=self.system_detector.is_unraid,
            file_utils=self.file_utils,
            debug=self.dry_run,
            mover_cache_exclude_file=str(mover_exclude),
            timestamp_tracker=self.timestamp_tracker,
            path_modifier=self.file_path_modifier
        )

    def _init_cache_management(self) -> None:
        """Initialize cache priority manager."""
        # Note: Empty folder cleanup is now handled immediately during file operations
        # (per File and Folder Management Policy) - see FileMover._cleanup_empty_parent_folders()

        self.priority_manager = CachePriorityManager(
            timestamp_tracker=self.timestamp_tracker,
            watchlist_tracker=self.watchlist_tracker,
            ondeck_tracker=self.ondeck_tracker,
            eviction_min_priority=self.config_manager.cache.eviction_min_priority,
            number_episodes=self.config_manager.plex.number_episodes
        )

    def _initialize_components(self) -> None:
        """Initialize components that depend on configuration."""
        logging.debug("Initializing application components...")

        # Initialize Plex manager
        self._init_plex_manager()

        # Initialize path modifier and subtitle finder
        self._init_path_modifier()

        # Get file paths for trackers
        mover_exclude = self.config_manager.get_mover_exclude_file()
        timestamp_file = self.config_manager.get_timestamp_file()
        logging.debug(f"Mover exclude file: {mover_exclude}")
        logging.debug(f"Timestamp file: {timestamp_file}")

        # Create exclude file on startup if it doesn't exist
        if not mover_exclude.exists():
            mover_exclude.touch()
            logging.info(f"Created mover exclude file: {mover_exclude}")

        # Initialize trackers
        self._init_trackers(mover_exclude, timestamp_file)

        # Initialize file filter and mover
        self._init_file_operations(mover_exclude)

        # Initialize cache cleanup and priority manager
        self._init_cache_management()

        logging.debug("All components initialized successfully")
    
    def _ensure_cache_path_exists(self, cache_path: str) -> None:
        """Ensure a cache directory exists, creating it if necessary."""
        if not os.path.exists(cache_path):
            try:
                os.makedirs(cache_path, exist_ok=True)
                logging.info(f"Created missing cache directory: {cache_path}")
            except OSError as e:
                raise FileNotFoundError(f"Cannot create cache directory {cache_path}: {e}")

    def _check_paths(self) -> None:
        """Check that required paths exist and are accessible."""
        if self.config_manager.paths.path_mappings:
            # Multi-path mode: check paths from enabled mappings
            for mapping in self.config_manager.paths.path_mappings:
                if mapping.enabled:
                    if mapping.real_path:
                        self.file_utils.check_path_exists(mapping.real_path)
                    if mapping.cacheable and mapping.cache_path:
                        # Create cache directory if it doesn't exist
                        self._ensure_cache_path_exists(mapping.cache_path)
        else:
            # Legacy single-path mode
            if self.config_manager.paths.real_source:
                self.file_utils.check_path_exists(self.config_manager.paths.real_source)
            if self.config_manager.paths.cache_dir:
                # Create cache directory if it doesn't exist
                self._ensure_cache_path_exists(self.config_manager.paths.cache_dir)
    
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
            # Use "main" as fallback username if plex.tv unreachable
            self.plex_manager.load_user_tokens(
                skip_users=skip_users,
                settings_users=self.config_manager.plex.users,
                main_username="main"  # Fallback if plex.tv unreachable
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

        # Clear OnDeck tracker at start of each run (OnDeck status is ephemeral)
        self.ondeck_tracker.clear_for_run()

        # Fetch OnDeck Media - returns List[OnDeckItem] with file path, username, and episode metadata
        logging.debug("Fetching OnDeck media...")
        ondeck_items_list = self.plex_manager.get_on_deck_media(
            self.config_manager.plex.valid_sections or [],
            self.config_manager.plex.days_to_monitor,
            self.config_manager.plex.number_episodes,
            self.config_manager.plex.users_toggle,
            self.config_manager.plex.skip_ondeck or []
        )

        # Extract just the file paths for path modification
        ondeck_files = [item.file_path for item in ondeck_items_list]

        # Log OnDeck summary (count users with items)
        ondeck_users = set(item.username for item in ondeck_items_list)
        if ondeck_items_list:
            logging.info(f"OnDeck: {len(ondeck_items_list)} items from {len(ondeck_users)} users")
        else:
            logging.info("OnDeck: 0 items")

        # Edit file paths for OnDeck media (convert plex paths to real paths)
        logging.debug("Modifying file paths for OnDeck media...")
        modified_ondeck = self.file_path_modifier.modify_file_paths(ondeck_files)

        # Build a mapping from original plex path to modified real path
        plex_to_real = dict(zip(ondeck_files, modified_ondeck))

        # Populate OnDeck tracker with user info and episode metadata using modified paths
        for item in ondeck_items_list:
            real_path = plex_to_real.get(item.file_path, item.file_path)
            self.ondeck_tracker.update_entry(
                real_path,
                item.username,
                episode_info=item.episode_info,
                is_current_ondeck=item.is_current_ondeck
            )

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
        if self.config_manager.cache.watchlist_toggle:
            logging.debug("Processing watchlist media...")
            watchlist_items = self._process_watchlist()
            if watchlist_items:
                # Store watchlist items (don't override ondeck source for items in both)
                self.watchlist_items = watchlist_items
                modified_paths_set.update(watchlist_items)

                # Track source for watchlist items (only if not already tracked as ondeck)
                for item in watchlist_items:
                    if item not in self.source_map:
                        self.source_map[item] = "watchlist"

        # Run modify_file_paths on all collected paths to ensure consistent path format
        logging.debug("Finalizing media to cache list...")
        self.media_to_cache = self.file_path_modifier.modify_file_paths(list(modified_paths_set))

        # Log consolidated summary of skipped disabled libraries
        self.file_path_modifier.log_disabled_skips_summary()

        # Log total media to cache
        logging.info(f"Total media to cache: {len(self.media_to_cache)} files")

        # Check for files that should be moved back to array (no longer needed in cache)
        # Only check if watched_move is enabled - otherwise files stay on cache indefinitely
        # Skip if watchlist data is incomplete (plex.tv unreachable) to prevent accidental moves
        if self.config_manager.cache.watched_move:
            if not self.plex_manager.is_watchlist_data_complete():
                logging.warning("Skipping array restore - watchlist data incomplete (plex.tv unreachable)")
                logging.warning("Files will remain on cache until next successful run")
            else:
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
            if retention_days > 0:
                logging.debug(f"Watchlist retention enabled: {retention_days} days")

            # --- Local Plex users ---
            # API returns (file_path, username, watchlisted_at) tuples
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
                    if self.watchlist_tracker.is_expired(file_path, retention_days):
                        expired_count += 1
                        continue

                result_set.add(file_path)

            # --- Remote users via RSS ---
            if self.config_manager.cache.remote_watchlist_toggle and self.config_manager.cache.remote_watchlist_rss_url:
                logging.debug("Fetching watchlist via RSS feed for remote users...")
                try:
                    # Use get_watchlist_media with rss_url parameter; users_toggle=False because this is just RSS
                    # RSS items return (file_path, username, pubDate) tuples
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

                        result_set.add(file_path)

                    if rss_expired_count > 0:
                        expired_count += rss_expired_count
                        logging.debug(f"Skipped {rss_expired_count} RSS watchlist items due to retention expiry")
                except Exception as e:
                    logging.error(f"Failed to fetch remote watchlist via RSS: {str(e)}")

            if expired_count > 0:
                logging.debug(f"Skipped {expired_count} watchlist items due to retention expiry ({retention_days} days)")

            # Log watchlist summary (show unique item count - raw counts include duplicates across users)
            total_watchlist = len(result_set)
            has_remote = 'remote_items' in locals() and len(remote_items) > 0
            source_info = " (local + remote)" if has_remote else ""
            logging.info(f"Watchlist: {total_watchlist} items{source_info}")

            # Modify file paths and fetch subtitles
            modified_items = self.file_path_modifier.modify_file_paths(list(result_set))
            result_set.update(modified_items)
            subtitles = self.subtitle_finder.get_media_subtitles(modified_items, files_to_skip=set(self.files_to_skip))
            result_set.update(subtitles)

        except Exception as e:
            logging.exception(f"An error occurred while processing the watchlist: {type(e).__name__}: {e}")

        return result_set

    
    def _extract_display_name(self, file_path: str) -> str:
        """Extract a human-readable display name from a file path.

        Returns clean filename without quality/codec info.
        """
        try:
            filename = os.path.basename(file_path)
            name = os.path.splitext(filename)[0]
            # Remove quality/codec info in brackets
            if '[' in name:
                name = name[:name.index('[')].strip()
            # Clean up trailing dashes
            name = name.rstrip(' -').rstrip('-').strip()
            return name if name else filename
        except Exception:
            return os.path.basename(file_path)

    def _file_needs_caching(self, file_path: str) -> bool:
        """Check if a file actually needs to be moved to cache.

        Returns False if the file is already on the cache drive.
        Uses the same path resolution logic as FileMover to ensure consistency.
        """
        try:
            cache_file_path = None

            # Use the file_path_modifier to get the cache path
            if hasattr(self, 'file_path_modifier') and self.file_path_modifier:
                cache_file_path, _ = self.file_path_modifier.convert_real_to_cache(file_path)

            # If convert_real_to_cache returned None, use legacy fallback (matches FileMover behavior)
            if cache_file_path is None:
                cache_dir = self.config_manager.paths.cache_dir
                real_source = self.config_manager.paths.real_source
                if cache_dir and real_source:
                    user_path = os.path.dirname(file_path)
                    relative_path = os.path.relpath(user_path, real_source)
                    cache_path = os.path.join(cache_dir, relative_path)
                    cache_file_path = os.path.join(cache_path, os.path.basename(file_path))

            # Check if cache file exists
            if cache_file_path and os.path.isfile(cache_file_path):
                return False  # Already on cache

            return True  # Needs caching
        except Exception:
            return True  # Assume it needs caching if we can't determine

    def _separate_restore_and_move(self, files_to_array: List[str]) -> Tuple[List[str], List[str]]:
        """Separate files into restore (.plexcached exists) vs actual move.

        Args:
            files_to_array: List of array paths to process

        Returns:
            Tuple of (files_to_restore, files_to_move)
        """
        to_restore = []
        to_move = []

        for array_path in files_to_array:
            plexcached_path = array_path + ".plexcached"
            if os.path.exists(plexcached_path):
                to_restore.append(array_path)
            else:
                to_move.append(array_path)

        return to_restore, to_move

    def _log_restore_and_move_summary(self, files_to_restore: List[str], files_to_move: List[str]) -> None:
        """Log summary of restore vs move operations at INFO level.

        Also tracks counts and bytes for the final summary message.
        """
        # Track counts and bytes for summary
        self.restored_count = len(files_to_restore)
        self.restored_bytes = 0

        if files_to_restore:
            # Calculate total size for restores (from .plexcached files)
            for f in files_to_restore:
                plexcached_path = f + ".plexcached"
                if os.path.exists(plexcached_path):
                    try:
                        self.restored_bytes += os.path.getsize(plexcached_path)
                    except OSError:
                        pass

            # These files have .plexcached backups on array - instant restore via rename
            count = len(files_to_restore)
            unit = "episode" if count == 1 else "episodes"
            logging.info(f"Returning to array ({count} {unit}, instant via .plexcached):")
            for f in files_to_restore[:6]:  # Show first 6
                display_name = self._extract_display_name(f)
                logging.info(f"  {display_name}")
            if len(files_to_restore) > 6:
                logging.info(f"  ...and {len(files_to_restore) - 6} more")

        if files_to_move:
            # Calculate total size for actual moves
            total_size = 0
            for f in files_to_move:
                # For moves, the file is on cache - need to get cache path
                cache_path = f.replace(
                    self.config_manager.paths.real_source,
                    self.config_manager.paths.cache_dir, 1
                )
                if os.path.exists(cache_path):
                    try:
                        total_size += os.path.getsize(cache_path)
                    except OSError:
                        pass

            # Track for summary
            self.moved_to_array_count = len(files_to_move)
            self.moved_to_array_bytes = total_size

            # These files need actual data transfer from cache to array
            count = len(files_to_move)
            unit = "episode" if count == 1 else "episodes"
            size_str = f"{total_size / (1024**3):.2f} GB" if total_size > 0 else ""
            size_part = f", {size_str}" if size_str else ""
            logging.info(f"Copying to array ({count} {unit}{size_part}):")
            for f in files_to_move[:6]:  # Show first 6
                display_name = self._extract_display_name(f)
                logging.info(f"  {display_name}")
            if len(files_to_move) > 6:
                logging.info(f"  ...and {len(files_to_move) - 6} more")

    def _move_files(self) -> None:
        """Move files to their destinations."""
        logging.info("")
        logging.info("--- Moving Files ---")

        # Move watched files to array
        if self.config_manager.cache.watched_move and self.media_to_array:
            # Log restore vs move summary before processing
            files_to_restore, files_to_move = self._separate_restore_and_move(self.media_to_array)
            if files_to_restore or files_to_move:
                self._log_restore_and_move_summary(files_to_restore, files_to_move)
            self._safe_move_files(self.media_to_array, 'array')

        # Move files to cache
        logging.debug(f"Files being passed to cache move: {self.media_to_cache}")
        # Log preview of files to be cached (similar to array move preview)
        if self.media_to_cache:
            # Filter to only files that actually need moving (not already on cache)
            files_to_cache = [f for f in self.media_to_cache if self._file_needs_caching(f)]
            if files_to_cache:
                count = len(files_to_cache)
                unit = "file" if count == 1 else "files"
                logging.info(f"Caching to cache drive ({count} {unit}):")
                for f in files_to_cache[:6]:  # Show first 6
                    display_name = self._extract_display_name(f)
                    logging.info(f"  {display_name}")
                if len(files_to_cache) > 6:
                    logging.info(f"  ...and {len(files_to_cache) - 6} more")
        self._safe_move_files(self.media_to_cache, 'cache')

    def _safe_move_files(self, files: List[str], destination: str) -> None:
        """Safely move files with consistent error handling."""
        try:
            # Pass source map only when moving to cache
            source_map = self.source_map if destination == 'cache' else None

            # Get real_source - in multi-path mode, use first enabled mapping's real_path
            real_source = self.config_manager.paths.real_source
            if not real_source and self.config_manager.paths.path_mappings:
                for mapping in self.config_manager.paths.path_mappings:
                    if mapping.enabled and mapping.real_path:
                        real_source = mapping.real_path
                        break

            # Get cache_dir - in multi-path mode, use first cacheable mapping's cache_path
            cache_dir = self.config_manager.paths.cache_dir
            if not cache_dir and self.config_manager.paths.path_mappings:
                for mapping in self.config_manager.paths.path_mappings:
                    if mapping.enabled and mapping.cacheable and mapping.cache_path:
                        cache_dir = mapping.cache_path
                        break

            self._check_free_space_and_move_files(
                files, destination,
                real_source,
                cache_dir,
                source_map
            )
        except Exception as e:
            error_msg = f"Error moving media files to {destination}: {type(e).__name__}: {e}"
            if self.dry_run:
                logging.error(error_msg)
            else:
                logging.critical(error_msg)
                sys.exit(1)

    def _get_effective_cache_limit(self, cache_dir: str) -> tuple:
        """Calculate effective cache limit in bytes, handling percentage-based limits.

        Args:
            cache_dir: Path to the cache directory.

        Returns:
            Tuple of (limit_bytes, limit_readable_str). Returns (0, None) if no limit set.
        """
        cache_limit_bytes = self.config_manager.cache.cache_limit_bytes

        if cache_limit_bytes == 0:
            return (0, None)

        if cache_limit_bytes < 0:
            # Negative value indicates percentage
            percent = abs(cache_limit_bytes)
            try:
                total_drive_size = self.file_utils.get_total_drive_size(cache_dir)
                limit_bytes = int(total_drive_size * percent / 100)
                limit_readable = f"{percent}% of {total_drive_size / (1024**3):.1f}GB = {limit_bytes / (1024**3):.1f}GB"
                return (limit_bytes, limit_readable)
            except Exception as e:
                logging.warning(f"Could not calculate cache drive size for percentage limit: {e}")
                return (0, None)
        else:
            limit_readable = f"{cache_limit_bytes / (1024**3):.1f}GB"
            return (cache_limit_bytes, limit_readable)

    def _get_plexcache_tracked_size(self) -> tuple:
        """Calculate current PlexCache tracked size from exclude file.

        Returns:
            Tuple of (total_bytes, cached_files_list). Returns (0, []) on error.
        """
        exclude_file = self.config_manager.get_mover_exclude_file()
        if not exclude_file.exists():
            return (0, [])

        plexcache_tracked = 0
        cached_files = []
        try:
            with open(exclude_file, 'r') as f:
                cached_files = [line.strip() for line in f if line.strip()]
            for cached_file in cached_files:
                try:
                    if os.path.exists(cached_file):
                        plexcache_tracked += os.path.getsize(cached_file)
                except (OSError, FileNotFoundError):
                    pass
        except Exception as e:
            logging.warning(f"Error reading exclude file: {e}")
            return (0, [])

        return (plexcache_tracked, cached_files)

    def _apply_cache_limit(self, media_files: List[str], cache_dir: str) -> List[str]:
        """Apply cache size limit, filtering out files that would exceed the limit.

        Returns the list of files that fit within the cache limit.
        Files are prioritized in the order they appear (OnDeck items should come first).
        """
        cache_limit_bytes, limit_readable = self._get_effective_cache_limit(cache_dir)

        # No limit set or error calculating
        if cache_limit_bytes == 0:
            return media_files

        # Get current PlexCache tracked size
        plexcache_tracked, _ = self._get_plexcache_tracked_size()

        # Get total cache drive usage
        try:
            disk_usage = shutil.disk_usage(cache_dir)
            drive_usage_gb = disk_usage.used / (1024**3)
        except Exception:
            drive_usage_gb = 0

        plexcache_tracked_gb = plexcache_tracked / (1024**3)
        logging.info(f"Cache limit: {limit_readable}")
        logging.info(f"Cache drive usage: {drive_usage_gb:.2f}GB, PlexCache tracked: {plexcache_tracked_gb:.2f}GB")

        # Filter files that fit within limit
        available_space = cache_limit_bytes - plexcache_tracked
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

    def _run_smart_eviction(self, needed_space_bytes: int = 0) -> tuple:
        """Run smart eviction to free cache space for higher-priority items.

        Evicts lowest-priority cached items that fall below the minimum priority
        threshold. Restores their .plexcached backup files on the array.

        Args:
            needed_space_bytes: Additional space needed (0 = just evict low-priority items)

        Returns:
            Tuple of (files_evicted_count, bytes_freed)
        """
        eviction_mode = self.config_manager.cache.cache_eviction_mode
        if eviction_mode == "none":
            return (0, 0)

        cache_dir = self.config_manager.paths.cache_dir
        cache_limit_bytes, _ = self._get_effective_cache_limit(cache_dir)
        if cache_limit_bytes == 0:
            return (0, 0)

        # Get current PlexCache tracked size and file list
        plexcache_tracked, cached_files = self._get_plexcache_tracked_size()
        if not cached_files:
            return (0, 0)

        # Check if we need to evict
        threshold_percent = self.config_manager.cache.cache_eviction_threshold_percent
        threshold_bytes = cache_limit_bytes * threshold_percent / 100

        if plexcache_tracked < threshold_bytes and needed_space_bytes == 0:
            logging.debug(f"Cache usage ({plexcache_tracked/1e9:.1f}GB) below threshold ({threshold_bytes/1e9:.1f}GB), skipping eviction")
            return (0, 0)

        # Calculate how much space to free
        space_to_free = max(needed_space_bytes, plexcache_tracked - threshold_bytes)
        if space_to_free <= 0:
            return (0, 0)

        logging.info(f"Smart eviction: need to free {space_to_free/1e9:.2f}GB")

        # Get eviction candidates based on mode
        if eviction_mode == "smart":
            candidates = self.priority_manager.get_eviction_candidates(cached_files, int(space_to_free))
        elif eviction_mode == "fifo":
            # FIFO: evict oldest cached files first (by timestamp)
            candidates = self._get_fifo_eviction_candidates(cached_files, int(space_to_free))
        else:
            return (0, 0)

        if not candidates:
            logging.info("No low-priority items available for eviction")
            return (0, 0)

        # Log what we're evicting
        for cache_path in candidates:
            if eviction_mode == "smart":
                priority = self.priority_manager.calculate_priority(cache_path)
                priority_info = f"priority={priority}"
            else:
                priority_info = "fifo"
            try:
                size_mb = os.path.getsize(cache_path) / (1024**2)
            except (OSError, FileNotFoundError):
                size_mb = 0
            logging.info(f"Evicting ({priority_info}): {os.path.basename(cache_path)} ({size_mb:.1f}MB)")

        if self.dry_run:
            logging.info(f"DRY-RUN: Would evict {len(candidates)} files")
            return (0, 0)

        # Perform eviction: restore .plexcached files, remove from exclude list
        files_evicted = 0
        bytes_freed = 0
        real_source = self.config_manager.paths.real_source

        for cache_path in candidates:
            try:
                file_size = os.path.getsize(cache_path) if os.path.exists(cache_path) else 0

                # Find and restore .plexcached backup
                array_path = cache_path.replace(cache_dir, real_source, 1)
                plexcached_path = array_path + ".plexcached"

                if os.path.exists(plexcached_path):
                    # Restore: rename .plexcached back to original
                    os.rename(plexcached_path, array_path)
                    logging.debug(f"Restored .plexcached: {array_path}")

                # Delete cache copy
                if os.path.exists(cache_path):
                    os.remove(cache_path)

                # Clean up tracking
                self.file_filter.remove_files_from_exclude_list([cache_path])
                self.timestamp_tracker.remove_entry(cache_path)

                files_evicted += 1
                bytes_freed += file_size

            except Exception as e:
                logging.warning(f"Failed to evict {cache_path}: {e}")

        logging.info(f"Smart eviction complete: freed {bytes_freed/1e9:.2f}GB from {files_evicted} files")
        return (files_evicted, bytes_freed)

    def _get_fifo_eviction_candidates(self, cached_files: List[str], target_bytes: int) -> List[str]:
        """Get files to evict using FIFO (oldest first) strategy.

        Args:
            cached_files: List of cache file paths.
            target_bytes: Amount of space needed to free.

        Returns:
            List of cache file paths to evict, in eviction order.
        """
        if target_bytes <= 0:
            return []

        # Get files with their cache timestamps, sorted by oldest first
        files_with_age = []
        for cache_path in cached_files:
            hours_cached = self.priority_manager._get_hours_since_cached(cache_path)
            files_with_age.append((cache_path, hours_cached if hours_cached >= 0 else float('inf')))

        # Sort by age descending (oldest first)
        files_with_age.sort(key=lambda x: x[1], reverse=True)

        candidates = []
        bytes_accumulated = 0

        for cache_path, hours in files_with_age:
            if not os.path.exists(cache_path):
                continue

            try:
                file_size = os.path.getsize(cache_path)
            except (OSError, IOError):
                continue

            candidates.append(cache_path)
            bytes_accumulated += file_size

            if bytes_accumulated >= target_bytes:
                break

        return candidates

    def _check_free_space_and_move_files(self, media_files: List[str], destination: str,
                                        real_source: str, cache_dir: str,
                                        source_map: dict = None) -> None:
        """Check free space and move files."""
        media_files_filtered = self.file_filter.filter_files(
            media_files, destination, self.media_to_cache, set(self.files_to_skip)
        )

        # Run smart eviction before applying cache limit (if enabled)
        if destination == 'cache':
            self._run_smart_eviction()

        # Apply cache size limit when moving to cache
        if destination == 'cache':
            media_files_filtered = self._apply_cache_limit(media_files_filtered, cache_dir)

        total_size, total_size_unit = self.file_utils.get_total_size_of_files(media_files_filtered)
        
        if total_size > 0:
            logging.debug(f"Moving {total_size:.2f} {total_size_unit} to {destination}")
            # Generate summary message with restore vs move separation for array moves
            if destination == 'array':
                parts = []
                if self.restored_count > 0:
                    unit = "episode" if self.restored_count == 1 else "episodes"
                    size_gb = self.restored_bytes / (1024**3)
                    parts.append(f"Returned {self.restored_count} {unit} ({size_gb:.2f} GB) to array")
                if self.moved_to_array_count > 0:
                    unit = "episode" if self.moved_to_array_count == 1 else "episodes"
                    size_gb = self.moved_to_array_bytes / (1024**3)
                    parts.append(f"Copied {self.moved_to_array_count} {unit} ({size_gb:.2f} GB) to array")
                if parts:
                    self.logging_manager.add_summary_message(', '.join(parts))
                else:
                    self.logging_manager.add_summary_message(
                        f"Moved {total_size:.2f} {total_size_unit} to {destination}"
                    )
            else:
                # Track cached bytes for summary
                size_multipliers = {'KB': 1024, 'MB': 1024**2, 'GB': 1024**3, 'TB': 1024**4}
                self.cached_bytes = int(total_size * size_multipliers.get(total_size_unit, 1))
                self.logging_manager.add_summary_message(
                    f"Cached {total_size:.2f} {total_size_unit}"
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
                if not self.dry_run:
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
            # Use the freshly fetched watchlist items (already filtered for retention in _process_watchlist)
            current_watchlist_items = getattr(self, 'watchlist_items', set())
            
            # Get files that should be moved back to array (tracked by exclude file)
            files_to_move_back, cache_paths_to_remove = self.file_filter.get_files_to_move_back_to_array(
                current_ondeck_items, current_watchlist_items
            )

            if files_to_move_back:
                logging.debug(f"Found {len(files_to_move_back)} files to move back to array")
                self.media_to_array.extend(files_to_move_back)

            # Always clean up stale entries from exclude list (files that no longer exist on cache)
            if cache_paths_to_remove and not self.dry_run:
                self.file_filter.remove_files_from_exclude_list(cache_paths_to_remove)
            elif cache_paths_to_remove and self.dry_run:
                logging.debug(f"[DRY RUN] Would remove {len(cache_paths_to_remove)} stale entries from exclude list")
        except Exception as e:
            logging.exception(f"Error checking files to move back to array: {type(e).__name__}: {e}")
    
    def _finish(self) -> None:
        """Finish the application and log summary."""
        end_time = time.time()
        execution_time_seconds = end_time - self.start_time
        execution_time = self._convert_time(execution_time_seconds)

        # Collect structured summary data for rich webhook formatting
        cached_count = getattr(self.file_mover, 'last_cache_moves_count', 0) if self.file_mover else 0
        cached_bytes = getattr(self, 'cached_bytes', 0)
        restored_count = getattr(self, 'restored_count', 0)
        restored_bytes = getattr(self, 'restored_bytes', 0)
        already_cached = getattr(self.file_filter, 'last_already_cached_count', 0) if self.file_filter else 0

        self.logging_manager.set_summary_data(
            cached_count=cached_count,
            cached_bytes=cached_bytes,
            restored_count=restored_count,
            restored_bytes=restored_bytes,
            already_cached=already_cached,
            duration_seconds=execution_time_seconds,
            had_errors=False,  # Could track this via error count if needed
            had_warnings=False
        )

        self.logging_manager.log_summary()

        # Note: Empty folder cleanup now happens immediately during file operations
        # (per File and Folder Management Policy) - no blanket cleanup needed here

        # Clean up stale timestamp entries for files that no longer exist
        if hasattr(self, 'timestamp_tracker') and self.timestamp_tracker and not self.dry_run:
            self.timestamp_tracker.cleanup_missing_files()

        # Clean up stale watchlist tracker entries
        if hasattr(self, 'watchlist_tracker') and self.watchlist_tracker and not self.dry_run:
            self.watchlist_tracker.cleanup_stale_entries()
            self.watchlist_tracker.cleanup_missing_files()

        # Log results summary for all runs (INFO level)
        self._log_results_summary()

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
    dry_run = "--dry-run" in sys.argv or "--debug" in sys.argv  # --debug is alias for backwards compatibility
    restore_plexcached = "--restore-plexcached" in sys.argv
    quiet = "--quiet" in sys.argv or "--notify-errors-only" in sys.argv
    verbose = "--verbose" in sys.argv or "-v" in sys.argv or "--v" in sys.argv
    show_priorities = "--show-priorities" in sys.argv
    show_mappings = "--show-mappings" in sys.argv

    # Derive config path from project root (go up one level if we're in core/)
    script_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    project_root = script_dir.parent if script_dir.name == 'core' else script_dir
    config_file = str(project_root / "plexcache_settings.json")

    # Handle emergency restore mode
    if restore_plexcached:
        _run_plexcached_restore(config_file, dry_run, verbose)
        return

    # Handle show priorities mode
    if show_priorities:
        _run_show_priorities(config_file, verbose)
        return

    # Handle show mappings mode
    if show_mappings:
        _run_show_mappings(config_file)
        return

    app = PlexCacheApp(config_file, dry_run, quiet, verbose)
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
    # In multi-path mode, get paths from all enabled mappings
    search_paths = []
    if config_manager.paths.real_source and config_manager.paths.real_source.strip():
        search_paths.append(config_manager.paths.real_source)
    elif config_manager.paths.path_mappings:
        for mapping in config_manager.paths.path_mappings:
            if mapping.enabled and mapping.real_path:
                search_paths.append(mapping.real_path)

    if not search_paths:
        logging.error("No search paths configured. Check your settings.")
        return

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


def _run_show_priorities(config_file: str, verbose: bool = False) -> None:
    """Show priority scores for all cached files."""
    import logging
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    print("*** PlexCache Priority Report ***\n")

    # Load config
    config_manager = ConfigManager(config_file)
    config_manager.load_config()

    # Get the mover exclude file to find cached files
    mover_exclude = config_manager.get_mover_exclude_file()
    if not mover_exclude.exists():
        print("No exclude file found. No files are currently cached.")
        return

    # Read cached files from exclude list
    with open(mover_exclude, 'r') as f:
        cached_files = [line.strip() for line in f if line.strip()]

    if not cached_files:
        print("Exclude file is empty. No files are currently cached.")
        return

    # Initialize trackers
    timestamp_file = config_manager.get_timestamp_file()
    watchlist_tracker_file = config_manager.get_watchlist_tracker_file()
    ondeck_tracker_file = config_manager.get_ondeck_tracker_file()

    timestamp_tracker = CacheTimestampTracker(str(timestamp_file))
    watchlist_tracker = WatchlistTracker(str(watchlist_tracker_file))
    ondeck_tracker = OnDeckTracker(str(ondeck_tracker_file))

    # Get eviction settings (use defaults if not set)
    eviction_min_priority = getattr(config_manager.cache, 'eviction_min_priority', 60)
    number_episodes = getattr(config_manager.plex, 'number_episodes', 5)

    # Initialize priority manager
    priority_manager = CachePriorityManager(
        timestamp_tracker=timestamp_tracker,
        watchlist_tracker=watchlist_tracker,
        ondeck_tracker=ondeck_tracker,
        eviction_min_priority=eviction_min_priority,
        number_episodes=number_episodes
    )

    # Generate and print report
    report = priority_manager.get_priority_report(cached_files)
    print(report)


def _run_show_mappings(config_file: str) -> None:
    """Show path mapping configuration and accessibility status."""
    print("*** PlexCache Path Mapping Configuration ***\n")

    # Load config
    config_manager = ConfigManager(config_file)
    config_manager.load_config()

    # Check if path_mappings is configured
    mappings = config_manager.paths.path_mappings
    if not mappings:
        print("No multi-path mappings configured.")
        print(f"\nLegacy single-path mode:")
        print(f"  Plex source: {config_manager.paths.plex_source or 'Not set'}")
        print(f"  Real source: {config_manager.paths.real_source or 'Not set'}")
        print(f"  Cache dir:   {config_manager.paths.cache_dir or 'Not set'}")
        print("\nRun the setup wizard to configure multi-path mappings.")
        return

    # Display mappings table
    print(f"Found {len(mappings)} path mapping(s):\n")

    # Calculate column widths
    name_width = max(len("Name"), max(len(m.name) for m in mappings))
    plex_width = max(len("Plex Path"), max(len(m.plex_path) for m in mappings))
    real_width = max(len("Real Path"), max(len(m.real_path) for m in mappings))

    # Header
    header = f"  {'#':<3} {'Name':<{name_width}}  {'Plex Path':<{plex_width}}  {'Real Path':<{real_width}}  {'Cacheable':<9}  {'Enabled':<7}"
    separator = "  " + "-" * (len(header) - 2)
    print(header)
    print(separator)

    # Rows
    for i, m in enumerate(mappings, 1):
        cacheable = "Yes" if m.cacheable else "No"
        enabled = "Yes" if m.enabled else "No"
        print(f"  {i:<3} {m.name:<{name_width}}  {m.plex_path:<{plex_width}}  {m.real_path:<{real_width}}  {cacheable:<9}  {enabled:<7}")

    # Path accessibility check
    print(f"\n{'Path Accessibility Check:'}")
    print(separator)

    for m in mappings:
        if not m.enabled:
            print(f"  [ ] {m.real_path} - DISABLED (skipping check)")
            continue

        if os.path.exists(m.real_path):
            print(f"  [✓] {m.real_path} - accessible")
        else:
            print(f"  [✗] {m.real_path} - NOT ACCESSIBLE")

    # Cache paths check (only for cacheable mappings)
    cacheable_mappings = [m for m in mappings if m.cacheable and m.enabled and m.cache_path]
    if cacheable_mappings:
        print(f"\n{'Cache Path Accessibility:'}")
        print(separator)

        for m in cacheable_mappings:
            # Check if cache path parent exists
            cache_parent = os.path.dirname(m.cache_path.rstrip('/'))
            if os.path.exists(cache_parent):
                print(f"  [✓] {m.cache_path} - accessible")
            else:
                print(f"  [✗] {m.cache_path} - NOT ACCESSIBLE (parent dir missing)")

    # Summary
    enabled_count = sum(1 for m in mappings if m.enabled)
    cacheable_count = sum(1 for m in mappings if m.enabled and m.cacheable)
    non_cacheable_count = enabled_count - cacheable_count

    print(f"\n{'Summary:'}")
    print(separator)
    print(f"  Total mappings:     {len(mappings)}")
    print(f"  Enabled:            {enabled_count}")
    print(f"  Cacheable:          {cacheable_count}")
    print(f"  Non-cacheable:      {non_cacheable_count} (files tracked but not cached)")


if __name__ == "__main__":
    main() 
