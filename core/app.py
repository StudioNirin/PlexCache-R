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
from datetime import datetime
from pathlib import Path
from typing import List, Set, Optional, Tuple
import os

from core import __version__
from core.config import ConfigManager
from core.logging_config import LoggingManager, reset_warning_error_flag
from core.system_utils import SystemDetector, FileUtils, SingleInstanceLock, get_disk_usage, get_array_direct_path, detect_zfs, set_zfs_prefixes
from core.plex_api import PlexManager, OnDeckItem
from core.file_operations import MultiPathModifier, SubtitleFinder, FileFilter, FileMover, PlexcachedRestorer, CacheTimestampTracker, WatchlistTracker, OnDeckTracker, CachePriorityManager, PlexcachedMigration, get_media_identity, find_matching_plexcached


class PlexCacheApp:
    """Main PlexCache application class."""

    def __init__(self, config_file: str, dry_run: bool = False,
                 quiet: bool = False, verbose: bool = False,
                 bytes_progress_callback=None):
        self.config_file = config_file
        self.dry_run = dry_run  # Don't move files, just simulate
        self.quiet = quiet  # Override notification level to errors-only
        self.verbose = verbose  # Enable DEBUG level logging
        self._bytes_progress_callback = bytes_progress_callback  # Byte-level progress for operation banner
        self.start_time = time.time()
        
        # Initialize components
        self.config_manager = ConfigManager(config_file)
        self.system_detector = SystemDetector()
        self.file_utils = FileUtils(
            self.system_detector.is_linux,
            is_docker=self.system_detector.is_docker
        )
        
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
        # Eviction tracking
        self.evicted_count = 0
        self.evicted_bytes = 0
        # Deferred exclude list removal for move-back files (issue #13)
        self._move_back_exclude_paths: list = []

        # Stop request flag (for web UI to abort operations)
        self._stop_requested = False

    def request_stop(self) -> None:
        """Request the operation to stop gracefully after current file."""
        self._stop_requested = True
        logging.info("Stop requested - operation will stop after current file completes")

    @property
    def should_stop(self) -> bool:
        """Check if stop has been requested."""
        return self._stop_requested

    def run(self) -> None:
        """Run the main application."""
        try:
            # Setup logging first before any log messages
            self._setup_logging()
            # Reset warning/error tracking for conditional summary notifications
            reset_warning_error_flag()
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

            # Wait for Unraid mover to finish (prevents race condition)
            if self._is_mover_running():
                max_wait_seconds = 4 * 60 * 60  # 4 hours
                poll_interval = 30  # Check every 30 seconds
                logging.warning("Unraid mover is currently running. Waiting for it to finish before proceeding...")
                print("WARNING: Unraid mover is running. Waiting for it to finish...")
                waited = 0
                while self._is_mover_running():
                    if self.should_stop:
                        logging.info("Stop requested while waiting for mover. Exiting.")
                        return
                    if waited >= max_wait_seconds:
                        logging.warning(f"Mover still running after {max_wait_seconds // 3600} hours. Skipping this run.")
                        return
                    time.sleep(poll_interval)
                    waited += poll_interval
                    if waited % 300 == 0:  # Log every 5 minutes
                        logging.info(f"Still waiting for mover to finish... ({waited // 60} minutes elapsed)")
                minutes_waited = waited / 60
                logging.info(f"Unraid mover finished after {minutes_waited:.1f} minutes of waiting. Proceeding with PlexCache run.")

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

            # Migrate old exclude file name before any initialization
            self._migrate_exclude_file()

            # Initialize components that depend on config
            logging.debug("Initializing components...")
            self._initialize_components()

            # Warn if .plexcached backups are disabled
            if not self.config_manager.cache.create_plexcached_backups:
                logging.warning("BACKUPS DISABLED - No .plexcached files will be created. Cached files cannot be recovered if cache drive fails.")

            # Log hard-linked files handling mode
            if self.config_manager.cache.hardlinked_files == "move":
                logging.info("Hard-linked files mode: MOVE - Hard-linked files will be cached (seed copies preserved via remaining hard links)")

            # Clean up stale exclude list entries (self-healing)
            # Skip in dry-run mode to avoid modifying tracking files
            if not self.dry_run:
                self.file_filter.clean_stale_exclude_entries()
            else:
                logging.debug("[DRY RUN] Skipping stale exclude list cleanup")

            # Check paths
            logging.debug("Validating paths...")
            self._check_paths()

            # Connect to Plex
            self._connect_to_plex()

            # Check for active sessions
            self._check_active_sessions()

            # Check for stop request before processing
            if self.should_stop:
                logging.info("Operation stopped before processing media")
                return

            # Process media
            self._process_media()

            # Check for stop request before moving files
            if self.should_stop:
                logging.info("Operation stopped before moving files")
                return

            # Move files
            self._move_files()

            # Check for stop request after moving files
            if self.should_stop:
                logging.info("Operation stopped by user")
                return

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

    def _migrate_exclude_file(self) -> None:
        """One-time migration: rename old exclude file to new name."""
        old_exclude_file = os.path.join(
            self.config_manager.paths.script_folder,
            "plexcache_mover_files_to_exclude.txt"
        )
        new_cached_file = os.path.join(
            self.config_manager.paths.script_folder,
            "plexcache_cached_files.txt"
        )
        
        old_exists = os.path.exists(old_exclude_file)
        new_exists = os.path.exists(new_cached_file)
        
        if old_exists and not new_exists:
            try:
                os.rename(old_exclude_file, new_cached_file)
                logging.info(f"Migrated {old_exclude_file} -> {new_cached_file}")
            except OSError as e:
                logging.error(f"Failed to migrate exclude file: {e}")
                logging.error(f"Please manually rename '{old_exclude_file}' to '{new_cached_file}'")
                raise
        elif old_exists and new_exists:
            try:
                os.remove(old_exclude_file)
                logging.info(f"Removed legacy exclude file: {old_exclude_file}")
            except OSError as e:
                logging.warning(f"Could not remove legacy exclude file: {e}")
    

    def _update_unraid_mover_exclusions(self, tag_line: str = "### Plexcache exclusions below this line") -> None:
        """
        Update the Unraid mover exclusions file by inserting or updating the
        PlexCache exclusions section. Paths are retrieved from the config.
        """

        # Get paths from config
        exclusion_path = self.config_manager.get_unraid_mover_exclusions_file()
        plexcache_path = self.config_manager.get_cached_files_file()

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
        # Log version and build info for debugging
        build_commit = os.environ.get('GIT_COMMIT', 'dev')
        logging.info(f"=== PlexCache-D v{__version__} (build: {build_commit}) ===")
        # Log file ownership configuration (PUID/PGID)
        self.file_utils.log_ownership_config()

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
        move_verb = "Would move" if self.dry_run else "Moved"
        logging.info(f"{move_verb} to cache: {actually_moved} files")
        logging.info(f"{move_verb} to array: {moved_to_array} files")

        # Show eviction stats if any files were evicted
        if self.evicted_count > 0:
            evict_verb = "Would evict" if self.dry_run else "Evicted"
            evicted_size = self.evicted_bytes / (1024**3)  # Convert to GB
            logging.info(f"{evict_verb}: {self.evicted_count} files ({evicted_size:.2f} GB freed)")

        # Additional detail at DEBUG level
        # Note: Empty folder cleanup now happens immediately during file operations
        # (per File and Folder Management Policy) and is logged at DEBUG level as it occurs

    def _is_mover_running(self) -> bool:
        """Check if the Unraid mover is currently running.

        This prevents race conditions where PlexCache caches files while
        the mover is actively moving files, which can result in files
        being moved back to the array before they're added to the exclude list.

        Detection methods (in order):
        1. PID file check - works in both CLI and Docker (if /var/run is mounted)
        2. pgrep fallback - works in CLI or Docker with --pid=host

        Returns:
            True if mover is running, False otherwise.
        """
        if not self.system_detector.is_unraid:
            return False

        # Check for mover PID file (created by mover/age_mover, removed on completion)
        # In Docker: /host_var_run/mover.pid (mount /var/run:/host_var_run:ro)
        # On host:   /var/run/mover.pid
        for pid_path in ['/host_var_run/mover.pid', '/var/run/mover.pid']:
            if os.path.exists(pid_path):
                logging.info(f"Unraid mover detected via PID file: {pid_path}")
                return True

        try:
            # Fallback: check for mover process using pgrep
            # Works on host or in Docker with --pid=host
            result = subprocess.run(
                ['pgrep', '-f', '/usr/local/sbin/mover'],
                capture_output=True,
                text=True
            )
            if result.returncode == 0 and result.stdout.strip():
                logging.info("Unraid mover detected via pgrep (mover)")
                return True

            # Also check for the age_mover script (CA Mover Tuning plugin)
            result = subprocess.run(
                ['pgrep', '-f', 'age_mover'],
                capture_output=True,
                text=True
            )
            if result.returncode == 0 and result.stdout.strip() != '':
                logging.info("Unraid mover detected via pgrep (age_mover)")
                return True

        except (subprocess.SubprocessError, FileNotFoundError):
            pass

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
            path_modifier=self.file_path_modifier,
            is_docker=self.system_detector.is_docker
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
            path_modifier=self.file_path_modifier,
            is_docker=self.system_detector.is_docker
        )

        self.file_mover = FileMover(
            real_source=self.config_manager.paths.real_source,
            cache_dir=self.config_manager.paths.cache_dir,
            is_unraid=self.system_detector.is_unraid,
            file_utils=self.file_utils,
            debug=self.dry_run,
            mover_cache_exclude_file=str(mover_exclude),
            timestamp_tracker=self.timestamp_tracker,
            path_modifier=self.file_path_modifier,
            stop_check=lambda: self.should_stop,  # Allow FileMover to check for stop requests
            create_plexcached_backups=self.config_manager.cache.create_plexcached_backups,
            hardlinked_files=self.config_manager.cache.hardlinked_files,
            cleanup_empty_folders=self.config_manager.cache.cleanup_empty_folders,
            bytes_progress_callback=self._bytes_progress_callback
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

    def _detect_zfs_paths(self) -> None:
        """Detect ZFS-backed path mappings and configure array-direct path conversion.

        On Unraid, /mnt/user/ is normally converted to /mnt/user0/ (array-direct) to
        avoid FUSE ambiguity. But ZFS pool-only shares (shareUseCache=only) never have
        files at /mnt/user0/ — their files live on the ZFS pool. For these paths, we
        skip the conversion so file operations work correctly.

        Hybrid detection: When a share has a ZFS cache but also has files on the array
        (shareUseCache=yes/prefer), it is NOT pool-only. We verify by probing /mnt/user0/
        for actual array files. If found, the share is hybrid and array-direct conversion
        stays enabled — critical for correct .plexcached renames.

        Note: This detection is a performance hint for get_array_direct_path(). Safety-
        critical operations (_move_to_cache, _move_to_array, _should_add_to_cache) also
        probe /mnt/user0/ directly as defense in depth.

        Only runs on Unraid (non-ZFS systems are unaffected).
        """
        if not self.system_detector.is_unraid:
            return

        zfs_prefixes = set()
        all_mappings = self.config_manager.paths.path_mappings or []

        for mapping in all_mappings:
            if not mapping.enabled:
                continue
            real_path = mapping.real_path or ""
            if real_path.startswith('/mnt/user/'):
                is_zfs = detect_zfs(real_path)
                if is_zfs:
                    # Verify truly pool-only by probing /mnt/user0/ for array files
                    user0_path = '/mnt/user0/' + real_path[len('/mnt/user/'):]
                    if os.path.exists('/mnt/user0'):
                        user0_has_files = False
                        if os.path.isdir(user0_path):
                            try:
                                with os.scandir(user0_path) as it:
                                    user0_has_files = next(it, None) is not None
                            except OSError:
                                pass

                        if user0_has_files:
                            logging.info(
                                f"ZFS cache detected for: {real_path}, but array files also exist "
                                f"at {user0_path} — hybrid share (likely shareUseCache=yes/prefer). "
                                f"Array-direct conversion remains enabled."
                            )
                        else:
                            prefix = real_path.rstrip('/') + '/'
                            zfs_prefixes.add(prefix)
                            logging.info(f"ZFS pool-only detected for: {real_path} (array-direct conversion disabled)")
                    else:
                        # /mnt/user0 not accessible — cannot verify, assume pool-only
                        prefix = real_path.rstrip('/') + '/'
                        zfs_prefixes.add(prefix)
                        logging.warning(
                            f"ZFS detected for {real_path} but /mnt/user0 not accessible to verify. "
                            f"Assuming pool-only. If running in Docker, ensure /mnt/user0 is mounted."
                        )
                else:
                    logging.debug(f"No ZFS detected for: {real_path} (standard array path)")

        if zfs_prefixes:
            set_zfs_prefixes(zfs_prefixes)
            logging.info(f"ZFS prefixes configured: {zfs_prefixes}")
        else:
            logging.debug("No ZFS-backed paths found — all paths use standard array-direct conversion")

    def _migrate_exclude_file_paths(self, exclude_file: Path) -> None:
        """Migrate exclude file entries from container paths to host paths (Docker only).

        When running in Docker, the container sees paths like /mnt/cache/... but the host
        (where Unraid mover runs) sees /mnt/cache_downloads/.... This method translates
        existing entries to use host paths so the Unraid mover recognizes them.

        This migration is:
        - Automatic: runs on startup without user intervention
        - Safe: only translates paths, doesn't delete entries
        - Idempotent: already-translated paths won't match container prefix
        """
        if not self.system_detector.is_docker:
            return

        if not exclude_file.exists():
            return

        # Build translation map from path_mappings: container_prefix -> host_prefix
        translations = {}
        for mapping in self.config_manager.paths.path_mappings:
            if mapping.cache_path and mapping.host_cache_path:
                if mapping.cache_path != mapping.host_cache_path:
                    container_prefix = mapping.cache_path.rstrip('/')
                    host_prefix = mapping.host_cache_path.rstrip('/')
                    translations[container_prefix] = host_prefix

        if not translations:
            return  # No translations needed

        # Read existing entries
        try:
            with open(exclude_file, 'r') as f:
                entries = [line.strip() for line in f if line.strip()]
        except (IOError, OSError) as e:
            logging.warning(f"Could not read exclude file for migration: {e}")
            return

        if not entries:
            return

        # Translate entries that still have container paths
        migrated_count = 0
        translated_entries = []

        for entry in entries:
            translated = entry
            for container_prefix, host_prefix in translations.items():
                if entry.startswith(container_prefix):
                    translated = entry.replace(container_prefix, host_prefix, 1)
                    migrated_count += 1
                    break
            translated_entries.append(translated)

        # Only write if we actually migrated something
        if migrated_count > 0:
            try:
                with open(exclude_file, 'w') as f:
                    for entry in translated_entries:
                        f.write(f"{entry}\n")
                logging.info(f"Migrated {migrated_count} exclude file entries to host paths")
                for container_prefix, host_prefix in translations.items():
                    logging.debug(f"  Translated: {container_prefix} -> {host_prefix}")
            except (IOError, OSError) as e:
                logging.error(f"Could not write migrated exclude file: {e}")

    def _initialize_components(self) -> None:
        """Initialize components that depend on configuration."""
        logging.debug("Initializing application components...")

        # Initialize Plex manager
        self._init_plex_manager()

        # Initialize path modifier and subtitle finder
        self._init_path_modifier()

        # Detect ZFS-backed path mappings (must happen before any file operations)
        self._detect_zfs_paths()

        # Get file paths for trackers
        mover_exclude = self.config_manager.get_cached_files_file()
        timestamp_file = self.config_manager.get_timestamp_file()
        logging.debug(f"Mover exclude file: {mover_exclude}")
        logging.debug(f"Timestamp file: {timestamp_file}")

        # Create exclude file on startup if it doesn't exist
        if not mover_exclude.exists():
            mover_exclude.touch()
            logging.info(f"Created mover exclude file: {mover_exclude}")

        # Migrate exclude file paths from container to host paths (Docker only)
        self._migrate_exclude_file_paths(mover_exclude)

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
        # In Docker, validate that mount points are properly configured
        # This prevents writing massive amounts of data inside the container
        if self.system_detector.is_docker:
            paths_to_validate = set()

            # Collect paths from path_mappings
            if self.config_manager.paths.path_mappings:
                for mapping in self.config_manager.paths.path_mappings:
                    if mapping.enabled:
                        if mapping.real_path:
                            # Extract base mount point (e.g., /mnt/user from /mnt/user/Movies/)
                            parts = mapping.real_path.strip('/').split('/')
                            if len(parts) >= 2:
                                paths_to_validate.add('/' + '/'.join(parts[:2]))
                        if mapping.cache_path:
                            parts = mapping.cache_path.strip('/').split('/')
                            if len(parts) >= 2:
                                paths_to_validate.add('/' + '/'.join(parts[:2]))

            # Also check common Unraid paths
            paths_to_validate.update(['/mnt/cache', '/mnt/user', '/mnt/user0'])

            # Validate mounts
            mount_warnings = self.system_detector.validate_docker_mounts(list(paths_to_validate))
            for warning in mount_warnings:
                logging.warning(warning)

            # If any critical mount is not properly mounted, abort
            if mount_warnings:
                raise RuntimeError(
                    "Docker mount validation failed! One or more paths may not be properly mounted. "
                    "This could cause data to be written inside the container instead of to mounted volumes. "
                    "Check your Docker volume configuration and ensure source paths exist on the host."
                )

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

            # Auto-add newly discovered users to settings (with tracking disabled)
            new_users = self.plex_manager.get_newly_discovered_users()
            if new_users:
                self._add_new_users_to_settings(new_users)

    def _add_new_users_to_settings(self, new_users: List[dict]) -> None:
        """Add newly discovered users to settings with tracking disabled.

        Args:
            new_users: List of user info dicts from plex.tv (with title, token, id, uuid, etc.)
        """
        added_count = 0
        for user_info in new_users:
            username = user_info.get('title')
            if not username:
                continue

            # Check if user already exists (shouldn't happen, but be safe)
            existing = any(u.get('title') == username for u in self.config_manager.plex.users)
            if existing:
                continue

            # Ensure settings_data['users'] exists and is linked to plex.users
            if 'users' not in self.config_manager.settings_data:
                self.config_manager.settings_data['users'] = []
                self.config_manager.plex.users = self.config_manager.settings_data['users']

            # Add user (plex.users and settings_data['users'] are the same list reference)
            self.config_manager.plex.users.append(user_info)

            logging.warning(
                f"[PLEX API] Auto-added new user '{username}' with tracking disabled. "
                f"Enable via Settings > Plex or re-run Setup wizard."
            )
            added_count += 1

        # Save updated settings
        if added_count > 0:
            try:
                self.config_manager._save_updated_config()
                logging.debug(f"Saved {added_count} new user(s) to settings")
            except Exception as e:
                logging.error(f"Failed to save new users to settings: {e}")

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
                            skip_watchlist=self.config_manager.plex.skip_watchlist,
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

        # Step 1: Move watched files to array (frees space naturally)
        if self.config_manager.cache.watched_move and self.media_to_array:
            # Log restore vs move summary before processing
            files_to_restore, files_to_move = self._separate_restore_and_move(self.media_to_array)
            if files_to_restore or files_to_move:
                self._log_restore_and_move_summary(files_to_restore, files_to_move)
            self._safe_move_files(self.media_to_array, 'array')

            # Deferred exclude list cleanup: only remove entries for files that actually moved (issue #13)
            # This prevents losing tracking of files if a move fails (e.g., disk full, I/O error)
            if self._move_back_exclude_paths and self.file_mover and not self.dry_run:
                successful_moves = set(self.file_mover._successful_array_moves)
                if successful_moves:
                    self.file_filter.remove_files_from_exclude_list(list(successful_moves))

                # Log warnings for files that failed to move (their exclude entries stay protected)
                deferred_count = len(self._move_back_exclude_paths)
                succeeded_count = len(successful_moves)
                if succeeded_count < deferred_count:
                    failed_count = deferred_count - succeeded_count
                    logging.warning(f"{failed_count} file(s) failed to move to array — exclude entries preserved for retry")

        # Step 2: Run smart eviction BEFORE filtering/caching (frees more space if needed)
        # This runs after array moves so we have accurate space calculations
        if self.media_to_cache:
            evicted_count, evicted_bytes = self._run_smart_eviction()
            self.evicted_count += evicted_count
            self.evicted_bytes += evicted_bytes

        # Step 3: Move files to cache
        logging.debug(f"Files being passed to cache move: {self.media_to_cache}")

        # Filter out files that would be immediately evicted (prevents cache/evict loop)
        # Now runs AFTER eviction, so threshold check is accurate
        if self.media_to_cache:
            self.media_to_cache = self._filter_low_priority_files(self.media_to_cache, self.source_map)

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
                # Use manual override if configured (important for ZFS)
                drive_size_override = self.config_manager.cache.cache_drive_size_bytes
                disk_usage = get_disk_usage(cache_dir, drive_size_override)
                total_drive_size = disk_usage.total
                limit_bytes = int(total_drive_size * percent / 100)
                limit_readable = f"{percent}% of {total_drive_size / (1024**3):.1f}GB = {limit_bytes / (1024**3):.1f}GB"
                return (limit_bytes, limit_readable)
            except Exception as e:
                logging.warning(f"Could not calculate cache drive size for percentage limit: {e}")
                return (0, None)
        else:
            limit_readable = f"{cache_limit_bytes / (1024**3):.1f}GB"
            return (cache_limit_bytes, limit_readable)

    def _get_effective_min_free_space(self, cache_dir: str) -> tuple:
        """Calculate effective min free space in bytes, handling percentage-based values.

        Args:
            cache_dir: Path to the cache directory.

        Returns:
            Tuple of (min_free_bytes, readable_str). Returns (0, None) if disabled.
        """
        min_free_bytes = self.config_manager.cache.min_free_space_bytes

        if min_free_bytes == 0:
            return (0, None)

        if min_free_bytes < 0:
            # Negative value indicates percentage
            percent = abs(min_free_bytes)
            try:
                drive_size_override = self.config_manager.cache.cache_drive_size_bytes
                disk_usage = get_disk_usage(cache_dir, drive_size_override)
                total_drive_size = disk_usage.total
                free_bytes = int(total_drive_size * percent / 100)
                readable = f"{percent}% of {total_drive_size / (1024**3):.1f}GB = {free_bytes / (1024**3):.1f}GB"
                return (free_bytes, readable)
            except Exception as e:
                logging.warning(f"Could not calculate cache drive size for min_free_space percentage: {e}")
                return (0, None)
        else:
            readable = f"{min_free_bytes / (1024**3):.1f}GB"
            return (min_free_bytes, readable)

    def _get_plexcache_tracked_size(self) -> tuple:
        """Calculate current PlexCache tracked size from exclude file.

        Returns:
            Tuple of (total_bytes, cached_files_list). Returns (0, []) on error.
            In Docker, paths are translated from host to container paths.
        """
        exclude_file = self.config_manager.get_cached_files_file()
        if not exclude_file.exists():
            return (0, [])

        plexcache_tracked = 0
        cached_files = []
        try:
            with open(exclude_file, 'r') as f:
                host_paths = [line.strip() for line in f if line.strip()]

            for host_path in host_paths:
                # In Docker, exclude file has host paths but we need container paths
                # to check existence and calculate size
                if self.file_filter:
                    container_path = self.file_filter._translate_from_host_path(host_path)
                else:
                    container_path = host_path

                try:
                    if os.path.exists(container_path):
                        plexcache_tracked += os.path.getsize(container_path)
                        cached_files.append(container_path)  # Return container paths for eviction
                except (OSError, FileNotFoundError):
                    pass
        except Exception as e:
            logging.warning(f"Error reading exclude file: {e}")
            return (0, [])

        return (plexcache_tracked, cached_files)

    def _apply_cache_limit(self, media_files: List[str], cache_dir: str) -> List[str]:
        """Apply cache size limit and min free space, filtering out files that would exceed limits.

        Returns the list of files that fit within the most restrictive constraint.
        Files are prioritized in the order they appear (OnDeck items should come first).
        """
        cache_limit_bytes, limit_readable = self._get_effective_cache_limit(cache_dir)
        min_free_bytes, min_free_readable = self._get_effective_min_free_space(cache_dir)

        # No constraints set
        if cache_limit_bytes == 0 and min_free_bytes == 0:
            return media_files

        # Get total cache drive usage (use manual override if configured)
        try:
            drive_size_override = self.config_manager.cache.cache_drive_size_bytes
            disk_usage = get_disk_usage(cache_dir, drive_size_override)
            drive_usage_bytes = disk_usage.used
            drive_usage_gb = drive_usage_bytes / (1024**3)
        except Exception as e:
            logging.warning(f"Could not determine cache drive usage: {e}, skipping limit check")
            return media_files

        if cache_limit_bytes > 0:
            logging.info(f"Cache limit: {limit_readable}")
        if min_free_bytes > 0:
            logging.info(f"Min free space: {min_free_readable}")
        logging.info(f"Cache drive usage: {drive_usage_gb:.2f}GB (free: {disk_usage.free / (1024**3):.2f}GB)")

        # Calculate available space from each constraint
        available_space = None
        bottleneck = None

        if cache_limit_bytes > 0:
            cache_limit_available = cache_limit_bytes - drive_usage_bytes
            available_space = cache_limit_available
            bottleneck = "cache_limit"

        if min_free_bytes > 0:
            min_free_available = disk_usage.free - min_free_bytes
            if available_space is None or min_free_available < available_space:
                available_space = min_free_available
                bottleneck = "min_free_space"

        if available_space is None:
            return media_files

        # Log which constraint is the bottleneck when both are active
        if cache_limit_bytes > 0 and min_free_bytes > 0:
            logging.info(f"Active constraint: {bottleneck.replace('_', ' ')} (available: {available_space / (1024**3):.2f}GB)")

        if available_space <= 0:
            if bottleneck == "min_free_space":
                logging.warning(f"Drive free space ({disk_usage.free / (1024**3):.2f}GB) is at or below min free space floor ({min_free_readable})")
            else:
                logging.warning(f"Cache drive already at or over limit ({drive_usage_gb:.2f}GB used, limit is {limit_readable})")
            return []
        files_to_cache = []
        skipped_count = 0
        skipped_size = 0
        inaccessible_files = []

        for file in media_files:
            try:
                file_size = os.path.getsize(file)
                if file_size <= available_space:
                    files_to_cache.append(file)
                    available_space -= file_size
                else:
                    skipped_count += 1
                    skipped_size += file_size
            except (OSError, FileNotFoundError) as e:
                # File doesn't exist or can't be accessed - track for logging
                inaccessible_files.append((file, str(e)))

        if inaccessible_files:
            logging.warning(f"Could not access {len(inaccessible_files)} files for caching (path not found or permission denied):")
            for file, error in inaccessible_files:
                logging.warning(f"  {os.path.basename(file)}: {error}")
                logging.debug(f"  Full path: {file}")

        if skipped_count > 0:
            skipped_gb = skipped_size / (1024**3)
            logging.warning(f"Cache limit reached: skipped {skipped_count} files ({skipped_gb:.2f}GB) that would exceed the {limit_readable} limit")

        return files_to_cache

    def _estimate_priority(self, file_path: str, source: str) -> int:
        """Estimate priority score for a file before it's cached.

        Uses tracker data to calculate an accurate priority estimate, matching
        the logic in CachePriorityManager.calculate_priority() as closely as possible.

        Args:
            file_path: Path to the media file.
            source: Source type ('ondeck', 'watchlist', or 'unknown').

        Returns:
            Estimated priority score (0-100).
        """
        score = 50  # Base score

        # Factor 1: Source Type (+15 for ondeck)
        is_ondeck = source == "ondeck"
        if is_ondeck:
            score += 15

        # Get tracker entries for user count and age info
        ondeck_entry = self.ondeck_tracker.get_entry(file_path)
        watchlist_entry = self.watchlist_tracker.get_entry(file_path)

        # Factor 2: User Count (+5 per user, max +15)
        user_count = 0
        if ondeck_entry:
            user_count = len(ondeck_entry.get('users', []))
        if watchlist_entry:
            watchlist_users = len(watchlist_entry.get('users', []))
            user_count = max(user_count, watchlist_users)

        # Default to 1 user if not in trackers yet (conservative estimate)
        if user_count == 0:
            user_count = 1

        score += min(user_count * 5, 15)

        # Factor 3: Cache Recency - skip this, file isn't cached yet
        # When cached, it will get +5 (fresh), so we could add it optimistically
        # But being conservative, we don't add it here

        # Factor 4: Watchlist Age (+10 fresh, 0 if >30 days, -10 if >60 days)
        if watchlist_entry and watchlist_entry.get('watchlisted_at'):
            try:
                watchlisted_at = datetime.fromisoformat(watchlist_entry['watchlisted_at'])
                days_on_watchlist = (datetime.now() - watchlisted_at).days
                if days_on_watchlist < 7:
                    score += 10  # Fresh watchlist item
                elif days_on_watchlist > 60:
                    score -= 10  # Very old, likely forgotten
            except (ValueError, TypeError):
                pass

        # Factor 5: OnDeck Staleness (+5 if fresh, decay over time)
        if is_ondeck and ondeck_entry:
            first_seen_str = ondeck_entry.get('first_seen')
            if first_seen_str:
                try:
                    first_seen = datetime.fromisoformat(first_seen_str)
                    days_on_ondeck = (datetime.now() - first_seen).days
                    if days_on_ondeck < 7:
                        score += 5   # Fresh - just added to OnDeck
                    elif days_on_ondeck < 14:
                        pass         # Normal - no adjustment
                    elif days_on_ondeck < 30:
                        score -= 5   # Getting stale
                    else:
                        score -= 10  # Stale - on OnDeck for 30+ days
                except (ValueError, TypeError):
                    pass

        # Factor 6: Episode Position - skip for estimation (complex to determine)
        # This could add up to +15 for current episode, but we skip it for simplicity

        # Clamp to 0-100 range
        return max(0, min(100, score))

    def _filter_low_priority_files(self, media_files: List[str], source_map: dict) -> List[str]:
        """Filter out files that would score below eviction_min_priority.

        ALWAYS filters regardless of current drive usage to prevent cache thrashing.
        A file that scores below the eviction threshold will eventually be evicted,
        so there's no point caching it (wastes I/O copying files that will be deleted).

        This prevents the oscillation pattern where:
        1. Drive dips below threshold → files cached without filtering
        2. Drive goes over threshold → files evicted
        3. Repeat forever

        Args:
            media_files: List of file paths to potentially cache.
            source_map: Dict mapping file paths to source ('ondeck' or 'watchlist').

        Returns:
            Filtered list of files with estimated priority >= eviction_min_priority.
        """
        eviction_mode = self.config_manager.cache.cache_eviction_mode
        if eviction_mode == "none":
            return media_files  # No eviction, no filtering needed

        cache_dir = self.config_manager.paths.cache_dir
        if not cache_dir:
            # Try to get from path mappings
            for mapping in self.config_manager.paths.path_mappings:
                if mapping.enabled and mapping.cacheable and mapping.cache_path:
                    cache_dir = mapping.cache_path
                    break

        if not cache_dir:
            return media_files

        # Check if drive is over eviction threshold
        cache_limit_bytes, _ = self._get_effective_cache_limit(cache_dir)
        if cache_limit_bytes == 0:
            return media_files

        threshold_percent = self.config_manager.cache.cache_eviction_threshold_percent
        threshold_bytes = cache_limit_bytes * threshold_percent / 100

        try:
            drive_size_override = self.config_manager.cache.cache_drive_size_bytes
            disk_usage = get_disk_usage(cache_dir, drive_size_override)
            total_drive_usage = disk_usage.used
        except Exception:
            total_drive_usage = 0  # Can't check drive usage, but still filter by priority

        is_over_threshold = total_drive_usage >= threshold_bytes

        # ALWAYS filter out files that would score below eviction_min_priority.
        # This prevents cache thrashing: caching a file only to evict it later wastes I/O.
        # Even when under threshold, don't cache files that would be evicted once threshold is hit.
        eviction_min_priority = self.config_manager.cache.eviction_min_priority
        filtered_files = []
        skipped_count = 0
        skipped_by_source = {"ondeck": 0, "watchlist": 0, "unknown": 0}

        for file_path in media_files:
            source = source_map.get(file_path, "unknown")

            # Use accurate priority estimation based on tracker data
            estimated_priority = self._estimate_priority(file_path, source)

            if estimated_priority >= eviction_min_priority:
                filtered_files.append(file_path)
            else:
                skipped_count += 1
                skipped_by_source[source] = skipped_by_source.get(source, 0) + 1
                logging.debug(f"Skipping cache (priority {estimated_priority} < {eviction_min_priority}): {os.path.basename(file_path)}")

        if skipped_count > 0:
            # Build a readable breakdown
            breakdown_parts = []
            for src, count in skipped_by_source.items():
                if count > 0:
                    breakdown_parts.append(f"{count} {src}")
            breakdown = ", ".join(breakdown_parts)

            if is_over_threshold:
                logging.warning(
                    f"Cache over eviction threshold ({threshold_percent}%): skipped {skipped_count} files "
                    f"that would be immediately evicted ({breakdown}). "
                    f"Increase 'Eviction Threshold' or lower 'Minimum Priority to Keep' in Settings."
                )
            else:
                logging.info(
                    f"Skipped {skipped_count} files below minimum priority ({eviction_min_priority}): {breakdown}. "
                    f"These would be evicted when threshold is reached."
                )

        return filtered_files

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

        # Check if we need to evict based on TOTAL drive usage (not just tracked files)
        # This ensures eviction triggers even when non-PlexCache files fill the drive
        threshold_percent = self.config_manager.cache.cache_eviction_threshold_percent
        threshold_bytes = cache_limit_bytes * threshold_percent / 100

        # Get actual total drive usage (use manual override if configured)
        try:
            drive_size_override = self.config_manager.cache.cache_drive_size_bytes
            disk_usage = get_disk_usage(cache_dir, drive_size_override)
            total_drive_usage = disk_usage.used
        except Exception:
            total_drive_usage = plexcache_tracked  # Fallback if can't get disk usage

        if total_drive_usage < threshold_bytes and needed_space_bytes == 0:
            logging.debug(f"Cache usage ({total_drive_usage/1e9:.1f}GB) below threshold ({threshold_bytes/1e9:.1f}GB), skipping eviction")
            return (0, 0)

        # Calculate how much space to free based on total drive usage
        space_to_free = max(needed_space_bytes, total_drive_usage - threshold_bytes)
        if space_to_free <= 0:
            return (0, 0)

        if needed_space_bytes > 0:
            logging.info(f"Smart eviction: drive over limit, need to free {space_to_free/1e9:.2f}GB")
        else:
            logging.info(f"Smart eviction: drive usage ({total_drive_usage/1e9:.1f}GB) over threshold ({threshold_bytes/1e9:.1f}GB), need to free {space_to_free/1e9:.2f}GB")
            logging.debug(f"PlexCache-tracked: {plexcache_tracked/1e9:.1f}GB, Other files: {(total_drive_usage-plexcache_tracked)/1e9:.1f}GB")

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

        # Filter out files that are about to be cached (prevents evict-then-recache loop)
        # Convert media_to_cache paths to cache paths for comparison
        files_to_cache_set = set()
        for f in self.media_to_cache:
            # media_to_cache contains array paths (/mnt/user/...), convert to cache paths
            if self.file_path_modifier:
                cache_path, _ = self.file_path_modifier.convert_real_to_cache(f)
                if cache_path:
                    files_to_cache_set.add(cache_path)

        original_count = len(candidates)
        candidates = [c for c in candidates if c not in files_to_cache_set]
        if len(candidates) < original_count:
            skipped = original_count - len(candidates)
            logging.debug(f"Skipped {skipped} eviction candidate(s) that would be immediately re-cached")

        if not candidates:
            logging.info("No eviction candidates after filtering (all would be re-cached)")
            return (0, 0)

        # Check if candidates can free enough space
        candidate_bytes = sum(os.path.getsize(f) for f in candidates if os.path.exists(f))
        if candidate_bytes < space_to_free:
            logging.warning(f"Can only evict {candidate_bytes/1e9:.1f}GB of {space_to_free/1e9:.1f}GB needed - non-PlexCache files may be filling the drive")

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

        for cache_path in candidates:
            try:
                file_size = os.path.getsize(cache_path) if os.path.exists(cache_path) else 0

                # Find the correct real_path for this cache_path using path_mappings
                array_path = None
                if self.config_manager.paths.path_mappings:
                    for mapping in self.config_manager.paths.path_mappings:
                        if mapping.enabled and mapping.cache_path and cache_path.startswith(mapping.cache_path):
                            # Found matching mapping - convert cache path to array path
                            array_path = cache_path.replace(mapping.cache_path, mapping.real_path, 1)
                            break

                # Fallback to legacy single-path mode
                if not array_path and self.config_manager.paths.real_source:
                    array_path = cache_path.replace(cache_dir, self.config_manager.paths.real_source, 1)

                if not array_path:
                    logging.warning(f"Could not determine array path for: {cache_path}")
                    continue

                plexcached_path = array_path + ".plexcached"
                logging.debug(f"Looking for backup at: {plexcached_path}")

                # Track whether we successfully restored/created array copy
                array_restored = False

                if os.path.exists(plexcached_path):
                    # Exact match: rename .plexcached back to original
                    os.rename(plexcached_path, array_path)
                    logging.debug(f"Restored .plexcached: {array_path}")
                    array_restored = True
                else:
                    # Check for upgrade scenario: old .plexcached with different filename but same media identity
                    # This handles cases where Radarr/Sonarr upgraded the file while it was cached
                    array_dir = os.path.dirname(array_path)
                    cache_identity = get_media_identity(cache_path)
                    old_plexcached = find_matching_plexcached(array_dir, cache_identity, cache_path)

                    if old_plexcached:
                        # Found an old backup with same media identity - this is an upgrade scenario
                        # IMPORTANT: Copy upgraded file FIRST, then delete old backup
                        old_name = os.path.basename(old_plexcached).replace(".plexcached", "")
                        new_name = os.path.basename(cache_path)
                        logging.info(f"Eviction upgrade detected: {old_name} -> {new_name}")

                        # Copy the upgraded cache file to array BEFORE deleting old backup
                        # CRITICAL: Copy to /mnt/user0/ (array direct), NOT /mnt/user/ (FUSE)
                        # If we copy to /mnt/user/, Unraid's cache policy may put the file
                        # back on cache (if shareUseCache=yes), causing data loss
                        array_direct_path = get_array_direct_path(array_path)
                        if os.path.exists(cache_path):
                            import shutil
                            try:
                                array_direct_dir = os.path.dirname(array_direct_path)
                                os.makedirs(array_direct_dir, exist_ok=True)
                                shutil.copy2(cache_path, array_direct_path)
                                logging.debug(f"Copied upgraded file to array: {array_direct_path}")
                                # Verify copy succeeded using array-direct path
                                if os.path.exists(array_direct_path):
                                    cache_size = os.path.getsize(cache_path)
                                    array_size = os.path.getsize(array_direct_path)
                                    if cache_size == array_size:
                                        array_restored = True
                                        # NOW safe to delete old backup
                                        os.remove(old_plexcached)
                                        logging.debug(f"Deleted old .plexcached: {old_plexcached}")
                                    else:
                                        logging.error(f"Size mismatch after copy! Cache: {cache_size}, Array: {array_size}. Skipping eviction.")
                                        os.remove(array_direct_path)  # Remove failed copy
                                        continue
                            except OSError as e:
                                logging.error(f"Failed to copy to array during eviction: {e}. Skipping eviction.")
                                continue
                    else:
                        # No .plexcached backup found - check if array file truly exists
                        # CRITICAL: Use /mnt/user0/ (array direct) NOT /mnt/user/ (user share)
                        # On Unraid's FUSE, /mnt/user/ shows files from cache too, which causes
                        # false positives - we'd think array has the file when it's only on cache!
                        array_direct_path = get_array_direct_path(array_path)

                        if not os.path.exists(array_direct_path):
                            # No backup exists and no array copy - must copy from cache first
                            logging.warning(f"No .plexcached backup found for: {os.path.basename(cache_path)}")
                            if os.path.exists(cache_path):
                                import shutil
                                try:
                                    # CRITICAL: Copy to /mnt/user0/ (array direct), NOT /mnt/user/ (FUSE)
                                    # If we copy to /mnt/user/, Unraid's cache policy may put the file
                                    # back on cache (if shareUseCache=yes), causing data loss
                                    array_direct_dir = os.path.dirname(array_direct_path)
                                    os.makedirs(array_direct_dir, exist_ok=True)
                                    shutil.copy2(cache_path, array_direct_path)
                                    logging.info(f"Created array copy before eviction: {os.path.basename(array_direct_path)}")
                                    # Verify copy succeeded using array-direct path
                                    if os.path.exists(array_direct_path):
                                        cache_size = os.path.getsize(cache_path)
                                        array_size = os.path.getsize(array_direct_path)
                                        if cache_size == array_size:
                                            array_restored = True
                                        else:
                                            logging.error(f"Size mismatch! Skipping eviction to prevent data loss.")
                                            os.remove(array_direct_path)
                                            continue
                                    else:
                                        logging.error(f"Copy appeared to succeed but file not found on array. Skipping eviction.")
                                        continue
                                except OSError as e:
                                    logging.error(f"Failed to copy to array: {e}. Skipping eviction to prevent data loss.")
                                    continue
                            else:
                                logging.error(f"Cannot evict - no backup and cache file missing: {cache_path}")
                                continue
                        else:
                            # Array file truly exists on array (verified via /mnt/user0/)
                            logging.debug(f"Array file exists (verified via array-direct path): {array_path}")
                            array_restored = True

                # CRITICAL: Only delete cache copy if array copy is confirmed
                if not array_restored:
                    logging.error(f"Skipping cache deletion - array copy not confirmed: {os.path.basename(cache_path)}")
                    continue

                if os.path.exists(cache_path):
                    # Check for hardlinks before deleting
                    try:
                        stat_info = os.stat(cache_path)
                        if stat_info.st_nlink > 1:
                            logging.debug(f"File has {stat_info.st_nlink} hardlinks, deleting won't free space: {os.path.basename(cache_path)}")
                    except OSError as e:
                        logging.warning(f"Cannot stat cache file, skipping eviction: {os.path.basename(cache_path)}: {e}")
                        continue
                    os.remove(cache_path)
                    logging.debug(f"Deleted cache file: {os.path.basename(cache_path)}")
                else:
                    logging.debug(f"Cache file already gone: {os.path.basename(cache_path)}")

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

        # Note: Smart eviction now runs earlier in _move_files() before filtering
        # This ensures eviction happens after array moves but before low-priority filtering

        # Apply cache size limit when moving to cache
        if destination == 'cache':
            media_files_filtered = self._apply_cache_limit(media_files_filtered, cache_dir)

        total_size, total_size_unit = self.file_utils.get_total_size_of_files(media_files_filtered)
        
        if total_size > 0:
            logging.debug(f"Moving {total_size:.2f} {total_size_unit} to {destination}")
            # Generate summary message with restore vs move separation for array moves
            # Use conditional wording for dry run mode
            would_prefix = "[DRY RUN] Would have " if self.dry_run else ""
            if destination == 'array':
                parts = []
                if self.restored_count > 0:
                    unit = "episode" if self.restored_count == 1 else "episodes"
                    size_gb = self.restored_bytes / (1024**3)
                    verb = "return" if self.dry_run else "Returned"
                    parts.append(f"{would_prefix}{verb} {self.restored_count} {unit} ({size_gb:.2f} GB) to array")
                if self.moved_to_array_count > 0:
                    unit = "episode" if self.moved_to_array_count == 1 else "episodes"
                    size_gb = self.moved_to_array_bytes / (1024**3)
                    verb = "copy" if self.dry_run else "Copied"
                    parts.append(f"{would_prefix}{verb} {self.moved_to_array_count} {unit} ({size_gb:.2f} GB) to array")
                if parts:
                    self.logging_manager.add_summary_message(', '.join(parts))
                else:
                    verb = "move" if self.dry_run else "Moved"
                    self.logging_manager.add_summary_message(
                        f"{would_prefix}{verb} {total_size:.2f} {total_size_unit} to {destination}"
                    )
            else:
                # Track cached bytes for summary
                size_multipliers = {'KB': 1024, 'MB': 1024**2, 'GB': 1024**3, 'TB': 1024**4}
                self.cached_bytes = int(total_size * size_multipliers.get(total_size_unit, 1))
                verb = "cache" if self.dry_run else "Cached"
                self.logging_manager.add_summary_message(
                    f"{would_prefix}{verb} {total_size:.2f} {total_size_unit}"
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
            # Pass files_to_skip to prevent removing active session files from exclude list
            files_to_move_back, stale_entries, move_back_exclude_paths = self.file_filter.get_files_to_move_back_to_array(
                current_ondeck_items, current_watchlist_items, set(self.files_to_skip)
            )

            if files_to_move_back:
                logging.debug(f"Found {len(files_to_move_back)} files to move back to array")
                self.media_to_array.extend(files_to_move_back)

            # Clean up stale entries immediately (files already gone from cache — safe to remove)
            # Skip in dry-run mode to avoid modifying tracking files
            if stale_entries and not self.dry_run:
                self.file_filter.remove_files_from_exclude_list(stale_entries)
            elif stale_entries and self.dry_run:
                logging.debug(f"[DRY RUN] Would remove {len(stale_entries)} stale entries from exclude list")

            # Store move-back exclude paths for deferred removal after moves succeed (issue #13)
            # These entries stay protected in the exclude list until the file is confirmed moved
            self._move_back_exclude_paths = move_back_exclude_paths
            if move_back_exclude_paths:
                logging.debug(f"Deferred {len(move_back_exclude_paths)} exclude entries pending successful array moves")
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
            had_warnings=False,
            dry_run=self.dry_run
        )

        self.logging_manager.log_summary()

        # Note: Empty folder cleanup now happens immediately during file operations
        # (per File and Folder Management Policy) - no blanket cleanup needed here

        # Clean up stale timestamp entries for files that no longer exist
        # Skip in dry-run mode to avoid modifying tracking files
        if hasattr(self, 'timestamp_tracker') and self.timestamp_tracker and not self.dry_run:
            self.timestamp_tracker.cleanup_missing_files()

        # Clean up stale watchlist tracker entries
        # Skip in dry-run mode to avoid modifying tracking files
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
    mover_exclude = config_manager.get_cached_files_file()
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
