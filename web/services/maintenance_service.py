"""Maintenance service - cache audit and fix actions"""

import json
import logging
import os
import shutil
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from datetime import datetime
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, Optional, Set, Any, Tuple

from web.config import PROJECT_ROOT, DATA_DIR, CONFIG_DIR, SETTINGS_FILE
from core.system_utils import get_array_direct_path, format_bytes, translate_container_to_host_path, translate_host_to_container_path, remove_from_exclude_file, remove_from_timestamps_file
from core.file_operations import PLEXCACHED_EXTENSION

# Common media extensions for validation
_MEDIA_EXTENSIONS = {
    '.mkv', '.mp4', '.avi', '.mov', '.wmv', '.flv', '.m4v', '.ts',
    '.mpg', '.mpeg', '.webm', '.ogv', '.3gp', '.divx', '.vob',
}


def _strip_plexcached(path: str) -> str:
    """Safely strip .plexcached suffix from a path.

    Returns the original path (with media extension intact).
    Raises ValueError if the result would lack a media extension,
    which indicates a malformed .plexcached file.
    """
    if not path.endswith(PLEXCACHED_EXTENSION):
        raise ValueError(f"Not a .plexcached file: {path}")
    original = path[:-len(PLEXCACHED_EXTENSION)]
    _, ext = os.path.splitext(original)
    if ext.lower() not in _MEDIA_EXTENSIONS:
        raise ValueError(
            f"Malformed .plexcached file (no media extension): {os.path.basename(path)}"
        )
    return original


@dataclass
class UnprotectedFile:
    """A cache file not in the exclude list (at risk from Unraid mover)"""
    cache_path: str
    filename: str
    size: int
    size_display: str
    has_plexcached_backup: bool
    backup_path: Optional[str]
    has_array_duplicate: bool
    array_path: Optional[str]
    recommended_action: str  # "fix_with_backup", "sync_to_array", "add_to_exclude"
    created_at: Optional[datetime] = None
    age_days: float = 0.0
    has_invalid_timestamp: bool = False  # True if file has future date or very old date


@dataclass
class OrphanedBackup:
    """.plexcached file on array with no corresponding cache file or redundant backup"""
    plexcached_path: str
    original_filename: str
    size: int
    size_display: str
    restore_path: str
    backup_type: str = "orphaned"  # "orphaned", "redundant", "superseded", or "malformed"
    # "orphaned" = no cache file AND no original on array (needs restore or delete)
    # "redundant" = no cache file BUT original exists on array (safe to delete)
    # "superseded" = old backup replaced by upgraded version on cache (safe to delete after review)
    # "malformed" = .plexcached without valid media extension (delete only — cannot restore)
    replacement_file: Optional[str] = None  # Path to the replacement file (for superseded backups)


@dataclass
class DuplicateFile:
    """File existing on both cache AND array"""
    cache_path: str
    array_path: str
    filename: str
    size: int
    size_display: str


@dataclass
class AuditResults:
    """Complete audit results"""
    cache_file_count: int
    exclude_entry_count: int
    timestamp_entry_count: int

    # Issues
    unprotected_files: List[UnprotectedFile] = field(default_factory=list)
    orphaned_plexcached: List[OrphanedBackup] = field(default_factory=list)
    stale_exclude_entries: List[str] = field(default_factory=list)
    stale_timestamp_entries: List[str] = field(default_factory=list)
    duplicates: List[DuplicateFile] = field(default_factory=list)

    # Health status
    health_status: str = "healthy"  # "healthy", "warnings", "critical"

    def calculate_health_status(self):
        """Calculate overall health status based on issues.

        Untracked files are informational (mover handles them naturally)
        and do NOT affect health status.
        """
        # Only truly orphaned backups are critical (not superseded or redundant)
        truly_orphaned = [b for b in self.orphaned_plexcached if b.backup_type == "orphaned"]
        # Critical: truly orphaned backups need immediate attention
        if truly_orphaned:
            self.health_status = "critical"
        # Warnings: stale entries need cleanup, superseded/redundant backups can be cleaned
        elif self.stale_exclude_entries or self.stale_timestamp_entries or self.orphaned_plexcached:
            self.health_status = "warnings"
        else:
            self.health_status = "healthy"

    @property
    def total_issues(self) -> int:
        """Count of issues needing attention (excludes untracked files which are informational)"""
        return (len(self.orphaned_plexcached) +
                len(self.stale_exclude_entries) +
                len(self.stale_timestamp_entries))


@dataclass
class ActionResult:
    """Result of a fix action"""
    success: bool
    message: str
    affected_count: int = 0
    errors: List[str] = field(default_factory=list)
    affected_paths: List[str] = field(default_factory=list)


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


class MaintenanceService:
    """Service for cache auditing and maintenance actions"""

    # Video extensions
    VIDEO_EXTENSIONS = ('.mkv', '.mp4', '.avi', '.m4v', '.mov', '.wmv', '.ts')
    # Subtitle extensions
    SUBTITLE_EXTENSIONS = ('.srt', '.sub', '.idx', '.ass', '.ssa', '.vtt', '.smi')
    # Chunk size for copy progress reporting (4 MB)
    _COPY_CHUNK_SIZE = 4 * 1024 * 1024

    def __init__(self):
        # Use CONFIG_DIR and DATA_DIR for Docker compatibility
        self.settings_file = SETTINGS_FILE
        self.exclude_file = CONFIG_DIR / "plexcache_cached_files.txt"
        self.timestamps_file = DATA_DIR / "timestamps.json"
        self._cache_dirs: List[str] = []
        self._array_dirs: List[str] = []
        self._settings: Dict = {}

    def _load_settings(self) -> Dict:
        """Load settings from plexcache_settings.json"""
        if self._settings:
            return self._settings

        if not self.settings_file.exists():
            return {}

        try:
            with open(self.settings_file, 'r', encoding='utf-8') as f:
                self._settings = json.load(f)
            return self._settings
        except (json.JSONDecodeError, IOError):
            return {}

    def _translate_host_to_container_path(self, path: str) -> str:
        """Translate host cache path to container path."""
        settings = self._load_settings()
        return translate_host_to_container_path(path, settings.get('path_mappings', []))

    def _translate_container_to_host_path(self, path: str) -> str:
        """Translate container cache path to host path for exclude file."""
        settings = self._load_settings()
        return translate_container_to_host_path(path, settings.get('path_mappings', []))

    def _should_skip_directory(self, dirname: str) -> bool:
        """Check if directory should be skipped during scanning.

        Always skips hidden directories (dot-prefixed like .Trash, .Recycle.Bin).
        Also skips user-configured excluded folders from settings.
        """
        # Always skip hidden directories (dot-prefixed)
        if dirname.startswith('.'):
            return True
        # Check user-configured excluded folders
        settings = self._load_settings()
        excluded = settings.get('excluded_folders', [])
        return dirname in excluded

    def _get_paths(self) -> tuple:
        """Get cache and array directory paths from settings"""
        if self._cache_dirs and self._array_dirs:
            return self._cache_dirs, self._array_dirs

        settings = self._load_settings()
        cache_dirs = []
        array_dirs = []

        path_mappings = settings.get('path_mappings', [])

        if path_mappings:
            for mapping in path_mappings:
                if not mapping.get('enabled', True):
                    continue

                cache_path = mapping.get('cache_path', '').rstrip('/\\') if mapping.get('cache_path') else ''
                real_path = mapping.get('real_path', '').rstrip('/\\')

                if mapping.get('cacheable', True) and cache_path and real_path:
                    # Convert real_path to array-direct path (ZFS-aware)
                    array_path = get_array_direct_path(real_path)
                    cache_dirs.append(cache_path)
                    array_dirs.append(array_path)
        else:
            # Legacy single-path mode
            cache_dir = settings.get('cache_dir', '').rstrip('/\\')
            real_source = settings.get('real_source', '').rstrip('/\\')
            nas_library_folders = settings.get('nas_library_folders', [])

            if cache_dir and real_source and nas_library_folders:
                array_source = get_array_direct_path(real_source)
                for folder in nas_library_folders:
                    folder = folder.strip('/\\')
                    cache_dirs.append(os.path.join(cache_dir, folder))
                    array_dirs.append(os.path.join(array_source, folder))

        self._cache_dirs = cache_dirs
        self._array_dirs = array_dirs
        return cache_dirs, array_dirs


    def _copy_with_progress(self, src: str, dst: str,
                             bytes_progress_callback: Optional[Callable] = None) -> None:
        """Copy file with chunked progress reporting, preserving metadata like shutil.copy2."""
        file_size = os.path.getsize(src)
        if bytes_progress_callback:
            bytes_progress_callback(0, file_size)

        copied = 0
        with open(src, 'rb') as fsrc, open(dst, 'wb') as fdst:
            while True:
                chunk = fsrc.read(self._COPY_CHUNK_SIZE)
                if not chunk:
                    break
                fdst.write(chunk)
                copied += len(chunk)
                if bytes_progress_callback:
                    bytes_progress_callback(copied, file_size)

        # Preserve file metadata (timestamps, permissions) like copy2
        shutil.copystat(src, dst)

    def _run_parallel(
        self,
        items: list,
        worker_fn: Callable,
        max_workers: int,
        stop_check: Optional[Callable[[], bool]] = None,
        progress_callback: Optional[Callable] = None,
        active_callback: Optional[Callable] = None,
    ) -> List[Tuple[str, bool, Optional[str]]]:
        """Run worker_fn on each item in parallel with throttled submission.

        Mirrors the throttled ThreadPoolExecutor pattern from FileMover._execute_move_commands.
        Only max_workers tasks are in-flight at once; 1-second timeout on wait() for responsive
        stop checking.

        Args:
            items: List of items to process.
            worker_fn: Callable(item) -> (path, success, error_msg).
            max_workers: Number of concurrent workers.
            stop_check: Optional callable returning True to request stop.
            progress_callback: Optional (completed_count, total, filename) callback.
            active_callback: Optional callback receiving list of in-flight basenames.

        Returns:
            List of (path, success, error_msg) tuples.
        """
        results: List[Tuple[str, bool, Optional[str]]] = []
        completed_count = 0
        total = len(items)
        future_to_item: dict = {}       # Map future -> original item path
        active_basenames: list = []     # Ordered list of in-flight basenames

        def _notify_active():
            if active_callback:
                active_callback(list(active_basenames))

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            pending: set = set()
            item_iter = iter(items)
            all_submitted = False
            stopped = False

            while True:
                # Check for stop request
                if stop_check and stop_check():
                    stopped = True
                    for f in pending:
                        f.cancel()
                    break

                # Submit new tasks up to max_workers
                while not all_submitted and len(pending) < max_workers:
                    try:
                        item = next(item_iter)
                        future = executor.submit(worker_fn, item)
                        pending.add(future)
                        future_to_item[future] = item
                        active_basenames.append(os.path.basename(item))
                        _notify_active()
                    except StopIteration:
                        all_submitted = True
                        break

                if not pending:
                    break

                # Wait for at least one task to complete (1s timeout for stop checks)
                done, pending = wait(pending, timeout=1.0, return_when=FIRST_COMPLETED)
                for future in done:
                    # Remove from active tracking
                    item_path = future_to_item.pop(future, "")
                    basename = os.path.basename(item_path) if item_path else ""
                    if basename in active_basenames:
                        active_basenames.remove(basename)

                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as e:
                        results.append(("", False, str(e)))
                    completed_count += 1
                    if progress_callback:
                        path = results[-1][0] or ""
                        progress_callback(
                            completed_count, total,
                            os.path.basename(path) if path else ""
                        )

                # Notify after processing all done futures
                if done:
                    _notify_active()

            # Wait for in-progress tasks on stop (up to 30s safety timeout)
            if stopped and pending:
                active_basenames.clear()
                _notify_active()
                done, still_pending = wait(pending, timeout=30.0)
                for future in done:
                    try:
                        results.append(future.result())
                    except Exception as e:
                        results.append(("", False, str(e)))
                for future in still_pending:
                    results.append(("", False, "Timed out waiting for task"))

        return results

    def get_cache_files(self) -> Set[str]:
        """Get all media files currently on cache"""
        cache_dirs, _ = self._get_paths()
        cache_files = set()
        extensions = self.VIDEO_EXTENSIONS + self.SUBTITLE_EXTENSIONS

        def _walk_error(err):
            logging.warning(f"Permission error scanning directory: {err}")

        for cache_dir in cache_dirs:
            if os.path.exists(cache_dir):
                for root, dirs, files in os.walk(cache_dir, onerror=_walk_error):
                    # Prune excluded directories (modifying dirs in-place skips them)
                    dirs[:] = [d for d in dirs if not self._should_skip_directory(d)]
                    for f in files:
                        if f.lower().endswith(extensions):
                            cache_files.add(os.path.join(root, f))

        return cache_files

    def get_exclude_files(self) -> Set[str]:
        """Get all files in exclude list (translated to container paths for comparison)"""
        exclude_files = set()
        if self.exclude_file.exists():
            try:
                with open(self.exclude_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            # Translate host paths back to container paths for comparison
                            container_path = self._translate_host_to_container_path(line)
                            exclude_files.add(container_path)
            except IOError:
                pass
        return exclude_files

    def get_timestamp_files(self) -> Set[str]:
        """Get all files in timestamps"""
        timestamp_files = set()
        if self.timestamps_file.exists():
            try:
                with open(self.timestamps_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    timestamp_files = set(data.keys())
            except (json.JSONDecodeError, IOError):
                pass
        return timestamp_files

    def _cache_to_array_path(self, cache_file: str) -> Optional[str]:
        """Convert a cache file path to its corresponding array path"""
        cache_dirs, array_dirs = self._get_paths()
        for i, cache_dir in enumerate(cache_dirs):
            if cache_file.startswith(cache_dir):
                return cache_file.replace(cache_dir, array_dirs[i], 1)
        return None

    def _check_plexcached_backup(self, cache_file: str) -> tuple:
        """Check if a .plexcached backup exists on array for a cache file"""
        array_file = self._cache_to_array_path(cache_file)
        if not array_file:
            return False, None

        plexcached_file = array_file + ".plexcached"
        return os.path.exists(plexcached_file), plexcached_file

    def _check_array_duplicate(self, cache_file: str) -> tuple:
        """Check if the same file already exists on the array (duplicate)"""
        array_file = self._cache_to_array_path(cache_file)
        if not array_file:
            return False, None

        return os.path.exists(array_file), array_file

    def run_full_audit(self) -> AuditResults:
        """Run a complete audit and return all results"""
        cache_files = self.get_cache_files()
        exclude_files = self.get_exclude_files()
        timestamp_files = self.get_timestamp_files()

        results = AuditResults(
            cache_file_count=len(cache_files),
            exclude_entry_count=len(exclude_files),
            timestamp_entry_count=len(timestamp_files)
        )

        # Find unprotected files (on cache but not in exclude list)
        unprotected_paths = cache_files - exclude_files
        now = datetime.now()

        # Cutoff for "invalid" timestamps - before year 2000 or in the future
        min_valid_date = datetime(2000, 1, 1)

        for cache_path in unprotected_paths:
            filename = os.path.basename(cache_path)
            has_invalid_timestamp = False
            try:
                stat_info = os.stat(cache_path) if os.path.exists(cache_path) else None
                size = stat_info.st_size if stat_info else 0

                # Use st_ctime (change time) for age detection on Linux/Unraid.
                # Radarr/Sonarr preserve the original release mtime when downloading,
                # but st_ctime is updated when the file is created on this filesystem.
                # This gives us the actual "download time" not the release date.
                #
                # On Windows, st_ctime is creation time (also what we want).
                # Fall back to st_mtime if st_ctime is somehow unavailable.
                file_timestamp = stat_info.st_ctime if stat_info else None
                if not file_timestamp and stat_info:
                    file_timestamp = stat_info.st_mtime

                created_at = datetime.fromtimestamp(file_timestamp) if file_timestamp else None
                age_days = (now - created_at).total_seconds() / 86400 if created_at else 999

                # Detect invalid timestamps (future dates or very old dates)
                if created_at and (created_at > now or created_at < min_valid_date):
                    has_invalid_timestamp = True
                    age_days = 999  # Treat as unknown age
                elif age_days < 0:
                    has_invalid_timestamp = True
                    age_days = 999
            except OSError:
                size = 0
                created_at = None
                age_days = 999

            has_backup, backup_path = self._check_plexcached_backup(cache_path)
            has_dup, array_path = self._check_array_duplicate(cache_path)

            # Determine recommended action
            if has_backup or has_dup:
                recommended = "fix_with_backup"
            else:
                recommended = "sync_to_array"

            results.unprotected_files.append(UnprotectedFile(
                cache_path=cache_path,
                filename=filename,
                size=size,
                size_display=format_bytes(size),
                has_plexcached_backup=has_backup,
                backup_path=backup_path,
                has_array_duplicate=has_dup,
                array_path=array_path if has_dup else None,
                recommended_action=recommended,
                created_at=created_at,
                age_days=age_days,
                has_invalid_timestamp=has_invalid_timestamp
            ))

        # Sort unprotected files by filename (default)
        results.unprotected_files.sort(key=lambda f: f.filename.lower())

        # Find orphaned .plexcached files
        results.orphaned_plexcached = self._get_orphaned_plexcached()

        # Find stale exclude entries (in exclude but not on cache)
        results.stale_exclude_entries = sorted(list(exclude_files - cache_files))

        # Check if stale entries are actually media upgrades (Sonarr/Radarr file swaps)
        if results.stale_exclude_entries:
            try:
                from web.services.cache_service import get_cache_service
                cache_service = get_cache_service()
                upgrade_result = cache_service.check_for_upgrades(results.stale_exclude_entries)
                if upgrade_result.get("upgrades_resolved", 0) > 0:
                    # Recompute stale entries after upgrade resolution
                    exclude_files = self.get_exclude_files()
                    timestamp_files = self.get_timestamp_files()
                    results.stale_exclude_entries = sorted(list(exclude_files - cache_files))
            except Exception as e:
                logging.warning(f"Upgrade check during audit failed (non-fatal): {e}")

        # Find stale timestamp entries (in timestamps but not on cache)
        results.stale_timestamp_entries = sorted(list(timestamp_files - cache_files))

        # Find duplicates (files that exist on BOTH cache and array)
        for cache_path in cache_files:
            has_dup, array_path = self._check_array_duplicate(cache_path)
            if has_dup:
                filename = os.path.basename(cache_path)
                try:
                    size = os.path.getsize(cache_path)
                except OSError:
                    size = 0

                results.duplicates.append(DuplicateFile(
                    cache_path=cache_path,
                    array_path=array_path,
                    filename=filename,
                    size=size,
                    size_display=format_bytes(size)
                ))

        results.duplicates.sort(key=lambda f: f.size, reverse=True)

        # Calculate health status
        results.calculate_health_status()

        return results

    def _get_orphaned_plexcached(self) -> List[OrphanedBackup]:
        """Find .plexcached files on array that need cleanup.

        Returns three types of backups:
        - "orphaned": No cache file AND no original on array (needs restore or delete)
        - "redundant": No cache file BUT original exists on array (safe to delete)
        - "superseded": Old backup replaced by an upgraded version on cache (safe to delete after review)
        """
        cache_dirs, array_dirs = self._get_paths()
        cache_files = self.get_cache_files()
        backups_to_cleanup = []

        for i, array_dir in enumerate(array_dirs):
            if not os.path.exists(array_dir):
                continue

            cache_dir = cache_dirs[i]

            for root, dirs, files in os.walk(array_dir):
                # Prune excluded directories
                dirs[:] = [d for d in dirs if not self._should_skip_directory(d)]
                for f in files:
                    if f.endswith('.plexcached'):
                        plexcached_path = os.path.join(root, f)
                        try:
                            original_name = _strip_plexcached(f)
                        except ValueError:
                            # Malformed .plexcached (no media extension) — show in UI for deletion
                            logging.warning(f"Malformed .plexcached file (no media extension): {f}")
                            try:
                                size = os.path.getsize(plexcached_path)
                            except OSError:
                                size = 0
                            backups_to_cleanup.append(OrphanedBackup(
                                plexcached_path=plexcached_path,
                                original_filename=f,
                                size=size,
                                size_display=format_bytes(size),
                                restore_path="",
                                backup_type="malformed"
                            ))
                            continue
                        original_array_path = os.path.join(root, original_name)

                        # Find corresponding cache path
                        relative_path = os.path.relpath(original_array_path, array_dir)
                        cache_path = os.path.join(cache_dir, relative_path)
                        cache_directory = os.path.dirname(cache_path)

                        # Skip if file is still actively cached (has cache copy)
                        if cache_path in cache_files:
                            continue

                        # Check if original exists on array
                        original_exists = os.path.exists(original_array_path)

                        if original_exists:
                            # Redundant backup: original was restored but .plexcached not cleaned up
                            # Safe to delete - the original file is already on the array
                            try:
                                size = os.path.getsize(plexcached_path)
                            except OSError:
                                size = 0

                            backups_to_cleanup.append(OrphanedBackup(
                                plexcached_path=plexcached_path,
                                original_filename=original_name,
                                size=size,
                                size_display=format_bytes(size),
                                restore_path=original_array_path,
                                backup_type="redundant"
                            ))
                        else:
                            # Orphaned backup: no cache file AND no original
                            # Check if this backup has been superseded by a newer version
                            # (e.g., Sonarr/Radarr upgraded from HDTV to WEB-DL)
                            replacement = self._find_replacement_file(
                                original_name, cache_directory, cache_files
                            )

                            try:
                                size = os.path.getsize(plexcached_path)
                            except OSError:
                                size = 0

                            if replacement:
                                # Superseded backup - flag for user review instead of auto-deleting
                                # The upgraded file exists on cache, so this old backup is no longer needed
                                backups_to_cleanup.append(OrphanedBackup(
                                    plexcached_path=plexcached_path,
                                    original_filename=original_name,
                                    size=size,
                                    size_display=format_bytes(size),
                                    restore_path=original_array_path,
                                    backup_type="superseded",
                                    replacement_file=replacement
                                ))
                            else:
                                # Truly orphaned - no replacement exists
                                backups_to_cleanup.append(OrphanedBackup(
                                    plexcached_path=plexcached_path,
                                    original_filename=original_name,
                                    size=size,
                                    size_display=format_bytes(size),
                                    restore_path=original_array_path,
                                    backup_type="orphaned"
                                ))

        backups_to_cleanup.sort(key=lambda f: f.size, reverse=True)
        return backups_to_cleanup

    def _find_replacement_file(self, original_name: str, cache_directory: str,
                               cache_files: Set[str]) -> Optional[str]:
        """Check if a replacement file exists for a .plexcached backup.

        This detects Sonarr/Radarr upgrades where the old file was replaced
        with a newer/better quality version.

        Args:
            original_name: Original filename (without .plexcached suffix)
            cache_directory: The cache directory where the file would be
            cache_files: Set of all cache files

        Returns:
            Path to replacement file if found, None otherwise
        """
        import re

        # Extract the base pattern (show/movie name + episode info)
        # TV: "Show Name - S01E02 - Episode Title [quality]..." -> "Show Name - S01E02"
        # Movie: "Movie Name (2024) - [quality]..." -> "Movie Name (2024)"

        # Try TV show pattern first: "Name - S##E##"
        tv_match = re.match(r'^(.+ - S\d{2}E\d{2})', original_name)
        if tv_match:
            base_pattern = tv_match.group(1)
        else:
            # Try movie pattern: "Name (Year)" or just take everything before the first "["
            movie_match = re.match(r'^(.+?\(\d{4}\))', original_name)
            if movie_match:
                base_pattern = movie_match.group(1)
            else:
                # Fallback: everything before first " - [" or " ["
                bracket_match = re.match(r'^(.+?)(?:\s*-\s*\[|\s*\[)', original_name)
                if bracket_match:
                    base_pattern = bracket_match.group(1).strip()
                else:
                    return None  # Can't determine pattern

        # Look for files in the same cache directory that match the base pattern
        if not os.path.exists(cache_directory):
            return None

        for cache_file in cache_files:
            if cache_file.startswith(cache_directory + os.sep):
                cache_filename = os.path.basename(cache_file)
                # Check if it's a different file but same show/episode
                if cache_filename != original_name and cache_filename.startswith(base_pattern):
                    return cache_file

        return None

    def get_health_summary(self) -> Dict[str, Any]:
        """Get a quick health summary for dashboard widget"""
        results = self.run_full_audit()
        return {
            "status": results.health_status,
            "total_issues": results.total_issues,
            "unprotected_count": len(results.unprotected_files),
            "orphaned_count": len(results.orphaned_plexcached),
            "stale_exclude_count": len(results.stale_exclude_entries),
            "stale_timestamp_count": len(results.stale_timestamp_entries),
            "cache_files": results.cache_file_count,
            "protected_files": results.exclude_entry_count
        }

    # === Fix Actions ===

    def restore_plexcached(self, paths: List[str], dry_run: bool = True,
                            stop_check: Optional[Callable[[], bool]] = None,
                            progress_callback: Optional[Callable] = None,
                            bytes_progress_callback: Optional[Callable] = None,
                            max_workers: int = 1,
                            active_callback: Optional[Callable] = None) -> ActionResult:
        """Restore orphaned .plexcached files to their original names"""
        if not paths:
            return ActionResult(success=False, message="No paths provided")

        # --- Parallel path ---
        if max_workers > 1 and not dry_run:
            def _restore_worker(plexcached_path: str) -> Tuple[str, bool, Optional[str]]:
                if not plexcached_path.endswith('.plexcached'):
                    return (plexcached_path, False, f"Not a .plexcached file: {os.path.basename(plexcached_path)}")
                try:
                    original_path = _strip_plexcached(plexcached_path)
                except ValueError as e:
                    return (plexcached_path, False, str(e))
                try:
                    if os.path.islink(original_path):
                        # Symlink at original location (from use_symlinks mode) - remove it, then restore
                        os.remove(original_path)
                        os.rename(plexcached_path, original_path)
                    elif os.path.exists(original_path):
                        os.remove(plexcached_path)
                    else:
                        os.rename(plexcached_path, original_path)
                    return (plexcached_path, True, None)
                except OSError as e:
                    return (plexcached_path, False, f"{os.path.basename(plexcached_path)}: {str(e)}")

            results = self._run_parallel(paths, _restore_worker, max_workers, stop_check, progress_callback, active_callback)

            successful_paths = [path for path, success, _ in results if success]
            errors = [err for _, success, err in results if not success and err]

            return ActionResult(
                success=len(successful_paths) > 0,
                message=f"Restored {len(successful_paths)} backup file(s)",
                affected_count=len(successful_paths),
                errors=errors,
                affected_paths=successful_paths,
            )

        # --- Sequential path (dry_run or max_workers <= 1) ---
        affected = 0
        errors = []
        affected_paths = []

        for i, plexcached_path in enumerate(paths):
            if stop_check and stop_check():
                break
            if progress_callback:
                progress_callback(i + 1, len(paths), os.path.basename(plexcached_path))
            if not plexcached_path.endswith('.plexcached'):
                errors.append(f"Not a .plexcached file: {os.path.basename(plexcached_path)}")
                continue

            try:
                original_path = _strip_plexcached(plexcached_path)
            except ValueError as e:
                errors.append(str(e))
                continue

            if dry_run:
                affected += 1
            else:
                try:
                    if os.path.islink(original_path):
                        # Symlink at original location (from use_symlinks mode) - remove it, then restore
                        os.remove(original_path)
                        os.rename(plexcached_path, original_path)
                        logging.debug(f"Removed symlink and restored: {plexcached_path}")
                    elif os.path.exists(original_path):
                        # Original exists - just delete the redundant backup
                        os.remove(plexcached_path)
                        logging.debug(f"Deleted redundant .plexcached (original exists): {plexcached_path}")
                    else:
                        os.rename(plexcached_path, original_path)
                    affected += 1
                    affected_paths.append(plexcached_path)
                except OSError as e:
                    errors.append(f"{os.path.basename(plexcached_path)}: {str(e)}")

        action = "Would restore" if dry_run else "Restored"
        return ActionResult(
            success=len(errors) == 0,
            message=f"{action} {affected} backup file(s)",
            affected_count=affected,
            errors=errors,
            affected_paths=affected_paths
        )

    def restore_all_plexcached(self, dry_run: bool = True, orphaned_only: bool = False, **kwargs) -> ActionResult:
        """Restore all orphaned .plexcached files

        Args:
            dry_run: If True, only simulate the restore
            orphaned_only: If True, only restore truly orphaned backups (not redundant ones)
        """
        backups = self._get_orphaned_plexcached()

        if orphaned_only:
            # Filter to only include truly orphaned backups (not redundant)
            backups = [b for b in backups if b.backup_type == "orphaned"]

        paths = [b.plexcached_path for b in backups]
        return self.restore_plexcached(paths, dry_run, **kwargs)

    def delete_plexcached(self, paths: List[str], dry_run: bool = True,
                          stop_check: Optional[Callable[[], bool]] = None,
                          progress_callback: Optional[Callable] = None,
                          bytes_progress_callback: Optional[Callable] = None,
                          max_workers: int = 1,
                          active_callback: Optional[Callable] = None) -> ActionResult:
        """Delete orphaned .plexcached backup files (e.g., when no longer needed)"""
        if not paths:
            return ActionResult(success=False, message="No paths provided")

        # --- Parallel path ---
        if max_workers > 1 and not dry_run:
            def _delete_worker(plexcached_path: str) -> Tuple[str, bool, Optional[str]]:
                if not plexcached_path.endswith('.plexcached'):
                    return (plexcached_path, False, f"Not a .plexcached file: {os.path.basename(plexcached_path)}")
                try:
                    if os.path.exists(plexcached_path):
                        os.remove(plexcached_path)
                        return (plexcached_path, True, None)
                    else:
                        return (plexcached_path, False, f"{os.path.basename(plexcached_path)}: File not found")
                except OSError as e:
                    return (plexcached_path, False, f"{os.path.basename(plexcached_path)}: {str(e)}")

            results = self._run_parallel(paths, _delete_worker, max_workers, stop_check, progress_callback, active_callback)

            successful_paths = [path for path, success, _ in results if success]
            errors = [err for _, success, err in results if not success and err]

            return ActionResult(
                success=len(successful_paths) > 0,
                message=f"Deleted {len(successful_paths)} backup file(s)",
                affected_count=len(successful_paths),
                errors=errors,
                affected_paths=successful_paths,
            )

        # --- Sequential path (dry_run or max_workers <= 1) ---
        affected = 0
        errors = []
        affected_paths = []

        for i, plexcached_path in enumerate(paths):
            if stop_check and stop_check():
                break
            if progress_callback:
                progress_callback(i + 1, len(paths), os.path.basename(plexcached_path))
            if not plexcached_path.endswith('.plexcached'):
                errors.append(f"Not a .plexcached file: {os.path.basename(plexcached_path)}")
                continue

            if dry_run:
                affected += 1
            else:
                try:
                    if os.path.exists(plexcached_path):
                        os.remove(plexcached_path)
                        affected += 1
                        affected_paths.append(plexcached_path)
                    else:
                        errors.append(f"{os.path.basename(plexcached_path)}: File not found")
                except OSError as e:
                    errors.append(f"{os.path.basename(plexcached_path)}: {str(e)}")

        action = "Would delete" if dry_run else "Deleted"
        return ActionResult(
            success=len(errors) == 0,
            message=f"{action} {affected} backup file(s)",
            affected_count=affected,
            errors=errors,
            affected_paths=affected_paths
        )

    def delete_all_plexcached(self, dry_run: bool = True, **kwargs) -> ActionResult:
        """Delete all orphaned .plexcached files"""
        orphaned = self._get_orphaned_plexcached()
        paths = [o.plexcached_path for o in orphaned]
        return self.delete_plexcached(paths, dry_run, **kwargs)

    def fix_with_backup(self, paths: List[str], dry_run: bool = True,
                        stop_check: Optional[Callable[[], bool]] = None,
                        progress_callback: Optional[Callable] = None,
                        bytes_progress_callback: Optional[Callable] = None,
                        max_workers: int = 1,
                        active_callback: Optional[Callable] = None) -> ActionResult:
        """Fix unprotected files that have .plexcached backup - delete cache copy, restore backup"""
        if not paths:
            return ActionResult(success=False, message="No paths provided")

        # --- Parallel path ---
        if max_workers > 1 and not dry_run:
            def _fix_worker(cache_path: str) -> Tuple[str, bool, Optional[str]]:
                try:
                    has_backup, backup_path = self._check_plexcached_backup(cache_path)
                    has_dup, _ = self._check_array_duplicate(cache_path)

                    if not has_backup and not has_dup:
                        return (cache_path, False, f"{os.path.basename(cache_path)}: No backup or array copy found")

                    if has_backup and backup_path:
                        try:
                            original_array_path = _strip_plexcached(backup_path)
                        except ValueError as e:
                            return (cache_path, False, str(e))
                        os.rename(backup_path, original_array_path)

                    if os.path.exists(cache_path):
                        os.remove(cache_path)

                    return (cache_path, True, None)
                except OSError as e:
                    return (cache_path, False, f"{os.path.basename(cache_path)}: {str(e)}")

            results = self._run_parallel(paths, _fix_worker, max_workers, stop_check, progress_callback, active_callback)

            successful_paths = [path for path, success, _ in results if success]
            errors = [err for _, success, err in results if not success and err]

            self._cleanup_empty_directories()

            return ActionResult(
                success=len(successful_paths) > 0,
                message=f"Fixed {len(successful_paths)} file(s) with backup",
                affected_count=len(successful_paths),
                errors=errors,
                affected_paths=successful_paths,
            )

        # --- Sequential path (dry_run or max_workers <= 1) ---
        affected = 0
        errors = []
        affected_paths = []

        for i, cache_path in enumerate(paths):
            if stop_check and stop_check():
                break
            if progress_callback:
                progress_callback(i + 1, len(paths), os.path.basename(cache_path))
            has_backup, backup_path = self._check_plexcached_backup(cache_path)
            has_dup, array_path = self._check_array_duplicate(cache_path)

            if not has_backup and not has_dup:
                errors.append(f"{os.path.basename(cache_path)}: No backup or array copy found")
                continue

            if dry_run:
                affected += 1
            else:
                try:
                    # If it's a .plexcached backup, rename it back FIRST (safer order)
                    if has_backup and backup_path:
                        try:
                            original_array_path = _strip_plexcached(backup_path)
                        except ValueError as e:
                            errors.append(str(e))
                            continue
                        os.rename(backup_path, original_array_path)

                    # Delete cache copy
                    if os.path.exists(cache_path):
                        os.remove(cache_path)

                    affected += 1
                    affected_paths.append(cache_path)
                except OSError as e:
                    errors.append(f"{os.path.basename(cache_path)}: {str(e)}")

        if not dry_run:
            self._cleanup_empty_directories()

        action = "Would fix" if dry_run else "Fixed"
        return ActionResult(
            success=len(errors) == 0,
            message=f"{action} {affected} file(s) with backup",
            affected_count=affected,
            errors=errors,
            affected_paths=affected_paths
        )

    def sync_to_array(self, paths: List[str], dry_run: bool = True,
                      stop_check: Optional[Callable[[], bool]] = None,
                      progress_callback: Optional[Callable] = None,
                      bytes_progress_callback: Optional[Callable] = None,
                      max_workers: int = 1,
                      active_callback: Optional[Callable] = None) -> ActionResult:
        """Move cache files to array - handles both files with and without backups.

        For each file:
        - If a .plexcached backup exists: restore it (rename to original), delete cache copy
        - If a duplicate exists on array: just delete cache copy
        - If no backup/duplicate: copy to array, verify, then delete cache copy
        """
        if not paths:
            return ActionResult(success=False, message="No paths provided")

        # --- Parallel path ---
        if max_workers > 1 and not dry_run:
            # Pre-calculate total bytes for files that need actual copying
            total_bytes = 0
            for cache_path in paths:
                array_path = self._cache_to_array_path(cache_path)
                if array_path:
                    has_backup, _ = self._check_plexcached_backup(cache_path)
                    has_dup, _ = self._check_array_duplicate(cache_path)
                    if not has_backup and not has_dup:
                        try:
                            total_bytes += os.path.getsize(cache_path)
                        except OSError:
                            pass

            aggregator = _ByteProgressAggregator(total_bytes, bytes_progress_callback) if total_bytes > 0 else None

            if bytes_progress_callback and total_bytes > 0:
                bytes_progress_callback(0, total_bytes)

            def _sync_worker(cache_path: str) -> Tuple[str, bool, Optional[str]]:
                try:
                    array_path = self._cache_to_array_path(cache_path)
                    if not array_path:
                        return (cache_path, False, f"{os.path.basename(cache_path)}: Unknown path mapping")

                    has_backup, backup_path = self._check_plexcached_backup(cache_path)
                    has_dup, _ = self._check_array_duplicate(cache_path)

                    if has_backup and backup_path:
                        try:
                            original_array_path = _strip_plexcached(backup_path)
                        except ValueError as e:
                            return (cache_path, False, str(e))
                        if os.path.exists(original_array_path):
                            os.remove(backup_path)
                        else:
                            os.rename(backup_path, original_array_path)
                        if os.path.exists(cache_path):
                            os.remove(cache_path)
                        return (cache_path, True, None)

                    elif has_dup:
                        if os.path.exists(cache_path):
                            os.remove(cache_path)
                        return (cache_path, True, None)

                    else:
                        array_dir = os.path.dirname(array_path)
                        os.makedirs(array_dir, exist_ok=True)

                        worker_cb = aggregator.make_worker_callback() if aggregator else None
                        self._copy_with_progress(cache_path, array_path, worker_cb)

                        if os.path.exists(array_path):
                            cache_size = os.path.getsize(cache_path)
                            array_size = os.path.getsize(array_path)
                            if cache_size == array_size:
                                os.remove(cache_path)
                                return (cache_path, True, None)
                            else:
                                return (cache_path, False, f"{os.path.basename(cache_path)}: Size mismatch after copy")
                        else:
                            return (cache_path, False, f"{os.path.basename(cache_path)}: Copy failed")

                except OSError as e:
                    return (cache_path, False, f"{os.path.basename(cache_path)}: {str(e)}")

            results = self._run_parallel(paths, _sync_worker, max_workers, stop_check, progress_callback, active_callback)

            successful_paths = [path for path, success, _ in results if success]
            errors = [err for _, success, err in results if not success and err]

            self._cleanup_empty_directories()

            return ActionResult(
                success=len(successful_paths) > 0,
                message=f"Moved {len(successful_paths)} file(s) to array",
                affected_count=len(successful_paths),
                errors=errors,
                affected_paths=successful_paths,
            )

        # --- Sequential path (dry_run or max_workers <= 1) ---
        affected = 0
        errors = []
        affected_paths = []

        for i, cache_path in enumerate(paths):
            if stop_check and stop_check():
                break
            if progress_callback:
                progress_callback(i + 1, len(paths), os.path.basename(cache_path))
            array_path = self._cache_to_array_path(cache_path)
            if not array_path:
                errors.append(f"{os.path.basename(cache_path)}: Unknown path mapping")
                continue

            # Check for existing backup or duplicate
            has_backup, backup_path = self._check_plexcached_backup(cache_path)
            has_dup, _ = self._check_array_duplicate(cache_path)

            if dry_run:
                affected += 1
            else:
                try:
                    if has_backup and backup_path:
                        try:
                            original_array_path = _strip_plexcached(backup_path)
                        except ValueError as e:
                            errors.append(str(e))
                            continue

                        # Check if original already exists (redundant backup scenario)
                        if os.path.exists(original_array_path):
                            # Original already restored - just delete the redundant .plexcached
                            os.remove(backup_path)
                            logging.debug(f"Deleted redundant .plexcached backup: {backup_path}")
                        else:
                            # Restore the .plexcached backup (rename to original)
                            os.rename(backup_path, original_array_path)

                        # Delete cache copy
                        if os.path.exists(cache_path):
                            os.remove(cache_path)
                        affected += 1
                        affected_paths.append(cache_path)

                    elif has_dup:
                        # Duplicate already exists on array, just delete cache copy
                        if os.path.exists(cache_path):
                            os.remove(cache_path)
                        affected += 1
                        affected_paths.append(cache_path)

                    else:
                        # No backup/duplicate - copy to array first
                        array_dir = os.path.dirname(array_path)
                        os.makedirs(array_dir, exist_ok=True)

                        # Copy file to array with progress
                        self._copy_with_progress(cache_path, array_path, bytes_progress_callback)

                        # Verify copy
                        if os.path.exists(array_path):
                            cache_size = os.path.getsize(cache_path)
                            array_size = os.path.getsize(array_path)

                            if cache_size == array_size:
                                os.remove(cache_path)
                                affected += 1
                                affected_paths.append(cache_path)
                            else:
                                errors.append(f"{os.path.basename(cache_path)}: Size mismatch after copy")
                        else:
                            errors.append(f"{os.path.basename(cache_path)}: Copy failed")

                except OSError as e:
                    errors.append(f"{os.path.basename(cache_path)}: {str(e)}")

        if not dry_run:
            self._cleanup_empty_directories()

        action = "Would move" if dry_run else "Moved"
        return ActionResult(
            success=len(errors) == 0,
            message=f"{action} {affected} file(s) to array",
            affected_count=affected,
            errors=errors,
            affected_paths=affected_paths
        )

    def add_to_exclude(self, paths: List[str], dry_run: bool = True) -> ActionResult:
        """Add unprotected cache files to exclude list (no backup created)"""
        if not paths:
            return ActionResult(success=False, message="No paths provided")

        if dry_run:
            return ActionResult(
                success=True,
                message=f"Would add {len(paths)} file(s) to exclude list",
                affected_count=len(paths)
            )

        try:
            with open(self.exclude_file, 'a', encoding='utf-8') as f:
                for path in paths:
                    # Translate container paths to host paths for Unraid mover
                    host_path = self._translate_container_to_host_path(path)
                    f.write(host_path + '\n')

            return ActionResult(
                success=True,
                message=f"Added {len(paths)} file(s) to exclude list",
                affected_count=len(paths)
            )
        except IOError as e:
            return ActionResult(
                success=False,
                message=f"Error writing to exclude file: {str(e)}",
                errors=[str(e)]
            )

    def protect_with_backup(self, paths: List[str], dry_run: bool = True,
                            stop_check: Optional[Callable[[], bool]] = None,
                            progress_callback: Optional[Callable] = None,
                            bytes_progress_callback: Optional[Callable] = None,
                            max_workers: int = 1,
                            active_callback: Optional[Callable] = None) -> ActionResult:
        """Protect cache files by creating .plexcached backup on array and adding to exclude list"""
        logging.info(f"protect_with_backup called with {len(paths)} paths, dry_run={dry_run}")

        if not paths:
            logging.warning("protect_with_backup: No paths provided")
            return ActionResult(success=False, message="No paths provided")

        # --- Parallel path ---
        if max_workers > 1 and not dry_run:
            # Pre-calculate total bytes for files that need copying
            total_bytes = 0
            for cache_path in paths:
                array_path = self._cache_to_array_path(cache_path)
                if array_path and not os.path.exists(array_path + ".plexcached"):
                    try:
                        total_bytes += os.path.getsize(cache_path)
                    except OSError:
                        pass

            aggregator = _ByteProgressAggregator(total_bytes, bytes_progress_callback) if total_bytes > 0 else None

            # Signal initial byte total to runner
            if bytes_progress_callback and total_bytes > 0:
                bytes_progress_callback(0, total_bytes)

            def _protect_worker(cache_path: str) -> Tuple[str, bool, Optional[str]]:
                try:
                    array_path = self._cache_to_array_path(cache_path)
                    if not array_path:
                        return (cache_path, False, f"{os.path.basename(cache_path)}: Unknown path mapping")

                    plexcached_path = array_path + ".plexcached"

                    if os.path.exists(plexcached_path):
                        logging.info(f"Backup already exists for {os.path.basename(cache_path)}")
                        return (cache_path, True, None)

                    array_dir = os.path.dirname(array_path)
                    os.makedirs(array_dir, exist_ok=True)

                    cache_size = os.path.getsize(cache_path) if os.path.exists(cache_path) else 0
                    logging.info(f"Copying {os.path.basename(cache_path)} ({cache_size / (1024**3):.2f} GB) to array...")

                    worker_cb = aggregator.make_worker_callback() if aggregator else None
                    self._copy_with_progress(cache_path, plexcached_path, worker_cb)

                    # Verify copy
                    if os.path.exists(plexcached_path):
                        backup_size = os.path.getsize(plexcached_path)
                        if cache_size != backup_size:
                            os.remove(plexcached_path)
                            return (cache_path, False, f"{os.path.basename(cache_path)}: Copy verification failed")
                    else:
                        return (cache_path, False, f"{os.path.basename(cache_path)}: Backup not created")

                    logging.info(f"Protected: {os.path.basename(cache_path)}")
                    return (cache_path, True, None)
                except (IOError, OSError) as e:
                    logging.exception(f"Error protecting {os.path.basename(cache_path)}: {e}")
                    return (cache_path, False, f"{os.path.basename(cache_path)}: {str(e)}")

            results = self._run_parallel(paths, _protect_worker, max_workers, stop_check, progress_callback, active_callback)

            # Phase 2: batch metadata for successful files
            successful_paths = [path for path, success, _ in results if success]
            errors = [err for _, success, err in results if not success and err]

            if successful_paths:
                self._batch_add_to_exclude(successful_paths)
                self._batch_add_to_timestamps(successful_paths)

            affected = len(successful_paths)
            logging.info(f"protect_with_backup complete: Protected {affected} file(s), {len(errors)} errors")
            if errors:
                logging.warning(f"protect_with_backup errors: {errors}")
            return ActionResult(
                success=len(errors) == 0,
                message=f"Protected {affected} file(s) with array backup",
                affected_count=affected,
                errors=errors,
                affected_paths=successful_paths,
            )

        # --- Sequential path (dry_run or max_workers <= 1) ---
        affected = 0
        errors = []
        affected_paths = []

        for i, cache_path in enumerate(paths):
            if stop_check and stop_check():
                break
            if progress_callback:
                progress_callback(i + 1, len(paths), os.path.basename(cache_path))
            logging.debug(f"Processing path {i+1}/{len(paths)}: {cache_path}")

            # Get the array path equivalent
            array_path = self._cache_to_array_path(cache_path)
            if not array_path:
                logging.warning(f"Could not convert cache path to array path: {cache_path}")
                errors.append(f"{os.path.basename(cache_path)}: Unknown path mapping")
                continue

            plexcached_path = array_path + ".plexcached"
            logging.debug(f"Array path: {array_path}, plexcached_path: {plexcached_path}")

            if dry_run:
                affected += 1
            else:
                try:
                    # Check if backup already exists
                    backup_exists = os.path.exists(plexcached_path)

                    if backup_exists:
                        # Backup already exists - just add to exclude list and timestamps
                        logging.info(f"Backup already exists for {os.path.basename(cache_path)}, adding to exclude list")
                    else:
                        # Need to create backup - copy file to array
                        array_dir = os.path.dirname(array_path)
                        os.makedirs(array_dir, exist_ok=True)

                        cache_size = os.path.getsize(cache_path) if os.path.exists(cache_path) else 0
                        logging.info(f"Copying {os.path.basename(cache_path)} ({cache_size / (1024**3):.2f} GB) to array...")

                        self._copy_with_progress(cache_path, plexcached_path, bytes_progress_callback)

                        # Verify copy
                        if os.path.exists(plexcached_path):
                            backup_size = os.path.getsize(plexcached_path)
                            if cache_size != backup_size:
                                os.remove(plexcached_path)
                                logging.error(f"Copy verification failed for {os.path.basename(cache_path)}: {cache_size} != {backup_size}")
                                errors.append(f"{os.path.basename(cache_path)}: Copy verification failed")
                                continue
                        else:
                            logging.error(f"Backup not created for {os.path.basename(cache_path)}")
                            errors.append(f"{os.path.basename(cache_path)}: Backup not created")
                            continue

                    # Add to exclude list (translate to host path for Unraid mover)
                    host_path = self._translate_container_to_host_path(cache_path)
                    with open(self.exclude_file, 'a', encoding='utf-8') as f:
                        f.write(host_path + '\n')

                    # Add to timestamps.json
                    self._add_to_timestamps(cache_path)

                    logging.info(f"Protected: {os.path.basename(cache_path)}")
                    affected += 1
                    affected_paths.append(cache_path)

                except (IOError, OSError) as e:
                    logging.exception(f"Error protecting {os.path.basename(cache_path)}: {e}")
                    errors.append(f"{os.path.basename(cache_path)}: {str(e)}")

        action = "Would protect" if dry_run else "Protected"
        logging.info(f"protect_with_backup complete: {action} {affected} file(s), {len(errors)} errors")
        if errors:
            logging.warning(f"protect_with_backup errors: {errors}")
        return ActionResult(
            success=len(errors) == 0,
            message=f"{action} {affected} file(s) with array backup",
            affected_count=affected,
            errors=errors,
            affected_paths=affected_paths
        )

    def _add_to_timestamps(self, cache_path: str):
        """Add a file to timestamps.json with current time"""
        import json
        from datetime import datetime

        timestamps = {}
        if self.timestamps_file.exists():
            try:
                with open(self.timestamps_file, 'r', encoding='utf-8') as f:
                    timestamps = json.load(f)
            except (json.JSONDecodeError, IOError):
                pass

        timestamps[cache_path] = datetime.now().isoformat()

        with open(self.timestamps_file, 'w', encoding='utf-8') as f:
            json.dump(timestamps, f, indent=2)

    def _batch_add_to_timestamps(self, paths: List[str]):
        """Add multiple files to timestamps.json in one read-merge-write."""
        timestamps = {}
        if self.timestamps_file.exists():
            try:
                with open(self.timestamps_file, 'r', encoding='utf-8') as f:
                    timestamps = json.load(f)
            except (json.JSONDecodeError, IOError):
                pass

        now = datetime.now().isoformat()
        for path in paths:
            timestamps[path] = now

        with open(self.timestamps_file, 'w', encoding='utf-8') as f:
            json.dump(timestamps, f, indent=2)

    def _batch_add_to_exclude(self, paths: List[str]):
        """Add multiple files to exclude list in one file open."""
        with open(self.exclude_file, 'a', encoding='utf-8') as f:
            for path in paths:
                host_path = self._translate_container_to_host_path(path)
                f.write(host_path + '\n')

    def clean_exclude(self, dry_run: bool = True) -> ActionResult:
        """Remove stale entries from exclude list"""
        cache_files = self.get_cache_files()
        exclude_files = self.get_exclude_files()
        stale = exclude_files - cache_files

        if not stale:
            return ActionResult(success=True, message="No stale entries to clean")

        if dry_run:
            return ActionResult(
                success=True,
                message=f"Would remove {len(stale)} stale entries from exclude list",
                affected_count=len(stale)
            )

        try:
            # Keep only entries that still exist on cache
            valid_entries = exclude_files & cache_files
            with open(self.exclude_file, 'w', encoding='utf-8') as f:
                for path in sorted(valid_entries):
                    # Translate container paths back to host paths for Unraid mover
                    host_path = self._translate_container_to_host_path(path)
                    f.write(host_path + '\n')

            return ActionResult(
                success=True,
                message=f"Removed {len(stale)} stale entries from exclude list",
                affected_count=len(stale)
            )
        except IOError as e:
            return ActionResult(
                success=False,
                message=f"Error writing to exclude file: {str(e)}",
                errors=[str(e)]
            )

    def clean_timestamps(self, dry_run: bool = True) -> ActionResult:
        """Remove stale entries from timestamps file"""
        cache_files = self.get_cache_files()
        timestamp_files = self.get_timestamp_files()
        stale = timestamp_files - cache_files

        if not stale:
            return ActionResult(success=True, message="No stale entries to clean")

        if dry_run:
            return ActionResult(
                success=True,
                message=f"Would remove {len(stale)} stale entries from timestamps",
                affected_count=len(stale)
            )

        try:
            with open(self.timestamps_file, 'r', encoding='utf-8') as f:
                timestamps_data = json.load(f)

            for stale_path in stale:
                if stale_path in timestamps_data:
                    del timestamps_data[stale_path]

            with open(self.timestamps_file, 'w', encoding='utf-8') as f:
                json.dump(timestamps_data, f, indent=2)

            return ActionResult(
                success=True,
                message=f"Removed {len(stale)} stale entries from timestamps",
                affected_count=len(stale)
            )
        except (IOError, json.JSONDecodeError) as e:
            return ActionResult(
                success=False,
                message=f"Error updating timestamps file: {str(e)}",
                errors=[str(e)]
            )

    def fix_file_timestamps(self, paths: List[str], dry_run: bool = True) -> ActionResult:
        """Fix invalid file timestamps by setting mtime to current time"""
        if not paths:
            return ActionResult(success=False, message="No paths provided")

        now = datetime.now()
        min_valid_date = datetime(2000, 1, 1)
        affected = 0
        errors = []

        for file_path in paths:
            if not os.path.exists(file_path):
                errors.append(f"{os.path.basename(file_path)}: File not found")
                continue

            try:
                stat_info = os.stat(file_path)
                file_time = datetime.fromtimestamp(stat_info.st_mtime)

                # Check if timestamp is invalid
                if file_time > now or file_time < min_valid_date:
                    if dry_run:
                        affected += 1
                    else:
                        # Set mtime to current time, preserve atime
                        os.utime(file_path, (stat_info.st_atime, now.timestamp()))
                        affected += 1
                else:
                    errors.append(f"{os.path.basename(file_path)}: Timestamp is valid")
            except OSError as e:
                errors.append(f"{os.path.basename(file_path)}: {str(e)}")

        action = "Would fix" if dry_run else "Fixed"
        return ActionResult(
            success=len(errors) == 0,
            message=f"{action} timestamps on {affected} file(s)",
            affected_count=affected,
            errors=errors
        )

    def resolve_duplicate(self, cache_path: str, keep: str, dry_run: bool = True) -> ActionResult:
        """Resolve a duplicate file - keep either cache or array copy"""
        has_dup, array_path = self._check_array_duplicate(cache_path)
        if not has_dup:
            return ActionResult(success=False, message="File is not a duplicate")

        if keep not in ("cache", "array"):
            return ActionResult(success=False, message="Invalid 'keep' option - must be 'cache' or 'array'")

        if dry_run:
            if keep == "cache":
                return ActionResult(
                    success=True,
                    message=f"Would delete array copy, keep cache copy",
                    affected_count=1
                )
            else:
                return ActionResult(
                    success=True,
                    message=f"Would delete cache copy, keep array copy",
                    affected_count=1
                )

        try:
            if keep == "cache":
                os.remove(array_path)
            else:
                os.remove(cache_path)
                # Also remove from exclude list and timestamps
                self._remove_from_exclude_file(cache_path)
                self._remove_from_timestamps(cache_path)

            return ActionResult(
                success=True,
                message=f"Resolved duplicate - kept {keep} copy",
                affected_count=1
            )
        except OSError as e:
            return ActionResult(
                success=False,
                message=f"Error resolving duplicate: {str(e)}",
                errors=[str(e)]
            )

    def _cleanup_empty_directories(self):
        """Remove empty directories from cache paths"""
        settings = self._load_settings()
        if not settings.get('cleanup_empty_folders', True):
            return
        cache_dirs, _ = self._get_paths()
        for cache_dir in cache_dirs:
            if os.path.exists(cache_dir):
                for root, dirs, files in os.walk(cache_dir, topdown=False):
                    for d in dirs:
                        # Don't delete excluded directories
                        if self._should_skip_directory(d):
                            continue
                        dir_path = os.path.join(root, d)
                        try:
                            if not os.listdir(dir_path):
                                os.rmdir(dir_path)
                        except OSError:
                            pass

    def _remove_from_exclude_file(self, cache_path: str):
        """Remove a path from the exclude file"""
        settings = self._load_settings()
        remove_from_exclude_file(self.exclude_file, cache_path, settings.get('path_mappings', []))

    def _remove_from_timestamps(self, cache_path: str):
        """Remove a path from the timestamps file"""
        remove_from_timestamps_file(self.timestamps_file, cache_path)


# Singleton instance
_maintenance_service: Optional[MaintenanceService] = None
_maintenance_service_lock = threading.Lock()


def get_maintenance_service() -> MaintenanceService:
    """Get or create the maintenance service singleton"""
    global _maintenance_service
    if _maintenance_service is None:
        with _maintenance_service_lock:
            if _maintenance_service is None:
                _maintenance_service = MaintenanceService()
    return _maintenance_service
