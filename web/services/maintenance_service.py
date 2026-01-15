"""Maintenance service - cache audit and fix actions"""

import json
import os
import shutil
import subprocess
from datetime import datetime
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set, Any

from web.config import PROJECT_ROOT, DATA_DIR, CONFIG_DIR, SETTINGS_FILE


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
    is_likely_new_download: bool = False  # True if file is recent and not tracked by PlexCache
    has_invalid_timestamp: bool = False  # True if file has future date or very old date


@dataclass
class OrphanedBackup:
    """.plexcached file on array with no corresponding cache file"""
    plexcached_path: str
    original_filename: str
    size: int
    size_display: str
    restore_path: str


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

    @property
    def new_downloads(self) -> List[UnprotectedFile]:
        """Files that are likely new Radarr/Sonarr downloads (not critical)"""
        return [f for f in self.unprotected_files if f.is_likely_new_download]

    @property
    def critical_unprotected(self) -> List[UnprotectedFile]:
        """Files that are truly unprotected (older files, critical)"""
        return [f for f in self.unprotected_files if not f.is_likely_new_download]

    def calculate_health_status(self):
        """Calculate overall health status based on issues"""
        # Critical: unprotected files (older) and orphaned backups need immediate attention
        if self.critical_unprotected or self.orphaned_plexcached:
            self.health_status = "critical"
        # Warnings: stale entries need cleanup (new_downloads are informational, not warnings)
        elif self.stale_exclude_entries or self.stale_timestamp_entries:
            self.health_status = "warnings"
        else:
            self.health_status = "healthy"

    @property
    def total_issues(self) -> int:
        """Count of actual issues (excludes new downloads which are informational)"""
        return (len(self.critical_unprotected) +
                len(self.orphaned_plexcached) +
                len(self.stale_exclude_entries) +
                len(self.stale_timestamp_entries))


@dataclass
class ActionResult:
    """Result of a fix action"""
    success: bool
    message: str
    affected_count: int = 0
    errors: List[str] = field(default_factory=list)


class MaintenanceService:
    """Service for cache auditing and maintenance actions"""

    # Video extensions
    VIDEO_EXTENSIONS = ('.mkv', '.mp4', '.avi', '.m4v', '.mov', '.wmv', '.ts')
    # Subtitle extensions
    SUBTITLE_EXTENSIONS = ('.srt', '.sub', '.idx', '.ass', '.ssa', '.vtt', '.smi')

    def __init__(self):
        # Use CONFIG_DIR and DATA_DIR for Docker compatibility
        self.settings_file = SETTINGS_FILE
        self.exclude_file = CONFIG_DIR / "plexcache_mover_files_to_exclude.txt"
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
        """Translate host cache path back to container path.

        The exclude file contains host paths (for Unraid mover), but cache_files
        uses container paths. This translates host paths back to container paths
        for accurate comparison.
        """
        settings = self._load_settings()
        path_mappings = settings.get('path_mappings', [])

        for mapping in path_mappings:
            host_cache_path = mapping.get('host_cache_path', '')
            cache_path = mapping.get('cache_path', '')

            if not host_cache_path or not cache_path:
                continue
            if host_cache_path == cache_path:
                continue  # No translation needed

            host_prefix = host_cache_path.rstrip('/')
            if path.startswith(host_prefix):
                container_prefix = cache_path.rstrip('/')
                return path.replace(host_prefix, container_prefix, 1)

        return path

    def _translate_container_to_host_path(self, path: str) -> str:
        """Translate container cache path to host path for exclude file.

        When writing to the exclude file, paths must be host paths so the
        Unraid mover can understand them.
        """
        settings = self._load_settings()
        path_mappings = settings.get('path_mappings', [])

        for mapping in path_mappings:
            host_cache_path = mapping.get('host_cache_path', '')
            cache_path = mapping.get('cache_path', '')

            if not host_cache_path or not cache_path:
                continue
            if host_cache_path == cache_path:
                continue  # No translation needed

            container_prefix = cache_path.rstrip('/')
            if path.startswith(container_prefix):
                host_prefix = host_cache_path.rstrip('/')
                return path.replace(container_prefix, host_prefix, 1)

        return path

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
                    # Convert real_path (/mnt/user/) to array path (/mnt/user0/)
                    array_path = real_path.replace('/mnt/user/', '/mnt/user0/')
                    cache_dirs.append(cache_path)
                    array_dirs.append(array_path)
        else:
            # Legacy single-path mode
            cache_dir = settings.get('cache_dir', '').rstrip('/\\')
            real_source = settings.get('real_source', '').rstrip('/\\')
            nas_library_folders = settings.get('nas_library_folders', [])

            if cache_dir and real_source and nas_library_folders:
                array_source = real_source.replace('/mnt/user/', '/mnt/user0/')
                for folder in nas_library_folders:
                    folder = folder.strip('/\\')
                    cache_dirs.append(os.path.join(cache_dir, folder))
                    array_dirs.append(os.path.join(array_source, folder))

        self._cache_dirs = cache_dirs
        self._array_dirs = array_dirs
        return cache_dirs, array_dirs

    def _format_size(self, size_bytes: int) -> str:
        """Format bytes into human-readable string"""
        if size_bytes == 0:
            return "0 B"

        units = ['B', 'KB', 'MB', 'GB', 'TB']
        unit_index = 0
        size = float(size_bytes)

        while size >= 1024 and unit_index < len(units) - 1:
            size /= 1024
            unit_index += 1

        if unit_index == 0:
            return f"{int(size)} B"
        return f"{size:.1f} {units[unit_index]}"

    def get_cache_files(self) -> Set[str]:
        """Get all media files currently on cache"""
        cache_dirs, _ = self._get_paths()
        cache_files = set()
        extensions = self.VIDEO_EXTENSIONS + self.SUBTITLE_EXTENSIONS

        for cache_dir in cache_dirs:
            if os.path.exists(cache_dir):
                for root, dirs, files in os.walk(cache_dir):
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

        # Threshold for "new download" detection (files created within this many days)
        NEW_DOWNLOAD_THRESHOLD_DAYS = 30

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

            # Determine if this is likely a new Radarr/Sonarr download:
            # - File is recent (created within threshold)
            # - No .plexcached backup (PlexCache didn't move it to cache)
            # Note: We don't check timestamps because PlexCache may have added
            # the file to timestamps during a scan even if it didn't move it
            is_new_download = (
                age_days <= NEW_DOWNLOAD_THRESHOLD_DAYS and
                not has_backup
            )

            # Determine recommended action
            if has_backup:
                recommended = "fix_with_backup"
            elif has_dup:
                recommended = "fix_with_backup"  # Treat duplicates like backups
            elif is_new_download:
                recommended = "add_to_exclude"  # New downloads just need protection
            else:
                recommended = "sync_to_array"

            results.unprotected_files.append(UnprotectedFile(
                cache_path=cache_path,
                filename=filename,
                size=size,
                size_display=self._format_size(size),
                has_plexcached_backup=has_backup,
                backup_path=backup_path,
                has_array_duplicate=has_dup,
                array_path=array_path if has_dup else None,
                recommended_action=recommended,
                created_at=created_at,
                age_days=age_days,
                is_likely_new_download=is_new_download,
                has_invalid_timestamp=has_invalid_timestamp
            ))

        # Sort unprotected files by filename (default)
        results.unprotected_files.sort(key=lambda f: f.filename.lower())

        # Find orphaned .plexcached files
        results.orphaned_plexcached = self._get_orphaned_plexcached()

        # Find stale exclude entries (in exclude but not on cache)
        results.stale_exclude_entries = sorted(list(exclude_files - cache_files))

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
                    size_display=self._format_size(size)
                ))

        results.duplicates.sort(key=lambda f: f.size, reverse=True)

        # Calculate health status
        results.calculate_health_status()

        return results

    def _get_orphaned_plexcached(self, auto_cleanup_superseded: bool = True) -> List[OrphanedBackup]:
        """Find .plexcached files on array with no corresponding cache file.

        Args:
            auto_cleanup_superseded: If True, automatically delete .plexcached backups
                that have been superseded by a newer version (e.g., Sonarr/Radarr upgrades)
        """
        cache_dirs, array_dirs = self._get_paths()
        cache_files = self.get_cache_files()
        orphaned = []
        superseded_deleted = 0

        for i, array_dir in enumerate(array_dirs):
            if not os.path.exists(array_dir):
                continue

            cache_dir = cache_dirs[i]

            for root, dirs, files in os.walk(array_dir):
                for f in files:
                    if f.endswith('.plexcached'):
                        plexcached_path = os.path.join(root, f)
                        original_name = f[:-11]  # Remove .plexcached suffix
                        original_array_path = os.path.join(root, original_name)

                        # Find corresponding cache path
                        relative_path = os.path.relpath(original_array_path, array_dir)
                        cache_path = os.path.join(cache_dir, relative_path)
                        cache_directory = os.path.dirname(cache_path)

                        # Check if orphaned: no cache copy AND no restored original
                        if cache_path not in cache_files and not os.path.exists(original_array_path):
                            # Check if this backup has been superseded by a newer version
                            # (e.g., Sonarr/Radarr upgraded from HDTV to WEB-DL)
                            if auto_cleanup_superseded:
                                replacement = self._find_replacement_file(
                                    original_name, cache_directory, cache_files
                                )
                                if replacement:
                                    # Superseded - auto-delete the old backup
                                    try:
                                        os.remove(plexcached_path)
                                        superseded_deleted += 1
                                        continue  # Don't add to orphaned list
                                    except OSError:
                                        pass  # If delete fails, treat as orphaned

                            try:
                                size = os.path.getsize(plexcached_path)
                            except OSError:
                                size = 0

                            orphaned.append(OrphanedBackup(
                                plexcached_path=plexcached_path,
                                original_filename=original_name,
                                size=size,
                                size_display=self._format_size(size),
                                restore_path=original_array_path
                            ))

        if superseded_deleted > 0:
            import logging
            logging.info(f"Auto-cleaned {superseded_deleted} superseded .plexcached backup(s) "
                        "(replaced by Sonarr/Radarr upgrades)")

        orphaned.sort(key=lambda f: f.size, reverse=True)
        return orphaned

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
            "unprotected_count": len(results.critical_unprotected),  # Only critical ones
            "new_downloads_count": len(results.new_downloads),
            "orphaned_count": len(results.orphaned_plexcached),
            "stale_exclude_count": len(results.stale_exclude_entries),
            "stale_timestamp_count": len(results.stale_timestamp_entries),
            "cache_files": results.cache_file_count,
            "protected_files": results.exclude_entry_count
        }

    # === Fix Actions ===

    def restore_plexcached(self, paths: List[str], dry_run: bool = True) -> ActionResult:
        """Restore orphaned .plexcached files to their original names"""
        if not paths:
            return ActionResult(success=False, message="No paths provided")

        affected = 0
        errors = []

        for plexcached_path in paths:
            if not plexcached_path.endswith('.plexcached'):
                errors.append(f"Not a .plexcached file: {os.path.basename(plexcached_path)}")
                continue

            original_path = plexcached_path[:-11]  # Remove .plexcached suffix

            if dry_run:
                affected += 1
            else:
                try:
                    os.rename(plexcached_path, original_path)
                    affected += 1
                except OSError as e:
                    errors.append(f"{os.path.basename(plexcached_path)}: {str(e)}")

        action = "Would restore" if dry_run else "Restored"
        return ActionResult(
            success=affected > 0,
            message=f"{action} {affected} backup file(s)",
            affected_count=affected,
            errors=errors
        )

    def restore_all_plexcached(self, dry_run: bool = True) -> ActionResult:
        """Restore all orphaned .plexcached files"""
        orphaned = self._get_orphaned_plexcached(auto_cleanup_superseded=False)
        paths = [o.plexcached_path for o in orphaned]
        return self.restore_plexcached(paths, dry_run)

    def delete_plexcached(self, paths: List[str], dry_run: bool = True) -> ActionResult:
        """Delete orphaned .plexcached backup files (e.g., when no longer needed)"""
        if not paths:
            return ActionResult(success=False, message="No paths provided")

        affected = 0
        errors = []

        for plexcached_path in paths:
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
                    else:
                        errors.append(f"{os.path.basename(plexcached_path)}: File not found")
                except OSError as e:
                    errors.append(f"{os.path.basename(plexcached_path)}: {str(e)}")

        action = "Would delete" if dry_run else "Deleted"
        return ActionResult(
            success=affected > 0,
            message=f"{action} {affected} backup file(s)",
            affected_count=affected,
            errors=errors
        )

    def delete_all_plexcached(self, dry_run: bool = True) -> ActionResult:
        """Delete all orphaned .plexcached files"""
        orphaned = self._get_orphaned_plexcached(auto_cleanup_superseded=False)
        paths = [o.plexcached_path for o in orphaned]
        return self.delete_plexcached(paths, dry_run)

    def fix_with_backup(self, paths: List[str], dry_run: bool = True) -> ActionResult:
        """Fix unprotected files that have .plexcached backup - delete cache copy, restore backup"""
        if not paths:
            return ActionResult(success=False, message="No paths provided")

        affected = 0
        errors = []

        for cache_path in paths:
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
                        original_array_path = backup_path[:-11]
                        os.rename(backup_path, original_array_path)

                    # Delete cache copy
                    if os.path.exists(cache_path):
                        os.remove(cache_path)

                    affected += 1
                except OSError as e:
                    errors.append(f"{os.path.basename(cache_path)}: {str(e)}")

        if not dry_run:
            self._cleanup_empty_directories()

        action = "Would fix" if dry_run else "Fixed"
        return ActionResult(
            success=affected > 0,
            message=f"{action} {affected} file(s) with backup",
            affected_count=affected,
            errors=errors
        )

    def sync_to_array(self, paths: List[str], dry_run: bool = True) -> ActionResult:
        """Move cache files to array - handles both files with and without backups.

        For each file:
        - If a .plexcached backup exists: restore it (rename to original), delete cache copy
        - If a duplicate exists on array: just delete cache copy
        - If no backup/duplicate: copy to array, verify, then delete cache copy
        """
        if not paths:
            return ActionResult(success=False, message="No paths provided")

        affected = 0
        errors = []

        for cache_path in paths:
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
                        # Restore the .plexcached backup first
                        original_array_path = backup_path[:-11]  # Remove .plexcached suffix
                        os.rename(backup_path, original_array_path)
                        # Delete cache copy
                        if os.path.exists(cache_path):
                            os.remove(cache_path)
                        affected += 1

                    elif has_dup:
                        # Duplicate already exists on array, just delete cache copy
                        if os.path.exists(cache_path):
                            os.remove(cache_path)
                        affected += 1

                    else:
                        # No backup/duplicate - copy to array first
                        array_dir = os.path.dirname(array_path)
                        os.makedirs(array_dir, exist_ok=True)

                        # Copy file to array
                        shutil.copy2(cache_path, array_path)

                        # Verify copy
                        if os.path.exists(array_path):
                            cache_size = os.path.getsize(cache_path)
                            array_size = os.path.getsize(array_path)

                            if cache_size == array_size:
                                os.remove(cache_path)
                                affected += 1
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
            success=affected > 0,
            message=f"{action} {affected} file(s) to array",
            affected_count=affected,
            errors=errors
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

    def protect_with_backup(self, paths: List[str], dry_run: bool = True) -> ActionResult:
        """Protect cache files by creating .plexcached backup on array and adding to exclude list"""
        if not paths:
            return ActionResult(success=False, message="No paths provided")

        affected = 0
        errors = []

        for cache_path in paths:
            # Get the array path equivalent
            array_path = self._cache_to_array_path(cache_path)
            if not array_path:
                errors.append(f"{os.path.basename(cache_path)}: Unknown path mapping")
                continue

            plexcached_path = array_path + ".plexcached"

            if dry_run:
                affected += 1
            else:
                try:
                    # Create destination directory if needed
                    array_dir = os.path.dirname(array_path)
                    os.makedirs(array_dir, exist_ok=True)

                    # Copy file to array as .plexcached backup
                    shutil.copy2(cache_path, plexcached_path)

                    # Verify copy
                    if os.path.exists(plexcached_path):
                        cache_size = os.path.getsize(cache_path)
                        backup_size = os.path.getsize(plexcached_path)

                        if cache_size == backup_size:
                            # Add to exclude list (translate to host path for Unraid mover)
                            host_path = self._translate_container_to_host_path(cache_path)
                            with open(self.exclude_file, 'a', encoding='utf-8') as f:
                                f.write(host_path + '\n')

                            # Add to timestamps.json
                            self._add_to_timestamps(cache_path)

                            affected += 1
                        else:
                            # Size mismatch - remove failed backup
                            os.remove(plexcached_path)
                            errors.append(f"{os.path.basename(cache_path)}: Copy verification failed")
                    else:
                        errors.append(f"{os.path.basename(cache_path)}: Backup not created")

                except (IOError, OSError) as e:
                    errors.append(f"{os.path.basename(cache_path)}: {str(e)}")

        action = "Would protect" if dry_run else "Protected"
        return ActionResult(
            success=affected > 0 or (dry_run and not errors),
            message=f"{action} {affected} file(s) with array backup",
            affected_count=affected,
            errors=errors
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
            success=affected > 0,
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
        cache_dirs, _ = self._get_paths()
        for cache_dir in cache_dirs:
            if os.path.exists(cache_dir):
                for root, dirs, files in os.walk(cache_dir, topdown=False):
                    for d in dirs:
                        dir_path = os.path.join(root, d)
                        try:
                            if not os.listdir(dir_path):
                                os.rmdir(dir_path)
                        except OSError:
                            pass

    def _remove_from_exclude_file(self, cache_path: str):
        """Remove a path from the exclude file"""
        if not self.exclude_file.exists():
            return

        try:
            with open(self.exclude_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()

            # Translate container path to host path for comparison
            # (exclude file contains host paths for Unraid mover)
            host_path = self._translate_container_to_host_path(cache_path)
            new_lines = [line for line in lines if line.strip() != host_path]

            with open(self.exclude_file, 'w', encoding='utf-8') as f:
                f.writelines(new_lines)
        except IOError:
            pass

    def _remove_from_timestamps(self, cache_path: str):
        """Remove a path from the timestamps file"""
        if not self.timestamps_file.exists():
            return

        try:
            with open(self.timestamps_file, 'r', encoding='utf-8') as f:
                timestamps = json.load(f)

            if cache_path in timestamps:
                del timestamps[cache_path]

                with open(self.timestamps_file, 'w', encoding='utf-8') as f:
                    json.dump(timestamps, f, indent=2)
        except (IOError, json.JSONDecodeError):
            pass


# Singleton instance
_maintenance_service: Optional[MaintenanceService] = None


def get_maintenance_service() -> MaintenanceService:
    """Get or create the maintenance service singleton"""
    global _maintenance_service
    if _maintenance_service is None:
        _maintenance_service = MaintenanceService()
    return _maintenance_service
