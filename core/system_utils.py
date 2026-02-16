"""
System utilities for PlexCache.
Handles OS detection, system-specific operations, and path conversions.
"""

import os
import platform
import shutil
import subprocess
import atexit
import fcntl
from typing import Tuple, Optional, NamedTuple, Callable
import logging


# ============================================================================
# Disk Usage Types
# ============================================================================

class DiskUsage(NamedTuple):
    """Disk usage statistics compatible with shutil.disk_usage() return type."""
    total: int
    used: int
    free: int


# ============================================================================
# Unraid Disk Utilities
# ============================================================================

def resolve_user0_to_disk(user0_path: str) -> Optional[str]:
    """Resolve /mnt/user0/path to the actual /mnt/diskX/path on Unraid.

    On Unraid, /mnt/user0/ is a FUSE-based aggregate of all array disks.
    This function finds which physical disk a file actually lives on.

    Args:
        user0_path: A path starting with /mnt/user0/

    Returns:
        The actual /mnt/diskX/ path if found, None otherwise.
    """
    if not user0_path.startswith('/mnt/user0/'):
        return None

    relative_path = user0_path[len('/mnt/user0/'):]

    # Check each disk (Unraid supports up to 30 data disks)
    for disk_num in range(1, 31):
        disk_path = f'/mnt/disk{disk_num}/{relative_path}'
        if os.path.exists(disk_path):
            return disk_path

    return None


# ZFS-backed path prefixes that should NOT be converted to /mnt/user0/.
# For ZFS pool-only shares (shareUseCache=only), files never appear at /mnt/user0/
# because that path only shows standard array disks. Using /mnt/user/ is safe for
# these paths since there is no cache/array split — no FUSE ambiguity exists.
# Populated at startup by detect_zfs() checks on each path_mapping's real_path.
#
# NOTE: This is a performance hint — get_array_direct_path() uses it to avoid
# unnecessary /mnt/user0/ conversion for known pool-only shares. Safety-critical
# operations (_move_to_cache, _move_to_array) also probe /mnt/user0/ directly
# as defense in depth, so incorrect detection here won't cause data loss.
_zfs_user_prefixes: set = set()


def set_zfs_prefixes(prefixes: set) -> None:
    """Set the ZFS-backed path prefixes (called once at startup)."""
    global _zfs_user_prefixes
    _zfs_user_prefixes = prefixes


def get_array_direct_path(user_share_path: str) -> str:
    """Convert a user share path to array-direct path for existence checks.

    On Unraid, /mnt/user/ is a FUSE virtual filesystem that merges cache + array.
    When checking if a file exists ONLY on the array (not on cache), we need to
    use /mnt/user0/ which provides direct access to the array only.

    This is critical for eviction: we must verify a backup truly exists on the
    array before deleting the cache copy. Using /mnt/user/ would incorrectly
    return True if the file only exists on cache.

    Exception: ZFS pool-backed shares (shareUseCache=only) never have files at
    /mnt/user0/ — their files live on a ZFS pool, not array disks. For these
    paths, we skip the conversion and keep /mnt/user/ which is safe because
    there is no cache/array FUSE ambiguity.

    NOTE: This function uses _zfs_user_prefixes as a performance hint. Safety-
    critical callers (_move_to_cache, _move_to_array) also probe the filesystem
    directly as defense in depth.

    Args:
        user_share_path: A path potentially starting with /mnt/user/

    Returns:
        The /mnt/user0/ equivalent path if input is /mnt/user/ and not ZFS-backed,
        otherwise unchanged.
    """
    if user_share_path.startswith('/mnt/user/'):
        for prefix in _zfs_user_prefixes:
            if user_share_path.startswith(prefix):
                return user_share_path  # ZFS pool — no user0 conversion
        return '/mnt/user0/' + user_share_path[len('/mnt/user/'):]
    return user_share_path


def parse_size_bytes(size_str: str) -> int:
    """Parse a human-readable size string and return bytes.

    Supports suffixes: TB/T, GB/G, MB/M. Bare numbers default to GB.
    Returns 0 for empty, zero, or invalid input.

    Args:
        size_str: Size string like "500GB", "1.5T", "100MB", or "2" (= 2GB).

    Returns:
        Size in bytes, or 0 if input is empty/zero/invalid.
    """
    if not size_str or size_str.strip() == "0":
        return 0
    size_str = size_str.strip().upper()
    try:
        if size_str.endswith('TB'):
            return int(float(size_str[:-2]) * 1024**4)
        elif size_str.endswith('GB'):
            return int(float(size_str[:-2]) * 1024**3)
        elif size_str.endswith('MB'):
            return int(float(size_str[:-2]) * 1024**2)
        elif size_str.endswith('T'):
            return int(float(size_str[:-1]) * 1024**4)
        elif size_str.endswith('G'):
            return int(float(size_str[:-1]) * 1024**3)
        elif size_str.endswith('M'):
            return int(float(size_str[:-1]) * 1024**2)
        else:
            return int(float(size_str) * 1024**3)  # Default to GB
    except ValueError:
        return 0


def format_bytes(bytes_value: int) -> str:
    """Format bytes into human-readable string (e.g., '1.5 GB').

    This is the canonical implementation — use this everywhere instead of
    creating local _format_size / _format_bytes methods.

    Args:
        bytes_value: Size in bytes to format.

    Returns:
        Human-readable string with appropriate unit.
    """
    size = float(bytes_value)
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024 or unit == 'TB':
            return f"{size:.2f} {unit}" if unit != 'B' else f"{int(size)} B"
        size /= 1024
    return f"{size:.2f} TB"


def format_duration(seconds: float) -> str:
    """Format seconds into human-readable duration like '1m 23s' or '45s'.

    This is the canonical implementation — use this everywhere instead of
    creating local _format_duration methods.

    Args:
        seconds: Duration in seconds.

    Returns:
        Human-readable duration string.
    """
    seconds = max(0, seconds)
    if seconds < 60:
        return f"{int(seconds)}s"
    minutes = int(seconds // 60)
    secs = int(seconds % 60)
    if minutes < 60:
        return f"{minutes}m {secs:02d}s"
    hours = int(minutes // 60)
    mins = minutes % 60
    return f"{hours}h {mins:02d}m"


def format_cache_age(updated_at) -> Optional[str]:
    """Format a datetime as a human-readable cache age string.

    Args:
        updated_at: datetime when the cache was last updated, or None.

    Returns:
        String like 'just now', '5 min ago', '2 hr ago', or None if no timestamp.
    """
    if not updated_at:
        return None

    from datetime import datetime
    age_seconds = (datetime.now() - updated_at).total_seconds()
    if age_seconds < 60:
        return "just now"
    elif age_seconds < 3600:
        return f"{int(age_seconds / 60)} min ago"
    else:
        return f"{int(age_seconds / 3600)} hr ago"


def translate_container_to_host_path(path: str, path_mappings: list) -> str:
    """Translate container cache path to host path for exclude file.

    When Docker remaps cache paths, the exclude file needs host paths
    so the Unraid mover can understand them.

    Args:
        path: Container-side file path.
        path_mappings: List of path mapping dicts with 'host_cache_path' and 'cache_path'.

    Returns:
        Host-side path, or original path if no translation needed.
    """
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


def translate_host_to_container_path(path: str, path_mappings: list) -> str:
    """Translate host cache path to container path.

    When reading from the exclude file, paths are host paths but we need
    container paths to check file existence inside Docker.

    Args:
        path: Host-side file path.
        path_mappings: List of path mapping dicts with 'host_cache_path' and 'cache_path'.

    Returns:
        Container-side path, or original path if no translation needed.
    """
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


def remove_from_exclude_file(exclude_file_path, cache_path: str, path_mappings: list) -> None:
    """Remove a path from the Unraid mover exclude file.

    Args:
        exclude_file_path: Path to the exclude file (str or Path).
        cache_path: Container-side cache path to remove.
        path_mappings: Path mapping dicts for host/container translation.
    """
    from pathlib import Path
    exclude_file = Path(exclude_file_path) if not isinstance(exclude_file_path, Path) else exclude_file_path
    if not exclude_file.exists():
        return

    try:
        with open(exclude_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        host_path = translate_container_to_host_path(cache_path, path_mappings)
        new_lines = [line for line in lines if line.strip() != host_path]

        with open(exclude_file, 'w', encoding='utf-8') as f:
            f.writelines(new_lines)
    except IOError as e:
        logging.warning(f"Could not update exclude file: {e}")


def remove_from_timestamps_file(timestamps_file_path, cache_path: str) -> None:
    """Remove a path from the timestamps JSON file.

    Args:
        timestamps_file_path: Path to timestamps.json (str or Path).
        cache_path: Cache path key to remove.
    """
    import json
    from pathlib import Path
    ts_file = Path(timestamps_file_path) if not isinstance(timestamps_file_path, Path) else timestamps_file_path
    if not ts_file.exists():
        return

    try:
        with open(ts_file, 'r', encoding='utf-8') as f:
            timestamps = json.load(f)

        if cache_path in timestamps:
            del timestamps[cache_path]
            with open(ts_file, 'w', encoding='utf-8') as f:
                json.dump(timestamps, f, indent=2)
        else:
            logging.debug(f"Path not found in timestamps (may already be removed): {cache_path}")
    except (IOError, json.JSONDecodeError) as e:
        logging.warning(f"Could not update timestamps file: {e}")


def get_disk_free_space_bytes(path: str) -> int:
    """Get free space in bytes for the filesystem containing the given path.

    Args:
        path: Any path on the filesystem to check.

    Returns:
        Free space in bytes available for writing.
    """
    if not os.path.exists(path):
        # For files that don't exist yet, check the parent directory
        parent = os.path.dirname(path)
        if not os.path.exists(parent):
            return 0
        path = parent

    stat = os.statvfs(path)
    # f_bavail = blocks available to non-superuser (more accurate than f_bfree)
    return stat.f_bavail * stat.f_frsize


def get_disk_usage(path: str, total_override_bytes: int = 0) -> DiskUsage:
    """Get disk usage with optional manual total size override.

    On ZFS filesystems, statvfs() reports dataset-level stats which can be
    misleading (e.g., showing 1.7TB total when the pool is 3.7TB). Use the
    manual override (cache_drive_size setting) to specify correct pool capacity.

    When manual override is set, we keep the actual free space (which IS accurate
    on ZFS - it reflects pool free space) and calculate used from total - free.
    This gives correct results when mixing pool-level total with dataset stats.

    Args:
        path: Any path on the filesystem to check.
        total_override_bytes: Manual override for total capacity in bytes.
            If > 0, uses this value for total and calculates used from free.
            If 0, uses statvfs (may be inaccurate on ZFS).

    Returns:
        DiskUsage namedtuple with total, used, and free bytes.
    """
    usage = shutil.disk_usage(path)
    actual_total, actual_used, actual_free = usage.total, usage.used, usage.free

    # Apply manual override if provided
    if total_override_bytes > 0:
        # Keep actual_free (accurate on ZFS - reflects pool free space)
        # Calculate used as: manual_total - actual_free
        calculated_used = max(0, total_override_bytes - actual_free)
        return DiskUsage(total_override_bytes, calculated_used, actual_free)

    return DiskUsage(actual_total, actual_used, actual_free)


def detect_zfs(path: str) -> bool:
    """Detect if a path is on a ZFS filesystem.

    First tries df -T on the exact path. If that reports a non-ZFS type
    AND the path is under /mnt/user/ (Unraid FUSE), falls back to checking
    /proc/mounts for ZFS datasets mounted with the same share name.

    This fallback is needed because Unraid's FUSE layer (/mnt/user/) reports
    filesystem type as 'shfs' even when the underlying storage is ZFS.

    Args:
        path: Path to check.

    Returns:
        True if the path is on ZFS, False otherwise.
    """
    try:
        result = subprocess.run(
            ['df', '-T', path],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0 and 'zfs' in result.stdout.lower():
            return True
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        pass

    # Fallback: For Unraid FUSE paths like /mnt/user/<share>/, df -T reports
    # 'shfs' instead of the underlying filesystem. Check /proc/mounts for
    # ZFS datasets with a mountpoint matching the share name.
    if path.startswith('/mnt/user/'):
        parts = path.rstrip('/').split('/')
        if len(parts) >= 4:
            share_name = parts[3]  # e.g., 'plex_media' from /mnt/user/plex_media/...
            return _check_zfs_mount_for_share(share_name)

    return False


def _check_zfs_mount_for_share(share_name: str) -> bool:
    """Check if a ZFS dataset is mounted with a matching share name.

    Reads /proc/mounts to find ZFS mounts where the mountpoint's last
    path component matches the Unraid share name. This detects ZFS-backed
    shares that are hidden behind Unraid's FUSE layer at /mnt/user/.

    Example /proc/mounts line:
        plex/plex_media /mnt/plex/plex_media zfs rw,xattr,posixacl ...

    Args:
        share_name: The Unraid share name (e.g., 'plex_media').

    Returns:
        True if a ZFS mount with a matching share name is found.
    """
    try:
        with open('/proc/mounts', 'r') as f:
            for line in f:
                fields = line.split()
                if len(fields) >= 3:
                    mountpoint = fields[1]
                    fs_type = fields[2]
                    if fs_type == 'zfs' and mountpoint.rstrip('/').endswith('/' + share_name):
                        logging.debug(f"ZFS mount detected via /proc/mounts: {mountpoint} (share: {share_name})")
                        return True
    except (OSError, IOError):
        pass
    return False


def get_disk_number_from_path(disk_path: str) -> Optional[str]:
    """Extract the disk number from a /mnt/diskX/ path.

    Args:
        disk_path: A path like /mnt/disk6/TV Shows/...

    Returns:
        The disk identifier (e.g., "disk6") or None if not a disk path.
    """
    if not disk_path.startswith('/mnt/disk'):
        return None

    # Extract "disk6" from "/mnt/disk6/TV Shows/..."
    parts = disk_path.split('/')
    if len(parts) >= 3 and parts[2].startswith('disk'):
        return parts[2]

    return None


class SingleInstanceLock:
    """
    Prevent multiple instances of PlexCache from running simultaneously.

    Uses flock to ensure only one instance can run at a time.
    The lock is automatically released when the process exits or crashes.
    """

    def __init__(self, lock_file: str):
        self.lock_file = lock_file
        self.lock_fd = None
        self.locked = False

    def acquire(self) -> bool:
        """
        Acquire the lock.

        Returns:
            True if lock acquired successfully, False if another instance is running.
        """
        try:
            self.lock_fd = open(self.lock_file, 'w')
            fcntl.flock(self.lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

            # Write PID for debugging
            self.lock_fd.write(str(os.getpid()))
            self.lock_fd.flush()
            self.locked = True

            # Register cleanup on exit
            atexit.register(self.release)

            return True

        except (IOError, OSError):
            # Lock is held by another process
            if self.lock_fd:
                self.lock_fd.close()
                self.lock_fd = None
            return False

    def release(self):
        """Release the lock and clean up."""
        if not self.locked:
            return

        try:
            if self.lock_fd:
                fcntl.flock(self.lock_fd, fcntl.LOCK_UN)
                self.lock_fd.close()
                self.lock_fd = None

            if os.path.exists(self.lock_file):
                os.remove(self.lock_file)

            self.locked = False
        except Exception:
            pass  # Best effort cleanup


class SystemDetector:
    """Detects and provides information about the current system."""
    
    def __init__(self):
        self.os_name = platform.system()
        self.is_linux = self.os_name != 'Windows'
        self.is_unraid = self._detect_unraid()
        self.is_docker = self._detect_docker()
        
    def _detect_unraid(self) -> bool:
        """Detect if running on Unraid system.

        Primary check: kernel version string contains 'Unraid' (e.g., '6.12.54-Unraid').
        Fallback: /mnt/user0/ exists (standard array systems).
        The kernel check works for all Unraid setups including ZFS-only pools
        where /mnt/user0/ doesn't exist.
        """
        if self.os_name != 'Linux':
            return False
        if 'unraid' in platform.release().lower():
            return True
        return os.path.exists('/mnt/user0/')
    
    def _detect_docker(self) -> bool:
        """Detect if running inside a Docker container."""
        return os.path.exists('/.dockerenv')

    def validate_docker_mounts(self, paths: list) -> list:
        """
        Validate that paths are actual mount points in Docker.

        In Docker, if a volume mount fails (e.g., source doesn't exist,
        trailing space in path), the path will exist as an empty directory
        inside the container rather than a mount point. This can cause
        massive data to be written inside the container.

        Args:
            paths: List of paths to validate (e.g., ['/mnt/cache', '/mnt/user0'])

        Returns:
            List of warning messages for any issues found
        """
        warnings = []

        if not self.is_docker:
            return warnings

        for path in paths:
            if not path:
                continue

            # Normalize path (remove trailing slashes for consistent checking)
            path = path.rstrip('/')

            if not os.path.exists(path):
                # Path doesn't exist - might be OK if not used
                continue

            # Check if it's a mount point
            if not os.path.ismount(path):
                # Not a mount point - could be a directory inside container
                # Check if it's suspiciously small (container rootfs is typically small)
                try:
                    stat = os.statvfs(path)
                    total_gb = (stat.f_blocks * stat.f_frsize) / (1024**3)

                    # If the filesystem is very small (< 100GB), it's likely container rootfs
                    if total_gb < 100:
                        warnings.append(
                            f"WARNING: {path} may not be properly mounted! "
                            f"Filesystem is only {total_gb:.1f}GB. "
                            f"Check your Docker volume configuration."
                        )
                except OSError:
                    pass

        return warnings

class FileUtils:
    """Utility functions for file operations."""

    def __init__(self, is_linux: bool, permissions: int = 0o777, is_docker: bool = False):
        self.is_linux = is_linux
        self.permissions = permissions
        self.is_docker = is_docker

        # Check for PUID/PGID environment variables (Docker user/group override)
        self.puid = None
        self.pgid = None
        puid_env = os.environ.get('PUID')
        pgid_env = os.environ.get('PGID')

        if puid_env is not None:
            try:
                self.puid = int(puid_env)
            except ValueError:
                pass  # Will log warning when log_ownership_config() is called

        if pgid_env is not None:
            try:
                self.pgid = int(pgid_env)
            except ValueError:
                pass  # Will log warning when log_ownership_config() is called

    def log_ownership_config(self) -> None:
        """Log the file ownership configuration. Call after logging is set up."""
        puid_env = os.environ.get('PUID')
        pgid_env = os.environ.get('PGID')

        # Log any parse errors
        if puid_env is not None and self.puid is None:
            logging.warning(f"Invalid PUID value: {puid_env}, ignoring")
        if pgid_env is not None and self.pgid is None:
            logging.warning(f"Invalid PGID value: {pgid_env}, ignoring")

        # Log the ownership mode
        if self.puid is not None or self.pgid is not None:
            logging.info(f"File ownership: PUID={self.puid}, PGID={self.pgid}")
        elif self.is_docker:
            logging.info("File ownership: Using source file ownership (no PUID/PGID set)")
    
    def check_path_exists(self, path: str) -> None:
        """Check if path exists, is a directory, and is writable."""
        logging.debug(f"Checking path: {path}")
        
        if not os.path.exists(path):
            logging.error(f"Path does not exist: {path}")
            raise FileNotFoundError(f"Path {path} does not exist.")
        
        if not os.path.isdir(path):
            logging.error(f"Path is not a directory: {path}")
            raise NotADirectoryError(f"Path {path} is not a directory.")
        
        if not os.access(path, os.W_OK):
            logging.error(f"Path is not writable: {path}")
            raise PermissionError(f"Path {path} is not writable.")
        
        logging.debug(f"Path validation successful: {path}")
    
    def get_free_space(self, directory: str) -> Tuple[float, str]:
        """Get free space in a human-readable format."""
        if not os.path.exists(directory):
            raise FileNotFoundError(f"Invalid path, unable to calculate free space for: {directory}.")

        stat = os.statvfs(directory)
        free_space_bytes = stat.f_bfree * stat.f_frsize
        return self._convert_bytes_to_readable_size(free_space_bytes)

    def get_total_drive_size(self, directory: str) -> int:
        """Get total size of the drive in bytes."""
        if not os.path.exists(directory):
            raise FileNotFoundError(f"Invalid path, unable to calculate drive size for: {directory}.")

        stat = os.statvfs(directory)
        return stat.f_blocks * stat.f_frsize

    def get_total_size_of_files(self, files: list) -> Tuple[float, str]:
        """Calculate total size of files in human-readable format."""
        total_size_bytes = 0
        skipped_files = []
        for file in files:
            try:
                total_size_bytes += os.path.getsize(file)
            except (OSError, FileNotFoundError):
                skipped_files.append(file)

        if skipped_files:
            file_word = "file" if len(skipped_files) == 1 else "files"
            logging.warning(f"Skipping {len(skipped_files)} {file_word} not found on disk (may have been renamed - try refreshing Plex library)")
            for f in skipped_files:
                logging.debug(f"  Not found: {f}")

        return self._convert_bytes_to_readable_size(total_size_bytes)
    
    def _convert_bytes_to_readable_size(self, size_bytes: int) -> Tuple[float, str]:
        """Convert bytes to human-readable format."""
        if size_bytes >= (1024 ** 4):
            size = size_bytes / (1024 ** 4)
            unit = 'TB'
        elif size_bytes >= (1024 ** 3):
            size = size_bytes / (1024 ** 3)
            unit = 'GB'
        elif size_bytes >= (1024 ** 2):
            size = size_bytes / (1024 ** 2)
            unit = 'MB'
        else:
            size = size_bytes / 1024
            unit = 'KB'
        
        return size, unit
    
    def copy_file_with_permissions(
        self,
        src: str,
        dest: str,
        verbose: bool = False,
        display_src: str = None,
        display_dest: str = None,
        stop_check: Callable[[], bool] = None,
        chunk_size: int = 10 * 1024 * 1024,  # 10MB chunks for stop checks
        progress_callback: Optional[Callable[[int, int], None]] = None
    ) -> int:
        """Copy a file preserving original ownership and permissions (Linux only).

        Args:
            src: Source file path
            dest: Destination file path
            verbose: If True, log detailed ownership info
            display_src: Optional path to show in logs instead of src (for Docker host paths)
            display_dest: Optional path to show in logs instead of dest (for Docker host paths)
            stop_check: Optional callback that returns True if copy should be cancelled.
                        Checked between chunks to allow mid-copy cancellation.
            chunk_size: Size of chunks for copy (default 10MB). Smaller = more responsive
                        to stop requests but slightly slower copy speed.
            progress_callback: Optional callback(bytes_copied, file_total) called after each chunk.

        If PUID/PGID environment variables are set, those values are used for ownership.
        Otherwise, the source file's ownership is preserved.

        Raises:
            InterruptedError: If stop_check returns True during copy (copy cancelled).
            RuntimeError: If copy fails for other reasons.
        """
        # Use display paths for logging if provided (Docker shows host paths)
        log_src = display_src or src
        log_dest = display_dest or dest
        logging.debug(f"Copying file from {log_src} to {log_dest}")

        try:
            if self.is_linux:
                # Get source file ownership and permissions before copy
                stat_info = os.stat(src)
                src_uid = stat_info.st_uid
                src_gid = stat_info.st_gid
                src_mode = stat_info.st_mode

                # Use PUID/PGID if set, otherwise use source ownership
                target_uid = self.puid if self.puid is not None else src_uid
                target_gid = self.pgid if self.pgid is not None else src_gid

                # Chunked copy with stop check and progress callback support
                # This allows cancelling mid-copy for large files
                file_size = stat_info.st_size
                bytes_copied = 0
                with open(src, 'rb') as fsrc:
                    with open(dest, 'wb') as fdest:
                        while True:
                            # Check for stop request between chunks
                            if stop_check and stop_check():
                                logging.debug(f"Copy cancelled by stop request: {log_dest}")
                                raise InterruptedError("Copy cancelled by user request")

                            chunk = fsrc.read(chunk_size)
                            if not chunk:
                                break
                            fdest.write(chunk)
                            bytes_copied += len(chunk)
                            if progress_callback:
                                progress_callback(bytes_copied, file_size)

                # Copy metadata (timestamps, etc.) - equivalent to what copy2 does
                shutil.copystat(src, dest)

                # Set ownership and permissions (shutil.copy2 doesn't preserve uid/gid)
                original_umask = os.umask(0)
                try:
                    os.chown(dest, target_uid, target_gid)
                except (PermissionError, OSError) as e:
                    logging.debug(f"Could not set file ownership (filesystem may not support it): {e}")

                try:
                    os.chmod(dest, src_mode)
                except (PermissionError, OSError) as e:
                    logging.debug(f"Could not set file permissions (filesystem may not support it): {e}")
                os.umask(original_umask)

                if verbose:
                    # Log ownership details for debugging
                    dest_stat = os.stat(dest)
                    logging.debug(f"File copied: {log_src} -> {log_dest}")
                    logging.debug(f"  Set ownership: uid={dest_stat.st_uid}, gid={dest_stat.st_gid}")
                    logging.debug(f"  Mode: {oct(dest_stat.st_mode)}")
                else:
                    logging.debug(f"File copied with permissions preserved: {log_dest}")
            else:  # Windows logic
                # Windows: use chunked copy for stop check or progress callback support
                if stop_check or progress_callback:
                    file_size = os.path.getsize(src)
                    bytes_copied = 0
                    with open(src, 'rb') as fsrc:
                        with open(dest, 'wb') as fdest:
                            while True:
                                if stop_check and stop_check():
                                    logging.debug(f"Copy cancelled by stop request: {log_dest}")
                                    raise InterruptedError("Copy cancelled by user request")
                                chunk = fsrc.read(chunk_size)
                                if not chunk:
                                    break
                                fdest.write(chunk)
                                bytes_copied += len(chunk)
                                if progress_callback:
                                    progress_callback(bytes_copied, file_size)
                    shutil.copystat(src, dest)
                else:
                    shutil.copy2(src, dest)
                logging.debug(f"File copied (Windows): {log_src} -> {log_dest}")

            return 0
        except InterruptedError:
            # Re-raise interruption so caller can handle cleanup
            raise
        except (FileNotFoundError, PermissionError, Exception) as e:
            logging.error(f"Error copying file from {log_src} to {log_dest}: {str(e)}")
            raise RuntimeError(f"Error copying file: {str(e)}")

    def create_directory_with_permissions(self, path: str, src_file_for_permissions: str) -> None:
        """Create directory with proper permissions.

        When creating multiple directory levels (e.g., Show/Season/), this ensures
        ALL newly created directories get the correct ownership, not just the final one.

        If PUID/PGID environment variables are set, those values are used for ownership.
        Otherwise, the source file's ownership is used.
        """
        logging.debug(f"Creating directory with permissions: {path}")

        if not os.path.exists(path):
            if self.is_linux:
                # Get the permissions of the source file
                stat_info = os.stat(src_file_for_permissions)
                src_uid = stat_info.st_uid
                src_gid = stat_info.st_gid

                # Use PUID/PGID if set, otherwise use source ownership
                target_uid = self.puid if self.puid is not None else src_uid
                target_gid = self.pgid if self.pgid is not None else src_gid

                # Find the first existing ancestor directory
                # We need to track which directories we create so we can chown them all
                dirs_to_create = []
                current = path
                while current and not os.path.exists(current):
                    dirs_to_create.append(current)
                    parent = os.path.dirname(current)
                    if parent == current:  # Reached root
                        break
                    current = parent

                # Reverse so we create from closest existing ancestor downward
                dirs_to_create.reverse()

                original_umask = os.umask(0)
                os.makedirs(path, exist_ok=True)

                # Set ownership and permissions on ALL newly created directories
                for dir_path in dirs_to_create:
                    try:
                        os.chown(dir_path, target_uid, target_gid)
                    except (PermissionError, OSError) as e:
                        logging.debug(f"Could not set directory ownership for {dir_path}: {e}")

                    try:
                        os.chmod(dir_path, self.permissions)
                    except (PermissionError, OSError) as e:
                        logging.debug(f"Could not set directory permissions for {dir_path}: {e}")

                os.umask(original_umask)
                logging.debug(f"Directory created with permissions (Linux): {path} ({len(dirs_to_create)} level(s), uid={target_uid}, gid={target_gid})")
            else:  # Windows platform
                os.makedirs(path, exist_ok=True)
                logging.debug(f"Directory created (Windows): {path}")
        else:
            logging.debug(f"Directory already exists: {path}") 