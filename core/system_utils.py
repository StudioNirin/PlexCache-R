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
from typing import Tuple, Optional, NamedTuple
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


def get_array_direct_path(user_share_path: str) -> str:
    """Convert a user share path to array-direct path for existence checks.

    On Unraid, /mnt/user/ is a FUSE virtual filesystem that merges cache + array.
    When checking if a file exists ONLY on the array (not on cache), we need to
    use /mnt/user0/ which provides direct access to the array only.

    This is critical for eviction: we must verify a backup truly exists on the
    array before deleting the cache copy. Using /mnt/user/ would incorrectly
    return True if the file only exists on cache.

    Args:
        user_share_path: A path potentially starting with /mnt/user/

    Returns:
        The /mnt/user0/ equivalent path if input is /mnt/user/, otherwise unchanged.
    """
    if user_share_path.startswith('/mnt/user/'):
        return '/mnt/user0/' + user_share_path[len('/mnt/user/'):]
    return user_share_path


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
        if result.returncode != 0:
            return False

        # Check if 'zfs' appears in the filesystem type column
        return 'zfs' in result.stdout.lower()

    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
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
        """Detect if running on Unraid system."""
        os_info = {
            'Linux': '/mnt/user0/',
            'Darwin': None,
            'Windows': None
        }
        
        unraid_path = os_info.get(self.os_name)
        return os.path.exists(unraid_path) if unraid_path else False
    
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
        if is_docker:
            logging.info("Docker detected - skipping chown/chmod operations (permissions handled by container)")
    
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
        display_dest: str = None
    ) -> int:
        """Copy a file preserving original ownership and permissions (Linux only).

        Args:
            src: Source file path
            dest: Destination file path
            verbose: If True, log detailed ownership info
            display_src: Optional path to show in logs instead of src (for Docker host paths)
            display_dest: Optional path to show in logs instead of dest (for Docker host paths)
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

                # Copy the file (preserves metadata like timestamps)
                shutil.copy2(src, dest)

                # Restore original ownership and permissions (shutil.copy2 doesn't preserve uid/gid)
                original_umask = os.umask(0)
                try:
                    os.chown(dest, src_uid, src_gid)
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
                    logging.debug(f"  Preserved ownership: uid={dest_stat.st_uid}, gid={dest_stat.st_gid}")
                    logging.debug(f"  Mode: {oct(dest_stat.st_mode)}")
                else:
                    logging.debug(f"File copied with permissions preserved: {log_dest}")
            else:  # Windows logic
                shutil.copy2(src, dest)
                logging.debug(f"File copied (Windows): {log_src} -> {log_dest}")

            return 0
        except (FileNotFoundError, PermissionError, Exception) as e:
            logging.error(f"Error copying file from {log_src} to {log_dest}: {str(e)}")
            raise RuntimeError(f"Error copying file: {str(e)}")

    def create_directory_with_permissions(self, path: str, src_file_for_permissions: str) -> None:
        """Create directory with proper permissions."""
        logging.debug(f"Creating directory with permissions: {path}")

        if not os.path.exists(path):
            if self.is_linux:
                # Get the permissions of the source file
                stat_info = os.stat(src_file_for_permissions)
                uid = stat_info.st_uid
                gid = stat_info.st_gid
                original_umask = os.umask(0)
                os.makedirs(path, exist_ok=True)

                # Restore original ownership (makedirs doesn't preserve uid/gid)
                try:
                    os.chown(path, uid, gid)
                except (PermissionError, OSError) as e:
                    logging.debug(f"Could not set directory ownership (filesystem may not support it): {e}")

                try:
                    os.chmod(path, self.permissions)
                except (PermissionError, OSError) as e:
                    logging.debug(f"Could not set directory permissions (filesystem may not support it): {e}")

                os.umask(original_umask)
                logging.debug(f"Directory created with permissions (Linux): {path}")
            else:  # Windows platform
                os.makedirs(path, exist_ok=True)
                logging.debug(f"Directory created (Windows): {path}")
        else:
            logging.debug(f"Directory already exists: {path}") 