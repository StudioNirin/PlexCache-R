"""
System utilities for PlexCache.
Handles OS detection, system-specific operations, and path conversions.
"""

import os
import platform
import re
import socket
import shutil
import ntpath
import posixpath
from typing import Tuple, Optional
import logging


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
    
    def get_system_info(self) -> str:
        """Get human-readable system information."""
        info_parts = [f"Script is currently running on {self.os_name}."]
        
        if self.is_unraid:
            info_parts.append("The script is also running on Unraid.")
        
        if self.is_docker:
            info_parts.append("The script is running inside a Docker container.")
            
        return ' '.join(info_parts)
    
    def is_connected(self) -> bool:
        """Check if internet connection is available."""
        try:
            socket.gethostbyname("www.google.com")
            return True
        except socket.error:
            return False


class PathConverter:
    """Handles path conversions between different operating systems."""
    
    def __init__(self, is_linux: bool):
        self.is_linux = is_linux
    
    def remove_trailing_slashes(self, value: str) -> str:
        """Remove trailing slashes from a path."""
        try:
            if isinstance(value, str):
                if ':' in value and value.rstrip('/\\') == '':
                    return value.rstrip('/') + "\\"
                else:
                    return value.rstrip('/\\')
            return value
        except Exception as e:
            raise ValueError(f"Error occurred while removing trailing slashes: {e}")
    
    def add_trailing_slashes(self, value: str) -> str:
        """Add trailing slashes to a path."""
        try:
            if ':' not in value:  # Not a Windows path
                if not value.startswith("/"):
                    value = "/" + value
                if not value.endswith("/"):
                    value = value + "/"
            return value
        except Exception as e:
            raise ValueError(f"Error occurred while adding trailing slashes: {e}")
    
    def remove_all_slashes(self, value_list: list) -> list:
        """Remove all slashes from a list of paths."""
        try:
            return [value.strip('/\\') for value in value_list]
        except Exception as e:
            raise ValueError(f"Error occurred while removing all slashes: {e}")
    
    def convert_path_to_nt(self, value: str, drive_letter: str) -> str:
        """Convert path to Windows NT format."""
        try:
            if value.startswith('/'):
                value = drive_letter.rstrip(':\\') + ':' + value
            value = value.replace(posixpath.sep, ntpath.sep)
            return ntpath.normpath(value)
        except Exception as e:
            raise ValueError(f"Error occurred while converting path to Windows compatible: {e}")
    
    def convert_path_to_posix(self, value: str) -> Tuple[str, Optional[str]]:
        """Convert path to POSIX format."""
        try:
            # Save the drive letter if exists
            drive_letter_match = re.search(r'^[A-Za-z]:', value)
            drive_letter = drive_letter_match.group() + '\\' if drive_letter_match else None
            
            # Remove drive letter if exists
            value = re.sub(r'^[A-Za-z]:', '', value)
            value = value.replace(ntpath.sep, posixpath.sep)
            return posixpath.normpath(value), drive_letter
        except Exception as e:
            raise ValueError(f"Error occurred while converting path to Posix compatible: {e}")
    
    def convert_path(self, value: str, key: str, settings_data: dict, drive_letter: Optional[str] = None) -> str:
        """Convert path according to the operating system."""
        try:
            if self.is_linux:
                value, drive_letter = self.convert_path_to_posix(value)
                if drive_letter:
                    settings_data[f"{key}_drive"] = drive_letter
            else:
                if drive_letter is None:
                    drive_letter = 'C:\\'
                value = self.convert_path_to_nt(value, drive_letter)
            
            return value
        except Exception as e:
            raise ValueError(f"Error occurred while converting path: {e}")


class FileUtils:
    """Utility functions for file operations."""
    
    def __init__(self, is_linux: bool, permissions: int = 0o777):
        self.is_linux = is_linux
        self.permissions = permissions
    
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

    def get_directory_size(self, directory: str) -> int:
        """Calculate total size of all files in a directory (recursive) in bytes."""
        total_size = 0
        try:
            for dirpath, _, filenames in os.walk(directory):
                for filename in filenames:
                    filepath = os.path.join(dirpath, filename)
                    try:
                        total_size += os.path.getsize(filepath)
                    except (OSError, FileNotFoundError):
                        pass  # Skip files that can't be accessed
        except Exception as e:
            logging.warning(f"Error calculating directory size for {directory}: {e}")
        return total_size
    
    def get_total_size_of_files(self, files: list) -> Tuple[float, str]:
        """Calculate total size of files in human-readable format."""
        total_size_bytes = sum(os.path.getsize(file) for file in files)
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
    
    def move_file(self, src: str, dest: str) -> int:
        """Move a file with proper permissions."""
        logging.debug(f"Moving file from {src} to {dest}")
        
        try:
            if self.is_linux:
                stat_info = os.stat(src)
                uid = stat_info.st_uid
                gid = stat_info.st_gid
                
                # Move the file first
                shutil.move(src, dest)
                logging.debug(f"File moved successfully: {src} -> {dest}")
                
                # Then set the owner and group to the original values
                os.chown(dest, uid, gid)
                original_umask = os.umask(0)
                os.chmod(dest, self.permissions)
                os.umask(original_umask)
                logging.debug(f"Permissions restored for: {dest}")
            else:  # Windows logic
                shutil.move(src, dest)
                logging.debug(f"File moved successfully (Windows): {src} -> {dest}")
            
            return 0
        except (FileNotFoundError, PermissionError, Exception) as e:
            logging.error(f"Error moving file from {src} to {dest}: {str(e)}")
            raise RuntimeError(f"Error moving file: {str(e)}")
    
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
                os.chown(path, uid, gid)
                os.chmod(path, self.permissions)
                os.umask(original_umask)
                logging.debug(f"Directory created with permissions (Linux): {path}")
            else:  # Windows platform
                os.makedirs(path, exist_ok=True)
                logging.debug(f"Directory created (Windows): {path}")
        else:
            logging.debug(f"Directory already exists: {path}") 