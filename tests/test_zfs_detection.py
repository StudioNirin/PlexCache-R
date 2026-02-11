"""Tests for ZFS detection logic in core/system_utils.py.

Covers both direct df -T detection and the FUSE fallback via /proc/mounts.
"""
import subprocess
from unittest.mock import patch, mock_open, MagicMock
import sys

# Mock fcntl for Windows test compatibility
if sys.platform == 'win32':
    sys.modules['fcntl'] = MagicMock()

import pytest
from core.system_utils import detect_zfs, _check_zfs_mount_for_share


class TestDetectZfsDirect:
    """Tests for direct df -T detection (non-FUSE paths)."""

    @patch('core.system_utils.subprocess.run')
    def test_direct_zfs_path(self, mock_run):
        """df -T on a direct ZFS mount returns True."""
        mock_run.return_value = subprocess.CompletedProcess(
            args=['df', '-T', '/mnt/plex/plex_media'],
            returncode=0,
            stdout="Filesystem        Type  1K-blocks      Used Available Use% Mounted on\n"
                   "plex/plex_media   zfs   5533960320  1923975680 3609984640  35% /mnt/plex/plex_media\n"
        )
        assert detect_zfs('/mnt/plex/plex_media') is True

    @patch('core.system_utils.subprocess.run')
    def test_non_zfs_path(self, mock_run):
        """df -T on a regular ext4/xfs path returns False."""
        mock_run.return_value = subprocess.CompletedProcess(
            args=['df', '-T', '/mnt/disk1/movies'],
            returncode=0,
            stdout="Filesystem        Type  1K-blocks      Used Available Use% Mounted on\n"
                   "/dev/md1          xfs   5533960320  1923975680 3609984640  35% /mnt/disk1\n"
        )
        assert detect_zfs('/mnt/disk1/movies') is False

    @patch('core.system_utils.subprocess.run')
    def test_df_failure(self, mock_run):
        """df -T failure returns False."""
        mock_run.return_value = subprocess.CompletedProcess(
            args=['df', '-T', '/nonexistent'],
            returncode=1,
            stdout="",
            stderr="df: /nonexistent: No such file or directory"
        )
        assert detect_zfs('/nonexistent') is False

    @patch('core.system_utils.subprocess.run')
    def test_df_timeout(self, mock_run):
        """df -T timeout returns False."""
        mock_run.side_effect = subprocess.TimeoutExpired(cmd='df', timeout=5)
        assert detect_zfs('/mnt/plex') is False


class TestDetectZfsFuseFallback:
    """Tests for FUSE path fallback via /proc/mounts."""

    @patch('core.system_utils._check_zfs_mount_for_share')
    @patch('core.system_utils.subprocess.run')
    def test_fuse_path_with_zfs_mount(self, mock_run, mock_check):
        """FUSE path /mnt/user/<share>/ falls back to /proc/mounts check."""
        # df -T returns shfs (FUSE), not zfs
        mock_run.return_value = subprocess.CompletedProcess(
            args=['df', '-T', '/mnt/user/plex_media/'],
            returncode=0,
            stdout="Filesystem  Type  1K-blocks Used Available Use% Mounted on\n"
                   "shfs        shfs  999999    0    999999    0%  /mnt/user\n"
        )
        mock_check.return_value = True
        assert detect_zfs('/mnt/user/plex_media/') is True
        mock_check.assert_called_once_with('plex_media')

    @patch('core.system_utils._check_zfs_mount_for_share')
    @patch('core.system_utils.subprocess.run')
    def test_fuse_path_no_zfs_mount(self, mock_run, mock_check):
        """FUSE path with no matching ZFS mount returns False."""
        mock_run.return_value = subprocess.CompletedProcess(
            args=['df', '-T', '/mnt/user/movies/'],
            returncode=0,
            stdout="Filesystem  Type  1K-blocks Used Available Use% Mounted on\n"
                   "shfs        shfs  999999    0    999999    0%  /mnt/user\n"
        )
        mock_check.return_value = False
        assert detect_zfs('/mnt/user/movies/') is False
        mock_check.assert_called_once_with('movies')

    @patch('core.system_utils._check_zfs_mount_for_share')
    @patch('core.system_utils.subprocess.run')
    def test_fuse_path_with_subdir(self, mock_run, mock_check):
        """FUSE path with subdirectory extracts correct share name."""
        mock_run.return_value = subprocess.CompletedProcess(
            args=['df', '-T', '/mnt/user/plex_media/movies/some_file.mkv'],
            returncode=0,
            stdout="Filesystem  Type  1K-blocks Used Available Use% Mounted on\n"
                   "shfs        shfs  999999    0    999999    0%  /mnt/user\n"
        )
        mock_check.return_value = True
        assert detect_zfs('/mnt/user/plex_media/movies/some_file.mkv') is True
        # Share name should be 'plex_media', not 'movies'
        mock_check.assert_called_once_with('plex_media')

    @patch('core.system_utils.subprocess.run')
    def test_non_fuse_non_zfs_no_fallback(self, mock_run):
        """Non-FUSE, non-ZFS paths do NOT trigger fallback."""
        mock_run.return_value = subprocess.CompletedProcess(
            args=['df', '-T', '/mnt/disk1/movies/'],
            returncode=0,
            stdout="Filesystem  Type  1K-blocks Used Available Use% Mounted on\n"
                   "/dev/md1    xfs   999999    0    999999    0%  /mnt/disk1\n"
        )
        assert detect_zfs('/mnt/disk1/movies/') is False

    @patch('core.system_utils.subprocess.run')
    def test_fuse_path_too_short(self, mock_run):
        """FUSE path /mnt/user/ (no share name) returns False."""
        mock_run.return_value = subprocess.CompletedProcess(
            args=['df', '-T', '/mnt/user/'],
            returncode=0,
            stdout="Filesystem  Type  1K-blocks Used Available Use% Mounted on\n"
                   "shfs        shfs  999999    0    999999    0%  /mnt/user\n"
        )
        assert detect_zfs('/mnt/user/') is False


class TestCheckZfsMountForShare:
    """Tests for _check_zfs_mount_for_share /proc/mounts parser."""

    def test_matching_zfs_mount(self):
        """ZFS mount with matching share name in mountpoint."""
        proc_mounts_content = (
            "shfs /mnt/user shfs rw,relatime 0 0\n"
            "shfs /mnt/user0 shfs rw,relatime 0 0\n"
            "plex /mnt/plex zfs rw,xattr,posixacl 0 0\n"
            "plex/plex_media /mnt/plex/plex_media zfs rw,xattr,posixacl 0 0\n"
            "zedfs /mnt/zedfs zfs rw,xattr,posixacl 0 0\n"
            "zedfs/plexcache /mnt/zedfs/plexcache zfs rw,xattr,posixacl 0 0\n"
        )
        with patch('builtins.open', mock_open(read_data=proc_mounts_content)):
            assert _check_zfs_mount_for_share('plex_media') is True

    def test_no_matching_zfs_mount(self):
        """No ZFS mount with matching share name."""
        proc_mounts_content = (
            "shfs /mnt/user shfs rw,relatime 0 0\n"
            "plex /mnt/plex zfs rw,xattr,posixacl 0 0\n"
            "zedfs /mnt/zedfs zfs rw,xattr,posixacl 0 0\n"
        )
        with patch('builtins.open', mock_open(read_data=proc_mounts_content)):
            assert _check_zfs_mount_for_share('movies') is False

    def test_partial_name_no_match(self):
        """Share name 'media' should NOT match mountpoint /mnt/plex/plex_media."""
        proc_mounts_content = (
            "plex/plex_media /mnt/plex/plex_media zfs rw,xattr,posixacl 0 0\n"
        )
        with patch('builtins.open', mock_open(read_data=proc_mounts_content)):
            assert _check_zfs_mount_for_share('media') is False

    def test_non_zfs_mount_no_match(self):
        """Non-ZFS mount with matching name should not match."""
        proc_mounts_content = (
            "/dev/md1 /mnt/disk1/plex_media ext4 rw,relatime 0 0\n"
        )
        with patch('builtins.open', mock_open(read_data=proc_mounts_content)):
            assert _check_zfs_mount_for_share('plex_media') is False

    def test_proc_mounts_not_available(self):
        """Gracefully handle /proc/mounts not being accessible."""
        with patch('builtins.open', side_effect=OSError("Permission denied")):
            assert _check_zfs_mount_for_share('plex_media') is False

    def test_multiple_zfs_pools(self):
        """Correctly matches share name across multiple ZFS pools."""
        proc_mounts_content = (
            "pool1/movies /mnt/pool1/movies zfs rw,xattr,posixacl 0 0\n"
            "pool2/tv_shows /mnt/pool2/tv_shows zfs rw,xattr,posixacl 0 0\n"
        )
        with patch('builtins.open', mock_open(read_data=proc_mounts_content)):
            assert _check_zfs_mount_for_share('movies') is True
            assert _check_zfs_mount_for_share('tv_shows') is True
            assert _check_zfs_mount_for_share('music') is False

    def test_trailing_slash_in_mountpoint(self):
        """Handle mountpoints with trailing slash."""
        proc_mounts_content = (
            "plex/plex_media /mnt/plex/plex_media/ zfs rw,xattr,posixacl 0 0\n"
        )
        with patch('builtins.open', mock_open(read_data=proc_mounts_content)):
            assert _check_zfs_mount_for_share('plex_media') is True
