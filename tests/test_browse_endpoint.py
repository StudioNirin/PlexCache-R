"""Tests for /api/browse and /api/validate-path endpoint logic."""

import os
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

# Mock fcntl/apscheduler for Windows test compat
sys.modules.setdefault('fcntl', MagicMock())
sys.modules.setdefault('apscheduler', MagicMock())
sys.modules.setdefault('apscheduler.schedulers', MagicMock())
sys.modules.setdefault('apscheduler.schedulers.background', MagicMock())
sys.modules.setdefault('apscheduler.triggers', MagicMock())
sys.modules.setdefault('apscheduler.triggers.cron', MagicMock())
sys.modules.setdefault('apscheduler.triggers.interval', MagicMock())
sys.modules.setdefault('plexapi', MagicMock())
sys.modules.setdefault('plexapi.server', MagicMock())

from web.routers.api import browse_directory, validate_path


class TestBrowseEndpoint:
    """Tests for browse_directory() function."""

    def test_browse_rejects_non_mnt_path(self):
        """403 for paths outside /mnt/."""
        result = browse_directory(path="/etc/")
        assert result.status_code == 403

    def test_browse_rejects_traversal(self):
        """403 for path traversal attempts — post-resolve check catches ../."""
        # /mnt/../../etc resolves to /etc which is outside /mnt/
        # On Windows, Path.resolve() may behave differently, so mock it
        mock_resolved = MagicMock()
        mock_resolved.__str__ = lambda self: "/etc"
        mock_resolved.is_dir.return_value = True

        with patch("web.routers.api.Path") as mock_path_cls:
            mock_instance = MagicMock()
            mock_instance.resolve.return_value = mock_resolved
            mock_path_cls.return_value = mock_instance

            result = browse_directory(path="/mnt/../../etc")
            assert result.status_code == 403

    def test_browse_rejects_null_bytes(self):
        """400 for null bytes in path."""
        result = browse_directory(path="/mnt/user\x00/evil")
        assert result.status_code == 400

    def test_browse_rejects_long_path(self):
        """400 for paths exceeding 4096 characters."""
        long_path = "/mnt/" + "a" * 4100
        result = browse_directory(path=long_path)
        assert result.status_code == 400

    def test_browse_rejects_empty_path(self):
        """400 for empty path."""
        result = browse_directory(path="")
        assert result.status_code == 400

    def test_browse_rejects_control_chars(self):
        """400 for control characters in path."""
        result = browse_directory(path="/mnt/user/\x01bad")
        assert result.status_code == 400

    @patch("web.routers.api.os.scandir")
    @patch("web.routers.api.Path")
    def test_browse_returns_directories_only(self, mock_path_cls, mock_scandir):
        """Only directories should appear in results, not files."""
        mock_resolved = MagicMock()
        mock_resolved.__str__ = lambda self: "/mnt/user"
        mock_resolved.is_dir.return_value = True

        mock_path_instance = MagicMock()
        mock_path_instance.resolve.return_value = mock_resolved
        mock_path_cls.return_value = mock_path_instance

        # Create mock directory entries
        dir_entry = MagicMock()
        dir_entry.name = "Movies"
        dir_entry.is_dir.return_value = True

        file_entry = MagicMock()
        file_entry.name = "notes.txt"
        file_entry.is_dir.return_value = False

        mock_scandir.return_value.__enter__ = MagicMock(return_value=iter([dir_entry, file_entry]))
        mock_scandir.return_value.__exit__ = MagicMock(return_value=False)

        result = browse_directory(path="/mnt/user/")
        assert "Movies" in result["directories"]
        assert "notes.txt" not in result["directories"]

    @patch("web.routers.api.os.scandir")
    @patch("web.routers.api.Path")
    def test_browse_caps_at_100_entries(self, mock_path_cls, mock_scandir):
        """Verify max 100 directory entries returned."""
        mock_resolved = MagicMock()
        mock_resolved.__str__ = lambda self: "/mnt/user"
        mock_resolved.is_dir.return_value = True

        mock_path_instance = MagicMock()
        mock_path_instance.resolve.return_value = mock_resolved
        mock_path_cls.return_value = mock_path_instance

        entries = []
        for i in range(150):
            entry = MagicMock()
            entry.name = f"dir_{i:03d}"
            entry.is_dir.return_value = True
            entries.append(entry)

        mock_scandir.return_value.__enter__ = MagicMock(return_value=iter(entries))
        mock_scandir.return_value.__exit__ = MagicMock(return_value=False)

        result = browse_directory(path="/mnt/user/")
        assert len(result["directories"]) == 100

    @patch("web.routers.api.os.scandir")
    @patch("web.routers.api.Path")
    def test_browse_hides_dotfiles(self, mock_path_cls, mock_scandir):
        """Dotfiles/directories should be hidden."""
        mock_resolved = MagicMock()
        mock_resolved.__str__ = lambda self: "/mnt/user"
        mock_resolved.is_dir.return_value = True

        mock_path_instance = MagicMock()
        mock_path_instance.resolve.return_value = mock_resolved
        mock_path_cls.return_value = mock_path_instance

        visible = MagicMock()
        visible.name = "Movies"
        visible.is_dir.return_value = True

        hidden = MagicMock()
        hidden.name = ".Trash"
        hidden.is_dir.return_value = True

        mock_scandir.return_value.__enter__ = MagicMock(return_value=iter([visible, hidden]))
        mock_scandir.return_value.__exit__ = MagicMock(return_value=False)

        result = browse_directory(path="/mnt/user/")
        assert "Movies" in result["directories"]
        assert ".Trash" not in result["directories"]

    def test_browse_symlink_escape(self):
        """Symlink under /mnt/ pointing outside should be rejected by post-resolve check."""
        mock_resolved = MagicMock()
        mock_resolved.__str__ = lambda self: "/etc/shadow"
        mock_resolved.is_dir.return_value = True

        with patch("web.routers.api.Path") as mock_path_cls:
            mock_instance = MagicMock()
            mock_instance.resolve.return_value = mock_resolved
            mock_path_cls.return_value = mock_instance

            result = browse_directory(path="/mnt/evil_symlink/")
            assert result.status_code == 403

    @patch("web.routers.api.os.scandir")
    @patch("web.routers.api.Path")
    def test_browse_returns_sorted(self, mock_path_cls, mock_scandir):
        """Results should be alphabetically sorted."""
        mock_resolved = MagicMock()
        mock_resolved.__str__ = lambda self: "/mnt/user"
        mock_resolved.is_dir.return_value = True

        mock_path_instance = MagicMock()
        mock_path_instance.resolve.return_value = mock_resolved
        mock_path_cls.return_value = mock_path_instance

        entries = []
        for name in ["Zebra", "Alpha", "Movies"]:
            entry = MagicMock()
            entry.name = name
            entry.is_dir.return_value = True
            entries.append(entry)

        mock_scandir.return_value.__enter__ = MagicMock(return_value=iter(entries))
        mock_scandir.return_value.__exit__ = MagicMock(return_value=False)

        result = browse_directory(path="/mnt/user/")
        assert result["directories"] == ["Alpha", "Movies", "Zebra"]


class TestValidatePathEndpoint:
    """Tests for validate_path() function."""

    def test_validate_empty_path(self):
        """Empty path returns empty response."""
        result = validate_path(path="")
        assert result.body == b""

    def test_validate_non_mnt_path(self):
        """Non /mnt/ path returns empty response."""
        result = validate_path(path="/etc/passwd")
        assert result.body == b""

    @patch("web.routers.api.Path")
    def test_validate_existing_directory(self, mock_path_cls):
        """Existing directory returns check icon."""
        mock_instance = MagicMock()
        mock_instance.exists.return_value = True
        mock_instance.is_dir.return_value = True
        mock_path_cls.return_value = mock_instance

        result = validate_path(path="/mnt/user/Movies/")
        assert b"check-circle" in result.body

    @patch("web.routers.api.Path")
    def test_validate_missing_directory(self, mock_path_cls):
        """Missing path returns warning icon."""
        mock_instance = MagicMock()
        mock_instance.exists.return_value = False
        mock_instance.is_dir.return_value = False
        mock_path_cls.return_value = mock_instance

        result = validate_path(path="/mnt/user/NotReal/")
        assert b"alert-triangle" in result.body
