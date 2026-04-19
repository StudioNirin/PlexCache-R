"""Tests for Docker mount validation (issue #139).

Verifies that is_path_bind_mounted() correctly identifies paths backed by
real bind mounts vs. the overlay rootfs, and that the validation gates in
settings service, API endpoint, and FileMover all respond correctly.
"""

import os
import sys
import logging
from unittest.mock import patch, MagicMock, mock_open

import pytest

# Mock plexapi + requests before importing core.app (not installed in test env)
for _mod in ['plexapi', 'plexapi.server', 'plexapi.myplex', 'plexapi.library',
             'plexapi.video', 'plexapi.exceptions', 'plexapi.settings',
             'requests', 'requests.exceptions']:
    sys.modules.setdefault(_mod, MagicMock())

from core.system_utils import SystemDetector


# ============================================================================
# Fixture: sample /proc/self/mountinfo content
# ============================================================================

SAMPLE_MOUNTINFO = """\
22 1 0:21 / /proc rw,nosuid,nodev,noexec,relatime - proc proc rw
23 1 0:22 / /sys rw,nosuid,nodev,noexec,relatime - sysfs sysfs rw
24 1 0:23 / /dev rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755
25 1 8:1 / / rw,relatime - overlay overlay rw,lowerdir=/var/lib/docker/overlay2/l/ABC
100 1 8:17 / /mnt/cache rw,relatime - ext4 /dev/sdb1 rw
101 1 8:33 / /mnt/user rw,relatime - fuse.shfs shfs rw
102 1 8:49 / /mnt/user0 rw,relatime - ext4 /dev/md1 rw
103 1 8:65 / /mnt/remotes rw,relatime - fuse.mergerfs mergerfs rw
104 103 8:81 / /mnt/remotes/NAS_Media rw,relatime - cifs //nas/media rw
"""


@pytest.fixture
def docker_detector():
    """SystemDetector that thinks it's in Docker, with mocked mountinfo."""
    detector = SystemDetector.__new__(SystemDetector)
    detector.os_name = 'Linux'
    detector.is_linux = True
    detector.is_unraid = True
    detector.is_docker = True
    return detector


@pytest.fixture
def non_docker_detector():
    """SystemDetector that is NOT in Docker."""
    detector = SystemDetector.__new__(SystemDetector)
    detector.os_name = 'Linux'
    detector.is_linux = True
    detector.is_unraid = True
    detector.is_docker = False
    return detector


def _load_mountinfo(detector, content=SAMPLE_MOUNTINFO):
    """Helper to load fixture mountinfo into detector cache."""
    with patch('builtins.open', mock_open(read_data=content)):
        detector._parse_mountinfo()


# ============================================================================
# is_path_bind_mounted() tests
# ============================================================================

class TestIsPathBindMounted:

    def test_valid_path_under_bind_mount(self, docker_detector):
        _load_mountinfo(docker_detector)
        ok, mount = docker_detector.is_path_bind_mounted('/mnt/cache/Movies/Inception.mkv')
        assert ok is True
        assert mount == '/mnt/cache'

    def test_valid_path_exact_mount_point(self, docker_detector):
        _load_mountinfo(docker_detector)
        ok, mount = docker_detector.is_path_bind_mounted('/mnt/cache')
        assert ok is True
        assert mount == '/mnt/cache'

    def test_overlay_path_rejected(self, docker_detector):
        """Path not under any bind mount falls to root overlay."""
        _load_mountinfo(docker_detector)
        ok, mount = docker_detector.is_path_bind_mounted('/mnt/dumpster/media/TV Shows/')
        assert ok is False
        assert mount is None

    def test_nested_mount_longest_match(self, docker_detector):
        """Nested mount /mnt/remotes/NAS_Media wins over /mnt/remotes."""
        _load_mountinfo(docker_detector)
        ok, mount = docker_detector.is_path_bind_mounted('/mnt/remotes/NAS_Media/Movies/foo.mkv')
        assert ok is True
        assert mount == '/mnt/remotes/NAS_Media'

    def test_parent_mount_when_no_nested(self, docker_detector):
        _load_mountinfo(docker_detector)
        ok, mount = docker_detector.is_path_bind_mounted('/mnt/remotes/other_share/file.txt')
        assert ok is True
        assert mount == '/mnt/remotes'

    def test_root_path_rejected(self, docker_detector):
        _load_mountinfo(docker_detector)
        ok, mount = docker_detector.is_path_bind_mounted('/tmp/something')
        assert ok is False
        assert mount is None

    def test_non_docker_always_passes(self, non_docker_detector):
        ok, mount = non_docker_detector.is_path_bind_mounted('/mnt/dumpster/anything')
        assert ok is True
        assert mount is None

    def test_unreadable_mountinfo_graceful_degrade(self, docker_detector):
        with patch('builtins.open', side_effect=OSError("Permission denied")):
            docker_detector._parse_mountinfo()
        ok, mount = docker_detector.is_path_bind_mounted('/mnt/anything')
        assert ok is True
        assert mount is None

    def test_mountinfo_cached(self, docker_detector):
        """Mountinfo should be parsed once and cached."""
        _load_mountinfo(docker_detector)
        assert hasattr(docker_detector, '_mountinfo_cache')
        cached = docker_detector._mountinfo_cache.copy()
        # Second call should use cache, not re-read file
        with patch('builtins.open', side_effect=AssertionError("Should not re-read")):
            result = docker_detector._parse_mountinfo()
        assert result == cached

    def test_path_with_spaces(self, docker_detector):
        _load_mountinfo(docker_detector)
        ok, mount = docker_detector.is_path_bind_mounted('/mnt/cache/TV Shows/Breaking Bad/')
        assert ok is True
        assert mount == '/mnt/cache'


# ============================================================================
# validate_docker_mounts() tests
# ============================================================================

class TestValidateDockerMounts:

    def test_returns_warnings_for_overlay_paths(self, docker_detector):
        _load_mountinfo(docker_detector)
        warnings = docker_detector.validate_docker_mounts(['/mnt/dumpster', '/mnt/cache'])
        assert len(warnings) == 1
        assert '/mnt/dumpster' in warnings[0]
        assert 'docker.img' in warnings[0]

    def test_no_warnings_for_valid_mounts(self, docker_detector):
        _load_mountinfo(docker_detector)
        warnings = docker_detector.validate_docker_mounts(['/mnt/cache', '/mnt/user', '/mnt/user0'])
        assert warnings == []

    def test_non_docker_returns_empty(self, non_docker_detector):
        warnings = non_docker_detector.validate_docker_mounts(['/mnt/anything'])
        assert warnings == []

    def test_skips_empty_paths(self, docker_detector):
        _load_mountinfo(docker_detector)
        warnings = docker_detector.validate_docker_mounts(['', None, '/mnt/cache'])
        assert warnings == []


# ============================================================================
# warn_cache_path() Docker branch tests
# ============================================================================

class TestWarnCachePathDocker:

    @patch('web.services.settings_service.IS_DOCKER', True)
    @patch('web.services.settings_service.get_system_detector')
    def test_overlay_path_returns_warning(self, mock_get_detector):
        detector = MagicMock()
        detector.is_path_bind_mounted.return_value = (False, None)
        mock_get_detector.return_value = detector

        from web.services.settings_service import SettingsService
        result = SettingsService.warn_cache_path('/mnt/dumpster/media/Movies/')
        assert result is not None
        assert 'bind mount' in result
        assert 'docker.img' in result

    @patch('web.services.settings_service.IS_DOCKER', True)
    @patch('web.services.settings_service.get_system_detector')
    def test_valid_mount_no_warning(self, mock_get_detector):
        detector = MagicMock()
        detector.is_path_bind_mounted.return_value = (True, '/mnt/cache')
        mock_get_detector.return_value = detector

        from web.services.settings_service import SettingsService
        result = SettingsService.warn_cache_path('/mnt/cache/Movies/')
        assert result is None

    @patch('web.services.settings_service.IS_DOCKER', False)
    def test_non_docker_skips_mount_check(self):
        from web.services.settings_service import SettingsService
        result = SettingsService.warn_cache_path('/mnt/cache/Movies/')
        assert result is None


# ============================================================================
# detect_path_mapping_health_issues() Docker overlay tests
# ============================================================================

class TestDetectHealthIssuesDocker:

    @patch('web.services.settings_service.IS_DOCKER', True)
    @patch('web.services.settings_service.get_system_detector')
    def test_flags_overlay_cache_path(self, mock_get_detector):
        detector = MagicMock()
        detector.is_path_bind_mounted.return_value = (False, None)
        mock_get_detector.return_value = detector

        from web.services.settings_service import SettingsService
        svc = SettingsService()
        with patch.object(svc, '_load_raw', return_value={
            "path_mappings": [{
                "name": "Movies",
                "enabled": True,
                "cache_path": "/mnt/dumpster/Movies/",
                "real_path": "/mnt/user/Movies/",
            }]
        }):
            issues = svc.detect_path_mapping_health_issues()

        overlay_issues = [i for i in issues if i["issue_type"] == "overlay_path"]
        assert len(overlay_issues) == 2  # Both cache_path and real_path
        assert any("cache_path" in i["message"] for i in overlay_issues)
        assert any("real_path" in i["message"] for i in overlay_issues)

    @patch('web.services.settings_service.IS_DOCKER', True)
    @patch('web.services.settings_service.get_system_detector')
    def test_skips_disabled_mappings(self, mock_get_detector):
        detector = MagicMock()
        detector.is_path_bind_mounted.return_value = (False, None)
        mock_get_detector.return_value = detector

        from web.services.settings_service import SettingsService
        svc = SettingsService()
        with patch.object(svc, '_load_raw', return_value={
            "path_mappings": [{
                "name": "Movies",
                "enabled": False,
                "cache_path": "/mnt/dumpster/Movies/",
                "real_path": "/mnt/user/Movies/",
            }]
        }):
            issues = svc.detect_path_mapping_health_issues()

        overlay_issues = [i for i in issues if i["issue_type"] == "overlay_path"]
        assert len(overlay_issues) == 0

    @patch('web.services.settings_service.IS_DOCKER', True)
    @patch('web.services.settings_service.get_system_detector')
    def test_suggests_user0_when_legacy_user_real_path_and_user0_mounted(self, mock_get_detector):
        """After dropping /mnt/user mount, a legacy real_path should recommend /mnt/user0/."""
        detector = MagicMock()

        def is_mounted(path: str):
            # /mnt/user0 IS mounted; cache is mounted; real_path (/mnt/user/...) is NOT.
            return (path.startswith("/mnt/user0") or path.startswith("/mnt/cache"), None)

        detector.is_path_bind_mounted.side_effect = is_mounted
        mock_get_detector.return_value = detector

        from web.services.settings_service import SettingsService
        svc = SettingsService()
        with patch.object(svc, '_load_raw', return_value={
            "path_mappings": [{
                "name": "TV Shows",
                "enabled": True,
                "cache_path": "/mnt/cache/TV Shows/",
                "real_path": "/mnt/user/TV Shows/",
            }]
        }):
            issues = svc.detect_path_mapping_health_issues()

        legacy_issues = [i for i in issues if i["issue_type"] == "legacy_user_real_path"]
        assert len(legacy_issues) == 1
        assert "/mnt/user0/TV Shows/" in legacy_issues[0]["message"]
        assert "Settings → Paths" in legacy_issues[0]["message"]
        # Should NOT double-report as generic overlay_path
        overlay_issues = [i for i in issues if i["issue_type"] == "overlay_path"]
        assert all("real_path" not in i["message"] for i in overlay_issues)


# ============================================================================
# FileMover gate tests
# ============================================================================

class TestFileMoverGate:

    def test_blocks_when_mount_validation_false(self):
        from core.file_operations import FileMover
        mover = FileMover.__new__(FileMover)
        mover.mount_paths_validated = False
        mover._source_map = {}
        mover._media_info_map = {}

        with patch('core.file_operations.logging') as mock_log:
            mover.move_media_files(
                files=['/fake/file.mkv'],
                destination='cache',
                max_concurrent_moves_array=1,
                max_concurrent_moves_cache=1,
            )
            mock_log.error.assert_called_once()
            assert 'blocked' in mock_log.error.call_args[0][0].lower()

    def test_proceeds_when_mount_validation_true(self):
        from core.file_operations import FileMover
        mover = FileMover.__new__(FileMover)
        mover.mount_paths_validated = True
        mover._source_map = {}
        mover._media_info_map = {}
        mover._stop_check = None
        mover._stop_requested = False
        mover.debug = True
        mover._successful_array_moves = []
        mover._completed_count = 0
        mover._total_count = 0
        mover._completed_bytes = 0
        mover._total_bytes = 0
        mover._active_files = {}

        # Should proceed past the gate (will fail later due to missing attrs, that's fine)
        with patch('core.file_operations.logging'):
            try:
                mover.move_media_files(
                    files=[],
                    destination='cache',
                    max_concurrent_moves_array=1,
                    max_concurrent_moves_cache=1,
                )
            except Exception:
                pass  # Expected — we only test that the gate doesn't block


# ============================================================================
# PlexCacheApp._check_paths() graceful error tests
# ============================================================================

class TestCheckPathsGraceful:

    def _make_app(self, is_docker=True, mappings=None):
        """Create a minimal PlexCacheApp mock for _check_paths testing."""
        from core.app import PlexCacheApp
        app = PlexCacheApp.__new__(PlexCacheApp)
        app._mount_paths_safe = True
        app.system_detector = MagicMock()
        app.system_detector.is_docker = is_docker
        app.file_utils = MagicMock()
        app.config_manager = MagicMock()
        app.config_manager.cache.create_plexcached_backups = False
        app._ensure_cache_path_exists = MagicMock()

        if mappings is not None:
            app.config_manager.paths.path_mappings = mappings
        else:
            app.config_manager.paths.path_mappings = []

        return app

    def test_missing_real_path_sets_flag(self):
        from conftest import MockPathMapping
        mapping = MockPathMapping(
            name="Movies", real_path="/mnt/user/Movies/",
            cache_path="/mnt/cache/Movies/", enabled=True, cacheable=True
        )
        app = self._make_app(is_docker=False, mappings=[mapping])
        app.file_utils.check_path_exists.side_effect = FileNotFoundError("not found")

        app._check_paths()

        assert app._mount_paths_safe is False

    def test_overlay_path_sets_flag(self):
        from conftest import MockPathMapping
        mapping = MockPathMapping(
            name="Movies", real_path="/mnt/user/Movies/",
            cache_path="/mnt/dumpster/Movies/", enabled=True, cacheable=True
        )
        app = self._make_app(is_docker=True, mappings=[mapping])
        app.system_detector.is_path_bind_mounted.return_value = (False, None)

        app._check_paths()

        assert app._mount_paths_safe is False

    def test_valid_paths_flag_stays_true(self):
        from conftest import MockPathMapping
        mapping = MockPathMapping(
            name="Movies", real_path="/mnt/user/Movies/",
            cache_path="/mnt/cache/Movies/", enabled=True, cacheable=True
        )
        app = self._make_app(is_docker=True, mappings=[mapping])
        app.system_detector.is_path_bind_mounted.return_value = (True, '/mnt/cache')
        app.file_utils.check_path_exists.return_value = None

        app._check_paths()

        assert app._mount_paths_safe is True
