"""Tests for MaintenanceRunner service and stop_check in maintenance service methods."""

import time
import threading
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from web.services.maintenance_runner import (
    MaintenanceRunner,
    MaintenanceState,
    ASYNC_ACTIONS,
    ACTION_DISPLAY,
)
from web.services.maintenance_service import ActionResult


def _start(runner, action_name="protect-with-backup", service_method=None, **kwargs):
    """Start an action with OperationRunner mocked as idle."""
    if service_method is None:
        service_method = lambda **kwargs: ActionResult(success=True, message="ok")

    # The local import in start_action resolves get_operation_runner from the source module
    mock_op = MagicMock()
    mock_op.is_running = False
    with patch("web.services.operation_runner.get_operation_runner", return_value=mock_op):
        return runner.start_action(action_name=action_name, service_method=service_method, **kwargs)


# ============================================================================
# MaintenanceRunner - State Transitions
# ============================================================================

class TestMaintenanceRunnerStates:
    """Test state lifecycle: IDLE -> RUNNING -> COMPLETED/FAILED -> IDLE."""

    def test_initial_state_is_idle(self):
        runner = MaintenanceRunner()
        assert runner.state == MaintenanceState.IDLE
        assert not runner.is_running

    def test_start_action_transitions_to_running(self):
        runner = MaintenanceRunner()
        hold = threading.Event()

        def slow(**kwargs):
            hold.wait(timeout=5)
            return ActionResult(success=True, message="ok")

        started = _start(runner, service_method=slow, file_count=3)

        assert started is True
        assert runner.state == MaintenanceState.RUNNING

        hold.set()
        runner._thread.join(timeout=2)
        assert runner.state == MaintenanceState.COMPLETED

    def test_completed_state_after_success(self):
        runner = MaintenanceRunner()
        result = ActionResult(success=True, message="Protected 3 files", affected_count=3)

        _start(runner, service_method=lambda **kwargs: result, file_count=3)
        runner._thread.join(timeout=2)

        assert runner.state == MaintenanceState.COMPLETED
        assert runner.result.action_result.message == "Protected 3 files"
        assert runner.result.action_result.affected_count == 3
        assert runner.result.duration_seconds > 0

    def test_failed_state_on_exception(self):
        runner = MaintenanceRunner()

        def failing_method(**kwargs):
            raise IOError("Disk full")

        _start(runner, action_name="sync-to-array", service_method=failing_method, file_count=1)
        runner._thread.join(timeout=2)

        assert runner.state == MaintenanceState.FAILED
        assert runner.result.error_message == "Disk full"

    def test_dismiss_resets_completed_to_idle(self):
        runner = MaintenanceRunner()
        _start(runner, action_name="delete-plexcached")
        runner._thread.join(timeout=2)
        assert runner.state == MaintenanceState.COMPLETED

        runner.dismiss()
        assert runner.state == MaintenanceState.IDLE

    def test_dismiss_resets_failed_to_idle(self):
        runner = MaintenanceRunner()

        def failing(**kwargs):
            raise RuntimeError("boom")

        _start(runner, action_name="sync-to-array", service_method=failing)
        runner._thread.join(timeout=2)
        assert runner.state == MaintenanceState.FAILED

        runner.dismiss()
        assert runner.state == MaintenanceState.IDLE

    def test_dismiss_does_nothing_when_idle(self):
        runner = MaintenanceRunner()
        runner.dismiss()
        assert runner.state == MaintenanceState.IDLE


# ============================================================================
# MaintenanceRunner - Mutual Exclusion
# ============================================================================

class TestMaintenanceRunnerMutualExclusion:
    """Test that maintenance and operations block each other."""

    def test_blocked_when_operation_running(self):
        runner = MaintenanceRunner()

        mock_op = MagicMock()
        mock_op.is_running = True
        with patch("web.services.operation_runner.get_operation_runner", return_value=mock_op):
            started = runner.start_action(
                action_name="protect-with-backup",
                service_method=lambda **kwargs: ActionResult(success=True, message="ok"),
            )

        assert started is False
        assert runner.state == MaintenanceState.IDLE

    def test_blocked_when_already_running(self):
        runner = MaintenanceRunner()

        hold = threading.Event()

        def slow_action(**kwargs):
            hold.wait(timeout=5)
            return ActionResult(success=True, message="ok")

        started1 = _start(runner, service_method=slow_action, file_count=1)
        assert started1 is True

        # Try to start a second action while first is still running
        started2 = _start(runner, action_name="sync-to-array")
        assert started2 is False

        hold.set()
        runner._thread.join(timeout=2)


# ============================================================================
# MaintenanceRunner - Stop
# ============================================================================

class TestMaintenanceRunnerStop:
    """Test stop functionality."""

    def test_stop_returns_false_when_not_running(self):
        runner = MaintenanceRunner()
        assert runner.stop_action() is False

    def test_stop_sets_flag(self):
        runner = MaintenanceRunner()
        hold = threading.Event()

        def slow_action(**kwargs):
            hold.wait(timeout=5)
            return ActionResult(success=True, message="ok")

        _start(runner, service_method=slow_action)
        assert runner.is_running

        result = runner.stop_action()
        assert result is True
        assert runner.stop_requested is True

        hold.set()
        runner._thread.join(timeout=2)

    def test_stop_check_callback_reflects_stop_flag(self):
        runner = MaintenanceRunner()
        stop_check_values = []

        def action_with_stop_check(**kwargs):
            stop_check = kwargs['stop_check']
            stop_check_values.append(stop_check())
            time.sleep(0.1)
            stop_check_values.append(stop_check())
            return ActionResult(success=True, message="ok")

        _start(runner, action_name="sync-to-array", service_method=action_with_stop_check)

        time.sleep(0.05)
        runner.stop_action()

        runner._thread.join(timeout=2)

        assert stop_check_values[0] is False
        assert stop_check_values[1] is True


# ============================================================================
# MaintenanceRunner - get_status_dict
# ============================================================================

class TestMaintenanceRunnerStatusDict:
    """Test status dictionary output."""

    def test_idle_status_dict(self):
        runner = MaintenanceRunner()
        status = runner.get_status_dict()
        assert status["state"] == "idle"
        assert status["is_running"] is False

    def test_running_status_dict(self):
        runner = MaintenanceRunner()
        hold = threading.Event()

        def slow(**kwargs):
            hold.wait(timeout=5)
            return ActionResult(success=True, message="ok")

        _start(runner, service_method=slow, file_count=5)

        status = runner.get_status_dict()
        assert status["state"] == "running"
        assert status["is_running"] is True
        assert status["action_name"] == "protect-with-backup"
        assert "5" in status["action_display"]
        assert status["file_count"] == 5

        hold.set()
        runner._thread.join(timeout=2)

    def test_completed_status_dict_has_result(self):
        runner = MaintenanceRunner()
        result = ActionResult(success=True, message="Moved 2 file(s) to array", affected_count=2, errors=[])

        _start(runner, action_name="sync-to-array", service_method=lambda **kwargs: result, file_count=2)
        runner._thread.join(timeout=2)

        status = runner.get_status_dict()
        assert status["state"] == "completed"
        assert status["result_message"] == "Moved 2 file(s) to array"
        assert status["affected_count"] == 2
        assert status["result_success"] is True
        assert status["duration_seconds"] >= 0


# ============================================================================
# MaintenanceRunner - on_complete callback
# ============================================================================

class TestMaintenanceRunnerOnComplete:
    """Test on_complete callback invocation."""

    def test_on_complete_called_after_success(self):
        runner = MaintenanceRunner()
        callback = MagicMock()

        _start(
            runner,
            action_name="delete-plexcached",
            service_method=lambda **kwargs: ActionResult(success=True, message="ok"),
            on_complete=callback,
        )
        runner._thread.join(timeout=2)
        callback.assert_called_once()

    def test_on_complete_called_after_failure(self):
        runner = MaintenanceRunner()
        callback = MagicMock()

        def failing(**kwargs):
            raise RuntimeError("fail")

        _start(runner, action_name="sync-to-array", service_method=failing, on_complete=callback)
        runner._thread.join(timeout=2)
        callback.assert_called_once()


# ============================================================================
# Action Display
# ============================================================================

class TestActionDisplay:

    def test_all_async_actions_have_display(self):
        for action in ASYNC_ACTIONS:
            assert action in ACTION_DISPLAY, f"Missing display for {action}"

    def test_display_format_with_count(self):
        display = ACTION_DISPLAY["protect-with-backup"].format(count=3)
        assert "3" in display
        assert "file" in display.lower()


# ============================================================================
# stop_check in MaintenanceService methods
# ============================================================================

class TestStopCheckInServiceMethods:
    """Test that stop_check breaks out of file loops."""

    def test_restore_plexcached_stops_early(self, tmp_path):
        from web.services.maintenance_service import MaintenanceService
        service = MaintenanceService()

        files = []
        for i in range(5):
            f = tmp_path / f"file{i}.mkv.plexcached"
            f.write_text("fake")
            files.append(str(f))

        call_count = 0
        def stop_after_two():
            nonlocal call_count
            call_count += 1
            return call_count > 2

        result = service.restore_plexcached(files, dry_run=False, stop_check=stop_after_two)
        assert result.affected_count <= 2

    def test_delete_plexcached_stops_early(self, tmp_path):
        from web.services.maintenance_service import MaintenanceService
        service = MaintenanceService()

        files = []
        for i in range(5):
            f = tmp_path / f"file{i}.mkv.plexcached"
            f.write_text("fake")
            files.append(str(f))

        call_count = 0
        def stop_after_two():
            nonlocal call_count
            call_count += 1
            return call_count > 2

        result = service.delete_plexcached(files, dry_run=False, stop_check=stop_after_two)
        assert result.affected_count <= 2

    def test_stop_check_none_processes_all(self, tmp_path):
        from web.services.maintenance_service import MaintenanceService
        service = MaintenanceService()

        files = []
        for i in range(3):
            f = tmp_path / f"file{i}.mkv.plexcached"
            f.write_text("fake")
            files.append(str(f))

        result = service.delete_plexcached(files, dry_run=False, stop_check=None)
        assert result.affected_count == 3

    def test_protect_with_backup_accepts_stop_check(self):
        import inspect
        from web.services.maintenance_service import MaintenanceService
        sig = inspect.signature(MaintenanceService.protect_with_backup)
        assert "stop_check" in sig.parameters

    def test_sync_to_array_accepts_stop_check(self):
        import inspect
        from web.services.maintenance_service import MaintenanceService
        sig = inspect.signature(MaintenanceService.sync_to_array)
        assert "stop_check" in sig.parameters

    def test_fix_with_backup_accepts_stop_check(self):
        import inspect
        from web.services.maintenance_service import MaintenanceService
        sig = inspect.signature(MaintenanceService.fix_with_backup)
        assert "stop_check" in sig.parameters


# ============================================================================
# Mutual Exclusion in OperationRunner
# ============================================================================

class TestOperationRunnerMutualExclusion:
    """Test that OperationRunner checks MaintenanceRunner before starting."""

    def test_operation_blocked_by_maintenance(self):
        from web.services.operation_runner import OperationRunner

        runner = OperationRunner()
        mock_maint = MagicMock(is_running=True)

        with patch("web.services.maintenance_runner.get_maintenance_runner", return_value=mock_maint):
            started = runner.start_operation(dry_run=True)
        assert started is False

    def test_operation_allowed_when_maintenance_idle(self):
        from web.services.operation_runner import OperationRunner

        runner = OperationRunner()
        mock_maint = MagicMock(is_running=False)

        with patch("web.services.maintenance_runner.get_maintenance_runner", return_value=mock_maint):
            with patch.object(runner, '_run_operation'):
                started = runner.start_operation(dry_run=True)
                assert started is True
                # Clean up state
                with runner._lock:
                    runner._state = runner._state.__class__("idle")
