"""Route test for GET /operations/activity (run-grouped Recent Activity).

Verifies the endpoint returns the HTMX partial with run headers, member rows,
and proper data attributes for the dashboard's run-grouped view.

Test isolation: earlier tests sometimes replace ``web.config`` in
``sys.modules`` with a MagicMock. Force-reload affected modules so route
tests run against the real Jinja2Templates instance.
"""

import sys
import importlib
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


def _force_real_modules():
    mocked_names = [
        "web.config",
        "web.routers",
        "web.routers.operations",
        "web.services",
        "web.services.activity_grouping",
        "web",
    ]
    for name in mocked_names:
        mod = sys.modules.get(name)
        if isinstance(mod, MagicMock):
            del sys.modules[name]
    import web  # noqa: F401
    import web.config  # noqa: F401
    import web.routers.operations  # noqa: F401
    import web.services.activity_grouping  # noqa: F401


_force_real_modules()


def _activity_dict(timestamp, action, filename, *, size_bytes=1000, run_id=None, run_source="legacy"):
    return {
        "timestamp": timestamp.isoformat(),
        "time_display": timestamp.strftime("%H:%M:%S"),
        "date_key": timestamp.date().isoformat(),
        "date_display": "Today",
        "action": action,
        "filename": filename,
        "size": f"{size_bytes} B",
        "size_bytes": size_bytes,
        "users": [],
        "run_id": run_id,
        "run_source": run_source,
        "associated_files": [],
    }


@pytest.fixture
def client():
    """Mount only the operations router on a minimal FastAPI app."""
    from web.routers import operations as operations_router

    app = FastAPI()
    app.include_router(operations_router.router, prefix="/operations")
    return TestClient(app)


class TestActivityEndpoint:
    def test_empty_activity_renders_empty_state(self, client):
        # Patch both the runner's recent_activity property and the auxiliary services
        fake_runner = MagicMock()
        fake_runner.recent_activity = []
        fake_settings = MagicMock()
        fake_settings.check_plex_connection.return_value = True
        fake_settings.get_last_run_time.return_value = None
        fake_cache = MagicMock()
        fake_cache.get_user_types.return_value = {}

        with patch("web.routers.operations.get_operation_runner", return_value=fake_runner), \
             patch("web.services.get_settings_service", return_value=fake_settings), \
             patch("web.services.get_cache_service", return_value=fake_cache):
            r = client.get("/operations/activity", headers={"HX-Request": "true"})

        assert r.status_code == 200
        # Empty state renders one of the three contextual messages
        assert any(msg in r.text for msg in ["No recent activity", "Ready to go", "Set up your Plex"])

    def test_grouped_run_renders_header_and_members(self, client):
        now = datetime.now()
        activities = [
            _activity_dict(now + timedelta(seconds=2), "Cached", "Show - S01E02 - Test.mkv",
                           run_id="abc", run_source="web"),
            _activity_dict(now + timedelta(seconds=1), "Cached", "Show - S01E01 - Test.mkv",
                           run_id="abc", run_source="web"),
            _activity_dict(now, "Cached", "Movie.mkv", run_id="abc", run_source="web"),
        ]
        fake_runner = MagicMock()
        fake_runner.recent_activity = activities
        fake_cache = MagicMock()
        fake_cache.get_user_types.return_value = {}

        with patch("web.routers.operations.get_operation_runner", return_value=fake_runner), \
             patch("web.services.get_cache_service", return_value=fake_cache):
            r = client.get("/operations/activity", headers={"HX-Request": "true"})

        assert r.status_code == 200
        # Run header is present with the right run_id and source
        assert 'data-run-id="abc"' in r.text
        assert 'data-run-source="web"' in r.text
        assert "Web UI Run" in r.text
        # Show grouping collapsed two episodes — group badge "2 eps" appears
        assert "2 eps" in r.text
        # Movie singleton is rendered too
        assert "Movie.mkv" in r.text
        # Stat pill for cached files (3 cached total)
        assert "3 cached" in r.text

    def test_separate_runs_get_separate_headers(self, client):
        now = datetime.now()
        activities = [
            _activity_dict(now + timedelta(minutes=10), "Cached", "later.mkv",
                           run_id="run-2", run_source="scheduled"),
            _activity_dict(now, "Restored", "earlier.mkv",
                           run_id="run-1", run_source="cli"),
        ]
        fake_runner = MagicMock()
        fake_runner.recent_activity = activities
        fake_cache = MagicMock()
        fake_cache.get_user_types.return_value = {}

        with patch("web.routers.operations.get_operation_runner", return_value=fake_runner), \
             patch("web.services.get_cache_service", return_value=fake_cache):
            r = client.get("/operations/activity", headers={"HX-Request": "true"})

        assert r.status_code == 200
        assert 'data-run-id="run-2"' in r.text
        assert 'data-run-id="run-1"' in r.text
        assert "Scheduled Run" in r.text
        assert "CLI Run" in r.text

    def test_newest_run_starts_open(self, client):
        now = datetime.now()
        activities = [
            _activity_dict(now + timedelta(minutes=5), "Cached", "a.mkv",
                           run_id="newer", run_source="web"),
            _activity_dict(now, "Cached", "b.mkv",
                           run_id="older", run_source="web"),
        ]
        fake_runner = MagicMock()
        fake_runner.recent_activity = activities
        fake_cache = MagicMock()
        fake_cache.get_user_types.return_value = {}

        with patch("web.routers.operations.get_operation_runner", return_value=fake_runner), \
             patch("web.services.get_cache_service", return_value=fake_cache):
            r = client.get("/operations/activity", headers={"HX-Request": "true"})

        # The newer run header has class "is-open"; older does not
        # (Look for the opening segment of each run header)
        newer_idx = r.text.find('data-run-id="newer"')
        older_idx = r.text.find('data-run-id="older"')
        assert newer_idx != -1 and older_idx != -1
        # Backtrack to find the class on the <tr> opening for "newer"
        newer_tr_start = r.text.rfind("<tr", 0, newer_idx)
        newer_tr = r.text[newer_tr_start:newer_idx]
        assert "is-open" in newer_tr

        older_tr_start = r.text.rfind("<tr", 0, older_idx)
        older_tr = r.text[older_tr_start:older_idx]
        assert "is-open" not in older_tr

    def test_legacy_entries_bucket_into_previous_activity(self, client):
        """Entries without run_id should appear under a 'Previous Activity' bucket."""
        now = datetime.now()
        activities = [
            _activity_dict(now, "Cached", "old.mkv"),  # no run_id, run_source=legacy
        ]
        fake_runner = MagicMock()
        fake_runner.recent_activity = activities
        fake_cache = MagicMock()
        fake_cache.get_user_types.return_value = {}

        with patch("web.routers.operations.get_operation_runner", return_value=fake_runner), \
             patch("web.services.get_cache_service", return_value=fake_cache):
            r = client.get("/operations/activity", headers={"HX-Request": "true"})

        assert r.status_code == 200
        assert "Previous Activity" in r.text

    def test_json_response_when_not_htmx(self, client):
        """Non-HTMX requests get JSON with both raw activity and grouped runs."""
        now = datetime.now()
        activities = [
            _activity_dict(now, "Cached", "a.mkv", run_id="r1", run_source="web"),
        ]
        fake_runner = MagicMock()
        fake_runner.recent_activity = activities
        fake_cache = MagicMock()
        fake_cache.get_user_types.return_value = {}

        with patch("web.routers.operations.get_operation_runner", return_value=fake_runner), \
             patch("web.services.get_cache_service", return_value=fake_cache):
            r = client.get("/operations/activity")

        assert r.status_code == 200
        body = r.json()
        assert "activity" in body
        assert "runs" in body
        assert len(body["runs"]) == 1
        assert body["runs"][0]["run_id"] == "r1"
        assert body["runs"][0]["files_cached"] == 1
