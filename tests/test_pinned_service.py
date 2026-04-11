"""Phase 3 tests for web/services/pinned_service.PinnedService.

Locks the contract the Cache service + routes will build on:
- budget_check blocks toggle when pinning would exceed the effective budget
- toggle_pin is idempotent (rapid double-click = two toggles that cancel out)
- list_pins_with_metadata returns one row per tracker entry with size data
- resolve_all_to_cache_paths returns cache-form paths via path_mappings

plexapi is mocked as a MagicMock in sys.modules (see conftest.py) so the
``from plexapi.server import PlexServer`` import in ``_get_plex_server()``
succeeds. Each test patches ``_get_plex_server`` directly with a fake server.
"""

import json
import os
from unittest.mock import patch, MagicMock

import pytest

# conftest.py handles fcntl/apscheduler mocks and sys.path setup


class FakePart:
    def __init__(self, file, size=1_000_000_000):
        self.file = file
        self.size = size


class FakeMedia:
    def __init__(self, resolution="1080", files=("/plex/movies/A.mkv",), bitrate=1000):
        self.videoResolution = resolution
        self.bitrate = bitrate
        self.parts = [FakePart(f) for f in files]


class FakeMovie:
    def __init__(self, rating_key, title, medias):
        self.ratingKey = rating_key
        self.title = title
        self.year = 2020
        self.media = medias
        self.librarySectionTitle = "Movies"


class FakeShow:
    def __init__(self, rating_key, title, seasons):
        self.ratingKey = rating_key
        self.title = title
        self.year = 2010
        self._seasons = seasons
        self.librarySectionTitle = "TV Shows"

    def seasons(self):
        return self._seasons


class FakeSeason:
    def __init__(self, rating_key, title, episodes):
        self.ratingKey = rating_key
        self.title = title
        self.leafCount = len(episodes)
        self._episodes = episodes

    def episodes(self):
        return self._episodes


class FakeEpisode:
    def __init__(self, rating_key, title, medias, index=1, season=1):
        self.ratingKey = rating_key
        self.title = title
        self.index = index
        self.parentIndex = season
        self.media = medias


class FakePlexServer:
    def __init__(self, items=None, search_results=None):
        self._items = items or {}
        self._search_results = search_results or {}

    def fetchItem(self, key):
        return self._items[int(key)]

    def search(self, query, mediatype=None, limit=25):
        return list(self._search_results.get(mediatype, []))


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _write_settings(tmp_path, extra=None):
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    settings = {
        "path_mappings": [
            {
                "name": "Movies",
                "plex_path": "/plex/movies",
                "real_path": "/mnt/user/media/Movies",
                "cache_path": "/mnt/cache/media/Movies",
                "cacheable": True,
                "enabled": True,
            },
        ],
        "cache_limit": "10GB",
        "min_free_space": "",
        "pinned_preferred_resolution": "highest",
    }
    if extra:
        settings.update(extra)

    settings_file = tmp_path / "plexcache_settings.json"
    settings_file.write_text(json.dumps(settings), encoding="utf-8")
    return settings_file, data_dir


def _make_service(tmp_path, extra_settings=None, plex_server=None):
    settings_file, data_dir = _write_settings(tmp_path, extra_settings)

    patches = [
        patch("web.services.pinned_service.logger"),  # silence noise
        patch("web.dependencies.DATA_DIR", data_dir),
        patch("web.config.DATA_DIR", data_dir, create=True),
        patch("web.config.SETTINGS_FILE", settings_file, create=True),
    ]
    for p in patches:
        p.start()

    # Build service fresh (no module-level singleton caching)
    from web.services.pinned_service import PinnedService
    svc = PinnedService()
    svc._get_plex_server = lambda: plex_server

    # Point tracker at a clean temp file
    from core.pinned_media import PinnedMediaTracker
    svc._tracker = PinnedMediaTracker(str(data_dir / "pinned_media.json"))

    # Patch settings getter to load our fixture file
    def _get_all():
        return json.loads(settings_file.read_text(encoding="utf-8"))

    mock_settings_service = MagicMock()
    mock_settings_service.get_all = _get_all
    mock_settings_service.get_plex_settings = lambda: {
        "plex_url": "http://fake", "plex_token": "t",
    }

    def _get_settings_service():
        return mock_settings_service

    # Patch the lookup used inside the service methods
    import web.services
    web.services.get_settings_service = _get_settings_service  # type: ignore

    yield svc

    for p in patches:
        p.stop()


@pytest.fixture
def service(tmp_path):
    yield from _make_service(tmp_path)


@pytest.fixture
def service_with_plex(tmp_path):
    movie = FakeMovie(100, "Matrix", [FakeMedia("1080", ("/plex/movies/Matrix.mkv",), bitrate=5000)])
    server = FakePlexServer(
        items={100: movie},
        search_results={"movie": [movie]},
    )
    yield from _make_service(tmp_path, plex_server=server)


# ---------------------------------------------------------------------------
# budget_check
# ---------------------------------------------------------------------------


class TestBudgetCheck:
    def test_no_limit_never_blocks(self, tmp_path):
        gen = _make_service(tmp_path, extra_settings={"cache_limit": ""})
        svc = next(gen)
        result = svc.budget_check()
        assert result["budget_bytes"] == 0
        assert result["effective_budget_bytes"] == 0
        assert result["over_budget"] is False
        assert result["would_exceed"] is False
        try:
            next(gen)
        except StopIteration:
            pass

    def test_empty_tracker_is_zero_bytes(self, service):
        result = service.budget_check()
        assert result["total_pinned_bytes"] == 0
        assert result["over_budget"] is False

    def test_min_free_space_reduces_effective_budget(self, tmp_path):
        gen = _make_service(tmp_path, extra_settings={
            "cache_limit": "10GB",
            "min_free_space": "2GB",
        })
        svc = next(gen)
        result = svc.budget_check()
        # 10 GiB - 2 GiB = 8 GiB
        assert result["budget_bytes"] == 10 * 1024 ** 3
        assert result["headroom_bytes"] == 2 * 1024 ** 3
        assert result["effective_budget_bytes"] == 8 * 1024 ** 3
        try:
            next(gen)
        except StopIteration:
            pass


# ---------------------------------------------------------------------------
# toggle_pin
# ---------------------------------------------------------------------------


class TestTogglePin:
    def test_toggle_adds_when_not_pinned(self, service_with_plex):
        svc = service_with_plex
        result = svc.toggle_pin("100", "movie", "Matrix")
        assert result["is_pinned"] is True
        assert result["error"] is None
        assert svc._tracker.is_pinned("100")

    def test_toggle_removes_when_already_pinned(self, service_with_plex):
        svc = service_with_plex
        svc._tracker.add_pin("100", "movie", "Matrix")
        result = svc.toggle_pin("100", "movie", "Matrix")
        assert result["is_pinned"] is False
        assert svc._tracker.is_pinned("100") is False

    def test_rapid_double_toggle_is_idempotent(self, service_with_plex):
        svc = service_with_plex
        r1 = svc.toggle_pin("100", "movie", "Matrix")
        r2 = svc.toggle_pin("100", "movie", "Matrix")
        assert r1["is_pinned"] is True
        assert r2["is_pinned"] is False
        assert not svc._tracker.list_pins()

    def test_budget_overrun_hard_blocks(self, tmp_path):
        # cache_limit = 1 byte, item = 1 GB → would exceed
        big_movie = FakeMovie(
            200, "Epic",
            [FakeMedia("4k", ("/plex/movies/Epic.mkv",), bitrate=50000)],
        )
        big_movie.media[0].parts[0].size = 5_000_000_000  # 5 GB
        server = FakePlexServer(items={200: big_movie})

        gen = _make_service(
            tmp_path,
            extra_settings={"cache_limit": "1GB"},
            plex_server=server,
        )
        svc = next(gen)
        result = svc.toggle_pin("200", "movie", "Epic")
        assert result["is_pinned"] is False
        assert "cache budget" in (result["error"] or "")
        assert svc._tracker.is_pinned("200") is False
        try:
            next(gen)
        except StopIteration:
            pass

    def test_invalid_pin_type_rejected(self, service_with_plex):
        svc = service_with_plex
        result = svc.toggle_pin("100", "artist", "Bogus")
        assert result["is_pinned"] is False
        assert "Invalid pin type" in (result["error"] or "")


# ---------------------------------------------------------------------------
# resolve_all_to_cache_paths
# ---------------------------------------------------------------------------


class TestResolveAllToCachePaths:
    def test_empty_tracker_returns_empty_set(self, service_with_plex):
        assert service_with_plex.resolve_all_to_cache_paths() == set()

    def test_returns_cache_paths_not_plex_paths(self, service_with_plex):
        svc = service_with_plex
        svc._tracker.add_pin("100", "movie", "Matrix")
        paths = svc.resolve_all_to_cache_paths()
        assert paths == {"/mnt/cache/media/Movies/Matrix.mkv"}

    def test_no_plex_connection_returns_empty_set(self, service):
        # No plex server → None from _get_plex_server
        service._tracker.add_pin("100", "movie", "Matrix")
        assert service.resolve_all_to_cache_paths() == set()


# ---------------------------------------------------------------------------
# list_pins_with_metadata
# ---------------------------------------------------------------------------


class TestListPinsWithMetadata:
    def test_empty_returns_empty_list(self, service):
        assert service.list_pins_with_metadata() == []

    def test_chip_shape(self, service_with_plex, tmp_path):
        svc = service_with_plex
        # Create the cached file so size shows up
        cache_path = "/mnt/cache/media/Movies/Matrix.mkv"
        os.makedirs(os.path.dirname(cache_path), exist_ok=True) if False else None
        svc._tracker.add_pin("100", "movie", "Matrix")
        chips = svc.list_pins_with_metadata()
        assert len(chips) == 1
        chip = chips[0]
        assert chip["rating_key"] == "100"
        assert chip["title"] == "Matrix"
        assert chip["type"] == "movie"
        assert "size_display" in chip
        assert "budget_percent" in chip


# ---------------------------------------------------------------------------
# search / expand
# ---------------------------------------------------------------------------


class TestSearch:
    def test_empty_query_returns_empty(self, service_with_plex):
        assert service_with_plex.search("") == []

    def test_search_returns_results_with_already_pinned_flag(self, service_with_plex):
        svc = service_with_plex
        svc._tracker.add_pin("100", "movie", "Matrix")
        results = svc.search("matrix")
        assert len(results) >= 1
        assert results[0]["rating_key"] == "100"
        assert results[0]["already_pinned"] is True

    def test_search_caps_limit(self, service_with_plex):
        # Provide many results and check the cap
        movies = [
            FakeMovie(rk, f"Movie {rk}", [FakeMedia("1080", (f"/plex/movies/M{rk}.mkv",))])
            for rk in range(1, 40)
        ]
        service_with_plex._get_plex_server = lambda: FakePlexServer(
            items={m.ratingKey: m for m in movies},
            search_results={"movie": movies},
        )
        assert len(service_with_plex.search("m", limit=5)) <= 5


class TestExpand:
    def test_invalid_level_raises(self, service_with_plex):
        with pytest.raises(ValueError):
            service_with_plex.expand("100", "episode")

    def test_show_expands_to_seasons(self, tmp_path):
        ep = FakeEpisode(301, "S1E1", [FakeMedia("1080", ("/plex/show/S1E1.mkv",))])
        season = FakeSeason(201, "Season 1", [ep])
        show = FakeShow(101, "Show", [season])
        server = FakePlexServer(items={101: show, 201: season})

        gen = _make_service(tmp_path, plex_server=server)
        svc = next(gen)
        children = svc.expand("101", "show")
        assert len(children) == 1
        assert children[0]["type"] == "season"
        assert children[0]["rating_key"] == "201"
        try:
            next(gen)
        except StopIteration:
            pass

    def test_season_expands_to_episodes(self, tmp_path):
        ep = FakeEpisode(301, "S1E1", [FakeMedia("1080", ("/plex/show/S1E1.mkv",))])
        season = FakeSeason(201, "Season 1", [ep])
        server = FakePlexServer(items={201: season})

        gen = _make_service(tmp_path, plex_server=server)
        svc = next(gen)
        children = svc.expand("201", "season")
        assert len(children) == 1
        assert children[0]["type"] == "episode"
        assert children[0]["rating_key"] == "301"
        try:
            next(gen)
        except StopIteration:
            pass
