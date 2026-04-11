"""Tests for core.pinned_media.resolve_pins_to_paths().

Uses a minimal fake plexapi interface:
- FakePart.file → the "file path" string
- FakeMedia.parts + videoResolution
- FakeMovie.media
- FakeEpisode.media + title
- FakeSeason.episodes() returns list
- FakeShow.seasons() returns list; each season.episodes() returns list
- FakePlexServer.fetchItem(key) returns one of the above or raises NotFound
"""

import logging

import pytest

from core.pinned_media import (
    PinnedMediaTracker,
    resolve_pins_to_paths,
)


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class FakeNotFound(Exception):
    """Stand-in for plexapi.exceptions.NotFound."""
    pass


class FakePart:
    def __init__(self, file):
        self.file = file
        self.size = 1_000_000_000


class FakeMedia:
    def __init__(self, resolution, *files, bitrate=1000):
        self.videoResolution = resolution
        self.bitrate = bitrate
        self.parts = [FakePart(f) for f in files]


class FakeMovie:
    def __init__(self, title, medias):
        self.title = title
        self.media = medias


class FakeEpisode:
    def __init__(self, title, medias):
        self.title = title
        self.media = medias


class FakeSeason:
    def __init__(self, title, episodes):
        self.title = title
        self._episodes = episodes

    def episodes(self):
        return self._episodes


class FakeShow:
    def __init__(self, title, seasons):
        self.title = title
        self._seasons = seasons

    def seasons(self):
        return self._seasons


class FakePlexServer:
    """Maps int rating_key → item. Raises FakeNotFound for misses."""

    def __init__(self, items):
        # items is a dict {int: item}
        self._items = items

    def fetchItem(self, key):
        if key in self._items:
            return self._items[key]
        raise FakeNotFound(f"rating_key={key} not found")


@pytest.fixture(autouse=True)
def _patch_notfound(monkeypatch):
    """Force resolve_pins_to_paths to treat FakeNotFound as the orphan signal.

    The module does a lazy `from plexapi.exceptions import NotFound` inside
    the function. We can't easily monkeypatch that, so the function already
    falls back to `Exception` when plexapi is missing — meaning FakeNotFound
    (a subclass of Exception) will be caught and trigger orphan removal.
    """
    # No-op — just documenting the behavior above.
    yield


@pytest.fixture
def tracker(tmp_path):
    return PinnedMediaTracker(str(tmp_path / "pinned_media.json"))


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestResolveMovie:
    def test_single_movie_single_version(self, tracker):
        movie = FakeMovie("Matrix", [FakeMedia("1080", "/plex/movies/Matrix.mkv")])
        server = FakePlexServer({100: movie})
        tracker.add_pin("100", "movie", "Matrix")

        resolved, orphaned = resolve_pins_to_paths(server, tracker, "highest")

        assert orphaned == []
        assert len(resolved) == 1
        path, rk, pin_type = resolved[0]
        assert path == "/plex/movies/Matrix.mkv"
        assert rk == "100"
        assert pin_type == "movie"

    def test_movie_multi_version_picks_highest(self, tracker):
        movie = FakeMovie("Dune", [
            FakeMedia("1080", "/plex/Dune.1080p.mkv"),
            FakeMedia("4k", "/plex/Dune.4k.mkv"),
        ])
        server = FakePlexServer({42: movie})
        tracker.add_pin("42", "movie", "Dune")

        resolved, _ = resolve_pins_to_paths(server, tracker, "highest")
        assert [p for p, _, _ in resolved] == ["/plex/Dune.4k.mkv"]

    def test_movie_multi_version_picks_lowest(self, tracker):
        movie = FakeMovie("Dune", [
            FakeMedia("1080", "/plex/Dune.1080p.mkv"),
            FakeMedia("4k", "/plex/Dune.4k.mkv"),
        ])
        server = FakePlexServer({42: movie})
        tracker.add_pin("42", "movie", "Dune")

        resolved, _ = resolve_pins_to_paths(server, tracker, "lowest")
        assert [p for p, _, _ in resolved] == ["/plex/Dune.1080p.mkv"]


class TestResolveEpisode:
    def test_single_episode(self, tracker):
        episode = FakeEpisode(
            "S01E01",
            [FakeMedia("1080", "/plex/office/S01E01.mkv")],
        )
        server = FakePlexServer({500: episode})
        tracker.add_pin("500", "episode", "The Office S01E01")

        resolved, _ = resolve_pins_to_paths(server, tracker, "highest")
        assert len(resolved) == 1
        assert resolved[0][0] == "/plex/office/S01E01.mkv"
        assert resolved[0][2] == "episode"


class TestResolveSeason:
    def test_season_resolves_all_episodes(self, tracker):
        ep1 = FakeEpisode("E01", [FakeMedia("1080", "/plex/s2/E01.mkv")])
        ep2 = FakeEpisode("E02", [FakeMedia("1080", "/plex/s2/E02.mkv")])
        ep3 = FakeEpisode("E03", [FakeMedia("1080", "/plex/s2/E03.mkv")])
        season = FakeSeason("S02", [ep1, ep2, ep3])
        server = FakePlexServer({200: season})
        tracker.add_pin("200", "season", "Office S2")

        resolved, _ = resolve_pins_to_paths(server, tracker, "highest")

        paths = [p for p, _, _ in resolved]
        assert paths == [
            "/plex/s2/E01.mkv",
            "/plex/s2/E02.mkv",
            "/plex/s2/E03.mkv",
        ]
        # Every resolved entry has the season's rating_key and type
        assert all(rk == "200" for _, rk, _ in resolved)
        assert all(pin_type == "season" for _, _, pin_type in resolved)

    def test_season_with_multi_version_episode_picks_one(self, tracker):
        """Multi-version episode → one file per episode, not all versions."""
        ep = FakeEpisode("E01", [
            FakeMedia("1080", "/plex/E01.1080p.mkv"),
            FakeMedia("4k", "/plex/E01.4k.mkv"),
        ])
        season = FakeSeason("S1", [ep])
        server = FakePlexServer({1: season})
        tracker.add_pin("1", "season", "S1")

        resolved, _ = resolve_pins_to_paths(server, tracker, "highest")
        paths = [p for p, _, _ in resolved]
        assert paths == ["/plex/E01.4k.mkv"]

    def test_season_skips_episode_without_media(self, tracker, caplog):
        ep_ok = FakeEpisode("E01", [FakeMedia("1080", "/plex/E01.mkv")])
        ep_broken = FakeEpisode("E02", [])  # no media → select_media_version raises
        season = FakeSeason("S1", [ep_ok, ep_broken])
        server = FakePlexServer({1: season})
        tracker.add_pin("1", "season", "S1")

        with caplog.at_level(logging.WARNING):
            resolved, _ = resolve_pins_to_paths(server, tracker, "highest")

        paths = [p for p, _, _ in resolved]
        assert paths == ["/plex/E01.mkv"]
        assert any("E02" in r.message for r in caplog.records)


class TestResolveShow:
    def test_show_walks_all_seasons_and_episodes(self, tracker):
        s1 = FakeSeason("S1", [
            FakeEpisode("S1E01", [FakeMedia("1080", "/plex/S1E01.mkv")]),
            FakeEpisode("S1E02", [FakeMedia("1080", "/plex/S1E02.mkv")]),
        ])
        s2 = FakeSeason("S2", [
            FakeEpisode("S2E01", [FakeMedia("1080", "/plex/S2E01.mkv")]),
        ])
        show = FakeShow("Office", [s1, s2])
        server = FakePlexServer({1: show})
        tracker.add_pin("1", "show", "The Office")

        resolved, _ = resolve_pins_to_paths(server, tracker, "highest")
        paths = [p for p, _, _ in resolved]
        assert paths == [
            "/plex/S1E01.mkv",
            "/plex/S1E02.mkv",
            "/plex/S2E01.mkv",
        ]

    def test_show_with_empty_seasons(self, tracker):
        show = FakeShow("Empty", [])
        server = FakePlexServer({1: show})
        tracker.add_pin("1", "show", "Empty")

        resolved, _ = resolve_pins_to_paths(server, tracker, "highest")
        assert resolved == []


class TestOrphanCleanup:
    def test_missing_rating_key_is_orphaned_and_removed(self, tracker, caplog):
        server = FakePlexServer({})  # no items
        tracker.add_pin("999", "movie", "Gone")

        with caplog.at_level(logging.WARNING):
            resolved, orphaned = resolve_pins_to_paths(server, tracker, "highest")

        assert resolved == []
        assert orphaned == ["999"]
        # Pin should be removed from the tracker
        assert tracker.get_pin("999") is None
        # Warning logged
        assert any("no longer in Plex" in r.message for r in caplog.records)

    def test_existing_pins_survive_orphan_cleanup(self, tracker):
        movie = FakeMovie("Alive", [FakeMedia("1080", "/plex/Alive.mkv")])
        server = FakePlexServer({100: movie})
        tracker.add_pin("100", "movie", "Alive")
        tracker.add_pin("999", "movie", "Gone")

        resolved, orphaned = resolve_pins_to_paths(server, tracker, "highest")

        assert [p for p, _, _ in resolved] == ["/plex/Alive.mkv"]
        assert orphaned == ["999"]
        assert tracker.is_pinned("100") is True
        assert tracker.is_pinned("999") is False

    def test_empty_tracker_returns_empty(self, tracker):
        server = FakePlexServer({})
        resolved, orphaned = resolve_pins_to_paths(server, tracker, "highest")
        assert resolved == []
        assert orphaned == []


class TestMixedPinTypes:
    def test_movie_plus_show_plus_season(self, tracker):
        movie = FakeMovie("Dune", [FakeMedia("1080", "/plex/Dune.mkv")])
        ep = FakeEpisode("E01", [FakeMedia("1080", "/plex/S1E01.mkv")])
        season = FakeSeason("S1", [ep])
        show = FakeShow(
            "Breaking Bad",
            [FakeSeason("S1", [
                FakeEpisode("BB_S1E01", [FakeMedia("1080", "/plex/BB_S1E01.mkv")])
            ])],
        )
        server = FakePlexServer({1: movie, 2: show, 3: season})
        tracker.add_pin("1", "movie", "Dune")
        tracker.add_pin("2", "show", "BB")
        tracker.add_pin("3", "season", "Office S1")

        resolved, orphaned = resolve_pins_to_paths(server, tracker, "highest")

        paths = sorted(p for p, _, _ in resolved)
        assert paths == sorted([
            "/plex/Dune.mkv",
            "/plex/BB_S1E01.mkv",
            "/plex/S1E01.mkv",
        ])
        assert orphaned == []

    def test_source_type_preserved_per_pin(self, tracker):
        movie = FakeMovie("Dune", [FakeMedia("1080", "/plex/Dune.mkv")])
        season = FakeSeason("S1", [FakeEpisode("E01", [FakeMedia("1080", "/plex/E01.mkv")])])
        server = FakePlexServer({1: movie, 2: season})
        tracker.add_pin("1", "movie", "Dune")
        tracker.add_pin("2", "season", "S1")

        resolved, _ = resolve_pins_to_paths(server, tracker, "highest")

        by_type = {pin_type: path for path, _, pin_type in resolved}
        assert by_type["movie"] == "/plex/Dune.mkv"
        assert by_type["season"] == "/plex/E01.mkv"


class TestPreferenceRespected:
    def test_highest_vs_lowest_across_whole_show(self, tracker):
        def multi_version_ep(name):
            return FakeEpisode(name, [
                FakeMedia("1080", f"/plex/{name}.1080p.mkv"),
                FakeMedia("4k", f"/plex/{name}.4k.mkv"),
            ])
        show = FakeShow("Show", [
            FakeSeason("S1", [multi_version_ep("E01"), multi_version_ep("E02")])
        ])
        server = FakePlexServer({1: show})
        tracker.add_pin("1", "show", "Show")

        highest, _ = resolve_pins_to_paths(server, tracker, "highest")
        assert sorted(p for p, _, _ in highest) == sorted([
            "/plex/E01.4k.mkv", "/plex/E02.4k.mkv"
        ])

        # Reset and try lowest
        tracker.remove_pin("1")
        tracker.add_pin("1", "show", "Show")
        lowest, _ = resolve_pins_to_paths(server, tracker, "lowest")
        assert sorted(p for p, _, _ in lowest) == sorted([
            "/plex/E01.1080p.mkv", "/plex/E02.1080p.mkv"
        ])
