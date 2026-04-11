"""Tests for core.pinned_media.select_media_version().

Exercises the multi-version selector with a minimal fake Plex item/media
interface (just the attributes the selector reads: ``title``, ``media``,
``videoResolution``, ``bitrate``, ``parts[].size``).
"""

import logging

import pytest

from core.pinned_media import select_media_version


class FakePart:
    def __init__(self, size):
        self.size = size


class FakeMedia:
    def __init__(self, resolution, bitrate=1000, size=1_000_000_000):
        self.videoResolution = resolution
        self.bitrate = bitrate
        self.parts = [FakePart(size)]


class FakeItem:
    def __init__(self, title, medias):
        self.title = title
        self.media = medias


@pytest.fixture
def single_1080():
    return FakeItem("Solo", [FakeMedia("1080")])


@pytest.fixture
def two_versions():
    # 4K + 1080p
    return FakeItem("Duo", [FakeMedia("1080", bitrate=8000, size=4_000_000_000),
                            FakeMedia("4k", bitrate=25000, size=15_000_000_000)])


@pytest.fixture
def three_versions():
    # 4K remux, 4K, 1080p
    return FakeItem("Trio", [
        FakeMedia("1080", bitrate=8000, size=4_000_000_000),
        FakeMedia("4k", bitrate=15000, size=12_000_000_000),
        FakeMedia("4k", bitrate=60000, size=60_000_000_000),  # remux
    ])


class TestSingleVersion:
    def test_single_version_returned_regardless_of_preference(self, single_1080):
        for pref in ["highest", "lowest", "1080p", "720p", "4k", "first"]:
            assert select_media_version(single_1080, pref) is single_1080.media[0]

    def test_empty_media_raises(self):
        item = FakeItem("Empty", [])
        with pytest.raises(ValueError, match="no media versions"):
            select_media_version(item, "highest")


class TestHighestLowest:
    def test_highest_two_versions(self, two_versions):
        chosen = select_media_version(two_versions, "highest")
        assert chosen.videoResolution == "4k"

    def test_lowest_two_versions(self, two_versions):
        chosen = select_media_version(two_versions, "lowest")
        assert chosen.videoResolution == "1080"

    def test_highest_tiebreak_by_bitrate(self, three_versions):
        """Two 4K versions — remux (higher bitrate) wins."""
        chosen = select_media_version(three_versions, "highest")
        assert chosen.videoResolution == "4k"
        assert chosen.bitrate == 60000

    def test_lowest_ignores_bitrate_when_resolution_differs(self, three_versions):
        """Lowest resolution wins even if its bitrate is higher than nothing."""
        chosen = select_media_version(three_versions, "lowest")
        assert chosen.videoResolution == "1080"


class TestExactMatch:
    def test_exact_1080p(self, two_versions):
        chosen = select_media_version(two_versions, "1080p")
        assert chosen.videoResolution == "1080"

    def test_exact_4k(self, two_versions):
        chosen = select_media_version(two_versions, "4k")
        assert chosen.videoResolution == "4k"

    def test_exact_miss_falls_back_to_highest(self, two_versions, caplog):
        """Request 720p on a 1080+4K item — falls back to highest (4K) and logs."""
        with caplog.at_level(logging.INFO):
            chosen = select_media_version(two_versions, "720p")
        assert chosen.videoResolution == "4k"
        assert any("720p" in r.message and "falling back" in r.message
                   for r in caplog.records)

    def test_exact_miss_logs_title(self, two_versions, caplog):
        with caplog.at_level(logging.INFO):
            select_media_version(two_versions, "720p")
        assert any("Duo" in r.message for r in caplog.records)


class TestFirst:
    def test_first_returns_index_zero(self, three_versions):
        chosen = select_media_version(three_versions, "first")
        assert chosen is three_versions.media[0]


class TestUnknownPreference:
    def test_unknown_warns_and_returns_first(self, two_versions, caplog):
        with caplog.at_level(logging.WARNING):
            chosen = select_media_version(two_versions, "bananas")
        assert chosen is two_versions.media[0]
        assert any("Unknown" in r.message for r in caplog.records)


class TestCaseInsensitivity:
    def test_uppercase_preference(self, two_versions):
        assert select_media_version(two_versions, "HIGHEST").videoResolution == "4k"
        assert select_media_version(two_versions, "1080P").videoResolution == "1080"
        assert select_media_version(two_versions, "4K").videoResolution == "4k"

    def test_none_preference_defaults_to_highest(self, two_versions):
        assert select_media_version(two_versions, None).videoResolution == "4k"

    def test_empty_string_defaults_to_highest(self, two_versions):
        assert select_media_version(two_versions, "").videoResolution == "4k"


class TestResolutionNormalization:
    def test_2160_treated_as_4k(self):
        item = FakeItem("X", [FakeMedia("1080"), FakeMedia("2160")])
        chosen = select_media_version(item, "4k")
        assert chosen.videoResolution == "2160"

    def test_1080p_suffix_handled(self):
        item = FakeItem("X", [FakeMedia("720"), FakeMedia("1080p")])
        chosen = select_media_version(item, "1080p")
        assert chosen.videoResolution == "1080p"

    def test_sd_is_lowest(self):
        item = FakeItem("X", [FakeMedia("1080"), FakeMedia("sd")])
        assert select_media_version(item, "lowest").videoResolution == "sd"
        assert select_media_version(item, "highest").videoResolution == "1080"


class TestTiebreakBySize:
    def test_same_resolution_same_bitrate_larger_wins_on_highest(self):
        """If resolution and bitrate tie, larger file wins highest (less compression)."""
        item = FakeItem("X", [
            FakeMedia("1080", bitrate=8000, size=4_000_000_000),
            FakeMedia("1080", bitrate=8000, size=8_000_000_000),
        ])
        chosen = select_media_version(item, "highest")
        assert chosen.parts[0].size == 8_000_000_000
