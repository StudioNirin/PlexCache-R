"""Tests for PinnedMediaTracker — the rating_key-keyed JSONTracker subclass."""

import json
import os
import threading

import pytest

from core.pinned_media import PinnedMediaTracker, VALID_PIN_TYPES


@pytest.fixture
def tracker_file(tmp_path):
    return str(tmp_path / "pinned_media.json")


@pytest.fixture
def tracker(tracker_file):
    return PinnedMediaTracker(tracker_file)


class TestAddPin:
    def test_add_pin_creates_entry(self, tracker):
        added = tracker.add_pin("12345", "show", "The Office")
        assert added is True
        pin = tracker.get_pin("12345")
        assert pin is not None
        assert pin["rating_key"] == "12345"
        assert pin["type"] == "show"
        assert pin["title"] == "The Office"
        assert pin["added_by"] == "web"
        assert "added_at" in pin

    def test_add_pin_idempotent(self, tracker):
        assert tracker.add_pin("12345", "show", "The Office") is True
        assert tracker.add_pin("12345", "show", "The Office") is False
        assert len(tracker.list_pins()) == 1

    def test_add_pin_accepts_int_rating_key_as_string(self, tracker):
        tracker.add_pin(12345, "movie", "Matrix")
        assert tracker.is_pinned("12345") is True
        assert tracker.is_pinned(12345) is True  # str() coerces

    def test_add_pin_rejects_invalid_type(self, tracker):
        with pytest.raises(ValueError, match="Invalid pin type"):
            tracker.add_pin("1", "artist", "Rush")

    @pytest.mark.parametrize("pin_type", sorted(VALID_PIN_TYPES))
    def test_add_pin_accepts_all_valid_types(self, tracker, pin_type):
        tracker.add_pin("1", pin_type, "Item")
        assert tracker.get_pin("1")["type"] == pin_type

    def test_added_by_recorded(self, tracker):
        tracker.add_pin("1", "movie", "A", added_by="cli")
        assert tracker.get_pin("1")["added_by"] == "cli"


class TestRemovePin:
    def test_remove_pin_returns_true_when_present(self, tracker):
        tracker.add_pin("1", "movie", "A")
        assert tracker.remove_pin("1") is True
        assert tracker.get_pin("1") is None

    def test_remove_pin_returns_false_when_absent(self, tracker):
        assert tracker.remove_pin("nope") is False

    def test_remove_pin_persists(self, tracker, tracker_file):
        tracker.add_pin("1", "movie", "A")
        tracker.remove_pin("1")
        # New tracker instance re-reads from disk
        t2 = PinnedMediaTracker(tracker_file)
        assert t2.get_pin("1") is None


class TestPersistence:
    def test_round_trip(self, tracker, tracker_file):
        tracker.add_pin("1", "show", "The Office")
        tracker.add_pin("2", "movie", "Matrix")
        tracker.add_pin("3", "episode", "Pilot")

        t2 = PinnedMediaTracker(tracker_file)
        pins = t2.list_pins()
        assert len(pins) == 3
        assert {p["rating_key"] for p in pins} == {"1", "2", "3"}

    def test_file_is_valid_json_with_indent(self, tracker, tracker_file):
        tracker.add_pin("1", "show", "The Office")
        with open(tracker_file, "r") as f:
            content = f.read()
        # Codebase convention: indent=2
        assert "\n" in content  # not one long line
        data = json.loads(content)
        assert "1" in data

    def test_list_pins_sorted_by_added_at(self, tracker):
        import time
        tracker.add_pin("1", "show", "First")
        time.sleep(0.01)
        tracker.add_pin("2", "show", "Second")
        time.sleep(0.01)
        tracker.add_pin("3", "show", "Third")

        pins = tracker.list_pins()
        assert [p["title"] for p in pins] == ["First", "Second", "Third"]

    def test_list_pins_returns_copies(self, tracker):
        tracker.add_pin("1", "show", "Original")
        pins = tracker.list_pins()
        pins[0]["title"] = "MUTATED"
        assert tracker.get_pin("1")["title"] == "Original"

    def test_pinned_rating_keys_set(self, tracker):
        tracker.add_pin("1", "show", "A")
        tracker.add_pin("2", "movie", "B")
        assert tracker.pinned_rating_keys() == {"1", "2"}


class TestThreadSafety:
    def test_concurrent_adds(self, tracker):
        """Spawn 10 threads each adding 10 unique pins — all should persist."""
        def worker(start: int):
            for i in range(10):
                rk = str(start * 10 + i)
                tracker.add_pin(rk, "movie", f"Movie {rk}")

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(tracker.list_pins()) == 100

    def test_concurrent_add_remove(self, tracker):
        """Adds and removes should not corrupt state."""
        for i in range(50):
            tracker.add_pin(str(i), "movie", f"Movie {i}")

        def remover():
            for i in range(25):
                tracker.remove_pin(str(i))

        def adder():
            for i in range(50, 75):
                tracker.add_pin(str(i), "movie", f"Movie {i}")

        t1 = threading.Thread(target=remover)
        t2 = threading.Thread(target=adder)
        t1.start(); t2.start()
        t1.join(); t2.join()

        keys = tracker.pinned_rating_keys()
        # 25-49 remain (25 items) + 50-74 added (25 items) = 50 total
        assert len(keys) == 50
        for i in range(25, 75):
            assert str(i) in keys


class TestDisabledBaseMethods:
    def test_get_entry_raises(self, tracker):
        with pytest.raises(NotImplementedError):
            tracker.get_entry("/some/path.mkv")

    def test_remove_entry_raises(self, tracker):
        with pytest.raises(NotImplementedError):
            tracker.remove_entry("/some/path.mkv")

    def test_mark_cached_raises(self, tracker):
        with pytest.raises(NotImplementedError):
            tracker.mark_cached("/some/path.mkv", "pinned")

    def test_mark_uncached_raises(self, tracker):
        with pytest.raises(NotImplementedError):
            tracker.mark_uncached("/some/path.mkv")

    def test_get_cached_entries_raises(self, tracker):
        with pytest.raises(NotImplementedError):
            tracker.get_cached_entries()

    def test_cleanup_stale_entries_raises(self, tracker):
        with pytest.raises(NotImplementedError):
            tracker.cleanup_stale_entries(max_days_since_seen=7)


class TestInitialLoad:
    def test_load_missing_file_empty(self, tracker_file):
        assert not os.path.exists(tracker_file)
        t = PinnedMediaTracker(tracker_file)
        assert t.list_pins() == []

    def test_load_existing_file(self, tracker_file):
        with open(tracker_file, "w") as f:
            json.dump({
                "1": {
                    "rating_key": "1",
                    "type": "show",
                    "title": "Preloaded",
                    "added_at": "2026-04-11T00:00:00",
                    "added_by": "web",
                }
            }, f)
        t = PinnedMediaTracker(tracker_file)
        assert t.is_pinned("1") is True
        assert t.get_pin("1")["title"] == "Preloaded"

    def test_load_malformed_file_falls_back_to_empty(self, tracker_file):
        with open(tracker_file, "w") as f:
            f.write("not json{")
        t = PinnedMediaTracker(tracker_file)
        assert t.list_pins() == []
