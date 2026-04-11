"""Pinned media tracking and version resolution.

Users can pin a show, season, episode, or movie to the cache so it is always
kept cached and never evicted, regardless of OnDeck/Watchlist state or priority
scoring. Backing store is ``data/pinned_media.json``, keyed by Plex rating_key.

Divergence from OnDeck/Watchlist gathering
-------------------------------------------
``core/plex_api.py`` currently caches *every* Media version attached to an
OnDeck or Watchlist item (1080p + 4K + remux). That behavior was added
intentionally in commit 2d8a587 ("Add multi-version (4K) media caching
support") so Plex can serve any version to the active client.

Pinned media deliberately diverges from that: ``select_media_version()`` picks
exactly one Media per item based on a global user preference. The pinned
use case is "set it once, forget it" ambient playback (e.g. a show used as
background noise while falling asleep) — caching every version would double
or triple the cache footprint, which is almost never what the user wants.

If the user later asks for per-pin version overrides, extend the pin record
with an optional ``media_id`` field. The global rule remains the default.
"""

import logging
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional

from core.file_operations import JSONTracker


VALID_PIN_TYPES = {"show", "season", "episode", "movie"}

VALID_PREFERENCES = {"highest", "lowest", "1080p", "720p", "4k", "first"}

# Numeric rank for resolution comparison. Higher = better.
_RESOLUTION_RANK: Dict[str, int] = {
    "4k": 4,
    "1080": 3,
    "720": 2,
    "480": 1,
    "sd": 0,
}

# Maps user-facing exact-match preferences to normalized resolution keys.
_EXACT_PREFERENCE_MAP: Dict[str, str] = {
    "1080p": "1080",
    "720p": "720",
    "4k": "4k",
}


def _normalize_resolution(value: Any) -> str:
    """Normalize a Plex videoResolution string to our canonical key.

    Plex reports videoResolution as strings like "1080", "720", "4k", "sd".
    This helper also handles "1080p", "2160", and unknown values.
    """
    v = str(value or "").strip().lower().rstrip("p")
    if v in ("4k", "2160"):
        return "4k"
    if v in ("1080", "720", "480"):
        return v
    return "sd"


def _media_total_size(media: Any) -> int:
    """Return total byte size across all parts of a Media object."""
    total = 0
    for part in getattr(media, "parts", None) or []:
        total += getattr(part, "size", 0) or 0
    return total


def _media_sort_key(media: Any) -> tuple:
    """Sort key for Media objects: (resolution_rank, bitrate, total_size).

    Used to pick the "best" or "worst" version. Bitrate is the first
    tiebreaker (higher bitrate = higher quality at the same resolution),
    total file size is the second (remux files are larger than x265 at
    the same bitrate).
    """
    rank = _RESOLUTION_RANK.get(
        _normalize_resolution(getattr(media, "videoResolution", "")), 0
    )
    bitrate = getattr(media, "bitrate", 0) or 0
    return (rank, bitrate, _media_total_size(media))


def select_media_version(item: Any, preference: str = "highest") -> Any:
    """Pick exactly one Media object from a Plex item per the user preference.

    Args:
        item: A plexapi Video/Movie/Episode. Must expose ``.media`` list.
        preference: One of ``highest``, ``lowest``, ``1080p``, ``720p``,
            ``4k``, ``first``. Case-insensitive. Unknown values fall back
            to ``first`` with a warning.

    Returns:
        The chosen Media object.

    Raises:
        ValueError: if the item has no media attached.
    """
    medias = list(getattr(item, "media", None) or [])
    if not medias:
        title = getattr(item, "title", "?")
        raise ValueError(f"Pinned item '{title}' has no media versions")

    if len(medias) == 1:
        return medias[0]

    pref = (preference or "highest").strip().lower()
    title = getattr(item, "title", "?")

    if pref == "first":
        return medias[0]

    if pref in _EXACT_PREFERENCE_MAP:
        target = _EXACT_PREFERENCE_MAP[pref]
        matches = [
            m for m in medias
            if _normalize_resolution(getattr(m, "videoResolution", "")) == target
        ]
        if matches:
            # Tiebreak: highest bitrate, then largest file
            chosen = sorted(matches, key=_media_sort_key, reverse=True)[0]
            return chosen
        # Exact-match miss — fall back to highest and log
        logging.info(
            f"Pinned '{title}': no {pref} version found among "
            f"{len(medias)} versions, falling back to highest"
        )
        pref = "highest"

    if pref in ("highest", "lowest"):
        reverse = pref == "highest"
        return sorted(medias, key=_media_sort_key, reverse=reverse)[0]

    logging.warning(
        f"Unknown pinned_preferred_resolution={preference!r}, using first media"
    )
    return medias[0]


class PinnedMediaTracker(JSONTracker):
    """Tracks user-pinned media items keyed by Plex rating_key.

    Unlike other ``JSONTracker`` subclasses (OnDeck, Watchlist) which key
    ``_data`` by file path, this tracker keys by ``rating_key`` because pins
    identify items at the Plex metadata level, not the filesystem level. A
    single rating_key can resolve to many paths (a show → many episodes) and
    to different paths over time (quality upgrades).

    The path-keyed base methods (``get_entry``, ``remove_entry``,
    ``mark_cached``, ``mark_uncached``, ``get_cached_entries``,
    ``cleanup_stale_entries``) are disabled to avoid accidental misuse.
    Callers should use the explicit rating-key API:
    ``add_pin``, ``remove_pin``, ``get_pin``, ``list_pins``, ``is_pinned``.

    Entry shape::

        {
          "rating_key": "12345",
          "type": "show",         # show | season | episode | movie
          "title": "The Office",
          "added_at": "2026-04-11T...",
          "added_by": "web",      # web | cli
        }
    """

    def __init__(self, tracker_file: str):
        super().__init__(tracker_file, "pinned_media")

    # ------------------------------------------------------------------
    # Public API — all operations are keyed by rating_key (str)
    # ------------------------------------------------------------------

    def add_pin(
        self,
        rating_key: str,
        pin_type: str,
        title: str,
        added_by: str = "web",
    ) -> bool:
        """Add a pin. Idempotent: returns False if the key was already pinned.

        Args:
            rating_key: Plex rating_key (stringified).
            pin_type: One of ``show``, ``season``, ``episode``, ``movie``.
            title: Human-readable title for display (can include scope suffix
                like "The Office — S3" for season pins).
            added_by: ``web`` or ``cli``.

        Returns:
            True if the pin was newly added, False if it already existed.
        """
        if pin_type not in VALID_PIN_TYPES:
            raise ValueError(
                f"Invalid pin type {pin_type!r}; must be one of {sorted(VALID_PIN_TYPES)}"
            )
        key = str(rating_key)
        with self._lock:
            if key in self._data:
                return False
            self._data[key] = {
                "rating_key": key,
                "type": pin_type,
                "title": title,
                "added_at": datetime.now().isoformat(),
                "added_by": added_by,
            }
            self._save()
            logging.info(f"Pinned {pin_type}: {title} (rating_key={key})")
            return True

    def remove_pin(self, rating_key: str) -> bool:
        """Remove a pin. Returns True if removed, False if it wasn't pinned."""
        key = str(rating_key)
        with self._lock:
            if key not in self._data:
                return False
            entry = self._data.pop(key)
            self._save()
            logging.info(
                f"Unpinned {entry.get('type', '?')}: "
                f"{entry.get('title', '?')} (rating_key={key})"
            )
            return True

    def get_pin(self, rating_key: str) -> Optional[Dict[str, Any]]:
        """Return a copy of the pin entry, or None."""
        key = str(rating_key)
        with self._lock:
            entry = self._data.get(key)
            return dict(entry) if entry else None

    def is_pinned(self, rating_key: str) -> bool:
        """Fast O(1) membership check."""
        with self._lock:
            return str(rating_key) in self._data

    def list_pins(self) -> List[Dict[str, Any]]:
        """Return all pins as copies, sorted by added_at ascending."""
        with self._lock:
            entries = [dict(e) for e in self._data.values()]
        entries.sort(key=lambda e: e.get("added_at", ""))
        return entries

    def pinned_rating_keys(self) -> set:
        """Return the set of all pinned rating_keys as strings."""
        with self._lock:
            return set(self._data.keys())

    # ------------------------------------------------------------------
    # Disabled path-keyed base methods — prevents accidental misuse
    # ------------------------------------------------------------------

    def get_entry(self, file_path: str):
        raise NotImplementedError(
            "PinnedMediaTracker is keyed by rating_key, not file path. "
            "Use get_pin(rating_key)."
        )

    def remove_entry(self, file_path: str):
        raise NotImplementedError(
            "PinnedMediaTracker is keyed by rating_key, not file path. "
            "Use remove_pin(rating_key)."
        )

    def mark_cached(self, file_path: str, source: str, cached_at: Optional[str] = None):
        raise NotImplementedError(
            "PinnedMediaTracker does not track cache state per file — "
            "pins are abstract metadata identifiers."
        )

    def mark_uncached(self, file_path: str):
        raise NotImplementedError(
            "PinnedMediaTracker does not track cache state per file."
        )

    def get_cached_entries(self):
        raise NotImplementedError(
            "PinnedMediaTracker does not track cache state per file."
        )

    def cleanup_stale_entries(self, max_days_since_seen: int = 7) -> int:
        raise NotImplementedError(
            "PinnedMediaTracker cleanup is orphan-based (rating_key no longer "
            "resolvable in Plex), not time-based. Use remove_pin() from the "
            "gather-phase orphan check."
        )
