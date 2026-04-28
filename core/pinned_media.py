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
import os
import threading
import time
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

from core.file_operations import JSONTracker

# Retry settings for transient local-Plex network errors during pin resolution.
# Runs inside the audit loop (every ~5 min), so short, bounded retries are
# preferable to letting a single transient timeout spam the error channel.
_PIN_FETCH_MAX_ATTEMPTS = 3
_PIN_FETCH_RETRY_WAIT = 2  # seconds between attempts


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


def _extract_paths(media: Any) -> List[str]:
    """Return the part.file paths from a single Media object."""
    return [p.file for p in (getattr(media, "parts", None) or []) if getattr(p, "file", None)]


def _resolve_episodes(episodes: List[Any], preference: str) -> List[str]:
    """Run select_media_version over a list of episodes and collect file paths.

    Skips (with a warning) any episode that has no media attached.
    """
    paths: List[str] = []
    for ep in episodes:
        try:
            media = select_media_version(ep, preference)
        except ValueError as e:
            logging.warning(
                f"Skipping pinned episode '{getattr(ep, 'title', '?')}': {e}"
            )
            continue
        paths.extend(_extract_paths(media))
    return paths


def _resolve_item_to_paths(item: Any, pin_type: str, preference: str) -> List[str]:
    """Walk a Plex item of the given scope and return all file paths to cache.

    Args:
        item: plexapi object (Movie, Show, Season, or Episode).
        pin_type: One of ``movie``, ``show``, ``season``, ``episode``.
        preference: Value from ``pinned_preferred_resolution``.

    Returns:
        A list of plex-form file paths (strings). Empty list if nothing resolves.
    """
    if pin_type in ("movie", "episode"):
        media = select_media_version(item, preference)
        return _extract_paths(media)

    if pin_type == "season":
        try:
            episodes = list(item.episodes())
        except Exception as e:
            logging.warning(
                f"Failed to enumerate episodes for pinned season "
                f"'{getattr(item, 'title', '?')}': {e}"
            )
            return []
        return _resolve_episodes(episodes, preference)

    if pin_type == "show":
        paths: List[str] = []
        try:
            seasons = list(item.seasons())
        except Exception as e:
            logging.warning(
                f"Failed to enumerate seasons for pinned show "
                f"'{getattr(item, 'title', '?')}': {e}"
            )
            return []
        for season in seasons:
            try:
                episodes = list(season.episodes())
            except Exception as e:
                logging.warning(
                    f"Failed to enumerate episodes for season "
                    f"'{getattr(season, 'title', '?')}': {e}"
                )
                continue
            paths.extend(_resolve_episodes(episodes, preference))
        return paths

    raise ValueError(f"Unknown pin_type: {pin_type!r}")


def resolve_pins_to_paths(
    plex_server: Any,
    tracker: "PinnedMediaTracker",
    preference: str = "highest",
) -> Tuple[List[Tuple[str, str, str]], List[str]]:
    """Resolve every pin in the tracker to its file paths.

    Orphan cleanup: pins whose ``rating_key`` is no longer reachable in Plex
    (item deleted, library removed) are removed from the tracker and returned
    in the ``orphaned`` list for caller-side logging/reporting.

    Args:
        plex_server: A connected plexapi ``PlexServer`` instance. Lazily
            references plexapi so unit tests can pass a mock.
        tracker: The ``PinnedMediaTracker`` instance.
        preference: Value from ``pinned_preferred_resolution``.

    Returns:
        ``(resolved, orphaned)`` where:

        - ``resolved`` is a list of ``(plex_file_path, rating_key, pin_type)``
          tuples. Paths are still in Plex form — the caller is responsible for
          running them through the path modifier to get real paths.
        - ``orphaned`` is a list of rating_key strings that were removed.
    """
    # Lazy import so tests that don't need plexapi can still import this module.
    # Fall back to Exception when plexapi is missing OR when it's been mocked
    # as a MagicMock in test environments (where attribute access returns a
    # MagicMock *instance*, which `except` cannot catch).
    try:
        from plexapi.exceptions import NotFound as _NotFound
        if not (isinstance(_NotFound, type) and issubclass(_NotFound, BaseException)):
            _NotFound = Exception
    except ImportError:
        _NotFound = Exception
    NotFound = _NotFound

    try:
        import requests as _requests
        _Timeout = _requests.Timeout
        _ConnectionError = _requests.ConnectionError
    except ImportError:
        _Timeout = _ConnectionError = Exception
    TransientNetError = (_Timeout, _ConnectionError)

    resolved: List[Tuple[str, str, str]] = []
    orphaned: List[str] = []

    for pin in tracker.list_pins():
        rk = pin["rating_key"]
        pin_type = pin["type"]
        title = pin.get("title", rk)

        item = None
        last_transient_err: Optional[Exception] = None
        for attempt in range(_PIN_FETCH_MAX_ATTEMPTS):
            try:
                # plexapi.fetchItem accepts int or str rating_key
                item = plex_server.fetchItem(int(rk))
                break
            except TransientNetError as e:
                # Check network errors BEFORE NotFound — some test environments
                # fall NotFound back to `Exception`, which would otherwise swallow
                # requests.Timeout/ConnectionError and mark the pin as orphaned.
                last_transient_err = e
                if attempt < _PIN_FETCH_MAX_ATTEMPTS - 1:
                    logging.warning(
                        f"Pinned item fetch attempt {attempt + 1}/{_PIN_FETCH_MAX_ATTEMPTS} "
                        f"timed out for '{title}' (rating_key={rk}): {e}. "
                        f"Retrying in {_PIN_FETCH_RETRY_WAIT}s..."
                    )
                    time.sleep(_PIN_FETCH_RETRY_WAIT)
            except (NotFound, ValueError) as e:
                logging.warning(
                    f"Pinned item no longer in Plex: '{title}' "
                    f"(rating_key={rk}) — removing pin. ({type(e).__name__}: {e})"
                )
                tracker.remove_pin(rk)
                orphaned.append(rk)
                item = None
                last_transient_err = None
                break
            except Exception as e:
                # Non-retriable error (e.g. malformed response, unexpected exception)
                logging.error(
                    f"Failed to fetch pinned item '{title}' (rating_key={rk}): "
                    f"{type(e).__name__}: {e}. Leaving pin in place."
                )
                last_transient_err = None
                break

        if item is None:
            if last_transient_err is not None:
                # Don't remove the pin — server might be back up next audit cycle.
                logging.error(
                    f"Failed to fetch pinned item '{title}' (rating_key={rk}) "
                    f"after {_PIN_FETCH_MAX_ATTEMPTS} attempts: "
                    f"{type(last_transient_err).__name__}: {last_transient_err}. "
                    f"Leaving pin in place."
                )
            continue

        try:
            paths = _resolve_item_to_paths(item, pin_type, preference)
        except Exception as e:
            logging.error(
                f"Failed to resolve pinned {pin_type} '{title}' "
                f"(rating_key={rk}): {type(e).__name__}: {e}"
            )
            continue

        if not paths:
            logging.warning(
                f"Pinned {pin_type} '{title}' (rating_key={rk}) resolved to 0 files"
            )
            continue

        for p in paths:
            resolved.append((p, rk, pin_type))
        logging.info(
            f"Pinned {pin_type} '{title}': resolved to {len(paths)} file(s) "
            f"per preferred_resolution={preference}"
        )

    return resolved, orphaned


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


# ---------------------------------------------------------------------------
# Budget math — shared by the web service and the CLI so the two can't drift
# on what counts as "over budget". All functions below are pure (no web or
# filesystem coupling beyond ``sum_pinned_bytes_on_disk``, which is the one
# unavoidable stat-the-file operation).
# ---------------------------------------------------------------------------


def resolve_size_setting(value: Any, disk_total_bytes: int) -> int:
    """Resolve a settings size value to bytes.

    Accepts either a byte-quantity string ("10GB", "500MB") parsed via
    ``core.system_utils.parse_size_bytes`` or a percentage string ("50%")
    resolved against ``disk_total_bytes``. Returns 0 on any parse failure,
    empty input, or percent-without-disk-size — matches the soft-fail
    behavior the budget guard has always relied on.
    """
    from core.system_utils import parse_size_bytes
    if value is None:
        return 0
    s = str(value).strip()
    if not s or s in ("0", "N/A", "none", "None"):
        return 0
    if s.endswith("%"):
        try:
            percent = float(s.rstrip("%"))
        except ValueError:
            return 0
        if disk_total_bytes <= 0 or percent <= 0:
            return 0
        return int(disk_total_bytes * percent / 100)
    return parse_size_bytes(s) or 0


def get_active_cache_total_bytes(settings: Dict[str, Any]) -> int:
    """Return the total size in bytes of the first enabled cache mapping.

    Used to resolve percent-based ``cache_limit`` / ``min_free_space`` values.
    Returns 0 if no active cache mapping has a probeable size, which degrades
    percent values to 0 (disabling the budget guard). Soft-fail by design —
    the budget is an optional safety net, not a hard invariant.
    """
    from core.system_utils import get_disk_usage
    mappings = settings.get("path_mappings", [])
    if not isinstance(mappings, list):
        return 0
    for m in mappings:
        if not isinstance(m, dict):
            continue
        if m.get("enabled") is False:
            continue
        cache_path = m.get("cache_path")
        if not cache_path:
            continue
        try:
            disk = get_disk_usage(cache_path)
            if disk and getattr(disk, "total", 0) > 0:
                return int(disk.total)
        except Exception:
            continue
    return 0


def parse_budget_from_settings(settings: Dict[str, Any]) -> Dict[str, int]:
    """Return the parsed cache budget (all values in bytes).

    Handles both byte-quantity strings and percentages. Percent values are
    resolved against the active cache drive's total size.
    """
    cache_limit = settings.get("cache_limit", "")
    min_free_space = settings.get("min_free_space", "")
    quota = settings.get("plexcache_quota", "")
    disk_total = get_active_cache_total_bytes(settings)
    return {
        "cache_limit_bytes": resolve_size_setting(cache_limit, disk_total),
        "min_free_space_bytes": resolve_size_setting(min_free_space, disk_total),
        "plexcache_quota_bytes": resolve_size_setting(quota, disk_total),
    }


def estimate_item_size(item: Any, pin_type: str, preference: str) -> int:
    """Compute total byte size for a Plex item under the preferred resolution.

    Walks Plex metadata only — no filesystem access. Used by
    ``estimate_item_bytes`` and by the web search UI to label rows with sizes.
    """
    def _single_size(it):
        try:
            media = select_media_version(it, preference)
            return _media_total_size(media)
        except Exception:
            return 0

    try:
        if pin_type in ("movie", "episode"):
            return _single_size(item)
        if pin_type == "season":
            return sum(_single_size(ep) for ep in item.episodes())
        if pin_type == "show":
            total = 0
            for season in item.seasons():
                for ep in season.episodes():
                    total += _single_size(ep)
            return total
    except Exception:
        pass
    return 0


def estimate_item_bytes(
    plex_server: Any,
    rating_key: str,
    pin_type: str,
    preference: str,
) -> int:
    """Best-effort size estimate for an item about to be pinned.

    Returns 0 on any Plex fetch failure so a missing item never hard-blocks
    the budget check — callers treat 0 as "can't estimate, let it through".
    """
    if plex_server is None or not rating_key:
        return 0
    try:
        item = plex_server.fetchItem(int(rating_key))
    except Exception:
        return 0
    return estimate_item_size(item, pin_type or "movie", preference)


def sum_pinned_bytes_on_disk(cache_paths: Iterable[str]) -> int:
    """Sum the byte size of every cache_path that currently exists on disk.

    Missing / unreachable paths are skipped silently. Kept here (rather than
    inside ``PinnedService``) so the CLI can compute current pinned usage
    without importing the web stack.
    """
    total = 0
    for p in cache_paths:
        if not p:
            continue
        try:
            if os.path.exists(p):
                total += os.path.getsize(p)
        except OSError:
            continue
    return total


def compute_budget_state(
    cache_limit_bytes: int,
    min_free_space_bytes: int,
    current_pinned_bytes: int,
    additional_bytes: int = 0,
) -> Dict[str, Any]:
    """Compute the pinned-bytes budget state — pure math, no side effects.

    ``effective_budget = max(0, cache_limit - min_free_space)`` unless
    ``cache_limit`` is 0 (budget disabled). When disabled, ``over_budget`` and
    ``would_exceed`` are always False — the guard never hard-blocks without an
    explicit limit.
    """
    effective_budget = max(0, cache_limit_bytes - min_free_space_bytes) if cache_limit_bytes > 0 else 0
    over_budget = bool(effective_budget) and current_pinned_bytes > effective_budget
    would_exceed = bool(effective_budget) and (current_pinned_bytes + additional_bytes) > effective_budget
    return {
        "total_pinned_bytes": current_pinned_bytes,
        "budget_bytes": cache_limit_bytes,
        "effective_budget_bytes": effective_budget,
        "headroom_bytes": min_free_space_bytes,
        "additional_bytes": additional_bytes,
        "over_budget": over_budget,
        "would_exceed": would_exceed,
    }


def plex_to_cache_path(plex_path: str, path_mappings: List[Dict[str, Any]]) -> Optional[str]:
    """Translate a Plex-form path to its cache-form path via prefix swap.

    Returns None if no enabled mapping matches. Mirrors the existing
    ``PinnedService._plex_to_cache`` semantics so the CLI and web paths
    produce identical results.
    """
    if not plex_path:
        return None
    for mapping in path_mappings or []:
        if not isinstance(mapping, dict):
            continue
        if not mapping.get("enabled", True):
            continue
        plex_prefix = (mapping.get("plex_path") or "").rstrip("/")
        cache_prefix = (mapping.get("cache_path") or "").rstrip("/")
        if plex_prefix and cache_prefix and plex_path.startswith(plex_prefix):
            return cache_prefix + plex_path[len(plex_prefix):]
    return None
