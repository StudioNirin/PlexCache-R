"""Pinned media service — web-layer business logic for pinning.

Wraps ``core.pinned_media.PinnedMediaTracker`` and ``resolve_pins_to_paths()``
for use by the web UI. Responsibilities:

- Plex search / child expansion for the pin-picker UI.
- Budget preflight: refuses to pin an item that would push the cache over
  ``cache_limit`` (hard-block per open question #2 in PINNED_MEDIA_PLAN.md).
- Resolving the full set of cache paths currently protected by pins, so
  the Cache service can surface ``is_pinned`` on every row.

Path semantics: the resolver returns **plex-form** file paths. Those are
converted to **real** paths and then **cache** paths via the settings'
``path_mappings``. Subtitles/sidecars are protected at the web layer by
inheriting from their grouped parent video (``CacheService.get_all_cached_files``
does the grouping), so this service only exposes video cache paths.
"""

import logging
import threading
from typing import Any, Dict, List, Optional, Set

from core.pinned_media import (
    PinnedMediaTracker,
    VALID_PIN_TYPES,
    resolve_pins_to_paths,
    select_media_version,
    _media_total_size,
)


logger = logging.getLogger(__name__)


class PinnedService:
    """Business logic for pinned media (web layer)."""

    def __init__(self):
        from web.dependencies import get_pinned_tracker
        self._tracker: PinnedMediaTracker = get_pinned_tracker()

    # ------------------------------------------------------------------
    # Plex helpers
    # ------------------------------------------------------------------

    def _get_plex_server(self) -> Optional[Any]:
        """Connect to Plex using saved settings. Returns None if unavailable."""
        from web.services import get_settings_service
        settings_service = get_settings_service()
        plex_settings = settings_service.get_plex_settings()
        plex_url = plex_settings.get("plex_url", "")
        plex_token = plex_settings.get("plex_token", "")
        if not plex_url or not plex_token:
            return None
        try:
            from plexapi.server import PlexServer
            return PlexServer(plex_url, plex_token, timeout=10)
        except Exception as e:
            logger.warning(f"PinnedService: could not connect to Plex: {e}")
            return None

    def _get_preference(self) -> str:
        """Read pinned_preferred_resolution from settings."""
        from web.services import get_settings_service
        settings = get_settings_service().get_all()
        return settings.get("pinned_preferred_resolution", "highest") or "highest"

    # ------------------------------------------------------------------
    # Pin picker — search + expand
    # ------------------------------------------------------------------

    _SEARCH_TYPES = ("movie", "show")

    def search(self, query: str, limit: int = 25) -> List[Dict[str, Any]]:
        """Search Plex for movies and shows matching ``query``.

        Returns a list of dicts shaped for the picker partial:
        ``{rating_key, title, type, year, library, poster_url}``.
        """
        query = (query or "").strip()
        if not query:
            return []

        plex = self._get_plex_server()
        if plex is None:
            return []

        results: List[Dict[str, Any]] = []
        try:
            for result_type in self._SEARCH_TYPES:
                try:
                    hits = plex.search(query, mediatype=result_type, limit=limit)
                except Exception as e:
                    logger.debug(f"PinnedService.search: {result_type} search failed: {e}")
                    continue

                for item in hits:
                    try:
                        results.append({
                            "rating_key": str(getattr(item, "ratingKey", "")),
                            "title": getattr(item, "title", ""),
                            "type": result_type,
                            "year": getattr(item, "year", None),
                            "library": getattr(
                                getattr(item, "librarySectionTitle", None), "title", None
                            ) or getattr(item, "librarySectionTitle", ""),
                            "already_pinned": self._tracker.is_pinned(
                                str(getattr(item, "ratingKey", ""))
                            ),
                        })
                    except Exception as e:
                        logger.debug(f"PinnedService.search: skipping malformed hit: {e}")
                        continue
        except Exception as e:
            logger.warning(f"PinnedService.search failed: {e}")
            return []

        return results[:limit]

    def expand(self, rating_key: str, level: str) -> List[Dict[str, Any]]:
        """Return lazy children for a show/season.

        Args:
            rating_key: Plex rating_key of the parent item.
            level: ``"show"`` returns seasons; ``"season"`` returns episodes.
        """
        if level not in ("show", "season"):
            raise ValueError(f"Unknown expand level: {level!r}")

        plex = self._get_plex_server()
        if plex is None:
            return []

        try:
            item = plex.fetchItem(int(rating_key))
        except Exception as e:
            logger.warning(f"PinnedService.expand: fetchItem({rating_key}) failed: {e}")
            return []

        children: List[Dict[str, Any]] = []
        try:
            if level == "show":
                seasons = list(item.seasons())
                for season in seasons:
                    children.append({
                        "rating_key": str(getattr(season, "ratingKey", "")),
                        "title": getattr(season, "title", ""),
                        "type": "season",
                        "parent_rating_key": str(rating_key),
                        "episode_count": getattr(season, "leafCount", None),
                        "already_pinned": self._tracker.is_pinned(
                            str(getattr(season, "ratingKey", ""))
                        ),
                    })
            else:  # season
                episodes = list(item.episodes())
                for ep in episodes:
                    children.append({
                        "rating_key": str(getattr(ep, "ratingKey", "")),
                        "title": getattr(ep, "title", ""),
                        "type": "episode",
                        "parent_rating_key": str(rating_key),
                        "index": getattr(ep, "index", None),
                        "season_number": getattr(ep, "parentIndex", None),
                        "already_pinned": self._tracker.is_pinned(
                            str(getattr(ep, "ratingKey", ""))
                        ),
                    })
        except Exception as e:
            logger.warning(
                f"PinnedService.expand: failed to enumerate children "
                f"for rating_key={rating_key} level={level}: {e}"
            )
            return []

        return children

    # ------------------------------------------------------------------
    # Budget preflight
    # ------------------------------------------------------------------

    def _load_parsed_settings(self) -> Dict[str, int]:
        """Return the parsed cache budget (bytes) from settings.

        Parses byte-quantity strings directly ("10GB", "500MB"). Does NOT
        currently resolve percentage-based limits ("50%") — that would
        require knowing the cache drive size, which we don't have here.
        If percent values become a common configuration, extend to call
        ``get_disk_usage()`` on the active cache mapping.
        """
        from core.system_utils import parse_size_bytes
        from web.services import get_settings_service
        try:
            settings = get_settings_service().get_all()
            cache_limit = settings.get("cache_limit", "")
            min_free_space = settings.get("min_free_space", "")
            quota = settings.get("plexcache_quota", "")
            return {
                "cache_limit_bytes": parse_size_bytes(cache_limit) or 0,
                "min_free_space_bytes": parse_size_bytes(min_free_space) or 0,
                "plexcache_quota_bytes": parse_size_bytes(quota) or 0,
            }
        except Exception as e:
            logger.warning(f"PinnedService: could not parse cache limits: {e}")
            return {
                "cache_limit_bytes": 0,
                "min_free_space_bytes": 0,
                "plexcache_quota_bytes": 0,
            }

    def _sum_pinned_bytes(self) -> int:
        """Return the total byte size of currently pinned cached video files.

        Only counts video paths (sidecars are protected via their parent in
        the CacheService grouping, but their bytes show up there too — we
        sum them here only for files already on disk).
        """
        total = 0
        for cache_path in self.resolve_all_to_cache_paths():
            try:
                import os
                if os.path.exists(cache_path):
                    total += os.path.getsize(cache_path)
            except OSError:
                continue
        return total

    def _estimate_item_bytes(self, rating_key: str, pin_type: str) -> int:
        """Best-effort size estimate for an item about to be pinned.

        Walks the Plex metadata only — does not touch the filesystem. Used
        by ``budget_check`` for the preflight when adding a new pin.
        """
        plex = self._get_plex_server()
        if plex is None:
            return 0
        try:
            item = plex.fetchItem(int(rating_key))
        except Exception:
            return 0

        preference = self._get_preference()

        def _item_bytes(it) -> int:
            try:
                media = select_media_version(it, preference)
            except Exception:
                return 0
            return _media_total_size(media)

        try:
            if pin_type in ("movie", "episode"):
                return _item_bytes(item)
            if pin_type == "season":
                return sum(_item_bytes(ep) for ep in item.episodes())
            if pin_type == "show":
                total = 0
                for season in item.seasons():
                    for ep in season.episodes():
                        total += _item_bytes(ep)
                return total
        except Exception as e:
            logger.debug(f"PinnedService._estimate_item_bytes failed: {e}")
            return 0
        return 0

    def budget_check(
        self,
        additional_rating_key: Optional[str] = None,
        additional_pin_type: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Compute the pinned-bytes budget state.

        If ``additional_rating_key`` is provided, preflights whether that
        pin would push pinned bytes over the ``cache_limit`` (minus any
        ``min_free_space`` headroom, if configured).

        Returns:
            dict with keys ``total_pinned_bytes``, ``budget_bytes``,
            ``headroom_bytes``, ``over_budget``, ``would_exceed``,
            ``additional_bytes``.
        """
        parsed = self._load_parsed_settings()
        budget = parsed["cache_limit_bytes"]
        headroom = parsed["min_free_space_bytes"]
        current = self._sum_pinned_bytes()

        additional = 0
        if additional_rating_key:
            additional = self._estimate_item_bytes(
                additional_rating_key, additional_pin_type or "movie"
            )

        # Effective budget = cache_limit minus min_free_space safety floor.
        # If cache_limit is 0 (disabled), no budget — never blocks.
        effective_budget = max(0, budget - headroom) if budget > 0 else 0
        over_budget = bool(effective_budget) and current > effective_budget
        would_exceed = bool(effective_budget) and (current + additional) > effective_budget

        return {
            "total_pinned_bytes": current,
            "budget_bytes": budget,
            "effective_budget_bytes": effective_budget,
            "headroom_bytes": headroom,
            "additional_bytes": additional,
            "over_budget": over_budget,
            "would_exceed": would_exceed,
        }

    # ------------------------------------------------------------------
    # Pin/unpin
    # ------------------------------------------------------------------

    def toggle_pin(
        self,
        rating_key: str,
        pin_type: str,
        title: str,
    ) -> Dict[str, Any]:
        """Toggle a pin. Adds if missing, removes if present.

        Returns:
            ``{is_pinned: bool, error: Optional[str], budget: dict}``.
            ``error`` is set when a pin-add is blocked by budget; the tracker
            is left untouched in that case.
        """
        rating_key = str(rating_key)

        if self._tracker.is_pinned(rating_key):
            self._tracker.remove_pin(rating_key)
            return {
                "is_pinned": False,
                "error": None,
                "budget": self.budget_check(),
            }

        if pin_type not in VALID_PIN_TYPES:
            return {
                "is_pinned": False,
                "error": f"Invalid pin type: {pin_type}",
                "budget": self.budget_check(),
            }

        # Budget preflight — hard-block on overrun (PINNED_MEDIA_PLAN.md OQ #2).
        budget = self.budget_check(
            additional_rating_key=rating_key,
            additional_pin_type=pin_type,
        )
        if budget["would_exceed"]:
            from core.system_utils import format_bytes
            return {
                "is_pinned": False,
                "error": (
                    f"Pinning this item would exceed the cache budget "
                    f"({format_bytes(budget['total_pinned_bytes'])} + "
                    f"~{format_bytes(budget['additional_bytes'])} > "
                    f"{format_bytes(budget['effective_budget_bytes'])}). "
                    f"Unpin something first."
                ),
                "budget": budget,
            }

        self._tracker.add_pin(rating_key, pin_type, title, added_by="web")
        return {
            "is_pinned": True,
            "error": None,
            "budget": self.budget_check(),
        }

    # ------------------------------------------------------------------
    # Currently-pinned chip list
    # ------------------------------------------------------------------

    def list_pins_with_metadata(self) -> List[Dict[str, Any]]:
        """Return pin entries decorated with resolved size + budget share.

        Does not hit Plex for every chip — size comes from files actually
        on disk so the chip reflects real cache footprint.
        """
        pins = self._tracker.list_pins()
        if not pins:
            return []

        # Resolve all pins once to compute per-rating-key byte totals.
        # Returns (resolved, orphaned) where resolved is a list of
        # (plex_path, rating_key, pin_type) tuples.
        preference = self._get_preference()
        resolved_paths: List[tuple] = []
        try:
            plex = self._get_plex_server()
            if plex is not None:
                resolved_paths, _ = resolve_pins_to_paths(plex, self._tracker, preference)
        except Exception as e:
            logger.warning(f"PinnedService.list_pins_with_metadata: resolve failed: {e}")

        # Convert plex paths → cache paths via the settings path_mappings,
        # then sum sizes per rating_key.
        from web.services import get_settings_service
        settings = get_settings_service().get_all()
        path_mappings = settings.get("path_mappings", [])

        import os
        bytes_by_rk: Dict[str, int] = {}
        files_by_rk: Dict[str, int] = {}
        for plex_path, rk, _pin_type in resolved_paths:
            cache_path = self._plex_to_cache(plex_path, path_mappings)
            files_by_rk[rk] = files_by_rk.get(rk, 0) + 1
            if cache_path and os.path.exists(cache_path):
                try:
                    bytes_by_rk[rk] = bytes_by_rk.get(rk, 0) + os.path.getsize(cache_path)
                except OSError:
                    pass

        parsed = self._load_parsed_settings()
        effective_budget = max(
            0, parsed["cache_limit_bytes"] - parsed["min_free_space_bytes"]
        ) if parsed["cache_limit_bytes"] > 0 else 0

        from core.system_utils import format_bytes
        out: List[Dict[str, Any]] = []
        for pin in pins:
            rk = pin["rating_key"]
            size = bytes_by_rk.get(rk, 0)
            out.append({
                "rating_key": rk,
                "type": pin["type"],
                "title": pin["title"],
                "added_at": pin.get("added_at", ""),
                "added_by": pin.get("added_by", "web"),
                "resolved_file_count": files_by_rk.get(rk, 0),
                "size_bytes": size,
                "size_display": format_bytes(size),
                "budget_percent": (
                    round((size / effective_budget) * 100, 1)
                    if effective_budget > 0 else 0
                ),
            })
        return out

    # ------------------------------------------------------------------
    # Full resolve: cache paths used by CacheService to flag is_pinned
    # ------------------------------------------------------------------

    def resolve_all_to_cache_paths(self) -> Set[str]:
        """Return the set of cache-form paths currently protected by pins.

        Does NOT include sidecars — the CacheService groups sidecars under
        their parent video, so checking the video path is sufficient.

        Hits Plex once to resolve pins, then translates plex → cache paths
        via the configured ``path_mappings``. Returns an empty set on any
        failure (no connection, no pins, resolver error) — callers should
        treat "unknown" as "not pinned" rather than failing the whole page.
        """
        if not self._tracker.list_pins():
            return set()

        plex = self._get_plex_server()
        if plex is None:
            return set()

        try:
            resolved, _orphaned = resolve_pins_to_paths(
                plex, self._tracker, self._get_preference()
            )
        except Exception as e:
            logger.warning(f"PinnedService.resolve_all_to_cache_paths failed: {e}")
            return set()

        from web.services import get_settings_service
        settings = get_settings_service().get_all()
        path_mappings = settings.get("path_mappings", [])

        cache_paths: Set[str] = set()
        for plex_path, _rk, _pin_type in resolved:
            cache_path = self._plex_to_cache(plex_path, path_mappings)
            if cache_path:
                cache_paths.add(cache_path)
        return cache_paths

    @staticmethod
    def _plex_to_cache(plex_path: str, path_mappings: List[Dict]) -> Optional[str]:
        """Convert a plex-form path to a cache-form path via prefix swap.

        Mirrors ``CacheService._plex_to_real`` + ``_real_to_cache`` as a
        single step. Returns None if no mapping matches.
        """
        for mapping in path_mappings:
            if not mapping.get("enabled", True):
                continue
            plex_prefix = (mapping.get("plex_path") or "").rstrip("/")
            cache_prefix = (mapping.get("cache_path") or "").rstrip("/")
            if plex_prefix and cache_prefix and plex_path.startswith(plex_prefix):
                return cache_prefix + plex_path[len(plex_prefix):]
        return None

    # ------------------------------------------------------------------
    # Direct tracker passthroughs (used by CacheService + routes)
    # ------------------------------------------------------------------

    def is_pinned_rating_key(self, rating_key: str) -> bool:
        return self._tracker.is_pinned(rating_key)

    def get_tracker(self) -> PinnedMediaTracker:
        return self._tracker


# Singleton
_pinned_service: Optional[PinnedService] = None
_pinned_service_lock = threading.Lock()


def get_pinned_service() -> PinnedService:
    """Get or create the pinned service singleton."""
    global _pinned_service
    if _pinned_service is None:
        with _pinned_service_lock:
            if _pinned_service is None:
                _pinned_service = PinnedService()
    return _pinned_service
