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
from typing import Any, Dict, List, Optional, Set, Tuple

from core.pinned_media import (
    PinnedMediaTracker,
    VALID_PIN_TYPES,
    resolve_pins_to_paths,
    select_media_version,
    _media_total_size,
    compute_budget_state,
    estimate_item_bytes,
    estimate_item_size,
    get_active_cache_total_bytes,
    parse_budget_from_settings,
    plex_to_cache_path,
    resolve_size_setting,
    sum_pinned_bytes_on_disk,
)


logger = logging.getLogger(__name__)


# Backward-compat alias so any caller that used the private name keeps working.
_resolve_size_setting = resolve_size_setting


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

        from core.system_utils import format_bytes
        preference = self._get_preference()

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
                        size_bytes = self._estimate_item_size(
                            item, result_type, preference
                        )
                        results.append({
                            "rating_key": str(getattr(item, "ratingKey", "")),
                            "title": getattr(item, "title", ""),
                            "type": result_type,
                            "year": getattr(item, "year", None),
                            "library": getattr(item, "librarySectionTitle", ""),
                            "size_bytes": size_bytes,
                            "size_display": format_bytes(size_bytes) if size_bytes else "",
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

    @staticmethod
    def _estimate_item_size(item: Any, item_type: str, preference: str) -> int:
        """Compute total byte size for an item from Plex metadata.

        Thin wrapper around :func:`core.pinned_media.estimate_item_size`.
        """
        return estimate_item_size(item, item_type, preference)

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

        from core.system_utils import format_bytes
        preference = self._get_preference()

        children: List[Dict[str, Any]] = []
        try:
            if level == "show":
                show_title = getattr(item, "title", "") or ""
                show_year = getattr(item, "year", None)
                show_label = f"{show_title} ({show_year})" if show_title and show_year else show_title
                seasons = list(item.seasons())
                for season in seasons:
                    season_title = getattr(season, "title", "") or ""
                    display_title = (
                        f"{show_label} — {season_title}"
                        if show_label and season_title else (season_title or show_label)
                    )
                    size_bytes = self._estimate_item_size(
                        season, "season", preference
                    )
                    children.append({
                        "rating_key": str(getattr(season, "ratingKey", "")),
                        "title": display_title,
                        "type": "season",
                        "parent_rating_key": str(rating_key),
                        "episode_count": getattr(season, "leafCount", None),
                        "size_bytes": size_bytes,
                        "size_display": format_bytes(size_bytes) if size_bytes else "",
                        "already_pinned": self._tracker.is_pinned(
                            str(getattr(season, "ratingKey", ""))
                        ),
                    })
            else:  # season
                show_title = (
                    getattr(item, "parentTitle", "")
                    or getattr(item, "grandparentTitle", "")
                    or ""
                )
                show_year = getattr(item, "parentYear", None)
                show_label = f"{show_title} ({show_year})" if show_title and show_year else show_title
                episodes = list(item.episodes())
                for ep in episodes:
                    season_num = getattr(ep, "parentIndex", None)
                    ep_num = getattr(ep, "index", None)
                    ep_title = getattr(ep, "title", "") or ""
                    ep_show_label = show_label or getattr(ep, "grandparentTitle", "") or ""
                    se_code = ""
                    if isinstance(season_num, int) and isinstance(ep_num, int):
                        se_code = f"S{season_num:02d}E{ep_num:02d}"
                    parts = [p for p in (ep_show_label, se_code, ep_title) if p]
                    display_title = " — ".join(parts) if parts else ep_title
                    size_bytes = self._estimate_item_size(
                        ep, "episode", preference
                    )
                    children.append({
                        "rating_key": str(getattr(ep, "ratingKey", "")),
                        "title": display_title,
                        "type": "episode",
                        "parent_rating_key": str(rating_key),
                        "index": ep_num,
                        "season_number": season_num,
                        "size_bytes": size_bytes,
                        "size_display": format_bytes(size_bytes) if size_bytes else "",
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

        Thin wrapper around :func:`core.pinned_media.parse_budget_from_settings`
        that pulls the settings dict from the web service layer and soft-fails
        to zeros on any unexpected error.
        """
        from web.services import get_settings_service
        try:
            settings = get_settings_service().get_all()
            return parse_budget_from_settings(settings)
        except Exception as e:
            logger.warning(f"PinnedService: could not parse cache limits: {e}")
            return {
                "cache_limit_bytes": 0,
                "min_free_space_bytes": 0,
                "plexcache_quota_bytes": 0,
            }

    def _get_active_cache_total_bytes(self, settings: Dict[str, Any]) -> int:
        """Return the total size in bytes of the first enabled cache mapping.

        Kept as a thin wrapper around the core helper so existing tests that
        monkey-patch this method on the service instance keep working.
        """
        return get_active_cache_total_bytes(settings)

    def _sum_pinned_bytes(self) -> int:
        """Return the total byte size of currently pinned cached video files.

        Wraps :func:`core.pinned_media.sum_pinned_bytes_on_disk` with the
        service's resolver so subclasses/tests can override by swapping out
        either end.
        """
        return sum_pinned_bytes_on_disk(self.resolve_all_to_cache_paths())

    def _estimate_item_bytes(self, rating_key: str, pin_type: str) -> int:
        """Best-effort size estimate for an item about to be pinned.

        Wrapper around :func:`core.pinned_media.estimate_item_bytes` that
        supplies the web layer's connected Plex instance + preference.
        """
        return estimate_item_bytes(
            self._get_plex_server(), rating_key, pin_type, self._get_preference()
        )

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
        current = self._sum_pinned_bytes()

        additional = 0
        if additional_rating_key:
            additional = self._estimate_item_bytes(
                additional_rating_key, additional_pin_type or "movie"
            )

        return compute_budget_state(
            cache_limit_bytes=parsed["cache_limit_bytes"],
            min_free_space_bytes=parsed["min_free_space_bytes"],
            current_pinned_bytes=current,
            additional_bytes=additional,
        )

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
            ``{is_pinned: bool, error: Optional[str], budget: dict,
               evict_paths: List[str], pinned_paths: List[str]}``.
            ``error`` is set when a pin-add is blocked by budget; the tracker
            is left untouched in that case. ``evict_paths`` (populated only on
            unpin) lists cache paths that were uniquely protected by the
            removed pin — callers may hand these to the maintenance runner to
            move them back to the array immediately instead of waiting for
            retention to expire. ``pinned_paths`` (populated only on pin add)
            lists cache paths newly protected by the added pin — callers may
            use this to log activity entries for files that were already on
            cache at pin time.
        """
        rating_key = str(rating_key)

        if self._tracker.is_pinned(rating_key):
            # Resolve before + after to compute which cache paths were uniquely
            # protected by THIS pin (i.e., aren't also held by another pin).
            # Soft-fail: if Plex is offline the diff returns empty and the
            # caller simply skips immediate eviction.
            before_paths = self.resolve_all_to_cache_paths()
            self._tracker.remove_pin(rating_key)
            after_paths = self.resolve_all_to_cache_paths()
            freshly_unpinned = sorted(before_paths - after_paths)
            return {
                "is_pinned": False,
                "error": None,
                "budget": self.budget_check(),
                "evict_paths": freshly_unpinned,
                "pinned_paths": [],
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

        # Capture resolved paths BEFORE adding the pin so we can diff after.
        # This tells us which cache paths are NEWLY protected by the new pin
        # (versus already-protected by another existing pin).
        before_paths = self.resolve_all_to_cache_paths()
        self._tracker.add_pin(rating_key, pin_type, title, added_by="web")
        after_paths = self.resolve_all_to_cache_paths()
        freshly_pinned = sorted(after_paths - before_paths)
        return {
            "is_pinned": True,
            "error": None,
            "budget": self.budget_check(),
            "evict_paths": [],
            "pinned_paths": freshly_pinned,
        }

    # ------------------------------------------------------------------
    # Currently-pinned chip list
    # ------------------------------------------------------------------

    def _decorate_title(self, plex, pin_type: str, rating_key: str, fallback: str) -> str:
        """Return a rich display title for a pin, falling back to ``fallback``.

        Movies/shows keep their stored title. Seasons gain ``{show} — {season}``
        and episodes gain ``{show} — SxxExx — {title}``. This covers legacy pins
        that were saved with a bare "Season 2" or plain episode title.
        """
        if pin_type not in ("season", "episode") or plex is None:
            return fallback
        try:
            item = plex.fetchItem(int(rating_key))
        except Exception:
            return fallback

        try:
            if pin_type == "season":
                show_title = getattr(item, "parentTitle", "") or getattr(item, "grandparentTitle", "") or ""
                show_year = getattr(item, "parentYear", None)
                show_label = f"{show_title} ({show_year})" if show_title and show_year else show_title
                season_title = getattr(item, "title", "") or fallback
                if show_label and season_title:
                    return f"{show_label} — {season_title}"
                return season_title or fallback

            # pin_type == "episode"
            show_title = getattr(item, "grandparentTitle", "") or ""
            # Episodes don't expose a grandparentYear on all plexapi versions
            show_year = getattr(item, "grandparentYear", None)
            show_label = f"{show_title} ({show_year})" if show_title and show_year else show_title
            season_num = getattr(item, "parentIndex", None)
            ep_num = getattr(item, "index", None)
            ep_title = getattr(item, "title", "") or fallback
            se_code = ""
            if isinstance(season_num, int) and isinstance(ep_num, int):
                se_code = f"S{season_num:02d}E{ep_num:02d}"
            parts = [p for p in (show_label, se_code, ep_title) if p]
            return " — ".join(parts) if parts else fallback
        except Exception:
            return fallback

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
        plex = None
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
            display_title = self._decorate_title(
                plex, pin["type"], rk, pin["title"]
            )
            out.append({
                "rating_key": rk,
                "type": pin["type"],
                "title": display_title,
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

    def unpin_many(self, rating_keys: List[str]) -> Dict[str, Any]:
        """Remove multiple pins in one pass.

        Takes the before/after resolve-diff once (not per-key) so the caller
        can start a single background eviction covering every freshly-released
        cache path. Unknown rating_keys are silently skipped. Returns::

            {removed: int, evict_paths: List[str], budget: dict}

        Shape intentionally mirrors toggle_pin's unpin branch so the route can
        reuse the same HX-Trigger + eviction-kickoff flow.
        """
        normalized = [str(rk) for rk in rating_keys if str(rk)]
        if not normalized:
            return {
                "removed": 0,
                "evict_paths": [],
                "budget": self.budget_check(),
            }

        before_paths = self.resolve_all_to_cache_paths()
        removed = 0
        for rk in normalized:
            if self._tracker.is_pinned(rk):
                self._tracker.remove_pin(rk)
                removed += 1
        after_paths = self.resolve_all_to_cache_paths()
        freshly_unpinned = sorted(before_paths - after_paths)
        return {
            "removed": removed,
            "evict_paths": freshly_unpinned,
            "budget": self.budget_check(),
        }

    def _pin_group_and_scope(self, plex, pin_type: str, rating_key: str,
                              stored_title: str) -> Dict[str, Any]:
        """Return group metadata + scope text for a pin.

        Output fields:
          group_rating_key, group_title, group_type ("movie" or "show"),
          scope_text (what's pinned within the group),
          scope_icon (lucide icon name),
          sort_key (tier, season_num, ep_num) for within-group ordering.

        Falls back gracefully when Plex is unreachable — the pin keeps its
        stored title as the scope text and is grouped as a standalone entry.
        """
        scope_icon_map = {
            "show": "tv",
            "season": "layers",
            "episode": "play",
            "movie": "film",
        }
        result = {
            "group_rating_key": rating_key,
            "group_title": stored_title,
            "group_type": "show" if pin_type in ("show", "season", "episode") else "movie",
            "scope_text": stored_title,
            "scope_icon": scope_icon_map.get(pin_type, "file-video"),
            "sort_key": (0, 0, 0),
        }

        if plex is None:
            return result

        try:
            item = plex.fetchItem(int(rating_key))
        except Exception:
            return result

        try:
            if pin_type == "movie":
                result["group_rating_key"] = rating_key
                title = getattr(item, "title", "") or stored_title
                year = getattr(item, "year", None)
                result["group_title"] = f"{title} ({year})" if title and year else title
                result["group_type"] = "movie"
                result["scope_text"] = "Movie"
                result["sort_key"] = (0, 0, 0)

            elif pin_type == "show":
                result["group_rating_key"] = rating_key
                title = getattr(item, "title", "") or stored_title
                year = getattr(item, "year", None)
                result["group_title"] = f"{title} ({year})" if title and year else title
                result["group_type"] = "show"
                result["scope_text"] = "Entire Show"
                result["sort_key"] = (0, 0, 0)

            elif pin_type == "season":
                show_rk = str(getattr(item, "parentRatingKey", "") or "")
                show_title = getattr(item, "parentTitle", "") or getattr(item, "grandparentTitle", "")
                show_year = getattr(item, "parentYear", None)
                result["group_rating_key"] = show_rk or rating_key
                result["group_title"] = (
                    f"{show_title} ({show_year})" if show_title and show_year else (show_title or stored_title)
                )
                result["group_type"] = "show"
                season_title = getattr(item, "title", "") or "Season"
                ep_count = getattr(item, "leafCount", None)
                if isinstance(ep_count, int) and ep_count > 0:
                    result["scope_text"] = f"{season_title} ({ep_count} eps)"
                else:
                    result["scope_text"] = season_title
                season_num = getattr(item, "index", 0)
                result["sort_key"] = (1, season_num if isinstance(season_num, int) else 0, 0)

            elif pin_type == "episode":
                show_rk = str(getattr(item, "grandparentRatingKey", "") or "")
                show_title = getattr(item, "grandparentTitle", "") or ""
                show_year = getattr(item, "grandparentYear", None)
                result["group_rating_key"] = show_rk or rating_key
                result["group_title"] = (
                    f"{show_title} ({show_year})" if show_title and show_year else (show_title or stored_title)
                )
                result["group_type"] = "show"
                season_num = getattr(item, "parentIndex", None)
                ep_num = getattr(item, "index", None)
                ep_title = getattr(item, "title", "") or ""
                se_code = ""
                if isinstance(season_num, int) and isinstance(ep_num, int):
                    se_code = f"S{season_num:02d}E{ep_num:02d}"
                if se_code and ep_title:
                    result["scope_text"] = f"{se_code} — {ep_title}"
                elif se_code:
                    result["scope_text"] = se_code
                else:
                    result["scope_text"] = ep_title or stored_title
                result["sort_key"] = (
                    2,
                    season_num if isinstance(season_num, int) else 0,
                    ep_num if isinstance(ep_num, int) else 0,
                )
        except Exception:
            # Any unexpected plexapi issue — keep the safe defaults
            pass

        return result

    def list_pins_grouped(self) -> List[Dict[str, Any]]:
        """Return pins grouped by show/movie, sorted for stable display.

        Shape::

            [
              {
                "group_rating_key": str,
                "group_title": "Show Name (Year)",
                "group_type": "show" | "movie",
                "pin_count": int,
                "group_bytes": int,
                "group_size_display": "X.YY GB",
                "pins": [ { rating_key, type, scope_text, scope_icon,
                            size_bytes, size_display, budget_percent,
                            title, sort_key }, ... ]
              },
              ...
            ]

        Groups are sorted alphabetically by group_title. Within each group,
        pins sort by (tier, season_num, ep_num): show-scope pin first, then
        seasons ascending, then episodes by (season, episode).
        """
        flat = self.list_pins_with_metadata()
        if not flat:
            return []

        plex = None
        try:
            plex = self._get_plex_server()
        except Exception as e:
            logger.debug(f"PinnedService.list_pins_grouped: plex unavailable: {e}")

        from core.system_utils import format_bytes
        groups: Dict[str, Dict[str, Any]] = {}
        for pin in flat:
            ctx = self._pin_group_and_scope(
                plex, pin["type"], pin["rating_key"], pin["title"]
            )
            gk = ctx["group_rating_key"]
            if gk not in groups:
                groups[gk] = {
                    "group_rating_key": gk,
                    "group_title": ctx["group_title"],
                    "group_type": ctx["group_type"],
                    "pin_count": 0,
                    "group_bytes": 0,
                    "pins": [],
                }
            enriched = dict(pin)
            enriched["scope_text"] = ctx["scope_text"]
            enriched["scope_icon"] = ctx["scope_icon"]
            enriched["sort_key"] = ctx["sort_key"]
            groups[gk]["pins"].append(enriched)
            groups[gk]["pin_count"] += 1
            groups[gk]["group_bytes"] += pin.get("size_bytes", 0) or 0

        for group in groups.values():
            group["pins"].sort(key=lambda p: p.get("sort_key") or (9, 0, 0))
            group["group_size_display"] = format_bytes(group["group_bytes"])

        return sorted(groups.values(), key=lambda g: (g["group_title"] or "").lower())

    # ------------------------------------------------------------------
    # Full resolve: cache paths used by CacheService to flag is_pinned
    # ------------------------------------------------------------------

    def resolve_all_to_cache_paths(self) -> Set[str]:
        """Return the set of cache-form paths currently protected by pins.

        Thin wrapper around ``resolve_all_to_cache_path_map`` that keeps the
        original set-returning shape for call sites that only need the
        membership check (simulate_eviction, evict_file, maintenance guards).
        """
        return set(self.resolve_all_to_cache_path_map().keys())

    def resolve_all_to_cache_path_map(self) -> Dict[str, Tuple[str, str]]:
        """Return ``{cache_path: (rating_key, pin_type)}`` for every pin.

        Same resolve path as ``resolve_all_to_cache_paths`` but preserves the
        rating_key and pin_type from the resolver tuples so the Cached Files
        row UI can render a matching unpin button without re-querying Plex.

        Does NOT include sidecars — the CacheService groups sidecars under
        their parent video. Returns an empty dict on any failure (no
        connection, no pins, resolver error).
        """
        if not self._tracker.list_pins():
            return {}

        plex = self._get_plex_server()
        if plex is None:
            return {}

        try:
            resolved, _orphaned = resolve_pins_to_paths(
                plex, self._tracker, self._get_preference()
            )
        except Exception as e:
            logger.warning(
                f"PinnedService.resolve_all_to_cache_path_map failed: {e}"
            )
            return {}

        from web.services import get_settings_service
        settings = get_settings_service().get_all()
        path_mappings = settings.get("path_mappings", [])

        path_map: Dict[str, Tuple[str, str]] = {}
        for plex_path, rk, pin_type in resolved:
            cache_path = self._plex_to_cache(plex_path, path_mappings)
            if cache_path and cache_path not in path_map:
                path_map[cache_path] = (rk, pin_type)
        return path_map

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
