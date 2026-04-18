"""CLI handlers for pinned media management (--list-pins, --pin, --unpin, --pin-by-title)."""

import logging
import sys
from typing import Any, Optional

from core.config import ConfigManager
from core.pinned_media import (
    PinnedMediaTracker,
    compute_budget_state,
    estimate_item_bytes,
    parse_budget_from_settings,
    plex_to_cache_path,
    resolve_pins_to_paths,
    sum_pinned_bytes_on_disk,
)


def _get_tracker(config_manager: ConfigManager) -> PinnedMediaTracker:
    tracker_file = config_manager.get_pinned_media_file()
    return PinnedMediaTracker(str(tracker_file))


def _preflight_budget(
    config_manager: ConfigManager,
    tracker: PinnedMediaTracker,
    plex: Any,
    rating_key: str,
    pin_type: str,
) -> Optional[str]:
    """Run the same budget check the web UI does before adding a pin.

    Returns an error string if adding ``rating_key`` would push pinned
    bytes over ``cache_limit`` (minus any ``min_free_space`` headroom).
    Returns ``None`` when the pin is allowed — including when the budget
    is unconfigured (``cache_limit`` empty or zero), matching the web
    behavior where the guard stays opt-in.
    """
    settings = config_manager.settings_data or {}
    parsed = parse_budget_from_settings(settings)
    if parsed["cache_limit_bytes"] <= 0:
        # Budget not configured — never blocks.
        return None

    preference = getattr(config_manager.plex, "pinned_preferred_resolution", "highest")
    path_mappings = settings.get("path_mappings", []) or []

    # Sum bytes for pins already on disk.
    try:
        resolved, _orphaned = resolve_pins_to_paths(plex, tracker, preference)
    except Exception as e:
        logging.debug(f"Budget preflight: resolver failed, assuming zero current pinned bytes: {e}")
        resolved = []
    cache_paths = {
        plex_to_cache_path(p, path_mappings)
        for (p, _rk, _pt) in resolved
    }
    cache_paths.discard(None)
    current = sum_pinned_bytes_on_disk(cache_paths)

    additional = estimate_item_bytes(plex, rating_key, pin_type, preference)

    state = compute_budget_state(
        cache_limit_bytes=parsed["cache_limit_bytes"],
        min_free_space_bytes=parsed["min_free_space_bytes"],
        current_pinned_bytes=current,
        additional_bytes=additional,
    )

    if not state["would_exceed"]:
        return None

    from core.system_utils import format_bytes
    return (
        f"Pinning this item would exceed the cache budget "
        f"({format_bytes(state['total_pinned_bytes'])} + "
        f"~{format_bytes(state['additional_bytes'])} > "
        f"{format_bytes(state['effective_budget_bytes'])}). "
        f"Unpin something first, or raise cache_limit in settings."
    )


def _connect_plex(config_manager: ConfigManager):
    """Connect to Plex server. Returns PlexServer instance or None."""
    plex_url = config_manager.plex.plex_url
    plex_token = config_manager.plex.plex_token
    if not plex_url or not plex_token:
        print("Error: Plex URL and token must be configured. Run --setup first.")
        return None
    try:
        from plexapi.server import PlexServer
        return PlexServer(plex_url, plex_token, timeout=10)
    except Exception as e:
        print(f"Error: Could not connect to Plex server: {e}")
        return None


def handle_list_pins(config_manager: ConfigManager) -> None:
    """Handle --list-pins: display all pinned media."""
    tracker = _get_tracker(config_manager)
    pins = tracker.list_pins()

    if not pins:
        print("No pinned media.")
        return

    print(f"Pinned media ({len(pins)} item{'s' if len(pins) != 1 else ''}):\n")

    for pin in pins:
        scope = pin.get("type", "unknown")
        title = pin.get("title", "Unknown")
        rk = pin.get("rating_key", "?")
        added_by = pin.get("added_by", "?")
        added_at = pin.get("added_at", "?")

        print(f"  [{scope}]  {title}")
        print(f"           rating_key={rk}  added_by={added_by}  added_at={added_at}")

    plex = _connect_plex(config_manager)
    if plex:
        preference = config_manager.plex.pinned_preferred_resolution
        from core.pinned_media import resolve_pins_to_paths
        resolved, orphaned = resolve_pins_to_paths(plex, tracker, preference)
        if resolved:
            print(f"\n  Resolved to {len(resolved)} file(s) (preference: {preference})")
        if orphaned:
            print(f"  {len(orphaned)} orphaned pin(s) were auto-removed (items no longer in Plex)")


def handle_pin(config_manager: ConfigManager, rating_key: str) -> None:
    """Handle --pin <rating_key>: pin a specific item by rating key."""
    tracker = _get_tracker(config_manager)

    if tracker.is_pinned(rating_key):
        print(f"Already pinned: rating_key={rating_key}")
        return

    plex = _connect_plex(config_manager)
    if not plex:
        return

    try:
        item = plex.fetchItem(int(rating_key))
    except Exception as e:
        print(f"Error: Could not fetch item {rating_key} from Plex: {e}")
        return

    pin_type = _derive_pin_type(item)
    title = getattr(item, "title", "Unknown")

    error = _preflight_budget(config_manager, tracker, plex, rating_key, pin_type)
    if error:
        print(f"Error: {error}")
        sys.exit(1)

    tracker.add_pin(rating_key, pin_type, title, added_by="cli")
    print(f"Pinned: [{pin_type}] {title} (rating_key={rating_key})")


def handle_unpin(config_manager: ConfigManager, rating_key: str) -> None:
    """Handle --unpin <rating_key>: unpin a specific item."""
    tracker = _get_tracker(config_manager)

    pin = tracker.get_pin(rating_key)
    if not pin:
        print(f"Not pinned: rating_key={rating_key}")
        return

    title = pin.get("title", "Unknown")
    tracker.remove_pin(rating_key)
    print(f"Unpinned: {title} (rating_key={rating_key})")


def handle_pin_by_title(config_manager: ConfigManager, query: str) -> None:
    """Handle --pin-by-title "title": search Plex and pin interactively."""
    plex = _connect_plex(config_manager)
    if not plex:
        return

    tracker = _get_tracker(config_manager)

    results = []
    for media_type in ("movie", "show"):
        try:
            hits = plex.search(query, mediatype=media_type, limit=10)
            for item in hits:
                rk = str(item.ratingKey)
                results.append({
                    "rating_key": rk,
                    "title": item.title,
                    "type": item.type,
                    "year": getattr(item, "year", ""),
                    "already_pinned": tracker.is_pinned(rk),
                })
        except Exception as e:
            logging.debug(f"Search for {media_type} failed: {e}")

    if not results:
        print(f'No results for "{query}".')
        return

    print(f'Search results for "{query}":\n')
    for i, r in enumerate(results, 1):
        pinned_marker = " [PINNED]" if r["already_pinned"] else ""
        year_str = f" ({r['year']})" if r["year"] else ""
        print(f"  {i}. [{r['type']}] {r['title']}{year_str}{pinned_marker}")

    print(f"\nEnter number to pin (1-{len(results)}), or 'q' to cancel: ", end="")
    try:
        choice = input().strip()
    except (EOFError, KeyboardInterrupt):
        print("\nCancelled.")
        return

    if choice.lower() in ("q", "quit", ""):
        print("Cancelled.")
        return

    try:
        idx = int(choice) - 1
        if idx < 0 or idx >= len(results):
            raise ValueError()
    except ValueError:
        print(f"Invalid choice: {choice}")
        return

    selected = results[idx]
    if selected["already_pinned"]:
        print(f"Already pinned: {selected['title']}")
        return

    pin_type = selected["type"] if selected["type"] in ("movie", "show") else "movie"

    error = _preflight_budget(
        config_manager, tracker, plex, selected["rating_key"], pin_type
    )
    if error:
        print(f"Error: {error}")
        sys.exit(1)

    tracker.add_pin(selected["rating_key"], pin_type, selected["title"], added_by="cli")
    print(f"Pinned: [{pin_type}] {selected['title']} (rating_key={selected['rating_key']})")


def _derive_pin_type(item) -> str:
    """Derive pin_type from a plexapi item."""
    item_type = getattr(item, "type", "")
    if item_type in ("movie", "show", "season", "episode"):
        return item_type
    return "movie"


def extract_flag_value(flag: str) -> Optional[str]:
    """Extract the value following a flag in sys.argv. Returns None if flag not found or no value."""
    try:
        idx = sys.argv.index(flag)
        if idx + 1 < len(sys.argv):
            return sys.argv[idx + 1]
    except ValueError:
        pass
    return None
