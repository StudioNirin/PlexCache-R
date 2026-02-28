"""Settings routes"""

import time
import uuid
import threading
from pathlib import Path
from typing import Dict, Any, List

import requests
from fastapi import APIRouter, Request, Form, Query
from fastapi.responses import HTMLResponse, JSONResponse

from web.config import templates, CONFIG_DIR, PLEXCACHE_PRODUCT_VERSION
from web.services import get_settings_service, get_scheduler_service
from core.system_utils import get_disk_usage, detect_zfs, parse_size_bytes
from core.file_operations import (
    PRIORITY_RANGE_ONDECK_MIN,
    PRIORITY_RANGE_ONDECK_MAX,
    PRIORITY_RANGE_WATCHLIST_MIN,
    PRIORITY_RANGE_WATCHLIST_MAX,
)


router = APIRouter()

# OAuth constants
PLEXCACHE_PRODUCT_NAME = 'PlexCache-D'

# Store OAuth state in memory (with lock for thread safety)
_oauth_state: Dict[str, Any] = {}
_oauth_state_lock = threading.Lock()


@router.get("/", response_class=HTMLResponse)
def settings_index(request: Request):
    """Settings overview - redirects to plex tab"""
    settings_service = get_settings_service()
    settings = settings_service.get_plex_settings()

    return templates.TemplateResponse(
        "settings/plex.html",
        {
            "request": request,
            "page_title": "Settings",
            "active_tab": "plex",
            "settings": settings,
        }
    )


@router.get("/plex", response_class=HTMLResponse)
def settings_plex(request: Request):
    """Plex Server connection settings tab"""
    settings_service = get_settings_service()
    settings = settings_service.get_plex_settings()

    return templates.TemplateResponse(
        "settings/plex.html",
        {
            "request": request,
            "page_title": "Plex Server Settings",
            "active_tab": "plex",
            "settings": settings,
        }
    )


@router.get("/plex/libraries", response_class=HTMLResponse)
def get_plex_libraries(request: Request):
    """Fetch library sections from Plex (HTMX partial)"""
    settings_service = get_settings_service()
    settings = settings_service.get_plex_settings()
    libraries = settings_service.get_plex_libraries()

    return templates.TemplateResponse(
        "settings/partials/library_checkboxes.html",
        {
            "request": request,
            "libraries": libraries,
            "selected_sections": settings.get("valid_sections", [])
        }
    )


@router.get("/plex/users", response_class=HTMLResponse)
def get_plex_users(request: Request):
    """Fetch users from Plex (HTMX partial)"""
    settings_service = get_settings_service()
    settings = settings_service.get_plex_settings()
    users = settings_service.get_plex_users()
    plex_error = settings_service.get_last_plex_error()

    return templates.TemplateResponse(
        "settings/partials/user_list.html",
        {
            "request": request,
            "users": users,
            "settings": settings,
            "plex_error": plex_error
        }
    )


@router.post("/plex/test", response_class=HTMLResponse)
def test_plex_connection(request: Request):
    """Test Plex connection and return detailed status"""
    settings_service = get_settings_service()
    settings = settings_service.get_plex_settings()

    plex_url = settings.get("plex_url", "")
    plex_token = settings.get("plex_token", "")

    if not plex_url or not plex_token:
        return templates.TemplateResponse(
            "partials/alert.html",
            {"request": request, "type": "error", "message": "Missing Plex URL or token. Save settings first."}
        )

    try:
        from plexapi.server import PlexServer
        plex = PlexServer(plex_url, plex_token, timeout=10)
        server_name = plex.friendlyName
        account = plex.myPlexAccount()
        username = account.username

        # Clear any previous error
        settings_service._last_plex_error = None

        return templates.TemplateResponse(
            "partials/alert.html",
            {"request": request, "type": "success", "message": f"Connected to '{server_name}' as {username}"}
        )
    except Exception as e:
        error_msg = str(e)
        # Provide helpful error messages
        if "Connection refused" in error_msg or "Errno 111" in error_msg:
            hint = "Cannot connect. Is Plex running? Try using your local IP (e.g., http://192.168.x.x:32400)"
        elif "timed out" in error_msg.lower() or "timeout" in error_msg.lower():
            hint = f"Connection timed out. The .plex.direct URL may not work from Docker. Try http://YOUR_LOCAL_IP:32400"
        elif "Name or service not known" in error_msg or "getaddrinfo failed" in error_msg:
            hint = "Cannot resolve hostname. Try using http://YOUR_LOCAL_IP:32400 instead of .plex.direct"
        elif "401" in error_msg or "Unauthorized" in error_msg:
            hint = "Invalid token. Try re-authenticating with Get Token."
        else:
            hint = f"Error: {error_msg[:150]}"

        return templates.TemplateResponse(
            "partials/alert.html",
            {"request": request, "type": "error", "message": hint}
        )


@router.put("/plex", response_class=HTMLResponse)
async def save_plex_settings(request: Request):
    """Save Plex Server connection settings (URL + token only)"""
    settings_service = get_settings_service()

    form = await request.form()
    plex_url = form.get("plex_url", "")
    plex_token = form.get("plex_token", "")

    success = settings_service.save_plex_settings({
        "plex_url": plex_url,
        "plex_token": plex_token,
    })

    if success:
        return templates.TemplateResponse(
            "partials/alert.html",
            {
                "request": request,
                "type": "success",
                "message": "Connection settings saved successfully"
            }
        )
    else:
        return templates.TemplateResponse(
            "partials/alert.html",
            {
                "request": request,
                "type": "error",
                "message": "Failed to save settings"
            }
        )


# =============================================================================
# Users tab endpoints
# =============================================================================

@router.get("/users", response_class=HTMLResponse)
def settings_users(request: Request):
    """Users settings tab - renders with skeleton for lazy load"""
    settings_service = get_settings_service()
    user_settings = settings_service.get_user_settings()
    plex_settings = settings_service.get_plex_settings()

    # Check if Plex is configured
    has_plex_config = bool(plex_settings.get("plex_url") and plex_settings.get("plex_token"))

    return templates.TemplateResponse(
        "settings/users.html",
        {
            "request": request,
            "page_title": "User Settings",
            "active_tab": "users",
            "settings": user_settings,
            "has_plex_config": has_plex_config
        }
    )


@router.get("/users/list", response_class=HTMLResponse)
def get_users_list(request: Request):
    """Fetch users list for lazy loading (HTMX partial)"""
    import logging
    logger = logging.getLogger(__name__)

    try:
        settings_service = get_settings_service()
        user_settings = settings_service.get_user_settings()
        plex_error = settings_service.get_last_plex_error()
        users = user_settings.get("users", [])

        logger.info(f"Loading users list: {len(users)} users found")

        return templates.TemplateResponse(
            "settings/partials/users_table.html",
            {
                "request": request,
                "users": users,
                "settings": user_settings,
                "plex_error": plex_error
            }
        )
    except Exception as e:
        logger.error(f"Error loading users list: {e}", exc_info=True)
        return HTMLResponse(
            f'<div class="alert alert-error"><i data-lucide="alert-circle"></i> Error loading users: {str(e)}</div>'
            '<script>lucide.createIcons();</script>'
        )


@router.post("/users/sync", response_class=HTMLResponse)
def sync_users(request: Request):
    """Sync users from Plex (HTMX)"""
    settings_service = get_settings_service()
    result = settings_service.sync_users_from_plex()

    if result["success"]:
        message = f"Synced {len(result['users'])} users"
        if result["added_count"] > 0:
            message += f" (+{result['added_count']} new)"
        if result["removed_count"] > 0:
            message += f" (-{result['removed_count']} removed)"

        # Return updated user list with success message
        user_settings = settings_service.get_user_settings()
        return templates.TemplateResponse(
            "settings/partials/users_sync_result.html",
            {
                "request": request,
                "success": True,
                "message": message,
                "users": result["users"],
                "settings": user_settings
            }
        )
    else:
        return templates.TemplateResponse(
            "partials/alert.html",
            {
                "request": request,
                "type": "error",
                "message": f"Sync failed: {result['error']}"
            }
        )


@router.put("/users", response_class=HTMLResponse)
async def save_user_settings(request: Request):
    """Save user preferences"""
    settings_service = get_settings_service()
    form = await request.form()

    # Get current users from settings
    user_settings = settings_service.get_user_settings()
    users = user_settings.get("users", [])

    # Update skip flags from form
    # Form uses: include_ondeck_{title} and include_watchlist_{title}
    # Checkbox ON = include (not skip), OFF = skip
    for user in users:
        title = user.get("title", "")
        # Checkboxes: "on" if checked, absent if unchecked
        include_ondeck = form.get(f"include_ondeck_{title}") == "on"
        include_watchlist = form.get(f"include_watchlist_{title}") == "on"

        # skip = NOT include (checkbox off means skip)
        user["skip_ondeck"] = not include_ondeck
        # All users can have watchlist disabled (local via API, remote via RSS filtering)
        user["skip_watchlist"] = not include_watchlist

    # Get toggle settings
    users_toggle = form.get("users_toggle") == "on"
    remote_watchlist_toggle = form.get("remote_watchlist_toggle") == "on"
    remote_watchlist_rss_url = form.get("remote_watchlist_rss_url", "")

    success = settings_service.save_user_settings(
        users=users,
        users_toggle=users_toggle,
        remote_watchlist_toggle=remote_watchlist_toggle,
        remote_watchlist_rss_url=remote_watchlist_rss_url
    )

    if success:
        return templates.TemplateResponse(
            "partials/alert.html",
            {"request": request, "type": "success", "message": "User settings saved successfully"}
        )
    else:
        return templates.TemplateResponse(
            "partials/alert.html",
            {"request": request, "type": "error", "message": "Failed to save settings"}
        )


@router.get("/paths", response_class=HTMLResponse)
def settings_paths(request: Request):
    """Path mappings tab — redirects to Libraries tab"""
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/settings/libraries", status_code=302)


@router.post("/paths", response_class=HTMLResponse)
def add_path_mapping(
    request: Request,
    name: str = Form(...),
    plex_path: str = Form(...),
    real_path: str = Form(...),
    cache_path: str = Form(""),
    host_cache_path: str = Form(""),
    cacheable: str = Form(None),
    enabled: str = Form(None)
):
    """Add a new path mapping"""
    settings_service = get_settings_service()

    # Default host_cache_path to cache_path if not provided
    effective_host_cache_path = host_cache_path if host_cache_path else cache_path

    mapping = {
        "name": name,
        "plex_path": plex_path,
        "real_path": real_path,
        "cache_path": cache_path if cache_path else None,
        "host_cache_path": effective_host_cache_path if effective_host_cache_path else None,
        "cacheable": cacheable == "on",
        "enabled": enabled == "on"
    }

    success = settings_service.add_path_mapping(mapping)

    if success:
        # Return the new mapping card with its index
        mappings = settings_service.get_path_mappings()
        index = len(mappings) - 1
        return templates.TemplateResponse(
            "settings/partials/path_mapping_card.html",
            {
                "request": request,
                "mapping": mapping,
                "index": index,
            }
        )
    else:
        return HTMLResponse("<div class='alert alert-error'>Failed to add mapping</div>")


@router.put("/paths/{index}", response_class=HTMLResponse)
def update_path_mapping(
    request: Request,
    index: int,
    name: str = Form(...),
    plex_path: str = Form(...),
    real_path: str = Form(...),
    cache_path: str = Form(""),
    host_cache_path: str = Form(""),
    cacheable: str = Form(None),
    enabled: str = Form(None)
):
    """Update an existing path mapping"""
    settings_service = get_settings_service()

    # Default host_cache_path to cache_path if not provided
    effective_host_cache_path = host_cache_path if host_cache_path else cache_path

    mapping = {
        "name": name,
        "plex_path": plex_path,
        "real_path": real_path,
        "cache_path": cache_path if cache_path else None,
        "host_cache_path": effective_host_cache_path if effective_host_cache_path else None,
        "cacheable": cacheable == "on",
        "enabled": enabled == "on"
    }

    success = settings_service.update_path_mapping(index, mapping)

    if success:
        return templates.TemplateResponse(
            "settings/partials/path_mapping_card.html",
            {
                "request": request,
                "mapping": mapping,
                "index": index,
            }
        )
    else:
        return HTMLResponse("<div class='alert alert-error'>Failed to update mapping</div>")


@router.delete("/paths/{index}", response_class=HTMLResponse)
def delete_path_mapping(request: Request, index: int):
    """Delete a path mapping and return the updated list"""
    settings_service = get_settings_service()

    success = settings_service.delete_path_mapping(index)

    if success:
        # Return the full updated list with fresh indices
        mappings = settings_service.get_path_mappings()
        return templates.TemplateResponse(
            "settings/partials/path_mappings_list.html",
            {"request": request, "mappings": mappings}
        )
    else:
        return HTMLResponse("<div class='alert alert-error'>Failed to delete mapping</div>")


# =============================================================================
# Libraries tab endpoints
# =============================================================================

@router.get("/libraries", response_class=HTMLResponse)
def settings_libraries(request: Request):
    """Libraries tab — combined library toggle + path mappings"""
    settings_service = get_settings_service()

    # Run one-time migration (links existing path_mappings to Plex libraries)
    settings_service.migrate_link_path_mappings_to_libraries()

    # Fetch libraries from Plex
    libraries = settings_service.get_plex_libraries()

    # Load path mappings
    raw_settings = settings_service.get_all()
    mappings = raw_settings.get("path_mappings", [])
    valid_sections = raw_settings.get("valid_sections", [])

    # Group mappings by section_id
    library_mappings = {}  # section_id -> list of mappings (with _index)
    orphan_mappings = []   # mappings without section_id
    for i, m in enumerate(mappings):
        m_copy = dict(m)
        m_copy["_index"] = i
        sid = m.get("section_id")
        if sid is not None:
            library_mappings.setdefault(sid, []).append(m_copy)
        else:
            orphan_mappings.append(m_copy)

    # Build library cards
    library_cards = []
    for lib in libraries:
        sid = lib["id"]
        lib_maps = library_mappings.get(sid, [])
        enabled = sid in valid_sections or any(m.get("enabled", True) for m in lib_maps)
        library_cards.append({
            "library": lib,
            "enabled": enabled,
            "mappings": lib_maps,
            "has_mappings": bool(lib_maps),
        })

    return templates.TemplateResponse(
        "settings/libraries.html",
        {
            "request": request,
            "page_title": "Library Settings",
            "active_tab": "libraries",
            "library_cards": library_cards,
            "orphan_mappings": orphan_mappings,
        }
    )


@router.post("/libraries/{section_id}/toggle", response_class=HTMLResponse)
def toggle_library(request: Request, section_id: int):
    """Toggle a Plex library on/off"""
    settings_service = get_settings_service()
    raw = settings_service._load_raw()
    mappings = raw.get("path_mappings", [])

    # Check current state — any enabled mapping with this section_id?
    current_mappings = [m for m in mappings if m.get("section_id") == section_id]
    currently_enabled = any(m.get("enabled", True) for m in current_mappings)

    if currently_enabled:
        # Turn OFF: disable all mappings with this section_id
        for m in mappings:
            if m.get("section_id") == section_id:
                m["enabled"] = False
    else:
        # Turn ON
        if current_mappings:
            # Re-enable existing mappings
            for m in mappings:
                if m.get("section_id") == section_id:
                    m["enabled"] = True
        else:
            # Auto-create mappings from Plex library
            libraries = settings_service.get_plex_libraries()
            library = next((lib for lib in libraries if lib["id"] == section_id), None)
            if library:
                for loc in library.get("locations", []):
                    new_mapping = settings_service.auto_fill_mapping(library, loc, raw)
                    mappings.append(new_mapping)

    raw["path_mappings"] = mappings
    settings_service._rebuild_valid_sections(raw)
    settings_service._save_raw(raw)

    # Re-render this library card
    libraries = settings_service.get_plex_libraries()
    library = next((lib for lib in libraries if lib["id"] == section_id), None)
    if not library:
        return HTMLResponse("<div class='alert alert-error'>Library not found</div>")

    # Reload mappings for this card
    raw = settings_service._load_raw()
    all_mappings = raw.get("path_mappings", [])
    lib_maps = []
    for i, m in enumerate(all_mappings):
        if m.get("section_id") == section_id:
            m_copy = dict(m)
            m_copy["_index"] = i
            lib_maps.append(m_copy)

    enabled = any(m.get("enabled", True) for m in lib_maps)
    card = {
        "library": library,
        "enabled": enabled,
        "mappings": lib_maps,
        "has_mappings": bool(lib_maps),
    }

    return templates.TemplateResponse(
        "settings/partials/library_card.html",
        {"request": request, "card": card}
    )


@router.put("/libraries/paths/{index}", response_class=HTMLResponse)
def update_library_path(
    request: Request,
    index: int,
    name: str = Form(...),
    plex_path: str = Form(...),
    real_path: str = Form(...),
    cache_path: str = Form(""),
    host_cache_path: str = Form(""),
    cacheable: str = Form(None),
):
    """Update a library's path mapping and return refreshed library card"""
    settings_service = get_settings_service()

    effective_host_cache_path = host_cache_path if host_cache_path else cache_path

    mapping = {
        "name": name,
        "plex_path": plex_path,
        "real_path": real_path,
        "cache_path": cache_path if cache_path else None,
        "host_cache_path": effective_host_cache_path if effective_host_cache_path else None,
        "cacheable": cacheable == "on",
        "enabled": True,  # Editing implies enabled
    }

    success = settings_service.update_path_mapping(index, mapping)

    if not success:
        return HTMLResponse("<div class='alert alert-error'>Failed to update mapping</div>")

    # Get the section_id to re-render the library card
    raw = settings_service._load_raw()
    all_mappings = raw.get("path_mappings", [])
    section_id = all_mappings[index].get("section_id") if index < len(all_mappings) else None

    if section_id is None:
        # Orphan mapping — return a path_mapping_card
        return templates.TemplateResponse(
            "settings/partials/path_mapping_card.html",
            {"request": request, "mapping": all_mappings[index], "index": index}
        )

    # Rebuild valid_sections after update
    settings_service._rebuild_valid_sections(raw)
    settings_service._save_raw(raw)

    # Re-render the full library card
    libraries = settings_service.get_plex_libraries()
    library = next((lib for lib in libraries if lib["id"] == section_id), None)
    if not library:
        return HTMLResponse("<div class='alert alert-success'>Saved</div>")

    lib_maps = []
    for i, m in enumerate(all_mappings):
        if m.get("section_id") == section_id:
            m_copy = dict(m)
            m_copy["_index"] = i
            lib_maps.append(m_copy)

    card = {
        "library": library,
        "enabled": any(m.get("enabled", True) for m in lib_maps),
        "mappings": lib_maps,
        "has_mappings": bool(lib_maps),
    }

    return templates.TemplateResponse(
        "settings/partials/library_card.html",
        {"request": request, "card": card}
    )


@router.get("/cache", response_class=HTMLResponse)
def settings_cache(request: Request):
    """Cache settings tab"""
    import shutil
    settings_service = get_settings_service()
    settings = settings_service.get_cache_settings()

    # Get cache drive info for real-time calculations
    drive_info = {"total_bytes": 0, "total_display": "Unknown"}
    all_settings = settings_service.get_all()

    # Try to get cache_dir from path_mappings first, then fall back to cache_dir setting
    cache_dir = None
    path_mappings = all_settings.get("path_mappings", [])
    for mapping in path_mappings:
        if mapping.get("enabled") and mapping.get("cacheable") and mapping.get("cache_path"):
            cache_dir = mapping.get("cache_path")
            break
    if not cache_dir:
        cache_dir = all_settings.get("cache_dir", "")

    if cache_dir:
        try:
            drive_size_override = parse_size_bytes(all_settings.get("cache_drive_size", ""))
            disk_usage = get_disk_usage(cache_dir, drive_size_override)
            drive_info["total_bytes"] = disk_usage.total
            # Format size
            total_gb = disk_usage.total / (1024**3)
            if total_gb >= 1024:
                drive_info["total_display"] = f"{total_gb/1024:.2f} TB"
            else:
                drive_info["total_display"] = f"{total_gb:.2f} GB"
            drive_info["free_bytes"] = disk_usage.free
            drive_info["used_bytes"] = disk_usage.used
            used_gb = disk_usage.used / (1024**3)
            if used_gb >= 1024:
                drive_info["used_display"] = f"{used_gb/1024:.2f} TB"
            else:
                drive_info["used_display"] = f"{used_gb:.2f} GB"
            # Add flag to indicate if using manual override
            if drive_size_override > 0:
                drive_info["is_manual_override"] = True
            # Detect ZFS (values may need manual override)
            drive_info["is_zfs"] = detect_zfs(cache_dir)
        except Exception:
            pass

    # Get tracked PlexCache files size for quota calculations
    try:
        from web.services import get_cache_service
        cache_service = get_cache_service()
        all_files = cache_service.get_all_cached_files()
        drive_info["cached_files_bytes"] = sum(f.size for f in all_files)
    except Exception:
        drive_info["cached_files_bytes"] = 0

    return templates.TemplateResponse(
        "settings/cache.html",
        {
            "request": request,
            "page_title": "Cache Settings",
            "active_tab": "cache",
            "settings": settings,
            "drive_info": drive_info,
            "priority_range": {
                "ondeck_min": PRIORITY_RANGE_ONDECK_MIN,
                "ondeck_max": PRIORITY_RANGE_ONDECK_MAX,
                "watchlist_min": PRIORITY_RANGE_WATCHLIST_MIN,
                "watchlist_max": PRIORITY_RANGE_WATCHLIST_MAX,
            }
        }
    )


@router.put("/cache", response_class=HTMLResponse)
async def save_cache_settings(request: Request):
    """Save cache settings"""
    settings_service = get_settings_service()

    # Parse form data
    form = await request.form()
    settings_dict = dict(form)

    # Handle list fields that need getlist() instead of single value
    excluded_folders = form.getlist("excluded_folders")
    settings_dict["excluded_folders"] = [f for f in excluded_folders if f and f.strip()]

    success = settings_service.save_cache_settings(settings_dict)

    if success:
        return templates.TemplateResponse(
            "partials/alert.html",
            {
                "request": request,
                "type": "success",
                "message": "Cache settings saved successfully"
            }
        )
    else:
        return templates.TemplateResponse(
            "partials/alert.html",
            {
                "request": request,
                "type": "error",
                "message": "Failed to save settings"
            }
        )


@router.get("/notifications", response_class=HTMLResponse)
def settings_notifications(request: Request):
    """Notification settings tab"""
    import os

    settings_service = get_settings_service()
    settings = settings_service.get_notification_settings()

    # Check if Unraid notify script is available (for Docker info message)
    notify_paths = [
        "/usr/local/emhttp/plugins/dynamix/scripts/notify",
        "/usr/local/emhttp/webGui/scripts/notify",
    ]
    unraid_notify_available = any(os.path.isfile(p) for p in notify_paths)

    return templates.TemplateResponse(
        "settings/notifications.html",
        {
            "request": request,
            "page_title": "Notification Settings",
            "active_tab": "notifications",
            "settings": settings,
            "unraid_notify_available": unraid_notify_available
        }
    )


@router.put("/notifications", response_class=HTMLResponse)
def save_notification_settings(
    request: Request,
    notification_type: str = Form("system"),
    webhook_url: str = Form(""),
    unraid_levels: List[str] = Form([]),
    webhook_levels: List[str] = Form([])
):
    """Save notification settings"""
    settings_service = get_settings_service()

    success = settings_service.save_notification_settings({
        "notification_type": notification_type,
        "webhook_url": webhook_url,
        "unraid_levels": unraid_levels,
        "webhook_levels": webhook_levels,
        # Keep legacy fields for backward compatibility
        "unraid_level": unraid_levels[0] if unraid_levels else "summary",
        "webhook_level": webhook_levels[0] if webhook_levels else "summary"
    })

    if success:
        return templates.TemplateResponse(
            "partials/alert.html",
            {
                "request": request,
                "type": "success",
                "message": "Notification settings saved successfully"
            }
        )
    else:
        return templates.TemplateResponse(
            "partials/alert.html",
            {
                "request": request,
                "type": "error",
                "message": "Failed to save settings"
            }
        )


@router.post("/notifications/test", response_class=HTMLResponse)
def test_webhook(request: Request, webhook_url: str = Form(...)):
    """Send a test message to the configured webhook"""
    import json
    import requests
    from datetime import datetime

    if not webhook_url:
        return templates.TemplateResponse(
            "partials/alert.html",
            {"request": request, "type": "error", "message": "No webhook URL provided"}
        )

    # Detect platform from URL
    url_lower = webhook_url.lower()
    if 'discord.com/api/webhooks/' in url_lower or 'discordapp.com/api/webhooks/' in url_lower:
        platform = 'discord'
    elif 'hooks.slack.com/services/' in url_lower:
        platform = 'slack'
    else:
        platform = 'generic'

    # Build test payload based on platform
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if platform == 'discord':
        payload = {
            "embeds": [{
                "title": "PlexCache-D Test Notification",
                "description": "This is a test message from PlexCache-D. Your webhook is configured correctly!",
                "color": 3066993,  # Green
                "fields": [
                    {"name": "Status", "value": "Connected", "inline": True},
                    {"name": "Platform", "value": "Discord", "inline": True}
                ],
                "footer": {"text": f"Sent at {timestamp}"}
            }]
        }
    elif platform == 'slack':
        payload = {
            "blocks": [
                {
                    "type": "header",
                    "text": {"type": "plain_text", "text": "PlexCache-D Test Notification"}
                },
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": "This is a test message from PlexCache-D. Your webhook is configured correctly!"}
                },
                {
                    "type": "context",
                    "elements": [{"type": "mrkdwn", "text": f"Sent at {timestamp}"}]
                }
            ]
        }
    else:
        payload = {
            "text": f"PlexCache-D Test Notification\n\nThis is a test message from PlexCache-D. Your webhook is configured correctly!\n\nSent at {timestamp}"
        }

    # Send the test message
    try:
        response = requests.post(
            webhook_url,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=10
        )

        if response.status_code in [200, 204]:
            return templates.TemplateResponse(
                "partials/alert.html",
                {"request": request, "type": "success", "message": f"Test message sent successfully! (Platform: {platform.title()})"}
            )
        else:
            return templates.TemplateResponse(
                "partials/alert.html",
                {"request": request, "type": "error", "message": f"Webhook returned HTTP {response.status_code}: {response.text[:100]}"}
            )
    except requests.Timeout:
        return templates.TemplateResponse(
            "partials/alert.html",
            {"request": request, "type": "error", "message": "Webhook request timed out"}
        )
    except requests.RequestException as e:
        return templates.TemplateResponse(
            "partials/alert.html",
            {"request": request, "type": "error", "message": f"Webhook request failed: {str(e)[:100]}"}
        )


@router.get("/logging", response_class=HTMLResponse)
def settings_logging(request: Request):
    """Logging settings tab"""
    settings_service = get_settings_service()
    settings = settings_service.get_logging_settings()

    return templates.TemplateResponse(
        "settings/logging.html",
        {
            "request": request,
            "page_title": "Logging Settings",
            "active_tab": "logging",
            "settings": settings
        }
    )


@router.put("/logging", response_class=HTMLResponse)
def save_logging_settings(
    request: Request,
    max_log_files: int = Form(24),
    keep_error_logs_days: int = Form(7),
    time_format: str = Form("24h"),
    activity_retention_hours: int = Form(24)
):
    """Save logging settings"""
    settings_service = get_settings_service()

    success = settings_service.save_logging_settings({
        "max_log_files": max_log_files,
        "keep_error_logs_days": keep_error_logs_days,
        "time_format": time_format,
        "activity_retention_hours": activity_retention_hours
    })

    if success:
        return templates.TemplateResponse(
            "partials/alert.html",
            {
                "request": request,
                "type": "success",
                "message": "Logging settings saved successfully"
            }
        )
    else:
        return templates.TemplateResponse(
            "partials/alert.html",
            {
                "request": request,
                "type": "error",
                "message": "Failed to save settings"
            }
        )


# =============================================================================
# Integrations tab endpoints (Sonarr/Radarr)
# =============================================================================

@router.get("/integrations", response_class=HTMLResponse)
def settings_integrations(request: Request):
    """Integrations settings tab"""
    settings_service = get_settings_service()
    instances = settings_service.get_arr_instances()

    return templates.TemplateResponse(
        "settings/integrations.html",
        {
            "request": request,
            "page_title": "Integration Settings",
            "active_tab": "integrations",
            "instances": instances,
        }
    )


@router.post("/integrations/instances", response_class=HTMLResponse)
def add_arr_instance(
    request: Request,
    name: str = Form(...),
    arr_type: str = Form(...),
    url: str = Form(""),
    api_key: str = Form(""),
    enabled: str = Form(None),
):
    """Add a new Sonarr/Radarr instance"""
    settings_service = get_settings_service()

    instance = {
        "name": name,
        "type": arr_type,
        "url": url,
        "api_key": api_key,
        "enabled": enabled == "on",
    }

    success = settings_service.add_arr_instance(instance)

    if success:
        instances = settings_service.get_arr_instances()
        index = len(instances) - 1
        return templates.TemplateResponse(
            "settings/partials/arr_instance_card.html",
            {"request": request, "instance": instances[index], "index": index}
        )
    else:
        return HTMLResponse("<div class='alert alert-error'>Failed to add instance</div>")


@router.put("/integrations/instances/{index}", response_class=HTMLResponse)
def update_arr_instance(
    request: Request,
    index: int,
    name: str = Form(...),
    arr_type: str = Form(...),
    url: str = Form(""),
    api_key: str = Form(""),
    enabled: str = Form(None),
):
    """Update an existing Sonarr/Radarr instance"""
    settings_service = get_settings_service()

    instance = {
        "name": name,
        "type": arr_type,
        "url": url,
        "api_key": api_key,
        "enabled": enabled == "on",
    }

    success = settings_service.update_arr_instance(index, instance)

    if success:
        return templates.TemplateResponse(
            "settings/partials/arr_instance_card.html",
            {"request": request, "instance": instance, "index": index}
        )
    else:
        return HTMLResponse("<div class='alert alert-error'>Failed to update instance</div>")


@router.delete("/integrations/instances/{index}", response_class=HTMLResponse)
def delete_arr_instance(request: Request, index: int):
    """Delete a Sonarr/Radarr instance and return the updated list"""
    settings_service = get_settings_service()

    success = settings_service.delete_arr_instance(index)

    if success:
        instances = settings_service.get_arr_instances()
        return templates.TemplateResponse(
            "settings/partials/arr_instances_list.html",
            {"request": request, "instances": instances}
        )
    else:
        return HTMLResponse("<div class='alert alert-error'>Failed to delete instance</div>")


@router.post("/integrations/test")
def test_arr_connection(
    url: str = Form(""),
    api_key: str = Form(""),
    arr_type: str = Form("sonarr"),
):
    """Test Sonarr/Radarr connection using form values (no save required)"""
    url = url.strip()
    api_key = api_key.strip()

    if not url or not api_key:
        return JSONResponse({"success": False, "message": "URL and API key are required"})

    type_label = arr_type.title()  # "Sonarr" or "Radarr"

    try:
        resp = requests.get(
            f'{url.rstrip("/")}/api/v3/system/status',
            headers={'X-Api-Key': api_key},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        version = data.get("version", "unknown")
        return JSONResponse({"success": True, "message": f"Connected to {type_label} v{version}"})
    except requests.Timeout:
        return JSONResponse({"success": False, "message": "Connection timed out"})
    except requests.ConnectionError:
        return JSONResponse({"success": False, "message": f"Cannot connect. Is {type_label} running?"})
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 401:
            return JSONResponse({"success": False, "message": "Invalid API key (401 Unauthorized)"})
        return JSONResponse({"success": False, "message": f"HTTP error: {e}"})
    except Exception as e:
        return JSONResponse({"success": False, "message": f"Error: {str(e)[:150]}"})


@router.get("/schedule", response_class=HTMLResponse)
def settings_schedule(request: Request):
    """Schedule settings tab"""
    scheduler_service = get_scheduler_service()
    schedule = scheduler_service.get_status()

    return templates.TemplateResponse(
        "settings/schedule.html",
        {
            "request": request,
            "page_title": "Schedule Settings",
            "active_tab": "schedule",
            "schedule": schedule
        }
    )


@router.get("/import", response_class=HTMLResponse)
def settings_import(request: Request):
    """Redirect old import tab to new import-export tab"""
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/settings/import-export", status_code=302)


@router.get("/backup", response_class=HTMLResponse)
def settings_backup_redirect(request: Request):
    """Redirect old backup tab to new import-export tab"""
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/settings/import-export", status_code=302)


# =============================================================================
# Import/Export endpoints
# =============================================================================

@router.get("/import-export", response_class=HTMLResponse)
def settings_import_export(request: Request):
    """Import/Export settings tab"""
    # Check if any previous CLI imports have been completed
    import_completed_dir = CONFIG_DIR / "import" / "completed"
    has_completed_import = import_completed_dir.exists() and any(import_completed_dir.iterdir()) if import_completed_dir.exists() else False

    return templates.TemplateResponse(
        "settings/backup.html",
        {
            "request": request,
            "page_title": "Import/Export Settings",
            "active_tab": "import-export",
            "has_completed_import": has_completed_import
        }
    )


@router.get("/import-export/export")
def export_settings_file(request: Request, include_sensitive: bool = True):
    """Export settings as downloadable JSON file"""
    import json
    from datetime import datetime
    from fastapi.responses import StreamingResponse
    from io import BytesIO

    settings_service = get_settings_service()
    settings = settings_service.export_settings(include_sensitive=include_sensitive)

    # Create JSON content
    content = json.dumps(settings, indent=2)
    content_bytes = content.encode('utf-8')

    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"plexcache_backup_{timestamp}.json"

    # Return as downloadable file
    return StreamingResponse(
        BytesIO(content_bytes),
        media_type="application/json",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Content-Length": str(len(content_bytes))
        }
    )


@router.post("/import-export/validate", response_class=HTMLResponse)
async def validate_settings_file(request: Request):
    """Validate uploaded settings JSON file"""
    import json

    settings_service = get_settings_service()

    # Get uploaded file
    form = await request.form()
    file = form.get("settings_file")

    if not file:
        return templates.TemplateResponse(
            "settings/partials/backup_validation.html",
            {
                "request": request,
                "valid": False,
                "errors": ["No file uploaded"],
                "warnings": []
            }
        )

    try:
        # Read and parse JSON
        content = await file.read()
        settings_data = json.loads(content.decode('utf-8'))
    except json.JSONDecodeError as e:
        return templates.TemplateResponse(
            "settings/partials/backup_validation.html",
            {
                "request": request,
                "valid": False,
                "errors": [f"Invalid JSON: {str(e)}"],
                "warnings": []
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            "settings/partials/backup_validation.html",
            {
                "request": request,
                "valid": False,
                "errors": [f"Error reading file: {str(e)}"],
                "warnings": []
            }
        )

    # Validate settings structure
    result = settings_service.validate_import_settings(settings_data)

    return templates.TemplateResponse(
        "settings/partials/backup_validation.html",
        {
            "request": request,
            "valid": result["valid"],
            "errors": result["errors"],
            "warnings": result["warnings"]
        }
    )


@router.post("/import-export/import", response_class=HTMLResponse)
async def import_settings_file(request: Request):
    """Import settings from uploaded JSON file"""
    import json

    settings_service = get_settings_service()

    # Get form data
    form = await request.form()
    file = form.get("settings_file")
    merge_mode = form.get("import_mode") == "merge"

    if not file:
        return templates.TemplateResponse(
            "partials/alert.html",
            {
                "request": request,
                "type": "error",
                "message": "No file uploaded"
            }
        )

    try:
        # Read and parse JSON
        content = await file.read()
        settings_data = json.loads(content.decode('utf-8'))
    except json.JSONDecodeError as e:
        return templates.TemplateResponse(
            "partials/alert.html",
            {
                "request": request,
                "type": "error",
                "message": f"Invalid JSON: {str(e)}"
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            "partials/alert.html",
            {
                "request": request,
                "type": "error",
                "message": f"Error reading file: {str(e)}"
            }
        )

    # Validate first
    validation = settings_service.validate_import_settings(settings_data)
    if not validation["valid"]:
        return templates.TemplateResponse(
            "partials/alert.html",
            {
                "request": request,
                "type": "error",
                "message": f"Validation failed: {', '.join(validation['errors'])}"
            }
        )

    # Import settings
    result = settings_service.import_settings(settings_data, merge=merge_mode)

    if result["success"]:
        mode_text = "merged with" if merge_mode else "replaced"
        return templates.TemplateResponse(
            "partials/alert.html",
            {
                "request": request,
                "type": "success",
                "message": f"Settings {mode_text} successfully. Refresh the page to see changes."
            }
        )
    else:
        return templates.TemplateResponse(
            "partials/alert.html",
            {
                "request": request,
                "type": "error",
                "message": result["message"]
            }
        )


# =============================================================================
# OAuth endpoints for Plex authentication (server-side flow)
# =============================================================================

def _get_or_create_client_id() -> str:
    """Get existing client ID from settings or create a new one"""
    try:
        settings_service = get_settings_service()
        settings = settings_service.get_all()

        if settings.get("plexcache_client_id"):
            return settings["plexcache_client_id"]

        # Generate and save new client ID
        client_id = str(uuid.uuid4())
        settings_service.save_general_settings({"plexcache_client_id": client_id})
        return client_id
    except Exception as e:
        import logging
        logging.warning(f"Could not load/save client ID: {e}")
        return str(uuid.uuid4())


@router.post("/plex/oauth/start")
def oauth_start():
    """Start Plex OAuth flow - returns auth URL"""
    client_id = _get_or_create_client_id()

    headers = {
        'Accept': 'application/json',
        'X-Plex-Product': PLEXCACHE_PRODUCT_NAME,
        'X-Plex-Version': PLEXCACHE_PRODUCT_VERSION,
        'X-Plex-Client-Identifier': client_id,
    }

    try:
        response = requests.post(
            'https://plex.tv/api/v2/pins',
            headers=headers,
            data={'strong': 'true'},
            timeout=30
        )
        response.raise_for_status()
        pin_data = response.json()
    except requests.RequestException as e:
        return JSONResponse({"success": False, "error": str(e)})

    pin_id = pin_data.get('id')
    pin_code = pin_data.get('code')

    if not pin_id or not pin_code:
        return JSONResponse({"success": False, "error": "Invalid response from Plex"})

    # Store pin for polling
    with _oauth_state_lock:
        _oauth_state[client_id] = {
            "pin_id": pin_id,
            "pin_code": pin_code,
            "created": time.time()
        }

    auth_url = f"https://app.plex.tv/auth#?clientID={client_id}&code={pin_code}&context%5Bdevice%5D%5Bproduct%5D={PLEXCACHE_PRODUCT_NAME}"

    return JSONResponse({
        "success": True,
        "auth_url": auth_url,
        "client_id": client_id
    })


@router.get("/plex/oauth/poll")
def oauth_poll(client_id: str = Query(...)):
    """Poll for OAuth completion"""
    with _oauth_state_lock:
        if client_id not in _oauth_state:
            return JSONResponse({"success": False, "error": "Invalid or expired client ID"})

        state = _oauth_state[client_id]
        pin_id = state["pin_id"]

        # Check if state is too old (10 minutes)
        if time.time() - state["created"] > 600:
            del _oauth_state[client_id]
            return JSONResponse({"success": False, "error": "OAuth session expired"})

    headers = {
        'Accept': 'application/json',
        'X-Plex-Product': PLEXCACHE_PRODUCT_NAME,
        'X-Plex-Version': PLEXCACHE_PRODUCT_VERSION,
        'X-Plex-Client-Identifier': client_id,
    }

    try:
        response = requests.get(
            f'https://plex.tv/api/v2/pins/{pin_id}',
            headers=headers,
            timeout=30
        )
        response.raise_for_status()
        pin_status = response.json()

        auth_token = pin_status.get('authToken')
        if auth_token:
            # Clean up state
            with _oauth_state_lock:
                _oauth_state.pop(client_id, None)
            return JSONResponse({
                "success": True,
                "complete": True,
                "token": auth_token
            })

        return JSONResponse({
            "success": True,
            "complete": False
        })

    except requests.RequestException as e:
        return JSONResponse({"success": False, "error": str(e)})
