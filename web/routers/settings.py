"""Settings routes"""

import time
import uuid
from pathlib import Path
from typing import Dict, Any, List

import requests
from fastapi import APIRouter, Request, Form, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from web.config import TEMPLATES_DIR, CONFIG_DIR
from web.services import get_settings_service, get_scheduler_service

router = APIRouter()
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# OAuth constants
PLEXCACHE_PRODUCT_NAME = 'PlexCache-R'
PLEXCACHE_PRODUCT_VERSION = '3.0'

# Store OAuth state in memory
_oauth_state: Dict[str, Any] = {}


@router.get("/", response_class=HTMLResponse)
async def settings_index(request: Request):
    """Settings overview - redirects to plex tab"""
    settings_service = get_settings_service()
    settings = settings_service.get_plex_settings()
    libraries = settings_service.get_plex_libraries()
    users = settings_service.get_plex_users()
    plex_error = settings_service.get_last_plex_error()

    return templates.TemplateResponse(
        "settings/plex.html",
        {
            "request": request,
            "page_title": "Settings",
            "active_tab": "plex",
            "settings": settings,
            "libraries": libraries,
            "users": users,
            "plex_error": plex_error
        }
    )


@router.get("/plex", response_class=HTMLResponse)
async def settings_plex(request: Request):
    """Plex settings tab"""
    settings_service = get_settings_service()
    settings = settings_service.get_plex_settings()
    libraries = settings_service.get_plex_libraries()
    users = settings_service.get_plex_users()
    plex_error = settings_service.get_last_plex_error()

    return templates.TemplateResponse(
        "settings/plex.html",
        {
            "request": request,
            "page_title": "Plex Settings",
            "active_tab": "plex",
            "settings": settings,
            "libraries": libraries,
            "users": users,
            "plex_error": plex_error
        }
    )


@router.get("/plex/libraries", response_class=HTMLResponse)
async def get_plex_libraries(request: Request):
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
async def get_plex_users(request: Request):
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
async def test_plex_connection(request: Request):
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
    """Save Plex settings"""
    settings_service = get_settings_service()

    # Parse form data (need to handle multi-value checkbox fields)
    form = await request.form()

    # Get single values
    plex_url = form.get("plex_url", "")
    plex_token = form.get("plex_token", "")
    days_to_monitor = int(form.get("days_to_monitor", 183))
    number_episodes = int(form.get("number_episodes", 5))
    users_toggle = form.get("users_toggle") == "on"

    # Get multi-value checkbox fields
    valid_sections = [int(v) for v in form.getlist("valid_sections")]

    # Convert "include" lists to "skip" lists
    # Get all users to determine who was unchecked
    all_users = settings_service.get_plex_users()
    all_usernames = {u["username"] for u in all_users if not u.get("is_admin")}

    include_ondeck = set(form.getlist("include_ondeck"))
    include_watchlist = set(form.getlist("include_watchlist"))

    # Users not in include list = skip list (exclude admin)
    skip_ondeck = list(all_usernames - include_ondeck)
    skip_watchlist = list(all_usernames - include_watchlist)

    success = settings_service.save_plex_settings({
        "plex_url": plex_url,
        "plex_token": plex_token,
        "valid_sections": valid_sections,
        "days_to_monitor": days_to_monitor,
        "number_episodes": number_episodes,
        "users_toggle": users_toggle,
        "skip_ondeck": skip_ondeck,
        "skip_watchlist": skip_watchlist
    })

    if success:
        return templates.TemplateResponse(
            "partials/alert.html",
            {
                "request": request,
                "type": "success",
                "message": "Plex settings saved successfully"
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


@router.get("/paths", response_class=HTMLResponse)
async def settings_paths(request: Request):
    """Path mappings tab"""
    import os
    settings_service = get_settings_service()
    mappings = settings_service.get_path_mappings()
    is_docker = os.path.exists("/.dockerenv") or os.path.exists("/run/.containerenv")

    return templates.TemplateResponse(
        "settings/paths.html",
        {
            "request": request,
            "page_title": "Path Settings",
            "active_tab": "paths",
            "mappings": mappings,
            "is_docker": is_docker
        }
    )


@router.post("/paths", response_class=HTMLResponse)
async def add_path_mapping(
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
    import os
    settings_service = get_settings_service()
    is_docker = os.path.exists("/.dockerenv") or os.path.exists("/run/.containerenv")

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
                "is_docker": is_docker
            }
        )
    else:
        return HTMLResponse("<div class='alert alert-error'>Failed to add mapping</div>")


@router.put("/paths/{index}", response_class=HTMLResponse)
async def update_path_mapping(
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
    import os
    settings_service = get_settings_service()
    is_docker = os.path.exists("/.dockerenv") or os.path.exists("/run/.containerenv")

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
                "is_docker": is_docker
            }
        )
    else:
        return HTMLResponse("<div class='alert alert-error'>Failed to update mapping</div>")


@router.delete("/paths/{index}", response_class=HTMLResponse)
async def delete_path_mapping(request: Request, index: int):
    """Delete a path mapping and return the updated list"""
    import os
    settings_service = get_settings_service()
    is_docker = os.path.exists("/.dockerenv") or os.path.exists("/run/.containerenv")

    success = settings_service.delete_path_mapping(index)

    if success:
        # Return the full updated list with fresh indices
        mappings = settings_service.get_path_mappings()
        return templates.TemplateResponse(
            "settings/partials/path_mappings_list.html",
            {"request": request, "mappings": mappings, "is_docker": is_docker}
        )
    else:
        return HTMLResponse("<div class='alert alert-error'>Failed to delete mapping</div>")


@router.get("/cache", response_class=HTMLResponse)
async def settings_cache(request: Request):
    """Cache settings tab"""
    settings_service = get_settings_service()
    settings = settings_service.get_cache_settings()

    return templates.TemplateResponse(
        "settings/cache.html",
        {
            "request": request,
            "page_title": "Cache Settings",
            "active_tab": "cache",
            "settings": settings
        }
    )


@router.put("/cache", response_class=HTMLResponse)
async def save_cache_settings(request: Request):
    """Save cache settings"""
    settings_service = get_settings_service()

    # Parse form data
    form = await request.form()
    settings_dict = dict(form)

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
async def settings_notifications(request: Request):
    """Notification settings tab"""
    settings_service = get_settings_service()
    settings = settings_service.get_notification_settings()

    return templates.TemplateResponse(
        "settings/notifications.html",
        {
            "request": request,
            "page_title": "Notification Settings",
            "active_tab": "notifications",
            "settings": settings
        }
    )


@router.put("/notifications", response_class=HTMLResponse)
async def save_notification_settings(
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
async def test_webhook(request: Request, webhook_url: str = Form(...)):
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
                "title": "PlexCache-R Test Notification",
                "description": "This is a test message from PlexCache-R. Your webhook is configured correctly!",
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
                    "text": {"type": "plain_text", "text": "PlexCache-R Test Notification"}
                },
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": "This is a test message from PlexCache-R. Your webhook is configured correctly!"}
                },
                {
                    "type": "context",
                    "elements": [{"type": "mrkdwn", "text": f"Sent at {timestamp}"}]
                }
            ]
        }
    else:
        payload = {
            "text": f"PlexCache-R Test Notification\n\nThis is a test message from PlexCache-R. Your webhook is configured correctly!\n\nSent at {timestamp}"
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
async def settings_logging(request: Request):
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
async def save_logging_settings(
    request: Request,
    max_log_files: int = Form(24),
    keep_error_logs_days: int = Form(7)
):
    """Save logging settings"""
    settings_service = get_settings_service()

    success = settings_service.save_logging_settings({
        "max_log_files": max_log_files,
        "keep_error_logs_days": keep_error_logs_days
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


@router.get("/schedule", response_class=HTMLResponse)
async def settings_schedule(request: Request):
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
async def settings_import(request: Request):
    """Import data tab - import CLI data to Docker"""
    # Check if any previous imports have been completed
    import_completed_dir = CONFIG_DIR / "import" / "completed"
    has_completed_import = import_completed_dir.exists() and any(import_completed_dir.iterdir()) if import_completed_dir.exists() else False

    return templates.TemplateResponse(
        "settings/import.html",
        {
            "request": request,
            "page_title": "Import Settings",
            "active_tab": "import",
            "has_completed_import": has_completed_import
        }
    )


# =============================================================================
# OAuth endpoints for Plex authentication (server-side flow)
# =============================================================================

def _get_or_create_client_id() -> str:
    """Get existing client ID from settings or create a new one"""
    settings_service = get_settings_service()
    settings = settings_service.get_all()

    if settings.get("plexcache_client_id"):
        return settings["plexcache_client_id"]

    # Generate and save new client ID
    client_id = str(uuid.uuid4())
    settings_service.save_general_settings({"plexcache_client_id": client_id})
    return client_id


@router.post("/plex/oauth/start")
async def oauth_start():
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
async def oauth_poll(client_id: str = Query(...)):
    """Poll for OAuth completion"""
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
            del _oauth_state[client_id]
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
