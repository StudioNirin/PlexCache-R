"""Settings routes"""

from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from web.config import TEMPLATES_DIR
from web.services import get_settings_service, get_scheduler_service

router = APIRouter()
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


@router.get("/", response_class=HTMLResponse)
async def settings_index(request: Request):
    """Settings overview - redirects to plex tab"""
    settings_service = get_settings_service()
    settings = settings_service.get_plex_settings()
    libraries = settings_service.get_plex_libraries()
    users = settings_service.get_plex_users()

    return templates.TemplateResponse(
        "settings/plex.html",
        {
            "request": request,
            "page_title": "Settings",
            "active_tab": "plex",
            "settings": settings,
            "libraries": libraries,
            "users": users
        }
    )


@router.get("/plex", response_class=HTMLResponse)
async def settings_plex(request: Request):
    """Plex settings tab"""
    settings_service = get_settings_service()
    settings = settings_service.get_plex_settings()
    libraries = settings_service.get_plex_libraries()
    users = settings_service.get_plex_users()

    return templates.TemplateResponse(
        "settings/plex.html",
        {
            "request": request,
            "page_title": "Plex Settings",
            "active_tab": "plex",
            "settings": settings,
            "libraries": libraries,
            "users": users
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

    return templates.TemplateResponse(
        "settings/partials/user_list.html",
        {
            "request": request,
            "users": users,
            "settings": settings
        }
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
    settings_service = get_settings_service()
    mappings = settings_service.get_path_mappings()

    return templates.TemplateResponse(
        "settings/paths.html",
        {
            "request": request,
            "page_title": "Path Mappings",
            "active_tab": "paths",
            "mappings": mappings
        }
    )


@router.post("/paths", response_class=HTMLResponse)
async def add_path_mapping(
    request: Request,
    name: str = Form(...),
    plex_path: str = Form(...),
    real_path: str = Form(...),
    cache_path: str = Form(""),
    cacheable: str = Form(None),
    enabled: str = Form(None)
):
    """Add a new path mapping"""
    settings_service = get_settings_service()

    mapping = {
        "name": name,
        "plex_path": plex_path,
        "real_path": real_path,
        "cache_path": cache_path if cache_path else None,
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
                "index": index
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
    cacheable: str = Form(None),
    enabled: str = Form(None)
):
    """Update an existing path mapping"""
    settings_service = get_settings_service()

    mapping = {
        "name": name,
        "plex_path": plex_path,
        "real_path": real_path,
        "cache_path": cache_path if cache_path else None,
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
                "index": index
            }
        )
    else:
        return HTMLResponse("<div class='alert alert-error'>Failed to update mapping</div>")


@router.delete("/paths/{index}", response_class=HTMLResponse)
async def delete_path_mapping(request: Request, index: int):
    """Delete a path mapping"""
    settings_service = get_settings_service()

    success = settings_service.delete_path_mapping(index)

    if success:
        # Return empty string to remove the element
        return HTMLResponse("")
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
    unraid_level: str = Form("summary"),
    webhook_url: str = Form(""),
    webhook_level: str = Form("summary")
):
    """Save notification settings"""
    settings_service = get_settings_service()

    success = settings_service.save_notification_settings({
        "notification_type": notification_type,
        "unraid_level": unraid_level,
        "webhook_url": webhook_url,
        "webhook_level": webhook_level
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
