"""Dashboard routes"""

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from web.config import TEMPLATES_DIR
from web.services import get_cache_service, get_settings_service, get_operation_runner, get_scheduler_service

router = APIRouter()
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Main dashboard page"""
    cache_service = get_cache_service()
    settings_service = get_settings_service()
    operation_runner = get_operation_runner()

    # Get real cache stats
    cache_stats = cache_service.get_cache_stats()

    # Check Plex connection and get last run time
    plex_connected = settings_service.check_plex_connection()
    last_run = settings_service.get_last_run_time() or "Never"

    # Get operation status
    op_status = operation_runner.get_status_dict()

    # Get schedule status for next run display
    scheduler_service = get_scheduler_service()
    schedule_status = scheduler_service.get_status()

    stats = {
        "cache_files": cache_stats["cache_files"],
        "cache_size": cache_stats["cache_size"],
        "cache_limit": cache_stats["cache_limit"],
        "usage_percent": cache_stats["usage_percent"],
        "ondeck_count": cache_stats["ondeck_count"],
        "watchlist_count": cache_stats["watchlist_count"],
        "last_run": last_run,
        "is_running": operation_runner.is_running,
        "plex_connected": plex_connected,
        "schedule_enabled": schedule_status.get("enabled", False),
        "next_run": schedule_status.get("next_run_display", "Not scheduled"),
        "next_run_relative": schedule_status.get("next_run_relative")
    }

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "page_title": "Dashboard",
            "stats": stats,
            "op_status": op_status
        }
    )
