"""Dashboard routes"""

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from web.config import templates
from web.services import get_operation_runner
from web.services.operation_runner import _get_activity_retention_hours
from web.services.web_cache import get_web_cache_service, CACHE_KEY_DASHBOARD_STATS, CACHE_KEY_MAINTENANCE_HEALTH

router = APIRouter()


def _retention_label(hours: int) -> str:
    """Convert retention hours to a human-readable label."""
    if hours % 24 == 0 and hours >= 24:
        days = hours // 24
        return f"{days} day{'s' if days != 1 else ''}"
    return f"{hours} hour{'s' if hours != 1 else ''}"


@router.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    """Main dashboard page - loads instantly with skeleton, data fetched via HTMX"""
    operation_runner = get_operation_runner()

    # Only get operation status - stats will be lazy loaded
    stats = {"is_running": operation_runner.is_running}
    op_status = operation_runner.get_status_dict()

    retention_hours = _get_activity_retention_hours()
    retention_label = _retention_label(retention_hours)

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "page_title": "Dashboard",
            "stats": stats,
            "op_status": op_status,
            "retention_label": retention_label
        }
    )


@router.post("/refresh-stats", response_class=HTMLResponse)
def refresh_stats(request: Request):
    """Force refresh dashboard stats and return updated container"""
    # Import here to avoid circular dependency
    from web.routers.api import _get_dashboard_stats_data

    # Invalidate cache first
    web_cache = get_web_cache_service()
    web_cache.invalidate(CACHE_KEY_DASHBOARD_STATS)
    web_cache.invalidate(CACHE_KEY_MAINTENANCE_HEALTH)

    # Get fresh stats
    stats, cache_age = _get_dashboard_stats_data(use_cache=False)

    return templates.TemplateResponse(
        "partials/dashboard_stats_container.html",
        {
            "request": request,
            "stats": stats,
            "cache_age": cache_age
        }
    )
