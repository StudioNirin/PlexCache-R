"""Cache management routes"""

import logging

from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse

from web.config import templates
from web.services import get_cache_service, get_settings_service
from web.services.cache_service import cached_files_to_dicts, calculate_file_totals

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
def cache_list(
    request: Request,
    source: str = Query("all", description="Filter by source"),
    search: str = Query("", description="Search filter"),
    sort: str = Query(None, description="Sort column"),
    dir: str = Query("desc", description="Sort direction")
):
    """List cached files"""
    # Get eviction mode setting
    settings_service = get_settings_service()
    settings = settings_service.get_all()
    eviction_enabled = settings.get("cache_eviction_mode", "none") != "none"

    # Default sort: priority if eviction enabled, otherwise filename
    if sort is None:
        sort = "priority" if eviction_enabled else "filename"

    cache_service = get_cache_service()
    files = cache_service.get_all_cached_files(
        source_filter=source, search=search, sort_by=sort, sort_dir=dir
    )

    files_data = cached_files_to_dicts(files)
    totals = calculate_file_totals(files_data)

    return templates.TemplateResponse(
        request,
        "cache/list.html",
        {
            "page_title": "Cached Files",
            "files": files_data,
            "source_filter": source,
            "search": search,
            "sort_by": sort,
            "sort_dir": dir,
            "totals": totals,
            "eviction_enabled": eviction_enabled,
            "user_types": cache_service.get_user_types(settings),
        }
    )


@router.get("/drive", response_class=HTMLResponse)
def cache_drive(request: Request, expiring_within: int = 7):
    """Cache drive details page

    Args:
        expiring_within: Show files expiring within N days (3, 7, 14, 30)
    """
    # Validate expiring_within to allowed values
    if expiring_within not in [3, 7, 14, 30]:
        expiring_within = 7
    cache_service = get_cache_service()
    drive_details = cache_service.get_drive_details(expiring_within_days=expiring_within)

    return templates.TemplateResponse(
        request,
        "cache/drive.html",
        {
            "page_title": "Storage",
            "data": drive_details,
            "user_types": cache_service.get_user_types(),
        }
    )


@router.get("/priorities", response_class=HTMLResponse)
def cache_priorities(
    request: Request,
    sort: str = Query("priority", description="Sort column"),
    dir: str = Query("desc", description="Sort direction")
):
    """Priority report page with detailed analysis (lazy loaded)"""
    cache_service = get_cache_service()
    return templates.TemplateResponse(
        request,
        "cache/priorities.html",
        {
            "page_title": "Priority Report",
            "sort_by": sort,
            "sort_dir": dir,
            "user_types": cache_service.get_user_types(),
        }
    )
