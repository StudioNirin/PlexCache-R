"""Cache management routes"""

from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from web.config import TEMPLATES_DIR
from web.services import get_cache_service

router = APIRouter()
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


@router.get("/", response_class=HTMLResponse)
async def cache_list(
    request: Request,
    source: str = Query("all", description="Filter by source"),
    search: str = Query("", description="Search filter"),
    sort: str = Query("priority", description="Sort column"),
    dir: str = Query("desc", description="Sort direction")
):
    """List cached files"""
    cache_service = get_cache_service()
    files = cache_service.get_all_cached_files(
        source_filter=source, search=search, sort_by=sort, sort_dir=dir
    )

    # Convert dataclass to dict for template
    files_data = [
        {
            "path": f.path,
            "filename": f.filename,
            "size": f.size,
            "size_display": f.size_display,
            "cache_age_hours": f.cache_age_hours,
            "source": f.source,
            "priority_score": f.priority_score,
            "users": f.users,
            "is_ondeck": f.is_ondeck,
            "is_watchlist": f.is_watchlist
        }
        for f in files
    ]

    # Calculate totals for the current filtered view
    totals = {
        "total_files": len(files_data),
        "ondeck_count": sum(1 for f in files_data if f["is_ondeck"]),
        "watchlist_count": sum(1 for f in files_data if f["is_watchlist"]),
        "other_count": sum(1 for f in files_data if not f["is_ondeck"] and not f["is_watchlist"]),
        "total_size": sum(f["size"] for f in files_data)
    }
    # Format total size
    if totals["total_size"] >= 1024 ** 3:
        totals["total_size_display"] = f"{totals['total_size'] / (1024 ** 3):.2f} GB"
    elif totals["total_size"] >= 1024 ** 2:
        totals["total_size_display"] = f"{totals['total_size'] / (1024 ** 2):.1f} MB"
    else:
        totals["total_size_display"] = f"{totals['total_size'] / 1024:.0f} KB"

    return templates.TemplateResponse(
        "cache/list.html",
        {
            "request": request,
            "page_title": "Cached Files",
            "files": files_data,
            "source_filter": source,
            "search": search,
            "sort_by": sort,
            "sort_dir": dir,
            "totals": totals
        }
    )


@router.get("/drive", response_class=HTMLResponse)
async def cache_drive(request: Request):
    """Cache drive details page"""
    cache_service = get_cache_service()
    drive_details = cache_service.get_drive_details()

    return templates.TemplateResponse(
        "cache/drive.html",
        {
            "request": request,
            "page_title": "Storage",
            "data": drive_details
        }
    )


@router.get("/priorities", response_class=HTMLResponse)
async def cache_priorities(request: Request):
    """Priority report page"""
    cache_service = get_cache_service()
    report = cache_service.get_priority_report()

    return templates.TemplateResponse(
        "cache/priorities.html",
        {
            "request": request,
            "page_title": "Priority Report",
            "report": report
        }
    )
