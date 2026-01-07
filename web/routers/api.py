"""API routes for HTMX partial updates"""

from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from typing import List
from urllib.parse import unquote

from web.config import TEMPLATES_DIR
from web.services import get_cache_service, get_settings_service, get_operation_runner, get_scheduler_service, ScheduleConfig

router = APIRouter()
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


@router.get("/dashboard/stats", response_class=HTMLResponse)
async def dashboard_stats(request: Request):
    """Dashboard stats partial for HTMX polling"""
    cache_service = get_cache_service()
    settings_service = get_settings_service()
    operation_runner = get_operation_runner()
    scheduler_service = get_scheduler_service()

    # Get real cache stats
    cache_stats = cache_service.get_cache_stats()

    # Check Plex connection and get last run time
    plex_connected = settings_service.check_plex_connection()
    last_run = settings_service.get_last_run_time() or "Never"

    # Get schedule status for next run display
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
        "partials/dashboard_stats.html",
        {
            "request": request,
            "stats": stats
        }
    )


@router.get("/cache/files", response_class=HTMLResponse)
async def cache_files_table(
    request: Request,
    source: str = "all",
    search: str = "",
    sort: str = "priority",
    dir: str = "desc"
):
    """Cache files table partial for HTMX"""
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
        "cache/partials/file_table.html",
        {
            "request": request,
            "files": files_data,
            "source_filter": source,
            "search": search,
            "sort_by": sort,
            "sort_dir": dir,
            "totals": totals
        }
    )


@router.post("/cache/evict/{file_path:path}", response_class=HTMLResponse)
async def evict_file(request: Request, file_path: str):
    """Evict a single file from cache"""
    cache_service = get_cache_service()

    # URL decode the path
    decoded_path = unquote(file_path)

    result = cache_service.evict_file(decoded_path)

    # Return an alert message
    if result["success"]:
        return f'''<div class="alert alert-success" id="evict-alert">
            <i data-lucide="check-circle"></i>
            <span>{result["message"]}</span>
        </div>
        <script>
            setTimeout(() => document.getElementById('evict-alert')?.remove(), 3000);
            htmx.trigger('#cache-table-body', 'refresh');
        </script>'''
    else:
        return f'''<div class="alert alert-error" id="evict-alert">
            <i data-lucide="alert-circle"></i>
            <span>{result["message"]}</span>
        </div>
        <script>
            setTimeout(() => document.getElementById('evict-alert')?.remove(), 5000);
        </script>'''


@router.post("/cache/evict-bulk", response_class=HTMLResponse)
async def evict_bulk(request: Request):
    """Evict multiple files from cache"""
    cache_service = get_cache_service()

    # Get form data
    form = await request.form()
    paths = form.getlist("paths")

    if not paths:
        return '''<div class="alert alert-warning" id="evict-alert">
            <i data-lucide="alert-triangle"></i>
            <span>No files selected</span>
        </div>
        <script>
            setTimeout(() => document.getElementById('evict-alert')?.remove(), 3000);
        </script>'''

    # URL decode paths
    decoded_paths = [unquote(p) for p in paths]

    result = cache_service.evict_files(decoded_paths)

    if result["success"]:
        msg = f"Evicted {result['evicted_count']} of {result['total_count']} files"
        if result["errors"]:
            msg += f" ({len(result['errors'])} errors)"

        return f'''<div class="alert alert-success" id="evict-alert">
            <i data-lucide="check-circle"></i>
            <span>{msg}</span>
        </div>
        <script>
            setTimeout(() => document.getElementById('evict-alert')?.remove(), 3000);
            htmx.trigger('#cache-table-body', 'refresh');
            document.querySelectorAll('.file-checkbox').forEach(cb => cb.checked = false);
            document.getElementById('select-all')?.checked && (document.getElementById('select-all').checked = false);
            updateBulkActions();
        </script>'''
    else:
        errors_str = "; ".join(result["errors"][:3])
        return f'''<div class="alert alert-error" id="evict-alert">
            <i data-lucide="alert-circle"></i>
            <span>Failed to evict files: {errors_str}</span>
        </div>
        <script>
            setTimeout(() => document.getElementById('evict-alert')?.remove(), 5000);
        </script>'''


@router.post("/settings/schedule", response_class=HTMLResponse)
async def save_schedule_settings(request: Request):
    """Save schedule settings"""
    scheduler_service = get_scheduler_service()

    # Parse form data
    form = await request.form()

    config = ScheduleConfig(
        enabled=form.get("enabled") == "on",
        schedule_type=form.get("schedule_type", "interval"),
        interval_hours=int(form.get("interval_hours", 4)),
        interval_start_time=form.get("interval_start_time", "00:00"),
        cron_expression=form.get("cron_expression", "0 */4 * * *"),
        dry_run=form.get("dry_run") == "on",
        verbose=form.get("verbose") == "on",
    )

    result = scheduler_service.update_config(config)

    if result["success"]:
        # Return alert with script to refresh status display
        return HTMLResponse(f'''
            <div class="alert alert-success">
                <i data-lucide="check-circle"></i>
                <span>Schedule settings saved successfully</span>
            </div>
            <script>
                lucide.createIcons();
                if (typeof refreshScheduleStatus === 'function') {{
                    refreshScheduleStatus();
                }}
            </script>
        ''')
    else:
        return templates.TemplateResponse(
            "partials/alert.html",
            {
                "request": request,
                "type": "error",
                "message": "Failed to save schedule settings"
            }
        )


@router.get("/settings/schedule/status")
async def get_schedule_status():
    """Get current scheduler status (JSON for polling)"""
    scheduler_service = get_scheduler_service()
    return scheduler_service.get_status()


@router.get("/cache/storage", response_class=HTMLResponse)
async def cache_storage_stats(request: Request):
    """Storage stats partial for HTMX polling"""
    cache_service = get_cache_service()
    drive_details = cache_service.get_drive_details()

    return templates.TemplateResponse(
        "cache/partials/storage_stats.html",
        {
            "request": request,
            "data": drive_details
        }
    )


@router.get("/settings/schedule/validate-cron")
async def validate_cron_expression(expression: str):
    """Validate a cron expression (JSON)"""
    scheduler_service = get_scheduler_service()
    return scheduler_service.validate_cron(expression)
