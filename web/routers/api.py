"""API routes for HTMX partial updates"""

import html
from datetime import datetime
from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse
from typing import List
from urllib.parse import unquote

from web.config import templates
from web.services import get_cache_service, get_settings_service, get_operation_runner, get_scheduler_service, ScheduleConfig, get_maintenance_service
from web.services.web_cache import get_web_cache_service, CACHE_KEY_DASHBOARD_STATS, CACHE_KEY_MAINTENANCE_HEALTH

router = APIRouter()


def _get_dashboard_stats_data(use_cache: bool = True) -> tuple[dict, str | None]:
    """Get dashboard stats, optionally from cache. Returns (stats, cache_age)"""
    web_cache = get_web_cache_service()
    cache_service = get_cache_service()
    settings_service = get_settings_service()
    operation_runner = get_operation_runner()
    scheduler_service = get_scheduler_service()
    maintenance_service = get_maintenance_service()

    # Try to get from cache first
    if use_cache:
        cached_stats = web_cache.get(CACHE_KEY_DASHBOARD_STATS)
        if cached_stats:
            # Calculate cache age
            _, updated_at = web_cache.get_with_age(CACHE_KEY_DASHBOARD_STATS)
            cache_age = None
            if updated_at:
                age_seconds = (datetime.now() - updated_at).total_seconds()
                if age_seconds < 60:
                    cache_age = "just now"
                elif age_seconds < 3600:
                    cache_age = f"{int(age_seconds / 60)} min ago"
                else:
                    cache_age = f"{int(age_seconds / 3600)} hr ago"

            # Update dynamic fields that shouldn't be cached
            cached_stats["is_running"] = operation_runner.is_running
            return cached_stats, cache_age

    # Compute fresh stats
    cache_stats = cache_service.get_cache_stats()
    plex_connected = settings_service.check_plex_connection()
    last_run = settings_service.get_last_run_time() or "Never"
    schedule_status = scheduler_service.get_status()
    health = maintenance_service.get_health_summary()

    stats = {
        "cache_files": cache_stats["cache_files"],
        "cache_size": cache_stats["cache_size"],
        "cache_limit": cache_stats["cache_limit"],
        "usage_percent": cache_stats["usage_percent"],
        "cached_files_size": cache_stats.get("cached_files_size"),
        "ondeck_count": cache_stats["ondeck_count"],
        "watchlist_count": cache_stats["watchlist_count"],
        "eviction_over_threshold": cache_stats.get("eviction_over_threshold", False),
        "eviction_over_by_display": cache_stats.get("eviction_over_by_display"),
        "cache_limit_exceeded": cache_stats.get("cache_limit_exceeded", False),
        "min_free_space_warning": cache_stats.get("min_free_space_warning", False),
        "last_run": last_run,
        "is_running": operation_runner.is_running,
        "plex_connected": plex_connected,
        "schedule_enabled": schedule_status.get("enabled", False),
        "next_run": schedule_status.get("next_run_display", "Not scheduled"),
        "next_run_relative": schedule_status.get("next_run_relative"),
        "health_status": health["status"],
        "health_issues": health["orphaned_count"],
        "health_warnings": health["stale_exclude_count"] + health["stale_timestamp_count"]
    }

    # Cache the results
    web_cache.set(CACHE_KEY_DASHBOARD_STATS, stats)

    return stats, "just now"


@router.get("/dashboard/stats-content", response_class=HTMLResponse)
def dashboard_stats_content(request: Request):
    """Full dashboard stats container for lazy loading"""
    stats, cache_age = _get_dashboard_stats_data(use_cache=True)

    return templates.TemplateResponse(
        "partials/dashboard_stats_container.html",
        {
            "request": request,
            "stats": stats,
            "cache_age": cache_age
        }
    )


@router.get("/dashboard/stats", response_class=HTMLResponse)
def dashboard_stats(request: Request):
    """Dashboard stats partial for HTMX polling"""
    stats, _ = _get_dashboard_stats_data(use_cache=True)

    return templates.TemplateResponse(
        "partials/dashboard_stats.html",
        {
            "request": request,
            "stats": stats
        }
    )


@router.get("/cache/files", response_class=HTMLResponse)
def cache_files_table(
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
            "is_watchlist": f.is_watchlist,
            "subtitle_count": f.subtitle_count
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

    # Get eviction mode setting
    settings_service = get_settings_service()
    settings = settings_service.get_all()
    eviction_enabled = settings.get("cache_eviction_mode", "none") != "none"

    return templates.TemplateResponse(
        "cache/partials/file_table.html",
        {
            "request": request,
            "files": files_data,
            "source_filter": source,
            "search": search,
            "sort_by": sort,
            "sort_dir": dir,
            "totals": totals,
            "eviction_enabled": eviction_enabled
        }
    )


@router.post("/cache/evict/{file_path:path}", response_class=HTMLResponse)
def evict_file(request: Request, file_path: str):
    """Evict a single file from cache"""
    cache_service = get_cache_service()

    # URL decode the path and validate
    decoded_path = unquote(file_path)
    if not decoded_path or not decoded_path.startswith("/"):
        return '''<div class="alert alert-error" id="evict-alert">
            <i data-lucide="alert-circle"></i>
            <span>Invalid file path</span>
        </div>
        <script>
            setTimeout(() => document.getElementById('evict-alert')?.remove(), 5000);
        </script>'''

    result = cache_service.evict_file(decoded_path)

    # Return an alert message
    if result.get("success"):
        message = html.escape(result.get("message", "File evicted"))
        return f'''<div class="alert alert-success" id="evict-alert">
            <i data-lucide="check-circle"></i>
            <span>{message}</span>
        </div>
        <script>
            setTimeout(() => document.getElementById('evict-alert')?.remove(), 3000);
            htmx.trigger('#cache-table-body', 'refresh');
        </script>'''
    else:
        message = html.escape(result.get("message", "Eviction failed"))
        return f'''<div class="alert alert-error" id="evict-alert">
            <i data-lucide="alert-circle"></i>
            <span>{message}</span>
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
        msg = html.escape(f"Evicted {result['evicted_count']} of {result['total_count']} files")
        if result["errors"]:
            msg += html.escape(f" ({len(result['errors'])} errors)")

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
        errors_str = html.escape("; ".join(result["errors"][:3]))
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
def cache_storage_stats(request: Request, expiring_within: int = 7):
    """Storage stats partial for HTMX polling"""
    # Validate expiring_within to allowed values
    if expiring_within not in [3, 7, 14, 30]:
        expiring_within = 7
    cache_service = get_cache_service()
    drive_details = cache_service.get_drive_details(expiring_within_days=expiring_within)

    return templates.TemplateResponse(
        "cache/partials/storage_stats.html",
        {
            "request": request,
            "data": drive_details
        }
    )


@router.get("/cache/priorities-content", response_class=HTMLResponse)
def cache_priorities_content(
    request: Request,
    sort: str = "priority",
    dir: str = "desc"
):
    """Priority report content partial for lazy loading"""
    cache_service = get_cache_service()
    settings_service = get_settings_service()

    # Get structured report data
    report_data = cache_service.get_priority_report_data()

    # Get eviction mode for conditional display
    settings = settings_service.get_all()
    eviction_enabled = settings.get("cache_eviction_mode", "none") != "none"

    # Sort files if needed
    files = report_data["files"]
    reverse = (dir == "desc")

    sort_keys = {
        "filename": lambda f: f["filename"].lower(),
        "size": lambda f: f["size"],
        "priority": lambda f: f["priority_score"],
        "age": lambda f: f["cache_age_hours"],
        "users": lambda f: len(f["users"]),
        "source": lambda f: (f["is_ondeck"], f["is_watchlist"]),
    }

    sort_key = sort_keys.get(sort, sort_keys["priority"])
    files.sort(key=sort_key, reverse=reverse)
    report_data["files"] = files

    return templates.TemplateResponse(
        "cache/partials/priorities_content.html",
        {
            "request": request,
            "data": report_data,
            "eviction_enabled": eviction_enabled,
            "sort_by": sort,
            "sort_dir": dir
        }
    )


@router.get("/cache/simulate-eviction", response_class=HTMLResponse)
def simulate_eviction(request: Request, threshold: int = 95):
    """Simulate eviction at a given threshold percentage"""
    cache_service = get_cache_service()

    # Validate threshold (50-100)
    threshold = max(50, min(100, threshold))

    result = cache_service.simulate_eviction(threshold)

    return templates.TemplateResponse(
        "cache/partials/eviction_simulation.html",
        {
            "request": request,
            "threshold": threshold,
            "result": result
        }
    )


@router.get("/settings/schedule/validate-cron")
async def validate_cron_expression(expression: str):
    """Validate a cron expression (JSON)"""
    scheduler_service = get_scheduler_service()
    return scheduler_service.validate_cron(expression)


# =============================================================================
# Docker API Endpoints
# =============================================================================

@router.get("/health")
async def health_check():
    """
    Health check endpoint for Docker container monitoring.

    Returns basic health status for container orchestration (Docker, Kubernetes, etc.).
    Used by Docker HEALTHCHECK and external monitoring tools.
    """
    settings_service = get_settings_service()
    scheduler_service = get_scheduler_service()
    operation_runner = get_operation_runner()

    # Check Plex connection (cached, won't block)
    plex_connected = settings_service.check_plex_connection()

    # Get scheduler status
    schedule_status = scheduler_service.get_status()

    return {
        "status": "healthy",
        "version": "3.0.0",
        "plex_connected": plex_connected,
        "scheduler_running": schedule_status.get("running", False),
        "operation_running": operation_runner.is_running,
    }


@router.get("/status")
def detailed_status():
    """
    Detailed status endpoint for monitoring and debugging.

    Returns comprehensive status information including:
    - Plex connection status
    - Scheduler configuration and next run
    - Current operation status
    - Cache statistics
    """
    settings_service = get_settings_service()
    scheduler_service = get_scheduler_service()
    operation_runner = get_operation_runner()
    cache_service = get_cache_service()

    # Get various status info
    plex_connected = settings_service.check_plex_connection()
    schedule_status = scheduler_service.get_status()
    operation_status = operation_runner.get_status_dict()
    cache_stats = cache_service.get_cache_stats()

    return {
        "status": "ok",
        "plex": {
            "connected": plex_connected,
        },
        "scheduler": {
            "enabled": schedule_status.get("enabled", False),
            "running": schedule_status.get("running", False),
            "schedule_description": schedule_status.get("schedule_description", ""),
            "next_run": schedule_status.get("next_run"),
            "next_run_display": schedule_status.get("next_run_display"),
            "last_run": schedule_status.get("last_run"),
            "last_run_display": schedule_status.get("last_run_display"),
        },
        "operation": operation_status,
        "cache": {
            "files": cache_stats.get("cache_files", 0),
            "size": cache_stats.get("cache_size", "0 B"),
            "ondeck_count": cache_stats.get("ondeck_count", 0),
            "watchlist_count": cache_stats.get("watchlist_count", 0),
        }
    }


@router.post("/run")
async def trigger_run(dry_run: bool = False, verbose: bool = False):
    """
    Trigger an immediate PlexCache operation.

    This endpoint allows external tools and automation to trigger cache operations.
    The operation runs in the background; poll /api/status to track progress.

    Args:
        dry_run: If true, simulate without moving files
        verbose: If true, enable debug logging for this run

    Returns:
        JSON with success status and message
    """
    operation_runner = get_operation_runner()

    if operation_runner.is_running:
        return {
            "success": False,
            "message": "Operation already in progress",
            "running": True
        }

    # Start the operation
    started = operation_runner.start_operation(dry_run=dry_run, verbose=verbose)

    if started:
        mode = []
        if dry_run:
            mode.append("dry-run")
        if verbose:
            mode.append("verbose")
        mode_str = f" ({', '.join(mode)})" if mode else ""

        return {
            "success": True,
            "message": f"Operation started{mode_str}",
            "running": True
        }
    else:
        return {
            "success": False,
            "message": "Failed to start operation",
            "running": False
        }


@router.get("/operation-indicator", response_class=HTMLResponse)
async def get_operation_indicator(request: Request):
    """Return global operation indicator HTML - used for header status across all pages"""
    operation_runner = get_operation_runner()
    is_running = operation_runner.is_running

    if is_running:
        return templates.TemplateResponse(
            "components/global_operation_indicator.html",
            {"request": request, "is_running": True}
        )
    else:
        # Return empty div that continues polling less frequently
        return templates.TemplateResponse(
            "components/global_operation_indicator.html",
            {"request": request, "is_running": False}
        )


@router.get("/operation-banner", response_class=HTMLResponse)
def get_operation_banner(request: Request):
    """Return global operation status banner HTML - shown on all pages when operation is running"""
    from web.services.maintenance_runner import get_maintenance_runner

    operation_runner = get_operation_runner()
    status = operation_runner.get_status_dict()
    maint_status = get_maintenance_runner().get_status_dict()

    context = {"request": request, "status": status, "maint_status": maint_status}

    # When both runners are idle, include scheduler countdown info
    if not operation_runner.is_running and not get_maintenance_runner().is_running:
        scheduler_service = get_scheduler_service()
        sched_status = scheduler_service.get_status()
        if sched_status.get("enabled") and sched_status.get("next_run_relative"):
            context["scheduler_status"] = {
                "next_run_relative": sched_status["next_run_relative"],
                "next_run_display": sched_status.get("next_run_display", ""),
            }

    return templates.TemplateResponse(
        "components/global_operation_banner.html",
        context
    )


@router.post("/dismiss-operation")
def dismiss_operation():
    """Dismiss a completed/failed operation banner, resetting state to idle."""
    get_operation_runner().dismiss()
    return JSONResponse({"ok": True})
