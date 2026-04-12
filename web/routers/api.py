"""API routes for HTMX partial updates"""

import html
import logging
import os
from datetime import datetime
from pathlib import Path
from fastapi import APIRouter, Depends, Request, Form, Query
from fastapi.responses import HTMLResponse, JSONResponse
from starlette.datastructures import ImmutableMultiDict
from typing import List
from urllib.parse import unquote

from web.config import templates, PLEXCACHE_PRODUCT_VERSION, IS_DOCKER
from web.dependencies import parse_form
from core.system_utils import format_bytes, format_duration, format_cache_age
from web.services import get_cache_service, get_settings_service, get_operation_runner, get_scheduler_service, ScheduleConfig, get_maintenance_service
from core.activity import load_last_run_summary
from web.services.operation_runner import OperationRunner
from web.services.web_cache import get_web_cache_service, CACHE_KEY_DASHBOARD_STATS, CACHE_KEY_MAINTENANCE_HEALTH

logger = logging.getLogger(__name__)

router = APIRouter()


def _render_alert(request: Request, type: str, message: str) -> str:
    """Render alert partial to string for HTML concatenation."""
    return templates.TemplateResponse(
        request,
        "partials/alert.html", {"type": type, "message": message}
    ).body.decode()


def _safe_int(value, default: int) -> int:
    """Parse integer from form/query value with fallback to default."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


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
            cache_age = format_cache_age(updated_at)

            # Update dynamic fields that shouldn't be cached
            cached_stats["is_running"] = operation_runner.is_running
            # Stale cache from before duplicate support — recompute fresh
            if "health_duplicate_count" not in cached_stats:
                web_cache.invalidate(CACHE_KEY_DASHBOARD_STATS)
            else:
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
        "associated_files_count": cache_stats.get("associated_files_count", 0),
        "ondeck_count": cache_stats["ondeck_count"],
        "ondeck_tracked_count": cache_stats.get("ondeck_tracked_count", 0),
        "watchlist_count": cache_stats["watchlist_count"],
        "watchlist_tracked_count": cache_stats.get("watchlist_tracked_count", 0),
        "eviction_over_threshold": cache_stats.get("eviction_over_threshold", False),
        "eviction_over_by_display": cache_stats.get("eviction_over_by_display"),
        "cache_limit_exceeded": cache_stats.get("cache_limit_exceeded", False),
        "cache_limit_approaching": cache_stats.get("cache_limit_approaching", False),
        "configured_limit_display": cache_stats.get("configured_limit_display"),
        "configured_limit_percent": cache_stats.get("configured_limit_percent", 0),
        "eviction_threshold_display": cache_stats.get("eviction_threshold_display"),
        "min_free_space_warning": cache_stats.get("min_free_space_warning", False),
        "last_run": last_run,
        "is_running": operation_runner.is_running,
        "plex_connected": plex_connected,
        "schedule_enabled": schedule_status.get("enabled", False),
        "next_run": schedule_status.get("next_run_display", "Not scheduled"),
        "next_run_relative": schedule_status.get("next_run_relative"),
        "health_status": health["status"],
        "health_issues": health["orphaned_count"],
        "health_warnings": health["stale_exclude_count"] + health["stale_timestamp_count"] + health["duplicate_orphan_count"],
        "health_orphaned_count": health["orphaned_count"],
        "health_stale_exclude_count": health["stale_exclude_count"],
        "health_stale_timestamp_count": health["stale_timestamp_count"],
        "health_duplicate_count": health["duplicate_count"],
        "health_duplicate_orphan_count": health["duplicate_orphan_count"],
        "health_duplicate_orphan_bytes": health["duplicate_orphan_bytes_display"],
        "last_run_summary": None,
    }

    # Load last run summary
    summary = load_last_run_summary()
    if summary:
        stats["last_run_summary"] = {
            "status": summary.get("status", "unknown"),
            "bytes_cached_display": format_bytes(summary["bytes_cached"]) if summary.get("bytes_cached") else "",
            "bytes_restored_display": format_bytes(summary["bytes_restored"]) if summary.get("bytes_restored") else "",
            "duration_display": format_duration(summary.get("duration_seconds", 0)),
            "error_count": summary.get("error_count", 0),
            "dry_run": summary.get("dry_run", False),
        }

    # Cache the results
    web_cache.set(CACHE_KEY_DASHBOARD_STATS, stats)

    return stats, "just now"


@router.get("/dashboard/stats-content", response_class=HTMLResponse)
def dashboard_stats_content(request: Request):
    """Full dashboard stats container for lazy loading"""
    stats, cache_age = _get_dashboard_stats_data(use_cache=True)

    return templates.TemplateResponse(
        request,
        "partials/dashboard_stats_container.html",
        {
            "stats": stats,
            "cache_age": cache_age
        }
    )


@router.get("/dashboard/stats", response_class=HTMLResponse)
def dashboard_stats(request: Request):
    """Dashboard stats partial for HTMX polling"""
    stats, _ = _get_dashboard_stats_data(use_cache=True)

    return templates.TemplateResponse(
        request,
        "partials/dashboard_stats.html",
        {
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
            "subtitle_count": f.subtitle_count,
            "sidecar_count": f.sidecar_count,
            "associated_files": f.associated_files
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
        totals["total_size_display"] = f"{totals['total_size'] / (1024 ** 2):.2f} MB"
    else:
        totals["total_size_display"] = f"{totals['total_size'] / 1024:.2f} KB"

    # Get eviction mode setting
    settings_service = get_settings_service()
    settings = settings_service.get_all()
    eviction_enabled = settings.get("cache_eviction_mode", "none") != "none"

    return templates.TemplateResponse(
        request,
        "cache/partials/file_table.html",
        {
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
        return templates.TemplateResponse(request, "partials/alert.html", {
            "type": "error", "message": "Invalid file path"
        })

    result = cache_service.evict_file(decoded_path)

    if result.get("success"):
        message = result.get("message", "File evicted")
        alert = _render_alert(request, "success", message)
        return HTMLResponse(alert + "<script>htmx.trigger('#cache-table-body', 'refresh');</script>")
    else:
        message = result.get("message", "Eviction failed")
        return templates.TemplateResponse(request, "partials/alert.html", {
            "type": "error", "message": message
        })


@router.post("/cache/evict-bulk", response_class=HTMLResponse)
def evict_bulk(request: Request, form_data: ImmutableMultiDict = Depends(parse_form)):
    """Evict multiple files from cache"""
    cache_service = get_cache_service()

    paths = form_data.getlist("paths")

    if not paths:
        return templates.TemplateResponse(request, "partials/alert.html", {
            "type": "warning", "message": "No files selected"
        })

    # URL decode paths
    decoded_paths = [unquote(p) for p in paths]

    result = cache_service.evict_files(decoded_paths)

    if result["success"]:
        msg = f"Evicted {result['evicted_count']} of {result['total_count']} files"
        if result["errors"]:
            msg += f" ({len(result['errors'])} errors)"

        alert = _render_alert(request, "success", msg)
        return HTMLResponse(alert + """<script>
            htmx.trigger('#cache-table-body', 'refresh');
            document.querySelectorAll('.file-checkbox').forEach(cb => cb.checked = false);
            document.getElementById('select-all')?.checked && (document.getElementById('select-all').checked = false);
            updateBulkActions();
        </script>""")
    else:
        errors_str = "; ".join(result["errors"][:3])
        return templates.TemplateResponse(request, "partials/alert.html", {
            "type": "error",
            "message": f"Failed to evict files: {errors_str}"
        })


@router.post("/settings/schedule", response_class=HTMLResponse)
def save_schedule_settings(request: Request, form_data: ImmutableMultiDict = Depends(parse_form)):
    """Save schedule settings"""
    scheduler_service = get_scheduler_service()

    config = ScheduleConfig(
        enabled=form_data.get("enabled") == "on",
        schedule_type=form_data.get("schedule_type", "interval"),
        interval_hours=_safe_int(form_data.get("interval_hours"), 4),
        interval_start_time=form_data.get("interval_start_time", "00:00"),
        cron_expression=form_data.get("cron_expression", "0 */4 * * *"),
        dry_run=form_data.get("dry_run") == "on",
        verbose=form_data.get("verbose") == "on",
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
                htmx.ajax('GET', '/api/operation-banner', {{target: '#global-operation-banner', swap: 'innerHTML'}});
            </script>
        ''')
    else:
        return templates.TemplateResponse(
            request,
            "partials/alert.html",
            {
                "type": "error",
                "message": "Failed to save schedule settings"
            }
        )


@router.get("/settings/schedule/status")
def get_schedule_status():
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
        request,
        "cache/partials/storage_stats.html",
        {
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
        request,
        "cache/partials/priorities_content.html",
        {
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
        request,
        "cache/partials/eviction_simulation.html",
        {
            "threshold": threshold,
            "result": result
        }
    )


@router.get("/settings/schedule/validate-cron")
def validate_cron_expression(expression: str):
    """Validate a cron expression (JSON)"""
    scheduler_service = get_scheduler_service()
    return scheduler_service.validate_cron(expression)


# =============================================================================
# Docker API Endpoints
# =============================================================================

@router.get("/config-health", response_class=HTMLResponse)
def config_health(request: Request):
    """Render a dashboard banner for any detected path_mapping health issues.

    Returns the rendered alert partial(s) when problems are found, or an empty
    body when the config is clean. Triggered by HTMX from the dashboard index
    template. Non-fatal — any exception collapses to an empty body.
    """
    try:
        settings_service = get_settings_service()
        issues = settings_service.detect_path_mapping_health_issues()
    except Exception:
        return HTMLResponse("")

    if not issues:
        return HTMLResponse("")

    html_parts: List[str] = []
    for issue in issues:
        html_parts.append(_render_alert(request, "warning", issue["message"]))
    return HTMLResponse("".join(html_parts))


@router.get("/health")
def health_check():
    """
    Health check endpoint for Docker container monitoring.

    Returns minimal status for container orchestration (Docker, Kubernetes, etc.).
    Used by Docker HEALTHCHECK — kept intentionally lean to avoid information disclosure.
    """
    return {"status": "healthy"}


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
def trigger_run(dry_run: bool = False, verbose: bool = False):
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
def get_operation_indicator(request: Request):
    """Return global operation indicator HTML - used for header status across all pages"""
    operation_runner = get_operation_runner()
    is_running = operation_runner.is_running

    if is_running:
        return templates.TemplateResponse(
            request,
            "components/global_operation_indicator.html",
            {"is_running": True}
        )
    else:
        # Return empty div that continues polling less frequently
        return templates.TemplateResponse(
            request,
            "components/global_operation_indicator.html",
            {"is_running": False}
        )


@router.get("/operation-banner", response_class=HTMLResponse)
def get_operation_banner(request: Request):
    """Return global operation status banner HTML - shown on all pages when operation is running"""
    from web.services.maintenance_runner import get_maintenance_runner

    operation_runner = get_operation_runner()
    status = operation_runner.get_status_dict()
    maint_status = get_maintenance_runner().get_status_dict()

    context = {"status": status, "maint_status": maint_status}

    # When both runners are idle, include scheduler countdown info
    if not operation_runner.is_running and not get_maintenance_runner().is_running:
        scheduler_service = get_scheduler_service()
        sched_status = scheduler_service.get_status()
        if sched_status.get("enabled"):
            context["scheduler_status"] = {
                "next_run_relative": sched_status.get("next_run_relative") or "momentarily",
                "next_run_display": sched_status.get("next_run_display", ""),
            }

    return templates.TemplateResponse(
        request,
        "components/global_operation_banner.html",
        context
    )


@router.post("/dismiss-operation")
def dismiss_operation():
    """Dismiss a completed/failed operation banner, resetting state to idle."""
    runner = get_operation_runner()
    runner.dismiss()
    runner.dismiss_external()
    return JSONResponse({"ok": True})


@router.post("/check-upgrades")
def check_upgrades():
    """Check for and resolve media file upgrades (Sonarr/Radarr swaps).

    Examines stale exclude entries to see if they represent upgraded files.
    For each stale entry with a rating_key in the OnDeck tracker, queries Plex
    to detect file path changes and transfers tracking data accordingly.
    """
    cache_service = get_cache_service()
    maintenance_service = get_maintenance_service()

    # Get current stale entries to scope the check
    exclude_files = maintenance_service.get_exclude_files()
    cache_files = maintenance_service.get_cache_files()
    stale = sorted(list(exclude_files - cache_files))

    if not stale:
        return {"upgrades_found": 0, "upgrades_resolved": 0, "details": []}

    return cache_service.check_for_upgrades(stale)


# =============================================================================
# Filesystem Browse Endpoints
# =============================================================================

@router.get("/browse")
def browse_directory(path: str = Query("")):
    """Directory listing for path autocomplete.

    Security:
    - Rejects null bytes, control characters, paths > 4096 chars
    - Pre-resolve jail: must start with an allowed prefix
    - Post-resolve jail: resolved path must still start with an allowed prefix
    - Only returns directories (not files), skips dotfiles
    - Capped at 100 entries
    """
    # Allowed path prefixes (media paths + common Docker mount points)
    ALLOWED_PREFIXES = ("/mnt/", "/config/", "/plex/", "/data/")

    # Input validation
    if not path:
        return JSONResponse({"error": "path is required"}, status_code=400)
    if len(path) > 4096:
        return JSONResponse({"error": "path too long"}, status_code=400)
    if "\x00" in path or any(ord(c) < 32 for c in path):
        return JSONResponse({"error": "invalid characters in path"}, status_code=400)

    # Pre-resolve jail check
    if not any(path.startswith(p) for p in ALLOWED_PREFIXES):
        return JSONResponse({"error": "path must be under an allowed directory"}, status_code=403)

    try:
        resolved = Path(path).resolve()
    except (OSError, ValueError):
        return JSONResponse({"error": "invalid path"}, status_code=400)

    # Post-resolve jail check (catches ../ traversal and symlink escapes)
    resolved_str = str(resolved)
    if not any(resolved_str == p.rstrip('/') or resolved_str.startswith(p) for p in ALLOWED_PREFIXES):
        return JSONResponse({"error": "path must be under an allowed directory"}, status_code=403)

    if not resolved.is_dir():
        return JSONResponse({"error": "not a directory"}, status_code=404)

    # List directories only, skip dotfiles, cap at 100
    directories = []
    try:
        with os.scandir(str(resolved)) as it:
            for entry in it:
                if entry.name.startswith("."):
                    continue
                try:
                    if entry.is_dir(follow_symlinks=False):
                        directories.append(entry.name)
                except (PermissionError, OSError):
                    continue
    except PermissionError:
        return JSONResponse({"error": "permission denied"}, status_code=403)

    directories.sort()
    directories = directories[:100]

    return {"path": str(resolved), "directories": directories}


@router.get("/validate-path", response_class=HTMLResponse)
def validate_path(path: str = Query("")):
    """Validate a filesystem path — returns an HTMX icon partial.

    Returns green check if path exists and is a directory,
    warning icon if not found, or empty if path is invalid.
    """
    if not path:
        return HTMLResponse("")
    if not path.startswith("/mnt/"):
        return HTMLResponse(
            '<i data-lucide="alert-triangle" style="width: 14px; height: 14px; color: var(--plex-warning, #e67e22); vertical-align: middle;" '
            'title="Path does not start with /mnt/ — may need correction"></i>'
            '<script>lucide.createIcons();</script>'
        )

    try:
        p = Path(path)
        # Post-resolve jail check (catches ../ traversal and symlink escapes)
        resolved_str = str(p.resolve())
        if resolved_str != "/mnt" and not resolved_str.startswith("/mnt/"):
            return HTMLResponse("")
        if p.exists() and p.is_dir():
            # Docker: verify path is backed by a real bind mount (issue #139)
            if IS_DOCKER:
                from web.dependencies import get_system_detector
                detector = get_system_detector()
                is_mounted, _ = detector.is_path_bind_mounted(path)
                if not is_mounted:
                    return HTMLResponse(
                        '<i data-lucide="alert-triangle" style="width: 14px; height: 14px; color: var(--plex-error, #e74c3c); vertical-align: middle;" '
                        'title="Path exists but is not backed by a bind mount — writes will go to docker.img"></i>'
                        '<script>lucide.createIcons();</script>'
                    )
            return HTMLResponse(
                '<i data-lucide="check-circle" style="width: 14px; height: 14px; color: var(--plex-success); vertical-align: middle;"></i>'
                '<script>lucide.createIcons();</script>'
            )
        else:
            return HTMLResponse(
                '<i data-lucide="alert-triangle" style="width: 14px; height: 14px; color: var(--plex-warning, #e67e22); vertical-align: middle;" title="Path not found"></i>'
                '<script>lucide.createIcons();</script>'
            )
    except (OSError, ValueError):
        return HTMLResponse("")
