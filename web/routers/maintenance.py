"""Maintenance routes - cache audit and fix actions"""

import logging
from datetime import datetime
from typing import List, Optional
from fastapi import APIRouter, Request, Form, Query
from fastapi.responses import HTMLResponse, JSONResponse

from web.config import templates, get_time_format
from web.services.maintenance_service import get_maintenance_service
from web.services.maintenance_runner import (
    get_maintenance_runner, ASYNC_ACTIONS, ACTION_HISTORY_LABELS,
    MaintenanceHistoryEntry, get_maintenance_history,
)
from web.services.duplicate_service import get_duplicate_service
from core.system_utils import format_duration, format_cache_age
from web.services.operation_runner import get_operation_runner
from web.services.web_cache import get_web_cache_service, CACHE_KEY_MAINTENANCE_AUDIT, CACHE_KEY_MAINTENANCE_HEALTH, CACHE_KEY_DASHBOARD_STATS

logger = logging.getLogger(__name__)

router = APIRouter()


# In-memory cache for full audit results (not JSON-serializable)
_audit_results_cache = {
    "results": None,
    "updated_at": None
}
_audit_cache_lock = __import__('threading').Lock()
AUDIT_CACHE_TTL_SECONDS = 300  # 5 minutes


def _get_cache_age_display(key: str) -> Optional[str]:
    """Get human-readable cache age for a key"""
    web_cache = get_web_cache_service()
    _, updated_at = web_cache.get_with_age(key)
    return format_cache_age(updated_at)


def _invalidate_caches():
    """Invalidate all related caches after a maintenance action"""
    # Clear in-memory audit cache
    with _audit_cache_lock:
        _audit_results_cache["results"] = None
        _audit_results_cache["updated_at"] = None

    # Clear web caches
    web_cache = get_web_cache_service()
    web_cache.invalidate(CACHE_KEY_MAINTENANCE_AUDIT)
    web_cache.invalidate(CACHE_KEY_MAINTENANCE_HEALTH)
    web_cache.invalidate(CACHE_KEY_DASHBOARD_STATS)


def _get_cached_audit_results(force_refresh: bool = False):
    """Get audit results from cache or run fresh audit"""
    from datetime import datetime

    with _audit_cache_lock:
        now = datetime.now()

        # Check if cache is valid
        if not force_refresh and _audit_results_cache["results"] is not None:
            if _audit_results_cache["updated_at"]:
                age = (now - _audit_results_cache["updated_at"]).total_seconds()
                if age < AUDIT_CACHE_TTL_SECONDS:
                    return _audit_results_cache["results"], _audit_results_cache["updated_at"]

        # Run fresh audit
        service = get_maintenance_service()
        results = service.run_full_audit()

        # Update cache
        _audit_results_cache["results"] = results
        _audit_results_cache["updated_at"] = now

        # Update the health summary in web cache and invalidate dashboard stats
        # so Dashboard will show fresh health data on next load
        web_cache = get_web_cache_service()
        web_cache.set(CACHE_KEY_MAINTENANCE_HEALTH, service.get_health_summary())
        web_cache.invalidate(CACHE_KEY_DASHBOARD_STATS)

        return results, now


def _check_blocked(action_name: str) -> Optional[str]:
    """Check if a maintenance action is blocked by another running operation.

    Returns an HTML alert string if blocked, None if OK to proceed.
    """
    runner = get_maintenance_runner()
    op_runner = get_operation_runner()

    if runner.is_running:
        return (
            '<div class="alert alert-warning maintenance-action-blocked" style="margin-bottom: 1rem;">'
            '<i data-lucide="alert-triangle"></i>'
            '<span>A maintenance action is already running. Please wait for it to complete.</span>'
            '</div><script>lucide.createIcons();</script>'
        )

    if op_runner.is_running:
        return (
            '<div class="alert alert-warning maintenance-action-blocked" style="margin-bottom: 1rem;">'
            '<i data-lucide="alert-triangle"></i>'
            '<span>A PlexCache operation is running. Please wait for it to complete.</span>'
            '</div><script>lucide.createIcons();</script>'
        )

    return None


def _get_max_workers() -> int:
    """Read max_concurrent_moves_array from settings for parallel maintenance."""
    try:
        import json
        from web.config import SETTINGS_FILE
        if SETTINGS_FILE.exists():
            with open(SETTINGS_FILE, 'r', encoding='utf-8') as f:
                settings = json.load(f)
            return max(1, int(settings.get('max_concurrent_moves_array', 2)))
    except (json.JSONDecodeError, IOError, ValueError, TypeError):
        pass
    return 2


def _start_async_action(action_name: str, service_method, method_args=(), method_kwargs=None, file_count=0, max_workers=1) -> Optional[str]:
    """Start an async maintenance action via the runner.

    Returns HTML response string if started, queued, or blocked.
    """
    blocked = _check_blocked(action_name)
    if blocked:
        # Try to queue instead of showing a blocked warning
        runner = get_maintenance_runner()
        if runner.queue_count >= runner._max_queue_size:
            return (
                '<div class="alert alert-warning maintenance-action-blocked" style="margin-bottom: 1rem;">'
                '<i data-lucide="alert-triangle"></i>'
                '<span>Queue is full (max 5). Please wait for current actions to complete.</span>'
                '</div><script>lucide.createIcons();</script>'
            )

        item_id = runner.enqueue_action(
            action_name=action_name,
            service_method=service_method,
            method_args=method_args,
            method_kwargs=method_kwargs or {},
            file_count=file_count,
            on_complete=_invalidate_caches,
            max_workers=max_workers,
        )
        if item_id:
            count = runner.queue_count
            return (
                '<div class="alert alert-info maintenance-action-queued" style="margin-bottom: 1rem;">'
                '<i data-lucide="list-plus"></i>'
                f'<span>Action queued (#{count}). Starts automatically after current action completes.</span>'
                '</div><script>lucide.createIcons();'
                'htmx.ajax("GET","/api/operation-banner",{target:"#global-operation-banner",swap:"innerHTML"});'
                '</script>'
            )
        return (
            '<div class="alert alert-warning maintenance-action-blocked" style="margin-bottom: 1rem;">'
            '<i data-lucide="alert-triangle"></i>'
            '<span>Could not queue action.</span>'
            '</div><script>lucide.createIcons();</script>'
        )

    runner = get_maintenance_runner()
    started = runner.start_action(
        action_name=action_name,
        service_method=service_method,
        method_args=method_args,
        method_kwargs=method_kwargs or {},
        file_count=file_count,
        on_complete=_invalidate_caches,
        max_workers=max_workers,
    )

    if started:
        return (
            '<div class="alert alert-info maintenance-async-started" style="margin-bottom: 1rem;">'
            '<i data-lucide="loader"></i>'
            '<span>Action started in background. You can navigate away from this page.</span>'
            '</div><script>lucide.createIcons();</script>'
        )
    else:
        return (
            '<div class="alert alert-warning" style="margin-bottom: 1rem;">'
            '<i data-lucide="alert-triangle"></i>'
            '<span>Could not start action. Another operation may be running.</span>'
            '</div><script>lucide.createIcons();</script>'
        )


@router.get("/", response_class=HTMLResponse)
def maintenance_page(request: Request):
    """Main maintenance page - loads instantly with skeleton, audit fetched via HTMX"""
    return templates.TemplateResponse(
        "maintenance/index.html",
        {
            "request": request,
            "page_title": "Maintenance"
        }
    )


@router.get("/audit", response_class=HTMLResponse)
def run_audit(request: Request, refresh: bool = Query(default=False, description="Force refresh")):
    """Run audit and return HTMX partial with results"""
    results, updated_at = _get_cached_audit_results(force_refresh=refresh)

    # Calculate cache age display
    cache_age = format_cache_age(updated_at)

    response = templates.TemplateResponse(
        "maintenance/partials/audit_results.html",
        {
            "request": request,
            "results": results,
            "cache_age": cache_age or "just now",
        }
    )
    # Prevent browser caching so refresh button always fetches fresh data
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    return response


@router.get("/health", response_class=HTMLResponse)
def health_summary(request: Request):
    """Get health summary for dashboard widget"""
    service = get_maintenance_service()
    health = service.get_health_summary()

    return templates.TemplateResponse(
        "maintenance/partials/health_widget.html",
        {
            "request": request,
            "health": health
        }
    )


# === Maintenance Runner Control Routes ===

@router.post("/stop-action", response_class=HTMLResponse)
def stop_maintenance_action(request: Request):
    """Stop the current maintenance action"""
    from web.services.maintenance_runner import get_maintenance_runner

    runner = get_maintenance_runner()
    runner.stop_action()

    # Return updated banner
    status = get_operation_runner().get_status_dict()
    maint_status = runner.get_status_dict()

    return templates.TemplateResponse(
        "components/global_operation_banner.html",
        {"request": request, "status": status, "maint_status": maint_status}
    )


@router.post("/dismiss-action")
def dismiss_maintenance_action():
    """Dismiss a completed/failed maintenance action"""
    runner = get_maintenance_runner()
    runner.dismiss()
    return JSONResponse({"ok": True})


def _record_sync_action(
    action_name: str,
    started_at: datetime,
    result,
):
    """Record a synchronous maintenance action to the persistent history."""
    import os
    import uuid
    try:
        completed_at = datetime.now()
        duration = (completed_at - started_at).total_seconds()

        affected_files = []
        if hasattr(result, "affected_paths") and result.affected_paths:
            affected_files = [os.path.basename(p) for p in result.affected_paths[:25]]

        entry = MaintenanceHistoryEntry(
            id=str(uuid.uuid4()),
            action_name=action_name,
            action_display=ACTION_HISTORY_LABELS.get(action_name, action_name),
            timestamp=started_at.isoformat(),
            completed_at=completed_at.isoformat(),
            duration_seconds=round(duration, 1),
            duration_display=format_duration(duration),
            file_count=result.affected_count if hasattr(result, "affected_count") else 0,
            affected_count=result.affected_count if hasattr(result, "affected_count") else 0,
            success=result.success if hasattr(result, "success") else True,
            was_stopped=False,
            errors=result.errors[:20] if hasattr(result, "errors") else [],
            error_count=len(result.errors) if hasattr(result, "errors") else 0,
            affected_files=affected_files,
            source="sync",
        )
        get_maintenance_history().record(entry)
    except Exception as e:
        logger.error(f"Failed to record sync maintenance history: {e}")


# === History Endpoint ===

@router.get("/history", response_class=HTMLResponse)
def action_history(
    request: Request,
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
):
    """Return the action history partial"""
    history = get_maintenance_history()
    all_entries = history.get_all()
    total_count = len(all_entries)
    entries = all_entries[offset:offset + limit]
    time_format = get_time_format()

    return templates.TemplateResponse(
        "maintenance/partials/action_history.html",
        {
            "request": request,
            "entries": entries,
            "time_format": time_format,
            "total_count": total_count,
            "showing": offset + len(entries),
            "offset": offset,
            "limit": limit,
        }
    )


# === Action Routes ===

@router.post("/restore-plexcached", response_class=HTMLResponse)
def restore_plexcached(
    request: Request,
    paths: List[str] = Form(default=[]),
    restore_all: bool = Form(default=False),
    orphaned_only: bool = Form(default=False),
    dry_run: bool = Form(default=True)
):
    """Restore orphaned .plexcached backups"""
    service = get_maintenance_service()

    if dry_run:
        # Synchronous dry-run path
        if restore_all:
            result = service.restore_all_plexcached(dry_run=True, orphaned_only=orphaned_only)
        else:
            result = service.restore_plexcached(paths, dry_run=True)

        audit_results = service.run_full_audit()
        return templates.TemplateResponse(
            "maintenance/partials/action_result.html",
            {"request": request, "action_result": result, "results": audit_results, "dry_run": True}
        )

    # Async path - run in background
    max_workers = _get_max_workers()
    file_count = len(paths) if not restore_all else 0
    if restore_all:
        response = _start_async_action(
            "restore-plexcached",
            service.restore_all_plexcached,
            method_kwargs={"dry_run": False, "orphaned_only": orphaned_only},
            file_count=file_count,
            max_workers=max_workers,
        )
    else:
        response = _start_async_action(
            "restore-plexcached",
            service.restore_plexcached,
            method_args=(paths,),
            method_kwargs={"dry_run": False},
            file_count=len(paths),
            max_workers=max_workers,
        )
    return HTMLResponse(response)


@router.post("/delete-plexcached", response_class=HTMLResponse)
def delete_plexcached(
    request: Request,
    paths: List[str] = Form(default=[]),
    delete_all: bool = Form(default=False),
    dry_run: bool = Form(default=True)
):
    """Delete orphaned .plexcached backups (when no longer needed)"""
    service = get_maintenance_service()

    if dry_run:
        if delete_all:
            result = service.delete_all_plexcached(dry_run=True)
        else:
            result = service.delete_plexcached(paths, dry_run=True)

        audit_results = service.run_full_audit()
        return templates.TemplateResponse(
            "maintenance/partials/action_result.html",
            {"request": request, "action_result": result, "results": audit_results, "dry_run": True}
        )

    # Async path
    max_workers = _get_max_workers()
    if delete_all:
        response = _start_async_action(
            "delete-plexcached",
            service.delete_all_plexcached,
            method_kwargs={"dry_run": False},
            max_workers=max_workers,
        )
    else:
        response = _start_async_action(
            "delete-plexcached",
            service.delete_plexcached,
            method_args=(paths,),
            method_kwargs={"dry_run": False},
            file_count=len(paths),
            max_workers=max_workers,
        )
    return HTMLResponse(response)


@router.post("/repair-plexcached", response_class=HTMLResponse)
def repair_plexcached(
    request: Request,
    paths: List[str] = Form(default=[]),
    repair_all: bool = Form(default=False),
    dry_run: bool = Form(default=True)
):
    """Repair malformed .plexcached backups by adding missing media extension"""
    service = get_maintenance_service()

    if dry_run:
        if repair_all:
            result = service.repair_all_plexcached(dry_run=True)
        else:
            result = service.repair_plexcached(paths, dry_run=True)

        audit_results = service.run_full_audit()
        return templates.TemplateResponse(
            "maintenance/partials/action_result.html",
            {"request": request, "action_result": result, "results": audit_results, "dry_run": True}
        )

    # Async path
    max_workers = _get_max_workers()
    if repair_all:
        response = _start_async_action(
            "repair-plexcached",
            service.repair_all_plexcached,
            method_kwargs={"dry_run": False},
            max_workers=max_workers,
        )
    else:
        response = _start_async_action(
            "repair-plexcached",
            service.repair_plexcached,
            method_args=(paths,),
            method_kwargs={"dry_run": False},
            file_count=len(paths),
            max_workers=max_workers,
        )
    return HTMLResponse(response)


@router.post("/delete-extensionless", response_class=HTMLResponse)
def delete_extensionless(
    request: Request,
    paths: List[str] = Form(default=[]),
    delete_all: bool = Form(default=False),
    dry_run: bool = Form(default=True)
):
    """Delete extensionless duplicate files (from malformed .plexcached restores)"""
    service = get_maintenance_service()

    if dry_run:
        if delete_all:
            result = service.delete_all_extensionless(dry_run=True)
        else:
            result = service.delete_extensionless_files(paths, dry_run=True)

        audit_results = service.run_full_audit()
        return templates.TemplateResponse(
            "maintenance/partials/action_result.html",
            {"request": request, "action_result": result, "results": audit_results, "dry_run": True}
        )

    # Async path
    max_workers = _get_max_workers()
    if delete_all:
        response = _start_async_action(
            "delete-extensionless",
            service.delete_all_extensionless,
            method_kwargs={"dry_run": False},
            max_workers=max_workers,
        )
    else:
        response = _start_async_action(
            "delete-extensionless",
            service.delete_extensionless_files,
            method_args=(paths,),
            method_kwargs={"dry_run": False},
            file_count=len(paths),
            max_workers=max_workers,
        )
    return HTMLResponse(response)


@router.post("/fix-with-backup", response_class=HTMLResponse)
def fix_with_backup(
    request: Request,
    paths: List[str] = Form(default=[]),
    dry_run: bool = Form(default=True)
):
    """Fix files that have .plexcached backup"""
    service = get_maintenance_service()

    if dry_run:
        result = service.fix_with_backup(paths, dry_run=True)
        audit_results = service.run_full_audit()
        return templates.TemplateResponse(
            "maintenance/partials/action_result.html",
            {"request": request, "action_result": result, "results": audit_results, "dry_run": True}
        )

    max_workers = _get_max_workers()
    response = _start_async_action(
        "fix-with-backup",
        service.fix_with_backup,
        method_args=(paths,),
        method_kwargs={"dry_run": False},
        file_count=len(paths),
        max_workers=max_workers,
    )
    return HTMLResponse(response)


@router.post("/sync-to-array", response_class=HTMLResponse)
def sync_to_array(
    request: Request,
    paths: List[str] = Form(default=[]),
    dry_run: bool = Form(default=True)
):
    """Move files to array - restores backups if they exist, copies if not"""
    service = get_maintenance_service()

    if dry_run:
        result = service.sync_to_array(paths, dry_run=True)
        audit_results = service.run_full_audit()
        return templates.TemplateResponse(
            "maintenance/partials/action_result.html",
            {"request": request, "action_result": result, "results": audit_results, "dry_run": True}
        )

    max_workers = _get_max_workers()
    response = _start_async_action(
        "sync-to-array",
        service.sync_to_array,
        method_args=(paths,),
        method_kwargs={"dry_run": False},
        file_count=len(paths),
        max_workers=max_workers,
    )
    return HTMLResponse(response)


@router.post("/evict-files", response_class=HTMLResponse)
async def evict_files(request: Request):
    """Evict file(s) from cache — runs in background via maintenance runner."""
    form = await request.form()
    paths = form.getlist("paths")

    if not paths:
        return HTMLResponse(
            '<div class="alert alert-warning"><i data-lucide="alert-triangle"></i>'
            '<span>No files selected</span></div><script>lucide.createIcons();</script>'
        )

    service = get_maintenance_service()
    max_workers = _get_max_workers()
    response = _start_async_action(
        "evict-files",
        service.evict_files,
        method_args=(paths,),
        method_kwargs={"dry_run": False},
        file_count=len(paths),
        max_workers=max_workers,
    )
    return HTMLResponse(response)


@router.post("/protect-with-backup", response_class=HTMLResponse)
def protect_with_backup(
    request: Request,
    paths: List[str] = Form(default=[]),
    dry_run: bool = Form(default=True)
):
    """Protect files by creating .plexcached backup on array and adding to exclude list"""
    service = get_maintenance_service()

    if dry_run:
        result = service.protect_with_backup(paths, dry_run=True)
        audit_results = service.run_full_audit()
        return templates.TemplateResponse(
            "maintenance/partials/action_result.html",
            {"request": request, "action_result": result, "results": audit_results, "dry_run": True}
        )

    max_workers = _get_max_workers()
    response = _start_async_action(
        "protect-with-backup",
        service.protect_with_backup,
        method_args=(paths,),
        method_kwargs={"dry_run": False},
        file_count=len(paths),
        max_workers=max_workers,
    )
    return HTMLResponse(response)


# === Synchronous Action Routes (instant operations) ===

@router.post("/add-to-exclude", response_class=HTMLResponse)
def add_to_exclude(
    request: Request,
    paths: List[str] = Form(default=[]),
    dry_run: bool = Form(default=True)
):
    """Add files to exclude list"""
    service = get_maintenance_service()
    started_at = datetime.now()
    result = service.add_to_exclude(paths, dry_run=dry_run)

    if not dry_run:
        _invalidate_caches()
        _record_sync_action("add-to-exclude", started_at, result)

    audit_results = service.run_full_audit()

    return templates.TemplateResponse(
        "maintenance/partials/action_result.html",
        {
            "request": request,
            "action_result": result,
            "results": audit_results,
            "dry_run": dry_run
        }
    )


@router.post("/clean-exclude", response_class=HTMLResponse)
def clean_exclude(
    request: Request,
    dry_run: bool = Form(default=True)
):
    """Clean stale exclude entries"""
    service = get_maintenance_service()
    started_at = datetime.now()
    result = service.clean_exclude(dry_run=dry_run)

    if not dry_run:
        _invalidate_caches()
        _record_sync_action("clean-exclude", started_at, result)

    audit_results = service.run_full_audit()

    return templates.TemplateResponse(
        "maintenance/partials/action_result.html",
        {
            "request": request,
            "action_result": result,
            "results": audit_results,
            "dry_run": dry_run
        }
    )


@router.post("/clean-timestamps", response_class=HTMLResponse)
def clean_timestamps(
    request: Request,
    dry_run: bool = Form(default=True)
):
    """Clean stale timestamp entries"""
    service = get_maintenance_service()
    started_at = datetime.now()
    result = service.clean_timestamps(dry_run=dry_run)

    if not dry_run:
        _invalidate_caches()
        _record_sync_action("clean-timestamps", started_at, result)

    audit_results = service.run_full_audit()

    return templates.TemplateResponse(
        "maintenance/partials/action_result.html",
        {
            "request": request,
            "action_result": result,
            "results": audit_results,
            "dry_run": dry_run
        }
    )


@router.post("/fix-timestamps", response_class=HTMLResponse)
def fix_timestamps(
    request: Request,
    paths: List[str] = Form(default=[]),
    dry_run: bool = Form(default=True)
):
    """Fix invalid file timestamps"""
    service = get_maintenance_service()
    started_at = datetime.now()
    result = service.fix_file_timestamps(paths, dry_run=dry_run)

    if not dry_run:
        _invalidate_caches()
        _record_sync_action("fix-timestamps", started_at, result)

    audit_results = service.run_full_audit()

    return templates.TemplateResponse(
        "maintenance/partials/action_result.html",
        {
            "request": request,
            "action_result": result,
            "results": audit_results,
            "dry_run": dry_run
        }
    )


@router.post("/resolve-duplicate", response_class=HTMLResponse)
def resolve_duplicate(
    request: Request,
    cache_path: str = Form(...),
    keep: str = Form(...),  # "cache" or "array"
    dry_run: bool = Form(default=True)
):
    """Resolve a duplicate file"""
    service = get_maintenance_service()
    started_at = datetime.now()
    result = service.resolve_duplicate(cache_path, keep, dry_run=dry_run)

    if not dry_run:
        _invalidate_caches()
        _record_sync_action("resolve-duplicate", started_at, result)

    audit_results = service.run_full_audit()

    return templates.TemplateResponse(
        "maintenance/partials/action_result.html",
        {
            "request": request,
            "action_result": result,
            "results": audit_results,
            "dry_run": dry_run
        }
    )


# === Queue Management Routes ===

@router.get("/check-blocked")
def check_blocked_status():
    """Check if actions would be blocked/queued (for modal button state)."""
    runner = get_maintenance_runner()
    op_runner = get_operation_runner()
    is_blocked = runner.is_running or op_runner.is_running
    return JSONResponse({
        "blocked": is_blocked,
        "can_queue": is_blocked and runner.queue_count < runner._max_queue_size,
        "queue_count": runner.queue_count,
        "queue_full": runner.queue_count >= runner._max_queue_size,
    })


@router.post("/queue/remove/{item_id}")
def remove_from_queue(item_id: str):
    """Remove an item from the maintenance queue."""
    return JSONResponse({"ok": get_maintenance_runner().remove_from_queue(item_id)})


@router.post("/queue/clear")
def clear_queue():
    """Clear all queued maintenance actions."""
    count = get_maintenance_runner().clear_queue()
    return JSONResponse({"ok": True, "cleared": count})


@router.post("/queue/resume")
def resume_queue():
    """Resume a paused maintenance queue."""
    get_maintenance_runner().resume_queue()
    return JSONResponse({"ok": True})


@router.post("/queue/skip")
def skip_next_queued():
    """Skip the next queued action during countdown."""
    get_maintenance_runner().skip_next_queued()
    return JSONResponse({"ok": True})


@router.post("/queue/start-now")
def start_next_now():
    """Cancel countdown and immediately start the next queued action."""
    get_maintenance_runner().start_next_now()
    return JSONResponse({"ok": True})


# === Duplicate Scanner Routes ===

@router.post("/scan-duplicates", response_class=HTMLResponse)
def scan_duplicates(request: Request):
    """Trigger a background Plex duplicate scan"""
    service = get_duplicate_service()
    response = _start_async_action(
        "scan-duplicates",
        service.scan_plex_libraries,
        file_count=0,
        max_workers=1,
    )
    return HTMLResponse(response)


@router.get("/duplicates", response_class=HTMLResponse)
def get_duplicates(request: Request):
    """Get duplicate scan results card (HTMX partial)"""
    service = get_duplicate_service()
    results = service.load_scan_results()

    # Check if arr is configured
    from web.services.settings_service import get_settings_service
    arr_instances = get_settings_service().get_arr_instances()
    arr_configured = any(
        i.get("enabled") and i.get("url") and i.get("api_key")
        for i in arr_instances
    )

    return templates.TemplateResponse(
        "maintenance/partials/duplicate_card.html",
        {
            "request": request,
            "scan_results": results,
            "arr_configured": arr_configured,
        }
    )


@router.post("/delete-duplicates", response_class=HTMLResponse)
def delete_duplicates(
    request: Request,
    paths: List[str] = Form(default=[]),
    all_orphans: bool = Form(default=False),
    dry_run: bool = Form(default=True),
):
    """Delete selected duplicate files or all orphans"""
    service = get_duplicate_service()

    if dry_run:
        # Synchronous dry-run
        if all_orphans:
            result = service.delete_all_orphans(dry_run=True)
        else:
            result = service.delete_files(paths=paths, dry_run=True)
        return templates.TemplateResponse(
            "maintenance/partials/action_result.html",
            {
                "request": request,
                "action_result": result,
                "results": _get_cached_audit_results()[0],
                "dry_run": True,
            }
        )

    # Async path
    if all_orphans:
        response = _start_async_action(
            "delete-duplicates",
            service.delete_all_orphans,
            method_kwargs={"dry_run": False},
        )
    else:
        response = _start_async_action(
            "delete-duplicates",
            service.delete_files,
            method_args=(paths,),
            method_kwargs={"dry_run": False},
            file_count=len(paths),
        )
    return HTMLResponse(response)


# === Preview Routes (always dry_run) ===

@router.get("/preview/restore-plexcached", response_class=HTMLResponse)
def preview_restore_plexcached(request: Request):
    """Preview what restore-plexcached would do"""
    service = get_maintenance_service()
    result = service.restore_all_plexcached(dry_run=True)

    return templates.TemplateResponse(
        "maintenance/partials/preview_result.html",
        {
            "request": request,
            "action": "Restore .plexcached Backups",
            "result": result
        }
    )


@router.get("/preview/clean-exclude", response_class=HTMLResponse)
def preview_clean_exclude(request: Request):
    """Preview what clean-exclude would do"""
    service = get_maintenance_service()
    result = service.clean_exclude(dry_run=True)
    stale_entries = list(service.get_exclude_files() - service.get_cache_files())[:50]

    return templates.TemplateResponse(
        "maintenance/partials/preview_result.html",
        {
            "request": request,
            "action": "Clean Stale Exclude Entries",
            "result": result,
            "items": stale_entries
        }
    )


@router.get("/preview/clean-timestamps", response_class=HTMLResponse)
def preview_clean_timestamps(request: Request):
    """Preview what clean-timestamps would do"""
    service = get_maintenance_service()
    result = service.clean_timestamps(dry_run=True)
    stale_entries = list(service.get_timestamp_files() - service.get_cache_files())[:50]

    return templates.TemplateResponse(
        "maintenance/partials/preview_result.html",
        {
            "request": request,
            "action": "Clean Stale Timestamp Entries",
            "result": result,
            "items": stale_entries
        }
    )
