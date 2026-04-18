"""Pinned media routes — HTMX-driven pin picker + chip list."""

import json
import logging
from typing import List

from fastapi import APIRouter, Request, Form, Query
from fastapi.responses import HTMLResponse

from web.config import templates
from web.services import get_pinned_service

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/search", response_class=HTMLResponse)
def pinned_search(
    request: Request,
    q: str = Query("", description="Plex search query"),
    limit: int = Query(25, ge=1, le=50),
):
    """HTMX partial: pin-picker search results for the given query."""
    service = get_pinned_service()
    results = service.search(q, limit=limit)
    return templates.TemplateResponse(
        request,
        "settings/partials/pinned_results.html",
        {"results": results, "query": q},
    )


@router.get("/expand", response_class=HTMLResponse)
def pinned_expand(
    request: Request,
    rating_key: str = Query(...),
    level: str = Query(..., pattern="^(show|season)$"),
):
    """HTMX partial: lazy children for a show (seasons) or season (episodes)."""
    service = get_pinned_service()
    children = service.expand(rating_key, level)
    return templates.TemplateResponse(
        request,
        "settings/partials/pinned_children.html",
        {
            "children": children,
            "parent_rating_key": rating_key,
            "level": level,
        },
    )


@router.post("/toggle", response_class=HTMLResponse)
def pinned_toggle(
    request: Request,
    rating_key: str = Form(...),
    pin_type: str = Form(...),
    title: str = Form(""),
):
    """Toggle a pin. Returns a button partial + inline error on budget overrun.

    On a successful pin/unpin (no error), the response sets an ``HX-Trigger:
    pinned-updated`` header so the Currently Pinned chip list in the Settings
    UI auto-refreshes. The chip list's ``×`` button uses ``hx-swap="none"``
    and relies on this trigger to stay in sync.

    On a successful unpin, any cache paths uniquely protected by the removed
    pin are handed to the maintenance runner so the files move back to the
    array immediately instead of waiting for retention to expire. The banner
    polls independently, so the running action becomes visible with no extra
    client work.
    """
    service = get_pinned_service()
    result = service.toggle_pin(rating_key, pin_type, title)

    status = 200
    if result.get("error"):
        status = 400

    response = templates.TemplateResponse(
        request,
        "settings/partials/pinned_toggle_response.html",
        {
            "rating_key": rating_key,
            "pin_type": pin_type,
            "title": title,
            "is_pinned": result["is_pinned"],
            "error": result.get("error"),
            "budget": result.get("budget", {}),
        },
        status_code=status,
    )
    if not result.get("error"):
        triggers = {"pinned-updated": {}}
        # If this was an unpin, kick off a background eviction for any paths
        # uniquely held by the removed pin. Queued if the runner is busy;
        # silently skipped on queue overflow (the next run still moves the
        # file back once retention expires).
        evict_paths = result.get("evict_paths") or []
        if evict_paths and not result.get("is_pinned"):
            started = _start_unpin_eviction(evict_paths)
            if started:
                # Banner hx-trigger listens for this event to refetch
                # immediately (skipping its 8s poll). base.html also scrolls
                # the page to the top so the banner is visible.
                triggers["pinned-eviction-started"] = {}
        # If this was a pin-add, log a "Cached" activity entry for each
        # freshly-pinned file that's already on cache. Files not yet on cache
        # will get their own "Cached" entry via FileMover on the next run.
        pinned_paths = result.get("pinned_paths") or []
        if pinned_paths and result.get("is_pinned"):
            _record_pin_activity(pinned_paths)
        response.headers["HX-Trigger"] = json.dumps(triggers)
    return response


def _record_pin_activity(cache_paths: list) -> None:
    """Record a Cached activity entry for each pinned path that exists on cache."""
    import os
    try:
        from core.activity import record_file_activity
        for path in cache_paths:
            try:
                if not os.path.exists(path):
                    continue
                size_bytes = os.path.getsize(path)
            except OSError:
                continue
            record_file_activity(
                action="Cached",
                filename=os.path.basename(path),
                size_bytes=size_bytes,
            )
    except Exception as e:
        logger.warning("Pin activity could not be recorded: %s", e)


def _start_unpin_eviction(cache_paths: list) -> bool:
    """Start a background eviction for cache paths freshly released by an unpin.

    Returns True if the action was started or queued, False if skipped.
    """
    try:
        from web.services.maintenance_runner import get_maintenance_runner
        from web.services.maintenance_service import get_maintenance_service
        from web.routers.maintenance import _invalidate_caches, _get_max_workers

        runner = get_maintenance_runner()
        service = get_maintenance_service()
        started = runner.start_action(
            action_name="evict-files",
            service_method=service.evict_files,
            method_args=(cache_paths,),
            method_kwargs={"dry_run": False},
            file_count=len(cache_paths),
            on_complete=_invalidate_caches,
            max_workers=_get_max_workers(),
        )
        if started:
            return True
        # Runner is busy; queue if there's room, otherwise give up quietly.
        if runner.queue_count < runner._max_queue_size:
            item_id = runner.enqueue_action(
                action_name="evict-files",
                service_method=service.evict_files,
                method_args=(cache_paths,),
                method_kwargs={"dry_run": False},
                file_count=len(cache_paths),
                on_complete=_invalidate_caches,
                max_workers=_get_max_workers(),
            )
            return bool(item_id)
        logger.warning(
            "Unpin eviction skipped: runner busy and queue full (%d paths)",
            len(cache_paths),
        )
        return False
    except Exception as e:
        logger.warning("Unpin eviction could not start: %s", e)
        return False


@router.post("/unpin-group", response_class=HTMLResponse)
def pinned_unpin_group(
    request: Request,
    rating_keys: List[str] = Form(default=[]),
):
    """Unpin every rating_key in ``rating_keys`` in a single batch.

    Used by the group header's "Unpin all" button. Reuses the same unpin
    flow as a single toggle: emits ``pinned-updated`` so the chip list
    refreshes, and starts one background eviction covering every cache
    path that was uniquely held by the removed pins.
    """
    service = get_pinned_service()
    result = service.unpin_many(rating_keys)

    response = HTMLResponse(
        '<div class="alert alert-info" style="margin-bottom: 1rem;">'
        '<i data-lucide="check-circle"></i>'
        f'<span>Unpinned {result["removed"]} item(s).</span>'
        '</div><script>lucide.createIcons();</script>',
        status_code=200,
    )

    triggers = {"pinned-updated": {}}
    evict_paths = result.get("evict_paths") or []
    if evict_paths:
        started = _start_unpin_eviction(evict_paths)
        if started:
            triggers["pinned-eviction-started"] = {}
    response.headers["HX-Trigger"] = json.dumps(triggers)
    return response


@router.get("/list", response_class=HTMLResponse)
def pinned_list(request: Request):
    """HTMX partial: currently-pinned grouped list + budget summary."""
    from core.system_utils import format_bytes
    service = get_pinned_service()
    groups = service.list_pins_grouped()
    budget = service.budget_check()
    total_pinned_display = format_bytes(budget["total_pinned_bytes"])
    pin_count = sum(g.get("pin_count", 0) for g in groups)
    return templates.TemplateResponse(
        request,
        "settings/partials/pinned_chip_list.html",
        {
            "groups": groups,
            "pin_count": pin_count,
            "budget": budget,
            "total_pinned_display": total_pinned_display,
        },
    )
