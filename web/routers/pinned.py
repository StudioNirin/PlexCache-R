"""Pinned media routes — HTMX-driven pin picker + chip list."""

import logging

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
    """Toggle a pin. Returns a button partial + inline error on budget overrun."""
    service = get_pinned_service()
    result = service.toggle_pin(rating_key, pin_type, title)

    status = 200
    if result.get("error"):
        status = 400

    return templates.TemplateResponse(
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


@router.get("/list", response_class=HTMLResponse)
def pinned_list(request: Request):
    """HTMX partial: currently-pinned chip list + budget summary."""
    service = get_pinned_service()
    pins = service.list_pins_with_metadata()
    budget = service.budget_check()
    return templates.TemplateResponse(
        request,
        "settings/partials/pinned_chip_list.html",
        {"pins": pins, "budget": budget},
    )
