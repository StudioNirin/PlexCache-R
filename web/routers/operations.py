"""Operation routes - run cache operations"""

from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from web.config import TEMPLATES_DIR
from web.services import get_operation_runner

router = APIRouter()
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


@router.post("/run")
async def run_operation(
    request: Request,
    dry_run: str = Form("false"),
    verbose: str = Form("false")
):
    """Trigger a cache operation"""
    # Convert strings to bool (form data comes as strings)
    dry_run_bool = dry_run.lower() in ("true", "1", "yes", "on")
    verbose_bool = verbose.lower() in ("true", "1", "yes", "on")

    runner = get_operation_runner()

    # Check if HTMX request
    is_htmx = request.headers.get("HX-Request") == "true"

    # Try to start the operation
    if runner.is_running:
        message = "Operation already in progress"
        success = False
    else:
        success = runner.start_operation(dry_run=dry_run_bool, verbose=verbose_bool)
        if success:
            mode_parts = []
            if dry_run_bool:
                mode_parts.append("Dry run")
            if verbose_bool:
                mode_parts.append("verbose")
            mode = " ".join(mode_parts) if mode_parts else "Operation"
            message = f"{mode.capitalize() if mode_parts else mode} started"
        else:
            message = "Failed to start operation"

    if is_htmx:
        status = runner.get_status_dict()
        return templates.TemplateResponse(
            "components/operation_status.html",
            {
                "request": request,
                "status": status,
                "message": message,
                "success": success
            }
        )

    return JSONResponse({
        "success": success,
        "message": message,
        "status": runner.get_status_dict()
    })


@router.get("/status")
async def get_status(request: Request):
    """Get current operation status"""
    runner = get_operation_runner()
    status = runner.get_status_dict()

    is_htmx = request.headers.get("HX-Request") == "true"

    if is_htmx:
        return templates.TemplateResponse(
            "components/operation_status.html",
            {
                "request": request,
                "status": status
            }
        )

    return JSONResponse(status)


@router.get("/logs")
async def get_operation_logs(request: Request):
    """Get captured log messages from current/last operation"""
    runner = get_operation_runner()
    logs = runner.log_messages

    is_htmx = request.headers.get("HX-Request") == "true"

    if is_htmx:
        return templates.TemplateResponse(
            "components/operation_logs.html",
            {
                "request": request,
                "logs": logs
            }
        )

    return JSONResponse({"logs": logs})


@router.get("/activity")
async def get_recent_activity(request: Request):
    """Get recent file activity from operations"""
    runner = get_operation_runner()
    activity = runner.recent_activity

    is_htmx = request.headers.get("HX-Request") == "true"

    if is_htmx:
        return templates.TemplateResponse(
            "components/recent_activity.html",
            {
                "request": request,
                "activity": activity
            }
        )

    return JSONResponse({"activity": activity})
