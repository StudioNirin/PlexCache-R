"""Operation routes - run cache operations"""

from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse

from web.config import templates
from web.services import get_operation_runner
from web.services.maintenance_runner import get_maintenance_runner

router = APIRouter()


@router.post("/run")
def run_operation(
    request: Request,
    dry_run: str = Form("false"),
    verbose: str = Form("false")
):
    """Trigger a cache operation"""
    # Convert strings to bool (form data comes as strings)
    dry_run_bool = dry_run.lower() in ("true", "1", "yes", "on")
    verbose_bool = verbose.lower() in ("true", "1", "yes", "on")

    runner = get_operation_runner()

    # Check if HTMX request and what target
    is_htmx = request.headers.get("HX-Request") == "true"
    hx_target = request.headers.get("HX-Target", "")

    # Try to start the operation
    maint_runner = get_maintenance_runner()
    if runner.is_running:
        message = "Operation already in progress"
        success = False
    elif maint_runner.is_running:
        message = "A maintenance action is in progress. Please wait for it to complete."
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
        maint_status = maint_runner.get_status_dict()
        # Use global banner template if targeting the global banner
        if hx_target == "global-operation-banner":
            response = templates.TemplateResponse(
                "components/global_operation_banner.html",
                {
                    "request": request,
                    "status": status,
                    "maint_status": maint_status,
                    "blocked_message": message if not success else None
                }
            )
            return response
        # Default to original operation_status template
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


@router.post("/stop")
def stop_operation(request: Request):
    """Stop the current operation"""
    runner = get_operation_runner()

    is_htmx = request.headers.get("HX-Request") == "true"
    hx_target = request.headers.get("HX-Target", "")

    if runner.is_running:
        success = runner.stop_operation()
        message = "Stop requested - operation will stop after current file" if success else "Failed to stop operation"
    else:
        success = False
        message = "No operation is currently running"

    if is_htmx:
        status = runner.get_status_dict()
        maint_status = get_maintenance_runner().get_status_dict()
        # Use global banner template if targeting the global banner
        if hx_target == "global-operation-banner":
            return templates.TemplateResponse(
                "components/global_operation_banner.html",
                {
                    "request": request,
                    "status": status,
                    "maint_status": maint_status
                }
            )
        # Default to original operation_status template
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
def get_status(request: Request):
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


@router.get("/activity")
def get_recent_activity(request: Request):
    """Get recent file activity from operations"""
    from web.services import get_settings_service

    runner = get_operation_runner()
    activity = runner.recent_activity

    is_htmx = request.headers.get("HX-Request") == "true"

    if is_htmx:
        context = {
            "request": request,
            "activity": activity,
        }
        # Pass extra context when activity is empty for contextual empty states
        if not activity:
            settings_service = get_settings_service()
            context["plex_connected"] = settings_service.check_plex_connection()
            context["last_run"] = settings_service.get_last_run_time()

        return templates.TemplateResponse(
            "components/recent_activity.html",
            context
        )

    return JSONResponse({"activity": activity})
