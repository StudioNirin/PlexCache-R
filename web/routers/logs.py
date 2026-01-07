"""Log viewing routes"""

import asyncio
from pathlib import Path

from fastapi import APIRouter, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from web.config import TEMPLATES_DIR, LOGS_DIR

router = APIRouter()
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


@router.get("/", response_class=HTMLResponse)
async def logs_viewer(request: Request):
    """Log viewer page"""
    # Get list of log files
    log_files = []
    if LOGS_DIR.exists():
        log_files = sorted(
            [f.name for f in LOGS_DIR.glob("*.log")],
            reverse=True
        )

    return templates.TemplateResponse(
        "logs/viewer.html",
        {
            "request": request,
            "page_title": "Logs",
            "log_files": log_files,
            "current_file": log_files[0] if log_files else None
        }
    )


@router.get("/content")
async def get_log_content(request: Request, filename: str = "", lines: int = 100):
    """Get log file content"""
    if not filename:
        return templates.TemplateResponse(
            "logs/partials/log_content.html",
            {"request": request, "content": "", "filename": ""}
        )

    # Security: prevent directory traversal
    safe_filename = Path(filename).name
    log_path = LOGS_DIR / safe_filename

    if not log_path.exists() or not log_path.is_file():
        content = f"Log file not found: {safe_filename}"
    else:
        # Read lines (0 = all lines)
        try:
            with open(log_path, 'r', encoding='utf-8', errors='replace') as f:
                all_lines = f.readlines()
                if lines == 0:
                    content = ''.join(all_lines)
                else:
                    content = ''.join(all_lines[-lines:])
        except Exception as e:
            content = f"Error reading log: {e}"

    is_htmx = request.headers.get("HX-Request") == "true"

    if is_htmx:
        return templates.TemplateResponse(
            "logs/partials/log_content.html",
            {
                "request": request,
                "content": content,
                "filename": safe_filename
            }
        )

    return {"filename": safe_filename, "content": content}


@router.websocket("/ws")
async def websocket_logs(websocket: WebSocket):
    """WebSocket endpoint for real-time log streaming"""
    await websocket.accept()

    # Get operation runner
    from web.services import get_operation_runner
    runner = get_operation_runner()

    try:
        # Send connection confirmation
        await websocket.send_text("--- Connected to log stream ---")

        # Track the last message we sent (handles ring buffer truncation)
        last_sent_msg = None

        # If there are existing logs, send recent backlog
        current_logs = runner.log_messages
        if current_logs:
            await websocket.send_text("--- Sending recent logs ---")
            # Send last 50 messages as backlog
            backlog = current_logs[-50:]
            for msg in backlog:
                await websocket.send_text(msg)
            last_sent_msg = current_logs[-1] if current_logs else None
            await websocket.send_text("--- Live streaming ---")

        # Poll for new logs (avoids asyncio.Queue threading issues)
        while True:
            await asyncio.sleep(0.3)  # Poll every 300ms for responsiveness

            # Get current logs
            current_logs = runner.log_messages
            if not current_logs:
                continue

            # Find where to start sending from
            if last_sent_msg is None:
                # First time, send all
                new_msgs = current_logs
            else:
                # Find last sent message in current list
                try:
                    last_idx = current_logs.index(last_sent_msg)
                    new_msgs = current_logs[last_idx + 1:]
                except ValueError:
                    # Last message no longer in buffer (truncated out)
                    # Send recent messages to catch up
                    new_msgs = current_logs[-20:]

            # Send new messages
            for msg in new_msgs:
                await websocket.send_text(msg)

            # Update tracking
            if current_logs:
                last_sent_msg = current_logs[-1]

    except WebSocketDisconnect:
        pass
    except Exception as e:
        # Log any unexpected errors
        import logging
        logging.getLogger(__name__).error(f"WebSocket error: {e}")
