"""Log viewing routes"""

import asyncio
import logging
import re
from pathlib import Path

from fastapi import APIRouter, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse

from web.config import templates, LOGS_DIR

router = APIRouter()
logger = logging.getLogger(__name__)

# Max lines rendered in DOM to prevent browser lag
MAX_RENDERED_LINES = 5000

# Log line regex: timestamp - LEVEL - message
# Supports both 24h (14:30:05) and 12h (2:30:05 PM) formats
_LOG_LINE_RE = re.compile(
    r'^(\d{1,2}:\d{2}:\d{2}(?:\s*[AP]M)?)\s*-\s*'
    r'(DEBUG|INFO|WARNING|ERROR|CRITICAL|SUMMARY)\s*-\s*(.*)$',
    re.IGNORECASE
)

# Phase detection markers (reused from OperationRunner._PHASE_MARKERS)
_PHASE_MARKERS = [
    ("--- Results ---", "results"),
    ("Smart eviction", "evicting"),
    ("Caching to cache drive", "caching"),
    ("Returning to array", "restoring"),
    ("Copying to array", "restoring"),
    ("--- Moving Files ---", "moving"),
    ("Total media to cache:", "analyzing"),
    ("--- Fetching Media ---", "fetching"),
]


def _detect_phase(message: str, current_phase: str) -> str:
    """Detect phase from message text. Returns new phase or current."""
    for marker, phase in _PHASE_MARKERS:
        if marker in message:
            return phase
    return current_phase


def parse_log_line(raw: str, current_phase: str) -> dict:
    """Parse a single log line into structured data.

    Returns dict with keys: raw, timestamp, level, message, phase, is_continuation.
    """
    m = _LOG_LINE_RE.match(raw)
    if m:
        timestamp = m.group(1).strip()
        level = m.group(2).upper()
        message = m.group(3)
        phase = _detect_phase(message, current_phase)
        return {
            'raw': raw,
            'timestamp': timestamp,
            'level': level,
            'message': message,
            'phase': phase,
            'is_continuation': False,
        }
    else:
        # Continuation line (traceback, indented text) — inherits previous context
        return {
            'raw': raw,
            'timestamp': '',
            'level': '',
            'message': raw,
            'phase': current_phase,
            'is_continuation': True,
        }


def parse_log_content(text: str) -> tuple:
    """Parse full log content into structured lines and level counts.

    Returns (lines: list[dict], counts: dict[str, int]).
    """
    counts = {'ERROR': 0, 'WARNING': 0, 'INFO': 0, 'DEBUG': 0, 'SUMMARY': 0, 'CRITICAL': 0}
    lines = []
    current_phase = ''
    prev_level = 'INFO'
    prev_timestamp = ''

    for raw_line in text.split('\n'):
        if not raw_line.strip():
            continue

        parsed = parse_log_line(raw_line, current_phase)
        current_phase = parsed['phase']

        if parsed['is_continuation']:
            # Inherit level/timestamp from previous parsed line
            parsed['level'] = prev_level
            parsed['timestamp'] = prev_timestamp
        else:
            prev_level = parsed['level']
            prev_timestamp = parsed['timestamp']
            # Count only non-continuation lines
            if parsed['level'] in counts:
                counts[parsed['level']] += 1

        lines.append(parsed)

    return lines, counts


@router.get("/", response_class=HTMLResponse)
def logs_viewer(request: Request):
    """Log viewer page"""
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
def get_log_content(request: Request, filename: str = "", lines: int = 100):
    """Get log file content with structured parsing"""
    if not filename:
        return templates.TemplateResponse(
            "logs/partials/log_content.html",
            {"request": request, "lines": [], "counts": {}, "filename": "", "capped": False}
        )

    # Security: prevent directory traversal
    safe_filename = Path(filename).name
    log_path = LOGS_DIR / safe_filename

    if not log_path.exists() or not log_path.is_file():
        return templates.TemplateResponse(
            "logs/partials/log_content.html",
            {
                "request": request,
                "lines": [{'raw': f'Log file not found: {safe_filename}', 'level': 'ERROR',
                           'timestamp': '', 'phase': '', 'is_continuation': False}],
                "counts": {'ERROR': 1},
                "filename": safe_filename,
                "capped": False
            }
        )

    try:
        with open(log_path, 'r', encoding='utf-8', errors='replace') as f:
            all_lines = f.readlines()

        if lines == 0:
            raw_text = ''.join(all_lines)
        else:
            raw_text = ''.join(all_lines[-lines:])

        parsed_lines, counts = parse_log_content(raw_text)

        # Cap rendered lines for DOM performance
        capped = len(parsed_lines) > MAX_RENDERED_LINES
        if capped:
            parsed_lines = parsed_lines[-MAX_RENDERED_LINES:]

    except Exception as e:
        parsed_lines = [{'raw': f'Error reading log: {e}', 'level': 'ERROR',
                         'timestamp': '', 'phase': '', 'is_continuation': False}]
        counts = {'ERROR': 1}
        capped = False

    is_htmx = request.headers.get("HX-Request") == "true"

    template_context = {
        "request": request,
        "lines": parsed_lines,
        "counts": counts,
        "filename": safe_filename,
        "capped": capped,
    }

    if is_htmx:
        return templates.TemplateResponse(
            "logs/partials/log_content.html",
            template_context
        )

    return {"filename": safe_filename, "line_count": len(parsed_lines), "capped": capped, "counts": counts}


@router.get("/download")
def download_log(filename: str = ""):
    """Download a log file"""
    if not filename:
        return {"error": "No filename specified"}

    safe_filename = Path(filename).name
    log_path = LOGS_DIR / safe_filename

    if not log_path.exists() or not log_path.is_file():
        return {"error": f"Log file not found: {safe_filename}"}

    return FileResponse(
        path=str(log_path),
        filename=safe_filename,
        media_type="text/plain",
        headers={"Content-Disposition": f"attachment; filename={safe_filename}"}
    )


@router.websocket("/ws")
async def websocket_logs(websocket: WebSocket):
    """WebSocket endpoint for real-time log streaming.

    Query params:
        source: 'file' (tail a log file) or 'live' (stream operation logs)
        filename: log filename (for source=file)
        lines: initial lines to send (for source=file, default 100)
    """
    await websocket.accept()

    params = websocket.query_params
    source = params.get('source', 'live')
    filename = params.get('filename', '')
    initial_lines = int(params.get('lines', '100'))

    try:
        if source == 'file':
            await _ws_tail_file(websocket, filename, initial_lines)
        else:
            await _ws_live_stream(websocket)
    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.error(f"WebSocket error: {e}")


async def _ws_tail_file(websocket: WebSocket, filename: str, initial_lines: int):
    """Tail a log file via WebSocket, sending structured parsed lines."""
    if not filename:
        await websocket.send_json({"type": "error", "message": "No filename specified"})
        return

    safe_filename = Path(filename).name
    log_path = LOGS_DIR / safe_filename

    if not log_path.exists():
        await websocket.send_json({"type": "error", "message": f"File not found: {safe_filename}"})
        return

    # Send initial content
    try:
        with open(log_path, 'r', encoding='utf-8', errors='replace') as f:
            all_lines = f.readlines()

        if initial_lines == 0:
            raw_text = ''.join(all_lines)
        else:
            raw_text = ''.join(all_lines[-initial_lines:])

        parsed, counts = parse_log_content(raw_text)
        capped = len(parsed) > MAX_RENDERED_LINES
        if capped:
            parsed = parsed[-MAX_RENDERED_LINES:]

        await websocket.send_json({
            "type": "initial",
            "lines": parsed,
            "counts": counts,
            "capped": capped,
        })
    except Exception as e:
        await websocket.send_json({"type": "error", "message": str(e)})
        return

    # Track file position for tailing
    last_size = log_path.stat().st_size
    heartbeat_counter = 0

    while True:
        await asyncio.sleep(0.5)
        heartbeat_counter += 1

        try:
            current_size = log_path.stat().st_size
        except FileNotFoundError:
            # File was rotated/deleted
            await websocket.send_json({"type": "info", "message": "Log file rotated"})
            last_size = 0
            continue

        if current_size < last_size:
            # File was truncated/rotated — re-read from start
            last_size = 0

        if current_size > last_size:
            with open(log_path, 'r', encoding='utf-8', errors='replace') as f:
                f.seek(last_size)
                new_text = f.read()
            last_size = current_size

            new_parsed, new_counts = parse_log_content(new_text)
            if new_parsed:
                await websocket.send_json({
                    "type": "append",
                    "lines": new_parsed,
                    "counts": new_counts,
                })

        # Heartbeat every 5 seconds (10 * 0.5s)
        if heartbeat_counter >= 10:
            heartbeat_counter = 0
            await websocket.send_json({"type": "heartbeat"})


async def _ws_live_stream(websocket: WebSocket):
    """Stream live operation logs via OperationRunner's subscriber queue."""
    from web.services import get_operation_runner
    runner = get_operation_runner()

    queue = runner.subscribe_logs()
    try:
        # Send backlog as initial data
        current_logs = runner.log_messages
        if current_logs:
            backlog_text = '\n'.join(current_logs[-50:])
            parsed, counts = parse_log_content(backlog_text)
            await websocket.send_json({
                "type": "initial",
                "lines": parsed,
                "counts": counts,
                "capped": False,
            })

        # Stream new messages as they arrive
        heartbeat_counter = 0
        while True:
            try:
                msg = await asyncio.wait_for(queue.get(), timeout=1.0)
                parsed = parse_log_line(msg, '')
                # Single-line counts
                line_counts = {}
                if not parsed['is_continuation'] and parsed['level']:
                    line_counts[parsed['level']] = 1
                await websocket.send_json({
                    "type": "append",
                    "lines": [parsed],
                    "counts": line_counts,
                })
            except asyncio.TimeoutError:
                # Send heartbeat
                heartbeat_counter += 1
                if heartbeat_counter >= 5:
                    heartbeat_counter = 0
                    await websocket.send_json({"type": "heartbeat"})
    finally:
        runner.unsubscribe_logs(queue)
