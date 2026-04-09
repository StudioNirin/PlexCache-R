"""PlexCache-D Web UI - FastAPI Application"""

import logging
from contextlib import asynccontextmanager
from pathlib import Path

from urllib.parse import urlparse

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, RedirectResponse, Response

from web import __version__
from web.config import templates, STATIC_DIR, PROJECT_ROOT, CONFIG_DIR, SETTINGS_FILE
from web.routers import dashboard, cache, settings, operations, logs, api, maintenance, setup, auth
from web.services import get_scheduler_service, get_settings_service
from web.services.web_cache import init_web_cache, get_web_cache_service
import os
from core.system_utils import SystemDetector, detect_zfs, set_zfs_prefixes


def _suppress_noisy_loggers():
    """Suppress debug spam from third-party libraries"""
    # Suppress python-multipart form parser debug spam
    logging.getLogger("multipart").setLevel(logging.WARNING)
    logging.getLogger("multipart.multipart").setLevel(logging.WARNING)
    logging.getLogger("python_multipart").setLevel(logging.WARNING)
    logging.getLogger("python-multipart").setLevel(logging.WARNING)
    # Suppress HTTP client noise
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)


def _detect_zfs_paths():
    """Detect ZFS-backed path mappings for the web UI.

    Same logic as PlexCacheApp._detect_zfs_paths() but reads settings from disk
    since the web UI doesn't use PlexCacheApp directly.
    """
    detector = SystemDetector()
    if not detector.is_unraid:
        return

    import json
    try:
        with open(str(SETTINGS_FILE), 'r') as f:
            settings = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return

    zfs_prefixes = set()
    for mapping in settings.get('path_mappings', []):
        if not mapping.get('enabled', True):
            continue
        real_path = mapping.get('real_path', '')
        if real_path and real_path.startswith('/mnt/user/') and detect_zfs(real_path):
            # Verify truly pool-only by probing /mnt/user0/ for array files
            # Hybrid shares (shareUseCache=yes/prefer) have files on BOTH ZFS cache + array
            user0_path = '/mnt/user0/' + real_path[len('/mnt/user/'):]
            if os.path.exists('/mnt/user0'):
                user0_has_files = False
                if os.path.isdir(user0_path):
                    try:
                        with os.scandir(user0_path) as it:
                            user0_has_files = next(it, None) is not None
                    except OSError:
                        pass

                if user0_has_files:
                    logging.info(
                        f"ZFS cache detected for: {real_path}, but array files also exist "
                        f"at {user0_path} — hybrid share (likely shareUseCache=yes/prefer). "
                        f"Array-direct conversion remains enabled."
                    )
                else:
                    prefix = real_path.rstrip('/') + '/'
                    zfs_prefixes.add(prefix)
                    logging.info(f"ZFS pool-only detected for: {real_path} (array-direct conversion disabled)")
            else:
                # /mnt/user0 not accessible — cannot verify, assume pool-only
                prefix = real_path.rstrip('/') + '/'
                zfs_prefixes.add(prefix)
                logging.warning(f"ZFS detected for {real_path} but /mnt/user0 not accessible — assuming pool-only")

    if zfs_prefixes:
        set_zfs_prefixes(zfs_prefixes)


def _migrate_exclude_file():
    """One-time migration: rename old exclude file to new name."""
    old_file = CONFIG_DIR / "plexcache_mover_files_to_exclude.txt"
    new_file = CONFIG_DIR / "plexcache_cached_files.txt"

    if old_file.exists() and not new_file.exists():
        try:
            old_file.rename(new_file)
            logging.info(f"Migrated {old_file} -> {new_file}")
        except OSError as e:
            logging.error(f"Failed to migrate exclude file: {e}")
    elif old_file.exists() and new_file.exists():
        try:
            old_file.unlink()
            logging.info(f"Removed legacy exclude file: {old_file}")
        except OSError as e:
            logging.warning(f"Could not remove legacy exclude file: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan - startup and shutdown"""
    # Startup
    _suppress_noisy_loggers()
    print(f"PlexCache-D Web UI starting...")
    print(f"Project root: {PROJECT_ROOT}")

    # Detect ZFS-backed path mappings before any file operations
    _detect_zfs_paths()

    # Migrate old exclude file name before services start reading it
    _migrate_exclude_file()

    # Ensure static directories exist
    STATIC_DIR.mkdir(parents=True, exist_ok=True)
    (STATIC_DIR / "css").mkdir(exist_ok=True)
    (STATIC_DIR / "js").mkdir(exist_ok=True)

    # Prefetch Plex data in background (libraries, users)
    # This prevents lag on first Settings page load
    settings_service = get_settings_service()
    settings_service.prefetch_plex_data()

    # Scan path_mappings for known-bad configurations (issue #136).
    # Read-only: just logs warnings and surfaces them in the dashboard
    # via /api/config-health. Does not auto-modify user settings.
    try:
        health_issues = settings_service.detect_path_mapping_health_issues()
        for issue in health_issues:
            logging.warning(
                "path_mapping health check: %s — %s",
                issue["issue_type"],
                issue["message"],
            )
    except Exception as e:
        logging.warning("path_mapping health check failed (non-fatal): %s", e)

    # Initialize web cache service (loads from disk, starts background refresh)
    print("Initializing web cache service...")
    init_web_cache()

    # Start the scheduler service (includes hourly Plex cache refresh)
    scheduler = get_scheduler_service()
    scheduler.start()

    yield

    # Shutdown
    print("PlexCache-D Web UI shutting down...")
    scheduler.stop()

    # Stop web cache background refresh
    web_cache = get_web_cache_service()
    web_cache.stop_background_refresh()


# Raise Starlette's default max_fields limit for multipart form parsing.
# Maintenance bulk actions (untracked files, orphaned backups) can submit >1000 paths.
import starlette.requests
_original_form = starlette.requests.Request.form
def _form_with_higher_limit(self, *, max_files=1000, max_fields=10000, max_part_size=20*1024*1024):
    return _original_form(self, max_files=max_files, max_fields=max_fields, max_part_size=max_part_size)
starlette.requests.Request.form = _form_with_higher_limit

# Create FastAPI app
app = FastAPI(
    title="PlexCache-D",
    description="Web UI for PlexCache-D media cache management",
    version=__version__,
    lifespan=lifespan
)

# Mount static files
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# Include routers
app.include_router(dashboard.router)
app.include_router(cache.router, prefix="/cache", tags=["cache"])
app.include_router(settings.router, prefix="/settings", tags=["settings"])
app.include_router(operations.router, prefix="/operations", tags=["operations"])
app.include_router(logs.router, prefix="/logs", tags=["logs"])
app.include_router(api.router, prefix="/api", tags=["api"])
app.include_router(maintenance.router, prefix="/maintenance", tags=["maintenance"])
app.include_router(setup.router, tags=["setup"])
app.include_router(auth.router, prefix="/auth", tags=["auth"])


# Middleware to redirect to setup wizard if not configured
@app.middleware("http")
async def setup_redirect_middleware(request: Request, call_next):
    """Redirect to setup wizard if PlexCache is not configured"""
    # Skip redirect for setup pages, static files, and API endpoints
    path = request.url.path
    if (path.startswith("/setup") or
        path.startswith("/static") or
        path.startswith("/api/health")):
        return await call_next(request)

    # Check if setup is complete
    if not setup.is_setup_complete():
        # During setup, also allow path browsing for the wizard
        if path.startswith("/api/browse") or path.startswith("/api/validate-path"):
            return await call_next(request)
        return RedirectResponse(url="/setup", status_code=307)

    return await call_next(request)


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    """Block unauthenticated access when auth is enabled"""
    from web.services.auth_service import get_auth_service

    auth_service = get_auth_service()

    if not auth_service.is_auth_enabled():
        return await call_next(request)

    path = request.url.path

    # Exempt paths
    if (path.startswith("/auth/") or
        path.startswith("/static") or
        path.startswith("/api/health")):
        return await call_next(request)

    # Setup wizard: only exempt from auth before initial setup is complete
    if path.startswith("/setup") and not setup.is_setup_complete():
        return await call_next(request)

    # WebSocket upgrades bypass HTTP middleware (can't return 302/401).
    # Each WebSocket handler must validate auth independently.
    if request.headers.get("upgrade", "").lower() == "websocket":
        return await call_next(request)

    # Check session cookie
    session_token = request.cookies.get("plexcache_session")
    if session_token:
        session = auth_service.validate_session(session_token)
        if session:
            request.state.user = session
            response = await call_next(request)

            # Sliding session: extend expiry when past the halfway point
            if auth_service.refresh_session_if_needed(session_token):
                ttl = auth_service.get_session_ttl(session.remember_me)
                response.set_cookie(
                    key="plexcache_session",
                    value=session_token,
                    max_age=ttl,
                    httponly=True,
                    samesite="lax",
                    secure=str(request.url.scheme) == "https",
                    path="/",
                )

            return response

    # HTMX requests: 401 + HX-Redirect (prevents partial HTML swap)
    # Use HX-Current-URL (the actual page) so login redirects back to the page,
    # not to an API/partial endpoint like /api/operation-banner
    if request.headers.get("HX-Request") == "true":
        current_page = request.headers.get("HX-Current-URL", "")
        next_path = urlparse(current_page).path if current_page else "/"
        response = Response(status_code=401)
        response.headers["HX-Redirect"] = f"/auth/login?next={next_path}"
        return response

    # Normal requests: 302 redirect
    return RedirectResponse(url=f"/auth/login?next={path}", status_code=302)


@app.middleware("http")
async def csrf_origin_check(request: Request, call_next):
    """Block cross-origin mutating requests (CSRF hardening).

    For POST/PUT/DELETE/PATCH, verifies the Origin or Referer header matches
    the Host header. Requests with no origin header (curl, scripts, API tools)
    are allowed — browsers always send Origin on cross-origin requests.
    """
    if request.method in ("GET", "HEAD", "OPTIONS"):
        return await call_next(request)

    origin = request.headers.get("origin")
    referer = request.headers.get("referer")

    # No origin info = non-browser client (curl, scripts) — allow
    if not origin and not referer:
        return await call_next(request)

    # Determine expected host (respect reverse proxy forwarding)
    expected_host = (
        request.headers.get("x-forwarded-host", "").split(",")[0].strip()
        or request.headers.get("host", "")
    )

    # Extract host from Origin (preferred) or Referer
    if origin and origin != "null":
        source_host = urlparse(origin).netloc
    elif referer and referer != "null":
        source_host = urlparse(referer).netloc
    else:
        # Origin: null — sandboxed iframe, cross-origin redirect, not legitimate
        logging.warning(f"CSRF blocked: null origin on {request.method} {request.url.path}")
        return Response("Cross-origin request blocked", status_code=403)

    if source_host != expected_host:
        logging.warning(
            f"CSRF blocked: {request.method} {request.url.path} "
            f"(origin={source_host}, expected={expected_host})"
        )
        return Response("Cross-origin request blocked", status_code=403)

    return await call_next(request)


@app.middleware("http")
async def security_headers_middleware(request: Request, call_next):
    """Add security headers to all responses"""
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "SAMEORIGIN"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline'; "
        "style-src 'self' 'unsafe-inline'; "
        "img-src 'self' data:; "
        "connect-src 'self' ws: wss:; "
        "font-src 'self'"
    )
    if request.url.scheme == "https":
        response.headers["Strict-Transport-Security"] = "max-age=31536000"
    return response


@app.exception_handler(404)
async def not_found_handler(request: Request, exc):
    """Custom 404 page"""
    return templates.TemplateResponse(
        request,
        "errors/404.html",
        {},
        status_code=404
    )


@app.exception_handler(500)
async def server_error_handler(request: Request, exc):
    """Custom 500 page"""
    return templates.TemplateResponse(
        request,
        "errors/500.html",
        {"error": str(exc)},
        status_code=500
    )
