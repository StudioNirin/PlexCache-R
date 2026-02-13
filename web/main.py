"""PlexCache-R Web UI - FastAPI Application"""

import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, RedirectResponse

from web.config import templates, STATIC_DIR, PROJECT_ROOT, CONFIG_DIR, SETTINGS_FILE
from web.routers import dashboard, cache, settings, operations, logs, api, maintenance, setup
from web.services import get_scheduler_service, get_settings_service
from web.services.web_cache import init_web_cache, get_web_cache_service
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
            prefix = real_path.rstrip('/') + '/'
            zfs_prefixes.add(prefix)
            logging.info(f"ZFS pool detected for: {real_path} (array-direct conversion disabled)")

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
    print(f"PlexCache-R Web UI starting...")
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

    # Initialize web cache service (loads from disk, starts background refresh)
    print("Initializing web cache service...")
    init_web_cache()

    # Start the scheduler service (includes hourly Plex cache refresh)
    scheduler = get_scheduler_service()
    scheduler.start()

    yield

    # Shutdown
    print("PlexCache-R Web UI shutting down...")
    scheduler.stop()

    # Stop web cache background refresh
    web_cache = get_web_cache_service()
    web_cache.stop_background_refresh()


# Create FastAPI app
app = FastAPI(
    title="PlexCache-R",
    description="Web UI for PlexCache-R media cache management",
    version="0.2.0",
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


# Middleware to redirect to setup wizard if not configured
@app.middleware("http")
async def setup_redirect_middleware(request: Request, call_next):
    """Redirect to setup wizard if PlexCache is not configured"""
    # Skip redirect for setup pages, static files, and API endpoints
    path = request.url.path
    if (path.startswith("/setup") or
        path.startswith("/static") or
        path.startswith("/api/health") or
        path.startswith("/api/status")):
        return await call_next(request)

    # Check if setup is complete
    if not setup.is_setup_complete():
        return RedirectResponse(url="/setup", status_code=307)

    return await call_next(request)


@app.exception_handler(404)
async def not_found_handler(request: Request, exc):
    """Custom 404 page"""
    return templates.TemplateResponse(
        "errors/404.html",
        {"request": request},
        status_code=404
    )


@app.exception_handler(500)
async def server_error_handler(request: Request, exc):
    """Custom 500 page"""
    return templates.TemplateResponse(
        "errors/500.html",
        {"request": request, "error": str(exc)},
        status_code=500
    )
