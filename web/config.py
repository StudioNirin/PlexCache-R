"""Web UI configuration"""

import json
import os
from datetime import datetime
from pathlib import Path

from fastapi.templating import Jinja2Templates

# Paths
WEB_DIR = Path(__file__).parent
TEMPLATES_DIR = WEB_DIR / "templates"
STATIC_DIR = WEB_DIR / "static"

# Project root (parent of web/)
PROJECT_ROOT = WEB_DIR.parent

# Config directory - /config in Docker, project root otherwise
# Docker containers have /.dockerenv or /run/.containerenv
IS_DOCKER = os.path.exists("/.dockerenv") or os.path.exists("/run/.containerenv")
CONFIG_DIR = Path("/config") if IS_DOCKER else PROJECT_ROOT

SETTINGS_FILE = CONFIG_DIR / "plexcache_settings.json" if IS_DOCKER else PROJECT_ROOT / "plexcache_settings.json"
LOGS_DIR = CONFIG_DIR / "logs" if IS_DOCKER else PROJECT_ROOT / "logs"
DATA_DIR = CONFIG_DIR / "data" if IS_DOCKER else PROJECT_ROOT / "data"

# Server defaults
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 5000

# Shared Jinja2 templates instance (all routers should import this)
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# Docker image tag - shows badge in sidebar when not "latest"
IMAGE_TAG = os.environ.get("IMAGE_TAG", "latest")
templates.env.globals["image_tag"] = IMAGE_TAG


def get_time_format() -> str:
    """Read time_format from settings JSON. Returns '12h' or '24h' (default)."""
    try:
        if SETTINGS_FILE.exists():
            with open(SETTINGS_FILE, 'r', encoding='utf-8') as f:
                settings = json.load(f)
            fmt = settings.get("time_format", "24h")
            if fmt in ("12h", "24h"):
                return fmt
    except (json.JSONDecodeError, IOError):
        pass
    return "24h"


def format_time(value, include_seconds=True):
    """Jinja2 filter: format a datetime based on user's time_format preference."""
    if not isinstance(value, datetime):
        return value
    fmt = get_time_format()
    if fmt == "12h":
        return value.strftime("%-I:%M:%S %p") if include_seconds else value.strftime("%-I:%M %p")
    return value.strftime("%H:%M:%S") if include_seconds else value.strftime("%H:%M")


templates.env.filters["format_time"] = format_time
