"""Web UI configuration"""

import json
import os
from datetime import datetime
from pathlib import Path

from fastapi.templating import Jinja2Templates

from web import __version__
from core.system_utils import SystemDetector

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

# Product version (sourced from core/__init__.py)
from core import __version__ as _core_version
PLEXCACHE_PRODUCT_VERSION = _core_version

# Server defaults
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 5000

# Shared Jinja2 templates instance (all routers should import this)
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# Docker image tag - shows badge in sidebar when not "latest"
IMAGE_TAG = os.environ.get("IMAGE_TAG", "latest")
templates.env.globals["image_tag"] = IMAGE_TAG


def _parse_tag_label(tag: str) -> str:
    """Derive a short display label from a Docker image tag.

    Examples: 'dev' → 'DEV', 'v3.1.0-beta1' → 'BETA 1', 'latest' → ''
    """
    if not tag or tag == "latest":
        return ""
    # Pre-release suffix after version: v3.1.0-beta1 → beta1 → BETA 1
    if tag.startswith("v") and "-" in tag:
        suffix = tag.split("-", 1)[1]  # "beta1", "rc2", etc.
        # Insert space before trailing digits: beta1 → BETA 1
        import re
        label = re.sub(r'(\D+)(\d+)$', r'\1 \2', suffix)
        return label.upper()
    return tag.upper()


templates.env.globals["tag_label"] = _parse_tag_label(IMAGE_TAG)

# Platform detection globals (available in all templates)
_detector = SystemDetector()
templates.env.globals["is_unraid"] = _detector.is_unraid
templates.env.globals["is_docker"] = IS_DOCKER
templates.env.globals["web_version"] = __version__
templates.env.globals["product_version"] = PLEXCACHE_PRODUCT_VERSION


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


def format_datetime(value, include_seconds=False):
    """Jinja2 filter: format an ISO string or datetime with user's time_format preference."""
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value)
        except (ValueError, TypeError):
            return value
    if not isinstance(value, datetime):
        return value
    fmt = get_time_format()
    if fmt == "12h":
        time_part = value.strftime("%-I:%M:%S %p") if include_seconds else value.strftime("%-I:%M %p")
    else:
        time_part = value.strftime("%H:%M:%S") if include_seconds else value.strftime("%H:%M")
    return f"{value.strftime('%Y-%m-%d')} {time_part}"


templates.env.filters["format_datetime"] = format_datetime


def truncate_filename(value, length=55, end='...'):
    """Truncate a filename while preserving the file extension.

    Example: 'Serenity (2005) - [REMUX-2160P][DTS-X 7.1][HEVC]-FGT.mkv'
           → 'Serenity (2005) - [REMUX-2160P][DTS-X 7...mkv'
    """
    if not isinstance(value, str) or len(value) <= length:
        return value
    dot_pos = value.rfind('.')
    if dot_pos == -1 or dot_pos == 0:
        # No extension — fall back to plain truncation
        return value[:length - len(end)] + end
    ext = value[dot_pos + 1:]  # e.g. "mkv"
    suffix = end + ext          # e.g. "...mkv"
    if length <= len(suffix):
        return value[:length]
    return value[:length - len(suffix)] + suffix


templates.env.filters["truncate_filename"] = truncate_filename
