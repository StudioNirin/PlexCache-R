"""Web UI configuration"""

from pathlib import Path

# Paths
WEB_DIR = Path(__file__).parent
TEMPLATES_DIR = WEB_DIR / "templates"
STATIC_DIR = WEB_DIR / "static"

# Project root (parent of web/)
PROJECT_ROOT = WEB_DIR.parent
SETTINGS_FILE = PROJECT_ROOT / "plexcache_settings.json"
LOGS_DIR = PROJECT_ROOT / "logs"
DATA_DIR = PROJECT_ROOT / "data"

# Server defaults
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 5000
