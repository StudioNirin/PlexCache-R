"""FastAPI dependencies - shared instances and utilities"""

import sys
from pathlib import Path
from functools import lru_cache
from typing import Optional

# Add project root to path for core imports
PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from web.config import SETTINGS_FILE, DATA_DIR, LOGS_DIR


@lru_cache()
def get_settings_path() -> Path:
    """Get path to settings file"""
    return SETTINGS_FILE


@lru_cache()
def get_data_dir() -> Path:
    """Get path to data directory"""
    return DATA_DIR


@lru_cache()
def get_logs_dir() -> Path:
    """Get path to logs directory"""
    return LOGS_DIR


def get_config_manager():
    """Get ConfigManager instance (lazy loaded)"""
    from core.config import ConfigManager
    return ConfigManager(str(SETTINGS_FILE))


def get_timestamp_tracker():
    """Get CacheTimestampTracker instance"""
    from core.file_operations import CacheTimestampTracker
    timestamp_file = DATA_DIR / "timestamps.json"
    return CacheTimestampTracker(str(timestamp_file))


def get_watchlist_tracker():
    """Get WatchlistTracker instance"""
    from core.file_operations import WatchlistTracker
    tracker_file = DATA_DIR / "watchlist_tracker.json"
    return WatchlistTracker(str(tracker_file))


def get_ondeck_tracker():
    """Get OnDeckTracker instance"""
    from core.file_operations import OnDeckTracker
    tracker_file = DATA_DIR / "ondeck_tracker.json"
    return OnDeckTracker(str(tracker_file))


def get_priority_manager():
    """Get CachePriorityManager instance"""
    from core.file_operations import CachePriorityManager
    return CachePriorityManager(
        timestamp_tracker=get_timestamp_tracker(),
        watchlist_tracker=get_watchlist_tracker(),
        ondeck_tracker=get_ondeck_tracker()
    )
