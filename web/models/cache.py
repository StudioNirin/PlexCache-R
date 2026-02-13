"""Pydantic models for cache data"""

from typing import Optional
from datetime import datetime
from pydantic import BaseModel


class CachedFileModel(BaseModel):
    """Cached file information"""
    path: str
    filename: str
    size_bytes: int
    size_display: str
    priority_score: int
    source: str  # "ondeck" or "watchlist"
    cached_at: Optional[datetime] = None
    cache_age_hours: float = 0.0
    user_count: int = 1
    retention_remaining_hours: Optional[float] = None


class CacheStatsModel(BaseModel):
    """Cache statistics"""
    total_files: int = 0
    total_size_bytes: int = 0
    total_size_display: str = "0 B"
    cache_limit_bytes: Optional[int] = None
    cache_limit_display: str = ""
    usage_percent: float = 0.0
    ondeck_count: int = 0
    watchlist_count: int = 0
    evictable_count: int = 0
    evictable_size_bytes: int = 0


class DashboardStatsModel(BaseModel):
    """Dashboard statistics"""
    cache_stats: CacheStatsModel
    last_run: Optional[datetime] = None
    last_run_display: str = "Never"
    is_running: bool = False
    plex_connected: bool = False
