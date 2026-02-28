"""Pydantic models for settings"""

from typing import Optional, List
from pydantic import BaseModel, Field


class PathMappingModel(BaseModel):
    """Single path mapping configuration"""
    name: str
    plex_path: str
    real_path: str
    cache_path: Optional[str] = None
    cacheable: bool = True
    enabled: bool = True
    section_id: Optional[int] = None


class PlexSettingsModel(BaseModel):
    """Plex server settings"""
    plex_url: str
    plex_token: str
    valid_sections: List[int] = Field(default_factory=list)
    number_episodes: int = 3
    days_to_monitor: int = 7
    users_toggle: bool = False
    skip_ondeck: List[str] = Field(default_factory=list)
    skip_watchlist: List[str] = Field(default_factory=list)


class CacheSettingsModel(BaseModel):
    """Cache behavior settings"""
    watchlist_toggle: bool = True
    watchlist_episodes: int = 3
    watched_move: bool = True
    remote_watchlist_toggle: bool = False
    remote_watchlist_rss_url: str = ""
    cache_retention_hours: int = 12
    watchlist_retention_days: float = 14.0
    ondeck_retention_days: float = 0
    cache_limit: str = "250GB"
    cache_eviction_mode: str = "smart"
    cache_eviction_threshold_percent: int = 90
    eviction_min_priority: int = 60


class NotificationSettingsModel(BaseModel):
    """Notification settings"""
    notification_type: str = "system"
    unraid_level: str = "summary"
    webhook_level: str = "summary"
    webhook_url: str = ""


class PerformanceSettingsModel(BaseModel):
    """Performance settings"""
    max_concurrent_moves_array: int = 2
    max_concurrent_moves_cache: int = 5
    retry_limit: int = 3
    delay: int = 1
    permissions: int = 777


class LoggingSettingsModel(BaseModel):
    """Logging settings"""
    max_log_files: int = 24
    keep_error_logs_days: int = 7


class AllSettingsModel(BaseModel):
    """Complete settings model"""
    plex_url: str
    plex_token: str
    valid_sections: List[int] = Field(default_factory=list)
    path_mappings: List[PathMappingModel] = Field(default_factory=list)
    # Include all other fields as needed
