"""Pydantic models for operations"""

from typing import Optional, List
from datetime import datetime
from pydantic import BaseModel
from enum import Enum


class OperationStatus(str, Enum):
    """Operation status enum"""
    IDLE = "idle"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class RunRequestModel(BaseModel):
    """Request to start a cache operation"""
    dry_run: bool = False
    verbose: bool = False


class OperationStatusModel(BaseModel):
    """Current operation status"""
    status: OperationStatus = OperationStatus.IDLE
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    cached_count: int = 0
    cached_bytes: int = 0
    restored_count: int = 0
    restored_bytes: int = 0
    errors: List[str] = []
    current_file: Optional[str] = None
    progress_percent: Optional[float] = None


class LogEntryModel(BaseModel):
    """Single log entry"""
    timestamp: datetime
    level: str
    message: str
    logger: str = ""
