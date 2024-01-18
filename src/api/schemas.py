# src/api/schemas.py
"""Pydantic schemas for API request/response models."""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class TaskState(str, Enum):
    """Task state enumeration."""

    PENDING = "PENDING"
    ACTIVE = "ACTIVE"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    SCHEDULED = "SCHEDULED"
    DLQ = "DLQ"


class TaskType(str, Enum):
    """Task type enumeration."""

    SUMMARIZE = "summarize"
    PDFXTRACT = "pdfxtract"


class QueueName(str, Enum):
    """Enum for the different task queues."""

    PRIMARY = "primary"
    RETRY = "retry"
    SCHEDULED = "scheduled"
    DLQ = "dlq"


# Centralized mapping from queue names to Redis keys
QUEUE_KEY_MAP = {
    QueueName.PRIMARY: "tasks:pending:primary",
    QueueName.RETRY: "tasks:pending:retry",
    QueueName.SCHEDULED: "tasks:scheduled",
    QueueName.DLQ: "dlq:tasks",
}


class TaskCreate(BaseModel):
    """Schema for creating a new task."""

    content: str = Field(..., description="Text content to summarize", min_length=1)


class TaskResponse(BaseModel):
    """Schema for task creation response."""

    task_id: str = Field(..., description="Unique task identifier")
    state: TaskState = Field(..., description="Current task state")


class TaskDetail(BaseModel):
    """Schema for detailed task information."""

    task_id: str = Field(..., description="Unique task identifier")
    state: TaskState = Field(..., description="Current task state")
    content: str = Field(..., description="Original text content")
    retry_count: int = Field(..., description="Number of retry attempts")
    max_retries: int = Field(..., description="Maximum allowed retries")
    last_error: Optional[str] = Field(None, description="Last error message")
    error_type: Optional[str] = Field(None, description="Type of last error")
    retry_after: Optional[datetime] = Field(None, description="Next retry time")
    created_at: datetime = Field(..., description="Task creation timestamp")
    updated_at: datetime = Field(..., description="Last update timestamp")
    completed_at: Optional[datetime] = Field(None, description="Completion timestamp")
    result: Optional[str] = Field(None, description="Summarization result")
    task_type: Optional[TaskType] = Field(
        TaskType.SUMMARIZE, description="Type of task"
    )
    error_history: List[Dict[str, Any]] = Field(
        default_factory=list, description="History of errors"
    )
    state_history: List[Dict[str, Any]] = Field(
        default_factory=list, description="History of state transitions"
    )


class TaskSummary(BaseModel):
    """Schema for task summary information (without content field)."""

    task_id: str = Field(..., description="Unique task identifier")
    state: TaskState = Field(..., description="Current task state")
    retry_count: int = Field(..., description="Number of retry attempts")
    max_retries: int = Field(..., description="Maximum allowed retries")
    last_error: Optional[str] = Field(None, description="Last error message")
    error_type: Optional[str] = Field(None, description="Type of last error")
    retry_after: Optional[datetime] = Field(None, description="Next retry time")
    created_at: datetime = Field(..., description="Task creation timestamp")
    updated_at: datetime = Field(..., description="Last update timestamp")
    completed_at: Optional[datetime] = Field(None, description="Completion timestamp")
    task_type: Optional[TaskType] = Field(
        TaskType.SUMMARIZE, description="Type of task"
    )
    content_length: Optional[int] = Field(
        None, description="Length of content in characters"
    )
    has_result: bool = Field(False, description="Whether task has a result")
    error_history: List[Dict[str, Any]] = Field(
        default_factory=list, description="History of errors"
    )
    state_history: List[Dict[str, Any]] = Field(
        default_factory=list, description="History of state transitions"
    )


class QueueStatus(BaseModel):
    """Schema for queue status information."""

    queues: Dict[str, int] = Field(..., description="Queue depths by name")
    states: Dict[str, int] = Field(..., description="Task counts by state")
    retry_ratio: float = Field(..., description="Current retry consumption ratio")


class HealthStatus(BaseModel):
    """Schema for health check response."""

    status: str = Field(..., description="Overall health status")
