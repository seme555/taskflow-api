# src/worker/tasks.py
"""
Celery tasks for text summarization and task processing.

Refactored for consistent async data access and idiomatic Celery patterns,
suitable for a FastAPI-based monitoring frontend.
"""

import asyncio
import base64
import io
import json
import os
import random
import time
from datetime import datetime

import redis.asyncio as aioredis
from celery import Celery, Task
from celery.worker.control import Panel
from pdf2image import convert_from_bytes

from circuit_breaker import (
    call_openrouter_api,
    get_circuit_breaker_status,
    reset_circuit_breaker,
    open_circuit_breaker,
)
from config import settings
from prompts import load_prompt
from redis_config import get_worker_standard_redis

# --- Custom Exceptions ----------------------------------------------------


class TaskError(Exception):
    """Base exception for task errors."""

    pass


class TransientError(TaskError):
    """Error that should trigger a retry."""

    pass


class PermanentError(TaskError):
    """Error that should not trigger a retry."""

    pass


# --- Constants ------------------------------------------------------------

ERROR_CLASSIFICATIONS = {
    400: PermanentError,  # Bad Request
    401: PermanentError,  # Invalid credentials
    402: TransientError,  # Insufficient credits
    403: PermanentError,  # Forbidden
    404: PermanentError,  # Not Found
    429: TransientError,  # Rate Limited
    500: TransientError,  # Internal Server Error
    503: TransientError,  # Service Unavailable
}

RETRY_SCHEDULES = {
    "InsufficientCredits": [300, 600, 1800],  # 5min, 10min, 30min
    "RateLimitError": [
        120,
        300,
        600,
        1200,
    ],  # 2min, 5min, 10min, 20min - longer delays for rate limits
    "ServiceUnavailable": [5, 10, 30, 60, 120],
    "NetworkTimeout": [2, 5, 10, 30, 60],
    "Default": [5, 15, 60, 300],
}

# --- Celery App Setup -----------------------------------------------------

app = Celery(
    "taskflow-api-worker",
    broker=settings.celery_broker_url,
    backend=None,  # Disable result backend - we use custom task:{task_id} storage
)

# Configure Celery
app.conf.update(
    # Task settings
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    # Worker settings
    worker_concurrency=settings.worker_concurrency,
    worker_prefetch_multiplier=settings.worker_prefetch_multiplier,
    task_soft_time_limit=settings.task_soft_time_limit,
    task_time_limit=settings.task_time_limit,
    # Disable result backend completely
    task_ignore_result=True,
    # Task routing
    task_routes={
        "summarize_text": {"queue": "celery"},
        "process_scheduled_tasks": {"queue": "celery"},
    },
)

# --- Remote-Control Health Commands --------------------------------------


@Panel.register
def get_worker_health(panel, **kwargs):
    """Return health info for this worker (invoked via broadcast)."""
    cb_status = get_circuit_breaker_status()
    return {
        "worker_id": f"worker-{os.getpid()}",
        "circuit_breaker": cb_status,
        "status": "healthy" if cb_status["state"] != "open" else "unhealthy",
        "timestamp": time.time(),
    }


@Panel.register
def reset_worker_circuit_breaker(panel, **kwargs):
    """Reset the circuit breaker on this worker (invoked via broadcast)."""
    try:
        reset_circuit_breaker()
        return {
            "status": "success",
            "message": "Circuit breaker reset.",
            "new_state": get_circuit_breaker_status(),
            "worker_id": f"worker-{os.getpid()}",
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "worker_id": f"worker-{os.getpid()}",
        }


@Panel.register
def open_worker_circuit_breaker(panel, **kwargs):
    """Open the circuit breaker on this worker (invoked via broadcast)."""
    try:
        open_circuit_breaker()
        return {
            "status": "success",
            "message": "Circuit breaker opened.",
            "new_state": get_circuit_breaker_status(),
            "worker_id": f"worker-{os.getpid()}",
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "worker_id": f"worker-{os.getpid()}",
        }


# --- Async Redis and State Management Helpers -----------------------------


async def get_async_redis_connection() -> aioredis.Redis:
    """Get an optimized async Redis connection."""
    try:
        return await get_worker_standard_redis()
    except RuntimeError:
        # Fallback to direct connection if worker Redis not initialized
        return aioredis.from_url(settings.redis_url, decode_responses=True)


def classify_error(status_code: int, error_message: str) -> str:
    """Classify error as transient or permanent."""
    # First check for dependency/environment errors that should go directly to DLQ
    dependency_error_patterns = [
        "poppler installed and in PATH",
        "command not found",
        "no such file or directory",
        "permission denied",
        "module not found",
        "import error",
        "library not found",
        "missing dependency",
        "environment variable not set",
        "configuration error",
        "invalid configuration",
        "database connection failed",
        "redis connection failed",
    ]

    error_lower = error_message.lower()
    for pattern in dependency_error_patterns:
        if pattern in error_lower:
            return "DependencyError"

    # Check for other permanent errors based on content
    permanent_error_patterns = [
        "invalid api key",
        "authentication failed",
        "unauthorized",
        "forbidden",
        "not found",
        "bad request",
        "invalid request",
        "malformed",
        "syntax error",
        "parse error",
        "invalid json",
        "invalid format",
        "unsupported format",
        "file too large",
        "quota exceeded",
        "limit exceeded",
    ]

    for pattern in permanent_error_patterns:
        if pattern in error_lower:
            return "PermanentError"

    # Check HTTP status codes
    if status_code in ERROR_CLASSIFICATIONS:
        err_cls = ERROR_CLASSIFICATIONS[status_code]
        if err_cls is TransientError:
            if status_code == 402:
                return "InsufficientCredits"
            if status_code == 429:
                return "RateLimitError"
            if status_code == 503:
                return "ServiceUnavailable"
            return "NetworkTimeout"
        return "PermanentError"

    # Fallback to default retry behavior for unknown errors
    return "Default"


def calculate_retry_delay(retry_count: int, error_type: str) -> float:
    """Calculate retry delay with exponential backoff and jitter."""
    schedule = RETRY_SCHEDULES.get(error_type, RETRY_SCHEDULES["Default"])
    base_delay = schedule[min(retry_count, len(schedule) - 1)]
    jitter = random.uniform(0, base_delay * 0.1)
    return base_delay + jitter


async def update_task_state(
    redis_conn: aioredis.Redis, task_id: str, state: str, **kwargs
) -> None:
    """Update task state and metadata in Redis asynchronously."""
    current_time = datetime.utcnow().isoformat()
    fields = {"state": state, "updated_at": current_time}
    fields.update(kwargs)

    # Get current state for counter updates
    current_task_data = await redis_conn.hgetall(f"task:{task_id}")
    old_state = current_task_data.get("state") if current_task_data else None

    # Add state-specific timestamps
    if state == "ACTIVE":
        fields["started_at"] = current_time
    elif state == "COMPLETED":
        fields["completed_at"] = current_time
    elif state == "FAILED":
        fields["failed_at"] = current_time
    elif state == "DLQ":
        fields["dlq_at"] = current_time
    elif state == "SCHEDULED":
        fields["scheduled_at"] = current_time

    # Handle error history and retry timestamps
    if "last_error" in kwargs and kwargs["last_error"]:
        # Get existing data
        existing_data = await redis_conn.hgetall(f"task:{task_id}")

        # Handle error history
        error_history = []
        if existing_data.get("error_history"):
            try:
                error_history = json.loads(existing_data["error_history"])
            except (json.JSONDecodeError, TypeError):
                error_history = []

        # Add new error to history
        error_entry = {
            "timestamp": current_time,
            "error": kwargs["last_error"],
            "error_type": kwargs.get("error_type", "Unknown"),
            "retry_count": kwargs.get("retry_count", 0),
            "state_transition": f"{existing_data.get('state', 'UNKNOWN')} -> {state}",
        }
        error_history.append(error_entry)
        fields["error_history"] = json.dumps(error_history)

        # Handle retry timestamps - track each retry attempt
        if state == "SCHEDULED":  # This is a retry being scheduled
            retry_timestamps = []
            if existing_data.get("retry_timestamps"):
                try:
                    retry_timestamps = json.loads(existing_data["retry_timestamps"])
                except (json.JSONDecodeError, TypeError):
                    retry_timestamps = []

            retry_entry = {
                "retry_number": kwargs.get("retry_count", 0),
                "scheduled_at": current_time,
                "retry_after": kwargs.get("retry_after"),
                "error_type": kwargs.get("error_type", "Unknown"),
                "delay_seconds": None,  # Will be calculated when actually retried
            }
            retry_timestamps.append(retry_entry)
            fields["retry_timestamps"] = json.dumps(retry_timestamps)

    # Track when a retry actually starts (PENDING -> ACTIVE transition)
    if state == "ACTIVE":
        existing_data = await redis_conn.hgetall(f"task:{task_id}")
        if existing_data.get("retry_timestamps"):
            try:
                retry_timestamps = json.loads(existing_data["retry_timestamps"])
                # Find the most recent retry entry and update it with actual start time
                if retry_timestamps:
                    latest_retry = retry_timestamps[-1]
                    if "actual_start_at" not in latest_retry:
                        latest_retry["actual_start_at"] = current_time
                        # Calculate actual delay
                        if latest_retry.get("scheduled_at"):
                            scheduled_time = datetime.fromisoformat(
                                latest_retry["scheduled_at"].replace("Z", "+00:00")
                            )
                            actual_time = datetime.fromisoformat(
                                current_time.replace("Z", "+00:00")
                            )
                            delay_seconds = (
                                actual_time - scheduled_time
                            ).total_seconds()
                            latest_retry["delay_seconds"] = delay_seconds
                        fields["retry_timestamps"] = json.dumps(retry_timestamps)
            except (json.JSONDecodeError, TypeError, ValueError):
                pass  # If parsing fails, just continue without updating retry timestamps

    # Serialize complex types
    for key, value in fields.items():
        if (
            isinstance(value, (dict, list)) and key != "error_history"
        ):  # error_history already serialized
            fields[key] = json.dumps(value)
        elif value is not None:
            fields[key] = str(value)

    # Update task data atomically
    async with redis_conn.pipeline(transaction=True) as pipe:
        # Update task data
        await pipe.hset(f"task:{task_id}", mapping=fields)
        await pipe.execute()

    # Publish real-time update
    try:
        # Get current queue depths for the update
        queue_depths = {}
        queue_depths["primary"] = await redis_conn.llen("tasks:pending:primary")
        queue_depths["retry"] = await redis_conn.llen("tasks:pending:retry")
        queue_depths["scheduled"] = await redis_conn.zcard("tasks:scheduled")
        queue_depths["dlq"] = await redis_conn.llen("dlq:tasks")

        # Publish update
        update_data = {
            "type": "task_state_changed",
            "task_id": task_id,
            "old_state": old_state,
            "new_state": state,
            "queue_depths": queue_depths,
            "timestamp": current_time,
        }

