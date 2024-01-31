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
