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
