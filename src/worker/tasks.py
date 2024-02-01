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

        await redis_conn.publish("queue-updates", json.dumps(update_data))

    except Exception as e:
        # Don't fail the task update if publishing fails
        print(f"Warning: Failed to publish queue update: {e}")


async def move_to_dlq(
    redis_conn: aioredis.Redis, task_id: str, reason: str, error_type: str = "Unknown"
) -> None:
    """Move a task to the dead-letter queue asynchronously."""
    await update_task_state(
        redis_conn,
        task_id,
        "DLQ",
        last_error=reason,
        error_type=error_type,
        completed_at=datetime.utcnow().isoformat(),
    )
    await redis_conn.lpush("dlq:tasks", task_id)


async def schedule_task_for_retry(
    redis_conn: aioredis.Redis, task_id: str, retry_count: int, exc: Exception
) -> None:
    """Schedule a task for a future retry by adding it to a sorted set."""
    error_type = classify_error(getattr(exc, "status_code", 0), str(exc))
    delay = calculate_retry_delay(retry_count, error_type)
    retry_at_timestamp = time.time() + delay

    await update_task_state(
        redis_conn,
        task_id,
        "SCHEDULED",
        retry_count=retry_count + 1,
        last_error=str(exc),
        error_type=error_type,
        retry_after=datetime.fromtimestamp(retry_at_timestamp).isoformat(),
    )
    await redis_conn.zadd("tasks:scheduled", {task_id: retry_at_timestamp})


async def summarize_text_with_pybreaker(content: str) -> str:
    """Summarize text via OpenRouter protected by centralized state management."""
    if not settings.openrouter_api_key:
        raise PermanentError("OpenRouter API key not configured")

    try:
        # Check centralized OpenRouter state before attempting API call
        redis_conn = await get_async_redis_connection()
        from openrouter_state import OpenRouterStateManager

        state_manager = OpenRouterStateManager(redis_conn)
        should_skip, skip_reason = await state_manager.should_skip_api_call()

        if should_skip:
            # Don't attempt API call - fail immediately for retry later
            raise TransientError(f"OpenRouter service unavailable: {skip_reason}")

        # Load the system prompt (no formatting needed - it's the complete system message)
        system_prompt = load_prompt("summarize")

        # Create the messages payload for the API with system and user roles
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"Summarize this text: {content}"},
        ]

        # Call the generic OpenRouter API function (which includes rate limiting and reporting)
        return await call_openrouter_api(messages)

    except (FileNotFoundError, ValueError) as e:
        # Prompt loading/formatting errors are permanent
        raise PermanentError(f"Prompt error: {str(e)}")
    except Exception as e:
        msg = str(e)
        if "circuit breaker" in msg.lower() or "service unavailable" in msg.lower():
            raise TransientError(f"OpenRouter service protection: {msg}")

        # Simple status code parsing
        code = 0
        if "status_code=" in msg:
            try:
                code_str = msg.split("status_code=")[1].split(" ")[0].strip()
                code = int(code_str)
            except (IndexError, ValueError):
                pass

        if classify_error(code, msg) == "PermanentError":
            raise PermanentError(f"OpenRouter API error: {msg}")
        else:
            exc = TransientError(f"OpenRouter API error: {msg}")
            exc.status_code = code
            raise exc


async def extract_pdf_with_pybreaker(
    pdf_content_b64: str, filename: str, issue_date: str = None
) -> str:
    """Extract articles from PDF pages via OpenRouter protected by centralized state management."""
    if not settings.openrouter_api_key:
        raise PermanentError("OpenRouter API key not configured")

    try:
        # Check centralized OpenRouter state before attempting API call
        redis_conn = await get_async_redis_connection()
        from openrouter_state import OpenRouterStateManager

        state_manager = OpenRouterStateManager(redis_conn)
        should_skip, skip_reason = await state_manager.should_skip_api_call()

        if should_skip:
            # Don't attempt API call - fail immediately for retry later
            raise TransientError(f"OpenRouter service unavailable: {skip_reason}")

        # Decode base64 PDF content
        pdf_bytes = base64.b64decode(pdf_content_b64)

        # Convert PDF to images - this is where poppler dependency errors occur
        try:
            pages = convert_from_bytes(pdf_bytes, dpi=300, fmt="PNG")
        except Exception as pdf_error:
            # Check if this is a poppler dependency error
            error_msg = str(pdf_error).lower()
            if (
                "poppler" in error_msg
                or "pdftoppm" in error_msg
                or "command not found" in error_msg
            ):
                # This is a dependency error - should go directly to DLQ
                raise PermanentError(
                    f"PDF extraction dependency error: {str(pdf_error)}"
                )
            else:
                # Re-raise the original error for other PDF processing issues
                raise

        # Load the PDF extraction prompt
        system_prompt = load_prompt("pdfxtract")

        # Process each page
        all_pages_data = []

        for page_num, page_image in enumerate(pages, 1):
            try:
                # Convert PIL Image to base64 for API
                img_buffer = io.BytesIO()
                page_image.save(img_buffer, format="PNG")
                img_base64 = base64.b64encode(img_buffer.getvalue()).decode("utf-8")

                # Create the messages payload for the API with system and user roles
                user_content = f"Analyze this newspaper page image. Filename: {filename}, Page number: {page_num}"
                if issue_date:
                    user_content += f", Issue date: {issue_date}"

                messages = [
                    {"role": "system", "content": system_prompt},
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": user_content},
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": f"data:image/png;base64,{img_base64}"
                                },
                            },
                        ],
                    },
                ]

                # Call the OpenRouter API for this page (includes rate limiting and reporting)
                page_result = await call_openrouter_api(messages)

                # Parse the JSON response
                try:
                    # Clean the response by removing markdown code blocks if present
                    cleaned_result = page_result.strip()
                    if cleaned_result.startswith("```json"):
                        # Remove opening ```json
                        cleaned_result = cleaned_result[7:]
                    if cleaned_result.startswith("```"):
                        # Remove opening ``` (in case it's just ```)
                        cleaned_result = cleaned_result[3:]
                    if cleaned_result.endswith("```"):
                        # Remove closing ```
                        cleaned_result = cleaned_result[:-3]

                    # Strip any remaining whitespace
                    cleaned_result = cleaned_result.strip()

                    page_data = json.loads(cleaned_result)
                    # Extract the pages array from the response
                    if "pages" in page_data and len(page_data["pages"]) > 0:
                        all_pages_data.extend(page_data["pages"])
                    else:
                        # If no pages array, create a skipped page entry
                        all_pages_data.append(
                            {
                                "page_number": page_num,
                                "status": "skipped",
                                "reason": "No valid page data returned from LLM",
                                "articles": [],
                            }
                        )
                except json.JSONDecodeError as e:
                    # If JSON parsing fails, create a skipped page entry
                    all_pages_data.append(
                        {
                            "page_number": page_num,
                            "status": "skipped",
                            "reason": f"JSON parsing failed: {str(e)}",
                            "articles": [],
                        }
                    )

            except Exception as e:
                # If page processing fails, create a skipped page entry
                all_pages_data.append(
                    {
                        "page_number": page_num,
                        "status": "skipped",
                        "reason": f"Page processing failed: {str(e)}",
                        "articles": [],
                    }
                )

        # Create the final document structure
        final_result = {
            "filename": filename,
            "issue_date": issue_date or "unknown",
            "pages": all_pages_data,
        }

        return json.dumps(final_result, ensure_ascii=False, indent=2)

    except (FileNotFoundError, ValueError) as e:
        # Prompt loading/formatting errors are permanent
        raise PermanentError(f"PDF extraction error: {str(e)}")
    except Exception as e:
        msg = str(e)
        if "circuit breaker" in msg.lower() or "service unavailable" in msg.lower():
            raise TransientError(f"OpenRouter service protection: {msg}")

        # Simple status code parsing
        code = 0
        if "status_code=" in msg:
            try:
                code_str = msg.split("status_code=")[1].split(" ")[0].strip()
                code = int(code_str)
