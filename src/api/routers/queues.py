# src/api/routers/queues.py
"""Queue monitoring API endpoints."""

import asyncio
import json
from typing import List

from fastapi import APIRouter, HTTPException, Query, status
from fastapi.responses import StreamingResponse
import redis.asyncio as aioredis

from schemas import QueueStatus, TaskDetail, QueueName
from services import queue_service
from config import settings
from fastapi import Request

router = APIRouter(prefix="/api/v1/queues", tags=["queues"])


@router.get("/status", response_model=QueueStatus)
async def get_queue_status(request: Request) -> QueueStatus:
    """
    Get comprehensive queue status and statistics.

    Returns:
    - Queue depths for all queues (primary, retry, scheduled, DLQ)
    - Task counts by state
    - Current adaptive retry ratio
    """
    # Try to get the queue service from app state first
    current_queue_service = getattr(request.app.state, "queue_service", None)

    # Fallback to global variable
    if not current_queue_service:
        current_queue_service = queue_service

    if not current_queue_service:
        # Try to get from app state as fallback
        from services import QueueService, RedisService
        from config import settings

        try:
            # Create a temporary service for this request
            temp_redis = RedisService(settings.redis_url)
            await temp_redis.initialize()
            temp_queue_service = QueueService(temp_redis)
            result = await temp_queue_service.get_queue_status()
            await temp_redis.close()
            return result
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Queue service not available: {str(e)}",
            )

    try:
        return await current_queue_service.get_queue_status()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get queue status: {str(e)}",
        )


@router.get("/{queue_name}/tasks", response_model=List[str])
async def get_tasks_in_queue(
    request: Request,
    queue_name: QueueName,
    limit: int = Query(
        default=10, ge=1, le=1000, description="Maximum number of task IDs to return"
    ),
) -> List[str]:
    """
    List task IDs from a specific queue.

    - **queue_name**: The name of the queue to inspect (primary, retry, scheduled, dlq)
    - **limit**: Maximum number of task IDs to return (1-1000, default: 10)

    Returns a list of task IDs currently in the specified queue.
    For the scheduled queue, tasks are ordered by their scheduled execution time.
    For other queues, tasks are in FIFO order.
    """
    # Try to get the queue service from app state first
    current_queue_service = getattr(request.app.state, "queue_service", None)

    # Fallback to global variable
    if not current_queue_service:
        current_queue_service = queue_service

    if not current_queue_service:
        # Try to get from app state as fallback
        from services import QueueService, RedisService
        from config import settings

        try:
            # Create a temporary service for this request
            temp_redis = RedisService(settings.redis_url)
            await temp_redis.initialize()
            temp_queue_service = QueueService(temp_redis)
            result = await temp_queue_service.list_tasks_in_queue(
                queue_name.value, limit
            )
            await temp_redis.close()
            return result
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Queue service not available: {str(e)}",
            )

    try:
        return await current_queue_service.list_tasks_in_queue(queue_name.value, limit)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get tasks from queue '{queue_name.value}': {str(e)}",
        )


@router.get("/dlq", response_model=List[TaskDetail])
async def get_dlq_tasks(
    request: Request,
    limit: int = Query(
        default=100, ge=1, le=1000, description="Maximum number of tasks to return"
    ),
