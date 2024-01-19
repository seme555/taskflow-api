# src/api/services.py
"""Service layer for task and queue management."""

import json
import math
from datetime import datetime
from typing import Dict, List, Optional, Any
from uuid import uuid4

from celery import Celery

from config import settings
from redis_config import get_standard_redis, initialize_redis, close_redis
from redis_config_simple import (
    initialize_simple_redis,
    close_simple_redis,
    get_simple_redis,
)
from schemas import (
    QueueStatus,
    TaskDetail,
    TaskState,
    QueueName,
    QUEUE_KEY_MAP,
    TaskListResponse,
    TaskSummaryListResponse,
    TaskSummary,
    TaskType,
)


class RedisService:
    """Redis service for task and queue management with optimized connection pool."""

    def __init__(self, redis_url: str):
        self.redis_url = redis_url
        self._manager: Any = None
        self._simple_manager: Any = None
        self.redis: Any = None

    async def initialize(self) -> None:
        """Initialize the optimized Redis connection manager with fallback."""
        if self._manager is None and self._simple_manager is None:
            try:
                # Try optimized Redis configuration first
                self._manager = await initialize_redis(self.redis_url)
                self.redis = await get_standard_redis()
                print("Using optimized Redis connection pool")
                # Test the connection
                await self.redis.ping()
                print("Optimized Redis connection successful")
            except Exception as e:
                print(f"Optimized Redis failed ({e}), falling back to simple Redis")
                # Clean up failed optimized connection
                if self._manager:
                    try:
                        await close_redis()
                    except Exception:
                        pass
                    self._manager = None

                # Fall back to simple Redis configuration
                try:
                    self._simple_manager = await initialize_simple_redis(self.redis_url)
                    self.redis = await get_simple_redis()
                    print("Using simple Redis connection")
                    # Test the simple connection
                    await self.redis.ping()
                    print("Simple Redis connection successful")
                except Exception as e2:
                    print(f"Simple Redis also failed: {e2}")
                    # Create a basic fallback connection
                    import redis.asyncio as redis

                    self.redis = redis.from_url(self.redis_url, decode_responses=True)
                    print("Using basic Redis connection as last resort")

    async def close(self) -> None:
        """Close Redis connection."""
        if self._manager:
            await close_redis()
            self._manager = None
        if self._simple_manager:
            await close_simple_redis()
            self._simple_manager = None
        self.redis = None

    async def ping(self) -> bool:
        """Check Redis connectivity."""
        try:
            if self.redis is None:
                await self.initialize()
            result = await self.redis.ping()
            return result is True or result == b"PONG" or result == "PONG"
        except Exception as e:
            print(f"Redis ping failed: {e}")
            return False

    async def get_pool_stats(self) -> Dict[str, Any]:
        """Get Redis connection pool statistics."""
        if self._manager:
            stats = await self._manager.get_pool_stats()
            return dict(stats) if stats else {"status": "manager_no_stats"}
        elif self._simple_manager:
            stats = await self._simple_manager.get_pool_stats()
            return dict(stats) if stats else {"status": "simple_manager_no_stats"}
        return {"status": "not_initialized"}

    async def publish_queue_update(self, update_data: Dict) -> None:
        """Publish queue update to Redis pub/sub channel."""
        await self.redis.publish("queue-updates", json.dumps(update_data))


class TaskService:
    """Service for managing tasks."""

    def __init__(self, redis_service: RedisService):
        self.redis = redis_service.redis
        self.redis_service = redis_service

    async def create_task(
        self,
        content: str,
        task_type: TaskType = TaskType.SUMMARIZE,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Create a new task and queue it for processing."""
        task_id = str(uuid4())
        now = datetime.utcnow()

        task_data = {
            "task_id": task_id,
            "content": content,
            "task_type": task_type.value,
            "state": TaskState.PENDING.value,
            "retry_count": 0,
            "max_retries": settings.max_retries,
            "last_error": "",
            "error_type": "",
            "retry_after": "",
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
            "completed_at": "",
            "result": "",
            "error_history": json.dumps([]),
            "state_history": json.dumps(
                [{"state": TaskState.PENDING.value, "timestamp": now.isoformat()}]
            ),
        }

        # Add metadata if provided
        if metadata:
            task_data["metadata"] = json.dumps(metadata)

        # Use Redis transaction to ensure atomicity
        async with self.redis.pipeline(transaction=True) as pipe:
            # Store task metadata and queue in primary queue atomically
            await pipe.hset(f"task:{task_id}", mapping=task_data)
            await pipe.lpush(QUEUE_KEY_MAP[QueueName.PRIMARY], task_id)
            await pipe.execute()

        # Publish queue update for real-time UI
        primary_depth = await self.redis.llen(QUEUE_KEY_MAP[QueueName.PRIMARY])

        await self.redis_service.publish_queue_update(
            {
                "type": "task_created",
                "task_id": task_id,
                "queue_depths": {"primary": primary_depth},
                "timestamp": now.isoformat(),
            }
        )

        return task_id

    async def get_task(self, task_id: str) -> Optional[TaskDetail]:
        """Get task details by ID."""
        task_data = await self.redis.hgetall(f"task:{task_id}")
        if not task_data:
            return None

        # Parse JSON fields
        error_history = json.loads(task_data.get("error_history", "[]"))
        state_history = json.loads(task_data.get("state_history", "[]"))

        # Convert string dates back to datetime objects
        created_at = datetime.fromisoformat(task_data["created_at"])
        updated_at = datetime.fromisoformat(task_data["updated_at"])
        completed_at = (
            datetime.fromisoformat(task_data["completed_at"])
            if task_data.get("completed_at")
            else None
        )
        retry_after = (
            datetime.fromisoformat(task_data["retry_after"])
            if task_data.get("retry_after")
            else None
        )

        # Get task type, defaulting to SUMMARIZE for backward compatibility
        task_type_str = task_data.get("task_type", TaskType.SUMMARIZE.value)
        try:
            task_type = TaskType(task_type_str)
        except ValueError:
            task_type = TaskType.SUMMARIZE

        return TaskDetail(
            task_id=task_data["task_id"],
            state=TaskState(task_data["state"]),
            content=task_data["content"],
            retry_count=int(task_data["retry_count"]),
            max_retries=int(task_data["max_retries"]),
            last_error=task_data.get("last_error") or None,
            error_type=task_data.get("error_type") or None,
            retry_after=retry_after,
            created_at=created_at,
            updated_at=updated_at,
            completed_at=completed_at,
            result=task_data.get("result") or None,
            task_type=task_type,
            error_history=error_history,
            state_history=state_history,
        )

    async def retry_task(self, task_id: str, reset_retry_count: bool = False) -> bool:
        """Manually retry a failed task."""
        task_data = await self.redis.hgetall(f"task:{task_id}")
        if not task_data:
            return False

        current_state = task_data.get("state")
        if current_state not in [TaskState.FAILED.value, TaskState.DLQ.value]:
            return False

        now = datetime.utcnow()
        state_history = json.loads(task_data.get("state_history", "[]"))
        state_history.append(
            {"state": TaskState.PENDING.value, "timestamp": now.isoformat()}
        )

        # Update task state
        updates = {
            "state": TaskState.PENDING.value,
            "updated_at": now.isoformat(),
            "state_history": json.dumps(state_history),
        }

        if reset_retry_count:
            updates["retry_count"] = "0"

        await self.redis.hset(f"task:{task_id}", mapping=updates)

        # Queue in retry queue
        await self.redis.lpush(QUEUE_KEY_MAP[QueueName.RETRY], task_id)

        return True

    async def requeue_orphaned_tasks(self) -> Dict[str, Any]:
        """Find and re-queue orphaned tasks."""
        found_count = 0
        requeued_count = 0
        errors = []

        try:
            # Get all task IDs from all queues to check what's already queued
            queued_task_ids = set()

            # Check primary queue
            primary_tasks = await self.redis.lrange(
                QUEUE_KEY_MAP[QueueName.PRIMARY], 0, -1
            )
            queued_task_ids.update(primary_tasks)

            # Check retry queue
            retry_tasks = await self.redis.lrange(QUEUE_KEY_MAP[QueueName.RETRY], 0, -1)
            queued_task_ids.update(retry_tasks)

            # Check scheduled queue (sorted set)
            scheduled_tasks = await self.redis.zrange(
                QUEUE_KEY_MAP[QueueName.SCHEDULED], 0, -1
            )
            queued_task_ids.update(scheduled_tasks)

            # Check DLQ
            dlq_tasks = await self.redis.lrange(QUEUE_KEY_MAP[QueueName.DLQ], 0, -1)
            queued_task_ids.update(dlq_tasks)

            # Scan all task keys to find orphaned ones
            async for key in self.redis.scan_iter("task:*"):
                task_id = key.split(":", 1)[1]  # Extract task_id from "task:uuid"

                # Get task state
                task_state = await self.redis.hget(key, "state")

                if (
                    task_state == TaskState.PENDING.value
                    and task_id not in queued_task_ids
                ):
                    found_count += 1

                    try:
                        # Re-queue the orphaned task in primary queue
                        await self.redis.lpush(
                            QUEUE_KEY_MAP[QueueName.PRIMARY], task_id
                        )

                        # Update the task's updated_at timestamp
                        await self.redis.hset(
                            key, "updated_at", datetime.utcnow().isoformat()
                        )

                        requeued_count += 1

                    except Exception as e:
                        error_msg = f"Failed to requeue task {task_id}: {str(e)}"
                        errors.append(error_msg)

            return {
                "found": found_count,
                "requeued": requeued_count,
                "errors": errors,
                "message": f"Found {found_count} orphaned tasks, successfully requeued {requeued_count}",
            }

        except Exception as e:
            errors.append(f"Error during orphaned task scan: {str(e)}")
            return {
                "found": found_count,
                "requeued": requeued_count,
                "errors": errors,
                "message": f"Partial completion due to error: {str(e)}",
            }

    async def delete_task(self, task_id: str) -> bool:
        """Delete a task and all its associated data from the system."""
        try:
            # First, get the task's current state to update counters
            task_data = await self.redis.hgetall(f"task:{task_id}")
            if not task_data:
                return False  # Task doesn't exist

            # Use Redis transaction to ensure atomicity
            async with self.redis.pipeline(transaction=True) as pipe:
                # Delete the main task hash
                await pipe.delete(f"task:{task_id}")

                # Delete any corresponding dead-letter queue hash
                await pipe.delete(f"dlq:task:{task_id}")

                # Remove the task_id from all potential list-based queues
                await pipe.lrem(QUEUE_KEY_MAP[QueueName.PRIMARY], 0, task_id)
                await pipe.lrem(QUEUE_KEY_MAP[QueueName.RETRY], 0, task_id)
                await pipe.lrem(QUEUE_KEY_MAP[QueueName.DLQ], 0, task_id)

                # Remove the task_id from the scheduled sorted set
                await pipe.zrem(QUEUE_KEY_MAP[QueueName.SCHEDULED], task_id)

                await pipe.execute()

            return True
        except Exception:
            # If there's an error getting task data but we know the task exists,
            # try to delete it anyway without updating counters
            try:
                # Check if task exists in Redis at all
                exists = await self.redis.exists(f"task:{task_id}")
                if not exists:
                    return False

                # Force delete without counter updates for corrupted tasks
                async with self.redis.pipeline(transaction=True) as pipe:
                    await pipe.delete(f"task:{task_id}")
                    await pipe.delete(f"dlq:task:{task_id}")
                    await pipe.lrem(QUEUE_KEY_MAP[QueueName.PRIMARY], 0, task_id)
                    await pipe.lrem(QUEUE_KEY_MAP[QueueName.RETRY], 0, task_id)
