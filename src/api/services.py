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
