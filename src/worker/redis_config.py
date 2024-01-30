"""
Optimized Redis connection configuration for worker tasks.
Specialized for long-running operations and high-throughput task processing.
"""

import asyncio
import logging
from typing import Optional
import redis.asyncio as redis
from redis.asyncio.connection import ConnectionPool
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)


class WorkerRedisConfig:
    """Redis connection pool configuration optimized for worker tasks."""

    # Connection Pool Settings for Workers
    MAX_CONNECTIONS = 30  # Fewer connections than API since workers are more focused
    MIN_IDLE_CONNECTIONS = 3  # Keep minimum connections alive
    MAX_IDLE_TIME = (
        600  # 10 minutes before idle connections are closed (longer for workers)
    )

    # Connection Timeouts (in seconds) - optimized for long-running tasks
    SOCKET_CONNECT_TIMEOUT = 15  # Longer connection timeout for workers
    SOCKET_TIMEOUT = 60  # Longer socket timeout for task processing
    SOCKET_KEEPALIVE = True  # Enable TCP keepalive
    SOCKET_KEEPALIVE_OPTIONS = {
        "TCP_KEEPIDLE": 300,  # Start keepalive after 5 minutes idle
        "TCP_KEEPINTVL": 30,  # Keepalive probe interval
        "TCP_KEEPCNT": 5,  # Number of failed probes before connection is dropped
    }

    # Health Check Settings
    HEALTH_CHECK_INTERVAL = 60  # Check connection health every minute

    # Retry Configuration
    RETRY_ON_ERROR = [
        redis.ConnectionError,
        redis.TimeoutError,
        redis.BusyLoadingError,
    ]
    RETRY_ATTEMPTS = 5  # More retries for workers
    RETRY_BACKOFF = ExponentialBackoff(cap=30, base=2)  # Longer backoff for workers

    # Command Timeouts for Worker Operations
    BLOCKING_TIMEOUT = 300  # 5 minutes for blocking operations like BLPOP
    TASK_UPDATE_TIMEOUT = 30  # 30 seconds for task state updates
    PIPELINE_TIMEOUT = 60  # 1 minute for pipeline operations
    HEARTBEAT_TIMEOUT = 10  # 10 seconds for heartbeat updates


class WorkerRedisConnectionManager:
    """Manages Redis connections optimized for worker task processing."""

    def __init__(self, redis_url: str):
        self.redis_url = redis_url
        self._pool: Optional[ConnectionPool] = None
        self._redis: Optional[redis.Redis] = None
        self._health_check_task: Optional[asyncio.Task] = None

    async def initialize(self) -> None:
        """Initialize the Redis connection pool with worker-optimized settings."""
        if self._pool is not None:
            return

        # Create connection pool with worker-optimized settings
        self._pool = ConnectionPool.from_url(
            self.redis_url,
            # Pool configuration
            max_connections=WorkerRedisConfig.MAX_CONNECTIONS,
            retry_on_error=WorkerRedisConfig.RETRY_ON_ERROR,
            retry=Retry(
                backoff=WorkerRedisConfig.RETRY_BACKOFF,
                retries=WorkerRedisConfig.RETRY_ATTEMPTS,
            ),
            # Connection timeouts
            socket_connect_timeout=WorkerRedisConfig.SOCKET_CONNECT_TIMEOUT,
            socket_timeout=WorkerRedisConfig.SOCKET_TIMEOUT,
            socket_keepalive=WorkerRedisConfig.SOCKET_KEEPALIVE,
            socket_keepalive_options=WorkerRedisConfig.SOCKET_KEEPALIVE_OPTIONS,
            # Encoding
            decode_responses=True,
            encoding="utf-8",
            # Connection lifecycle
            health_check_interval=WorkerRedisConfig.HEALTH_CHECK_INTERVAL,
        )

        # Create Redis client
        self._redis = redis.Redis(connection_pool=self._pool)

        # Start health monitoring
        self._health_check_task = asyncio.create_task(self._health_monitor())

        logger.info(
            f"Worker Redis connection pool initialized: "
            f"max_connections={WorkerRedisConfig.MAX_CONNECTIONS}, "
            f"health_check_interval={WorkerRedisConfig.HEALTH_CHECK_INTERVAL}s"
        )

    async def close(self) -> None:
        """Close the Redis connection pool and cleanup resources."""
        if self._health_check_task:
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass

        if self._redis:
            await self._redis.aclose()
            self._redis = None

        if self._pool:
            await self._pool.aclose()
            self._pool = None

        logger.info("Worker Redis connection pool closed")

    @property
    def redis(self) -> redis.Redis:
