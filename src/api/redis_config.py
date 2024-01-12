"""
Optimized Redis connection pool configuration for long-running async tasks.
Addresses connection pool exhaustion, timeouts, and connection lifecycle management.
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


class OptimizedRedisConfig:
    """Redis connection pool configuration optimized for long-running tasks."""

    # Connection Pool Settings
    MAX_CONNECTIONS = 50  # Increased from default 10
    MIN_IDLE_CONNECTIONS = 5  # Keep minimum connections alive
    MAX_IDLE_TIME = 300  # 5 minutes before idle connections are closed

    # Connection Timeouts (in seconds)
    SOCKET_CONNECT_TIMEOUT = 10  # Time to establish connection
    SOCKET_TIMEOUT = 30  # Socket read/write timeout
    SOCKET_KEEPALIVE = True  # Enable TCP keepalive
    SOCKET_KEEPALIVE_OPTIONS = {
        "TCP_KEEPIDLE": 600,  # Start keepalive after 10 minutes idle
        "TCP_KEEPINTVL": 60,  # Keepalive probe interval
        "TCP_KEEPCNT": 3,  # Number of failed probes before connection is dropped
    }

    # Health Check Settings
    HEALTH_CHECK_INTERVAL = 30  # Check connection health every 30 seconds

    # Retry Configuration
    RETRY_ON_ERROR = [
        redis.ConnectionError,
        redis.TimeoutError,
        redis.BusyLoadingError,
    ]
    RETRY_ATTEMPTS = 3
    RETRY_BACKOFF = ExponentialBackoff(cap=10, base=1)

    # Command Timeouts for Long-Running Operations
    LONG_RUNNING_TIMEOUT = 120  # 2 minutes for operations like BLPOP
    STANDARD_TIMEOUT = 30  # 30 seconds for regular operations
    PIPELINE_TIMEOUT = 60  # 1 minute for pipeline operations


class RedisConnectionManager:
    """Manages Redis connections with optimized pool settings for async tasks."""

    def __init__(self, redis_url: str):
        self.redis_url = redis_url
        self._pool: Optional[ConnectionPool] = None
        self._redis: Optional[redis.Redis] = None
        self._health_check_task: Optional[asyncio.Task] = None

    async def initialize(self) -> None:
        """Initialize the Redis connection pool with optimized settings."""
        if self._pool is not None:
            return

        # Create connection pool with optimized settings
        self._pool = ConnectionPool.from_url(
            self.redis_url,
            # Pool configuration
            max_connections=OptimizedRedisConfig.MAX_CONNECTIONS,
            retry_on_error=OptimizedRedisConfig.RETRY_ON_ERROR,
            retry=Retry(
                backoff=OptimizedRedisConfig.RETRY_BACKOFF,
                retries=OptimizedRedisConfig.RETRY_ATTEMPTS,
            ),
            # Connection timeouts
            socket_connect_timeout=OptimizedRedisConfig.SOCKET_CONNECT_TIMEOUT,
            socket_timeout=OptimizedRedisConfig.SOCKET_TIMEOUT,
            socket_keepalive=OptimizedRedisConfig.SOCKET_KEEPALIVE,
            socket_keepalive_options=OptimizedRedisConfig.SOCKET_KEEPALIVE_OPTIONS,
            # Encoding
            decode_responses=True,
            encoding="utf-8",
            # Connection lifecycle
            health_check_interval=OptimizedRedisConfig.HEALTH_CHECK_INTERVAL,
        )

        # Create Redis client
        self._redis = redis.Redis(connection_pool=self._pool)

        # Start health monitoring
        self._health_check_task = asyncio.create_task(self._health_monitor())

        logger.info(
            f"Redis connection pool initialized: "
            f"max_connections={OptimizedRedisConfig.MAX_CONNECTIONS}, "
            f"health_check_interval={OptimizedRedisConfig.HEALTH_CHECK_INTERVAL}s"
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

        logger.info("Redis connection pool closed")

    @property
    def redis(self) -> redis.Redis:
        """Get the Redis client instance."""
        if self._redis is None:
            raise RuntimeError(
