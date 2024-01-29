"""
Worker-side OpenRouter state reporting.

This module allows workers to report OpenRouter API errors to the centralized
state management system, enabling efficient coordination between workers and API.
"""

import os
from typing import Optional
from datetime import datetime, timezone

import redis.asyncio as aioredis
from redis_config import get_worker_standard_redis


class WorkerOpenRouterReporter:
    """Reports OpenRouter errors from workers to the centralized state system."""

    def __init__(self, redis_client: Optional[aioredis.Redis] = None):
        self.redis = redis_client
        self._worker_id = None

    async def _get_redis(self) -> aioredis.Redis:
        """Get Redis connection, initializing if needed."""
        if self.redis is None:
            try:
                self.redis = await get_worker_standard_redis()
            except Exception:
                # Fallback to basic connection
                from config import settings

                self.redis = aioredis.from_url(
                    settings.redis_url, decode_responses=True
                )
        return self.redis

    def _get_worker_id(self) -> str:
        """Get worker ID for reporting."""
        if self._worker_id is None:
            try:
                # Try to get container ID from cgroup
                with open("/proc/self/cgroup", "r") as f:
                    for line in f:
                        if "docker" in line:
                            parts = line.strip().split("/")
                            for part in reversed(parts):
                                if len(part) == 64 and part.isalnum():
                                    self._worker_id = f"worker-{part[:12]}"
                                    return self._worker_id

                # Fallback to hostname + PID
                hostname = os.uname().nodename
                pid = os.getpid()
                self._worker_id = f"worker-{hostname}-{pid}"

            except Exception:
                self._worker_id = f"worker-unknown-{os.getpid()}"

        return self._worker_id

    async def report_api_error(
        self,
        error_message: str,
        status_code: Optional[int] = None,
        error_type: Optional[str] = None,
    ) -> bool:
        """
        Report an OpenRouter API error to the state management system.

        Args:
            error_message: The error message from the API call
            status_code: HTTP status code if available
            error_type: Specific error type if known

        Returns:
            True if report was successful, False otherwise
        """
        try:
            redis_client = await self._get_redis()
            worker_id = self._get_worker_id()

            # Classify the error if not provided
            if error_type is None:
                error_type = self._classify_error(error_message, status_code)

            # Use the same state management keys as the API
            STATE_KEY = "openrouter:state"
            LOCK_KEY = "openrouter:state:lock"
            LOCK_TIMEOUT = 10

            # Try to acquire lock for state update
            lock_acquired = await redis_client.set(
                LOCK_KEY, f"worker-{worker_id}", nx=True, ex=LOCK_TIMEOUT
            )

            if not lock_acquired:
                # Another process is updating, just log the error for now
                await self._log_worker_error(
                    redis_client, worker_id, error_type, error_message, status_code
                )
                return False

            try:
                current_time = datetime.now(timezone.utc)

                # Get current state
                current_state_data = await redis_client.hgetall(STATE_KEY)
                consecutive_failures = 0

                if current_state_data:
                    consecutive_failures = int(
                        current_state_data.get("consecutive_failures", 0)
                    )

                # Increment failure count for error states
                if error_type in [
                    "api_key_invalid",
                    "credits_exhausted",
                    "rate_limited",
                    "service_unavailable",
                    "timeout",
                    "network_error",
                ]:
                    consecutive_failures += 1

