"""
OpenRouter state management with Redis caching.

This module provides centralized state management for OpenRouter API status,
allowing both the API and workers to read and update the state efficiently.
"""

import json
from datetime import datetime, timezone
from typing import Dict, Optional, Any
from enum import Enum

import redis.asyncio as aioredis
from pydantic import BaseModel


class OpenRouterState(str, Enum):
    """OpenRouter service states."""

    ACTIVE = "active"
    API_KEY_MISSING = "api_key_missing"
    API_KEY_INVALID = "api_key_invalid"
    CREDITS_EXHAUSTED = "credits_exhausted"
    RATE_LIMITED = "rate_limited"
    SERVICE_UNAVAILABLE = "service_unavailable"
    ERROR = "error"


class OpenRouterStateData(BaseModel):
    """OpenRouter state data structure."""

    state: OpenRouterState
    message: str
    balance: Optional[float] = None
    usage_today: Optional[float] = None
    usage_month: Optional[float] = None
    last_check: datetime
    last_api_call: Optional[datetime] = None
    error_details: Optional[str] = None
    consecutive_failures: int = 0
    last_success: Optional[datetime] = None
    circuit_breaker_open: bool = False
    rate_limit_reset: Optional[datetime] = None


class OpenRouterStateManager:
    """Manages OpenRouter state in Redis with efficient caching and updates."""

    # Redis keys
    STATE_KEY = "openrouter:state"
    METRICS_KEY = "openrouter:metrics"
    LOCK_KEY = "openrouter:state:lock"

    # Cache settings
    DEFAULT_TTL = 300  # 5 minutes default TTL
    FRESH_THRESHOLD = 60  # Consider data fresh if less than 1 minute old
    STALE_THRESHOLD = 300  # Force refresh if older than 5 minutes
    LOCK_TIMEOUT = 10  # Lock timeout for state updates

    def __init__(self, redis_client: aioredis.Redis):
        self.redis = redis_client

    async def get_state(
        self, force_refresh: bool = False
    ) -> Optional[OpenRouterStateData]:
        """
        Get current OpenRouter state from cache.

        Args:
            force_refresh: If True, ignore cache age and return current state

        Returns:
            OpenRouterStateData if available, None if no cached state
        """
        try:
            state_data = await self.redis.hgetall(self.STATE_KEY)
            if not state_data:
                return None

            # Parse the cached state
            last_check_str = state_data.get("last_check")
            if not last_check_str:
                return None

            last_check = datetime.fromisoformat(last_check_str.replace("Z", "+00:00"))

            # Check if data is stale and needs refresh
            if not force_refresh:
                age_seconds = (datetime.now(timezone.utc) - last_check).total_seconds()
                if age_seconds > self.STALE_THRESHOLD:
                    return None  # Data too old, caller should refresh

            # Parse optional datetime fields
            last_api_call = None
            if state_data.get("last_api_call"):
                try:
                    last_api_call = datetime.fromisoformat(
                        state_data["last_api_call"].replace("Z", "+00:00")
                    )
                except (ValueError, TypeError):
                    pass

            last_success = None
            if state_data.get("last_success"):
                try:
                    last_success = datetime.fromisoformat(
                        state_data["last_success"].replace("Z", "+00:00")
                    )
                except (ValueError, TypeError):
                    pass

            rate_limit_reset = None
            if state_data.get("rate_limit_reset"):
                try:
                    rate_limit_reset = datetime.fromisoformat(
                        state_data["rate_limit_reset"].replace("Z", "+00:00")
                    )
                except (ValueError, TypeError):
                    pass

            return OpenRouterStateData(
                state=OpenRouterState(state_data.get("state", OpenRouterState.ERROR)),
                message=state_data.get("message", "Unknown state"),
                balance=float(state_data["balance"])
                if state_data.get("balance")
                else None,
                usage_today=float(state_data["usage_today"])
                if state_data.get("usage_today")
                else None,
                usage_month=float(state_data["usage_month"])
                if state_data.get("usage_month")
                else None,
                last_check=last_check,
                last_api_call=last_api_call,
                error_details=state_data.get("error_details"),
                consecutive_failures=int(state_data.get("consecutive_failures", 0)),
                last_success=last_success,
                circuit_breaker_open=state_data.get(
                    "circuit_breaker_open", "false"
                ).lower()
                == "true",
                rate_limit_reset=rate_limit_reset,
            )

        except Exception as e:
            print(f"Error getting OpenRouter state: {e}")
            return None

    async def update_state(
        self,
        state: OpenRouterState,
        message: str,
        balance: Optional[float] = None,
        usage_today: Optional[float] = None,
        usage_month: Optional[float] = None,
        error_details: Optional[str] = None,
        is_api_success: bool = False,
        rate_limit_reset: Optional[datetime] = None,
    ) -> bool:
        """
        Update OpenRouter state in Redis.

        Args:
            state: New state
            message: State message
            balance: Account balance
            usage_today: Today's usage
            usage_month: Monthly usage
            error_details: Error details if applicable
            is_api_success: Whether this update represents a successful API call
            rate_limit_reset: When rate limit resets (if rate limited)

        Returns:
            True if update successful, False otherwise
        """
        try:
            # Acquire lock to prevent concurrent updates
            lock_acquired = await self.redis.set(
                self.LOCK_KEY, "locked", nx=True, ex=self.LOCK_TIMEOUT
            )

            if not lock_acquired:
                # Another process is updating, skip this update
                return False

            try:
                current_time = datetime.now(timezone.utc)

                # Get current state for failure counting
                current_state = await self.get_state(force_refresh=True)
                consecutive_failures = 0
                last_success = None

                if current_state:
                    consecutive_failures = current_state.consecutive_failures
                    last_success = current_state.last_success

                # Update failure count and success timestamp
                if is_api_success:
                    consecutive_failures = 0
                    last_success = current_time
                elif state in [
                    OpenRouterState.ERROR,
                    OpenRouterState.SERVICE_UNAVAILABLE,
                    OpenRouterState.RATE_LIMITED,
                ]:
                    consecutive_failures += 1

                # Determine if circuit breaker should be open
                circuit_breaker_open = (
                    consecutive_failures >= 5
                )  # Open after 5 consecutive failures

                # Prepare state data
                state_data = {
                    "state": state.value,
                    "message": message,
                    "last_check": current_time.isoformat(),
                    "last_api_call": current_time.isoformat(),
                    "consecutive_failures": str(consecutive_failures),
                    "circuit_breaker_open": str(circuit_breaker_open).lower(),
                }

                # Add optional fields
                if balance is not None:
                    state_data["balance"] = str(balance)
                if usage_today is not None:
                    state_data["usage_today"] = str(usage_today)
                if usage_month is not None:
                    state_data["usage_month"] = str(usage_month)
                if error_details:
                    state_data["error_details"] = error_details
                if last_success:
                    state_data["last_success"] = last_success.isoformat()
                if rate_limit_reset:
                    state_data["rate_limit_reset"] = rate_limit_reset.isoformat()

                # Update state in Redis with TTL
                await self.redis.hset(self.STATE_KEY, mapping=state_data)
                await self.redis.expire(
                    self.STATE_KEY, self.DEFAULT_TTL * 2
                )  # 10 minutes TTL

                # Update metrics
                await self._update_metrics(state, is_api_success)

                return True

            finally:
                # Release lock
