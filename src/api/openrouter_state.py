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
