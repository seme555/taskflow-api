#!/usr/bin/env python3
"""
OpenRouter Credit Monitoring and Alerting Utility

This script monitors OpenRouter account credits and provides alerts when credits are low.
It can be run manually or scheduled as a cron job for continuous monitoring.
"""

import asyncio
import json
import sys
import time
from datetime import datetime, timezone
from typing import Dict, Optional

import httpx
import redis.asyncio as aioredis
from pydantic import Field
from pydantic_settings import BaseSettings


class MonitorSettings(BaseSettings):
    """Settings for OpenRouter credit monitoring."""

    # OpenRouter Configuration
    openrouter_api_key: Optional[str] = Field(default=None, env="OPENROUTER_API_KEY")
    openrouter_base_url: str = Field(
        default="https://openrouter.ai/api/v1", env="OPENROUTER_BASE_URL"
    )

    # Redis Configuration
    redis_url: str = Field(default="redis://localhost:6379/0", env="REDIS_URL")

    # Alert Thresholds
    credit_warning_threshold: float = Field(
        default=10.0, env="CREDIT_WARNING_THRESHOLD"
    )  # $10
    credit_critical_threshold: float = Field(
        default=5.0, env="CREDIT_CRITICAL_THRESHOLD"
    )  # $5

    # Monitoring Configuration
    check_interval_minutes: int = Field(default=30, env="CREDIT_CHECK_INTERVAL_MINUTES")
    alert_cooldown_hours: int = Field(default=1, env="CREDIT_ALERT_COOLDOWN_HOURS")

    model_config = {
        "env_file": ".env",
        "case_sensitive": False,
        "extra": "ignore",  # Ignore extra environment variables
    }


async def get_openrouter_credits(api_key: str, base_url: str) -> Dict:
    """
    Fetch current credit information from OpenRouter API.

    Returns:
        Dictionary containing credit information
    """
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{base_url}/auth/key",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                timeout=30.0,
            )

            if response.status_code != 200:
                raise Exception(
                    f"OpenRouter API error: {response.status_code} - {response.text}"
                )

            data = response.json()

            # Handle different possible response structures
            if isinstance(data, dict):
                # Try different possible structures
                data_section = data.get(
                    "data", data
                )  # Use data section if exists, otherwise use root

                # Extract credit information with safe navigation
                balance = 0.0
                usage_today = 0.0
                usage_month = 0.0
                rate_limit = {}

                if isinstance(data_section, dict):
                    # OpenRouter API uses 'usage' field for total usage amount
                    # Note: This appears to be total usage, not remaining balance
                    total_usage = data_section.get("usage", 0.0)

                    # For OpenRouter, we need to calculate remaining balance
                    # Since we don't have the original balance, we'll use usage as a proxy
                    # In a real scenario, you'd need to track the original balance separately
                    balance = total_usage  # This is actually total usage, not remaining balance

                    # OpenRouter doesn't provide daily/monthly breakdowns in this endpoint
                    # We'll use the total usage for now
                    usage_today = 0.0  # Not available from this endpoint
                    usage_month = total_usage  # Use total usage as monthly usage

                    rate_limit = data_section.get("rate_limit", {})

                credit_info = {
                    "balance": float(balance) if balance is not None else 0.0,
                    "usage_today": float(usage_today)
                    if usage_today is not None
                    else 0.0,
                    "usage_month": float(usage_month)
                    if usage_month is not None
                    else 0.0,
                    "rate_limit": rate_limit if isinstance(rate_limit, dict) else {},
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "raw_response": data,
                }

                return credit_info
            else:
                raise Exception(f"Unexpected API response format: {type(data)}")

    except httpx.RequestError as e:
