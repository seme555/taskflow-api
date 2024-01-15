# src/api/routers/openrouter.py
"""OpenRouter status monitoring endpoints."""

import json
from typing import Dict, Optional
from datetime import datetime, timezone

import httpx
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel

from config import settings
from services import get_redis_service
from openrouter_state import (
    OpenRouterStateManager,
    OpenRouterState,
)


router = APIRouter(prefix="/api/v1/openrouter", tags=["openrouter"])


class OpenRouterStatus(BaseModel):
    """OpenRouter status response model."""

    status: str  # "active", "api_key_missing", "api_key_invalid", "credits_exhausted", "error"
    message: str
    balance: Optional[float] = None
    usage_today: Optional[float] = None
    usage_month: Optional[float] = None
    last_check: Optional[str] = None
    error_details: Optional[str] = None
    consecutive_failures: Optional[int] = None
    circuit_breaker_open: Optional[bool] = None
    cache_hit: Optional[bool] = None


async def check_openrouter_api_key(api_key: str, base_url: str) -> Dict:
    """
    Check OpenRouter API key validity and get credit information.

    Returns:
        Dictionary containing status and credit information
    """
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{base_url}/auth/key",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                timeout=10.0,
            )

            if response.status_code == 401:
                return {
                    "status": "api_key_invalid",
                    "message": "API key is invalid",
                    "error_details": "Authentication failed",
                }
            elif response.status_code == 403:
                return {
                    "status": "credits_exhausted",
                    "message": "Credits exhausted",
                    "error_details": "Insufficient credits",
                }
            elif response.status_code != 200:
                return {
                    "status": "error",
                    "message": f"API error: {response.status_code}",
                    "error_details": response.text,
                }

            data = response.json()

            # Handle different possible response structures
            if isinstance(data, dict):
                data_section = data.get("data", data)

                # Extract credit information
                balance = 0.0
                usage_today = 0.0
                usage_month = 0.0

                if isinstance(data_section, dict):
                    # OpenRouter API uses 'usage' field for total usage amount
                    total_usage = data_section.get("usage", 0.0)
                    balance = total_usage  # This is actually total usage, not remaining balance
                    usage_today = 0.0  # Not available from this endpoint
                    usage_month = total_usage  # Use total usage as monthly usage

                # Check if credits are exhausted (this is a simplified check)
                # In a real scenario, you'd need to know the actual credit limit
                if balance is not None and balance <= 0:
                    return {
                        "status": "credits_exhausted",
                        "message": "Credits exhausted",
                        "balance": float(balance),
                        "usage_today": float(usage_today),
                        "usage_month": float(usage_month),
                    }

                return {
                    "status": "active",
                    "message": "Service active",
                    "balance": float(balance) if balance is not None else 0.0,
                    "usage_today": float(usage_today)
                    if usage_today is not None
                    else 0.0,
                    "usage_month": float(usage_month)
                    if usage_month is not None
                    else 0.0,
                }
            else:
                return {
                    "status": "error",
                    "message": "Unexpected API response format",
                    "error_details": f"Response type: {type(data)}",
                }

    except httpx.TimeoutException:
        return {
            "status": "error",
            "message": "API timeout",
