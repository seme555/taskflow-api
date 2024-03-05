#!/usr/bin/env python3
"""
DLQ Task Injection Utility for Frontend Testing

This script creates test tasks directly in the Dead Letter Queue (DLQ)
with proper metadata including error history, retry counts, and state transitions.
Useful for testing frontend DLQ handling, error display, and retry functionality.
"""

import asyncio
import json
import sys
from datetime import datetime, timedelta
from typing import List, Dict, Any
import uuid

import redis.asyncio as redis


class DLQTaskInjector:
    """Utility for injecting test tasks directly into the DLQ."""

    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis_url = redis_url
        self.redis = None
        self.created_tasks = []

    async def __aenter__(self):
        self.redis = redis.from_url(self.redis_url, decode_responses=True)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.redis:
            await self.redis.close()

    async def create_dlq_task(
        self, content: str, task_number: int, error_scenario: str
    ) -> Dict[str, Any]:
        """Create a single task directly in the DLQ with realistic error metadata."""
        task_id = str(uuid.uuid4())
        now = datetime.utcnow()

        # Create realistic error scenarios
        error_scenarios = {
            "timeout": {
                "error_type": "TimeoutError",
                "last_error": "Task execution timed out after 300 seconds. The external API did not respond within the configured timeout period.",
                "error_history": [
                    {
                        "error": "Connection timeout to external service",
                        "timestamp": (now - timedelta(hours=2)).isoformat(),
                        "retry_count": 1,
                    },
                    {
                        "error": "API rate limit exceeded, retrying after backoff",
                        "timestamp": (now - timedelta(hours=1, minutes=30)).isoformat(),
                        "retry_count": 2,
                    },
                    {
                        "error": "Task execution timed out after 300 seconds",
                        "timestamp": (now - timedelta(minutes=45)).isoformat(),
                        "retry_count": 3,
                    },
                ],
            },
            "api_error": {
                "error_type": "APIError",
                "last_error": "External API returned HTTP 503: Service temporarily unavailable. The summarization service is experiencing high load.",
                "error_history": [
                    {
                        "error": "HTTP 429: Too Many Requests",
                        "timestamp": (now - timedelta(hours=3)).isoformat(),
                        "retry_count": 1,
                    },
                    {
                        "error": "HTTP 502: Bad Gateway",
                        "timestamp": (now - timedelta(hours=2, minutes=15)).isoformat(),
                        "retry_count": 2,
                    },
                    {
                        "error": "HTTP 503: Service temporarily unavailable",
                        "timestamp": (now - timedelta(hours=1)).isoformat(),
                        "retry_count": 3,
                    },
                ],
            },
            "validation_error": {
                "error_type": "ValidationError",
                "last_error": "Content validation failed: Text contains unsupported characters or exceeds maximum length limit of 10,000 characters.",
                "error_history": [
                    {
                        "error": "Content too short for meaningful summarization",
                        "timestamp": (now - timedelta(hours=4)).isoformat(),
                        "retry_count": 1,
                    },
                    {
                        "error": "Invalid character encoding detected",
                        "timestamp": (now - timedelta(hours=3, minutes=30)).isoformat(),
                        "retry_count": 2,
                    },
                    {
                        "error": "Content exceeds maximum length limit",
                        "timestamp": (now - timedelta(hours=2, minutes=45)).isoformat(),
                        "retry_count": 3,
                    },
                ],
            },
        }

        scenario_data = error_scenarios.get(error_scenario, error_scenarios["timeout"])

        # Create comprehensive state history
        state_history = [
            {"state": "PENDING", "timestamp": (now - timedelta(hours=5)).isoformat()},
            {
                "state": "ACTIVE",
                "timestamp": (now - timedelta(hours=4, minutes=45)).isoformat(),
            },
            {
                "state": "FAILED",
                "timestamp": (now - timedelta(hours=4, minutes=30)).isoformat(),
            },
            {
                "state": "PENDING",
                "timestamp": (now - timedelta(hours=3, minutes=45)).isoformat(),
