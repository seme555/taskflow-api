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
            },
            {
                "state": "ACTIVE",
                "timestamp": (now - timedelta(hours=3, minutes=30)).isoformat(),
            },
            {
                "state": "FAILED",
                "timestamp": (now - timedelta(hours=3, minutes=15)).isoformat(),
            },
            {
                "state": "PENDING",
                "timestamp": (now - timedelta(hours=2, minutes=30)).isoformat(),
            },
            {
                "state": "ACTIVE",
                "timestamp": (now - timedelta(hours=2, minutes=15)).isoformat(),
            },
            {"state": "FAILED", "timestamp": (now - timedelta(hours=2)).isoformat()},
            {"state": "DLQ", "timestamp": (now - timedelta(hours=1)).isoformat()},
        ]

        # Task metadata with comprehensive error information
        task_data = {
            "task_id": task_id,
            "content": f"Test DLQ task #{task_number}: {content}",
            "state": "DLQ",
            "retry_count": 3,  # Max retries reached
            "max_retries": 3,
            "last_error": scenario_data["last_error"],
            "error_type": scenario_data["error_type"],
            "retry_after": "",  # No more retries scheduled
            "created_at": (now - timedelta(hours=5)).isoformat(),
            "updated_at": (now - timedelta(hours=1)).isoformat(),
            "completed_at": "",  # Never completed
            "result": "",  # No result due to failure
            "error_history": json.dumps(scenario_data["error_history"]),
            "state_history": json.dumps(state_history),
        }

        try:
            # Use Redis transaction to ensure atomicity
            async with self.redis.pipeline(transaction=True) as pipe:
                # Store task metadata in both locations for compatibility
                await pipe.hset(f"task:{task_id}", mapping=task_data)
                await pipe.hset(f"dlq:task:{task_id}", mapping=task_data)

                # Add to DLQ queue
                await pipe.lpush("dlq:tasks", task_id)

                # Update state counters
                await pipe.incrby("metrics:tasks:state:dlq", 1)

                await pipe.execute()

            self.created_tasks.append(
                {
                    "task_id": task_id,
                    "task_number": task_number,
                    "content": content,
                    "error_scenario": error_scenario,
                    "created_at": datetime.utcnow().isoformat(),
                    "status": "injected_to_dlq",
                }
            )

            return {
                "success": True,
                "task_id": task_id,
                "task_number": task_number,
                "error_scenario": error_scenario,
                "message": f"Task {task_number} injected into DLQ with {scenario_data['error_type']} scenario",
            }

        except Exception as e:
            return {"success": False, "task_number": task_number, "error": str(e)}

    async def inject_dlq_tasks(self, count: int = 3) -> List[Dict[str, Any]]:
        """
        Inject multiple test tasks directly into the DLQ.

        Args:
            count: Number of tasks to create (default: 3)
        """
        print(f"Injecting {count} test tasks directly into DLQ...")
        print(f"Redis URL: {self.redis_url}")
        print("=" * 60)

        results = []

        # Test content for DLQ tasks
        test_contents = [
            "This is a comprehensive analysis of modern web development practices that failed to process due to external service issues.",
            "Exploring the benefits of containerized applications in microservices architecture - this task encountered validation errors during processing.",
            "Understanding the role of asynchronous task processing in building responsive web applications - processing timed out multiple times.",
        ]

        # Error scenarios to simulate
        error_scenarios = ["timeout", "api_error", "validation_error"]

        for i in range(count):
            task_number = i + 1
            content = test_contents[i % len(test_contents)]
            error_scenario = error_scenarios[i % len(error_scenarios)]

            print(
                f"Creating DLQ task {task_number}/{count} with {error_scenario} scenario..."
            )

            result = await self.create_dlq_task(content, task_number, error_scenario)
            results.append(result)

            if result["success"]:
                print(f"  âœ… DLQ Task {task_number} created: {result['task_id']}")
                print(f"     Error scenario: {result['error_scenario']}")
            else:
                print(f"  âŒ DLQ Task {task_number} failed: {result['error']}")

            # Small delay between tasks
            await asyncio.sleep(0.1)

        return results

    async def get_dlq_status(self) -> Dict[str, Any]:
        """Get current DLQ status."""
        try:
