#!/usr/bin/env python3
"""
Task Injection Utility for Frontend Testing

This script creates multiple test tasks to verify that the frontend
reacts properly to task creation and state changes. Useful for testing
real-time updates, queue monitoring, and dashboard responsiveness.
"""

import asyncio
import json
import sys
from datetime import datetime
from typing import List, Dict, Any

import httpx


class TaskInjector:
    """Utility for injecting test tasks and monitoring frontend reactions."""

    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url.rstrip("/")
        self.client = None
        self.created_tasks = []

    async def __aenter__(self):
        self.client = httpx.AsyncClient(timeout=30.0)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.client:
            await self.client.aclose()

    async def create_task(self, content: str, task_number: int) -> Dict[str, Any]:
        """Create a single summarization task."""
        task_data = {"content": f"Test task #{task_number}: {content}"}

        try:
            response = await self.client.post(
                f"{self.base_url}/api/v1/tasks/summarize/", json=task_data
            )

            if response.status_code == 201:
                result = response.json()
                task_id = result.get("task_id")
                self.created_tasks.append(
                    {
                        "task_id": task_id,
                        "task_number": task_number,
                        "content": content,
                        "created_at": datetime.utcnow().isoformat(),
                        "status": "created",
                    }
                )
                return {
                    "success": True,
                    "task_id": task_id,
                    "task_number": task_number,
                    "response": result,
                }
            else:
                return {
                    "success": False,
                    "task_number": task_number,
                    "error": f"HTTP {response.status_code}: {response.text}",
                }

        except Exception as e:
            return {"success": False, "task_number": task_number, "error": str(e)}

    async def inject_tasks(
        self, count: int = 10, delay_between: float = 0.5, batch_size: int = 1
    ) -> List[Dict[str, Any]]:
        """
        Inject multiple test tasks.

        Args:
            count: Number of tasks to create
            delay_between: Delay in seconds between task creation
            batch_size: Number of tasks to create in each batch
        """
        print(f"Injecting {count} test tasks...")
        print(f"Delay between tasks: {delay_between}s")
        print(f"Batch size: {batch_size}")
        print(f"Target API: {self.base_url}")
        print("=" * 60)

        results = []

        # Generate varied test content
        test_contents = [
            "This is a comprehensive analysis of modern web development practices and their impact on user experience.",
            "Exploring the benefits of containerized applications in microservices architecture for scalable systems.",
            "Understanding the role of asynchronous task processing in building responsive web applications.",
            "A detailed examination of real-time data streaming and its applications in modern dashboard interfaces.",
            "Investigating the performance implications of different database indexing strategies in high-traffic applications.",
            "Analyzing the security considerations when implementing API authentication and authorization mechanisms.",
            "Evaluating the trade-offs between different frontend frameworks for building interactive user interfaces.",
            "Discussing the importance of monitoring and observability in distributed system architectures.",
            "Examining best practices for error handling and recovery in asynchronous processing systems.",
            "A comprehensive guide to implementing efficient caching strategies in web application development.",
        ]

        for i in range(count):
            task_number = i + 1
            content = test_contents[i % len(test_contents)]

            print(f"Creating task {task_number}/{count}...")

            # Create tasks in batches
            if batch_size == 1:
                result = await self.create_task(content, task_number)
                results.append(result)

                if result["success"]:
                    print(f"  âœ… Task {task_number} created: {result['task_id']}")
                else:
                    print(f"  âŒ Task {task_number} failed: {result['error']}")
            else:
                # Batch creation (for future enhancement)
                result = await self.create_task(content, task_number)
                results.append(result)

                if result["success"]:
