#!/usr/bin/env python3
"""
API Endpoint Testing Utility for TaskFlow API

This script exercises all API endpoints and checks if they return expected status codes.
It performs comprehensive testing of all routes including error conditions.
"""

import asyncio
import json
import sys
from typing import Dict, Any, Optional
from datetime import datetime, timezone
import uuid

import httpx


class APITester:
    """Comprehensive API endpoint tester."""

    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url.rstrip("/")
        self.client = None
        self.test_results = []
        self.created_task_ids = []  # Track created tasks for cleanup

    async def __aenter__(self):
        self.client = httpx.AsyncClient(timeout=30.0)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.client:
            await self.client.aclose()

    async def test_endpoint(
        self,
        method: str,
        path: str,
        expected_status: int = 200,
        json_data: Optional[Dict] = None,
        params: Optional[Dict] = None,
        description: str = "",
    ) -> Dict[str, Any]:
        """Test a single endpoint and return results."""
        url = f"{self.base_url}{path}"

        try:
            if method.upper() == "GET":
                response = await self.client.get(url, params=params)
            elif method.upper() == "POST":
                response = await self.client.post(url, json=json_data, params=params)
            elif method.upper() == "DELETE":
                response = await self.client.delete(url, params=params)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            success = response.status_code == expected_status

            result = {
                "method": method.upper(),
                "path": path,
                "url": url,
                "expected_status": expected_status,
                "actual_status": response.status_code,
                "success": success,
                "description": description,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "response_size": len(response.content),
            }

            # Add response data for successful requests (truncated for readability)
            if success and response.status_code < 400:
                try:
                    response_json = response.json()
                    # Truncate large responses
                    if (
                        isinstance(response_json, dict)
                        and len(str(response_json)) > 500
                    ):
                        result["response_preview"] = str(response_json)[:500] + "..."
                    else:
                        result["response_preview"] = response_json
                except Exception:
                    result["response_preview"] = (
                        response.text[:200] + "..."
                        if len(response.text) > 200
                        else response.text
                    )
            else:
                result["error_detail"] = (
                    response.text[:200] + "..."
                    if len(response.text) > 200
                    else response.text
                )

            self.test_results.append(result)
            return result

        except Exception as e:
            result = {
                "method": method.upper(),
                "path": path,
                "url": url,
                "expected_status": expected_status,
                "actual_status": None,
                "success": False,
                "description": description,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "error": str(e),
            }
            self.test_results.append(result)
            return result

    async def test_streaming_endpoint(
        self, method: str, path: str, expected_status: int = 200, description: str = ""
    ) -> Dict[str, Any]:
        """Test a streaming endpoint (Server-Sent Events)."""
        url = f"{self.base_url}{path}"

        try:
            # For streaming endpoints, we just check that they start properly
            # and return the expected status code with the right content type
            async with self.client.stream(method.upper(), url) as response:
                success = response.status_code == expected_status

                # For SSE endpoints, check content type
                content_type = response.headers.get("content-type", "")
                is_sse = "text/event-stream" in content_type

                # Read first chunk to verify it's streaming
                first_chunk = None
                try:
                    async for chunk in response.aiter_text():
                        first_chunk = chunk[:100]  # First 100 chars
                        break  # Just get the first chunk
                except Exception:
                    pass

                result = {
                    "method": method.upper(),
                    "path": path,
                    "url": url,
                    "expected_status": expected_status,
                    "actual_status": response.status_code,
                    "success": success and is_sse,
                    "description": description,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "content_type": content_type,
                    "is_streaming": is_sse,
                    "first_chunk": first_chunk,
                }

                self.test_results.append(result)
                return result

        except Exception as e:
            result = {
                "method": method.upper(),
                "path": path,
                "url": url,
                "expected_status": expected_status,
                "actual_status": None,
                "success": False,
                "description": description,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "error": str(e),
            }
            self.test_results.append(result)
            return result

    async def test_root_endpoints(self):
        """Test root and basic endpoints."""
        print("Testing root endpoints...")

        await self.test_endpoint("GET", "/", description="Root endpoint")

    async def test_health_endpoints(self):
        """Test all health check endpoints."""
        print("Testing health endpoints...")

        await self.test_endpoint("GET", "/health", description="Main health check")
        await self.test_endpoint("GET", "/ready", description="Readiness check")
        await self.test_endpoint("GET", "/live", description="Liveness check")
        await self.test_endpoint(
            "GET", "/api/v1/workers/", description="Worker health check"
        )
        await self.test_endpoint(
            "POST",
            "/api/v1/workers/reset-circuit-breaker",
            description="Reset circuit breakers",
        )
        await self.test_endpoint(
            "POST",
            "/api/v1/workers/open-circuit-breaker",
            description="Open circuit breakers",
        )

    async def test_task_creation(self) -> Optional[str]:
        """Test task creation and return a task ID for further testing."""
        print("Testing task creation...")

        # Test summarization task creation
        task_data = {
            "content": "This is a test text for summarization. It should be processed by the worker system."
        }
        result = await self.test_endpoint(
            "POST",
            "/api/v1/tasks/summarize/",
            expected_status=201,
            json_data=task_data,
            description="Create summarization task",
        )

        # Extract task ID if successful
        task_id = None
        if result["success"] and "response_preview" in result:
            try:
                if isinstance(result["response_preview"], dict):
                    task_id = result["response_preview"].get("task_id")
                elif isinstance(result["response_preview"], str):
                    # Try to parse JSON from string
                    import json

                    data = json.loads(result["response_preview"])
                    task_id = data.get("task_id")
            except Exception:
                pass

        if task_id:
            self.created_task_ids.append(task_id)
            print(f"  Created task: {task_id}")

        # Test PDF extraction task creation (without actual file upload for now)
        # Note: This will fail with 422 since we're not providing a file, but it tests the endpoint exists
        await self.test_endpoint(
            "POST",
            "/api/v1/tasks/pdfxtract",
            expected_status=422,
            description="Test PDF extraction endpoint (expected 422 - no file)",
        )

        return task_id

    async def test_task_management(self, task_id: Optional[str] = None):
        """Test task management endpoints."""
        print("Testing task management endpoints...")

        if task_id:
            # Test getting specific task
