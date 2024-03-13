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
