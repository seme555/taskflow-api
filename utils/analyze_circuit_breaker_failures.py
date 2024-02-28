#!/usr/bin/env python3
"""
Script to analyze circuit breaker failures and extract root cause information.
"""

import redis
import json
import os
from collections import Counter, defaultdict

# Configuration
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")


def analyze_error_patterns():
    """Analyze error patterns from DLQ tasks and task history."""
    r = redis.from_url(REDIS_URL, decode_responses=True)

    print("ðŸ” Analyzing Circuit Breaker Failure Root Causes")
    print("=" * 60)

    # Get all DLQ task IDs
    dlq_task_ids = r.lrange("dlq:tasks", 0, -1)
    print(f"Found {len(dlq_task_ids)} tasks in DLQ")

    error_analysis = {
        "http_errors": Counter(),
        "error_types": Counter(),
        "timeline": [],
        "detailed_errors": [],
    }

    # Analyze each DLQ task
    for task_id in dlq_task_ids:
        task_data = r.hgetall(f"task:{task_id}")
        if not task_data:
            continue

        print(f"\nðŸ“‹ Task: {task_id}")
        print(f"   State: {task_data.get('state', 'unknown')}")
        print(f"   Last Error: {task_data.get('last_error', 'none')}")
        print(f"   Error Type: {task_data.get('error_type', 'unknown')}")
        print(f"   Retry Count: {task_data.get('retry_count', 0)}")

        # Parse error history
        error_history = []
        if task_data.get("error_history"):
            try:
                error_history = json.loads(task_data["error_history"])
            except (json.JSONDecodeError, TypeError):
                pass

        if error_history:
            print(f"   ðŸ“œ Error History ({len(error_history)} entries):")
            for i, error_entry in enumerate(error_history, 1):
                error_msg = error_entry.get("error", "unknown")
                timestamp = error_entry.get("timestamp", "unknown")
                error_type = error_entry.get("error_type", "unknown")
                retry_count = error_entry.get("retry_count", 0)

                print(f"      {i}. [{timestamp}] Retry {retry_count}: {error_msg}")

                # Extract HTTP status codes
                if "HTTP" in error_msg and any(
                    code in error_msg
                    for code in ["429", "502", "503", "500", "401", "402", "403", "404"]
                ):
                    for code in [
                        "429",
                        "502",
                        "503",
                        "500",
                        "401",
                        "402",
                        "403",
                        "404",
                    ]:
                        if code in error_msg:
                            error_analysis["http_errors"][code] += 1
                            break

                # Count error types
                error_analysis["error_types"][error_type] += 1

                # Add to timeline
                error_analysis["timeline"].append(
                    {
                        "task_id": task_id,
                        "timestamp": timestamp,
                        "error": error_msg,
                        "error_type": error_type,
                        "retry_count": retry_count,
                    }
                )

                # Add detailed error for analysis
                error_analysis["detailed_errors"].append(
                    {
                        "task_id": task_id,
                        "error": error_msg,
                        "error_type": error_type,
                        "timestamp": timestamp,
                    }
                )

    # Analyze all task states to find patterns
    print("\nðŸ” Scanning all tasks for error patterns...")
    error_patterns = defaultdict(int)
    circuit_breaker_triggers = []

    for key in r.scan_iter("task:*"):
        task_data = r.hgetall(key)
        if not task_data:
            continue

        # Check for error history in any task
        if task_data.get("error_history"):
            try:
                error_history = json.loads(task_data["error_history"])
                for error_entry in error_history:
                    error_msg = error_entry.get("error", "")

                    # Look for circuit breaker related errors
                    if "circuit breaker" in error_msg.lower():
                        circuit_breaker_triggers.append(
