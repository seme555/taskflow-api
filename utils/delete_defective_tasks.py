#!/usr/bin/env python3
"""
Utility script to identify and delete defective tasks from Redis.

This script finds and removes tasks with:
- task_id = "unknown_id"
- Invalid timestamps (datetime.min or year 1)
- Any other data corruption indicators

Usage:
    python utils/delete_defective_tasks.py --dry-run  # Preview what would be deleted
    python utils/delete_defective_tasks.py           # Actually delete the tasks
"""

import redis
from datetime import datetime
from typing import List, Dict, Any


def is_defective_task(task_data: Dict[str, str]) -> bool:
    """
    Check if a task is defective based on known corruption patterns.

    Args:
        task_data: Task data from Redis hash

    Returns:
        True if task is defective and should be deleted
    """
    # Check for unknown_id
    if task_data.get("task_id") == "unknown_id":
        return True

    # Check for invalid timestamps
    for date_field in ["created_at", "updated_at", "completed_at"]:
        date_str = task_data.get(date_field)
        if date_str:
            try:
                parsed_date = datetime.fromisoformat(date_str)
                # Check if year is 1 (datetime.min) or other invalid dates
                if parsed_date.year == 1:
                    return True
            except (ValueError, TypeError):
                # Invalid date format is also defective
                return True

    # Check for missing required fields
    required_fields = ["task_id", "state", "created_at"]
    for field in required_fields:
        if not task_data.get(field):
            return True

    return False


def find_defective_tasks(
    redis_url: str = "redis://localhost:6379/0",
) -> List[Dict[str, Any]]:
    """
    Find all defective tasks in Redis.

    Args:
        redis_url: Redis connection URL

    Returns:
        List of defective task information
    """
    defective_tasks = []

    try:
        # Connect to Redis
        r = redis.from_url(redis_url, decode_responses=True)
        r.ping()
        print(f"Connected to Redis at {redis_url}")

        # Scan all task keys
        task_count = 0
        for key in r.scan_iter("task:*"):
            task_count += 1
            task_data = r.hgetall(key)

            if is_defective_task(task_data):
                defective_info = {
                    "redis_key": key,
                    "task_id": task_data.get("task_id", "missing"),
                    "state": task_data.get("state", "missing"),
                    "created_at": task_data.get("created_at", "missing"),
                    "updated_at": task_data.get("updated_at", "missing"),
                    "completed_at": task_data.get("completed_at", "missing"),
                    "task_type": task_data.get("task_type", "missing"),
                    "content_preview": task_data.get("content", "")[:100] + "..."
                    if task_data.get("content", "")
                    else "missing",
                }
                defective_tasks.append(defective_info)

        print(f"Scanned {task_count} total tasks")
        print(f"Found {len(defective_tasks)} defective tasks")

        return defective_tasks

    except redis.RedisError as e:
        print(f"Redis error: {e}")
        return []
    except Exception as e:
        print(f"Unexpected error: {e}")
        return []


def delete_defective_tasks(
    redis_url: str = "redis://localhost:6379/0", dry_run: bool = False
) -> int:
    """
    Delete defective tasks from Redis.

    Args:
        redis_url: Redis connection URL
        dry_run: If True, only show what would be deleted

    Returns:
        Number of tasks deleted (or would be deleted in dry run)
    """
    defective_tasks = find_defective_tasks(redis_url)

    if not defective_tasks:
