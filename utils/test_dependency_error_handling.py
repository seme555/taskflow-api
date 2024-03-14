#!/usr/bin/env python3
"""
Test script to verify that dependency errors are properly handled and sent to DLQ.
This script simulates a poppler dependency error to test the new error classification.
"""

import asyncio
import json
import time
import uuid
from datetime import datetime

import redis.asyncio as aioredis


async def test_dependency_error_handling():
    """Test that dependency errors go directly to DLQ instead of being retried."""

    # Connect to Redis
    redis_conn = aioredis.from_url("redis://localhost:6379", decode_responses=True)

    # Create a test task that will trigger a dependency error
    task_id = str(uuid.uuid4())

    # Create a fake PDF content that will trigger the poppler error
    # We'll use invalid base64 content to simulate the error
    fake_pdf_content = "invalid_base64_content_that_will_cause_poppler_error"

    task_data = {
        "task_id": task_id,
        "task_type": "pdfxtract",
        "content": fake_pdf_content,
        "metadata": json.dumps(
            {"filename": "test_dependency_error.pdf", "issue_date": "2025-07-21"}
        ),
        "state": "PENDING",
        "created_at": datetime.utcnow().isoformat(),
        "updated_at": datetime.utcnow().isoformat(),
    }

    print(f"Creating test task {task_id} to test dependency error handling...")

    # Store the task in Redis
    await redis_conn.hset(f"task:{task_id}", mapping=task_data)

    # Add the task to the primary queue
    await redis_conn.lpush("tasks:pending:primary", task_id)

    print(f"Task {task_id} added to primary queue. Waiting for processing...")

    # Wait for the task to be processed
    max_wait_time = 30  # seconds
    start_time = time.time()

    while time.time() - start_time < max_wait_time:
        # Check task state
        task_state = await redis_conn.hget(f"task:{task_id}", "state")

        if task_state == "DLQ":
            print(f"âœ… SUCCESS: Task {task_id} was correctly moved to DLQ!")

            # Get task details
            task_details = await redis_conn.hgetall(f"task:{task_id}")
            error_type = task_details.get("error_type", "Unknown")
            last_error = task_details.get("last_error", "No error message")

            print(f"   Error Type: {error_type}")
            print(f"   Last Error: {last_error}")

            # Check if it's in the DLQ
            dlq_tasks = await redis_conn.lrange("dlq:tasks", 0, -1)
            if task_id in dlq_tasks:
                print("   âœ… Task is correctly present in DLQ")
            else:
                print("   âŒ Task is NOT in DLQ list")

            # Check that it's not in retry queue or scheduled
            retry_tasks = await redis_conn.lrange("tasks:pending:retry", 0, -1)
            scheduled_tasks = await redis_conn.zrange("tasks:scheduled", 0, -1)

            if task_id not in retry_tasks and task_id not in scheduled_tasks:
                print("   âœ… Task is correctly NOT in retry or scheduled queues")
                return True
            else:
                print(
                    "   âŒ Task found in retry or scheduled queues (should not be there)"
                )
                return False

        elif task_state == "SCHEDULED":
            print(
                f"âŒ FAILURE: Task {task_id} was scheduled for retry instead of going to DLQ!"
            )

            # Get task details
            task_details = await redis_conn.hgetall(f"task:{task_id}")
            error_type = task_details.get("error_type", "Unknown")
            last_error = task_details.get("last_error", "No error message")
            retry_count = task_details.get("retry_count", "0")

            print(f"   Error Type: {error_type}")
            print(f"   Last Error: {last_error}")
            print(f"   Retry Count: {retry_count}")

            return False

        elif task_state in ["ACTIVE", "PENDING"]:
            print(f"Task {task_id} is still being processed (state: {task_state})...")
            await asyncio.sleep(2)
            continue
        else:
            print(f"Task {task_id} has unexpected state: {task_state}")
            await asyncio.sleep(2)
            continue

    print(
        f"âŒ TIMEOUT: Task {task_id} was not processed within {max_wait_time} seconds"
    )

    # Get final task state
    final_state = await redis_conn.hget(f"task:{task_id}", "state")
    print(f"Final task state: {final_state}")

    return False

