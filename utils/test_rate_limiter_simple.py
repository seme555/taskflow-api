#!/usr/bin/env python3
"""
Simple test script for the distributed rate limiter functionality without Redis dependency.
"""

import asyncio
import time
from datetime import datetime

# Add the worker directory to the path so we can import the rate limiter
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src", "worker"))

# Import rate limiter components at the top
from rate_limiter import (
    RedisTokenBucketRateLimiter,
    global_rate_limiter,
    get_rate_limit_status,
)


# Mock Redis for testing
class MockRedis:
    def __init__(self):
        self.data = {}

    async def eval(self, script, num_keys, *args):
        # Simple mock implementation for testing
        bucket_key = args[0]
        current_time = float(args[2])
        tokens_requested = int(args[3])

        # Initialize bucket if not exists
        if bucket_key not in self.data:
            self.data[bucket_key] = {
                "tokens": 230,  # Start with full capacity
                "capacity": 230,
                "refill_rate": 23.0,  # 230 requests per 10 seconds
                "last_refill": current_time,
            }

        bucket = self.data[bucket_key]

        # Calculate refill
        time_elapsed = current_time - bucket["last_refill"]
        tokens_to_add = time_elapsed * bucket["refill_rate"]
        bucket["tokens"] = min(bucket["capacity"], bucket["tokens"] + tokens_to_add)
        bucket["last_refill"] = current_time

        # Check if we have enough tokens
        if bucket["tokens"] >= tokens_requested:
            bucket["tokens"] -= tokens_requested
            return [
                1,
                bucket["tokens"],
                bucket["capacity"],
                bucket["refill_rate"],
            ]  # Success
        else:
            tokens_needed = tokens_requested - bucket["tokens"]
            wait_time = tokens_needed / bucket["refill_rate"]
            return [
                0,
                bucket["tokens"],
                bucket["capacity"],
                bucket["refill_rate"],
                wait_time,
            ]  # Failure

    async def hmget(self, key, *fields):
        if key not in self.data:
            return [None] * len(fields)

        bucket = self.data[key]
        result = []
        for field in fields:
            result.append(
                str(bucket.get(field, 0)) if bucket.get(field) is not None else None
            )
        return result

    async def delete(self, key):
        if key in self.data:
            del self.data[key]

    async def hset(self, key, mapping):
        if key not in self.data:
            self.data[key] = {}
        self.data[key].update(mapping)

    async def aclose(self):
        pass


# Monkey patch the rate limiter to use our mock Redis
original_get_redis = RedisTokenBucketRateLimiter._get_redis_connection
mock_redis = MockRedis()


async def mock_get_redis_connection(self):
    return mock_redis


RedisTokenBucketRateLimiter._get_redis_connection = mock_get_redis_connection


async def test_basic_functionality():
    """Test basic rate limiter functionality with mock Redis."""
    print("ðŸ§ª Testing Basic Rate Limiter Functionality (Mock Redis)")
    print("=" * 60)

    # Reset bucket
    await global_rate_limiter.reset_bucket()

    print("ðŸ“Š Testing token acquisition:")

    # Test acquiring tokens rapidly
    successful = 0
    failed = 0

    for i in range(10):
        start_time = time.time()
        success = await global_rate_limiter.acquire(1, timeout=0.1)  # Short timeout
