#!/usr/bin/env python3
"""
Test script for the distributed rate limiter functionality.
"""

import asyncio
import time
from datetime import datetime

# Add the worker directory to the path so we can import the rate limiter
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src", "worker"))

from rate_limiter import global_rate_limiter, get_rate_limit_status


async def test_rate_limiter_basic():
    """Test basic rate limiter functionality."""
    print("ðŸ§ª Testing Distributed Rate Limiter")
    print("=" * 50)

    # Reset bucket for clean test
    await global_rate_limiter.reset_bucket()

    # Set test configuration (lower limits for testing)
    await global_rate_limiter.update_rate_limit_config(
        5, "10s"
    )  # 5 requests per 10 seconds

    print("ðŸ“Š Initial status:")
    status = await get_rate_limit_status()
    print(f"   Capacity: {status['capacity']}")
    print(f"   Current tokens: {status['current_tokens']:.2f}")
    print(f"   Refill rate: {status['refill_rate']:.2f} tokens/second")
    print(
        f"   Config: {status['config']['requests']} requests per {status['config']['interval']}"
    )

    # Test acquiring tokens
    print("\nðŸ”„ Testing token acquisition:")
    for i in range(7):  # Try to get more tokens than capacity
        start_time = time.time()
        success = await global_rate_limiter.acquire(1, timeout=2.0)
        elapsed = time.time() - start_time

        if success:
            print(f"   Request {i+1}: âœ… Acquired token in {elapsed:.3f}s")
        else:
            print(f"   Request {i+1}: âŒ Timeout after {elapsed:.3f}s")

        # Show current status
        status = await get_rate_limit_status()
        print(
            f"      Remaining tokens: {status['current_tokens']:.2f} ({status['utilization_percent']:.1f}% utilized)"
        )

    print("\nðŸ“ˆ Final status:")
    status = await get_rate_limit_status()
    print(f"   Current tokens: {status['current_tokens']:.2f}")
    print(f"   Utilization: {status['utilization_percent']:.1f}%")


async def test_rate_limiter_with_openrouter_config():
    """Test rate limiter with actual OpenRouter configuration."""
    print("\n" + "=" * 50)
    print("ðŸŒ Testing with OpenRouter Configuration")
    print("=" * 50)

    # Reset bucket
    await global_rate_limiter.reset_bucket()

    # Set OpenRouter configuration (230 requests per 10s)
    await global_rate_limiter.update_rate_limit_config(230, "10s")

    print("ðŸ“Š OpenRouter configuration loaded:")
    status = await get_rate_limit_status()
    print(f"   Capacity: {status['capacity']} requests")
    print(f"   Refill rate: {status['refill_rate']:.2f} requests/second")
    print(f"   Max requests per second: {status['refill_rate']:.1f}")

    # Test burst requests
    print("\nðŸš€ Testing burst requests (simulating multiple workers):")
    burst_size = 10
    start_time = time.time()

    tasks = []
    for i in range(burst_size):
        task = asyncio.create_task(global_rate_limiter.acquire(1, timeout=5.0))
        tasks.append(task)

    results = await asyncio.gather(*tasks)
    elapsed = time.time() - start_time

    successful = sum(1 for r in results if r)
    print(
        f"   Burst of {burst_size} requests: {successful} successful in {elapsed:.3f}s"
    )
    print(f"   Effective rate: {successful/elapsed:.2f} requests/second")

    # Show final status
    status = await get_rate_limit_status()
    print(f"   Remaining tokens: {status['current_tokens']:.2f}")
    print(f"   Utilization: {status['utilization_percent']:.1f}%")


async def test_rate_limiter_recovery():
    """Test rate limiter token recovery over time."""
    print("\n" + "=" * 50)
    print("â° Testing Token Recovery Over Time")
    print("=" * 50)

    # Use a smaller bucket for faster testing
    await global_rate_limiter.update_rate_limit_config(
        10, "5s"
    )  # 10 requests per 5 seconds

    # Exhaust the bucket
    print("ðŸ”¥ Exhausting token bucket...")
    for i in range(10):
        success = await global_rate_limiter.acquire(1, timeout=0.1)
        if not success:
            break

