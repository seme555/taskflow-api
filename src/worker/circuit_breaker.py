# src/worker/circuit_breaker.py
import asyncio
import os
import random
from typing import List, Dict
import pybreaker
import httpx
from config import settings
from rate_limiter import wait_for_rate_limit_token
from openrouter_state_reporter import report_openrouter_error, report_openrouter_success

# Create circuit breaker instance
openrouter_breaker = pybreaker.CircuitBreaker(
    fail_max=10,  # Open after 10 failures
    reset_timeout=120,  # Try again after 120 seconds
    exclude=[KeyboardInterrupt],  # Don't count these as failures
)


def calculate_backoff_delay(
    attempt: int, base_delay: float = 1.0, max_delay: float = 300.0
) -> float:
    """
    Calculate exponential backoff delay with jitter for rate limiting.

    Args:
        attempt: The retry attempt number (0-based)
        base_delay: Base delay in seconds
        max_delay: Maximum delay in seconds

    Returns:
        Delay in seconds with jitter applied
    """
    # Exponential backoff: base_delay * (2 ^ attempt)
    delay = base_delay * (2**attempt)

    # Cap at max_delay
    delay = min(delay, max_delay)

    # Add jitter (Â±25% of the delay)
    jitter = delay * 0.25 * (2 * random.random() - 1)
    final_delay = delay + jitter

    # Ensure minimum delay of base_delay
    return max(final_delay, base_delay)


@openrouter_breaker
async def call_openrouter_api(
    messages: List[Dict[str, str]], retry_attempt: int = 0
) -> str:
    """
    Generic function to call OpenRouter API with circuit breaker protection and distributed rate limiting.

    Args:
        messages: List of message dictionaries for the chat completion API
                 (e.g., [{"role": "user", "content": "..."}])
        retry_attempt: Current retry attempt number for backoff calculation

    Returns:
        The response content from the API

    Raises:
        Exception: If the API call fails
    """
    # First, acquire a rate limit token from the distributed rate limiter
    # This ensures all workers coordinate to respect OpenRouter's global rate limits
    rate_limit_timeout = 60.0  # Wait up to 60 seconds for a rate limit token

    if not await wait_for_rate_limit_token(tokens=1, timeout=rate_limit_timeout):
        raise Exception(
            f"Rate limit token acquisition timeout after {rate_limit_timeout}s - OpenRouter API may be overloaded"
        )

    max_retries = 5

    for attempt in range(max_retries):
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{settings.openrouter_base_url}/chat/completions",
                    headers={
                        "Authorization": f"Bearer {settings.openrouter_api_key}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": settings.openrouter_model,
                        "messages": messages,
                    },
                    timeout=settings.openrouter_timeout,
                )

                # Handle rate limiting (HTTP 429) with exponential backoff
                if response.status_code == 429:
                    # Report rate limiting to state management
                    try:
                        await report_openrouter_error(
                            error_message="Rate limit exceeded",
                            status_code=429,
                            error_type="rate_limited",
                        )
                    except Exception:
                        pass  # Don't fail the main operation if reporting fails

                    if attempt < max_retries - 1:  # Don't sleep on the last attempt
                        # Check for Retry-After header
                        retry_after = response.headers.get("retry-after")
                        if retry_after:
                            try:
                                delay = float(retry_after)
                            except ValueError:
                                delay = calculate_backoff_delay(
                                    attempt, base_delay=60.0
                                )
                        else:
                            # Use exponential backoff for rate limiting
                            delay = calculate_backoff_delay(attempt, base_delay=60.0)

                        # Add extra jitter for thundering herd prevention
                        jitter = random.uniform(0, min(delay * 0.1, 30))
                        total_delay = delay + jitter

                        await asyncio.sleep(total_delay)
                        continue
                    else:
