#!/usr/bin/env python3
"""
Distributed Rate Limiter for OpenRouter API

This module implements a Redis-based token bucket rate limiter that coordinates
rate limiting across multiple Celery workers to respect OpenRouter's API limits.
"""

import asyncio
import time
from typing import Dict, Any
import redis.asyncio as aioredis
from config import settings


class RedisTokenBucketRateLimiter:
    """
    Distributed token bucket rate limiter using Redis.

    This implementation uses Redis Lua scripts to ensure atomic operations
    and prevent race conditions between multiple workers.
    """

    def __init__(
        self, redis_url: str = None, bucket_key: str = "openrouter:rate_limit:bucket"
    ):
        """
        Initialize the rate limiter.

        Args:
            redis_url: Redis connection URL (defaults to settings.redis_url)
            bucket_key: Redis key for the token bucket
        """
        self.redis_url = redis_url or settings.redis_url
        self.bucket_key = bucket_key
        self.config_key = "openrouter:rate_limit_config"

        # Lua script for atomic token bucket operations
        self.lua_script = """
        local bucket_key = KEYS[1]
        local config_key = KEYS[2]
        local current_time = tonumber(ARGV[1])
        local tokens_requested = tonumber(ARGV[2])
        
        -- Get current bucket state
        local bucket_data = redis.call('HMGET', bucket_key, 'tokens', 'last_refill', 'capacity', 'refill_rate')
        local tokens = tonumber(bucket_data[1]) or 0
        local last_refill = tonumber(bucket_data[2]) or current_time
        local capacity = tonumber(bucket_data[3]) or 0
        local refill_rate = tonumber(bucket_data[4]) or 0
        
        -- If capacity is 0, try to load from config
        if capacity == 0 then
            local config_data = redis.call('HMGET', config_key, 'requests', 'interval')
            local requests = tonumber(config_data[1]) or 230
            local interval = config_data[2] or '10s'
            
            -- Parse interval (e.g., "10s" -> 10)
            local interval_seconds = 10
            if interval then
                local num = string.match(interval, '(%d+)')
                if num then
                    interval_seconds = tonumber(num)
                end
            end
            
            capacity = requests
            refill_rate = requests / interval_seconds
            tokens = capacity  -- Start with full bucket
        end
        
        -- Calculate tokens to add based on time elapsed
        local time_elapsed = current_time - last_refill
        local tokens_to_add = time_elapsed * refill_rate
        tokens = math.min(capacity, tokens + tokens_to_add)
        
        -- Check if we have enough tokens
        if tokens >= tokens_requested then
            tokens = tokens - tokens_requested
            
            -- Update bucket state
            redis.call('HMSET', bucket_key, 
                'tokens', tokens,
                'last_refill', current_time,
                'capacity', capacity,
                'refill_rate', refill_rate
            )
            
            -- Set expiration (cleanup after 1 hour of inactivity)
            redis.call('EXPIRE', bucket_key, 3600)
            
            return {1, tokens, capacity, refill_rate}  -- Success
        else
            -- Not enough tokens, calculate wait time
            local tokens_needed = tokens_requested - tokens
            local wait_time = tokens_needed / refill_rate
            
            -- Update bucket state (even on failure, to track refill time)
            redis.call('HMSET', bucket_key, 
                'tokens', tokens,
                'last_refill', current_time,
                'capacity', capacity,
                'refill_rate', refill_rate
            )
            
            redis.call('EXPIRE', bucket_key, 3600)
            
            return {0, tokens, capacity, refill_rate, wait_time}  -- Failure with wait time
        end
        """

    async def _get_redis_connection(self) -> aioredis.Redis:
        """Get an async Redis connection."""
        return aioredis.from_url(self.redis_url, decode_responses=True)

    async def acquire(self, tokens: int = 1, timeout: float = 30.0) -> bool:
        """
        Acquire tokens from the bucket.

        Args:
            tokens: Number of tokens to acquire (default: 1)
            timeout: Maximum time to wait for tokens (seconds)

        Returns:
            True if tokens were acquired, False if timeout occurred
