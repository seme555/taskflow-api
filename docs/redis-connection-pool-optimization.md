# Redis Connection Pool Optimization for Long-Running Tasks

## Overview

This document describes the Redis connection pool optimizations implemented to address connection issues with long-running async tasks in the FastAPI TaskFlow API system.

## Problem Statement

The original Redis connection setup was experiencing issues with long-running tasks:

- **Connection Pool Exhaustion**: Default Redis connection pools (10 connections) were insufficient for concurrent long-running tasks
- **Connection Timeouts**: Socket timeouts were too short for operations like BLPOP that could block for extended periods
- **Connection Lifecycle Issues**: Connections were not being properly managed for different operation types
- **No Connection Health Monitoring**: Failed connections were not being detected and recovered automatically

## Solution Architecture

### 1. Optimized Connection Pool Configuration

#### API Service Configuration (`src/api/redis_config.py`)
```python
class OptimizedRedisConfig:
    MAX_CONNECTIONS = 50        # Increased from default 10
    SOCKET_TIMEOUT = 30         # 30 seconds for regular operations
    LONG_RUNNING_TIMEOUT = 120  # 2 minutes for blocking operations
    HEALTH_CHECK_INTERVAL = 30  # Health checks every 30 seconds
```

#### Worker Service Configuration (`src/worker/redis_config.py`)
```python
class WorkerRedisConfig:
    MAX_CONNECTIONS = 30        # Optimized for worker focus
    SOCKET_TIMEOUT = 60         # Longer timeout for task processing
    BLOCKING_TIMEOUT = 300      # 5 minutes for BLPOP operations
    HEALTH_CHECK_INTERVAL = 60  # Health checks every minute
```

### 2. Specialized Redis Clients

#### Long-Running Operations Client
- **Purpose**: Optimized for blocking operations like BLPOP/BRPOP
- **Features**: Extended socket timeouts, automatic timeout calculation
- **Usage**: Queue consumption, task waiting operations

#### Pipeline Operations Client
- **Purpose**: Optimized for batch operations and transactions
- **Features**: Transaction support, optimized timeouts for bulk operations
- **Usage**: State updates, counter management, bulk task operations

#### Task Management Client
- **Purpose**: Optimized for frequent task state updates
- **Features**: Heartbeat management, optimized pipeline operations
- **Usage**: Worker heartbeats, task state transitions

### 3. Connection Lifecycle Management

#### Automatic Connection Recovery
```python
async def execute_with_retry(self, operation, *args, **kwargs):
    """Execute Redis operations with automatic retry logic."""
    for attempt in range(RETRY_ATTEMPTS):
        try:
            return await operation(*args, **kwargs)
        except RETRY_ON_ERROR as e:
            if attempt < RETRY_ATTEMPTS - 1:
                delay = RETRY_BACKOFF.compute(attempt)
                await asyncio.sleep(delay)
```

#### Health Monitoring
```python
async def _health_monitor(self):
    """Background task to monitor Redis connection health."""
    while True:
        await asyncio.sleep(HEALTH_CHECK_INTERVAL)
        await self._redis.ping()
        # Log pool statistics
```

### 4. TCP Keepalive Configuration

```python
SOCKET_KEEPALIVE_OPTIONS = {
    'TCP_KEEPIDLE': 600,    # Start keepalive after 10 minutes idle
    'TCP_KEEPINTVL': 60,    # Keepalive probe interval
    'TCP_KEEPCNT': 3        # Failed probes before connection drop
}
```

## Implementation Details

### API Service Integration

1. **Service Initialization**: Updated `RedisService` to use optimized connection manager
2. **Lifecycle Management**: Proper initialization and cleanup in FastAPI lifespan
3. **Pool Statistics**: Added monitoring endpoints for connection pool health

### Worker Service Integration

1. **Task Processing**: Updated task functions to use optimized Redis connections
2. **Error Handling**: Improved error handling with proper connection cleanup
3. **Heartbeat Management**: Optimized worker heartbeat updates

### Connection Pool Statistics

The system now provides detailed connection pool statistics:

```python
{
    "max_connections": 50,
    "created_connections": 15,
    "available_connections": 12,
    "in_use_connections": 3
}
```

## Performance Benefits

### 1. Increased Throughput
- **50 connections** for API service (vs 10 default)
- **30 connections** for worker service (optimized for focused operations)
- **Reduced connection contention** for concurrent operations

### 2. Improved Reliability
- **Automatic retry logic** with exponential backoff
