#!/bin/bash
# src/worker/start_worker.sh
# Enhanced Celery worker startup script with monitoring and error recovery

set -e  # Exit on any error

# Configure logging
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log "Starting Celery worker with enhanced startup..."
log "Working directory: $(pwd)"
log "User: $(whoami)"

# Function to clean Python bytecode cache
clean_pycache() {
    log "Cleaning Python bytecode cache..."
    
    # Remove all __pycache__ directories and .pyc files
    find /app -name "*.pyc" -delete 2>/dev/null || true
    find /app -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
    
    log "Python bytecode cache cleaned"
}

# Function to check if Redis is available
check_redis() {
    log "Checking Redis connectivity..."
    
    # Use uv to run a simple Redis connectivity check
    if uv run python -c "import redis; r=redis.from_url('${REDIS_URL:-redis://redis:6379/0}'); r.ping(); print('Redis OK')" 2>/dev/null; then
        log "Redis connection: OK"
        return 0
    else
        log "Redis connection: FAILED"
        return 1
    fi
}

# Function to validate imports
validate_imports() {
    log "Validating imports..."
    
    if uv run python -c "
from tasks import app
from config import settings
from circuit_breaker import get_circuit_breaker_status
print('All imports successful')
" 2>/dev/null; then
        log "Import validation: OK"
        return 0
    else
        log "Import validation: FAILED"
        return 1
    fi
}

# Function to start consumer in background
start_consumer() {
    log "Starting Redis queue consumer..."
    
    # Start the consumer in the background
    uv run python consumer.py &
    CONSUMER_PID=$!
    
    log "Consumer started with PID: $CONSUMER_PID"
    echo $CONSUMER_PID > /tmp/consumer.pid
    
    # Give consumer a moment to start
    sleep 2
    
    # Check if consumer is still running
    if kill -0 $CONSUMER_PID 2>/dev/null; then
        log "Consumer is running successfully"
        return 0
    else
        log "Consumer failed to start"
        return 1
    fi
}

# Function to start worker with retry logic
start_worker_with_retry() {
    local max_retries=3
    local retry_delay=5
    
    for attempt in $(seq 1 $max_retries); do
        log "Starting worker (attempt $attempt/$max_retries)..."
        
        # Clean bytecode cache on each retry
        clean_pycache
        
        # Validate imports
        if ! validate_imports; then
            log "Import validation failed"
            if [ $attempt -lt $max_retries ]; then
                log "Retrying in $retry_delay seconds..."
                sleep $retry_delay
                continue
            else
                log "Max retries exceeded. Worker startup failed."
                exit 1
            fi
        fi
        
        # Start the consumer first
        if ! start_consumer; then
            log "Consumer startup failed"
            if [ $attempt -lt $max_retries ]; then
                log "Retrying in $retry_delay seconds..."
                sleep $retry_delay
                continue
            else
                log "Max retries exceeded. Consumer startup failed."
                exit 1
            fi
        fi
        
        # Environment setup
        export CELERY_ENABLE_REMOTE_CONTROL=true
        export PYTHONDONTWRITEBYTECODE=1
        export PYTHONUNBUFFERED=1
        
        # Build Celery command arguments
