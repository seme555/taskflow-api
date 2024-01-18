# src/api/routers/workers.py
"""Worker management API endpoints."""

from datetime import datetime
from fastapi import APIRouter, Request

from services import health_service

router = APIRouter(prefix="/api/v1/workers", tags=["workers-management"])


def get_health_service(request: Request):
    """Get health service from global variable or app state."""
    # Try global service first
    if health_service:
        return health_service

    # Fallback to app state (for reload mode)
    return getattr(request.app.state, "health_service", None)


@router.get("/")
async def get_worker_status(request: Request) -> dict:
    """
    Get detailed health from all workers including real circuit breaker status.

    Uses Celery broadcast to get actual worker health and circuit breaker status,
    with Redis heartbeats as fallback for basic connectivity.
    """
    try:
        current_health_service = get_health_service(request)

        if not current_health_service:
            return {
                "error": "Health service not initialized",
                "total_workers": 0,
                "timestamp": datetime.utcnow().isoformat(),
            }

        # Try to get real worker health via Celery broadcast first
        worker_details = []
        try:
            from main import celery_app

            # Use broadcast to get health from all active workers
            broadcast_results = celery_app.control.broadcast(
                "get_worker_health", reply=True, timeout=5.0
            )

            if broadcast_results:
                for result in broadcast_results:
                    if result and isinstance(result, dict):
                        # Extract worker data from nested structure
                        for worker_name, worker_data in result.items():
                            if (
                                isinstance(worker_data, dict)
                                and "worker_id" in worker_data
                            ):
                                # Add worker name for reference
                                worker_data["worker_name"] = worker_name

                                # Determine overall worker status based on circuit breaker
                                cb_state = worker_data.get("circuit_breaker", {}).get(
                                    "state", "unknown"
                                )
                                if cb_state == "open":
                                    worker_data["status"] = "unhealthy"
                                elif cb_state in ["closed", "half-open"]:
                                    worker_data["status"] = "healthy"
                                else:
                                    worker_data["status"] = "unknown"

                                worker_details.append(worker_data)

        except Exception as broadcast_error:
            # If broadcast fails, we'll fall back to heartbeat-only method
            print(f"Broadcast failed: {broadcast_error}")

        # If we didn't get worker details from broadcast, fall back to heartbeat method
        if not worker_details:
            # Get all heartbeat keys
            heartbeat_keys = []
            async for key in current_health_service.redis_service.redis.scan_iter(
                "worker:heartbeat:*"
            ):
                heartbeat_keys.append(key)

            if not heartbeat_keys:
                return {
                    "error": "No worker heartbeats found - workers may not be running",
                    "total_workers": 0,
                    "timestamp": datetime.utcnow().isoformat(),
                }

            # Check each heartbeat
            import time

            current_time = time.time()

            for key in heartbeat_keys:
                worker_id = key.split(":", 2)[2]  # Extract worker ID from key
                try:
                    heartbeat_time = (
                        await current_health_service.redis_service.redis.get(key)
                    )
                    if heartbeat_time:
                        heartbeat_timestamp = float(heartbeat_time)
                        age = current_time - heartbeat_timestamp
                        is_healthy = (
                            age < 60
                        )  # Consider healthy if heartbeat within last 60 seconds

                        worker_details.append(
                            {
                                "worker_id": worker_id,
                                "status": "healthy" if is_healthy else "stale",
                                "last_heartbeat": heartbeat_timestamp,
                                "heartbeat_age_seconds": round(age, 2),
                                "circuit_breaker": {
                                    "state": "unknown",
                                    "note": "Circuit breaker status unavailable - worker not responding to broadcast",
                                },
                            }
                        )
                    else:
