# src/api/routers/tasks.py
"""Generic task management API endpoints."""

from fastapi import APIRouter, HTTPException, status, Depends, Query
from typing import Optional
from celery import Celery

from schemas import (
    TaskDetail,
    TaskResponse,
    TaskRetryRequest,
    TaskDeleteResponse,
    TaskListResponse,
    TaskSummaryListResponse,
    TaskState,
    QueueName,
    TaskType,
)
from datetime import datetime
from services import TaskService

router = APIRouter(prefix="/api/v1/tasks", tags=["task-management"])

# Celery app instance (will be initialized in main.py)
celery_app: Celery = None


def get_task_service() -> TaskService:
    """Dependency to get task service from app state."""
    from services import task_service

    # Try to get from global first
    if task_service is not None:
        return task_service

    # If global is None, raise service unavailable
    raise HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail="Task service not available",
    )


@router.get(
    "/", response_model=TaskListResponse, summary="List Tasks with FULL payloads"
)
async def list_tasks(
    task_status: Optional[TaskState] = Query(
        None, description="Filter tasks by status"
    ),
    task_type: Optional[TaskType] = Query(
        None, description="Filter tasks by type (summarize or pdfxtract)"
    ),
    queue: Optional[QueueName] = Query(None, description="Filter tasks by queue"),
    start_date: Optional[datetime] = Query(
        None, description="Start date for filtering"
    ),
    end_date: Optional[datetime] = Query(None, description="End date for filtering"),
    sort_by: str = Query("created_at", description="Field to sort by"),
    sort_order: str = Query("desc", description="Sort order (asc or desc)"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page"),
    task_id: Optional[str] = Query(None, description="Search by task ID"),
    task_svc: TaskService = Depends(get_task_service),
) -> TaskListResponse:
    """
    List Tasks with FULL payloads.
    """
    try:
        result = await task_svc.list_tasks(
            status=task_status,
            task_type=task_type,
            queue=queue,
            start_date=start_date,
            end_date=end_date,
            sort_by=sort_by,
            sort_order=sort_order,
            page=page,
            page_size=page_size,
            task_id=task_id,
        )
        return result
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list tasks: {str(e)}",
        )


@router.get("/summaries/", response_model=TaskSummaryListResponse)
async def list_task_summaries(
    task_status: Optional[TaskState] = Query(
        None, description="Filter tasks by status"
    ),
    task_type: Optional[TaskType] = Query(
        None, description="Filter tasks by type (summarize or pdfxtract)"
    ),
    queue: Optional[QueueName] = Query(None, description="Filter tasks by queue"),
    start_date: Optional[datetime] = Query(
        None, description="Start date for filtering"
    ),
    end_date: Optional[datetime] = Query(None, description="End date for filtering"),
    sort_by: str = Query("created_at", description="Field to sort by"),
    sort_order: str = Query("desc", description="Sort order (asc or desc)"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page"),
    task_id: Optional[str] = Query(None, description="Search by task ID"),
    task_svc: TaskService = Depends(get_task_service),
) -> TaskSummaryListResponse:
    """
    List and filter task summaries (without content field) with pagination, sorting, and advanced filtering.

    This endpoint is useful when you need to list tasks but don't want to include the potentially large
    content field (especially for PDF extraction tasks with base64-encoded files).

    Returns task summaries with content_length and has_result fields instead of the full content and result.
    """
    try:
        result = await task_svc.list_task_summaries(
            status=task_status,
            task_type=task_type,
            queue=queue,
            start_date=start_date,
            end_date=end_date,
