"""Unit tests for API Pydantic schemas and queue key map."""
import pytest
from pydantic import ValidationError

from src.api.schemas import (
    TaskCreate,
    TaskResponse,
    TaskState,
    TaskType,
    QueueName,
    QUEUE_KEY_MAP,
)


def test_task_create_requires_non_empty_content():
    task = TaskCreate(content="hello world")
    assert task.content == "hello world"


def test_task_create_rejects_empty_content():
    with pytest.raises(ValidationError):
        TaskCreate(content="")


def test_task_response_uses_enum_state():
    response = TaskResponse(task_id="abc-123", state=TaskState.PENDING)
    assert response.state == TaskState.PENDING
    assert response.task_id == "abc-123"


def test_task_state_values():
    assert TaskState.PENDING.value == "PENDING"
    assert TaskState.DLQ.value == "DLQ"
    assert TaskType.SUMMARIZE.value == "summarize"


def test_queue_key_map_covers_all_queues():
    assert set(QUEUE_KEY_MAP.keys()) == set(QueueName)
    assert QUEUE_KEY_MAP[QueueName.PRIMARY] == "tasks:pending:primary"
    assert QUEUE_KEY_MAP[QueueName.DLQ] == "dlq:tasks"
