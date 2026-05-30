"""Pydantic request/response models for the task queue API."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from task_queue.db.models import TaskStatus


# ============================================================
# Requests
# ============================================================


class CreateTaskRequest(BaseModel):
    """Body of POST /tasks."""

    task_type: str = Field(min_length=1, max_length=64)
    payload: dict[str, Any] = Field(default_factory=dict)
    max_attempts: int = Field(default=3, ge=1, le=10)


# ============================================================
# Responses
# ============================================================


class TaskResponse(BaseModel):
    """Single task DTO."""

    id: UUID
    task_type: str
    payload: dict[str, Any]
    status: TaskStatus
    attempts: int
    max_attempts: int
    last_error: str | None
    result: dict[str, Any] | None
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class TaskListResponse(BaseModel):
    items: list[TaskResponse]
    count: int


class ErrorResponse(BaseModel):
    """Envelope for error responses."""

    error: str
    detail: str
    request_id: str | None = None