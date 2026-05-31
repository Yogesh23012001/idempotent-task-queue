"""Task management routes."""

from __future__ import annotations

from datetime import timedelta
from typing import Annotated
from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, Header, HTTPException, Path, Request
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from task_queue.api.idempotency import canonical_request_hash
from task_queue.api.model import (
    CreateTaskRequest,
    TaskListResponse,
    TaskResponse,
)
from task_queue.config import get_settings
from task_queue.db.models import IdempotencyKey, Task, TaskStatus

from task_queue.observability.metrics import idempotency_events_total

logger = structlog.get_logger(__name__)
router = APIRouter(tags=["tasks"])


# ============================================================
# Dependency
# ============================================================


def get_session_factory(request: Request) -> async_sessionmaker[AsyncSession]:
    factory: async_sessionmaker[AsyncSession] | None = getattr(
        request.app.state, "session_factory", None,
    )
    if factory is None:
        raise RuntimeError("session_factory not initialized — check lifespan")
    return factory


SessionFactoryDep = Annotated[
    async_sessionmaker[AsyncSession], Depends(get_session_factory)
]


# ============================================================
# Health
# ============================================================


@router.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


# ============================================================
# POST /tasks  — idempotent task creation
# ============================================================


@router.post("/tasks", status_code=202)
async def create_task(
    payload: CreateTaskRequest,
    factory: SessionFactoryDep,
    idempotency_key: Annotated[
        str,
        Header(
            alias="Idempotency-Key",
            min_length=1,
            max_length=255,
            description="Client-generated UUID. Same key + same body returns cached response.",
        ),
    ],
) -> dict:
    """Submit a task. Idempotent on `Idempotency-Key` header."""
    settings = get_settings()
    request_hash = canonical_request_hash(payload.model_dump())

    async with factory() as session:
        # 1. Check if this idempotency key already exists
        stmt = select(IdempotencyKey).where(IdempotencyKey.key == idempotency_key)
        result = await session.execute(stmt)
        existing = result.scalar_one_or_none()

        if existing is not None:
            # 2a. Same key, same body → replay cached response
            if existing.request_hash == request_hash:
                idempotency_events_total.labels(outcome="replay").inc()
                logger.info(
                    "idempotent_replay",
                    idempotency_key=idempotency_key,
                    cached_status=existing.response_status,
                )
                return existing.response_body

            # 2b. Same key, DIFFERENT body → reject
            idempotency_events_total.labels(outcome="mismatch").inc()
            logger.warning(
                "idempotency_key_mismatch",
                idempotency_key=idempotency_key,
                stored_hash=existing.request_hash[:16],
                new_hash=request_hash[:16],
            )
            raise HTTPException(
                status_code=422,
                detail=(
                    "idempotency key was previously used with a different request body"
                ),
            )

        # 3. New key → create the task
        task = Task(
            task_type=payload.task_type,
            payload=payload.payload,
            max_attempts=payload.max_attempts,
        )
        session.add(task)
        await session.flush()  # populates task.id without committing

        # 4. Build the response we'll return AND cache
        response_body = TaskResponse.model_validate(task).model_dump(mode="json")

        # 5. Store the idempotency record alongside the task
        now = _utc_now_factory()
        idem = IdempotencyKey(
            key=idempotency_key,
            request_hash=request_hash,
            response_status=202,
            response_body=response_body,
            task_id=task.id,
            created_at=now,
            expires_at=now + timedelta(seconds=settings.idempotency_ttl_seconds),
        )
        session.add(idem)
        idempotency_events_total.labels(outcome="new").inc()
        # 6. Single commit — both task and idempotency record together
        try:
            await session.commit()
        except IntegrityError as e:
            # Race: another request raced us with the same key between
            # our SELECT and INSERT. Roll back and re-fetch the winner.
            idempotency_events_total.labels(outcome="race").inc()
            await session.rollback()
            logger.warning(
                "idempotency_race_detected", idempotency_key=idempotency_key,
            )
            result = await session.execute(
                select(IdempotencyKey).where(IdempotencyKey.key == idempotency_key)
            )
            winner = result.scalar_one()
            if winner.request_hash == request_hash:
                return winner.response_body
            raise HTTPException(
                status_code=422,
                detail="idempotency key was concurrently used with a different body",
            ) from e

        logger.info(
            "task_created",
            task_id=str(task.id),
            task_type=task.task_type,
            idempotency_key=idempotency_key,
        )

        return response_body

@router.get("/tasks/{task_id}", response_model=TaskResponse)
async def get_task(
    factory: SessionFactoryDep,
    task_id: Annotated[UUID, Path(description="Task UUID")],
) -> Task:
    """Retrieve a single task by ID."""
    async with factory() as session:
        result = await session.execute(select(Task).where(Task.id == task_id))
        task = result.scalar_one_or_none()
        if task is None:
            raise HTTPException(status_code=404, detail=f"task {task_id} not found")
        return task

def _utc_now_factory():
    """Local helper — imports stay clean."""
    from task_queue.db.models import _utc_now
    return _utc_now()

from typing import Annotated
from fastapi import Query


@router.get("/tasks", response_model=TaskListResponse)
async def list_tasks(
    factory: SessionFactoryDep,
    status_filter: Annotated[
        TaskStatus | None,
        Query(
            alias="status",
            description="Filter by task status",
        ),
    ] = None,
    task_type: Annotated[
        str | None,
        Query(min_length=1, max_length=64, description="Filter by task type"),
    ] = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> TaskListResponse:
    """List tasks, newest first. Supports pagination and filtering."""
    async with factory() as session:
        stmt = select(Task).order_by(Task.created_at.desc())
        if status_filter is not None:
            stmt = stmt.where(Task.status == status_filter)
        if task_type is not None:
            stmt = stmt.where(Task.task_type == task_type)

        # Apply pagination
        stmt = stmt.limit(limit).offset(offset)

        result = await session.execute(stmt)
        items = list(result.scalars().all())
        return TaskListResponse(items=items, count=len(items))   # type: ignore[arg-type]

@router.post("/tasks/{task_id}/retry", response_model=TaskResponse)
async def retry_task(
    factory: SessionFactoryDep,
    task_id: Annotated[UUID, Path()],
) -> Task:
    """Reset a failed task to pending so the worker re-attempts it.

    Only valid for tasks in FAILED or DEAD_LETTER status.
    """
    async with factory() as session:
        result = await session.execute(select(Task).where(Task.id == task_id))
        task = result.scalar_one_or_none()
        if task is None:
            raise HTTPException(status_code=404, detail=f"task {task_id} not found")

        if task.status not in {TaskStatus.FAILED, TaskStatus.DEAD_LETTER}:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"cannot retry task in status={task.status.value}; "
                    f"only FAILED or DEAD_LETTER tasks can be retried"
                ),
            )

        # Reset state for re-processing
        task.status = TaskStatus.PENDING
        task.last_error = None
        # Note: we DO NOT reset attempts — we want the history.
        # We bump max_attempts so the worker has fresh retries budget.
        task.max_attempts += 3

        await session.commit()
        await session.refresh(task)

        logger.info(
            "task_manually_retried",
            task_id=str(task.id),
            attempts=task.attempts,
            new_max_attempts=task.max_attempts,
        )
        return task