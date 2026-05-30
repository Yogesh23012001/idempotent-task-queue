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
from task_queue.db.models import IdempotencyKey, Task

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
                logger.info(
                    "idempotent_replay",
                    idempotency_key=idempotency_key,
                    cached_status=existing.response_status,
                )
                return existing.response_body

            # 2b. Same key, DIFFERENT body → reject
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

        # 6. Single commit — both task and idempotency record together
        try:
            await session.commit()
        except IntegrityError as e:
            # Race: another request raced us with the same key between
            # our SELECT and INSERT. Roll back and re-fetch the winner.
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


def _utc_now_factory():
    """Local helper — imports stay clean."""
    from task_queue.db.models import _utc_now
    return _utc_now()