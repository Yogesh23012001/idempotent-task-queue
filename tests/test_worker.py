"""Worker logic tests."""

from __future__ import annotations

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession

from task_queue.db.models import Task, TaskStatus
from task_queue.worker.runner import process_one


# ============================================================
# Helpers
# ============================================================


async def insert_task(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    task_type: str,
    payload: dict | None = None,
    max_attempts: int = 3,
) -> Task:
    async with session_factory() as session:
        task = Task(
            task_type=task_type,
            payload=payload or {},
            max_attempts=max_attempts,
        )
        session.add(task)
        await session.commit()
        await session.refresh(task)
        return task


async def fetch_task(
    session_factory: async_sessionmaker[AsyncSession], task_id
) -> Task:
    async with session_factory() as session:
        result = await session.execute(select(Task).where(Task.id == task_id))
        return result.scalar_one()


# ============================================================
# Tests
# ============================================================


@pytest.mark.asyncio
async def test_worker_processes_email_task_to_success(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    task = await insert_task(
        session_factory,
        task_type="send_email",
        payload={"to": "yogesh@example.com", "subject": "test"},
    )

    did_work = await process_one(session_factory)

    assert did_work is True
    refreshed = await fetch_task(session_factory, task.id)
    assert refreshed.status == TaskStatus.SUCCEEDED
    assert refreshed.attempts == 1
    assert refreshed.result is not None
    assert refreshed.result["sent_to"] == "yogesh@example.com"


@pytest.mark.asyncio
async def test_worker_returns_false_when_queue_empty(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    # No tasks inserted — fresh DB after truncate
    did_work = await process_one(session_factory)
    assert did_work is False


@pytest.mark.asyncio
async def test_worker_fails_task_with_missing_payload(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    task = await insert_task(
        session_factory,
        task_type="send_email",
        payload={},  # missing 'to' field — handler will raise
        max_attempts=1,  # one shot
    )
    await process_one(session_factory)

    refreshed = await fetch_task(session_factory, task.id)
    # max_attempts=1 + 1 attempt that failed → dead_letter
    assert refreshed.status == TaskStatus.DEAD_LETTER
    assert refreshed.attempts == 1
    assert refreshed.last_error is not None
    assert "missing 'to'" in refreshed.last_error


@pytest.mark.asyncio
async def test_worker_retries_then_dead_letters(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    task = await insert_task(
        session_factory,
        task_type="always_fail",
        payload={},
        max_attempts=2,
    )
    # First attempt → should retry
    await process_one(session_factory)
    refreshed = await fetch_task(session_factory, task.id)
    assert refreshed.status == TaskStatus.PENDING
    assert refreshed.attempts == 1

    # Second attempt → should dead-letter
    await process_one(session_factory)
    refreshed = await fetch_task(session_factory, task.id)
    assert refreshed.status == TaskStatus.DEAD_LETTER
    assert refreshed.attempts == 2


@pytest.mark.asyncio
async def test_worker_dead_letters_on_unknown_task_type(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    task = await insert_task(
        session_factory,
        task_type="this_handler_does_not_exist",
        payload={},
        max_attempts=5,
    )
    await process_one(session_factory)

    refreshed = await fetch_task(session_factory, task.id)
    # Missing handler is terminal — dead-letter immediately
    assert refreshed.status == TaskStatus.DEAD_LETTER
    assert "no handler registered" in refreshed.last_error