"""The worker loop — polls Postgres for pending tasks and processes them."""

from __future__ import annotations

import asyncio
import signal
from uuid import UUID

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession

from task_queue.config import get_settings
from task_queue.db.engine import make_engine, make_session_factory
from task_queue.db.models import Task, TaskStatus
from task_queue.worker.handlers import HandlerNotFoundError, get_handler

logger = structlog.get_logger(__name__)


# ============================================================
# The single-task pickup query
# ============================================================


async def claim_one_task(session: AsyncSession) -> Task | None:
    """Atomically grab one pending task and mark it as processing.

    Uses SELECT ... FOR UPDATE SKIP LOCKED so multiple worker processes
    can run concurrently without contention.
    """
    stmt = (
        select(Task)
        .where(Task.status == TaskStatus.PENDING)
        .order_by(Task.created_at)
        .limit(1)
        .with_for_update(skip_locked=True)
    )
    result = await session.execute(stmt)
    task = result.scalar_one_or_none()
    if task is None:
        return None

    # Mark as processing inside the same transaction → atomic claim
    task.status = TaskStatus.PROCESSING
    task.attempts += 1
    return task


# ============================================================
# Outcome handling
# ============================================================


async def mark_succeeded(session: AsyncSession, task: Task, result: dict) -> None:
    task.status = TaskStatus.SUCCEEDED
    task.result = result
    task.last_error = None


async def mark_failed_or_retry(session: AsyncSession, task: Task, error: str) -> None:
    """Decide: should this task retry, or move to dead-letter?"""
    task.last_error = error
    if task.attempts >= task.max_attempts:
        task.status = TaskStatus.DEAD_LETTER
        logger.warning(
            "task_dead_lettered",
            task_id=str(task.id),
            task_type=task.task_type,
            attempts=task.attempts,
            error=error[:200],
        )
    else:
        task.status = TaskStatus.PENDING  # back in queue for retry
        logger.info(
            "task_will_retry",
            task_id=str(task.id),
            attempts=task.attempts,
            max_attempts=task.max_attempts,
            error=error[:200],
        )


# ============================================================
# Process one task end-to-end
# ============================================================


async def process_one(factory: async_sessionmaker[AsyncSession]) -> bool:
    """Try to claim and process one task.

    Returns True if a task was processed (success or failure), False if
    no work was available.
    """
    # ---- Phase 1: claim a task in its own transaction ----
    async with factory() as session:
        task = await claim_one_task(session)
        if task is None:
            return False
        await session.commit()
        # Refresh attributes now that the row is committed
        await session.refresh(task)

    task_id = task.id
    task_type = task.task_type
    payload = task.payload

    structlog.contextvars.bind_contextvars(
        task_id=str(task_id),
        task_type=task_type,
        attempt=task.attempts,
    )
    logger.info("task_picked_up")

    # ---- Phase 2: run the handler OUTSIDE any DB transaction ----
    # We don't want long-running handlers holding DB connections.
    try:
        handler = get_handler(task_type)
        result = await handler(payload)
    except HandlerNotFoundError as e:
        # No handler for this task_type — terminal, dead-letter immediately
        async with factory() as session:
            db_task = await session.get(Task, task_id)
            if db_task is None:
                logger.error("task_disappeared_during_processing")
                structlog.contextvars.unbind_contextvars("task_id", "task_type", "attempt")
                return True
            db_task.status = TaskStatus.DEAD_LETTER
            db_task.last_error = str(e)
            await session.commit()
        logger.error("task_dead_lettered_no_handler", error=str(e))
        structlog.contextvars.unbind_contextvars("task_id", "task_type", "attempt")
        return True
    except Exception as e:
        # ---- Phase 3a: handler failed → write failure outcome ----
        async with factory() as session:
            db_task = await session.get(Task, task_id)
            if db_task is None:
                logger.error("task_disappeared_during_processing")
                structlog.contextvars.unbind_contextvars("task_id", "task_type", "attempt")
                return True
            await mark_failed_or_retry(session, db_task, error=f"{type(e).__name__}: {e}")
            await session.commit()
        structlog.contextvars.unbind_contextvars("task_id", "task_type", "attempt")
        return True

    # ---- Phase 3b: handler succeeded → write success outcome ----
    async with factory() as session:
        db_task = await session.get(Task, task_id)
        if db_task is None:
            logger.error("task_disappeared_during_processing")
            structlog.contextvars.unbind_contextvars("task_id", "task_type", "attempt")
            return True
        await mark_succeeded(session, db_task, result)
        await session.commit()
    logger.info("task_succeeded", result_keys=list(result.keys()))
    structlog.contextvars.unbind_contextvars("task_id", "task_type", "attempt")
    return True


# ============================================================
# The poll loop
# ============================================================


async def run_worker(*, stop_event: asyncio.Event | None = None) -> None:
    """Main worker loop. Polls indefinitely until stop_event is set."""
    settings = get_settings()
    engine = make_engine()
    factory = make_session_factory(engine)
    stop_event = stop_event or asyncio.Event()

    logger.info(
        "worker_starting",
        poll_interval_s=settings.worker_poll_interval_seconds,
    )

    try:
        while not stop_event.is_set():
            did_work = await process_one(factory)
            if not did_work:
                # No tasks available — wait before polling again
                try:
                    await asyncio.wait_for(
                        stop_event.wait(),
                        timeout=settings.worker_poll_interval_seconds,
                    )
                except TimeoutError:
                    pass  # normal: poll interval expired, loop again
            # else: keep going immediately — drain the queue greedily
    finally:
        await engine.dispose()
        logger.info("worker_stopped")


# ============================================================
# Entry point
# ============================================================


def _configure_logging() -> None:
    """Same shape as the API; could be factored into a shared module."""
    import logging
    settings = get_settings()
    logging.basicConfig(level=settings.log_level.value, format="%(message)s")
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.dev.ConsoleRenderer(colors=True)
            if not settings.log_format_json
            else structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, settings.log_level.value)
        ),
    )


def main() -> None:
    _configure_logging()
    stop_event = asyncio.Event()

    def _handle_signal(signum: int, _frame: object) -> None:
        logger.info("received_shutdown_signal", signal=signum)
        stop_event.set()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    asyncio.run(run_worker(stop_event=stop_event))


if __name__ == "__main__":
    main()