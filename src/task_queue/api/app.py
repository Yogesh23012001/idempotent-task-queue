"""FastAPI app entry point."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from uuid import uuid4

import structlog
from fastapi import FastAPI, Request

from task_queue.api.routes import router as tasks_router
from task_queue.config import get_settings
from task_queue.db.engine import make_engine, make_session_factory


def _configure_logging() -> None:
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


_configure_logging()
logger = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    logger.info("starting_up")
    app.state.engine = make_engine()
    app.state.session_factory = make_session_factory(app.state.engine)
    yield
    logger.info("shutting_down")
    await app.state.engine.dispose()


app = FastAPI(
    title="Idempotent Task Queue",
    version="0.1.0",
    lifespan=lifespan,
)


@app.middleware("http")
async def request_id_middleware(request: Request, call_next):
    request_id = request.headers.get("X-Request-ID") or str(uuid4())
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(
        request_id=request_id,
        method=request.method,
        path=request.url.path,
    )
    response = await call_next(request)
    response.headers["X-Request-ID"] = request_id
    return response


app.include_router(tasks_router)