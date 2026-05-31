"""Shared pytest fixtures for the task queue test suite."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from task_queue.api.app import app
from task_queue.db.engine import make_engine, make_session_factory


@pytest_asyncio.fixture
async def engine() -> AsyncIterator[AsyncEngine]:
    """Per-test engine. Truncates tables to ensure isolation."""
    eng = make_engine()
    # Clean slate
    async with eng.begin() as conn:
        await conn.execute(
            text("TRUNCATE TABLE idempotency_keys, tasks RESTART IDENTITY CASCADE")
        )
    yield eng
    await eng.dispose()


@pytest_asyncio.fixture
async def session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return make_session_factory(engine)


@pytest.fixture
def client(session_factory: async_sessionmaker[AsyncSession]) -> TestClient:
    """TestClient wired to the same session factory as the engine fixture."""
    # Inject the session_factory into app.state so dependency picks it up
    app.state.session_factory = session_factory
    with TestClient(app) as c:
        yield c