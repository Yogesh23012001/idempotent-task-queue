"""ORM models for tasks and idempotency keys."""

from __future__ import annotations

import enum
from datetime import datetime, timezone

from sqlalchemy import (
    JSON,
    DateTime,
    Enum as SAEnum,
    ForeignKey,
    Integer,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship
from uuid import UUID as PyUUID, uuid4

from task_queue.db.engine import Base


class TaskStatus(str, enum.Enum):
    PENDING = "pending"          # accepted, not yet picked up by worker
    PROCESSING = "processing"    # worker has it
    SUCCEEDED = "succeeded"      # done, result stored
    FAILED = "failed"            # all retries exhausted
    DEAD_LETTER = "dead_letter"  # quarantined; manual intervention


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class Task(Base):
    """A unit of work submitted by a client."""

    __tablename__ = "tasks"

    id: Mapped[PyUUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid4,
    )
    # What kind of task — used by the worker to dispatch to the right handler
    task_type: Mapped[str] = mapped_column(String(64), index=True)
    # Free-form payload the worker will read
    payload: Mapped[dict] = mapped_column(JSON, default=dict)
    # Status state machine
    status: Mapped[TaskStatus] = mapped_column(
        SAEnum(TaskStatus, name="task_status"),
        default=TaskStatus.PENDING,
        index=True,
    )
    # Worker-set fields
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, default=3)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    result: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utc_now, index=True,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utc_now, onupdate=_utc_now,
    )


class IdempotencyKey(Base):
    """Stores a (client-supplied key → cached response) mapping.

    Used to deduplicate retried writes. Storing both the request hash
    AND the response lets us:
      1. Replay the original response on retry (same key, same body)
      2. Reject mismatched bodies with a 422 (same key, different body)
    """

    __tablename__ = "idempotency_keys"

    key: Mapped[str] = mapped_column(String(255), primary_key=True)
    request_hash: Mapped[str] = mapped_column(String(64), index=True)
    # The HTTP status code we returned originally
    response_status: Mapped[int] = mapped_column(Integer)
    # The full JSON response body
    response_body: Mapped[dict] = mapped_column(JSON)
    # The task this key created (for traceability)
    task_id: Mapped[PyUUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("tasks.id", ondelete="SET NULL"),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utc_now, index=True,
    )
    # When this entry should be garbage-collected
    expires_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), index=True,
    )