"""Task handlers — pure async functions that process one task each.

Each handler takes the task payload and returns a result dict.
Raises on failure; the worker catches and routes to retry/dead-letter.
"""

from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


# A handler is a function: payload dict → result dict
TaskHandler = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]


# ============================================================
# Handlers
# ============================================================


async def handle_send_email(payload: dict[str, Any]) -> dict[str, Any]:
    """Pretend to send an email. Sleeps to simulate I/O."""
    to = payload.get("to")
    subject = payload.get("subject", "(no subject)")
    if not to:
        raise ValueError("missing 'to' field in payload")

    logger.info("sending_email", to=to, subject=subject)
    await asyncio.sleep(0.5)  # simulate SMTP latency
    return {
        "sent_to": to,
        "subject": subject,
        "provider_message_id": f"smtp-{random.randint(100000, 999999)}",
    }


async def handle_send_sms(payload: dict[str, Any]) -> dict[str, Any]:
    """Pretend to send an SMS. Occasionally fails (transient)."""
    to = payload.get("to")
    if not to:
        raise ValueError("missing 'to' field in payload")

    logger.info("sending_sms", to=to)
    await asyncio.sleep(0.3)

    # Simulate transient failure ~20% of the time
    if random.random() < 0.2:
        raise RuntimeError("simulated SMS gateway timeout")

    return {
        "sent_to": to,
        "provider_message_id": f"sms-{random.randint(100000, 999999)}",
    }


async def handle_webhook(payload: dict[str, Any]) -> dict[str, Any]:
    """Pretend to POST a webhook. Always succeeds for the demo."""
    url = payload.get("url")
    if not url:
        raise ValueError("missing 'url' field in payload")

    logger.info("calling_webhook", url=url)
    await asyncio.sleep(0.2)
    return {"url": url, "status_code": 200}


async def handle_always_fail(payload: dict[str, Any]) -> dict[str, Any]:
    """Test handler — always raises. Use to verify dead-letter behavior."""
    raise RuntimeError("this handler is designed to always fail")


# ============================================================
# Registry
# ============================================================


HANDLERS: dict[str, TaskHandler] = {
    "send_email": handle_send_email,
    "send_sms": handle_send_sms,
    "webhook": handle_webhook,
    "always_fail": handle_always_fail,
}


class HandlerNotFoundError(Exception):
    """Raised when no handler is registered for a task_type."""


def get_handler(task_type: str) -> TaskHandler:
    handler = HANDLERS.get(task_type)
    if handler is None:
        raise HandlerNotFoundError(f"no handler registered for task_type={task_type!r}")
    return handler