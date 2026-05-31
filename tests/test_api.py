"""API endpoint tests."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient


# ============================================================
# Health
# ============================================================


def test_health(client: TestClient) -> None:
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


# ============================================================
# POST /tasks
# ============================================================


def test_create_task_returns_202_with_full_response(client: TestClient) -> None:
    response = client.post(
        "/tasks",
        headers={"Idempotency-Key": "k1"},
        json={
            "task_type": "send_email",
            "payload": {"to": "a@b.com", "subject": "hi"},
        },
    )
    assert response.status_code == 202
    body = response.json()
    assert body["task_type"] == "send_email"
    assert body["status"] == "pending"
    assert body["attempts"] == 0
    assert body["max_attempts"] == 3
    assert "id" in body
    assert "created_at" in body


def test_create_task_missing_idempotency_key_returns_422(client: TestClient) -> None:
    response = client.post(
        "/tasks",
        json={"task_type": "send_email", "payload": {}},
    )
    assert response.status_code == 422
    # FastAPI reports missing header validation errors
    assert any(
        err["loc"] == ["header", "Idempotency-Key"]
        for err in response.json()["detail"]
    )


def test_idempotent_replay_returns_identical_response(client: TestClient) -> None:
    body = {"task_type": "send_email", "payload": {"to": "x@y.com"}}
    first = client.post("/tasks", headers={"Idempotency-Key": "replay-key"}, json=body)
    second = client.post("/tasks", headers={"Idempotency-Key": "replay-key"}, json=body)

    assert first.status_code == 202
    assert second.status_code == 202
    # IDs are identical — second call did not create a new task
    assert first.json()["id"] == second.json()["id"]
    assert first.json()["created_at"] == second.json()["created_at"]


def test_idempotency_key_with_different_body_rejected(client: TestClient) -> None:
    client.post(
        "/tasks",
        headers={"Idempotency-Key": "k-mismatch"},
        json={"task_type": "send_email", "payload": {"to": "a@b.com"}},
    )
    response = client.post(
        "/tasks",
        headers={"Idempotency-Key": "k-mismatch"},
        json={"task_type": "send_sms", "payload": {"to": "x"}},
    )
    assert response.status_code == 422
    assert "different request body" in response.json()["detail"]


# ============================================================
# GET /tasks/{id}
# ============================================================


def test_get_task_returns_200(client: TestClient) -> None:
    created = client.post(
        "/tasks",
        headers={"Idempotency-Key": "get-test-1"},
        json={"task_type": "webhook", "payload": {"url": "https://x.com"}},
    )
    task_id = created.json()["id"]
    response = client.get(f"/tasks/{task_id}")
    assert response.status_code == 200
    assert response.json()["id"] == task_id


def test_get_unknown_task_returns_404(client: TestClient) -> None:
    response = client.get("/tasks/00000000-0000-0000-0000-000000000000")
    assert response.status_code == 404


# ============================================================
# GET /tasks (list)
# ============================================================


def test_list_tasks_filters_by_status(client: TestClient) -> None:
    # Create 3 tasks
    for i in range(3):
        client.post(
            "/tasks",
            headers={"Idempotency-Key": f"list-key-{i}"},
            json={"task_type": "send_email", "payload": {"to": f"a{i}@b.com"}},
        )

    response = client.get("/tasks?status=pending")
    assert response.status_code == 200
    items = response.json()["items"]
    assert len(items) == 3
    assert all(t["status"] == "pending" for t in items)


def test_list_tasks_pagination(client: TestClient) -> None:
    for i in range(5):
        client.post(
            "/tasks",
            headers={"Idempotency-Key": f"page-{i}"},
            json={"task_type": "send_email", "payload": {}},
        )
    response = client.get("/tasks?limit=2")
    assert response.status_code == 200
    assert len(response.json()["items"]) == 2


# ============================================================
# POST /tasks/{id}/retry
# ============================================================


def test_retry_pending_task_returns_409(client: TestClient) -> None:
    created = client.post(
        "/tasks",
        headers={"Idempotency-Key": "retry-pending"},
        json={"task_type": "send_email", "payload": {}},
    )
    task_id = created.json()["id"]
    response = client.post(f"/tasks/{task_id}/retry")
    assert response.status_code == 409
    assert "FAILED or DEAD_LETTER" in response.json()["detail"]


def test_retry_unknown_task_returns_404(client: TestClient) -> None:
    response = client.post("/tasks/00000000-0000-0000-0000-000000000000/retry")
    assert response.status_code == 404
    