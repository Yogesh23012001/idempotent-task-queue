"""Prometheus metrics for both API and worker."""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

# ============================================================
# API metrics
# ============================================================


http_requests_total = Counter(
    "http_requests_total",
    "Total HTTP requests by method, path template, status",
    labelnames=("method", "path", "status"),
)

http_request_duration_seconds = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds",
    labelnames=("method", "path"),
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

http_requests_in_flight = Gauge(
    "http_requests_in_flight",
    "HTTP requests currently being processed",
)


# ============================================================
# Idempotency metrics
# ============================================================


idempotency_events_total = Counter(
    "idempotency_events_total",
    "Idempotency check outcomes",
    labelnames=("outcome",),    # "new" | "replay" | "mismatch" | "race"
)


# ============================================================
# Worker metrics
# ============================================================


tasks_processed_total = Counter(
    "tasks_processed_total",
    "Tasks processed by the worker, labeled by outcome",
    labelnames=("task_type", "outcome"),     # outcome: succeeded|will_retry|dead_letter
)

task_handler_duration_seconds = Histogram(
    "task_handler_duration_seconds",
    "Time taken to execute a task handler",
    labelnames=("task_type",),
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0),
)

worker_polls_total = Counter(
    "worker_polls_total",
    "Worker poll attempts",
    labelnames=("outcome",),     # "claimed" | "empty"
)