# Idempotent Task Queue

A production-shape async task queue built in Python with FastAPI + Postgres + asyncio.
Demonstrates patterns that real payment, webhook, and background-job systems rely on:
**idempotent request handling, atomic task claiming, retry-with-dead-letter,
and full observability**.

## What this demonstrates

- **Stripe-style idempotency** with `Idempotency-Key` header. Same key + same body
  returns the cached response; same key + different body returns 422; concurrent
  requests with the same key are handled via optimistic concurrency control on
  Postgres unique constraints.
- **Postgres as a queue** using `SELECT ... FOR UPDATE SKIP LOCKED`. Multiple
  worker processes can drain the queue in parallel without contention. Scales
  linearly with worker count up to Postgres connection-pool limits.
- **Retry with bounded budget + dead-letter quarantine.** Tasks that fail transiently
  retry up to `max_attempts` times. Tasks that exhaust their budget move to
  `DEAD_LETTER` for operator review. A manual retry endpoint resets dead-lettered
  tasks for re-processing while preserving attempt history.
- **Graceful shutdown.** Workers complete the in-flight task and drain cleanly on
  SIGTERM/SIGINT. No tasks lost on deploys.
- **Two-phase transaction split.** Workers claim tasks in a short transaction, run
  the handler outside any DB transaction, then write the outcome in another short
  transaction. DB connections are never held during long-running handler I/O.

## Architecture

```
┌────────┐  POST /tasks         ┌────────────────┐
│ Client │ ────────────────────▶│  FastAPI app   │
└────────┘  Idempotency-Key:abc │  - validate    │
   ▲                            │  - dedup       │
   │ same key →                 │  - persist     │
   │ same answer                └────────┬───────┘
                                         │
                                         ▼
                              ┌──────────────────────┐
                              │  Postgres            │
                              │  - tasks             │
                              │  - idempotency_keys  │
                              └──────────┬───────────┘
                                         ▲
                                         │ SELECT FOR UPDATE
                                         │ SKIP LOCKED
                              ┌──────────┴───────────┐
                              │  Worker (async)      │
                              │  - poll              │
                              │  - retry/dead-letter │
                              └──────────────────────┘
```

## API surface

| Method | Path | Description |
|---|---|---|
| `POST` | `/tasks` | Submit a task. Requires `Idempotency-Key` header. Returns 202 with task ID. |
| `GET` | `/tasks/{id}` | Get task by ID. 404 if not found. |
| `GET` | `/tasks` | List tasks. Supports `?status=`, `?task_type=`, `?limit=`, `?offset=`. |
| `POST` | `/tasks/{id}/retry` | Reset a `FAILED` or `DEAD_LETTER` task to `PENDING`. 409 if in another state. |
| `GET` | `/health` | Liveness probe. |
| `GET` | `/metrics` | Prometheus metrics. |

Auto-generated OpenAPI docs at `/docs` when running.

## Task lifecycle

```
   ┌─── PENDING ◄─────────────┐
   │       │                  │
   ▼       │ (worker picks)   │ (retry within budget)
PROCESSING │                  │
   │       ▼                  │
   ├─► SUCCEEDED              │
   │                          │
   └─► FAILED (handler raised)┘
           │
           │ (attempts >= max_attempts)
           ▼
       DEAD_LETTER ◄── (POST /tasks/{id}/retry resets to PENDING with +3 max_attempts)
```

## Quick start

```bash
# 1. Bring up Postgres, Prometheus, Jaeger
docker compose up -d

# 2. Apply schema
uv run alembic upgrade head

# 3. Run the API (terminal 1)
uv run uvicorn task_queue.api.app:app --reload --app-dir src --port 8001

# 4. Run the worker (terminal 2)
uv run taskq-worker
\`\`\`

UIs:
- API docs: http://localhost:8001/docs
- Prometheus: http://localhost:9091
- Jaeger: http://localhost:16687

## Submit a task

\`\`\`bash
curl -X POST http://localhost:8001/tasks \\
  -H "Content-Type: application/json" \\
  -H "Idempotency-Key: $(uuidgen)" \\
  -d '{"task_type":"send_email","payload":{"to":"a@example.com","subject":"hi"}}'
```

Available task types: `send_email`, `send_sms`, `webhook`, `always_fail` (test only).
Add new handlers by writing an async function and registering it in
`src/task_queue/worker/handlers.py`.

## Tests

```bash
uv run pytest
```

16 tests covering API endpoints, worker outcomes, and idempotency correctness.
Coverage: 74% (see `coverage report` for line-by-line).

## Benchmark

Load test on a MacBook Air M2, single API process + single worker process,
Postgres in Docker:

| Operation | Throughput | p50 latency | p99 latency |
|---|---|---|---|
| `GET /tasks` (paginated list, 1000 req @ c=50) | **590 req/s** | **67 ms** | **228 ms** |
| `POST /tasks` (with unique idempotency keys) | not measured | — | — |
| Worker drain | not measured | — | — |

The latency distribution is unimodal with a fat tail driven by DB connection
pool contention: with `pool_size=10` and concurrency 50, the requests that
can't immediately check out a connection wait, and that wait time becomes
the p95+ tail. Production tuning would raise `pool_size`, cap incoming
concurrency, or front the DB with PgBouncer.


## What I'd add for production

This is a learning artifact — these are the things I'd add before deploying to
real users:

- **Worker metrics via Pushgateway or `prometheus-client` multi-process mode.**
  Currently worker counters live in the worker process and aren't scraped by
  Prometheus. The API metrics work end-to-end.
- **Idempotency key TTL cleanup.** Records are created with `expires_at` set, but
  no job removes them. A small cron-style task would scan and delete expired
  keys nightly.
- **Distributed tracing across the API/worker boundary.** OTel is set up but
  spans currently don't propagate from API request → worker pickup. Would
  add a `trace_context` column to `tasks` and inject it on pickup.
- **Authentication.** No auth layer; would add JWT middleware with per-tenant
  rate limiting.
- **Worker liveness + horizontal scaling.** Currently one worker process. Multiple
  would just work (FOR UPDATE SKIP LOCKED handles contention), but you'd want
  liveness probes and a coordinator (or just let Kubernetes restart unhealthy pods).

## Why this repo exists

I'm a backend engineer (Go, Kafka, distributed payment systems at NPCI and ShopUp)
transitioning into AI infrastructure roles. This repo is part of a 12-week
roadmap teaching myself Python and modern Python/AI tooling. Built in one
8-hour session on Day 6 of the roadmap.

The same engineering patterns — typed configuration, async-first I/O, structured
logging, observability, idempotency, dead-letter queues — transfer directly to
LLM infrastructure problems, where reliability under transient downstream failure
matters even more than in payments.

## Tech

Python 3.12 · FastAPI · SQLAlchemy 2.0 async · Alembic · asyncpg · structlog ·
OpenTelemetry · Prometheus · Docker Compose · pytest-asyncio
