# Webhook Delivery Service

[![CI](https://github.com/ardacanaltundasar/webhook-delivery-service/actions/workflows/ci.yml/badge.svg)](https://github.com/ardacanaltundasar/webhook-delivery-service/actions/workflows/ci.yml)

An outbound webhook gateway. Internal services publish events to it over HTTP. The gateway fans each event out to every customer endpoint subscribed to that event type, signs each request, retries failures with backoff, and keeps an audit log of every attempt.

Built with FastAPI, PostgreSQL, Redis and Celery.

## Why a separate service

Sending webhooks inline from business code works until a customer endpoint is slow, down or rate limited. Then the producer has to deal with timeouts, retries, duplicate sends and "did they ever get it?" support tickets. This service takes on those concerns:

- Producers make one fast, idempotent call and move on.
- Delivery is asynchronous, retried and rate limited per customer endpoint.
- Every attempt is recorded with status code, latency and a response excerpt, so failures can be debugged and replayed.

## Architecture

```
                         +-------------------+
 internal service ------>|  FastAPI (api)    |---- Idempotency-Key ----> Redis (db 0)
   POST /events          |                   |
                         |  1. store event   |---- events, deliveries --> PostgreSQL
                         |  2. fan out       |
                         |  3. enqueue       |---- task per delivery ---> Redis (db 1, Celery broker)
                         +-------------------+                                  |
                                                                                v
                         +-------------------+                         +-----------------+
 customer endpoint <-----|  httpx POST       |<------------------------|  Celery worker  |
   (signed request)      |  + HMAC headers   |   rate limit check ---->|  (prefork)      |
                         +-------------------+   (Redis sliding window) +-----------------+
                                                                                |
                                                     delivery_attempts, status  v
                                                                           PostgreSQL
```

There are four tables:

| Table | Purpose |
|---|---|
| `subscriptions` | Target URL, subscribed event types (`ARRAY`), signing secret, per-minute rate limit, active flag |
| `events` | Immutable event payloads (`JSONB`) with an optional unique `idempotency_key` |
| `deliveries` | One row per (event, subscription) pair holding the current state of that delivery |
| `delivery_attempts` | Append-only log: attempt number, status code, latency, response excerpt, error |

### Delivery lifecycle

```
            2xx
 PENDING ----------------------------------------------> DELIVERED
    |
    | 429 / 5xx / timeout / network error
    v
 RETRYING --(backoff, up to DELIVERY_MAX_ATTEMPTS)--+--> DELIVERED
    |                                               |
    | budget exhausted, or 3xx/4xx other than 429   |
    v                                               |
 FAILED  <------------------------------------------+
    |
    | POST /deliveries/{id}/retry
    v
 RETRYING
```

## Design decisions

### Idempotent ingestion in two layers

`POST /events` accepts an optional `Idempotency-Key` header, which goes through a three-step protocol in Redis:

1. **claim**: `SET key __pending__ NX EX 30`. Only one request can win. A concurrent request carrying the same key gets `409 Conflict`.
2. **complete**: after the database commit, the value is replaced with the event id and given the full retention TTL (24h by default). A later request with the same key gets `200` and the original response, and no new deliveries are created.
3. **release**: if ingestion fails, the key is deleted so the client can retry.

The pending marker has a short TTL on purpose. If the API process crashes mid-request, the key unlocks within seconds instead of staying blocked for a day.

Redis is the fast path but not the source of truth. `events.idempotency_key` also has a unique constraint. If the Redis entry was evicted or expired, the insert hits the constraint, the API returns the existing event with `200`, and the Redis entry is restored.

### Signed requests (HMAC-SHA256)

Each subscription gets a random secret (`whsec_...`). The API returns it only once, in the creation response. Every outbound request carries:

| Header | Value |
|---|---|
| `X-Webhook-Id` | Event id. Stable across retries. |
| `X-Webhook-Event` | Event type, e.g. `order.created` |
| `X-Webhook-Delivery` | Delivery id |
| `X-Webhook-Attempt` | Attempt number, starting at 1 |
| `X-Webhook-Timestamp` | Unix timestamp of this attempt |
| `X-Webhook-Signature-256` | `sha256=` + hex HMAC-SHA256 of `"{timestamp}.{raw body}"` |

The signature covers the timestamp as well as the body. A captured request therefore cannot be replayed later with a fresh timestamp, and receivers can reject anything outside a tolerance window (300 seconds by default). A receiver can verify a request like this:

```python
import hashlib
import hmac
import time


def verify(secret: str, body: bytes, timestamp: str, signature: str, tolerance: int = 300) -> bool:
    if abs(time.time() - int(timestamp)) > tolerance:
        return False
    expected = hmac.new(secret.encode(), f"{timestamp}.".encode() + body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(f"sha256={expected}", signature)
```

The body is a small envelope around the producer's payload:

```json
{"id": "6f1c...", "type": "order.created", "created_at": "2026-10-25T19:33:40+00:00", "data": {"order_id": 42}}
```

Internal callers authenticate with a shared `X-API-Key`. The key is compared in constant time with `secrets.compare_digest`.

### At-least-once delivery

The service guarantees at-least-once delivery, not exactly-once. A worker can crash after the receiver accepted a request but before the attempt was recorded. Celery's `acks_late` then redelivers the task and the receiver sees the request again. Exactly-once delivery over HTTP is not achievable without the receiver's cooperation, so receivers should deduplicate on `X-Webhook-Id`.

On the gateway side, duplicates are kept out of the audit log. `delivery_attempts` has a unique constraint on `(delivery_id, attempt_number)`. If two workers race on the same attempt, the second one hits the constraint and drops its result without changing the delivery state.

### Retries: exponential backoff with jitter

A response's status decides whether the delivery is retried:

| Outcome | Classification |
|---|---|
| `2xx` | Delivered |
| `429`, `5xx`, timeout, connection error | Retryable |
| `3xx`, other `4xx` | Permanent failure. Redirects are not followed, so a delivery never lands on a URL other than the registered one. |

The retry delay uses "equal jitter":

```
ceiling = min(RETRY_BACKOFF_MAX_SECONDS, RETRY_BACKOFF_BASE_SECONDS * 2 ** (attempt - 1))
delay   = ceiling / 2 + uniform(0, ceiling / 2)
```

Half of the exponential delay is a guaranteed floor, so retries never collapse to zero. The other half is randomized, so the many deliveries that failed during the same outage do not all hit the recovering receiver at the same moment. If the receiver sends a `Retry-After` header, the delay becomes the larger of the two, still capped at the maximum.

The attempt budget (`DELIVERY_MAX_ATTEMPTS`, default 5) is tracked in PostgreSQL, not in Celery's `max_retries`. That way rate-limit deferrals do not consume attempts, and the budget survives task redelivery.

### Per-subscription rate limiting

Before every HTTP call, the worker checks a per-subscription sliding-window log stored as a Redis sorted set. Trimming the window, counting, and inserting run in a single Lua script, so the check is atomic across all workers. The timestamp comes from Redis `TIME` rather than the worker clock, so clock skew between worker hosts cannot widen or narrow the window. A fixed-window counter is cheaper, but it lets through up to twice the limit around a window boundary.

When the limit is hit, the task is re-scheduled for the moment the oldest entry leaves the window, plus a little jitter. No HTTP request is made, no attempt is recorded, and the attempt budget is not consumed.

### Fan-out lookup with a GIN index

`subscriptions.event_types` is a PostgreSQL array with a GIN index. Finding the subscribers for an event is a single indexed query:

```sql
SELECT id FROM subscriptions WHERE is_active AND event_types @> ARRAY['order.created'];
```

The alternative is a join table. The array keeps the event-type list next to the rest of the subscription and keeps the read path a single query.

### Manual retry with row locking

`POST /deliveries/{id}/retry` loads the delivery with `SELECT ... FOR UPDATE`, checks that it is eligible, flips it to `RETRYING` and commits before enqueueing. If two operators click retry at the same time, the requests are serialized on the row lock, and the second one sees the new state and gets `409`.

A delivery is eligible for manual retry if it is in one of these states:

- `FAILED`. When the attempt budget is already spent, this grants one more attempt, similar to a "redeliver" button.
- `PENDING` or `RETRYING`, but at least `STUCK_DELIVERY_THRESHOLD_SECONDS` past its scheduled time. This recovers deliveries whose task was lost, for example when the broker was unreachable at enqueue time. Before that threshold the queued task is still expected to run, so a manual retry would only race it.

### Async code inside a sync Celery worker

Celery tasks are synchronous, while the data layer (asyncpg, redis.asyncio, httpx) is async. Calling `asyncio.run()` per task would create and close an event loop each time, which leaves pooled connections bound to a dead loop. Instead, each prefork child process owns one event loop and one set of clients (`WorkerContext`) for its lifetime. Tasks run on that loop with `run_until_complete`. The context is rebuilt if the PID changes after a fork, and closed on `worker_process_shutdown`. This is also why the worker pool is pinned to `prefork`.

Each attempt is split into three steps so that no database transaction stays open during network I/O:

1. A short transaction reads the delivery into an immutable snapshot.
2. The HTTP call runs with no transaction open.
3. A second short transaction writes the attempt row and the new delivery state.

The HTTP call also has three safeguards:

- It runs under an overall `asyncio.timeout`, because httpx timeouts apply per phase, not to the whole request.
- The response body is streamed and cut off at `RESPONSE_SNIPPET_MAX_LENGTH`, so a hostile or broken receiver cannot exhaust worker memory.
- Celery runs with `acks_late`, `prefetch_multiplier=1` and a Redis `visibility_timeout` larger than the longest backoff. Without that, countdown tasks still waiting in a worker would be redelivered by the broker and run twice.

## API

All endpoints except `/health` require the `X-API-Key` header. Interactive docs are served at `/docs`.

| Method | Path | Description |
|---|---|---|
| `POST` | `/subscriptions` | Create a subscription. The response includes the signing secret (only time it is shown). |
| `GET` | `/subscriptions` | List subscriptions. Filters: `is_active`, `event_type`, `limit`, `offset` |
| `GET` | `/subscriptions/{id}` | Get a subscription |
| `PATCH` | `/subscriptions/{id}` | Update target URL, event types, rate limit, description or active flag |
| `DELETE` | `/subscriptions/{id}` | Delete a subscription and its delivery history |
| `POST` | `/events` | Ingest an event. `202` when new, `200` on an idempotent replay, `409` if the same key is in flight |
| `GET` | `/events/{id}` | Get an event |
| `GET` | `/deliveries` | List deliveries with their attempts. Filters: `event_id`, `subscription_id`, `status`, `limit`, `offset` |
| `GET` | `/deliveries/stats` | Counts by status, success rate, average and p95 latency. Filters: `subscription_id`, `since` |
| `GET` | `/deliveries/{id}` | Get a delivery with its full attempt history |
| `POST` | `/deliveries/{id}/retry` | Re-queue a failed or stuck delivery |
| `GET` | `/health` | Database and Redis connectivity (`503` when degraded) |

In `/deliveries/stats`, the success rate is `delivered / (delivered + failed)`; deliveries that are still in flight are excluded. Latency figures only include attempts that received an HTTP response, so timeouts do not drag the average toward the client timeout.

## Running locally

```bash
docker compose up --build
```

This starts PostgreSQL 16, Redis, a one-shot `migrate` container (`alembic upgrade head`), the API on `localhost:8000` and a Celery worker. The API and the worker start only after the migration completes and Redis reports healthy. The default API key is `local-dev-api-key`; override it with `API_KEY=... docker compose up`.

Create a subscription and send an event:

```bash
curl -s -X POST localhost:8000/subscriptions \
  -H "X-API-Key: local-dev-api-key" -H "Content-Type: application/json" \
  -d '{"target_url": "https://webhook.site/<your-id>", "event_types": ["order.created"], "rate_limit_per_minute": 120}'

curl -s -X POST localhost:8000/events \
  -H "X-API-Key: local-dev-api-key" -H "Content-Type: application/json" \
  -H "Idempotency-Key: order-1001-created" \
  -d '{"event_type": "order.created", "payload": {"order_id": 1001, "total": "49.90"}}'

curl -s "localhost:8000/deliveries?status=DELIVERED" -H "X-API-Key: local-dev-api-key"
```

To deliver to a receiver running on the host machine, use `http://host.docker.internal:<port>/...` as the target URL.

### Configuration

All settings are environment variables. See `.env.example` for the full list.

| Variable | Default | Notes |
|---|---|---|
| `API_KEY` | (required) | Shared key for internal callers |
| `DATABASE_URL` | `postgresql+asyncpg://...localhost.../webhooks` | |
| `REDIS_URL` / `CELERY_BROKER_URL` | `redis://localhost:6379/0` and `/1` | Idempotency and rate limiting live in a different DB from the broker |
| `DELIVERY_MAX_ATTEMPTS` | `5` | |
| `DELIVERY_TIMEOUT_SECONDS` | `10` | Overall per-attempt deadline |
| `RETRY_BACKOFF_BASE_SECONDS` | `2` | Keeps local runs fast; around `30` is more realistic in production |
| `RETRY_BACKOFF_MAX_SECONDS` | `600` | |
| `IDEMPOTENCY_TTL_SECONDS` | `86400` | How long a completed key is remembered |
| `STUCK_DELIVERY_THRESHOLD_SECONDS` | `300` | Grace period before a pending delivery can be retried manually |

## Tests

The suite runs against real PostgreSQL and Redis instances, because the behavior under test depends on them: Lua scripts, `JSONB`, array containment, row locks and unique constraints. HTTP calls to receivers are served by `httpx.MockTransport`, and the API is exercised in-process through `httpx.ASGITransport`.

```bash
docker compose up -d postgres redis
docker compose exec postgres createdb -U webhooks webhooks_test

pip install -r requirements-dev.txt
pytest
```

The suite reads `TEST_DATABASE_URL` and `TEST_REDIS_URL`, never `DATABASE_URL`, so it cannot be pointed at development data by accident. It migrates the schema up at the start of the session, truncates tables between tests and migrates back down at the end, which also checks that the migration is reversible. Warnings are treated as errors.

CI (`.github/workflows/ci.yml`) runs three jobs:

- `ruff check`.
- The test suite on Python 3.11, 3.12 and 3.13, with PostgreSQL and Redis service containers.
- A Docker job that builds the image, brings up the full compose stack, waits for `/health`, and smoke-tests ingestion.

## Project layout

```
app/
  main.py            FastAPI app, lifespan, /health
  config.py          Settings (pydantic-settings)
  database.py        Async engine, session factory, naming conventions
  models.py          SQLAlchemy 2.0 models
  schemas.py         Pydantic v2 request/response models
  dependencies.py    DB session, Redis, API key, pagination dependencies
  api/
    subscriptions.py
    events.py        Ingestion, delivery queries, stats, manual retry
  core/
    security.py      HMAC signing/verification, API key check
    rate_limiter.py  Idempotency store, sliding-window rate limiter
  worker/
    celery_app.py    Celery configuration
    tasks.py         Delivery pipeline, backoff, WorkerContext
alembic/             Migrations
tests/
```

## Known limitations

- **Stuck deliveries are recovered by hand.** Deliveries are committed before their tasks are published. If the broker is down at that moment, the deliveries stay `PENDING` until someone retries them. A transactional outbox, or a periodic sweeper that re-enqueues overdue deliveries, would close this gap without operator involvement.
- **Reused idempotency keys are not checked.** A request that reuses a key with a different payload gets the original response back. Storing a request fingerprint next to the key would let the API reject such requests.
- **Secrets cannot be rotated.** Rotation without downtime would require signing with two secrets for an overlap period.
- **Deleting a subscription also deletes its delivery history.** Deactivating the subscription (`is_active: false`) keeps the audit trail.
