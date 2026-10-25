import uuid
from collections.abc import Awaitable, Callable
from typing import Any

import httpx
import pytest
from redis.asyncio import Redis
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.api import events as events_api
from app.config import Settings
from app.core.rate_limiter import IdempotencyStore, SlidingWindowRateLimiter
from app.core.security import SIGNATURE_HEADER, TIMESTAMP_HEADER, verify_signature
from app.models import Delivery, DeliveryAttempt, DeliveryStatus
from app.worker.tasks import DeliveryOutcome, compute_backoff

CreateSubscription = Callable[..., Awaitable[dict[str, Any]]]
IngestEvent = Callable[..., Awaitable[list[dict[str, Any]]]]
Deliver = Callable[..., Awaitable[DeliveryOutcome]]


class Receiver:
    def __init__(self, *responses: httpx.Response | Exception) -> None:
        self._responses = list(responses)
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        response = self._responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


async def get_delivery(client: httpx.AsyncClient, delivery_id: str) -> dict[str, Any]:
    response = await client.get(f"/deliveries/{delivery_id}")
    assert response.status_code == 200
    return response.json()


async def test_subscription_secret_is_only_returned_on_creation(
    client: httpx.AsyncClient, create_subscription: CreateSubscription
) -> None:
    created = await create_subscription(event_types=["order.created", "monitor.down", "order.created"])
    fetched = (await client.get(f"/subscriptions/{created['id']}")).json()

    assert created["secret"].startswith("whsec_")
    assert created["event_types"] == ["monitor.down", "order.created"]
    assert "secret" not in fetched


async def test_subscription_update_rejects_null_for_required_fields(
    client: httpx.AsyncClient, create_subscription: CreateSubscription
) -> None:
    subscription = await create_subscription(description="billing")

    rejected = await client.patch(f"/subscriptions/{subscription['id']}", json={"is_active": None})
    cleared = await client.patch(f"/subscriptions/{subscription['id']}", json={"description": None})

    assert rejected.status_code == 422
    assert cleared.status_code == 200
    assert cleared.json()["description"] is None


async def test_list_subscriptions_filters_by_event_type(
    client: httpx.AsyncClient, create_subscription: CreateSubscription
) -> None:
    orders = await create_subscription(event_types=["order.created"])
    await create_subscription(event_types=["monitor.down"])

    response = await client.get("/subscriptions", params={"event_type": "order.created"})

    assert [s["id"] for s in response.json()] == [orders["id"]]


async def test_event_fans_out_to_active_matching_subscriptions(
    client: httpx.AsyncClient,
    create_subscription: CreateSubscription,
    ingest_event: IngestEvent,
    published: list[str],
) -> None:
    first = await create_subscription(event_types=["order.created", "order.refunded"])
    second = await create_subscription(event_types=["order.created"])
    paused = await create_subscription(event_types=["order.created"])
    await create_subscription(event_types=["monitor.down"])
    await client.patch(f"/subscriptions/{paused['id']}", json={"is_active": False})

    deliveries = await ingest_event("order.created", {"order_id": 42})

    assert {d["subscription_id"] for d in deliveries} == {first["id"], second["id"]}
    assert all(d["status"] == "PENDING" and d["attempts"] == [] for d in deliveries)
    assert sorted(published) == sorted(d["id"] for d in deliveries)


async def test_payload_with_null_byte_is_rejected(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/events", json={"event_type": "order.created", "payload": {"items": [{"note": "a\u0000b"}]}}
    )

    assert response.status_code == 422


async def test_idempotent_replay_returns_original_event(
    client: httpx.AsyncClient, create_subscription: CreateSubscription, published: list[str]
) -> None:
    await create_subscription()
    body = {"event_type": "order.created", "payload": {"order_id": 1}}
    headers = {"Idempotency-Key": "order-1-created"}

    first = await client.post("/events", json=body, headers=headers)
    replay = await client.post("/events", json=body, headers=headers)

    assert first.status_code == 202
    assert replay.status_code == 200
    assert replay.json() == first.json()
    assert len(published) == 1


async def test_in_flight_idempotency_key_returns_conflict(
    client: httpx.AsyncClient, redis: Redis, settings: Settings
) -> None:
    store = IdempotencyStore(redis, settings.idempotency_ttl_seconds, settings.idempotency_lock_ttl_seconds)
    await store.claim("order-2-created")

    response = await client.post(
        "/events",
        json={"event_type": "order.created", "payload": {}},
        headers={"Idempotency-Key": "order-2-created"},
    )

    assert response.status_code == 409


async def test_failed_ingestion_releases_idempotency_key(
    client: httpx.AsyncClient, redis: Redis, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def broken_create(*args: Any) -> None:
        raise RuntimeError("database unavailable")

    body = {"event_type": "order.created", "payload": {}}
    headers = {"Idempotency-Key": "order-3-created"}

    with monkeypatch.context() as patch:
        patch.setattr(events_api, "_create_event", broken_create)
        with pytest.raises(RuntimeError):
            await client.post("/events", json=body, headers=headers)

    assert await redis.get("idempotency:order-3-created") is None
    assert (await client.post("/events", json=body, headers=headers)).status_code == 202


async def test_database_constraint_covers_evicted_idempotency_key(
    client: httpx.AsyncClient, redis: Redis
) -> None:
    body = {"event_type": "order.created", "payload": {}}
    headers = {"Idempotency-Key": "order-4-created"}

    first = await client.post("/events", json=body, headers=headers)
    await redis.delete("idempotency:order-4-created")
    replay = await client.post("/events", json=body, headers=headers)

    assert replay.status_code == 200
    assert replay.json()["event_id"] == first.json()["event_id"]
    assert await redis.get("idempotency:order-4-created") == first.json()["event_id"]


async def test_sliding_window_rate_limiter(redis: Redis) -> None:
    limiter = SlidingWindowRateLimiter(redis)

    decisions = [await limiter.acquire("sub-a", limit=3, window_seconds=60) for _ in range(4)]
    other_key = await limiter.acquire("sub-b", limit=3, window_seconds=60)

    assert [d.allowed for d in decisions] == [True, True, True, False]
    assert 0 < decisions[-1].retry_after_seconds <= 60
    assert other_key.allowed


def test_backoff_grows_exponentially_with_bounded_jitter() -> None:
    for attempt, ceiling in [(1, 2), (2, 4), (3, 8), (4, 16), (12, 600)]:
        samples = [compute_backoff(attempt, base_seconds=2, max_seconds=600) for _ in range(200)]
        assert all(ceiling / 2 <= delay <= ceiling for delay in samples)


async def test_successful_delivery_is_signed_and_recorded(
    client: httpx.AsyncClient,
    create_subscription: CreateSubscription,
    ingest_event: IngestEvent,
    deliver: Deliver,
) -> None:
    subscription = await create_subscription()
    [delivery] = await ingest_event("order.created", {"order_id": 7})
    receiver = Receiver(httpx.Response(200, text="received"))

    outcome = await deliver(delivery["id"], receiver)

    request = receiver.requests[0]
    verify_signature(
        subscription["secret"],
        request.content,
        request.headers[TIMESTAMP_HEADER],
        request.headers[SIGNATURE_HEADER],
    )
    assert request.headers["X-Webhook-Id"] == delivery["event_id"]
    assert request.headers["X-Webhook-Attempt"] == "1"

    stored = await get_delivery(client, delivery["id"])
    assert outcome.status == DeliveryStatus.DELIVERED
    assert stored["status"] == "DELIVERED"
    assert stored["delivered_at"] is not None
    assert stored["attempts"][0]["status_code"] == 200
    assert stored["attempts"][0]["response_snippet"] == "received"
    assert stored["attempts"][0]["latency_ms"] >= 0


async def test_server_errors_retry_until_attempts_are_exhausted(
    client: httpx.AsyncClient,
    create_subscription: CreateSubscription,
    ingest_event: IngestEvent,
    deliver: Deliver,
    settings: Settings,
) -> None:
    await create_subscription()
    [delivery] = await ingest_event()
    limited = settings.model_copy(update={"delivery_max_attempts": 3})

    outcomes = [await deliver(delivery["id"], Receiver(httpx.Response(503)), limited) for _ in range(3)]

    assert [o.status for o in outcomes] == [
        DeliveryStatus.RETRYING,
        DeliveryStatus.RETRYING,
        DeliveryStatus.FAILED,
    ]
    assert all(o.retry_in_seconds for o in outcomes[:2])
    assert outcomes[2].retry_in_seconds is None

    stored = await get_delivery(client, delivery["id"])
    assert stored["status"] == "FAILED"
    assert stored["attempt_count"] == 3
    assert stored["next_retry_at"] is None
    assert [a["status"] for a in stored["attempts"]] == ["RETRYING", "RETRYING", "FAILED"]


async def test_client_errors_fail_without_retry(
    create_subscription: CreateSubscription, ingest_event: IngestEvent, deliver: Deliver
) -> None:
    await create_subscription()
    [delivery] = await ingest_event()

    outcome = await deliver(delivery["id"], Receiver(httpx.Response(410)))

    assert outcome.status == DeliveryStatus.FAILED
    assert outcome.retry_in_seconds is None


async def test_rate_limited_receiver_retry_after_is_honoured(
    create_subscription: CreateSubscription, ingest_event: IngestEvent, deliver: Deliver
) -> None:
    await create_subscription()
    [delivery] = await ingest_event()

    outcome = await deliver(delivery["id"], Receiver(httpx.Response(429, headers={"Retry-After": "120"})))

    assert outcome.status == DeliveryStatus.RETRYING
    assert outcome.retry_in_seconds == 120


async def test_network_errors_are_retried(
    client: httpx.AsyncClient,
    create_subscription: CreateSubscription,
    ingest_event: IngestEvent,
    deliver: Deliver,
) -> None:
    await create_subscription()
    [delivery] = await ingest_event()

    outcome = await deliver(delivery["id"], Receiver(httpx.ConnectError("connection refused")))

    attempt = (await get_delivery(client, delivery["id"]))["attempts"][0]
    assert outcome.status == DeliveryStatus.RETRYING
    assert attempt["status_code"] is None
    assert attempt["error"] == "ConnectError: connection refused"


async def test_rate_limited_delivery_is_deferred_without_consuming_an_attempt(
    session: AsyncSession,
    create_subscription: CreateSubscription,
    ingest_event: IngestEvent,
    deliver: Deliver,
) -> None:
    await create_subscription(rate_limit_per_minute=1)
    [first] = await ingest_event()
    [second] = await ingest_event()
    receiver = Receiver(httpx.Response(200), httpx.Response(200))

    await deliver(first["id"], receiver)
    deferred = await deliver(second["id"], receiver)

    attempts = await session.scalar(
        select(func.count()).select_from(DeliveryAttempt).where(DeliveryAttempt.delivery_id == second["id"])
    )
    assert deferred.rate_limited
    assert 0 < deferred.retry_in_seconds <= 61
    assert len(receiver.requests) == 1
    assert attempts == 0


async def test_manual_retry_requeues_failed_delivery_once(
    client: httpx.AsyncClient,
    create_subscription: CreateSubscription,
    ingest_event: IngestEvent,
    deliver: Deliver,
    published: list[str],
) -> None:
    await create_subscription()
    [delivery] = await ingest_event()
    await deliver(delivery["id"], Receiver(httpx.Response(400)))
    published.clear()

    retried = await client.post(f"/deliveries/{delivery['id']}/retry")
    duplicate = await client.post(f"/deliveries/{delivery['id']}/retry")

    assert retried.status_code == 202
    assert retried.json()["status"] == "RETRYING"
    assert duplicate.status_code == 409
    assert published == [delivery["id"]]


async def test_manual_retry_rejects_delivered_and_recent_pending(
    client: httpx.AsyncClient,
    session: AsyncSession,
    create_subscription: CreateSubscription,
    ingest_event: IngestEvent,
    deliver: Deliver,
) -> None:
    await create_subscription()
    [delivered] = await ingest_event()
    [pending] = await ingest_event()
    await deliver(delivered["id"], Receiver(httpx.Response(204)))

    assert (await client.post(f"/deliveries/{delivered['id']}/retry")).status_code == 409
    assert (await client.post(f"/deliveries/{pending['id']}/retry")).status_code == 409

    await session.execute(
        text("UPDATE deliveries SET updated_at = now() - interval '1 hour' WHERE id = :id"),
        {"id": pending["id"]},
    )
    await session.commit()

    assert (await client.post(f"/deliveries/{pending['id']}/retry")).status_code == 202


async def test_delivery_stats(
    client: httpx.AsyncClient,
    session: AsyncSession,
    create_subscription: CreateSubscription,
    ingest_event: IngestEvent,
    deliver: Deliver,
) -> None:
    subscription = await create_subscription()
    deliveries = [(await ingest_event())[0] for _ in range(3)]
    await deliver(deliveries[0]["id"], Receiver(httpx.Response(200)))
    await deliver(deliveries[1]["id"], Receiver(httpx.Response(404)))

    await session.execute(
        text("UPDATE delivery_attempts SET latency_ms = CASE status_code WHEN 200 THEN 100 ELSE 300 END")
    )
    await session.commit()

    stats = (await client.get("/deliveries/stats", params={"subscription_id": subscription["id"]})).json()

    assert stats["total"] == 3
    assert (stats["delivered"], stats["failed"], stats["pending"]) == (1, 1, 1)
    assert stats["success_rate"] == 0.5
    assert stats["avg_latency_ms"] == 200
    assert stats["p95_latency_ms"] == 290


async def test_deleting_subscription_removes_its_deliveries(
    client: httpx.AsyncClient,
    session: AsyncSession,
    create_subscription: CreateSubscription,
    ingest_event: IngestEvent,
) -> None:
    subscription = await create_subscription()
    await ingest_event()

    response = await client.delete(f"/subscriptions/{subscription['id']}")

    subscription_id = uuid.UUID(subscription["id"])
    remaining = await session.scalar(
        select(func.count()).select_from(Delivery).where(Delivery.subscription_id == subscription_id)
    )
    assert response.status_code == 204
    assert remaining == 0


async def test_health_reports_dependencies(client: httpx.AsyncClient) -> None:
    client.headers.pop("X-API-Key")

    response = await client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "database": "ok", "redis": "ok"}
