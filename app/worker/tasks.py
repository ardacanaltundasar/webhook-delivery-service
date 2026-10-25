import asyncio
import json
import os
import random
import time
import uuid
from collections.abc import Coroutine
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, TypeVar

import httpx
from celery import Task
from celery.signals import worker_process_shutdown
from celery.utils.log import get_task_logger
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import joinedload

from app.config import Settings, get_settings
from app.core.rate_limiter import SlidingWindowRateLimiter, create_redis
from app.core.security import build_signature_headers
from app.models import Delivery, DeliveryAttempt, DeliveryStatus
from app.worker.celery_app import celery_app

logger = get_task_logger(__name__)

USER_AGENT = "webhook-delivery-service/1.0"

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class DeliveryJob:
    delivery_id: uuid.UUID
    subscription_id: uuid.UUID
    event_id: uuid.UUID
    event_type: str
    target_url: str
    secret: str
    rate_limit_per_minute: int
    attempt_number: int
    body: bytes


@dataclass(frozen=True, slots=True)
class HttpResult:
    latency_ms: int
    status_code: int | None = None
    response_snippet: str | None = None
    error: str | None = None
    retry_after_seconds: float | None = None

    @property
    def succeeded(self) -> bool:
        return self.status_code is not None and 200 <= self.status_code < 300

    @property
    def retryable(self) -> bool:
        return self.status_code is None or self.status_code == 429 or self.status_code >= 500


@dataclass(frozen=True, slots=True)
class DeliveryOutcome:
    status: DeliveryStatus | None = None
    retry_in_seconds: float | None = None
    rate_limited: bool = False


@dataclass(frozen=True, slots=True)
class AttemptVerdict:
    status: DeliveryStatus
    retry_in_seconds: float | None = None


def compute_backoff(attempt: int, base_seconds: float, max_seconds: float) -> float:
    # "Equal jitter": keep half of the exponential delay as a floor so retries never collapse
    # to near-zero, and randomize the other half so failing receivers are not hit in lockstep
    # by every delivery that failed at the same moment.
    ceiling = min(max_seconds, base_seconds * 2 ** (attempt - 1))
    return ceiling / 2 + random.uniform(0, ceiling / 2)


def parse_retry_after(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        seconds = float(value)
    except ValueError:
        return None
    return seconds if seconds >= 0 else None


def build_body(
    event_id: uuid.UUID, event_type: str, created_at: datetime, payload: dict[str, Any]
) -> bytes:
    envelope = {
        "id": str(event_id),
        "type": event_type,
        "created_at": created_at.isoformat(),
        "data": payload,
    }
    return json.dumps(envelope, separators=(",", ":"), ensure_ascii=False).encode()


async def load_job(
    sessionmaker: async_sessionmaker[AsyncSession], delivery_id: uuid.UUID
) -> DeliveryJob | None:
    async with sessionmaker() as session:
        stmt = (
            select(Delivery)
            .options(joinedload(Delivery.subscription), joinedload(Delivery.event))
            .where(Delivery.id == delivery_id)
        )
        delivery = (await session.execute(stmt)).scalar_one_or_none()

        if delivery is None:
            logger.warning("Delivery %s not found, dropping task", delivery_id)
            return None
        if delivery.status in (DeliveryStatus.DELIVERED, DeliveryStatus.FAILED):
            logger.info("Delivery %s already %s, skipping", delivery_id, delivery.status)
            return None

        subscription = delivery.subscription
        if not subscription.is_active:
            delivery.status = DeliveryStatus.FAILED
            delivery.last_error = "Subscription is inactive"
            delivery.next_retry_at = None
            await session.commit()
            logger.info("Delivery %s failed: subscription %s is inactive", delivery_id, subscription.id)
            return None

        event = delivery.event
        return DeliveryJob(
            delivery_id=delivery.id,
            subscription_id=subscription.id,
            event_id=event.id,
            event_type=event.event_type,
            target_url=subscription.target_url,
            secret=subscription.secret,
            rate_limit_per_minute=subscription.rate_limit_per_minute,
            attempt_number=delivery.attempt_count + 1,
            body=build_body(event.id, event.event_type, event.created_at, event.payload),
        )


async def _read_snippet(response: httpx.Response, limit: int) -> str | None:
    if limit == 0:
        return None
    # Stream instead of response.text so an oversized or endless body cannot exhaust worker memory.
    buffer = bytearray()
    async for chunk in response.aiter_bytes():
        buffer.extend(chunk)
        if len(buffer) >= limit:
            break
    return bytes(buffer[:limit]).decode(response.encoding or "utf-8", errors="replace") or None


async def send_request(http: httpx.AsyncClient, job: DeliveryJob, settings: Settings) -> HttpResult:
    headers = {
        "Content-Type": "application/json",
        "User-Agent": USER_AGENT,
        "X-Webhook-Id": str(job.event_id),
        "X-Webhook-Event": job.event_type,
        "X-Webhook-Delivery": str(job.delivery_id),
        "X-Webhook-Attempt": str(job.attempt_number),
        **build_signature_headers(job.secret, job.body),
    }

    started = time.perf_counter()
    try:
        # httpx timeouts apply per phase (connect/read/write); this caps the whole exchange
        # so a receiver trickling bytes cannot hold the worker indefinitely.
        async with asyncio.timeout(settings.delivery_timeout_seconds):
            async with http.stream(
                "POST", job.target_url, content=job.body, headers=headers
            ) as response:
                snippet = await _read_snippet(response, settings.response_snippet_max_length)
    except (TimeoutError, httpx.TimeoutException):
        return HttpResult(
            latency_ms=_elapsed_ms(started),
            error=f"Timed out after {settings.delivery_timeout_seconds:g}s",
        )
    except httpx.HTTPError as exc:
        return HttpResult(latency_ms=_elapsed_ms(started), error=f"{type(exc).__name__}: {exc}")

    return HttpResult(
        latency_ms=_elapsed_ms(started),
        status_code=response.status_code,
        response_snippet=snippet,
        retry_after_seconds=parse_retry_after(response.headers.get("Retry-After")),
    )


def _elapsed_ms(started: float) -> int:
    return round((time.perf_counter() - started) * 1000)


def judge_attempt(job: DeliveryJob, result: HttpResult, settings: Settings) -> AttemptVerdict:
    if result.succeeded:
        return AttemptVerdict(status=DeliveryStatus.DELIVERED)
    if not result.retryable or job.attempt_number >= settings.delivery_max_attempts:
        return AttemptVerdict(status=DeliveryStatus.FAILED)

    delay = compute_backoff(
        job.attempt_number,
        settings.retry_backoff_base_seconds,
        settings.retry_backoff_max_seconds,
    )
    if result.retry_after_seconds is not None:
        delay = min(max(delay, result.retry_after_seconds), settings.retry_backoff_max_seconds)
    return AttemptVerdict(status=DeliveryStatus.RETRYING, retry_in_seconds=delay)


async def record_attempt(
    sessionmaker: async_sessionmaker[AsyncSession],
    job: DeliveryJob,
    result: HttpResult,
    verdict: AttemptVerdict,
) -> bool:
    now = datetime.now(UTC)

    async with sessionmaker() as session:
        delivery = await session.get(Delivery, job.delivery_id)
        if delivery is None:
            return False

        session.add(
            DeliveryAttempt(
                delivery_id=job.delivery_id,
                attempt_number=job.attempt_number,
                status=verdict.status,
                status_code=result.status_code,
                latency_ms=result.latency_ms,
                response_snippet=result.response_snippet,
                error=result.error,
            )
        )
        delivery.status = verdict.status
        delivery.attempt_count = job.attempt_number
        delivery.last_status_code = result.status_code
        delivery.last_error = None if result.succeeded else (result.error or f"HTTP {result.status_code}")
        delivery.delivered_at = now if verdict.status == DeliveryStatus.DELIVERED else None
        delivery.next_retry_at = (
            now + timedelta(seconds=verdict.retry_in_seconds)
            if verdict.retry_in_seconds is not None
            else None
        )

        try:
            await session.commit()
        except IntegrityError:
            # Another worker already recorded this attempt number (duplicate task after a
            # redelivery or a manual retry racing a scheduled one); let that one own the state.
            await session.rollback()
            logger.warning(
                "Attempt %d for delivery %s was already recorded, discarding duplicate",
                job.attempt_number,
                job.delivery_id,
            )
            return False
    return True


async def process_delivery(
    delivery_id: uuid.UUID,
    *,
    sessionmaker: async_sessionmaker[AsyncSession],
    http: httpx.AsyncClient,
    rate_limiter: SlidingWindowRateLimiter,
    settings: Settings,
) -> DeliveryOutcome:
    job = await load_job(sessionmaker, delivery_id)
    if job is None:
        return DeliveryOutcome()

    decision = await rate_limiter.acquire(str(job.subscription_id), limit=job.rate_limit_per_minute)
    if not decision.allowed:
        delay = decision.retry_after_seconds + random.uniform(0, 1)
        logger.info("Delivery %s rate limited, deferring %.2fs", delivery_id, delay)
        return DeliveryOutcome(retry_in_seconds=delay, rate_limited=True)

    result = await send_request(http, job, settings)
    verdict = judge_attempt(job, result, settings)

    if not await record_attempt(sessionmaker, job, result, verdict):
        return DeliveryOutcome()

    logger.info(
        "Delivery %s attempt %d -> %s (status=%s, latency=%dms%s)",
        delivery_id,
        job.attempt_number,
        verdict.status,
        result.status_code,
        result.latency_ms,
        f", retry in {verdict.retry_in_seconds:.1f}s" if verdict.retry_in_seconds is not None else "",
    )
    return DeliveryOutcome(status=verdict.status, retry_in_seconds=verdict.retry_in_seconds)


class WorkerContext:
    """Async resources owned by a single worker process.

    Celery runs tasks synchronously, so each prefork child keeps one event loop for its whole
    lifetime and drives coroutines on it. Pooled asyncpg, Redis and httpx connections are bound
    to the loop that opened them; calling asyncio.run() per task would close that loop and leave
    the pools holding dead connections.
    """

    def __init__(self, settings: Settings) -> None:
        self.pid = os.getpid()
        self.loop = asyncio.new_event_loop()
        self.engine: AsyncEngine = create_async_engine(
            settings.database_url, pool_size=2, max_overflow=0, pool_pre_ping=True
        )
        self.sessionmaker = async_sessionmaker(self.engine, expire_on_commit=False)
        self.redis = create_redis(settings.redis_url)
        self.rate_limiter = SlidingWindowRateLimiter(self.redis)
        self.http = httpx.AsyncClient(
            timeout=settings.delivery_timeout_seconds,
            follow_redirects=False,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
        )

    def run(self, coro: Coroutine[Any, Any, T]) -> T:
        return self.loop.run_until_complete(coro)

    async def _aclose(self) -> None:
        await self.http.aclose()
        await self.redis.aclose()
        await self.engine.dispose()

    def close(self) -> None:
        try:
            self.run(self._aclose())
        finally:
            self.loop.close()


_context: WorkerContext | None = None


def get_worker_context() -> WorkerContext:
    global _context
    # A context inherited across fork() would share sockets with the parent; rebuild per process.
    if _context is None or _context.pid != os.getpid():
        _context = WorkerContext(get_settings())
    return _context


@worker_process_shutdown.connect
def _close_worker_context(**_: Any) -> None:
    global _context
    if _context is not None and _context.pid == os.getpid():
        _context.close()
        _context = None


@celery_app.task(bind=True, name="webhooks.deliver", max_retries=None)
def deliver_webhook(self: Task, delivery_id: str) -> None:
    ctx = get_worker_context()
    outcome = ctx.run(
        process_delivery(
            uuid.UUID(delivery_id),
            sessionmaker=ctx.sessionmaker,
            http=ctx.http,
            rate_limiter=ctx.rate_limiter,
            settings=get_settings(),
        )
    )
    # max_retries is unbounded on purpose: the attempt budget is enforced from the database,
    # and rate-limit deferrals must not consume it.
    if outcome.retry_in_seconds is not None:
        raise self.retry(countdown=outcome.retry_in_seconds)
