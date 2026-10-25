import os

# Dedicated variables so a developer's DATABASE_URL never points the suite at real data;
# the fixtures below truncate tables and flush Redis between tests.
os.environ["DATABASE_URL"] = os.environ.get(
    "TEST_DATABASE_URL", "postgresql+asyncpg://webhooks:webhooks@localhost:5432/webhooks_test"
)
os.environ["REDIS_URL"] = os.environ.get("TEST_REDIS_URL", "redis://localhost:6379/15")
os.environ["API_KEY"] = "test-api-key"
os.environ["ENVIRONMENT"] = "test"

import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
from alembic import command
from alembic.config import Config
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings, get_settings
from app.core.rate_limiter import SlidingWindowRateLimiter, create_redis
from app.database import SessionLocal, engine
from app.main import app
from app.worker.tasks import DeliveryOutcome, deliver_webhook, process_delivery

API_KEY = "test-api-key"
ALEMBIC_INI = Path(__file__).resolve().parents[1] / "alembic.ini"


@pytest.fixture(scope="session")
def database() -> Iterator[None]:
    config = Config(str(ALEMBIC_INI))
    command.upgrade(config, "head")
    yield
    command.downgrade(config, "base")


@pytest.fixture(scope="session")
async def db_engine(database: None) -> AsyncIterator[None]:
    yield
    await engine.dispose()


@pytest.fixture
async def clean_db(db_engine: None) -> AsyncIterator[None]:
    yield
    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE subscriptions, events, deliveries, delivery_attempts CASCADE"))


@pytest.fixture
def settings() -> Settings:
    return get_settings()


@pytest.fixture
async def redis(settings: Settings) -> AsyncIterator[Redis]:
    client = create_redis(settings.redis_url)
    await client.flushdb()
    yield client
    await client.aclose()


@pytest.fixture
async def session(clean_db: None) -> AsyncIterator[AsyncSession]:
    async with SessionLocal() as db_session:
        yield db_session


@pytest.fixture
def published(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    queued: list[str] = []
    monkeypatch.setattr(deliver_webhook, "delay", queued.append)
    return queued


@pytest.fixture
async def client(clean_db: None, redis: Redis, published: list[str]) -> AsyncIterator[httpx.AsyncClient]:
    app.state.redis = redis
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://testserver", headers={"X-API-Key": API_KEY}
    ) as http_client:
        yield http_client


@pytest.fixture
def create_subscription(client: httpx.AsyncClient) -> Callable[..., Awaitable[dict[str, Any]]]:
    async def _create(**overrides: Any) -> dict[str, Any]:
        body = {"target_url": "https://receiver.test/webhooks", "event_types": ["order.created"]}
        response = await client.post("/subscriptions", json=body | overrides)
        assert response.status_code == 201, response.text
        return response.json()

    return _create


@pytest.fixture
def ingest_event(client: httpx.AsyncClient) -> Callable[..., Awaitable[list[dict[str, Any]]]]:
    """Ingests an event and returns the deliveries it fanned out to."""

    async def _ingest(event_type: str = "order.created", payload: dict[str, Any] | None = None):
        response = await client.post("/events", json={"event_type": event_type, "payload": payload or {}})
        assert response.status_code == 202, response.text
        deliveries = await client.get("/deliveries", params={"event_id": response.json()["event_id"]})
        return deliveries.json()

    return _ingest


@pytest.fixture
def deliver(redis: Redis, settings: Settings) -> Callable[..., Awaitable[DeliveryOutcome]]:
    rate_limiter = SlidingWindowRateLimiter(redis)

    async def _deliver(
        delivery_id: str | uuid.UUID,
        receiver: Callable[[httpx.Request], httpx.Response],
        settings_override: Settings | None = None,
    ) -> DeliveryOutcome:
        async with httpx.AsyncClient(transport=httpx.MockTransport(receiver)) as http:
            return await process_delivery(
                uuid.UUID(str(delivery_id)),
                sessionmaker=SessionLocal,
                http=http,
                rate_limiter=rate_limiter,
                settings=settings_override or settings,
            )

    return _deliver
