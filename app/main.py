import asyncio
import logging
from collections.abc import AsyncIterator, Awaitable
from contextlib import asynccontextmanager
from typing import Any

from fastapi import Depends, FastAPI, Response, status
from sqlalchemy import text

from app.api import events, subscriptions
from app.config import get_settings
from app.core.rate_limiter import create_redis
from app.database import engine
from app.dependencies import RedisDep, SessionDep, require_api_key
from app.schemas import HealthRead

settings = get_settings()

logging.basicConfig(level=settings.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

HEALTH_CHECK_TIMEOUT_SECONDS = 2.0


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    app.state.redis = create_redis(settings.redis_url)
    try:
        yield
    finally:
        await app.state.redis.aclose()
        await engine.dispose()


app = FastAPI(title="Webhook Delivery Service", version="1.0.0", lifespan=lifespan)

authenticated = [Depends(require_api_key)]
app.include_router(subscriptions.router, dependencies=authenticated)
app.include_router(events.router, dependencies=authenticated)
app.include_router(events.deliveries_router, dependencies=authenticated)


async def _probe(name: str, check: Awaitable[Any]) -> bool:
    try:
        async with asyncio.timeout(HEALTH_CHECK_TIMEOUT_SECONDS):
            await check
    except Exception:
        logger.warning("Health check failed for %s", name, exc_info=True)
        return False
    return True


@app.get("/health", response_model=HealthRead, tags=["health"])
async def health(response: Response, session: SessionDep, redis: RedisDep) -> HealthRead:
    database_ok = await _probe("database", session.execute(text("SELECT 1")))
    redis_ok = await _probe("redis", redis.ping())

    healthy = database_ok and redis_ok
    if not healthy:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return HealthRead(
        status="ok" if healthy else "degraded",
        database="ok" if database_ok else "unavailable",
        redis="ok" if redis_ok else "unavailable",
    )
