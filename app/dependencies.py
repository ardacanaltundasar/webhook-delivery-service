from dataclasses import dataclass
from typing import Annotated

from fastapi import Depends, Header, HTTPException, Query, Request, Security, status
from fastapi.security import APIKeyHeader
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings, get_settings
from app.core.rate_limiter import IdempotencyStore
from app.core.security import is_valid_api_key
from app.database import get_session

api_key_scheme = APIKeyHeader(name="X-API-Key", auto_error=False)

SettingsDep = Annotated[Settings, Depends(get_settings)]
SessionDep = Annotated[AsyncSession, Depends(get_session)]


def get_redis(request: Request) -> Redis:
    return request.app.state.redis


RedisDep = Annotated[Redis, Depends(get_redis)]


def get_idempotency_store(redis: RedisDep, settings: SettingsDep) -> IdempotencyStore:
    return IdempotencyStore(
        redis,
        ttl_seconds=settings.idempotency_ttl_seconds,
        lock_ttl_seconds=settings.idempotency_lock_ttl_seconds,
    )


IdempotencyStoreDep = Annotated[IdempotencyStore, Depends(get_idempotency_store)]


@dataclass(frozen=True, slots=True)
class Pagination:
    limit: int
    offset: int


def get_pagination(
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> Pagination:
    return Pagination(limit=limit, offset=offset)


PaginationDep = Annotated[Pagination, Depends(get_pagination)]

IdempotencyKeyHeader = Annotated[
    str | None,
    Header(alias="Idempotency-Key", min_length=1, max_length=255),
]


async def require_api_key(
    api_key: Annotated[str | None, Security(api_key_scheme)],
    settings: SettingsDep,
) -> None:
    if not is_valid_api_key(api_key, settings.api_key.get_secret_value()):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
            headers={"WWW-Authenticate": "ApiKey"},
        )
