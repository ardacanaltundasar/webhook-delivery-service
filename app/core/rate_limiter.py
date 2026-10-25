import uuid
from dataclasses import dataclass

from redis.asyncio import Redis

_PENDING = "__pending__"

# Sliding window log on a sorted set: each permitted request is a member scored by its
# timestamp in ms. Unlike a fixed window, this cannot let 2x the limit through around a
# window boundary. Running it as one script keeps trim/count/insert atomic across workers,
# and using Redis TIME avoids clock skew between worker hosts.
_SLIDING_WINDOW_SCRIPT = """
local key = KEYS[1]
local window_ms = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local member = ARGV[3]

local t = redis.call('TIME')
local now_ms = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

redis.call('ZREMRANGEBYSCORE', key, '-inf', now_ms - window_ms)

if redis.call('ZCARD', key) < limit then
    redis.call('ZADD', key, now_ms, member)
    redis.call('PEXPIRE', key, window_ms)
    return {1, 0}
end

local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
return {0, tonumber(oldest[2]) + window_ms - now_ms}
"""


def create_redis(url: str) -> Redis:
    return Redis.from_url(url, decode_responses=True)


@dataclass(frozen=True, slots=True)
class IdempotencyClaim:
    acquired: bool
    event_id: uuid.UUID | None = None

    @property
    def in_progress(self) -> bool:
        return not self.acquired and self.event_id is None


class IdempotencyStore:
    def __init__(
        self,
        redis: Redis,
        ttl_seconds: int,
        lock_ttl_seconds: int,
        prefix: str = "idempotency",
    ) -> None:
        self._redis = redis
        self._ttl = ttl_seconds
        self._lock_ttl = lock_ttl_seconds
        self._prefix = prefix

    def _key(self, idempotency_key: str) -> str:
        return f"{self._prefix}:{idempotency_key}"

    async def claim(self, idempotency_key: str) -> IdempotencyClaim:
        # The pending marker gets a short TTL so a request that crashes mid-flight does not
        # block the key for the full retention period; complete() extends it once committed.
        key = self._key(idempotency_key)
        if await self._redis.set(key, _PENDING, nx=True, ex=self._lock_ttl):
            return IdempotencyClaim(acquired=True)

        existing = await self._redis.get(key)
        if existing is None or existing == _PENDING:
            return IdempotencyClaim(acquired=False)
        return IdempotencyClaim(acquired=False, event_id=uuid.UUID(existing))

    async def complete(self, idempotency_key: str, event_id: uuid.UUID) -> None:
        await self._redis.set(self._key(idempotency_key), str(event_id), ex=self._ttl)

    async def release(self, idempotency_key: str) -> None:
        await self._redis.delete(self._key(idempotency_key))


@dataclass(frozen=True, slots=True)
class RateLimitDecision:
    allowed: bool
    retry_after_seconds: float


class SlidingWindowRateLimiter:
    def __init__(self, redis: Redis, prefix: str = "ratelimit") -> None:
        self._prefix = prefix
        self._script = redis.register_script(_SLIDING_WINDOW_SCRIPT)

    async def acquire(self, key: str, limit: int, window_seconds: int = 60) -> RateLimitDecision:
        allowed, retry_after_ms = await self._script(
            keys=[f"{self._prefix}:{key}"],
            args=[window_seconds * 1000, limit, uuid.uuid4().hex],
        )
        return RateLimitDecision(
            allowed=bool(allowed),
            retry_after_seconds=max(int(retry_after_ms), 0) / 1000,
        )
