import uuid
from datetime import datetime
from typing import Annotated, Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator, model_validator

from app.models import DeliveryStatus

EventType = Annotated[
    str,
    Field(min_length=3, max_length=128, pattern=r"^[a-z0-9_]+(\.[a-z0-9_]+)+$", examples=["monitor.down"]),
]


def _normalize_event_types(value: list[str]) -> list[str]:
    return sorted(set(value))


class SubscriptionCreate(BaseModel):
    target_url: HttpUrl
    event_types: list[EventType] = Field(min_length=1, max_length=50)
    description: str | None = Field(default=None, max_length=255)
    rate_limit_per_minute: int = Field(default=60, ge=1, le=10_000)

    @field_validator("event_types")
    @classmethod
    def dedupe_event_types(cls, value: list[str]) -> list[str]:
        return _normalize_event_types(value)


class SubscriptionUpdate(BaseModel):
    target_url: HttpUrl | None = None
    event_types: list[EventType] | None = Field(default=None, min_length=1, max_length=50)
    description: str | None = Field(default=None, max_length=255)
    rate_limit_per_minute: int | None = Field(default=None, ge=1, le=10_000)
    is_active: bool | None = None

    @field_validator("event_types")
    @classmethod
    def dedupe_event_types(cls, value: list[str] | None) -> list[str] | None:
        return _normalize_event_types(value) if value is not None else None

    @model_validator(mode="after")
    def reject_explicit_nulls(self) -> Self:
        non_nullable = ("target_url", "event_types", "rate_limit_per_minute", "is_active")
        nulls = [name for name in non_nullable if name in self.model_fields_set and getattr(self, name) is None]
        if nulls:
            raise ValueError(f"Fields cannot be null: {', '.join(nulls)}")
        return self


class SubscriptionRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    target_url: str
    event_types: list[str]
    description: str | None
    rate_limit_per_minute: int
    is_active: bool
    created_at: datetime
    updated_at: datetime


class SubscriptionCreated(SubscriptionRead):
    """Only returned on creation; the secret is never exposed again."""

    secret: str


class EventCreate(BaseModel):
    event_type: EventType
    payload: dict[str, Any]


class EventRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    event_type: str
    payload: dict[str, Any]
    idempotency_key: str | None
    created_at: datetime


class EventAccepted(BaseModel):
    event_id: uuid.UUID
    event_type: str
    deliveries_scheduled: int
    created_at: datetime


class DeliveryAttemptRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    attempt_number: int
    status: DeliveryStatus
    status_code: int | None
    latency_ms: int | None
    response_snippet: str | None
    error: str | None
    created_at: datetime


class DeliveryRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    event_id: uuid.UUID
    subscription_id: uuid.UUID
    status: DeliveryStatus
    attempt_count: int
    last_status_code: int | None
    last_error: str | None
    next_retry_at: datetime | None
    delivered_at: datetime | None
    created_at: datetime
    updated_at: datetime


class DeliveryDetail(DeliveryRead):
    attempts: list[DeliveryAttemptRead]


class DeliveryStats(BaseModel):
    subscription_id: uuid.UUID | None
    since: datetime | None
    total: int
    pending: int
    retrying: int
    delivered: int
    failed: int
    success_rate: float | None
    avg_latency_ms: float | None
    p95_latency_ms: float | None


class HealthRead(BaseModel):
    status: Literal["ok", "degraded"]
    database: Literal["ok", "unavailable"]
    redis: Literal["ok", "unavailable"]
