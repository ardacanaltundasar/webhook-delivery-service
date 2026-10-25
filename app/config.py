from functools import lru_cache
from typing import Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "webhook-delivery-service"
    environment: Literal["development", "test", "production"] = "development"
    log_level: str = "INFO"

    api_key: SecretStr

    database_url: str = "postgresql+asyncpg://webhooks:webhooks@localhost:5432/webhooks"
    db_pool_size: int = Field(default=10, ge=1)
    db_max_overflow: int = Field(default=20, ge=0)
    db_echo: bool = False

    redis_url: str = "redis://localhost:6379/0"
    celery_broker_url: str = "redis://localhost:6379/1"

    idempotency_ttl_seconds: int = Field(default=86_400, ge=1)
    rate_limit_window_seconds: int = Field(default=60, ge=1)

    delivery_max_attempts: int = Field(default=5, ge=1)
    delivery_timeout_seconds: float = Field(default=10.0, gt=0)
    retry_backoff_base_seconds: float = Field(default=2.0, gt=0)
    retry_backoff_max_seconds: float = Field(default=600.0, gt=0)
    response_snippet_max_length: int = Field(default=1024, ge=0)
    signature_tolerance_seconds: int = Field(default=300, ge=1)


@lru_cache
def get_settings() -> Settings:
    return Settings()
