"""Application configuration."""

from __future__ import annotations

from enum import Enum
from functools import lru_cache

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class LogLevel(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


class Settings(BaseSettings):
    database_url: SecretStr = Field(
        default=SecretStr("postgresql+asyncpg://taskq:taskq@localhost:5433/taskq"),
    )
    log_level: LogLevel = Field(default=LogLevel.INFO)
    log_format_json: bool = Field(default=False)

    # Worker behavior
    worker_poll_interval_seconds: float = Field(default=2.0, gt=0.0)
    worker_max_retries: int = Field(default=3, ge=0, le=10)

    # Idempotency
    idempotency_ttl_seconds: int = Field(default=86400, ge=60)

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()