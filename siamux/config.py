from functools import lru_cache

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SIAMUX_", env_file=".env", extra="ignore")
    database_url: str = "postgresql+psycopg://siamux:siamux@localhost:5432/siamux"
    broker_url: str = "redis://localhost:6379/0"
    provider_timeout: float = Field(default=5, gt=0, le=60)
    lease_seconds: float = Field(default=30, gt=0)
    max_attempts: int = Field(default=3, ge=1, le=10)
    retry_base_seconds: float = Field(default=1, gt=0)
    dispatch_interval: float = Field(default=1, gt=0)
    redispatch_seconds: float = Field(default=10, gt=0)
    otlp_endpoint: str | None = None

    @model_validator(mode="after")
    def valid_lease(self):
        if self.lease_seconds <= self.provider_timeout + 5:
            raise ValueError("lease_seconds must exceed provider_timeout by more than 5 seconds")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
