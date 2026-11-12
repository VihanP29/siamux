from datetime import UTC, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class MockOptions(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    delay_seconds: float = Field(default=0.1, ge=0, le=120)
    fail_first_attempts: int = Field(default=0, ge=0, le=10)
    permanent_failure: bool = False


class CreateRun(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    provider: str
    model: str = "mock-basic"
    input: str = Field(min_length=1, max_length=10000)
    mock: MockOptions = Field(default_factory=MockOptions)

    @field_validator("input", mode="before")
    @classmethod
    def trim_input(cls, value):
        return value.strip() if isinstance(value, str) else value


class RunView(BaseModel):
    id: str
    status: Literal["PENDING", "RUNNING", "SUCCEEDED", "FAILED", "CANCELED"]
    provider: str
    model: str
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    attempts: int
    output: dict | None
    error: dict | None
    queue_latency_ms: float | None
    provider_duration_ms: float | None
    status_url: str


def view(run) -> RunView:
    def utc(value):
        return value.replace(tzinfo=UTC) if value else None

    return RunView(
        id=run.id,
        status=run.status,
        provider=run.request["provider"],
        model=run.request["model"],
        created_at=utc(run.created_at),
        started_at=utc(run.started_at),
        completed_at=utc(run.completed_at),
        attempts=run.attempts,
        output=run.output,
        error=run.error,
        queue_latency_ms=run.queue_latency_ms,
        provider_duration_ms=run.provider_duration_ms,
        status_url=f"/v1/runs/{run.id}",
    )
