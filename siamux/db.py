from datetime import UTC, datetime
from functools import lru_cache
from uuid import uuid4

from sqlalchemy import JSON, CheckConstraint, DateTime, Float, Index, Integer, String, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

from siamux.config import get_settings


def utcnow() -> datetime:
    """Database datetimes are UTC, stored without a timezone for SQLite portability."""
    return datetime.now(UTC).replace(tzinfo=None)


class Base(DeclarativeBase):
    pass


class Run(Base):
    __tablename__ = "runs"
    __table_args__ = (
        CheckConstraint("status IN ('PENDING','RUNNING','SUCCEEDED','FAILED')"),
        CheckConstraint("attempts >= 0"),
        Index("ix_runs_dispatch", "status", "dispatch_after"),
    )
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    idempotency_key: Mapped[str] = mapped_column(String(100), unique=True)
    request_hash: Mapped[str] = mapped_column(String(64))
    request: Mapped[dict] = mapped_column(JSON)
    status: Mapped[str] = mapped_column(String(16), default="PENDING")
    output: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    error: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    started_at: Mapped[datetime | None] = mapped_column(DateTime)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime)
    due_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    dispatch_after: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    lease_until: Mapped[datetime | None] = mapped_column(DateTime)
    claim_token: Mapped[str | None] = mapped_column(String(36))
    trace_context: Mapped[dict] = mapped_column(JSON, default=dict)
    queue_latency_ms: Mapped[float | None] = mapped_column(Float)
    provider_duration_ms: Mapped[float | None] = mapped_column(Float)


@lru_cache
def engine():
    url = get_settings().database_url
    kwargs = {"connect_args": {"check_same_thread": False}} if url.startswith("sqlite") else {}
    return create_engine(url, pool_pre_ping=True, **kwargs)


def session_factory():
    return sessionmaker(engine(), expire_on_commit=False)
