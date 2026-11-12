from datetime import UTC, datetime
from functools import lru_cache
from uuid import uuid4

from sqlalchemy import (
    JSON,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    create_engine,
    event,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

from siamux.config import get_settings


def utcnow() -> datetime:
    """Database datetimes are UTC, stored without a timezone for SQLite portability."""
    return datetime.now(UTC).replace(tzinfo=None)


class Base(DeclarativeBase):
    pass


LEGACY_TENANT_ID = "00000000-0000-0000-0000-000000000001"


class Tenant(Base):
    __tablename__ = "tenants"
    __table_args__ = (CheckConstraint("max_outstanding > 0", name="ck_tenants_capacity"),)
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    name: Mapped[str] = mapped_column(String(100), unique=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    max_outstanding: Mapped[int] = mapped_column(Integer, default=100, server_default="100")


class APIKey(Base):
    __tablename__ = "api_keys"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(ForeignKey("tenants.id"), index=True)
    digest: Mapped[str] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime)


class AdmissionPolicy(Base):
    __tablename__ = "admission_policy"
    __table_args__ = (
        CheckConstraint("id = 1", name="ck_admission_singleton"),
        CheckConstraint("max_outstanding > 0", name="ck_admission_capacity"),
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    max_outstanding: Mapped[int] = mapped_column(Integer, default=1000, server_default="1000")


class Run(Base):
    __tablename__ = "runs"
    __table_args__ = (
        CheckConstraint(
            "status IN ('PENDING','RUNNING','SUCCEEDED','FAILED','CANCELED')",
            name="ck_runs_status",
        ),
        CheckConstraint("attempts >= 0"),
        Index("ix_runs_dispatch", "status", "dispatch_after"),
        UniqueConstraint("tenant_id", "idempotency_key", name="uq_runs_tenant_key"),
        Index("ix_runs_tenant_id", "tenant_id"),
        Index("ix_runs_tenant_status", "tenant_id", "status"),
    )
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    tenant_id: Mapped[str] = mapped_column(ForeignKey("tenants.id"))
    idempotency_key: Mapped[str] = mapped_column(String(100))
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
    db_engine = create_engine(url, pool_pre_ping=True, hide_parameters=True, **kwargs)
    if db_engine.dialect.name == "sqlite":

        @event.listens_for(db_engine, "connect")
        def sqlite_setup(connection, record):
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute("PRAGMA busy_timeout=5000")

    return db_engine


def session_factory():
    return sessionmaker(engine(), expire_on_commit=False)
