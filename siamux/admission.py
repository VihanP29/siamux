"""Serialize new admissions using durable policy, then tenant locks.

Terminal transitions only decrease the authoritative count and need no policy lock.
SQLite uses BEGIN IMMEDIATE before any read; PostgreSQL row locks are authoritative.
"""

from contextlib import contextmanager

from fastapi import HTTPException
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError

from siamux.db import AdmissionPolicy, Run, Tenant, session_factory

OUTSTANDING = ("PENDING", "RUNNING")


@contextmanager
def locked_policy(session):
    if session.bind.dialect.name == "sqlite":
        session.connection().exec_driver_sql("BEGIN IMMEDIATE")
    else:
        session.execute(text("SET LOCAL lock_timeout = '5s'"))
    with session.begin_nested():
        policy = session.scalar(
            select(AdmissionPolicy).where(AdmissionPolicy.id == 1).with_for_update()
        )
        if policy is None:
            raise RuntimeError("Admission policy missing; run migrations first")
        yield policy


def lookup(session, tenant_id, key):
    return session.scalar(select(Run).where(Run.tenant_id == tenant_id, Run.idempotency_key == key))


def replay(run, digest):
    if run.request_hash != digest:
        raise HTTPException(
            409,
            detail={
                "code": "IDEMPOTENCY_CONFLICT",
                "message": "Key already used for another request.",
            },
        )
    return run


def admit(tenant_id, key, digest, payload, carrier):
    with session_factory()() as session:
        existing = lookup(session, tenant_id, key)
        if existing is not None:
            return replay(existing, digest)
        # End the fast-path read transaction before taking admission locks.
        session.rollback()
        try:
            with locked_policy(session) as policy:
                tenant = session.scalar(
                    select(Tenant).where(Tenant.id == tenant_id).with_for_update()
                )
                existing = lookup(session, tenant_id, key)
                if existing is not None:
                    return replay(existing, digest)
                count = select(func.count()).select_from(Run).where(Run.status.in_(OUTSTANDING))
                if (
                    session.scalar(count.where(Run.tenant_id == tenant_id))
                    >= tenant.max_outstanding
                ):
                    raise HTTPException(
                        429,
                        detail={
                            "code": "TENANT_CAPACITY_EXCEEDED",
                            "message": "Tenant backlog is full.",
                        },
                        headers={"Retry-After": "5"},
                    )
                if session.scalar(count) >= policy.max_outstanding:
                    raise HTTPException(
                        503,
                        detail={
                            "code": "SERVICE_CAPACITY_EXCEEDED",
                            "message": "Service backlog is full.",
                        },
                        headers={"Retry-After": "5"},
                    )
                run = Run(
                    tenant_id=tenant_id,
                    idempotency_key=key,
                    request_hash=digest,
                    request=payload,
                    trace_context=carrier,
                )
                session.add(run)
                session.flush()
            session.commit()
            return run
        except IntegrityError:
            session.rollback()
            existing = lookup(session, tenant_id, key)
            if existing is None:
                raise
            return replay(existing, digest)
