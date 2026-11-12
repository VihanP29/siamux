import asyncio
from datetime import timedelta
from time import monotonic
from uuid import uuid4

from celery import Celery
from celery.signals import worker_process_init
from opentelemetry import propagate, trace
from sqlalchemy import and_, or_, select, update

from siamux.config import get_settings
from siamux.db import Run, session_factory, utcnow
from siamux.provider import ProviderFailure, invoke
from siamux.telemetry import configure, event

settings = get_settings()
celery_app = Celery("siamux", broker=settings.broker_url)
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    task_ignore_result=True,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    broker_connection_retry_on_startup=True,
    broker_transport_options={
        "visibility_timeout": 120,
        "socket_connect_timeout": 2,
        "socket_timeout": 2,
    },
)


@worker_process_init.connect
def init_worker(**kwargs):
    configure("siamux-worker")


def eligible(now):
    return or_(
        and_(Run.status == "PENDING", Run.due_at <= now),
        and_(Run.status == "RUNNING", Run.lease_until <= now),
    )


def process_run(run_id: str) -> bool:
    """Claim with an atomic conditional UPDATE; fence every completion with its token."""
    config = get_settings()
    now = utcnow()
    token = str(uuid4())
    with session_factory()() as session:
        # A killed worker consumes an attempt too; repeated crashes cannot retry forever.
        session.execute(
            update(Run)
            .where(
                Run.id == run_id,
                eligible(now),
                Run.attempts >= config.max_attempts,
            )
            .values(
                status="FAILED",
                completed_at=now,
                claim_token=None,
                lease_until=None,
                error={"code": "ATTEMPTS_EXHAUSTED", "message": "Execution attempts exhausted."},
            )
        )
        claimed = session.execute(
            update(Run)
            .where(
                Run.id == run_id,
                eligible(now),
                Run.attempts < config.max_attempts,
            )
            .values(
                status="RUNNING",
                attempts=Run.attempts + 1,
                claim_token=token,
                lease_until=now + timedelta(seconds=config.lease_seconds),
            )
        )
        if claimed.rowcount != 1:
            session.commit()
            return False
        run = session.scalar(select(Run).where(Run.id == run_id))
        if run.started_at is None:
            run.started_at = now
            run.queue_latency_ms = max(0, (now - run.created_at).total_seconds() * 1000)
        session.commit()
        request, attempt, carrier = run.request, run.attempts, run.trace_context

    tracer = trace.get_tracer("siamux")
    with tracer.start_as_current_span(
        "run.execute",
        context=propagate.extract(carrier),
        kind=trace.SpanKind.CONSUMER,
        record_exception=False,
        set_status_on_exception=False,
    ) as span:
        span.set_attribute("run.id", run_id)
        span.set_attribute("run.attempt", attempt)
        event("run_started", run_id=run_id, attempt=attempt, status="RUNNING")
        start = monotonic()
        output, error = None, None
        retryable = False
        try:
            with tracer.start_as_current_span(
                "provider.generate", record_exception=False, set_status_on_exception=False
            ):
                output = asyncio.run(invoke(request, attempt, config.provider_timeout))
        except ProviderFailure as exc:
            error = {"code": exc.code, "message": str(exc)}
            retryable = exc.retryable
        except Exception as exc:
            # Never persist arbitrary exception messages, which may contain credentials or input.
            event("unexpected_provider_error", run_id=run_id, exception_type=type(exc).__name__)
            error = {"code": "INTERNAL_ERROR", "message": "Unexpected provider error."}
        duration = (monotonic() - start) * 1000
        finished = utcnow()
        retry = error is not None and retryable and attempt < config.max_attempts
        state = "PENDING" if retry else ("FAILED" if error else "SUCCEEDED")
        values = dict(
            status=state,
            output=output,
            error=error,
            claim_token=None,
            lease_until=None,
            provider_duration_ms=duration,
        )
        if retry:
            due = finished + timedelta(seconds=config.retry_base_seconds * (2 ** (attempt - 1)))
            values.update(due_at=due, dispatch_after=due)
        else:
            values["completed_at"] = finished
        with session_factory()() as session:
            result = session.execute(
                update(Run)
                .where(
                    Run.id == run_id,
                    Run.status == "RUNNING",
                    Run.claim_token == token,
                )
                .values(**values)
            )
            session.commit()
        if error:
            span.set_attribute("error.type", error["code"])
        event(
            "run_finished" if result.rowcount else "stale_completion_ignored",
            run_id=run_id,
            attempt=attempt,
            status=state,
            duration_ms=round(duration, 2),
        )
        return result.rowcount == 1


@celery_app.task(name="siamux.execute")
def execute(run_id: str):
    process_run(run_id)
