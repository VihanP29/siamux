import hashlib
import json
from contextlib import asynccontextmanager
from time import monotonic
from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from opentelemetry import propagate, trace
from sqlalchemy import select, text, update
from sqlalchemy.exc import SQLAlchemyError

from siamux.admission import admit
from siamux.auth import authenticate
from siamux.db import Run, session_factory, utcnow
from siamux.schemas import CreateRun, RunView, view
from siamux.telemetry import configure, event


@asynccontextmanager
async def lifespan(app):
    configure("siamux-api")
    yield


app = FastAPI(title="Siamux", version="0.1.0", lifespan=lifespan)


def problem(status, code, message, headers=None):
    return JSONResponse(
        status_code=status, content={"error": {"code": code, "message": message}}, headers=headers
    )


@app.exception_handler(HTTPException)
async def http_error(request, exc):
    return problem(exc.status_code, exc.detail["code"], exc.detail["message"], exc.headers)


@app.exception_handler(RequestValidationError)
async def validation_error(request, exc):
    return JSONResponse(
        status_code=422,
        content={
            "error": {
                "code": "INVALID_REQUEST",
                "message": "Request validation failed.",
                "fields": [
                    {"field": ".".join(map(str, e["loc"])), "message": e["msg"]}
                    for e in exc.errors()
                ],
            }
        },
    )


@app.exception_handler(SQLAlchemyError)
async def database_error(request, exc):
    event("database_unavailable", exception_type=type(exc).__name__)
    return problem(503, "DATABASE_UNAVAILABLE", "Database unavailable; retry with the same key.")


@app.middleware("http")
async def trace_request(request: Request, call_next):
    start = monotonic()
    tracer = trace.get_tracer("siamux")
    with tracer.start_as_current_span(
        f"HTTP {request.method}",
        context=propagate.extract(dict(request.headers)),
        kind=trace.SpanKind.SERVER,
        record_exception=False,
        set_status_on_exception=False,
    ) as span:
        response = await call_next(request)
        span.set_attribute("http.response.status_code", response.status_code)
        event(
            "http_request",
            method=request.method,
            status_code=response.status_code,
            duration_ms=round((monotonic() - start) * 1000, 2),
        )
        return response


@app.get("/health/live")
def live():
    return {"status": "ok"}


@app.get("/health/ready")
def ready():
    with session_factory()() as session:
        session.execute(text("SELECT 1 FROM runs LIMIT 1"))
    return {"status": "ready"}


@app.post("/v1/runs", response_model=RunView, status_code=202)
def create_run(
    body: CreateRun,
    tenant_id: Annotated[str, Depends(authenticate)],
    response: Response,
    idempotency_key: Annotated[str, Header(min_length=1, max_length=100)],
):
    if not idempotency_key.strip():
        raise HTTPException(422, detail={"code": "INVALID_REQUEST", "message": "Key is blank."})
    if body.provider != "mock" or body.model != "mock-basic":
        raise HTTPException(
            400,
            detail={
                "code": "UNSUPPORTED_MODEL",
                "message": "Use provider mock and model mock-basic.",
            },
        )
    payload = body.model_dump()
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    carrier = {}
    propagate.inject(carrier)
    run = admit(tenant_id, idempotency_key, digest, payload, carrier)
    result = view(run)
    response.headers["Location"] = result.status_url
    event("run_accepted", run_id=run.id, status=run.status)
    return result


@app.get("/v1/runs/{run_id}", response_model=RunView)
def get_run(run_id: UUID, tenant_id: Annotated[str, Depends(authenticate)]):
    with session_factory()() as session:
        run = session.scalar(select(Run).where(Run.id == str(run_id), Run.tenant_id == tenant_id))
        if run is None:
            raise HTTPException(404, detail={"code": "RUN_NOT_FOUND", "message": "Run not found."})
        return view(run)


@app.post("/v1/runs/{run_id}/cancel", response_model=RunView)
def cancel_run(run_id: UUID, tenant_id: Annotated[str, Depends(authenticate)]):
    """Atomically cancel accepted work without allowing a late result to overwrite it."""
    now = utcnow()
    run_id = str(run_id)
    with session_factory()() as session:
        canceled = session.execute(
            update(Run)
            .where(
                Run.id == run_id, Run.tenant_id == tenant_id, Run.status.in_(("PENDING", "RUNNING"))
            )
            .values(
                status="CANCELED",
                completed_at=now,
                claim_token=None,
                lease_until=None,
                output=None,
                error={"code": "RUN_CANCELED", "message": "Run canceled by client."},
            )
        )
        session.commit()
        run = session.scalar(select(Run).where(Run.id == run_id, Run.tenant_id == tenant_id))
        if run is None:
            raise HTTPException(404, detail={"code": "RUN_NOT_FOUND", "message": "Run not found."})
        if canceled.rowcount == 0 and run.status != "CANCELED":
            raise HTTPException(
                409,
                detail={
                    "code": "RUN_NOT_CANCELABLE",
                    "message": f"Run already reached terminal status {run.status}.",
                },
            )
        event("run_canceled", run_id=run.id, status=run.status)
        return view(run)
