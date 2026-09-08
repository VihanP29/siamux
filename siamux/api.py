import hashlib
import json
from contextlib import asynccontextmanager
from time import monotonic
from typing import Annotated
from uuid import UUID

from fastapi import FastAPI, Header, HTTPException, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from opentelemetry import propagate, trace
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from siamux.db import Run, session_factory
from siamux.schemas import CreateRun, RunView, view
from siamux.telemetry import configure, event


@asynccontextmanager
async def lifespan(app):
    configure("siamux-api")
    yield


app = FastAPI(title="Siamux", version="0.1.0", lifespan=lifespan)


def problem(status, code, message):
    return JSONResponse(status_code=status, content={"error": {"code": code, "message": message}})


@app.exception_handler(HTTPException)
async def http_error(request, exc):
    return problem(exc.status_code, exc.detail["code"], exc.detail["message"])


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
    with session_factory()() as session:
        run = Run(
            idempotency_key=idempotency_key,
            request_hash=digest,
            request=payload,
            trace_context=carrier,
        )
        session.add(run)
        try:
            # The durable row doubles as dispatch intent: no database/broker dual write.
            session.commit()
        except IntegrityError:
            session.rollback()
            run = session.scalar(select(Run).where(Run.idempotency_key == idempotency_key))
            if run is None:
                raise
            if run.request_hash != digest:
                raise HTTPException(
                    409,
                    detail={
                        "code": "IDEMPOTENCY_CONFLICT",
                        "message": "Key already used for another request.",
                    },
                ) from None
        result = view(run)
        response.headers["Location"] = result.status_url
        event("run_accepted", run_id=run.id, status=run.status)
        return result


@app.get("/v1/runs/{run_id}", response_model=RunView)
def get_run(run_id: UUID):
    with session_factory()() as session:
        run = session.get(Run, str(run_id))
        if run is None:
            raise HTTPException(404, detail={"code": "RUN_NOT_FOUND", "message": "Run not found."})
        return view(run)
