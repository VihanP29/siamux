# Siamux

Siamux is an asynchronous job-processing service built with FastAPI, PostgreSQL, Redis, and Celery. Clients submit generation requests, receive a run ID, and poll for results. PostgreSQL stores job state and retry schedules; Redis transports work to the workers.

The current implementation uses a configurable mock provider for testing timeouts, retries, and recovery. It requires no provider credentials; clients authenticate with locally issued Siamux API keys.

## Quick start

Requires Docker with Compose.

```sh
git clone https://github.com/VihanP29/siamux.git
cd siamux
docker compose up --build -d --wait
```

This starts the API, PostgreSQL, Redis, dispatcher, Celery worker, and Jaeger. Database migrations run automatically before the application starts.

- [API documentation](http://localhost:8000/docs)
- [Traces](http://localhost:16686)

New to the codebase? Follow the [guided codebase tour](docs/guide.md) before changing it.

Issue a client key for the migrated development tenant (the key is shown only once).
There is no default client key:

```sh
export SIAMUX_API_KEY="$(docker compose exec -T api python -m siamux.admin key create \
  --tenant 00000000-0000-0000-0000-000000000001 \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["key"])')"
```

Submit a run:

```sh
curl -i http://localhost:8000/v1/runs \
  -H "Authorization: Bearer $SIAMUX_API_KEY" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: example-001' \
  -d '{"provider":"mock","input":"Summarize this request"}'
```

The API returns `202 Accepted` with a run ID and `status_url`. Use that URL to retrieve the state and result:

```sh
curl -H "Authorization: Bearer $SIAMUX_API_KEY" \
  'http://localhost:8000/v1/runs/<run_id>'
```

To inspect logs or stop the services:

```sh
docker compose logs -f api dispatcher worker
docker compose down
```

Database and Redis volumes are retained when the stack stops.

## Architecture

```mermaid
flowchart LR
    Client --> API[FastAPI]
    API --> DB[(PostgreSQL)]
    DB --> Dispatcher
    Dispatcher --> Redis[(Redis)]
    Redis --> Worker[Celery worker]
    Worker --> Provider[Mock provider]
    Worker --> DB
```

The API commits a run before returning its ID. A dispatcher reads due runs from PostgreSQL and publishes their IDs to Redis. Accepted jobs remain in the database during a broker outage and are dispatched when Redis becomes available again.

Workers claim runs with an atomic database update and an execution lease. Each attempt receives a token that must match when saving its result, preventing an older attempt from overwriting a newer one. Expired leases allow work to be retried within the attempt limit.

Execution is **at least once**. A crash after a provider call but before the result is committed can cause that call to be repeated. Submission idempotency prevents duplicate run records; it does not guarantee exactly-once provider execution.

## API

| Endpoint | Description |
| --- | --- |
| `POST /v1/runs` | Accept a request or return an existing run for the same idempotency key |
| `GET /v1/runs/{run_id}` | Retrieve state, result, error, timestamps, and attempt count |
| `POST /v1/runs/{run_id}/cancel` | Idempotently cancel pending or running work |
| `GET /health/live` | Check API availability |
| `GET /health/ready` | Check database connectivity and schema availability |

All `/v1/runs` operations require `Authorization: Bearer <key>`. Health probes remain anonymous. Foreign and missing run IDs both return `404 RUN_NOT_FOUND`. Submissions also require an `Idempotency-Key` header containing 1–100 characters and a nonblank value. Repeating a key with the same normalized request returns the original run; a different request returns `409 Conflict`. Keys are scoped to the authenticated tenant and retained with the run. Tenant identity cannot be supplied in the request body.

The request body requires `provider: "mock"` and `input` containing 1–10,000 characters after trimming surrounding whitespace. `model` defaults to `mock-basic`. Unknown fields are rejected. See the generated API documentation for the full schema.

Runs move from `PENDING` to `RUNNING`, then to `SUCCEEDED`, `FAILED`, or `CANCELED`. Retryable failures return to `PENDING` until the next attempt is due. Canceling a pending run prevents execution. Canceling a running run prevents its result from being committed, although an in-flight provider call may still finish. Cancellation is idempotent; attempting to cancel a succeeded or failed run returns `409 Conflict`. A failed or canceled run is returned by GET with HTTP `200` and a structured `error`; invalid submissions are rejected before a run is created.

## Configuration

Settings use the `SIAMUX_` prefix. See [.env.example](.env.example) for local process settings. Container settings are defined in [compose.yaml](compose.yaml).

| Setting | Default |
| --- | --- |
| `SIAMUX_PROVIDER_TIMEOUT` | 5 seconds |
| `SIAMUX_MAX_ATTEMPTS` | 3 total attempts |
| `SIAMUX_RETRY_BASE_SECONDS` | 1 second, doubled after each retryable failure |
| `SIAMUX_LEASE_SECONDS` | 30 seconds |
| `SIAMUX_DISPATCH_INTERVAL` | 1 second |
| `SIAMUX_REDISPATCH_SECONDS` | 10 seconds |

The mock provider accepts optional fault-injection settings:

```json
{
  "provider": "mock",
  "input": "retry test",
  "mock": {
    "delay_seconds": 0.1,
    "fail_first_attempts": 2
  }
}
```

With the default configuration, this request succeeds on its third attempt. Set `permanent_failure` to `true` to fail without retrying, or `delay_seconds` to `6` to exercise provider timeouts.

## Development

Requires Python 3.11 or later. The Docker image uses Python 3.12.

```sh
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.lock
pip install --no-deps -e .

pytest -q
ruff check .
ruff format --check .
```

Tests use temporary SQLite databases by default. Set `SIAMUX_TEST_DATABASE_URL` to run them against PostgreSQL. Use a disposable test database: the fixtures migrate it and clear runs, keys, tenants, and admission policy. Populated migration tests use separate SQLite files or temporary PostgreSQL schemas.

With the Compose stack running:

```sh
python scripts/smoke.py
python scripts/recovery.py
python scripts/benchmark.py --jobs 100 --concurrency 10
```

The smoke test covers successful execution, retries, timeouts, permanent failures, and idempotency. The recovery test temporarily stops Redis and kills a worker, then restores both services and checks job completion.

The benchmark reports acceptance latency, completion latency, queue latency, and successful jobs per second. It limits concurrent in-flight jobs and includes polling overhead; it is not a measurement of maximum API throughput.

[CI](.github/workflows/ci.yml) checks formatting, PostgreSQL tests, migrations, and the Compose smoke and recovery tests.

## Observability

Application logs include run IDs, attempt counts, state transitions, durations, and trace IDs. Prompt text, output, and idempotency keys are excluded. OpenTelemetry context is persisted with each run so API, worker, and provider spans share a trace across retries. Compose exports traces to Jaeger.

Run responses include `queue_latency_ms` for the time between acceptance and the first attempt, and `provider_duration_ms` for the latest attempt.

## Current scope

This is a local development setup. Services bind to localhost and use development credentials; Jaeger storage is ephemeral. Tenant authentication and durable backlog limits are implemented. Real provider integrations, provider-side aborts, attempt history, and aggregate monitoring remain follow-up work. PostgreSQL contention tests and isolated container benchmarks now pass; see [verification](docs/verification.md) for workloads, clock diagnostics, and measurement limits.

## Resume benchmark experiments

See [the benchmark guide](docs/benchmarks.md) for isolated Redis-outage, worker-crash,
worker-scaling, and concurrent-idempotency experiments with saved JSON results.


## Tenants, migrations, and backlog limits

See [operator instructions](docs/operations.md) for tenant creation, key rotation,
migration/backfill and downgrade restrictions, and changing database-stored limits.
Defaults are 100 outstanding runs per tenant and 1,000 globally. Pending and running
runs (including retries and expired leases) consume capacity. Replays and conflicts
are resolved before checking capacity. Full tenants receive `429
TENANT_CAPACITY_EXCEEDED`; a full service receives `503 SERVICE_CAPACITY_EXCEEDED`.
Both include `Retry-After: 5`, a hint rather than a completion promise. Database
failures return `503 DATABASE_UNAVAILABLE`; always retry with the same key.

The [canonical roadmap](docs/roadmap.md) describes the product direction;
[plan.md](plan.md) records the immediate PR 0–3 scope and remaining validation gates.
