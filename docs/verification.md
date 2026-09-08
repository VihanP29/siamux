# Verification record

Verified on September 7, 2026, on macOS ARM64 with Python 3.12 and Docker Desktop.

| Check | Result |
|---|---|
| Ruff lint and format | Passed |
| SQLite unit/service suite | 30 passed |
| Real PostgreSQL unit/service suite | 30 passed |
| Alembic upgrade, drift check, downgrade, re-upgrade | Passed against PostgreSQL and SQLite |
| Docker Compose image build and startup | Passed; API, PostgreSQL and Redis healthy |
| Container-backed HTTP smoke test | Success, transient retries, permanent failure, timeout, replay and conflict passed |
| Jaeger trace ingestion | Confirmed one trace containing HTTP POST, run.execute and provider.generate across API and worker services |
| Redis outage | HTTP accepted a job while Redis was stopped; completed after Redis restarted, one attempt |
| Worker crash | SIGKILL during RUNNING; completed after lease recovery, two attempts |

The service suite includes concurrent idempotency submissions, competing worker claims,
late completion fencing, lost-message redispatch, retry exhaustion and trace propagation.
Two upstream test-client deprecation warnings remain; there were no test failures.

Before Docker was available, the backend was also exercised using temporary native
PostgreSQL and Redis instances, separate API/dispatcher processes, and two Celery worker
processes. All four smoke scenarios passed. A small 20-job synthetic run completed
20/20 jobs; this was a functional check, not a capacity benchmark. No production
throughput, scale, or availability claims are made.

The Docker stack was left running after verification. Temporary native test services
were stopped. The GitHub Actions workflow has been added but was not remotely executed
as part of this local session.

To reproduce from the repository root:

```sh
docker compose up --build -d --wait
.venv/bin/pytest -q
.venv/bin/python scripts/smoke.py
.venv/bin/python scripts/recovery.py
```

Use a dedicated disposable PostgreSQL database with `SIAMUX_TEST_DATABASE_URL` for the
PostgreSQL service suite. Do not point it at the running application's database: the
test fixture deletes the rows in its test runs table. See the README for details.
