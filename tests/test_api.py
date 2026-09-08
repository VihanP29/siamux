from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest
from sqlalchemy import func, select

from siamux.db import Run, session_factory
from siamux.worker import process_run


def count_runs():
    with session_factory()() as session:
        return session.scalar(select(func.count()).select_from(Run))


def test_create_retrieve_and_duplicate(submit, client):
    first = submit()
    assert first.status_code == 202
    run = first.json()
    assert run["status"] == "PENDING"
    assert run["attempts"] == 0
    assert run["output"] is None and run["error"] is None
    assert run["created_at"].endswith("Z")
    assert first.headers["location"] == run["status_url"]
    assert client.get(run["status_url"]).json() == run
    assert submit().json()["id"] == run["id"]
    assert count_runs() == 1
    assert process_run(run["id"])
    replay = submit().json()
    assert replay["id"] == run["id"] and replay["status"] == "SUCCEEDED"


def test_conflict(submit):
    submit()
    response = submit(input="different")
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"
    assert count_runs() == 1


def test_canonical_defaults_and_trim(client):
    headers = {"Idempotency-Key": "canonical"}
    first = client.post("/v1/runs", headers=headers, json={"provider": "mock", "input": " hello "})
    second = client.post(
        "/v1/runs",
        headers=headers,
        json={"provider": "mock", "model": "mock-basic", "input": "hello"},
    )
    assert first.json()["id"] == second.json()["id"]


@pytest.mark.parametrize(
    "body",
    [
        {},
        {"provider": "mock"},
        {"provider": "mock", "input": "   "},
        {"provider": "mock", "input": 123},
        {"provider": "mock", "input": "a" * 10001},
        {"provider": "mock", "input": "hi", "unknown": True},
        {"provider": "mock", "input": "hi", "mock": {"fail_first_attempts": -1}},
    ],
)
def test_invalid_body(client, body):
    result = client.post("/v1/runs", headers={"Idempotency-Key": "invalid"}, json=body)
    assert result.status_code == 422
    assert result.json()["error"]["code"] == "INVALID_REQUEST"
    assert count_runs() == 0


@pytest.mark.parametrize("headers", [{}, {"Idempotency-Key": " "}, {"Idempotency-Key": "a" * 101}])
def test_invalid_key(client, headers):
    result = client.post("/v1/runs", headers=headers, json={"provider": "mock", "input": "hi"})
    assert result.status_code == 422
    assert count_runs() == 0


@pytest.mark.parametrize("overrides", [{"provider": "openai"}, {"model": "unknown"}])
def test_unsupported(submit, overrides):
    assert submit(**overrides).status_code == 400
    assert count_runs() == 0


def test_missing_and_malformed_id(client):
    assert client.get(f"/v1/runs/{uuid4()}").status_code == 404
    assert client.get("/v1/runs/not-a-uuid").status_code == 422


def test_concurrent_idempotency(submit):
    with ThreadPoolExecutor(max_workers=8) as pool:
        responses = list(pool.map(lambda _: submit(), range(16)))
    assert {r.status_code for r in responses} == {202}
    assert len({r.json()["id"] for r in responses}) == 1
    assert count_runs() == 1


def test_health(client):
    assert client.get("/health/live").status_code == 200
    assert client.get("/health/ready").status_code == 200
