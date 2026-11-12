import json
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.exc import OperationalError

from siamux import auth
from siamux.auth import issue_key
from siamux.db import APIKey, Run, Tenant, session_factory, utcnow


def other_identity():
    with session_factory()() as session:
        tenant = Tenant(name="other")
        session.add(tenant)
        session.flush()
        raw = issue_key(session, tenant.id)
        session.commit()
        return tenant.id, raw


@pytest.mark.parametrize(
    "value", [None, "Basic bad", "Bearer bad", f"Bearer sx_{uuid4()}.{'x' * 43}"]
)
def test_invalid_credentials(anonymous, value):
    headers = {} if value is None else {"Authorization": value}
    for method, url, kwargs in [
        ("post", "/v1/runs", {"json": {"provider": "mock", "input": "test"}}),
        ("get", f"/v1/runs/{uuid4()}", {}),
        ("post", f"/v1/runs/{uuid4()}/cancel", {}),
    ]:
        response = getattr(anonymous, method)(url, headers=headers, **kwargs)
        assert response.status_code == 401
        assert response.json()["error"]["code"] == "UNAUTHORIZED"
        assert response.headers["WWW-Authenticate"] == "Bearer"
    assert anonymous.get("/health/live").status_code == 200
    assert anonymous.get("/health/ready").status_code == 200


@pytest.mark.parametrize("state", ["PENDING", "RUNNING", "SUCCEEDED", "FAILED", "CANCELED"])
def test_foreign_reads_and_cancellation_are_missing(submit, client, state):
    run = submit().json()
    with session_factory()() as session:
        session.get(Run, run["id"]).status = state
        session.commit()
    _, raw = other_identity()
    headers = {"Authorization": f"Bearer {raw}"}
    missing = client.get(f"/v1/runs/{uuid4()}", headers=headers)
    assert client.get(run["status_url"], headers=headers).json() == missing.json()
    foreign = client.post(run["status_url"] + "/cancel", headers=headers)
    assert foreign.status_code == 404 and foreign.json() == missing.json()
    assert client.get(run["status_url"]).json()["status"] == state


def test_keys_are_tenant_scoped_and_revocable(submit, client, identity):
    first = submit().json()
    _, raw = other_identity()
    second = client.post(
        "/v1/runs",
        headers={"Authorization": f"Bearer {raw}", "Idempotency-Key": "test-key"},
        json={"provider": "mock", "input": "different"},
    )
    assert second.status_code == 202 and second.json()["id"] != first["id"]
    assert submit(input="different").status_code == 409
    with session_factory()() as session:
        old = session.scalar(select(APIKey).where(APIKey.tenant_id == identity[0]))
        old.revoked_at = utcnow()
        alternate = issue_key(session, identity[0])
        session.commit()
        stored = [{"digest": k.digest, "id": k.id} for k in session.scalars(select(APIKey))]
    assert identity[1] not in json.dumps(stored)
    assert client.get(first["status_url"]).status_code == 401
    assert (
        client.get(
            first["status_url"], headers={"Authorization": f"Bearer {alternate}"}
        ).status_code
        == 200
    )
    wrong = alternate[:-1] + ("A" if alternate[-1] != "A" else "B")
    assert (
        client.get(first["status_url"], headers={"Authorization": f"Bearer {wrong}"}).status_code
        == 401
    )


def test_auth_database_failure_is_retryable(client, monkeypatch):
    def broken():
        raise OperationalError("hidden", {}, Exception("sentinel-secret"))

    monkeypatch.setattr(auth, "session_factory", broken)
    response = client.get(f"/v1/runs/{uuid4()}")
    assert response.status_code == 503
    assert response.json()["error"]["code"] == "DATABASE_UNAVAILABLE"
    assert "sentinel-secret" not in response.text
