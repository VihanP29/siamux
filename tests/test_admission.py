from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Barrier
from uuid import uuid4

import pytest
from sqlalchemy import func, select
from test_auth import other_identity

from siamux.admin import main
from siamux.db import AdmissionPolicy, Run, Tenant, session_factory, utcnow
from siamux.worker import process_run


def limits(tenant_id, tenant=100, global_limit=1000):
    main(["limits", "set", "--tenant", tenant_id, "--max-outstanding", str(tenant)])
    main(["limits", "set", "--global", "--max-outstanding", str(global_limit)])


def outstanding():
    with session_factory()() as session:
        return session.scalar(
            select(func.count()).select_from(Run).where(Run.status.in_(("PENDING", "RUNNING")))
        )


def race(function, count=8):
    gate = Barrier(count)

    def one(index):
        gate.wait(timeout=10)
        return function(index)

    with ThreadPoolExecutor(max_workers=count) as pool:
        return list(pool.map(one, range(count)))


def test_tenant_cap_under_concurrent_distinct_submissions(submit, identity):
    limits(identity[0], tenant=3)
    responses = race(lambda i: submit(key=str(i)))
    assert sum(r.status_code == 202 for r in responses) == 3
    assert sum(r.status_code == 429 for r in responses) == 5
    assert outstanding() == 3
    for response in responses:
        if response.status_code == 429:
            assert response.headers["Retry-After"] == "5"
            assert response.json()["error"]["code"] == "TENANT_CAPACITY_EXCEEDED"


def test_global_cap_with_two_tenants(client, identity):
    _, other = other_identity()
    limits(identity[0], global_limit=3)

    def one(index):
        headers = {"Idempotency-Key": str(uuid4())}
        if index % 2:
            headers["Authorization"] = f"Bearer {other}"
        return client.post("/v1/runs", headers=headers, json={"provider": "mock", "input": "race"})

    responses = race(one)
    assert sum(r.status_code == 202 for r in responses) == 3
    assert sum(r.status_code == 503 for r in responses) == 5
    assert outstanding() == 3
    assert all(r.headers["Retry-After"] == "5" for r in responses if r.status_code == 503)


def test_replay_racing_acceptance_at_both_caps(submit, identity):
    limits(identity[0], tenant=1, global_limit=1)
    responses = race(lambda _: submit())
    assert {r.status_code for r in responses} == {202}
    assert len({r.json()["id"] for r in responses}) == 1
    assert submit(input="conflict").status_code == 409
    assert submit(key="new").status_code == 429  # Tenant error takes precedence.
    assert outstanding() == 1


@pytest.mark.parametrize("terminal", ["SUCCEEDED", "FAILED", "CANCELED"])
def test_terminal_work_releases_capacity(submit, client, identity, terminal):
    limits(identity[0], tenant=1)
    run = submit(mock={"delay_seconds": 0, "permanent_failure": terminal == "FAILED"}).json()
    assert submit(key="blocked").status_code == 429
    if terminal == "CANCELED":
        client.post(run["status_url"] + "/cancel")
        client.post(run["status_url"] + "/cancel")
    else:
        assert process_run(run["id"])
        assert not process_run(run["id"])
    assert client.get(run["status_url"]).json()["status"] == terminal
    assert submit(key="next").status_code == 202
    assert outstanding() == 1


@pytest.mark.parametrize("state", ["PENDING", "RUNNING"])
def test_delayed_retry_and_expired_lease_consume_capacity(submit, identity, state):
    limits(identity[0], tenant=1)
    run_id = submit().json()["id"]
    with session_factory()() as session:
        run = session.get(Run, run_id)
        run.status = state
        run.due_at = utcnow() + timedelta(days=1)
        run.lease_until = utcnow() - timedelta(days=1)
        session.commit()
    assert submit(key="new").status_code == 429


def test_lower_limits_drain_without_removing_work(submit, client, identity):
    runs = [submit(key=str(i)).json() for i in range(3)]
    limits(identity[0], tenant=1, global_limit=1)
    assert submit(key="blocked").status_code == 429
    for run in runs[:2]:
        client.post(run["status_url"] + "/cancel")
    assert submit(key="blocked").status_code == 429
    client.post(runs[2]["status_url"] + "/cancel")
    assert submit(key="blocked").status_code == 202


def test_policy_and_tenant_constraints(identity):
    from sqlalchemy.exc import IntegrityError

    with session_factory()() as session:
        session.get(Tenant, identity[0]).max_outstanding = 0
        with pytest.raises(IntegrityError):
            session.commit()
        session.rollback()
        session.add(AdmissionPolicy(id=2, max_outstanding=10))
        with pytest.raises(IntegrityError):
            session.commit()


@pytest.mark.parametrize("after_commit", [False, True])
def test_commit_failure_and_response_loss_resolve_with_same_key(
    submit, identity, monkeypatch, after_commit
):
    from sqlalchemy.exc import OperationalError
    from sqlalchemy.orm import Session

    limits(identity[0], tenant=1)
    original = Session.commit

    def broken(session):
        if after_commit:
            original(session)
        raise OperationalError("hidden", {}, Exception("private"))

    with monkeypatch.context() as patch:
        patch.setattr(Session, "commit", broken)
        response = submit()
    assert response.status_code == 503
    assert response.json()["error"]["code"] == "DATABASE_UNAVAILABLE"
    assert outstanding() == int(after_commit)
    accepted = submit()
    assert accepted.status_code == 202
    assert outstanding() == 1
    assert submit().json()["id"] == accepted.json()["id"]


def test_broker_outage_at_cap_preserves_reads_replays_and_cancellation(submit, client, identity):
    from sqlalchemy import update

    from siamux.dispatcher import dispatch_once

    limits(identity[0], tenant=2)
    runs = [submit(key=str(i)).json() for i in range(2)]

    def unavailable(_):
        raise ConnectionError("offline")

    assert dispatch_once(publish=unavailable) == 0
    assert submit(key="blocked").status_code == 429
    assert submit(key="0").json()["id"] == runs[0]["id"]
    assert client.get(runs[0]["status_url"]).status_code == 200
    assert client.post(runs[0]["status_url"] + "/cancel").status_code == 200
    assert submit(key="next").status_code == 202
    with session_factory()() as session:
        session.execute(update(Run).values(dispatch_after=utcnow() - timedelta(seconds=1)))
        session.commit()
    assert dispatch_once(publish=process_run) == 2
    assert outstanding() == 0


def test_limits_updates_and_submissions_share_lock_order(submit, identity):
    gate = Barrier(2)

    def update_limits():
        gate.wait(timeout=10)
        main(["limits", "set", "--tenant", identity[0], "--max-outstanding", "1"])

    def accept():
        gate.wait(timeout=10)
        return submit()

    with ThreadPoolExecutor(max_workers=2) as pool:
        changing = pool.submit(update_limits)
        accepting = pool.submit(accept)
        changing.result(timeout=10)
        assert accepting.result(timeout=10).status_code == 202
    assert submit(key="blocked").status_code == 429
    assert outstanding() == 1
