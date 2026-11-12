import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Event

import pytest
from sqlalchemy import update

from siamux import worker
from siamux.db import Run, session_factory, utcnow
from siamux.dispatcher import dispatch_once


def read(run_id):
    with session_factory()() as session:
        return session.get(Run, run_id)


def change(run_id, **values):
    with session_factory()() as session:
        session.execute(update(Run).where(Run.id == run_id).values(**values))
        session.commit()


def test_success_and_duplicate_delivery(submit, client):
    run = submit().json()
    assert dispatch_once(publish=worker.process_run) == 1
    result = client.get(run["status_url"]).json()
    assert result["status"] == "SUCCEEDED"
    assert result["output"]["text"] == "Mock response: hello"
    assert result["attempts"] == 1
    assert result["started_at"] and result["completed_at"]
    assert result["queue_latency_ms"] >= 0
    assert not worker.process_run(run["id"])
    assert dispatch_once(publish=worker.process_run) == 0


def test_transient_failure_and_backoff(submit):
    run_id = submit(mock={"delay_seconds": 0.0, "fail_first_attempts": 2}).json()["id"]
    for attempt in (1, 2):
        assert worker.process_run(run_id)
        run = read(run_id)
        assert run.status == "PENDING" and run.attempts == attempt
        assert run.completed_at is None
        # Retrying immediately cannot bypass persisted backoff.
        change(run_id, due_at=utcnow() + timedelta(seconds=10))
        assert not worker.process_run(run_id)
        change(run_id, due_at=utcnow() - timedelta(seconds=1))
    assert worker.process_run(run_id)
    run = read(run_id)
    assert run.status == "SUCCEEDED" and run.attempts == 3 and run.error is None


@pytest.mark.parametrize(
    "options,code,attempts",
    [
        ({"delay_seconds": 0.0, "permanent_failure": True}, "PROVIDER_REJECTED", 1),
        ({"delay_seconds": 0.0, "fail_first_attempts": 10}, "PROVIDER_UNAVAILABLE", 3),
        ({"delay_seconds": 1.0}, "PROVIDER_TIMEOUT", 3),
    ],
)
def test_failures(submit, options, code, attempts):
    run_id = submit(mock=options).json()["id"]
    for _ in range(attempts):
        change(run_id, due_at=utcnow() - timedelta(seconds=1))
        assert worker.process_run(run_id)
    run = read(run_id)
    assert run.status == "FAILED" and run.attempts == attempts
    assert run.error["code"] == code
    assert run.output is None and run.completed_at
    assert not worker.process_run(run_id)


def test_broker_outage_recovery(submit):
    run_id = submit().json()["id"]

    def unavailable(_):
        raise ConnectionError("simulated broker outage")

    assert dispatch_once(publish=unavailable) == 0
    assert read(run_id).status == "PENDING"
    change(run_id, dispatch_after=utcnow() - timedelta(seconds=1))
    assert dispatch_once(publish=worker.process_run) == 1
    assert read(run_id).status == "SUCCEEDED"


def test_lost_message_is_republished(submit):
    run_id = submit().json()["id"]
    assert dispatch_once(publish=lambda _: None) == 1
    change(run_id, dispatch_after=utcnow() - timedelta(seconds=1))
    assert dispatch_once(publish=worker.process_run) == 1
    assert read(run_id).status == "SUCCEEDED"


def test_worker_crash_recovery_and_exhaustion(submit):
    run_id = submit().json()["id"]
    change(
        run_id,
        status="RUNNING",
        attempts=1,
        claim_token="old",
        lease_until=utcnow() - timedelta(seconds=1),
    )
    assert worker.process_run(run_id)
    assert read(run_id).attempts == 2 and read(run_id).status == "SUCCEEDED"
    other = submit(key="exhausted").json()["id"]
    change(
        other,
        status="RUNNING",
        attempts=3,
        claim_token="old",
        lease_until=utcnow() - timedelta(seconds=1),
    )
    assert not worker.process_run(other)
    assert read(other).status == "FAILED"
    assert read(other).error["code"] == "ATTEMPTS_EXHAUSTED"


def test_simultaneous_delivery_claims_once(submit, monkeypatch):
    run_id = submit().json()["id"]
    started, finish = Event(), Event()

    async def blocked(*args):
        started.set()
        await asyncio.to_thread(finish.wait, 5)
        return {"text": "single execution"}

    monkeypatch.setattr(worker, "invoke", blocked)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(worker.process_run, run_id)
        try:
            assert started.wait(5)
            assert not pool.submit(worker.process_run, run_id).result(timeout=5)
        finally:
            finish.set()
        assert first.result(timeout=5)
    assert read(run_id).attempts == 1


def test_stale_worker_cannot_overwrite_new_attempt(submit, monkeypatch):
    run_id = submit().json()["id"]
    started, finish = Event(), Event()

    async def blocked(request, attempt, timeout):
        if attempt == 1:
            started.set()
            await asyncio.to_thread(finish.wait, 5)
            return {"text": "stale"}
        return {"text": "new"}

    monkeypatch.setattr(worker, "invoke", blocked)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(worker.process_run, run_id)
        try:
            assert started.wait(5)
            change(run_id, lease_until=utcnow() - timedelta(seconds=1))
            assert worker.process_run(run_id)
        finally:
            finish.set()
        assert not first.result(timeout=5)
    assert read(run_id).output == {"text": "new"}
    assert read(run_id).attempts == 2


def test_dispatch_waits_for_active_lease(submit):
    run_id = submit().json()["id"]
    change(
        run_id,
        status="RUNNING",
        attempts=1,
        claim_token="active",
        lease_until=utcnow() + timedelta(seconds=30),
    )
    published = []
    assert dispatch_once(publish=published.append) == 0
    assert published == []
    change(run_id, lease_until=utcnow() - timedelta(seconds=1))
    assert dispatch_once(publish=published.append) == 1
    assert published == [run_id]


def test_cancel_running_run_fences_late_completion(submit, client, monkeypatch):
    run = submit().json()
    started, finish = Event(), Event()

    async def blocked(*args):
        started.set()
        await asyncio.to_thread(finish.wait, 5)
        return {"text": "too late"}

    monkeypatch.setattr(worker, "invoke", blocked)
    with ThreadPoolExecutor(max_workers=1) as pool:
        execution = pool.submit(worker.process_run, run["id"])
        try:
            assert started.wait(5)
            canceled = client.post(f"{run['status_url']}/cancel")
            assert canceled.status_code == 200
            assert canceled.json()["status"] == "CANCELED"
        finally:
            finish.set()
        assert not execution.result(timeout=5)
    saved = read(run["id"])
    assert saved.status == "CANCELED"
    assert saved.output is None
    assert saved.error["code"] == "RUN_CANCELED"
    assert dispatch_once(publish=worker.process_run) == 0
