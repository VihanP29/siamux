"""Regression checks for benchmark reporting and transient polling failures."""

import argparse
import asyncio
import importlib
from pathlib import Path

import httpx
import pytest


@pytest.fixture
def runner(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1] / "scripts"))
    return importlib.import_module("reliability_benchmark")


def test_poll_recovers_from_disconnect(runner):
    calls = 0

    def handle(request):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise httpx.RemoteProtocolError("Server disconnected", request=request)
        return httpx.Response(200, json={"id": "run", "status": "SUCCEEDED"})

    async def exercise():
        async with httpx.AsyncClient(
            base_url="http://test", transport=httpx.MockTransport(handle)
        ) as client:
            result = await runner.wait(client, {"id": "run", "status_url": "/run"}, 1)
            assert result["status"] == "SUCCEEDED"
            assert result["poll_transport_errors"] == 1

    asyncio.run(exercise())


def test_poll_disconnects_remain_bounded(runner):
    def handle(request):
        raise httpx.RemoteProtocolError("Server disconnected", request=request)

    async def exercise():
        async with httpx.AsyncClient(
            base_url="http://test", transport=httpx.MockTransport(handle)
        ) as client:
            with pytest.raises(TimeoutError, match="poll transport errors=1"):
                await runner.wait(client, {"id": "run", "status_url": "/run"}, 0.01)

    asyncio.run(exercise())


def test_idempotency_null_error_is_not_failure(runner):
    class Stack:
        experiment_id = "exp"
        intents = []

        def reconcile(self):
            for record in self.intents:
                record.database = {"id": "run", "status": "SUCCEEDED"}

    def handle(request):
        return httpx.Response(
            202 if request.method == "POST" else 200,
            json={"id": "run", "status_url": "/run", "status": "SUCCEEDED", "error": None},
        )

    async def exercise():
        async with httpx.AsyncClient(
            base_url="http://test", transport=httpx.MockTransport(handle)
        ) as client:
            result = await runner.idempotency(
                Stack(), client, argparse.Namespace(requests=3, delay=0.1, timeout=1)
            )
            assert result["passed"]
            assert result["errors"] == []

    asyncio.run(exercise())


@pytest.fixture
def common(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1] / "scripts"))
    return importlib.import_module("benchmark_common")


class FakeClock:
    def __init__(self):
        self.now = 0

    def __call__(self):
        return self.now

    async def sleep(self, duration):
        self.now += duration


def test_canceled_terminates_and_counts_separately(common):
    async def exercise():
        record = common.Intent(
            "exp", {}, run_id="run", status_url="/run", acceptance="OBSERVED_202", began=0
        )
        clock = FakeClock()
        async with httpx.AsyncClient(
            base_url="http://test",
            transport=httpx.MockTransport(
                lambda _: httpx.Response(
                    200, json={"id": "run", "status": "CANCELED", "error": None}
                )
            ),
        ) as client:
            result = await common.poll_intent(client, record, 1, clock=clock, sleep=clock.sleep)
        assert result["status"] == "CANCELED"
        assert common.accounting([record])["canceled"] == 1
        assert record.observation == "TERMINAL"

    asyncio.run(exercise())


def test_response_loss_reuses_one_intent_and_key(common):
    keys = []
    bodies = []
    clock = FakeClock()

    def handle(request):
        keys.append(request.headers["Idempotency-Key"])
        bodies.append(request.content)
        if len(keys) == 1:
            raise httpx.ReadError("sentinel-raw-key", request=request)
        return httpx.Response(
            202, json={"id": "one-run", "status_url": "/run", "status": "PENDING"}
        )

    async def exercise():
        record = common.Intent("exp", {"input": "private"}, began=0)
        async with httpx.AsyncClient(
            base_url="http://test", transport=httpx.MockTransport(handle)
        ) as client:
            assert await common.submit_intent(client, record, 1, clock=clock, sleep=clock.sleep)
        assert len(keys) == 2 and len(set(keys)) == 1 and bodies[0] == bodies[1]
        assert record.post_transport_errors == 1
        assert common.accounting([record])["committed"] == 1
        import json

        assert "sentinel-raw-key" not in json.dumps(record.report())
        assert record.key not in json.dumps(record.report())

    asyncio.run(exercise())


def test_persistent_disconnect_and_http_failures_end_at_deadline(common):
    async def exercise():
        for response in (None, 503):
            clock = FakeClock()
            record = common.Intent(
                "exp",
                {},
                run_id="run",
                status_url="/run",
                acceptance="OBSERVED_202",
                observed={"status": "RUNNING"},
                began=0,
            )

            def handle(request, response=response):
                if response is None:
                    raise httpx.ReadError("private", request=request)
                return httpx.Response(response)

            async with httpx.AsyncClient(
                base_url="http://test", transport=httpx.MockTransport(handle)
            ) as client:
                assert (
                    await common.poll_intent(client, record, 1, clock=clock, sleep=clock.sleep)
                    is None
                )
            assert record.observation == "POLL_DEADLINE"
            assert record.poll_transport_errors + record.poll_http_errors > 0
            assert clock.now == 1
            assert common.accounting([record])["client_unresolved_committed"] == 1

    asyncio.run(exercise())


def test_reconciliation_accounts_rejection_failure_unknown_and_committed_unobserved(common):
    records = [
        common.Intent("exp", {}, acceptance="REJECTED", rejection_status=429),
        common.Intent("exp", {}),
        common.Intent(
            "exp",
            {},
            acceptance="OBSERVED_202",
            run_id="failed",
            observation="TERMINAL",
            observed={"status": "FAILED"},
        ),
        common.Intent("exp", {}, post_transport_errors=1),
    ]
    common.reconcile(
        records,
        [
            {"intent_id": records[-1].intent_id, "id": "lost-response", "status": "PENDING"},
            {"intent_id": records[2].intent_id, "id": "failed", "status": "FAILED"},
        ],
    )
    counts = common.accounting(records)
    assert (
        counts["unique_intents"]
        == counts["committed"] + counts["definitive_rejections"] + counts["acceptance_unknown"]
        == 4
    )
    assert counts["committed"] == counts["failed"] + counts["pending"] == 2
    assert counts["observed_202"] == 1
    assert counts["client_unresolved_committed"] == 1
    assert records[-1].acceptance == "UNKNOWN"  # DB discovery is not an HTTP acceptance.


def test_clock_anomalies_remain_visible_and_fail_summary(common):
    module = importlib.import_module("benchmark")
    record = common.Intent(
        "exp",
        {},
        acceptance="OBSERVED_202",
        observation="TERMINAL",
        completion_ms=500,
        observed={
            "id": "run",
            "status": "SUCCEEDED",
            "queue_latency_ms": 0,
            "created_at": "2026-01-01T00:00:01",
            "started_at": "2026-01-01T00:00:00",
        },
    )
    result = common.timing_diagnostics(record)
    assert result["raw_queue_delta_ms"] == -1000
    assert result["anomalies"] == ["NEGATIVE_QUEUE_DELTA", "STORED_QUEUE_MISMATCH"]
    assert not module.summarize([record], 1)["passed"]
    record.observed.update(created_at="2026-01-01T00:00:00", started_at="2026-01-01T00:10:00")
    assert "QUEUE_EXCEEDS_OBSERVATION_WINDOW" in common.timing_diagnostics(record)["anomalies"]


def test_diagnostics_and_partial_report_precede_teardown(runner, tmp_path):
    import json

    from benchmark_common import Intent

    calls = []

    class Stack:
        intents = [Intent("exp", {"input": "sentinel-prompt"}, key="sentinel-key")]

        def reconcile(self):
            calls.append("reconcile")
            raise RuntimeError("sentinel-key")

        def diagnostics(self):
            calls.append("diagnostics")
            return {
                "logs": runner.safe_logs(
                    "exception sentinel-key\n"
                    '{"event":"http_request","raw":"sentinel-key","status_code":202}'
                )
            }

        def compose(self, *args):
            calls.append("teardown")
            assert (tmp_path / "report.json").exists()
            raise RuntimeError("sentinel-key")

    report = {"passed": True}
    runner.finalize(Stack(), report, lambda: runner.save_report(tmp_path / "report.json", report))
    assert calls == ["reconcile", "diagnostics", "teardown"]
    serialized = (tmp_path / "report.json").read_text()
    assert "sentinel-key" not in serialized and "sentinel-prompt" not in serialized
    assert json.loads(serialized)["cleanup_error"] == "CLEANUP_FAILED"
    assert not report["passed"]


def test_safe_config_excludes_secrets(runner):
    import json

    stack = object.__new__(runner.Stack)
    stack.config = {
        "services": {
            "api": {
                "environment": {
                    "SIAMUX_DATABASE_URL": "postgres://sentinel-secret",
                    "SIAMUX_API_KEY": "sentinel-secret",
                    "SIAMUX_PROVIDER_TIMEOUT": "sentinel-secret",
                    "SIAMUX_LEASE_SECONDS": "30",
                }
            }
        }
    }
    assert "sentinel-secret" not in json.dumps(stack.safe_config())
    assert stack.safe_config()["api"]["timing"] == {"SIAMUX_LEASE_SECONDS": 30}


@pytest.mark.parametrize(
    "status,code",
    [(429, "TENANT_CAPACITY_EXCEEDED"), (503, "SERVICE_CAPACITY_EXCEEDED"), (401, "UNAUTHORIZED")],
)
def test_definitive_rejections_are_separate_from_unknown_acceptance(common, status, code):
    async def exercise():
        clock = FakeClock()
        record = common.Intent("exp", {}, began=0)
        async with httpx.AsyncClient(
            base_url="http://test",
            transport=httpx.MockTransport(
                lambda _: httpx.Response(
                    status, json={"error": {"code": code, "message": "sentinel-secret"}}
                )
            ),
        ) as client:
            assert not await common.submit_intent(client, record, 1, clock=clock, sleep=clock.sleep)
        assert record.rejection_status == status and record.rejection_code == code
        counts = common.accounting([record])
        assert counts["definitive_rejections"] == 1 and counts["acceptance_unknown"] == 0
        import json

        assert "sentinel-secret" not in json.dumps(record.report())

    asyncio.run(exercise())


def test_crash_canceled_before_injection_is_unsuccessful(runner):
    class Stack:
        experiment_id = "exp"
        intents = []

        def reconcile(self):
            for record in self.intents:
                record.database = record.observed

        def compose(self, *args):
            pytest.fail("Terminal work must not trigger crash injection")

    def handle(request):
        return httpx.Response(
            202 if request.method == "POST" else 200,
            json={"id": "run", "status_url": "/run", "status": "CANCELED"},
        )

    async def exercise():
        async with httpx.AsyncClient(
            base_url="http://test", transport=httpx.MockTransport(handle)
        ) as client:
            result = await runner.crashes(Stack(), client, argparse.Namespace(trials=1, timeout=1))
            assert not result["passed"]
            assert result["trials"][0]["error"] == "INJECTION_NOT_PERFORMED"

    asyncio.run(exercise())


def test_incomplete_scaling_never_produces_capacity_summary(runner, monkeypatch):
    from benchmark_common import Intent

    class Stack:
        experiment_id = "exp"
        intents = []

        def workers(self, count):
            pass

        def reconcile(self):
            pass

    async def failed(args, **kwargs):
        kwargs["intents"].append(
            Intent(
                "exp",
                {},
                acceptance="OBSERVED_202",
                observed={"status": "RUNNING"},
                observation="POLL_DEADLINE",
            )
        )
        return {"elapsed_seconds": 1, "passed": False}

    monkeypatch.setattr(runner, "benchmark", failed)

    def handle(request):
        return httpx.Response(
            202 if request.method == "POST" else 200,
            json={"id": "run", "status_url": "/run", "status": "SUCCEEDED"},
        )

    async def exercise():
        async with httpx.AsyncClient(
            base_url="http://test", transport=httpx.MockTransport(handle)
        ) as client:
            args = argparse.Namespace(
                workers=[1, 2, 4], repeats=3, jobs=1, concurrency=1, delay=0, timeout=1
            )
            result = await runner.scaling(Stack(), client, args, "http://test")
            assert not result["passed"]
            assert "median_jobs_per_second" not in result
            assert len(result["runs"]) == 1

    asyncio.run(exercise())


def test_timestamp_offsets_and_invalid_samples(common):
    record = common.Intent(
        "exp",
        {},
        observed={
            "created_at": "2026-01-01T01:00:00+01:00",
            "started_at": "2026-01-01T00:00:01Z",
            "queue_latency_ms": 1000,
        },
    )
    assert common.timing_diagnostics(record)["raw_queue_delta_ms"] == 1000
    assert not common.timing_diagnostics(record)["anomalies"]
    record.observed["started_at"] = "invalid"
    assert common.timing_diagnostics(record)["anomalies"] == ["INVALID_TIMESTAMP"]


def test_ambiguous_submission_deadline_stays_unknown(common):
    async def exercise():
        clock = FakeClock()
        record = common.Intent("exp", {}, began=0)
        keys = []

        def handle(request):
            keys.append(request.headers["Idempotency-Key"])
            raise httpx.ReadError("sentinel-secret", request=request)

        async with httpx.AsyncClient(
            base_url="http://test", transport=httpx.MockTransport(handle)
        ) as client:
            assert not await common.submit_intent(client, record, 1, clock=clock, sleep=clock.sleep)
        assert clock.now == 1 and len(set(keys)) == 1
        assert common.accounting([record])["acceptance_unknown"] == 1
        assert record.observation == "ACCEPTANCE_DEADLINE"

    asyncio.run(exercise())


def test_clock_shift_is_visible_even_without_per_run_anomaly(common):
    start = [
        {
            "source": "api",
            "utc": "2026-01-01T00:00:00Z",
            "host_before": "2026-01-01T00:00:06Z",
            "host_after": "2026-01-01T00:00:06.1Z",
            "host_monotonic_before": 100,
        }
    ]
    end = [
        {
            "source": "api",
            "utc": "2026-01-01T00:00:20Z",
            "host_before": "2026-01-01T00:00:20Z",
            "host_after": "2026-01-01T00:00:20.1Z",
            "host_monotonic_before": 114,
        }
    ]
    row = common.clock_diagnostics(start, end)[0]
    assert row["anomalies"] == ["CLOCK_OFFSET_SHIFT"]
    assert row["offset_shift_seconds"] == pytest.approx([5.9, 6.1])
    assert row["host_wall_minus_monotonic_seconds"] == 0
    # A stable offset is harmless; sampling uncertainty is retained.
    end[0]["utc"] = "2026-01-01T00:00:14Z"
    assert common.clock_diagnostics(start, end)[0]["anomalies"] == []
    # A host UTC jump is also diagnosed relative to that same runner's monotonic clock.
    end[0]["host_monotonic_before"] = 111
    assert "HOST_CLOCK_SHIFT" in common.clock_diagnostics(start, end)[0]["anomalies"]
    assert common.clock_diagnostics(start, [])[0]["anomalies"] == ["INVALID_CLOCK_SAMPLE"]


def test_clock_shift_invalidates_final_capacity_summary(runner):
    from benchmark_common import Intent

    start = {
        "source": "api",
        "utc": "2026-01-01T00:00:00Z",
        "host_before": "2026-01-01T00:00:06Z",
        "host_after": "2026-01-01T00:00:06.1Z",
    }
    end = {
        "source": "api",
        "utc": "2026-01-01T00:00:20Z",
        "host_before": "2026-01-01T00:00:20Z",
        "host_after": "2026-01-01T00:00:20.1Z",
    }

    class Stack:
        intents = [
            Intent(
                "exp",
                {},
                acceptance="OBSERVED_202",
                observation="TERMINAL",
                observed={"status": "SUCCEEDED"},
                database={"status": "SUCCEEDED"},
            )
        ]

        def reconcile(self):
            pass

        def diagnostics(self):
            return {"clocks": [end]}

        def compose(self, *args):
            pass

    report = {
        "passed": True,
        "clocks_start": [start],
        "results": {"scaling": {"passed": True, "runs": [{"succeeded": 1}]}},
    }
    runner.finalize(Stack(), report, lambda: None)
    assert not report["passed"]
    assert not report["results"]["scaling"]["passed"]
    assert report["results"]["scaling"]["error"] == "CLOCK_INTEGRITY_FAILED"
    assert report["results"]["scaling"]["runs"] == [{"succeeded": 1}]
    assert report["accounting"]["succeeded"] == 1
