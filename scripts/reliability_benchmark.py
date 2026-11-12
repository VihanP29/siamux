"""Reliability/scaling in a private disposable stack, with durable reconciliation."""

import argparse
import asyncio
import json
import statistics
import subprocess
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import httpx

if __package__:
    from scripts.benchmark import benchmark, summarize
    from scripts.benchmark_common import (
        SCHEMA_VERSION,
        Intent,
        accounting,
        clock_diagnostics,
        poll_intent,
        provenance,
        reconcile,
        submit_intent,
        timing_diagnostics,
        utc,
    )
else:
    from benchmark import benchmark, summarize
    from benchmark_common import (
        SCHEMA_VERSION,
        Intent,
        accounting,
        clock_diagnostics,
        poll_intent,
        provenance,
        reconcile,
        submit_intent,
        timing_diagnostics,
        utc,
    )

ROOT = Path(__file__).resolve().parents[1]
SAFE_ENV = {
    "SIAMUX_PROVIDER_TIMEOUT",
    "SIAMUX_LEASE_SECONDS",
    "SIAMUX_MAX_ATTEMPTS",
    "SIAMUX_DISPATCH_INTERVAL",
    "SIAMUX_REDISPATCH_SECONDS",
    "SIAMUX_RETRY_BASE_SECONDS",
}


def docker(*args, input_data=None):
    result = subprocess.run(
        ["docker", *args], cwd=ROOT, capture_output=True, text=True, timeout=60, input=input_data
    )
    if result.returncode:
        # stderr and commands can embed credentials from resolved configuration.
        raise RuntimeError("DOCKER_COMMAND_FAILED")
    return result.stdout.strip()


def safe_logs(value):
    events = []
    omitted = 0
    allowed = {
        "http_request",
        "run_accepted",
        "run_canceled",
        "run_started",
        "run_finished",
        "stale_completion_ignored",
        "database_unavailable",
        "unexpected_provider_error",
        "dispatch_failed",
        "run_dispatched",
    }
    for line in value.splitlines()[-300:]:
        try:
            row = json.loads(line[line.index("{") :])
        except (ValueError, TypeError):
            omitted += 1
            continue
        if row.get("event") not in allowed:
            omitted += 1
            continue
        safe = {"event": row["event"]}
        for key in ("attempt", "duration_ms", "status_code"):
            if isinstance(row.get(key), (float, int)):
                safe[key] = row[key]
        if row.get("status") in {"PENDING", "RUNNING", "SUCCEEDED", "FAILED", "CANCELED"}:
            safe["status"] = row["status"]
        events.append(safe)
    return {"events": events, "omitted_lines": omitted}


class Stack:
    def __init__(self, directory, experiment_id):
        self.project = f"siamux-bench-{uuid4().hex[:10]}"
        self.experiment_id = experiment_id
        self.intents = []
        self.tenant_id = None
        self.key = None
        self.path = Path(directory) / "compose.json"
        self.config = json.loads(docker("compose", "config", "--format", "json"))
        self.config.pop("name", None)
        for kind in ("volumes", "networks"):
            for resource in self.config.get(kind, {}).values():
                resource.pop("name", None)
                if resource.get("external"):
                    raise ValueError("ISOLATION_REQUIRES_PRIVATE_RESOURCES")
        for service in self.config["services"].values():
            service.pop("ports", None)
            service.pop("container_name", None)
        self.config["services"]["api"]["ports"] = [
            {"target": 8000, "published": "0", "host_ip": "127.0.0.1"}
        ]
        self.write()

    def write(self):
        self.path.write_text(json.dumps(self.config))
        self.path.chmod(0o600)

    def compose(self, *args, input_data=None):
        return docker(
            "compose", "-p", self.project, "-f", str(self.path), *args, input_data=input_data
        )

    def probe(self, operation, **data):
        return json.loads(
            self.compose(
                "exec",
                "-T",
                "api",
                "python",
                "-m",
                "siamux.benchmark_probe",
                input_data=json.dumps({"operation": operation, **data}),
            )
        )

    def reconcile(self):
        if self.tenant_id:
            # Keys enter stdin, never the command line, report, or returned DB snapshot.
            rows = self.probe(
                "snapshot",
                tenant_id=self.tenant_id,
                intents=[
                    {"intent_id": i.intent_id, "run_id": i.run_id, "key": i.key}
                    for i in self.intents
                ],
            )
            reconcile(self.intents, rows)

    def safe_config(self):
        result = {}
        for name, service in self.config["services"].items():
            env = service.get("environment", {})
            # Only numeric timing values are exported, even for whitelisted names.
            settings = {}
            for key in SAFE_ENV & env.keys():
                try:
                    settings[key] = float(env[key])
                except (TypeError, ValueError):
                    pass
            result[name] = {
                "timing": settings,
                "resource_limits": service.get("deploy", {}).get("resources", {}).get("limits", {}),
            }
        return result

    def workers(self, count):
        self.config["services"]["worker"]["command"] = [
            "celery",
            "-A",
            "siamux.worker:celery_app",
            "worker",
            "--loglevel=INFO",
            f"--concurrency={count}",
        ]
        self.write()
        self.compose("up", "-d", "--no-deps", "--force-recreate", "worker")

    def clocks(self):
        samples = []
        for service in ("api", "worker"):
            start, began = utc(), time.monotonic()
            try:
                value = self.compose(
                    "exec",
                    "-T",
                    service,
                    "python",
                    "-c",
                    "from datetime import datetime,UTC; print(datetime.now(UTC).isoformat())",
                )
                value = datetime.fromisoformat(value).isoformat()
                samples.append(
                    {
                        "source": service,
                        "utc": value,
                        "host_before": start,
                        "host_after": utc(),
                        "uncertainty_seconds": time.monotonic() - began,
                        "host_monotonic_before": began,
                        "host_monotonic_after": time.monotonic(),
                    }
                )
            except Exception:
                samples.append({"source": service, "error": "CLOCK_SAMPLE_FAILED"})
        began, before = time.monotonic(), utc()
        try:
            state = self.probe("database_state")
            samples.append(
                {
                    "source": "database",
                    **state,
                    "host_before": before,
                    "host_after": utc(),
                    "uncertainty_seconds": time.monotonic() - began,
                    "host_monotonic_before": began,
                    "host_monotonic_after": time.monotonic(),
                }
            )
        except Exception:
            samples.append({"source": "database", "error": "CLOCK_SAMPLE_FAILED"})
        return samples

    def diagnostics(self):
        result = {"captured_at": utc(), "logs": {}, "containers": []}
        for service in ("api", "dispatcher", "worker", "postgres"):
            try:
                result["logs"][service] = safe_logs(
                    self.compose("logs", "--no-color", "--tail", "300", service)
                )
            except Exception:
                result["logs"][service] = {"error": "LOG_CAPTURE_FAILED"}
        try:
            ids = self.compose("ps", "-aq").splitlines()
            if ids:
                for container in json.loads(docker("inspect", *ids)):
                    state = container["State"]
                    result["containers"].append(
                        {
                            "service": container.get("Config", {})
                            .get("Labels", {})
                            .get("com.docker.compose.service"),
                            **{
                                k: state.get(k)
                                for k in (
                                    "Status",
                                    "ExitCode",
                                    "OOMKilled",
                                    "StartedAt",
                                    "FinishedAt",
                                )
                            },
                            "restart_count": container.get("RestartCount"),
                            "memory_limit": container.get("HostConfig", {}).get("Memory"),
                            "nano_cpus": container.get("HostConfig", {}).get("NanoCpus"),
                        }
                    )
                result["resources"] = [
                    json.loads(line)
                    for line in docker(
                        "stats",
                        "--no-stream",
                        "--format",
                        '{"cpu":"{{.CPUPerc}}","memory":"{{.MemUsage}}","pids":"{{.PIDs}}"}',
                        *ids,
                    ).splitlines()
                ]
        except Exception:
            result["container_capture_error"] = "CAPTURE_FAILED"
        result["clocks"] = self.clocks()
        return result


def intent(stack, delay):
    record = Intent(
        stack.experiment_id,
        {"provider": "mock", "input": "reliability benchmark", "mock": {"delay_seconds": delay}},
    )
    stack.intents.append(record)
    return record


async def wait(client, run, timeout, running=False):
    """Compatibility polling helper; always stops on any terminal state."""
    record = Intent("poll", {}, run_id=run["id"], status_url=run["status_url"])
    result = await poll_intent(client, record, timeout, running=running)
    if result is None:
        raise TimeoutError(f"POLL_DEADLINE; poll transport errors={record.poll_transport_errors}")
    return {**result, "poll_transport_errors": record.poll_transport_errors}


async def run_one(stack, client, delay, timeout):
    record = intent(stack, delay)
    if await submit_intent(client, record, min(timeout, 10)):
        await poll_intent(client, record, timeout)
    return record


def result_for(records):
    counts = accounting(records)
    timings = [timing_diagnostics(i) for i in records]
    return {
        **counts,
        "intents": [i.report() for i in records],
        "timing_diagnostics": timings,
        "passed": bool(records)
        and counts["succeeded"] == len(records)
        and not counts["client_unresolved_committed"]
        and not any(t["anomalies"] for t in timings),
    }


async def outage(stack, client, args):
    records = [intent(stack, args.delay) for _ in range(args.jobs)]
    semaphore = asyncio.Semaphore(args.concurrency)

    async def post(record):
        async with semaphore:
            await submit_intent(client, record, min(10, args.timeout))

    try:
        stack.compose("stop", "redis")
        began = time.monotonic()
        await asyncio.gather(*(post(i) for i in records))
        offline_seconds = time.monotonic() - began
    finally:
        restored = time.monotonic()
        stack.compose("start", "redis")
    deadline = time.monotonic() + args.timeout

    async def observe(record):
        async with semaphore:
            if record.run_id:
                await poll_intent(client, record, max(0, deadline - time.monotonic()))

    await asyncio.gather(*(observe(i) for i in records))
    stack.reconcile()
    return {
        **result_for(records),
        "accepted_while_redis_stopped": sum(i.acceptance == "OBSERVED_202" for i in records),
        "offline_submission_seconds": offline_seconds,
        "drain_seconds_from_redis_start_command": time.monotonic() - restored,
    }


async def crashes(stack, client, args):
    trials = []
    for _ in range(args.trials):
        record = intent(stack, 4.0)
        if not await submit_intent(client, record, min(10, args.timeout)):
            trials.append({"intent_id": record.intent_id, "passed": False, "error": "NOT_ACCEPTED"})
            continue
        observed = await poll_intent(client, record, args.timeout, running=True)
        if not observed or observed["status"] != "RUNNING":
            trials.append(
                {"intent_id": record.intent_id, "passed": False, "error": "INJECTION_NOT_PERFORMED"}
            )
            continue
        began = time.monotonic()
        try:
            stack.compose("kill", "-s", "SIGKILL", "worker")
        finally:
            stack.compose("start", "worker")
        result = await poll_intent(client, record, args.timeout)
        trials.append(
            {
                "intent_id": record.intent_id,
                "seconds_from_kill_command": time.monotonic() - began,
                "passed": bool(
                    result and result["status"] == "SUCCEEDED" and result["attempts"] >= 2
                ),
            }
        )
    stack.reconcile()
    durations = [t["seconds_from_kill_command"] for t in trials if t["passed"]]
    return {
        "trials": trials,
        "recovered": len(durations),
        "passed": len(durations) == args.trials,
        "recovery_median_seconds": statistics.median(durations) if durations else None,
    }


async def idempotency(stack, client, args):
    record = intent(stack, args.delay)
    gate = asyncio.Event()

    # Each HTTP request has private counters; all reuse one intent/key/payload.
    async def one():
        request = Intent(record.experiment_id, record.payload, key=record.key)
        await gate.wait()
        await submit_intent(client, request, min(10, args.timeout))
        return request

    tasks = [asyncio.create_task(one()) for _ in range(args.requests)]
    gate.set()
    responses = await asyncio.gather(*tasks)
    accepted = [r for r in responses if r.acceptance == "OBSERVED_202"]
    ids = {r.run_id for r in accepted}
    for name in (
        "post_requests",
        "accepted_responses",
        "post_transport_errors",
        "post_http_errors",
    ):
        setattr(record, name, sum(getattr(r, name) for r in responses))
    if accepted:
        record.acceptance = "OBSERVED_202"
        record.run_id, record.status_url = accepted[0].run_id, accepted[0].status_url
        record.observed = accepted[0].observed
        await poll_intent(client, record, args.timeout)
    elif all(r.acceptance == "REJECTED" for r in responses):
        record.acceptance = "REJECTED"
    stack.reconcile()
    count = int(record.database is not None)
    return {
        **result_for([record]),
        "requests": args.requests,
        "accepted_responses": len(accepted),
        "unique_run_ids": len(ids),
        "database_rows_for_key": count,
        "errors": ["SUBMISSION_NOT_OBSERVED"] * (len(responses) - len(accepted)),
        "passed": len(accepted) == args.requests
        and len(ids) == count == 1
        and record.observed.get("status") == "SUCCEEDED",
    }


async def scaling(stack, client, args, url):
    rows = []
    for processes in args.workers:
        stack.workers(processes)
        warmup = await run_one(stack, client, args.delay, args.timeout)
        if warmup.observed.get("status") != "SUCCEEDED":
            return {"runs": rows, "passed": False, "error": "WARMUP_FAILED"}
        for repeat in range(args.repeats):
            print(f"Scaling: {processes} processes, repeat {repeat + 1}/{args.repeats}", flush=True)
            offset = len(stack.intents)
            result = await benchmark(
                argparse.Namespace(
                    url=url,
                    jobs=args.jobs,
                    concurrency=args.concurrency,
                    delay=args.delay,
                    fail_first=0,
                    timeout=args.timeout,
                ),
                client=client,
                intents=stack.intents,
                experiment_id=stack.experiment_id,
            )
            stack.reconcile()
            result.update(summarize(stack.intents[offset:], result["elapsed_seconds"]))
            rows.append({"worker_processes": processes, "repeat": repeat + 1, **result})
            if not result["passed"]:
                return {"runs": rows, "passed": False, "error": "INCOMPLETE_SCALING_DATA"}
    medians = {
        str(n): statistics.median(
            r["successful_jobs_per_second"] for r in rows if r["worker_processes"] == n
        )
        for n in args.workers
    }
    baseline = medians[str(args.workers[0])]
    return {
        "runs": rows,
        "median_jobs_per_second": medians,
        "speedup_vs_first_configuration": {n: v / baseline for n, v in medians.items()},
        "passed": True,
    }


async def execute(stack, args, report, checkpoint=lambda: None):
    stack.compose("up", "--build", "-d", "--wait", "--wait-timeout", "50")
    port = stack.compose("port", "api", "8000").splitlines()[0]
    url = f"http://{port}"
    bootstrap = stack.probe("bootstrap", name=stack.experiment_id, limit=max(100, args.jobs + 20))
    stack.tenant_id, stack.key = bootstrap["tenant_id"], bootstrap["key"]
    report["effective_limits"] = {k: bootstrap[k] for k in ("tenant_limit", "global_limit")}
    report["source"] = provenance()
    report["safe_config"] = stack.safe_config()
    report["clocks_start"] = stack.clocks()
    report["docker_version"] = docker("version", "--format", "{{.Server.Version}}")
    resources = json.loads(docker("info", "--format", "{{json .}}"))
    report["docker_resources"] = {k: resources.get(k) for k in ("NCPU", "MemTotal", "Architecture")}
    async with httpx.AsyncClient(
        base_url=url,
        timeout=10,
        headers={"Authorization": f"Bearer {stack.key}"},
        limits=httpx.Limits(max_connections=max(args.requests, args.concurrency)),
    ) as client:
        await run_one(stack, client, args.delay, args.timeout)
        selected = (
            [args.scenario]
            if args.scenario != "all"
            else ["idempotency", "outage", "crash", "scaling"]
        )
        for name in selected:
            print(f"Running {name} benchmark", flush=True)
            if name == "scaling":
                result = await scaling(stack, client, args, url)
            else:
                result = await {"outage": outage, "crash": crashes, "idempotency": idempotency}[
                    name
                ](stack, client, args)
            report["results"][name] = result
            checkpoint()
            if not result["passed"]:
                break


def save_report(output, report):
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(".tmp")
    temporary.write_text(json.dumps(report, indent=2) + "\n")
    temporary.replace(output)


def finalize(stack, report, checkpoint):
    if stack:
        # Capture partial accounting and diagnostics BEFORE removing any containers/volumes.
        try:
            stack.reconcile()
        except Exception:
            report["reconciliation_error"] = "RECONCILIATION_FAILED"
            report["passed"] = False
        report["intents"] = [i.report() for i in stack.intents]
        try:
            counts = report["accounting"] = accounting(stack.intents)
            report["timing_diagnostics"] = [timing_diagnostics(i) for i in stack.intents]
            if (
                any(t["anomalies"] for t in report["timing_diagnostics"])
                or counts["acceptance_unknown"]
                or counts["client_unresolved_committed"]
                or counts["succeeded"] != counts["unique_intents"]
            ):
                report["passed"] = False
        except Exception:
            report["accounting_error"] = "INVALID_OBSERVATION_DATA"
            report["passed"] = False
        try:
            report["diagnostics"] = stack.diagnostics()
            report["clock_diagnostics"] = clock_diagnostics(
                report.get("clocks_start", []),
                report["diagnostics"].get("clocks", []),
            )
            if any(row["anomalies"] for row in report["clock_diagnostics"]):
                report["passed"] = False
                scaling = report.get("results", {}).get("scaling")
                if scaling is not None:
                    scaling["passed"] = False
                    scaling["timing_valid"] = False
                    scaling["error"] = "CLOCK_INTEGRITY_FAILED"
        except Exception:
            report["diagnostics_error"] = "CAPTURE_FAILED"
            report["passed"] = False
        checkpoint()
        try:
            stack.compose("down", "--volumes", "--remove-orphans")
        except Exception:
            report["cleanup_error"] = "CLEANUP_FAILED"
            report["passed"] = False
    report["finished_at"] = utc()
    checkpoint()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scenario", choices=["all", "outage", "crash", "scaling", "idempotency"])
    parser.add_argument("--jobs", type=int, default=1000)
    parser.add_argument("--concurrency", type=int, default=100)
    parser.add_argument("--delay", type=float, default=0.1)
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--trials", type=int, default=10)
    parser.add_argument("--requests", type=int, default=100)
    parser.add_argument("--workers", type=int, nargs="+", default=[1, 2, 4])
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if (
        min(args.jobs, args.concurrency, args.trials, args.requests, args.repeats, *args.workers)
        < 1
        or args.timeout <= 0
        or not 0 <= args.delay < 5
    ):
        parser.error("counts and timeout must be positive; delay must be in [0, 5)")
    experiment_id = str(uuid4())
    output = args.output or ROOT / "benchmark-results" / (
        datetime.now(UTC).strftime("%Y%m%dT%H%M%S") + f"-{args.scenario}-{experiment_id[:6]}.json"
    )
    report = {
        "schema_version": SCHEMA_VERSION,
        "experiment_id": experiment_id,
        "started_at": utc(),
        "source": provenance(),
        "settings": {k: v for k, v in vars(args).items() if k != "output"},
        "results": {},
        "passed": False,
    }

    def checkpoint():
        save_report(output, report)

    with tempfile.TemporaryDirectory(prefix="siamux-bench-") as directory:
        stack = None
        try:
            stack = Stack(directory, experiment_id)
            report["project"] = stack.project
            asyncio.run(execute(stack, args, report, checkpoint))
            expected = 4 if args.scenario == "all" else 1
            report["passed"] = len(report["results"]) == expected and all(
                r["passed"] for r in report["results"].values()
            )
        except (Exception, KeyboardInterrupt):
            report["error"] = "RUNNER_FAILED_OR_INTERRUPTED"
        finally:
            finalize(stack, report, checkpoint)
    print(f"{'PASS' if report['passed'] else 'FAIL'}: {output}", flush=True)
    raise SystemExit(0 if report["passed"] else 1)


if __name__ == "__main__":
    main()
