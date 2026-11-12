"""Client-only observation/accounting; credentials and payloads never enter reports."""

import asyncio
import hashlib
import os
import platform
import subprocess
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import httpx

TERMINAL = {"SUCCEEDED", "FAILED", "CANCELED"}
STATES = ("SUCCEEDED", "FAILED", "CANCELED", "PENDING", "RUNNING")
SAFE_FIELDS = {
    "id",
    "status",
    "attempts",
    "created_at",
    "started_at",
    "completed_at",
    "due_at",
    "dispatch_after",
    "lease_until",
    "queue_latency_ms",
    "provider_duration_ms",
}
SCHEMA_VERSION = 2


def utc():
    return datetime.now(UTC).isoformat()


def credential_headers():
    raw = os.environ.get("SIAMUX_API_KEY")
    if not raw:
        raise RuntimeError("SIAMUX_API_KEY is required")
    return {"Authorization": f"Bearer {raw}"}


def provenance():
    root = Path(__file__).resolve().parents[1]

    def git(*args):
        return subprocess.check_output(["git", *args], cwd=root, stderr=subprocess.DEVNULL)

    try:
        files = git("ls-files", "--cached", "--others", "--exclude-standard", "-z").split(b"\0")
        digest = hashlib.sha256()
        for name in sorted(set(files)):
            if not name:
                continue
            path = root / os.fsdecode(name)
            # Hash source/config/docs only; no ignored .env files, reports, or credentials.
            if path.is_file() and (
                path.suffix in {".py", ".md", ".toml", ".lock", ".yaml", ".yml", ".ini", ".example"}
                or path.name in {"Dockerfile", ".gitignore", ".dockerignore"}
            ):
                digest.update(name + b"\0" + path.read_bytes() + b"\0")
        return {
            "git_sha": git("rev-parse", "HEAD").decode().strip(),
            "dirty_tree": bool(git("status", "--porcelain")),
            "source_fingerprint_sha256": digest.hexdigest(),
            "python": platform.python_version(),
            "runtime": platform.platform(),
        }
    except (OSError, subprocess.SubprocessError):
        return {"source_metadata": "UNAVAILABLE", "python": platform.python_version()}


def safe_run(run):
    return {key: value for key, value in run.items() if key in SAFE_FIELDS}


@dataclass
class Intent:
    experiment_id: str
    # These two fields are private transport data and are deliberately never serialized.
    payload: dict = field(repr=False)
    key: str = field(default_factory=lambda: str(uuid4()), repr=False)
    intent_id: str = field(default_factory=lambda: str(uuid4()))
    run_id: str | None = None
    status_url: str | None = field(default=None, repr=False)
    acceptance: str = "UNKNOWN"
    rejection_status: int | None = None
    rejection_code: str | None = None
    observed: dict = field(default_factory=dict)
    database: dict | None = None
    reconciliation: str = "UNAVAILABLE"
    post_requests: int = 0
    accepted_responses: int = 0
    post_transport_errors: int = 0
    post_http_errors: int = 0
    poll_transport_errors: int = 0
    poll_http_errors: int = 0
    observation: str = "NOT_STARTED"
    created_at: str = field(default_factory=utc)
    last_observed_at: str | None = None
    began: float = field(default_factory=time.monotonic, repr=False)
    acceptance_ms: float | None = None
    completion_ms: float | None = None

    def report(self):
        return {
            k: v
            for k, v in vars(self).items()
            if k not in {"payload", "key", "status_url", "began"}
        }


async def submit_intent(client, intent, timeout, *, clock=time.monotonic, sleep=asyncio.sleep):
    deadline = clock() + timeout
    ambiguous = False
    while clock() < deadline:
        intent.post_requests += 1
        try:
            async with asyncio.timeout(max(0.001, deadline - clock())):
                response = await client.post(
                    "/v1/runs",
                    headers={"Idempotency-Key": intent.key},
                    json=intent.payload,
                    timeout=max(0.001, min(10, deadline - clock())),
                )
        except (httpx.TransportError, TimeoutError):
            ambiguous = True
            intent.post_transport_errors += 1
        else:
            if response.status_code == 202:
                run = response.json()
                intent.run_id, intent.status_url = run["id"], run["status_url"]
                intent.acceptance = "OBSERVED_202"
                intent.accepted_responses += 1
                intent.acceptance_ms = (clock() - intent.began) * 1000
                intent.observed = safe_run(run)
                intent.last_observed_at = utc()
                return True
            intent.post_http_errors += 1
            try:
                code = response.json().get("error", {}).get("code")
            except (ValueError, AttributeError):
                code = None
            safe_code = (
                code
                if code
                in {
                    "UNAUTHORIZED",
                    "INVALID_REQUEST",
                    "UNSUPPORTED_MODEL",
                    "IDEMPOTENCY_CONFLICT",
                    "TENANT_CAPACITY_EXCEEDED",
                    "SERVICE_CAPACITY_EXCEEDED",
                    "DATABASE_UNAVAILABLE",
                }
                else "HTTP_REJECTION"
            )

            # Server/database failures can follow an ambiguous commit. Reuse the key.
            if response.status_code >= 500 and response.status_code != 503:
                ambiguous = True
            elif response.status_code == 503:
                try:
                    capacity = (
                        response.json().get("error", {}).get("code") == "SERVICE_CAPACITY_EXCEEDED"
                    )
                except ValueError:
                    capacity = False
                if capacity and not ambiguous:
                    intent.acceptance, intent.rejection_status = "REJECTED", 503
                    intent.rejection_code = safe_code
                    return False
                ambiguous = True
            else:
                if not ambiguous:
                    intent.acceptance = "REJECTED"
                    intent.rejection_status = response.status_code
                    intent.rejection_code = safe_code
                return False
        await sleep(min(0.1, max(0, deadline - clock())))
    intent.observation = "ACCEPTANCE_DEADLINE"
    return False


async def poll_intent(
    client, intent, timeout, *, running=False, clock=time.monotonic, sleep=asyncio.sleep
):
    deadline = clock() + timeout
    while clock() < deadline:
        try:
            async with asyncio.timeout(max(0.001, deadline - clock())):
                response = await client.get(
                    intent.status_url, timeout=max(0.001, min(10, deadline - clock()))
                )
        except (httpx.TransportError, TimeoutError):
            intent.poll_transport_errors += 1
        else:
            if response.status_code != 200:
                intent.poll_http_errors += 1
            else:
                run = response.json()
                intent.observed = safe_run(run)
                intent.last_observed_at = utc()
                status = run["status"]
                if status in TERMINAL or (running and status == "RUNNING"):
                    intent.observation = "TERMINAL" if status in TERMINAL else "RUNNING"
                    if status in TERMINAL:
                        intent.completion_ms = (clock() - intent.began) * 1000
                    return intent.observed
        await sleep(min(0.1, max(0, deadline - clock())))
    intent.observation = "POLL_DEADLINE"
    return None


def reconcile(intents, rows):
    """Bound query results use intent_id aliases; raw submission keys never leave the helper."""
    by_intent = {row["intent_id"]: row for row in rows}
    for intent in intents:
        row = by_intent.get(intent.intent_id)
        if row:
            intent.database = safe_run(row)
            intent.run_id = row["id"]
            intent.reconciliation = "MATCHED"
        else:
            intent.reconciliation = "MISSING" if intent.run_id else "NOT_FOUND"


def accounting(intents):
    counts = {state.lower(): 0 for state in STATES}
    committed = rejected = unknown = unresolved = observed_202 = 0
    for intent in intents:
        if intent.acceptance == "OBSERVED_202":
            observed_202 += 1
        if intent.database is not None or intent.acceptance == "OBSERVED_202":
            committed += 1
            source = intent.database or intent.observed
            state = source.get("status")
            if state not in STATES:
                raise ValueError("INVALID_COMMITTED_STATE")
            counts[state.lower()] += 1
            if intent.observation != "TERMINAL" or intent.reconciliation == "MISSING":
                unresolved += 1
        elif intent.acceptance == "REJECTED":
            rejected += 1
        else:
            unknown += 1
    assert len(intents) == committed + rejected + unknown
    assert committed == sum(counts.values())
    return {
        "unique_intents": len(intents),
        "committed": committed,
        "definitive_rejections": rejected,
        "acceptance_unknown": unknown,
        "observed_202": observed_202,
        "client_unresolved_committed": unresolved,
        **counts,
    }


def timing_diagnostics(intent):
    source = intent.database or intent.observed
    result = {"intent_id": intent.intent_id, "anomalies": []}
    if source.get("started_at") and source.get("created_at"):

        def timestamp(value):
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed.astimezone(UTC).replace(tzinfo=None) if parsed.tzinfo else parsed

        try:
            started = timestamp(source["started_at"])
            created = timestamp(source["created_at"])
        except (ValueError, TypeError):
            result["anomalies"].append("INVALID_TIMESTAMP")
            return result
        raw = (started - created).total_seconds() * 1000
        result["raw_queue_delta_ms"] = raw
        stored = source.get("queue_latency_ms")
        if raw < 0:
            result["anomalies"].append("NEGATIVE_QUEUE_DELTA")
        if stored is not None and abs(raw - stored) > 1:
            result["anomalies"].append("STORED_QUEUE_MISMATCH")
        window = intent.completion_ms
        if window is not None and raw > window + 1000:
            result["anomalies"].append("QUEUE_EXCEEDS_OBSERVATION_WINDOW")
    return result


def clock_diagnostics(start, end, tolerance_seconds=1):
    """Bound clock-offset changes by sampling uncertainty; never correct raw samples.

    Host monotonic readings, when present, all belong to this runner process.
    A constant Docker/host UTC offset cancels in durations; a changing offset does not.
    """
    before = {sample["source"]: sample for sample in start}
    after = {sample["source"]: sample for sample in end}
    rows = []

    def timestamp(value):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)

    def bounds(sample):
        observed = timestamp(sample.get("utc") or sample["database_utc"])
        return (
            (observed - timestamp(sample["host_after"])).total_seconds(),
            (observed - timestamp(sample["host_before"])).total_seconds(),
        )

    for source in sorted(before.keys() | after.keys()):
        row = {"source": source, "anomalies": []}
        try:
            first, last = before[source], after[source]
            low, high = bounds(first)
            end_low, end_high = bounds(last)
            shift_low, shift_high = end_low - high, end_high - low
            row.update(
                start_offset_seconds=[low, high],
                end_offset_seconds=[end_low, end_high],
                offset_shift_seconds=[shift_low, shift_high],
            )
            if shift_low > tolerance_seconds or shift_high < -tolerance_seconds:
                row["anomalies"].append("CLOCK_OFFSET_SHIFT")
            if "host_monotonic_before" in first and "host_monotonic_before" in last:
                wall_elapsed = (
                    timestamp(last["host_before"]) - timestamp(first["host_before"])
                ).total_seconds()
                mono_elapsed = last["host_monotonic_before"] - first["host_monotonic_before"]
                row["host_wall_minus_monotonic_seconds"] = wall_elapsed - mono_elapsed
                if abs(wall_elapsed - mono_elapsed) > tolerance_seconds:
                    row["anomalies"].append("HOST_CLOCK_SHIFT")
        except (KeyError, ValueError, TypeError):
            row["anomalies"].append("INVALID_CLOCK_SAMPLE")
        rows.append(row)
    return rows
