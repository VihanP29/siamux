"""Bounded client-observation workload; durable reconciliation is supplied by isolated runner."""

import argparse
import asyncio
import json
import math
import time
from uuid import uuid4

import httpx

if __package__:
    from scripts.benchmark_common import (
        SCHEMA_VERSION,
        Intent,
        accounting,
        credential_headers,
        poll_intent,
        provenance,
        submit_intent,
        timing_diagnostics,
    )
else:
    from benchmark_common import (
        SCHEMA_VERSION,
        Intent,
        accounting,
        credential_headers,
        poll_intent,
        provenance,
        submit_intent,
        timing_diagnostics,
    )


def percentile(values, fraction):
    values = [v for v in values if v is not None]
    return sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)] if values else None


def summarize(intents, elapsed):
    counts = accounting(intents)
    timings = [timing_diagnostics(i) for i in intents]
    passed = (
        bool(intents)
        and counts["succeeded"] == len(intents)
        and not counts["client_unresolved_committed"]
        and not any(t["anomalies"] for t in timings)
    )
    return {
        **counts,
        "elapsed_seconds": elapsed,
        "passed": passed,
        "successful_jobs_per_second": counts["succeeded"] / elapsed if elapsed else None,
        "acceptance_p50_ms": percentile([i.acceptance_ms for i in intents], 0.5),
        "acceptance_p95_ms": percentile([i.acceptance_ms for i in intents], 0.95),
        "completion_p95_ms": percentile([i.completion_ms for i in intents], 0.95),
        "queue_p95_ms": percentile(
            [(i.database or i.observed).get("queue_latency_ms") for i in intents], 0.95
        ),
        "total_attempts": sum((i.database or i.observed).get("attempts", 0) for i in intents),
        "timing_diagnostics": timings,
        "intents": [i.report() for i in intents],
    }


async def benchmark(args, *, client=None, intents=None, experiment_id=None):
    if client is None:
        async with httpx.AsyncClient(
            base_url=args.url, headers=credential_headers(), timeout=10
        ) as owned:
            return await benchmark(args, client=owned, intents=intents, experiment_id=experiment_id)
    experiment_id = experiment_id or str(uuid4())
    records = []
    semaphore = asyncio.Semaphore(args.concurrency)
    start = time.monotonic()

    async def run(index):
        async with semaphore:
            intent = Intent(
                experiment_id,
                {
                    "provider": "mock",
                    "input": f"benchmark {index}",
                    "mock": {"delay_seconds": args.delay, "fail_first_attempts": args.fail_first},
                },
            )
            records.append(intent)
            if intents is not None:
                intents.append(intent)
            began = time.monotonic()
            if await submit_intent(client, intent, min(10, args.timeout)):
                await poll_intent(client, intent, max(0, args.timeout - (time.monotonic() - began)))

    await asyncio.gather(*(run(i) for i in range(args.jobs)))
    return {
        "schema_version": SCHEMA_VERSION,
        "experiment_id": experiment_id,
        "source": provenance(),
        "workload": {k: v for k, v in vars(args).items() if k != "url"},
        **summarize(records, time.monotonic() - start),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:8000")
    parser.add_argument("--jobs", type=int, default=100)
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument("--delay", type=float, default=0.1)
    parser.add_argument("--fail-first", type=int, default=0)
    parser.add_argument("--timeout", type=float, default=120)
    args = parser.parse_args()
    if args.jobs < 1 or args.concurrency < 1 or args.timeout <= 0:
        parser.error("jobs, concurrency, and timeout must be positive")
    print(json.dumps(asyncio.run(benchmark(args)), indent=2))


if __name__ == "__main__":
    main()
