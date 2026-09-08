"""Synthetic workload; reports measured values only, never estimates."""

import argparse
import asyncio
import json
import math
import time
from uuid import uuid4

import httpx


def percentile(values, fraction):
    return sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)] if values else None


async def benchmark(args):
    semaphore = asyncio.Semaphore(args.concurrency)
    accepted, completed, queue_ms, attempts, failures = [], [], [], [], []
    start = time.monotonic()
    async with httpx.AsyncClient(base_url=args.url, timeout=10) as client:

        async def run(index):
            async with semaphore:
                began = time.monotonic()
                try:
                    response = await client.post(
                        "/v1/runs",
                        headers={"Idempotency-Key": str(uuid4())},
                        json={
                            "provider": "mock",
                            "input": f"benchmark {index}",
                            "mock": {
                                "delay_seconds": args.delay,
                                "fail_first_attempts": args.fail_first,
                            },
                        },
                    )
                    response.raise_for_status()
                    accepted.append((time.monotonic() - began) * 1000)
                    status_url = response.json()["status_url"]
                    while time.monotonic() - began < args.timeout:
                        result = await client.get(status_url)
                        result.raise_for_status()
                        job = result.json()
                        if job["status"] in {"SUCCEEDED", "FAILED"}:
                            if job["status"] == "FAILED":
                                failures.append(job["error"]["code"])
                            else:
                                completed.append((time.monotonic() - began) * 1000)
                            queue_ms.append(job["queue_latency_ms"])
                            attempts.append(job["attempts"])
                            return
                        await asyncio.sleep(0.1)
                    failures.append("POLL_TIMEOUT")
                except httpx.HTTPError as exc:
                    failures.append(type(exc).__name__)

        await asyncio.gather(*(run(i) for i in range(args.jobs)))
    elapsed = time.monotonic() - start
    return {
        "workload": vars(args),
        "elapsed_seconds": elapsed,
        "accepted": len(accepted),
        "succeeded": len(completed),
        "failed": len(failures),
        "errors": failures,
        "successful_jobs_per_second": len(completed) / elapsed,
        "acceptance_p50_ms": percentile(accepted, 0.5),
        "acceptance_p95_ms": percentile(accepted, 0.95),
        "completion_p95_ms": percentile(completed, 0.95),
        "queue_p95_ms": percentile(queue_ms, 0.95),
        "total_attempts": sum(attempts),
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
