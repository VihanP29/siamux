"""Disrupt this project's local Compose worker and Redis, then verify recovery."""

import argparse
import subprocess
import time
from pathlib import Path
from uuid import uuid4

import httpx


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--docker", default="docker")
    parser.add_argument("--url", default="http://localhost:8000")
    args = parser.parse_args()

    def compose(*command):
        subprocess.run(
            [args.docker, "compose", *command],
            check=True,
            cwd=Path(__file__).resolve().parents[1],
        )

    with httpx.Client(base_url=args.url, timeout=10) as client:

        def submit(delay):
            response = client.post(
                "/v1/runs",
                headers={"Idempotency-Key": str(uuid4())},
                json={
                    "provider": "mock",
                    "input": "recovery experiment",
                    "mock": {"delay_seconds": delay},
                },
            )
            assert response.status_code == 202, response.text
            return response.json()["status_url"]

        def wait_for(url, status, timeout=90):
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                response = client.get(url)
                response.raise_for_status()
                result = response.json()
                if result["status"] == status:
                    return result
                if result["status"] in {"SUCCEEDED", "FAILED"}:
                    raise AssertionError(result)
                time.sleep(0.1)
            raise AssertionError(f"Timed out waiting for {url} to reach {status}")

        try:
            compose("stop", "redis")
            url = submit(0.1)
            assert client.get(url).json()["status"] == "PENDING"
        finally:
            compose("start", "redis")
        recovered = wait_for(url, "SUCCEEDED")
        print(
            f"PASS Redis outage: accepted offline, recovered in {recovered['attempts']} attempt(s)",
            flush=True,
        )

        url = submit(4.0)
        wait_for(url, "RUNNING")
        try:
            compose("kill", "-s", "SIGKILL", "worker")
        finally:
            compose("start", "worker")
        recovered = wait_for(url, "SUCCEEDED")
        assert recovered["attempts"] == 2, recovered
        print("PASS worker crash: expired lease recovered on attempt 2", flush=True)


if __name__ == "__main__":
    main()
