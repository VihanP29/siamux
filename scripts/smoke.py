"""Exercise an actual running HTTP API, broker, dispatcher, and worker."""

import argparse
import time
from uuid import uuid4

import httpx

if __package__:
    from scripts.benchmark_common import credential_headers
else:
    from benchmark_common import credential_headers


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:8000")
    args = parser.parse_args()
    with httpx.Client(base_url=args.url, timeout=10, headers=credential_headers()) as client:
        client.get("/health/ready").raise_for_status()
        for scenario, options, expected, attempts in [
            ("success", {}, "SUCCEEDED", 1),
            ("retry", {"fail_first_attempts": 2}, "SUCCEEDED", 3),
            ("failure", {"permanent_failure": True}, "FAILED", 1),
            ("timeout", {"delay_seconds": 6.0}, "FAILED", 3),
        ]:
            key = str(uuid4())
            headers = {"Idempotency-Key": key}
            body = {"provider": "mock", "input": "smoke test", "mock": options}
            response = client.post("/v1/runs", json=body, headers=headers)
            assert response.status_code == 202, response.status_code
            run = response.json()
            replay = client.post("/v1/runs", json=body, headers=headers)
            assert replay.status_code == 202 and replay.json()["id"] == run["id"]
            conflict = client.post("/v1/runs", json={**body, "input": "changed"}, headers=headers)
            assert conflict.status_code == 409
            deadline = time.monotonic() + 90
            while time.monotonic() < deadline:
                status = client.get(run["status_url"])
                status.raise_for_status()
                result = status.json()
                if result["status"] in {"SUCCEEDED", "FAILED", "CANCELED"}:
                    break
                time.sleep(0.2)
            else:
                raise AssertionError(f"Run did not finish: {run['id']}")
            assert result["status"] == expected, result
            assert result["attempts"] == attempts, result
            print(f"PASS {scenario}: {run['id']} -> {expected} ({attempts} attempts)")


if __name__ == "__main__":
    main()
