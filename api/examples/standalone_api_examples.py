"""Simple examples for calling the standalone baseline/statistics APIs."""

from __future__ import annotations

import json
import os

import requests

DEFAULT_BASE_URL = os.getenv("BASELINE_QUERY_API_BASE_URL", "http://127.0.0.1:8003")


def call_baseline_query() -> dict[str, object]:
    """Call the baseline average query endpoint with a sample payload."""

    payload = {
        "cluster_name": "lf-lan-ha1",
        "metric_name": "cpu_avg",
        "start_time": "2026-03-12 16:49:46",
        "end_time": "2026-03-12 19:49:46",
    }
    response = requests.post(
        f"{DEFAULT_BASE_URL}/api/v1/baseline-query",
        json=payload,
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def call_statistics_query() -> dict[str, object]:
    """Call the statistics query endpoint with a sample payload."""

    payload = {
        "cluster_name": "lf-lan-ha1",
        "metric_name": "cpu_avg",
        "start_time": "2026-03-12 16:49:46",
        "end_time": "2026-03-12 19:49:46",
    }
    response = requests.post(
        f"{DEFAULT_BASE_URL}/api/v1/statistics-query",
        json=payload,
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def main() -> int:
    """Print sample responses for both standalone API endpoints."""

    baseline_result = call_baseline_query()
    statistics_result = call_statistics_query()

    print("baseline query result:")
    print(json.dumps(baseline_result, ensure_ascii=False, indent=2))
    print()
    print("statistics query result:")
    print(json.dumps(statistics_result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
