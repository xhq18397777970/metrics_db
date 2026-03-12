from __future__ import annotations

import io
import json
from datetime import datetime, timezone

from cluster_metrics_platform.api.app import create_app
from cluster_metrics_platform.domain.models import BaselineResponse


class FakeBaselineService:
    def __init__(self, response: BaselineResponse) -> None:
        self.response = response
        self.calls = []

    def query_baseline(self, request):
        self.calls.append(request)
        return self.response


def _call_app(app, payload: dict[str, object]):
    body = json.dumps(payload).encode("utf-8")
    status_headers: dict[str, object] = {}

    def start_response(status, headers):
        status_headers["status"] = status
        status_headers["headers"] = headers

    response_body = b"".join(
        app(
            {
                "REQUEST_METHOD": "POST",
                "PATH_INFO": "/api/v1/baselines/query",
                "CONTENT_LENGTH": str(len(body)),
                "wsgi.input": io.BytesIO(body),
            },
            start_response,
        )
    )
    return status_headers["status"], json.loads(response_body.decode("utf-8"))


def test_baseline_api_returns_serialized_response() -> None:
    response = BaselineResponse(
        cluster_name="cluster-a",
        metric_name="cpu_avg",
        mode="historical_range",
        start_time=datetime(2026, 3, 12, 10, 0, tzinfo=timezone.utc),
        end_time=datetime(2026, 3, 12, 10, 10, tzinfo=timezone.utc),
        baseline_summary={"avg": 12.3},
        points=(
            {
                "bucket_time": datetime(2026, 3, 12, 10, 0, tzinfo=timezone.utc),
                "avg": 12.3,
            },
        ),
    )
    service = FakeBaselineService(response)
    app = create_app(service)

    status, body = _call_app(
        app,
        {
            "cluster_name": "cluster-a",
            "metric_name": "cpu_avg",
            "start_time": "2026-03-12T10:00:00+00:00",
            "end_time": "2026-03-12T10:10:00+00:00",
            "mode": "historical_range",
        },
    )

    assert status == "200 OK"
    assert body["status"] == "success"
    assert body["cluster_name"] == "cluster-a"
    assert body["points"] == [
        {
            "bucket_time": "2026-03-12T10:00:00+00:00",
            "avg": 12.3,
        }
    ]
    assert service.calls[0].cluster_name == "cluster-a"


def test_baseline_api_validates_request_body() -> None:
    response = BaselineResponse(
        cluster_name="cluster-a",
        metric_name="cpu_avg",
        mode="historical_range",
        start_time=datetime(2026, 3, 12, 10, 0, tzinfo=timezone.utc),
        end_time=datetime(2026, 3, 12, 10, 10, tzinfo=timezone.utc),
        baseline_summary={},
        points=(),
        status="no_data",
    )
    service = FakeBaselineService(response)
    app = create_app(service)

    status, body = _call_app(
        app,
        {
            "metric_name": "cpu_avg",
            "start_time": "2026-03-12T10:00:00+00:00",
            "end_time": "2026-03-12T10:10:00+00:00",
            "mode": "historical_range",
        },
    )

    assert status == "400 Bad Request"
    assert body == {"error": "missing required field: cluster_name"}
    assert service.calls == []
