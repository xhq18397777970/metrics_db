from __future__ import annotations

import io
import json
from datetime import datetime, timezone

from cluster_metrics_platform.api.app import create_app


class FakeBaselineService:
    def query_baseline(self, request):  # pragma: no cover - unused in this module
        raise AssertionError("baseline endpoint should not be called")


class FakeMetricsTableService:
    def __init__(self) -> None:
        self.calls = []

    def list_recent_points(self, *, page: int = 1, page_size: int = 100):
        self.calls.append((page, page_size))
        return {
            "page": page,
            "page_size": page_size,
            "total_rows": 1234,
            "total_pages": 13,
            "max_rows": 5000,
            "start_row": ((page - 1) * page_size) + 1,
            "end_row": ((page - 1) * page_size) + 1,
            "rows": [
                {
                    "bucket_time": datetime(2026, 3, 12, 5, 40, tzinfo=timezone.utc),
                    "application_name": "应用A",
                    "cluster_name": "cluster-a",
                    "metric_name": "cpu_avg",
                    "metric_value": 42.0,
                    "labels": {},
                    "source_tool": "cpu",
                    "collected_at": datetime(2026, 3, 12, 5, 45, tzinfo=timezone.utc),
                }
            ],
        }


def _call_get(app, path: str, query_string: str = ""):
    status_headers: dict[str, object] = {}

    def start_response(status, headers):
        status_headers["status"] = status
        status_headers["headers"] = headers

    response_body = b"".join(
        app(
            {
                "REQUEST_METHOD": "GET",
                "PATH_INFO": path,
                "QUERY_STRING": query_string,
                "CONTENT_LENGTH": "0",
                "wsgi.input": io.BytesIO(b""),
            },
            start_response,
        )
    )
    return (
        status_headers["status"],
        dict(status_headers["headers"]),
        response_body,
    )


def test_dashboard_page_renders_refresh_button() -> None:
    app = create_app(FakeBaselineService(), FakeMetricsTableService())

    status, headers, body = _call_get(app, "/")

    assert status == "200 OK"
    assert headers["Content-Type"] == "text/html; charset=utf-8"
    html = body.decode("utf-8")
    assert "<title>集群指标看板</title>" in html
    assert "<h1>集群指标库</h1>" in html
    assert "展示 metric_points 中最新的 5000 条数据" in html
    assert 'id="refresh-button"' in html
    assert ">刷新<" in html
    assert ">应用<" in html
    assert 'id="prev-button"' in html
    assert 'id="next-button"' in html
    assert ">上一页<" in html
    assert ">下一页<" in html
    assert 'href="/collection-status"' in html
    assert "任务后台" in html
    assert '<option value="100" selected>100</option>' in html


def test_metrics_recent_api_returns_serialized_rows() -> None:
    service = FakeMetricsTableService()
    app = create_app(FakeBaselineService(), service)

    status, headers, body = _call_get(app, "/api/v1/metrics/recent", "page=2&page_size=50")

    assert status == "200 OK"
    assert headers["Content-Type"] == "application/json"
    payload = json.loads(body.decode("utf-8"))
    assert payload == {
        "page": 2,
        "page_size": 50,
        "total_rows": 1234,
        "total_pages": 13,
        "max_rows": 5000,
        "start_row": 51,
        "end_row": 51,
        "rows": [
            {
                "bucket_time": "2026-03-12T05:40:00+00:00",
                "application_name": "应用A",
                "cluster_name": "cluster-a",
                "metric_name": "cpu_avg",
                "metric_value": 42.0,
                "labels": {},
                "source_tool": "cpu",
                "collected_at": "2026-03-12T05:45:00+00:00",
            }
        ],
    }
    assert service.calls == [(2, 50)]


def test_metrics_recent_api_rejects_invalid_page_size() -> None:
    app = create_app(FakeBaselineService(), FakeMetricsTableService())

    status, _, body = _call_get(app, "/api/v1/metrics/recent", "page_size=abc")

    assert status == "400 Bad Request"
    assert json.loads(body.decode("utf-8")) == {"error": "page_size must be an integer"}


def test_metrics_recent_api_rejects_invalid_page() -> None:
    app = create_app(FakeBaselineService(), FakeMetricsTableService())

    status, _, body = _call_get(app, "/api/v1/metrics/recent", "page=abc")

    assert status == "400 Bad Request"
    assert json.loads(body.decode("utf-8")) == {"error": "page must be an integer"}


def test_metrics_recent_api_defaults_to_page_1_with_100_rows() -> None:
    service = FakeMetricsTableService()
    app = create_app(FakeBaselineService(), service)

    status, headers, body = _call_get(app, "/api/v1/metrics/recent")

    assert status == "200 OK"
    assert headers["Content-Type"] == "application/json"
    payload = json.loads(body.decode("utf-8"))
    assert payload["page"] == 1
    assert payload["page_size"] == 100
    assert service.calls == [(1, 100)]
