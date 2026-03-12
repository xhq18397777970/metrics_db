from __future__ import annotations

import io
import json
from datetime import datetime, timezone

from cluster_metrics_platform.api.app import create_app


class FakeBaselineService:
    def query_baseline(self, request):  # pragma: no cover - unused in this module
        raise AssertionError("baseline endpoint should not be called")


class FakeCollectionStatusService:
    def __init__(self) -> None:
        self.calls = []

    def get_dashboard_snapshot(self, *, limit: int = 10):
        self.calls.append(limit)
        return {
            "scheduler": {
                "status": "running",
                "is_stale": False,
                "step_minutes": 5,
                "active_bucket_time": datetime(2026, 3, 12, 5, 40, tzinfo=timezone.utc),
                "active_window_start": datetime(2026, 3, 12, 5, 40, tzinfo=timezone.utc),
                "active_window_end": datetime(2026, 3, 12, 5, 45, tzinfo=timezone.utc),
                "selected_cluster_count": 70,
                "total_tasks": 280,
                "completed_tasks": 120,
                "remaining_tasks": 160,
                "success_count": 115,
                "partial_success_count": 2,
                "failed_count": 3,
                "last_points_written": 0,
                "last_runs_written": 0,
                "last_started_at": datetime(2026, 3, 12, 5, 40, tzinfo=timezone.utc),
                "last_finished_at": None,
                "last_heartbeat_at": datetime(2026, 3, 12, 5, 41, tzinfo=timezone.utc),
                "updated_at": datetime(2026, 3, 12, 5, 41, tzinfo=timezone.utc),
                "last_error": "tp timeout",
            },
            "windows": (
                {
                    "bucket_time": datetime(2026, 3, 12, 5, 40, tzinfo=timezone.utc),
                    "window_start": datetime(2026, 3, 12, 5, 40, tzinfo=timezone.utc),
                    "window_end": datetime(2026, 3, 12, 5, 45, tzinfo=timezone.utc),
                    "status": "running",
                    "selected_cluster_count": 70,
                    "total_tasks": 280,
                    "completed_tasks": 120,
                    "remaining_tasks": 160,
                    "success_count": 115,
                    "partial_success_count": 2,
                    "failed_count": 3,
                    "points_written": 0,
                    "runs_written": 0,
                    "started_at": datetime(2026, 3, 12, 5, 40, tzinfo=timezone.utc),
                    "updated_at": datetime(2026, 3, 12, 5, 41, tzinfo=timezone.utc),
                    "finished_at": None,
                    "last_error": "tp timeout",
                    "completion_rate": 42.86,
                },
            ),
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


def test_collection_status_page_renders_dashboard() -> None:
    app = create_app(
        FakeBaselineService(),
        metrics_table_service=None,
        collection_status_service=FakeCollectionStatusService(),
    )

    status, headers, body = _call_get(app, "/collection-status")

    assert status == "200 OK"
    assert headers["Content-Type"] == "text/html; charset=utf-8"
    html = body.decode("utf-8")
    assert "<title>任务后台</title>" in html
    assert "<h1>任务后台</h1>" in html
    assert 'id="refresh-button"' in html
    assert 'id="status-table-body"' in html
    assert 'href="/"' in html
    assert "指标前台" in html


def test_collection_status_api_returns_snapshot() -> None:
    service = FakeCollectionStatusService()
    app = create_app(
        FakeBaselineService(),
        metrics_table_service=None,
        collection_status_service=service,
    )

    status, headers, body = _call_get(app, "/api/v1/collection/status", "limit=20")

    assert status == "200 OK"
    assert headers["Content-Type"] == "application/json"
    payload = json.loads(body.decode("utf-8"))
    assert payload["scheduler"]["status"] == "running"
    assert payload["scheduler"]["completed_tasks"] == 120
    assert payload["windows"][0]["status"] == "running"
    assert service.calls == [20]


def test_collection_status_api_validates_limit() -> None:
    app = create_app(
        FakeBaselineService(),
        metrics_table_service=None,
        collection_status_service=FakeCollectionStatusService(),
    )

    status, _, body = _call_get(app, "/api/v1/collection/status", "limit=abc")

    assert status == "400 Bad Request"
    assert json.loads(body.decode("utf-8")) == {"error": "limit must be an integer"}
