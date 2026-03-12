from __future__ import annotations

import io
import json
from pathlib import Path

import pytest

from cluster_metrics_platform.bootstrap import build_default_collectors, create_application
from cluster_metrics_platform.collectors.base import Collector
from cluster_metrics_platform.domain.models import CollectorResult, MetricPoint
from cluster_metrics_platform.settings import AppSettings
from cluster_metrics_platform.storage.db import DatabaseConfig


class StaticCollector(Collector):
    def __init__(self, name: str, points_factory) -> None:
        self.name = name
        self._points_factory = points_factory

    def collect(self, cluster: str, window):
        return CollectorResult(status="success", points=self._points_factory(cluster, window))


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
    return status_headers["status"], json.loads(response_body.decode("utf-8"))


def _write_cluster_config(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "group-a": {
                    "clusters": ["cluster-a"],
                }
            }
        ),
        encoding="utf-8",
    )


def _metric_point(cluster: str, window, metric_name: str, metric_value: float, labels=None):
    return MetricPoint(
        cluster_name=cluster,
        bucket_time=window.bucket_time,
        window_start=window.start_time,
        window_end=window.end_time,
        metric_name=metric_name,
        metric_value=metric_value,
        labels=labels or {},
        source_tool="static",
    )


def test_build_default_collectors_registers_builtin_set() -> None:
    collectors = build_default_collectors()

    assert [collector.name for collector in collectors] == ["cpu", "qps", "http_code", "tp"]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bootstrapped_application_runs_collection_and_serves_baseline(
    timescale_connection,
    tmp_path,
    sample_window,
) -> None:
    cluster_config_path = tmp_path / "cluster.json"
    _write_cluster_config(cluster_config_path)

    settings = AppSettings(
        cluster_config_path=cluster_config_path,
        database=DatabaseConfig(dsn="postgresql:///cluster_metrics_test"),
        initialize_storage=True,
    )
    collectors = [
        StaticCollector(
            "cpu",
            lambda cluster, window: [
                _metric_point(cluster, window, "cpu_avg", 42.0),
                _metric_point(cluster, window, "net_bps", 128.0, {"direction": "in"}),
                _metric_point(cluster, window, "net_bps", 256.0, {"direction": "out"}),
            ],
        ),
        StaticCollector(
            "qps",
            lambda cluster, window: [_metric_point(cluster, window, "qps_avg", 88.0)],
        ),
        StaticCollector(
            "http_code",
            lambda cluster, window: [
                _metric_point(cluster, window, "http_code_count", 100.0, {"class": "2xx"}),
                _metric_point(cluster, window, "http_code_count", 3.0, {"class": "4xx"}),
                _metric_point(cluster, window, "http_code_count", 1.0, {"class": "5xx"}),
            ],
        ),
        StaticCollector(
            "tp",
            lambda cluster, window: [_metric_point(cluster, window, "tp_avg", 12.5)],
        ),
    ]

    application = create_application(
        settings,
        connection=timescale_connection,
        collectors=collectors,
    )

    execution = await application.collection_service.collect_window(sample_window)

    assert execution.points_written == 8
    assert execution.runs_written == 4
    assert [collector.name for collector in application.registry.enabled_collectors()] == [
        "cpu",
        "qps",
        "http_code",
        "tp",
    ]

    with timescale_connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                to_regclass('metric_rollup_1h') AS hourly_view,
                to_regclass('metric_rollup_1d') AS daily_view
            """
        )
        rollups = cursor.fetchone()
        cursor.execute(
            """
            SELECT metric_name, COUNT(*) AS count
            FROM metric_points
            GROUP BY metric_name
            ORDER BY metric_name
            """
        )
        metrics = cursor.fetchall()

    assert rollups["hourly_view"] == "metric_rollup_1h"
    assert rollups["daily_view"] == "metric_rollup_1d"
    assert metrics == [
        {"metric_name": "cpu_avg", "count": 1},
        {"metric_name": "http_code_count", "count": 3},
        {"metric_name": "net_bps", "count": 2},
        {"metric_name": "qps_avg", "count": 1},
        {"metric_name": "tp_avg", "count": 1},
    ]

    status, body = _call_app(
        application.api_app,
        {
            "cluster_name": "cluster-a",
            "metric_name": "qps_avg",
            "start_time": "2026-03-19T10:00:00+00:00",
            "end_time": "2026-03-19T10:05:00+00:00",
            "mode": "last_week_same_range",
        },
    )

    assert status == "200 OK"
    assert body["status"] == "success"
    assert body["baseline_summary"] == {
        "avg": pytest.approx(88.0),
        "p50": pytest.approx(88.0),
        "p95": pytest.approx(88.0),
    }
    assert body["points"] == [
        {
            "bucket_time": "2026-03-19T10:00:00+00:00",
            "avg": pytest.approx(88.0),
            "p50": pytest.approx(88.0),
            "p95": pytest.approx(88.0),
        }
    ]

    metrics_status, metrics_body = _call_get(
        application.api_app,
        "/api/v1/metrics/recent",
        "page=1&page_size=3",
    )

    assert metrics_status == "200 OK"
    assert metrics_body["page"] == 1
    assert metrics_body["page_size"] == 3
    assert metrics_body["total_rows"] == 8
    assert metrics_body["total_pages"] == 3
    assert [row["metric_name"] for row in metrics_body["rows"]] == [
        "cpu_avg",
        "http_code_count",
        "http_code_count",
    ]

    status_status, status_body = _call_get(
        application.api_app,
        "/api/v1/collection/status",
        "limit=3",
    )

    assert status_status == "200 OK"
    assert status_body["scheduler"]["status"] in {"idle", "running", "stopped"}
    assert status_body["windows"][0]["bucket_time"] == "2026-03-12T10:00:00+00:00"
    assert status_body["windows"][0]["selected_cluster_count"] == 1
