from __future__ import annotations

import io
import json
from datetime import datetime
from zoneinfo import ZoneInfo

from api.app import create_app
from api.baseline_query_service import BaselineAverageResponse
from api.metric_statistics_service import MetricStatisticsResponse

SHANGHAI = ZoneInfo("Asia/Shanghai")


class FakeBaselineQueryService:
    def __init__(self, response: BaselineAverageResponse) -> None:
        self.response = response
        self.calls = []

    def query_average(self, request):
        self.calls.append(request)
        return self.response


class FakeMetricStatisticsService:
    def __init__(self, response: MetricStatisticsResponse) -> None:
        self.response = response
        self.calls = []

    def query_statistics(self, request):
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
                "PATH_INFO": "/api/v1/baseline-query",
                "CONTENT_LENGTH": str(len(body)),
                "wsgi.input": io.BytesIO(body),
            },
            start_response,
        )
    )
    return status_headers["status"], json.loads(response_body.decode("utf-8"))


def test_external_baseline_query_api_returns_average() -> None:
    response = BaselineAverageResponse(
        cluster_name="cluster-a",
        metric_name="cpu_avg",
        start_time=datetime(2026, 3, 12, 10, 0, tzinfo=SHANGHAI),
        end_time=datetime(2026, 3, 12, 12, 0, tzinfo=SHANGHAI),
        baseline_value=23.5,
        sample_count=24,
        status="success",
    )
    service = FakeBaselineQueryService(response)
    app = create_app(service)

    status, body = _call_app(
        app,
        {
            "cluster_name": "cluster-a",
            "metric_name": "cpu_avg",
            "start_time": "2026-03-12 10:00:00",
            "end_time": "2026-03-12 12:00:00",
        },
    )

    assert status == "200 OK"
    assert body == {
        "cluster_name": "cluster-a",
        "metric_name": "cpu_avg",
        "start_time": "2026-03-12 10:00:00",
        "end_time": "2026-03-12 12:00:00",
        "baseline_value": 23.5,
        "sample_count": 24,
        "status": "success",
    }
    assert service.calls[0].cluster_name == "cluster-a"
    assert service.calls[0].start_time == datetime(2026, 3, 12, 10, 0, tzinfo=SHANGHAI)
    assert service.calls[0].end_time == datetime(2026, 3, 12, 12, 0, tzinfo=SHANGHAI)


def test_external_baseline_query_api_validates_required_fields() -> None:
    response = BaselineAverageResponse(
        cluster_name="cluster-a",
        metric_name="cpu_avg",
        start_time=datetime(2026, 3, 12, 10, 0, tzinfo=SHANGHAI),
        end_time=datetime(2026, 3, 12, 12, 0, tzinfo=SHANGHAI),
        baseline_value=None,
        sample_count=0,
        status="no_data",
    )
    service = FakeBaselineQueryService(response)
    app = create_app(service)

    status, body = _call_app(
        app,
        {
            "metric_name": "cpu_avg",
            "start_time": "2026-03-12 10:00:00",
            "end_time": "2026-03-12 12:00:00",
        },
    )

    assert status == "400 Bad Request"
    assert body == {"error": "missing required field: cluster_name"}
    assert service.calls == []


def test_external_baseline_query_api_validates_datetime_format() -> None:
    response = BaselineAverageResponse(
        cluster_name="cluster-a",
        metric_name="cpu_avg",
        start_time=datetime(2026, 3, 12, 10, 0, tzinfo=SHANGHAI),
        end_time=datetime(2026, 3, 12, 12, 0, tzinfo=SHANGHAI),
        baseline_value=None,
        sample_count=0,
        status="no_data",
    )
    service = FakeBaselineQueryService(response)
    app = create_app(service)

    status, body = _call_app(
        app,
        {
            "cluster_name": "cluster-a",
            "metric_name": "cpu_avg",
            "start_time": "2026/03/12 10:00:00",
            "end_time": "2026-03-12 12:00:00",
        },
    )

    assert status == "400 Bad Request"
    assert body == {"error": "datetime values must use format YYYY-MM-DD HH:MM:SS"}
    assert service.calls == []


def test_external_statistics_query_api_returns_dispersion_metrics() -> None:
    baseline_service = FakeBaselineQueryService(
        BaselineAverageResponse(
            cluster_name="cluster-a",
            metric_name="cpu_avg",
            start_time=datetime(2026, 3, 12, 10, 0, tzinfo=SHANGHAI),
            end_time=datetime(2026, 3, 12, 12, 0, tzinfo=SHANGHAI),
            baseline_value=23.5,
            sample_count=24,
            status="success",
        )
    )
    statistics_response = MetricStatisticsResponse(
        cluster_name="cluster-a",
        metric_name="cpu_avg",
        start_time=datetime(2026, 3, 12, 10, 0, tzinfo=SHANGHAI),
        end_time=datetime(2026, 3, 12, 12, 0, tzinfo=SHANGHAI),
        variance_value=6.25,
        standard_deviation=2.5,
        range_value=8.0,
        sample_count=24,
        status="success",
    )
    statistics_service = FakeMetricStatisticsService(statistics_response)
    app = create_app(baseline_service, statistics_service)

    body = json.dumps(
        {
            "cluster_name": "cluster-a",
            "metric_name": "cpu_avg",
            "start_time": "2026-03-12 10:00:00",
            "end_time": "2026-03-12 12:00:00",
        }
    ).encode("utf-8")
    status_headers: dict[str, object] = {}

    def start_response(status, headers):
        status_headers["status"] = status
        status_headers["headers"] = headers

    response_body = b"".join(
        app(
            {
                "REQUEST_METHOD": "POST",
                "PATH_INFO": "/api/v1/statistics-query",
                "CONTENT_LENGTH": str(len(body)),
                "wsgi.input": io.BytesIO(body),
            },
            start_response,
        )
    )
    payload = json.loads(response_body.decode("utf-8"))

    assert status_headers["status"] == "200 OK"
    assert payload == {
        "cluster_name": "cluster-a",
        "metric_name": "cpu_avg",
        "start_time": "2026-03-12 10:00:00",
        "end_time": "2026-03-12 12:00:00",
        "variance_value": 6.25,
        "standard_deviation": 2.5,
        "range_value": 8.0,
        "sample_count": 24,
        "status": "success",
    }
    assert statistics_service.calls[0].metric_name == "cpu_avg"


def test_external_statistics_query_api_validates_required_fields() -> None:
    baseline_service = FakeBaselineQueryService(
        BaselineAverageResponse(
            cluster_name="cluster-a",
            metric_name="cpu_avg",
            start_time=datetime(2026, 3, 12, 10, 0, tzinfo=SHANGHAI),
            end_time=datetime(2026, 3, 12, 12, 0, tzinfo=SHANGHAI),
            baseline_value=23.5,
            sample_count=24,
            status="success",
        )
    )
    statistics_service = FakeMetricStatisticsService(
        MetricStatisticsResponse(
            cluster_name="cluster-a",
            metric_name="cpu_avg",
            start_time=datetime(2026, 3, 12, 10, 0, tzinfo=SHANGHAI),
            end_time=datetime(2026, 3, 12, 12, 0, tzinfo=SHANGHAI),
            variance_value=None,
            standard_deviation=None,
            range_value=None,
            sample_count=0,
            status="no_data",
        )
    )
    app = create_app(baseline_service, statistics_service)

    body = json.dumps(
        {
            "metric_name": "cpu_avg",
            "start_time": "2026-03-12 10:00:00",
            "end_time": "2026-03-12 12:00:00",
        }
    ).encode("utf-8")
    status_headers: dict[str, object] = {}

    def start_response(status, headers):
        status_headers["status"] = status
        status_headers["headers"] = headers

    response_body = b"".join(
        app(
            {
                "REQUEST_METHOD": "POST",
                "PATH_INFO": "/api/v1/statistics-query",
                "CONTENT_LENGTH": str(len(body)),
                "wsgi.input": io.BytesIO(body),
            },
            start_response,
        )
    )
    payload = json.loads(response_body.decode("utf-8"))

    assert status_headers["status"] == "400 Bad Request"
    assert payload == {"error": "missing required field: cluster_name"}
    assert statistics_service.calls == []
