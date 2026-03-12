"""Standalone WSGI app exposing the baseline query interface."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime
from http import HTTPStatus
from typing import Any
from zoneinfo import ZoneInfo

from api.baseline_query_service import BaselineAverageQuery, BaselineAverageResponse
from api.metric_statistics_service import (
    MetricStatisticsQuery,
    MetricStatisticsResponse,
)

StartResponse = Callable[[str, list[tuple[str, str]]], object]
DEFAULT_TIMEZONE = ZoneInfo("Asia/Shanghai")
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def create_app(query_service, statistics_service=None):
    """Create a WSGI application for the external baseline query endpoint."""

    def app(environ: dict[str, Any], start_response: StartResponse):
        method = environ.get("REQUEST_METHOD", "GET")
        path = environ.get("PATH_INFO", "")

        try:
            if path == "/api/v1/baseline-query":
                if method != "POST":
                    return _json_response(
                        start_response,
                        HTTPStatus.METHOD_NOT_ALLOWED,
                        {"error": "method not allowed"},
                    )
                payload = _load_json_body(environ)
                request = build_baseline_average_query(payload)
                response = query_service.query_average(request)
                return _json_response(
                    start_response,
                    HTTPStatus.OK,
                    serialize_baseline_average_response(response),
                )

            if path == "/api/v1/statistics-query":
                if method != "POST":
                    return _json_response(
                        start_response,
                        HTTPStatus.METHOD_NOT_ALLOWED,
                        {"error": "method not allowed"},
                    )
                if statistics_service is None:
                    return _json_response(
                        start_response,
                        HTTPStatus.NOT_FOUND,
                        {"error": "not found"},
                    )
                payload = _load_json_body(environ)
                request = build_metric_statistics_query(payload)
                response = statistics_service.query_statistics(request)
                return _json_response(
                    start_response,
                    HTTPStatus.OK,
                    serialize_metric_statistics_response(response),
                )

            return _json_response(
                start_response,
                HTTPStatus.NOT_FOUND,
                {"error": "not found"},
            )
        except ValueError as exc:
            return _json_response(
                start_response,
                HTTPStatus.BAD_REQUEST,
                {"error": str(exc)},
            )

    return app


def build_baseline_average_query(payload: dict[str, Any]) -> BaselineAverageQuery:
    """Normalize JSON payload into a range-average baseline query."""

    required_fields = ("cluster_name", "metric_name", "start_time", "end_time")
    for field_name in required_fields:
        if field_name not in payload:
            raise ValueError(f"missing required field: {field_name}")

    return BaselineAverageQuery(
        cluster_name=str(payload["cluster_name"]).strip(),
        metric_name=str(payload["metric_name"]).strip(),
        start_time=parse_datetime(payload["start_time"]),
        end_time=parse_datetime(payload["end_time"]),
    )


def serialize_baseline_average_response(
    response: BaselineAverageResponse,
) -> dict[str, Any]:
    """Convert a baseline average response into JSON-ready payload."""

    return {
        "cluster_name": response.cluster_name,
        "metric_name": response.metric_name,
        "start_time": _serialize_datetime(response.start_time),
        "end_time": _serialize_datetime(response.end_time),
        "baseline_value": response.baseline_value,
        "sample_count": response.sample_count,
        "status": response.status,
    }


def build_metric_statistics_query(payload: dict[str, Any]) -> MetricStatisticsQuery:
    """Normalize JSON payload into a metric statistics query."""

    required_fields = ("cluster_name", "metric_name", "start_time", "end_time")
    for field_name in required_fields:
        if field_name not in payload:
            raise ValueError(f"missing required field: {field_name}")

    return MetricStatisticsQuery(
        cluster_name=str(payload["cluster_name"]).strip(),
        metric_name=str(payload["metric_name"]).strip(),
        start_time=parse_datetime(payload["start_time"]),
        end_time=parse_datetime(payload["end_time"]),
    )


def serialize_metric_statistics_response(
    response: MetricStatisticsResponse,
) -> dict[str, Any]:
    """Convert a metric statistics response into JSON-ready payload."""

    return {
        "cluster_name": response.cluster_name,
        "metric_name": response.metric_name,
        "start_time": _serialize_datetime(response.start_time),
        "end_time": _serialize_datetime(response.end_time),
        "variance_value": response.variance_value,
        "standard_deviation": response.standard_deviation,
        "range_value": response.range_value,
        "sample_count": response.sample_count,
        "status": response.status,
    }


def parse_datetime(raw_value: Any) -> datetime:
    """Parse datetimes and default timezone-less values to Asia/Shanghai."""

    if not isinstance(raw_value, str):
        raise ValueError("datetime values must be strings")

    try:
        parsed = datetime.strptime(raw_value, DATETIME_FORMAT)
        return parsed.replace(tzinfo=DEFAULT_TIMEZONE)
    except ValueError:
        pass

    try:
        parsed = datetime.fromisoformat(raw_value)
    except ValueError as exc:
        raise ValueError(
            "datetime values must use format YYYY-MM-DD HH:MM:SS"
        ) from exc

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=DEFAULT_TIMEZONE)
    return parsed


def _load_json_body(environ: dict[str, Any]) -> dict[str, Any]:
    content_length = int(environ.get("CONTENT_LENGTH", "0") or "0")
    raw_body = environ["wsgi.input"].read(content_length)
    if not raw_body:
        raise ValueError("request body must not be empty")

    try:
        payload = json.loads(raw_body.decode("utf-8"))
    except json.JSONDecodeError as exc:  # pragma: no cover - defensive path
        raise ValueError("request body must be valid JSON") from exc

    if not isinstance(payload, dict):
        raise ValueError("request body must be a JSON object")
    return payload


def _json_response(
    start_response: StartResponse,
    status: HTTPStatus,
    payload: dict[str, Any],
):
    encoded = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    start_response(
        f"{status.value} {status.phrase}",
        [
            ("Content-Type", "application/json"),
            ("Content-Length", str(len(encoded))),
        ],
    )
    return [encoded]


def _serialize_datetime(value: datetime) -> str:
    if value.tzinfo is None:
        return value.replace(tzinfo=DEFAULT_TIMEZONE).strftime(DATETIME_FORMAT)
    return value.astimezone(DEFAULT_TIMEZONE).strftime(DATETIME_FORMAT)
