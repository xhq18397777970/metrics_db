"""Baseline route parsing and serialization helpers."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from cluster_metrics_platform.domain.models import BaselineQuery, BaselineResponse


def build_baseline_query(payload: dict[str, Any]) -> BaselineQuery:
    """Normalize an API payload into a baseline query model."""

    required_fields = ("cluster_name", "metric_name", "start_time", "end_time", "mode")
    for field_name in required_fields:
        if field_name not in payload:
            raise ValueError(f"missing required field: {field_name}")

    aggregations = tuple(payload.get("aggregations", ("avg", "p50", "p95")))
    if not aggregations:
        raise ValueError("aggregations must not be empty")

    return BaselineQuery(
        cluster_name=str(payload["cluster_name"]),
        metric_name=str(payload["metric_name"]),
        start_time=parse_datetime(payload["start_time"]),
        end_time=parse_datetime(payload["end_time"]),
        mode=str(payload["mode"]),
        lookback_days=int(payload.get("lookback_days", 7)),
        aggregations=tuple(str(item) for item in aggregations),
    )


def serialize_baseline_response(response: BaselineResponse) -> dict[str, Any]:
    """Convert a baseline response model into a JSON-serializable payload."""

    return {
        "cluster_name": response.cluster_name,
        "metric_name": response.metric_name,
        "mode": response.mode,
        "start_time": _serialize_datetime(response.start_time),
        "end_time": _serialize_datetime(response.end_time),
        "status": response.status,
        "baseline_summary": response.baseline_summary,
        "points": tuple(_serialize_point(point) for point in response.points),
    }


def parse_datetime(raw_value: Any) -> datetime:
    """Parse ISO-like timestamps and default naive inputs to UTC."""

    if not isinstance(raw_value, str):
        raise ValueError("datetime values must be strings")

    parsed = datetime.fromisoformat(raw_value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _serialize_point(point: dict[str, Any]) -> dict[str, Any]:
    serialized = {}
    for key, value in point.items():
        if isinstance(value, datetime):
            serialized[key] = _serialize_datetime(value)
        else:
            serialized[key] = value
    return serialized


def _serialize_datetime(value: datetime) -> str:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc).isoformat()
    return value.astimezone(timezone.utc).isoformat()
