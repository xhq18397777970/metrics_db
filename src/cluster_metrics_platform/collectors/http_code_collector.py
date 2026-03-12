"""Collector for HTTP status code metrics."""

from __future__ import annotations

from typing import Any

import tools.code as code_tool
from cluster_metrics_platform.collectors.base import Collector
from cluster_metrics_platform.domain.models import MetricPoint, TimeWindow


class HttpCodeCollector(Collector):
    name = "http_code"

    def collect(self, cluster: str, window: TimeWindow):
        try:
            payload = code_tool.get_cluster_status_code_api(
                cluster,
                window.start_time,
                window.end_time,
            )
        except Exception as exc:  # pragma: no cover - defensive path
            return self._failure(
                f"http code collector execution failed: {exc}",
                code="collector_exception",
            )

        if not isinstance(payload, dict):
            return self._failure(
                "http code collector returned malformed payload",
                code="malformed_response",
            )
        if payload.get("error"):
            return self._failure(str(payload["error"]), code="tool_error")

        metric_mapping = {
            "2xx": ("http_code_count", {"class": "2xx"}),
            "4xx": ("http_code_count", {"class": "4xx"}),
            "5xx": ("http_code_count", {"class": "5xx"}),
        }
        points, invalid_fields = _build_points(
            cluster=cluster,
            window=window,
            source_tool=self.name,
            payload=payload,
            metric_mapping=metric_mapping,
        )
        if not points:
            return self._failure(
                "http code collector returned malformed or empty metrics",
                code="malformed_response",
            )
        if invalid_fields:
            return self._partial_success(
                points,
                message=f"http code collector skipped invalid fields: {', '.join(invalid_fields)}",
                code="partial_metrics",
            )
        return self._success(points)


def _build_points(
    cluster: str,
    window: TimeWindow,
    source_tool: str,
    payload: dict[str, Any],
    metric_mapping: dict[str, tuple[str, dict[str, str]]],
) -> tuple[list[MetricPoint], list[str]]:
    points: list[MetricPoint] = []
    invalid_fields: list[str] = []
    for field_name, (metric_name, labels) in metric_mapping.items():
        value = payload.get(field_name)
        if value is None:
            continue
        if not isinstance(value, (int, float)):
            invalid_fields.append(field_name)
            continue
        points.append(
            MetricPoint(
                cluster_name=cluster,
                bucket_time=window.bucket_time,
                window_start=window.start_time,
                window_end=window.end_time,
                metric_name=metric_name,
                metric_value=float(value),
                labels=labels,
                source_tool=source_tool,
            )
        )
    return points, invalid_fields
