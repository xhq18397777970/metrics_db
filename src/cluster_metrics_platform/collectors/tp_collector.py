"""Collector for TP metrics."""

from __future__ import annotations

import tools.tp as tp_tool
from cluster_metrics_platform.collectors.base import Collector
from cluster_metrics_platform.domain.models import MetricPoint, TimeWindow


class TpCollector(Collector):
    name = "tp"

    def collect(self, cluster: str, window: TimeWindow):
        try:
            payload = tp_tool.get_cluster_tp_api(cluster, window.start_time, window.end_time)
        except Exception as exc:  # pragma: no cover - defensive path
            return self._failure(
                f"tp collector execution failed: {exc}",
                code="collector_exception",
            )

        if not isinstance(payload, dict):
            return self._failure(
                "tp collector returned malformed payload",
                code="malformed_response",
            )
        if payload.get("error"):
            return self._failure(str(payload["error"]), code="tool_error")

        value = payload.get("tp")
        if not isinstance(value, (int, float)):
            return self._failure(
                "tp collector returned malformed metrics",
                code="malformed_response",
            )

        point = MetricPoint(
            cluster_name=cluster,
            bucket_time=window.bucket_time,
            window_start=window.start_time,
            window_end=window.end_time,
            metric_name="tp_avg",
            metric_value=float(value),
            source_tool=self.name,
        )
        return self._success([point])
