"""Collector for QPS metrics."""

from __future__ import annotations

import tools.qps as qps_tool
from cluster_metrics_platform.collectors.base import Collector
from cluster_metrics_platform.domain.models import MetricPoint, TimeWindow


class QpsCollector(Collector):
    name = "qps"

    def collect(self, cluster: str, window: TimeWindow):
        try:
            payload = qps_tool.get_cluster_qps(cluster, window.start_time, window.end_time)
        except Exception as exc:  # pragma: no cover - defensive path
            return self._failure(
                f"qps collector execution failed: {exc}",
                code="collector_exception",
            )

        if not isinstance(payload, dict):
            return self._failure(
                "qps collector returned malformed payload",
                code="malformed_response",
            )
        if payload.get("error"):
            return self._failure(str(payload["error"]), code="tool_error")

        value = payload.get("qps")
        if not isinstance(value, (int, float)):
            return self._failure(
                "qps collector returned malformed metrics",
                code="malformed_response",
            )

        point = MetricPoint(
            cluster_name=cluster,
            bucket_time=window.bucket_time,
            window_start=window.start_time,
            window_end=window.end_time,
            metric_name="qps_avg",
            metric_value=float(value),
            source_tool=self.name,
        )
        return self._success([point])
