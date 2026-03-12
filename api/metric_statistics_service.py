"""Standalone metric statistics query service backed by TimescaleDB."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

METRIC_STATISTICS_SQL = """
SELECT
    VAR_POP(metric_value) AS variance_value,
    STDDEV_POP(metric_value) AS standard_deviation,
    MAX(metric_value) - MIN(metric_value) AS range_value,
    COUNT(*) AS sample_count
FROM metric_points
WHERE cluster_name = %(cluster_name)s
  AND metric_name = %(metric_name)s
  AND bucket_time >= %(start_time)s::timestamptz
  AND bucket_time < %(end_time)s::timestamptz
"""


@dataclass(frozen=True, slots=True)
class MetricStatisticsQuery:
    """Request model for a single metric statistics lookup."""

    cluster_name: str
    metric_name: str
    start_time: datetime
    end_time: datetime


@dataclass(frozen=True, slots=True)
class MetricStatisticsResponse:
    """Response model for metric variance, standard deviation, and range."""

    cluster_name: str
    metric_name: str
    start_time: datetime
    end_time: datetime
    variance_value: float | None
    standard_deviation: float | None
    range_value: float | None
    sample_count: int
    status: str = "success"


class MetricStatisticsService:
    """Read dispersion metrics from the metric_points hypertable."""

    def __init__(self, connection) -> None:
        self._connection = connection

    def query_statistics(self, request: MetricStatisticsQuery) -> MetricStatisticsResponse:
        """Return variance, standard deviation, and range for one metric query."""

        self._validate_request(request)
        with self._connection.cursor() as cursor:
            cursor.execute(
                METRIC_STATISTICS_SQL,
                {
                    "cluster_name": request.cluster_name,
                    "metric_name": request.metric_name,
                    "start_time": request.start_time,
                    "end_time": request.end_time,
                },
            )
            row = cursor.fetchone() or {}

        sample_count = int(row.get("sample_count") or 0)
        if sample_count == 0:
            return MetricStatisticsResponse(
                cluster_name=request.cluster_name,
                metric_name=request.metric_name,
                start_time=request.start_time,
                end_time=request.end_time,
                variance_value=None,
                standard_deviation=None,
                range_value=None,
                sample_count=0,
                status="no_data",
            )

        return MetricStatisticsResponse(
            cluster_name=request.cluster_name,
            metric_name=request.metric_name,
            start_time=request.start_time,
            end_time=request.end_time,
            variance_value=_round_metric(row["variance_value"]),
            standard_deviation=_round_metric(row["standard_deviation"]),
            range_value=_round_metric(row["range_value"]),
            sample_count=sample_count,
            status="success",
        )

    @staticmethod
    def _validate_request(request: MetricStatisticsQuery) -> None:
        if not request.cluster_name:
            raise ValueError("cluster_name must not be empty")
        if not request.metric_name:
            raise ValueError("metric_name must not be empty")
        if request.end_time <= request.start_time:
            raise ValueError("end_time must be greater than start_time")


def _round_metric(value: float | None) -> float | None:
    if value is None:
        return None
    return round(float(value), 2)
